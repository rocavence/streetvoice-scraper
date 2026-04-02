const express = require('express');
const multer = require('multer');
const ExcelJS = require('exceljs');
const puppeteer = require('puppeteer');
const path = require('path');
const fs = require('fs');
const cors = require('cors');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    }
});
const PORT = process.env.PORT || 3000;

// ====== Configuration ======
const CONCURRENCY = 3; // Number of parallel browser pages
const REQUEST_DELAY = 500; // ms between requests (per page)
const PAGE_TIMEOUT = 30000; // ms to wait for page load
const SELECTOR_TIMEOUT = 8000; // ms to wait for a selector

// Middleware
app.use(cors());
app.use(express.static('public'));
app.use(express.json());

// Configure multer for file uploads
const upload = multer({
    dest: 'uploads/',
    fileFilter: (req, file, cb) => {
        if (file.mimetype === 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet') {
            cb(null, true);
        } else {
            cb(new Error('只支援 .xlsx 格式的檔案'));
        }
    },
    limits: {
        fileSize: 10 * 1024 * 1024 // 10MB limit
    }
});

// Global variables
let browser = null;

// Initialize browser
async function initBrowser() {
    if (!browser) {
        browser = await puppeteer.launch({
            headless: true,
            args: [
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-accelerated-2d-canvas',
                '--no-first-run',
                '--no-zygote',
                '--disable-gpu',
                '--disable-background-timer-throttling',
                '--disable-backgrounding-occluded-windows',
                '--disable-renderer-backgrounding',
                '--disable-features=TranslateUI',
                '--disable-extensions',
                '--disable-plugins',
                '--disable-images' // Skip loading images for speed
            ],
            defaultViewport: { width: 1280, height: 720 },
            timeout: 60000
        });
    }
    return browser;
}

// Create a pool of reusable pages
async function createPagePool(size) {
    const pages = [];
    for (let i = 0; i < size; i++) {
        const page = await browser.newPage();
        await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
        // Block unnecessary resources for speed
        await page.setRequestInterception(true);
        page.on('request', (req) => {
            const type = req.resourceType();
            if (['image', 'stylesheet', 'font', 'media'].includes(type)) {
                req.abort();
            } else {
                req.continue();
            }
        });
        pages.push(page);
    }
    return pages;
}

// Close page pool
async function closePagePool(pages) {
    for (const page of pages) {
        try { await page.close(); } catch (e) { /* ignore */ }
    }
}

// Clean up browser on exit
process.on('exit', async () => {
    if (browser) {
        await browser.close();
    }
});

// Excel parsing and validation
async function parseExcelFile(filePath) {
    const workbook = new ExcelJS.Workbook();
    await workbook.xlsx.readFile(filePath);

    const worksheet = workbook.getWorksheet(1);
    if (!worksheet) {
        throw new Error('Excel 檔案中沒有找到工作表');
    }

    const headers = [];
    const firstRow = worksheet.getRow(1);
    firstRow.eachCell((cell, colNumber) => {
        headers[colNumber] = cell.text;
    });

    const songNameCol = headers.findIndex(header => header === '作品名稱') + 1;
    const artistCol = headers.findIndex(header => header === '作者') + 1;

    if (!songNameCol || !artistCol) {
        throw new Error('Excel 檔案必須包含「作品名稱」和「作者」欄位');
    }

    const data = [];
    worksheet.eachRow((row, rowNumber) => {
        if (rowNumber === 1) return;

        const rowData = {};
        row.eachCell((cell, colNumber) => {
            const header = headers[colNumber];
            if (header) {
                rowData[header] = cell.text;
                if (cell.hyperlink) {
                    const hyperlinkUrl = cell.hyperlink.hyperlink || cell.hyperlink.target || cell.hyperlink;
                    rowData[`${header}_hyperlink`] = hyperlinkUrl;
                }
            }
        });

        if (rowData['作品名稱'] && rowData['作者']) {
            rowData.rowNumber = rowNumber;
            data.push(rowData);
        }
    });

    return { workbook, worksheet, headers, data, songNameCol, artistCol };
}

// ====== Optimized Scraping Functions ======

async function scrapeLyrics(page, songUrl) {
    try {
        console.log(`  擷取歌詞: ${songUrl}`);

        await page.goto(songUrl, { waitUntil: 'domcontentloaded', timeout: PAGE_TIMEOUT });

        // Wait for the actual content container instead of blind timeout
        try {
            await page.waitForSelector('.work-page-container-right, .song-content, [class*="lyrics"], [class*="work"]', {
                timeout: SELECTOR_TIMEOUT
            });
        } catch (e) {
            // Content area not found, wait a brief moment and continue
            await new Promise(r => setTimeout(r, 1500));
        }

        // Try clicking "查看更多" if it exists
        try {
            const expanded = await page.evaluate(() => {
                const allEls = document.querySelectorAll('*');
                for (const el of allEls) {
                    const text = el.textContent ? el.textContent.trim() : '';
                    if (text === '查看更多' || text === '...查看更多') {
                        el.click();
                        return true;
                    }
                }
                // Also try expand buttons
                const btns = document.querySelectorAll('button, a, span, div[onclick]');
                for (const btn of btns) {
                    const t = btn.textContent ? btn.textContent.trim() : '';
                    if (t.includes('更多') || t.includes('展開') || t.includes('完整')) {
                        btn.click();
                        return true;
                    }
                }
                return false;
            });

            if (expanded) {
                // Wait briefly for content to expand
                await new Promise(r => setTimeout(r, 1500));
            }
        } catch (e) { /* ignore */ }

        // Extract lyrics
        const lyrics = await page.evaluate(() => {
            // Strategy 1: work-page-container-right
            const rightContainer = document.querySelector('.work-page-container-right');
            if (rightContainer) {
                let text = rightContainer.textContent.trim();
                if (text && text.length > 50) {
                    text = text.replace(/\.\.\.查看更多[\s\S]*$/g, '');
                    text = text.replace(/查看更多[\s\S]*$/g, '');
                    text = text.replace(/收合[\s\S]*$/g, '');

                    const lines = text.split('\n')
                        .map(l => l.trim())
                        .filter(l => {
                            if (!l) return false;
                            if (l === '歌詞') return false;
                            const noise = ['編輯推薦', '發布時間', '街聲', 'StreetVoice', '追蹤', '分享', '播放', '登入', '好聽', '留言'];
                            return !noise.some(n => l.includes(n));
                        });

                    const cleaned = lines.join('\n').trim();
                    if (cleaned.length > 50 && /[\u4e00-\u9fff]/.test(cleaned)) {
                        return cleaned;
                    }
                }
            }

            // Strategy 2: Find longest Chinese text block
            let best = '';
            const allEls = document.querySelectorAll('*');
            for (const el of allEls) {
                const t = el.textContent ? el.textContent.trim() : '';
                if (t.length > 200 && t.length < 3000 && /[\u4e00-\u9fff]/.test(t)
                    && !t.includes('Copyright') && !t.includes('街聲') && !t.includes('登入')) {
                    let clean = t.replace(/\.\.\.查看更多[\s\S]*$/g, '').replace(/查看更多[\s\S]*$/g, '').replace(/收合[\s\S]*$/g, '').replace(/歌詞\s*/, '');
                    const lines = clean.split('\n').map(l => l.trim()).filter(l => l && l !== '歌詞' && !l.includes('編輯') && !l.includes('發布時間') && !l.includes('街聲') && !l.includes('追蹤') && !l.includes('分享'));
                    const final = lines.join('\n').trim();
                    if (final.length > best.length && final.length > 100) best = final;
                }
            }
            return best || null;
        });

        return lyrics || '未找到歌詞';
    } catch (error) {
        console.error(`  擷取歌詞失敗 ${songUrl}:`, error.message);
        return '擷取失敗';
    }
}

async function scrapeArtistInfo(page, artistUrl) {
    try {
        const aboutUrl = artistUrl.endsWith('/') ? artistUrl + 'about' : artistUrl + '/about';
        console.log(`  擷取音樂人資訊: ${aboutUrl}`);

        await page.goto(aboutUrl, { waitUntil: 'domcontentloaded', timeout: PAGE_TIMEOUT });

        // Wait for content to render
        try {
            await page.waitForSelector('[class*="about"], [class*="profile"], [class*="user-page"]', {
                timeout: SELECTOR_TIMEOUT
            });
        } catch (e) {
            await new Promise(r => setTimeout(r, 2000));
        }

        const artistInfo = await page.evaluate(() => {
            let category = '';
            let bio = '';

            const pageText = document.body.textContent;

            // Find category
            const categoryRegex = /音樂人類別[：:\s]*([^\n\r\t]+)/i;
            const categoryMatch = pageText.match(categoryRegex);
            if (categoryMatch && categoryMatch[1]) {
                category = categoryMatch[1].trim();
            } else {
                const allEls = document.querySelectorAll('*');
                for (const el of allEls) {
                    const t = el.textContent;
                    if (t && t.includes('音樂人類別')) {
                        const m = t.match(/音樂人類別[：:\s]*([^\n\r]+)/);
                        if (m && m[1]) { category = m[1].trim(); break; }
                        if (el.nextElementSibling) {
                            const s = el.nextElementSibling.textContent.trim();
                            if (s && s.length < 50) { category = s; break; }
                        }
                    }
                }
            }

            // Find bio
            const allEls = document.querySelectorAll('*');
            const introEl = Array.from(allEls).find(el => el.textContent && el.textContent.trim() === '介紹');

            if (introEl) {
                let container = introEl.parentElement;
                while (container && !bio) {
                    const ct = container.textContent;
                    if (ct && ct.length > 20) {
                        const idx = ct.indexOf('介紹');
                        if (idx !== -1) {
                            const after = ct.substring(idx + 2);
                            const lines = after.split('\n').map(l => l.trim()).filter(l => l && !l.includes('關於') && !l.includes('音樂人類別') && l !== '介紹');
                            if (lines.length > 0) { bio = lines.join('\n').trim(); break; }
                        }
                    }
                    container = container.parentElement;
                }

                if (!bio) {
                    let next = introEl.nextElementSibling;
                    const bioLines = [];
                    let max = 10;
                    while (next && max > 0) {
                        const t = next.textContent.trim();
                        if (t && !t.includes('關於')) bioLines.push(t);
                        next = next.nextElementSibling;
                        max--;
                        if (next && next.tagName && next.tagName.match(/^H[1-6]$/)) break;
                    }
                    if (bioLines.length) bio = bioLines.join('\n');
                }
            }

            return { category: category || '未找到類別', bio: bio || '未找到介紹' };
        });

        return artistInfo;
    } catch (error) {
        console.error(`  擷取音樂人資訊失敗 ${artistUrl}:`, error.message);
        return { category: '擷取失敗', bio: '擷取失敗' };
    }
}

// ====== Parallel Processing Engine ======

async function processRowBatch(pages, rows, options, worksheet, newColumns, startIdx, totalRows) {
    const tasks = rows.map((row, batchIdx) => {
        const pageIdx = batchIdx % pages.length;
        const globalIdx = startIdx + batchIdx;
        return processOneRow(pages[pageIdx], row, options, worksheet, newColumns, globalIdx, totalRows);
    });
    return Promise.all(tasks);
}

async function processOneRow(page, row, options, worksheet, newColumns, idx, totalRows) {
    const progress = Math.round(((idx + 1) / totalRows) * 100);
    const label = `[${idx + 1}/${totalRows}] ${row['作品名稱']}`;

    console.log(`\n處理 ${label}`);

    io.emit('progress', {
        progress: Math.round((idx / totalRows) * 100),
        current: idx + 1,
        total: totalRows,
        status: `處理中：${row['作品名稱']}`,
        log: `開始處理：${row['作品名稱']} (${row['作者']})`
    });

    try {
        // Extract lyrics
        if (options.includes('lyrics')) {
            if (row['作品名稱_hyperlink']) {
                const lyrics = await scrapeLyrics(page, row['作品名稱_hyperlink']);
                worksheet.getCell(row.rowNumber, newColumns.lyrics).value = lyrics;

                io.emit('progress', {
                    progress,
                    current: idx + 1,
                    total: totalRows,
                    status: `歌詞完成：${row['作品名稱']}`,
                    log: `歌詞擷取成功：${lyrics.length} 字`
                });

                await new Promise(r => setTimeout(r, REQUEST_DELAY));
            } else {
                worksheet.getCell(row.rowNumber, newColumns.lyrics).value = '無連結';
            }
        }

        // Extract artist info
        if (options.includes('category') || options.includes('bio')) {
            if (row['作者_hyperlink']) {
                // Use cached result if available
                if (!row._artistInfo) {
                    row._artistInfo = await scrapeArtistInfo(page, row['作者_hyperlink']);
                    await new Promise(r => setTimeout(r, REQUEST_DELAY));
                }

                const artistInfo = row._artistInfo;
                if (options.includes('category')) {
                    worksheet.getCell(row.rowNumber, newColumns.category).value = artistInfo.category;
                }
                if (options.includes('bio')) {
                    worksheet.getCell(row.rowNumber, newColumns.bio).value = artistInfo.bio;
                }

                io.emit('progress', {
                    progress,
                    current: idx + 1,
                    total: totalRows,
                    status: `音樂人資訊完成：${row['作者']}`,
                    log: `音樂人資訊擷取成功：${artistInfo.category}`
                });
            } else {
                if (options.includes('category')) worksheet.getCell(row.rowNumber, newColumns.category).value = '無連結';
                if (options.includes('bio')) worksheet.getCell(row.rowNumber, newColumns.bio).value = '無連結';
            }
        }
    } catch (error) {
        console.error(`  處理失敗 ${label}:`, error.message);
        io.emit('progress', {
            progress,
            current: idx + 1,
            total: totalRows,
            status: `處理失敗：${row['作品名稱']}`,
            log: `錯誤：${error.message}`
        });

        if (options.includes('lyrics') && newColumns.lyrics) worksheet.getCell(row.rowNumber, newColumns.lyrics).value = '擷取失敗';
        if (options.includes('category') && newColumns.category) worksheet.getCell(row.rowNumber, newColumns.category).value = '擷取失敗';
        if (options.includes('bio') && newColumns.bio) worksheet.getCell(row.rowNumber, newColumns.bio).value = '擷取失敗';
    }
}

// ====== Socket.IO ======

io.on('connection', (socket) => {
    console.log('客戶端已連接:', socket.id);
    socket.on('disconnect', () => {
        console.log('客戶端已斷開:', socket.id);
    });
});

// ====== Main Processing Endpoint ======

app.post('/process', upload.single('file'), async (req, res) => {
    if (!req.file) {
        return res.status(400).json({ error: '請上傳檔案' });
    }

    let pages = [];

    try {
        const options = JSON.parse(req.body.options);
        const filePath = req.file.path;

        console.log('開始處理檔案:', req.file.originalname);
        console.log('處理選項:', options);
        console.log(`並行數: ${CONCURRENCY}, 請求間隔: ${REQUEST_DELAY}ms`);

        // Initialize browser and page pool
        await initBrowser();
        pages = await createPagePool(CONCURRENCY);

        // Parse Excel file
        const { workbook, worksheet, data } = await parseExcelFile(filePath);
        console.log(`找到 ${data.length} 筆資料`);

        // Add new columns
        let newColIndex = worksheet.columnCount + 1;
        const newColumns = {};

        if (options.includes('lyrics')) {
            worksheet.getCell(1, newColIndex).value = '歌詞';
            newColumns.lyrics = newColIndex++;
        }
        if (options.includes('category')) {
            worksheet.getCell(1, newColIndex).value = '音樂人類別';
            newColumns.category = newColIndex++;
        }
        if (options.includes('bio')) {
            worksheet.getCell(1, newColIndex).value = '音樂人介紹';
            newColumns.bio = newColIndex++;
        }

        // Pre-cache: deduplicate artist URLs so we don't fetch the same artist twice
        if (options.includes('category') || options.includes('bio')) {
            const artistCache = new Map();
            // First pass: identify unique artists
            for (const row of data) {
                const url = row['作者_hyperlink'];
                if (url && !artistCache.has(url)) {
                    artistCache.set(url, null); // placeholder
                }
            }
            console.log(`共 ${artistCache.size} 位不重複音樂人（共 ${data.length} 筆資料）`);
        }

        io.emit('progress', {
            progress: 0,
            current: 0,
            total: data.length,
            status: `準備處理 ${data.length} 筆資料（${CONCURRENCY} 並行）...`,
            log: `找到 ${data.length} 筆資料，使用 ${CONCURRENCY} 個並行頁面開始處理`
        });

        // Process in batches of CONCURRENCY
        const artistInfoCache = new Map();

        for (let i = 0; i < data.length; i += CONCURRENCY) {
            const batch = data.slice(i, Math.min(i + CONCURRENCY, data.length));

            // Pre-fill artist cache for this batch
            if (options.includes('category') || options.includes('bio')) {
                for (const row of batch) {
                    const url = row['作者_hyperlink'];
                    if (url && artistInfoCache.has(url)) {
                        row._artistInfo = artistInfoCache.get(url);
                    }
                }
            }

            await processRowBatch(pages, batch, options, worksheet, newColumns, i, data.length);

            // Post-process: cache artist info from this batch
            if (options.includes('category') || options.includes('bio')) {
                for (const row of batch) {
                    const url = row['作者_hyperlink'];
                    if (url && row._artistInfo && !artistInfoCache.has(url)) {
                        artistInfoCache.set(url, row._artistInfo);
                    }
                }
            }
        }

        // Completion
        io.emit('progress', {
            progress: 100,
            current: data.length,
            total: data.length,
            status: '處理完成！正在生成檔案...',
            log: `所有 ${data.length} 筆資料處理完成`
        });

        // Generate output file
        const outputPath = `outputs/processed_${Date.now()}_${req.file.originalname}`;
        if (!fs.existsSync('outputs')) fs.mkdirSync('outputs');
        await workbook.xlsx.writeFile(outputPath);

        console.log('處理完成，輸出檔案:', outputPath);

        // Close page pool
        await closePagePool(pages);
        pages = [];

        res.download(outputPath, `processed_${req.file.originalname}`, (err) => {
            if (err) console.error('下載失敗:', err);
            fs.unlink(filePath, () => {});
            setTimeout(() => { fs.unlink(outputPath, () => {}); }, 3600000);
        });

    } catch (error) {
        console.error('處理失敗:', error);
        // Clean up page pool on error
        if (pages.length) await closePagePool(pages);
        res.status(500).json({ error: error.message });
        if (req.file) fs.unlink(req.file.path, () => {});
    }
});

// Health check
app.get('/health', (req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// Start server
server.listen(PORT, () => {
    console.log(`服務器已啟動在 http://localhost:${PORT}`);
    console.log(`並行數: ${CONCURRENCY} | 請求間隔: ${REQUEST_DELAY}ms`);
    console.log('請在瀏覽器中開啟上述網址使用工具');
});

// Graceful shutdown
process.on('SIGINT', async () => {
    console.log('\n正在關閉服務器...');
    if (browser) await browser.close();
    process.exit(0);
});

process.on('SIGTERM', async () => {
    console.log('\n正在關閉服務器...');
    if (browser) await browser.close();
    process.exit(0);
});
