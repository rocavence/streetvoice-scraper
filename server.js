const express = require('express');
const multer = require('multer');
const ExcelJS = require('exceljs');
const axios = require('axios');
const cheerio = require('cheerio');
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
const CONCURRENCY = 3; // Number of parallel requests
const REQUEST_DELAY = 1000; // ms between requests to avoid rate limiting
const REQUEST_TIMEOUT = 30000; // ms timeout for HTTP requests

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

// ====== HTTP Client Setup ======

// Create axios instance with default settings
const axiosInstance = axios.create({
    timeout: REQUEST_TIMEOUT,
    headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-TW,zh;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
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

// ====== HTTP-based Scraping Functions ======

async function scrapeLyrics(songUrl) {
    try {
        console.log(`  擷取歌詞: ${songUrl}`);

        // Make HTTP request to get the page HTML
        const response = await axiosInstance.get(songUrl);
        const $ = cheerio.load(response.data);

        // Extract lyrics using multiple strategies
        let lyrics = null;

        // Strategy 1: Look for work-page-container-right
        const rightContainer = $('.work-page-container-right');
        if (rightContainer.length > 0) {
            let text = rightContainer.text().trim();
            if (text && text.length > 50) {
                // Clean up the text
                text = text.replace(/\.\.\.查看更多[\s\S]*$/g, '');
                text = text.replace(/查看更多[\s\S]*$/g, '');
                text = text.replace(/收合[\s\S]*$/g, '');

                const lines = text.split('\n')
                    .map(l => l.trim())
                    .filter(l => {
                        if (!l) return false;
                        if (l === '歌詞') return false;
                        const noise = ['編輯推薦', '發布時間', '街聲', 'StreetVoice', '追蹤', '分享', '播放', '登入', '好聽', '留言', '收聽次數'];
                        return !noise.some(n => l.includes(n));
                    });

                const cleaned = lines.join('\n').trim();
                if (cleaned.length > 50 && /[\u4e00-\u9fff]/.test(cleaned)) {
                    lyrics = cleaned;
                }
            }
        }

        // Strategy 2: Look for meta description (often contains lyrics preview)
        if (!lyrics) {
            const metaDescription = $('meta[property="og:description"]').attr('content');
            if (metaDescription && metaDescription.length > 50 && /[\u4e00-\u9fff]/.test(metaDescription)) {
                lyrics = metaDescription.trim();
            }
        }

        // Strategy 3: Look for any element with substantial Chinese text
        if (!lyrics) {
            let best = '';
            $('*').each((i, el) => {
                const text = $(el).text().trim();
                if (text.length > 100 && text.length < 5000 && /[\u4e00-\u9fff]/.test(text)
                    && !text.includes('Copyright') && !text.includes('街聲') && !text.includes('登入')
                    && !text.includes('關於') && !text.includes('追蹤')) {

                    let clean = text.replace(/\.\.\.查看更多[\s\S]*$/g, '')
                                  .replace(/查看更多[\s\S]*$/g, '')
                                  .replace(/收合[\s\S]*$/g, '')
                                  .replace(/歌詞\s*/, '');

                    const lines = clean.split('\n')
                        .map(l => l.trim())
                        .filter(l => l && l !== '歌詞' && !l.includes('編輯') && !l.includes('發布時間')
                                 && !l.includes('街聲') && !l.includes('追蹤') && !l.includes('分享'));

                    const final = lines.join('\n').trim();
                    if (final.length > best.length && final.length > 100) {
                        best = final;
                    }
                }
            });
            lyrics = best || null;
        }

        return lyrics || '未找到歌詞';
    } catch (error) {
        console.error(`  擷取歌詞失敗 ${songUrl}:`, error.message);
        return '擷取失敗';
    }
}

async function scrapeArtistInfo(artistUrl) {
    try {
        const aboutUrl = artistUrl.endsWith('/') ? artistUrl + 'about' : artistUrl + '/about';
        console.log(`  擷取音樂人資訊: ${aboutUrl}`);

        // Make HTTP request to get the about page HTML
        const response = await axiosInstance.get(aboutUrl);
        const $ = cheerio.load(response.data);

        let category = '';
        let bio = '';

        // Find category using regex first
        const pageText = $.text();
        const categoryRegex = /音樂人類別[：:\s]*([^\n\r\t]+)/i;
        const categoryMatch = pageText.match(categoryRegex);

        if (categoryMatch && categoryMatch[1]) {
            category = categoryMatch[1].trim();
        } else {
            // Look for elements containing "音樂人類別"
            $('*').each((i, el) => {
                const text = $(el).text();
                if (text && text.includes('音樂人類別')) {
                    const match = text.match(/音樂人類別[：:\s]*([^\n\r]+)/);
                    if (match && match[1]) {
                        category = match[1].trim();
                        return false; // break the loop
                    }
                    // Check next sibling
                    const nextSibling = $(el).next();
                    if (nextSibling.length) {
                        const siblingText = nextSibling.text().trim();
                        if (siblingText && siblingText.length < 50) {
                            category = siblingText;
                            return false; // break the loop
                        }
                    }
                }
            });
        }

        // Find bio by looking for "介紹" heading
        $('*').each((i, el) => {
            const text = $(el).text().trim();
            if (text === '介紹') {
                // Look in parent container
                let container = $(el).parent();
                while (container.length && !bio) {
                    const containerText = container.text();
                    if (containerText && containerText.length > 20) {
                        const introIndex = containerText.indexOf('介紹');
                        if (introIndex !== -1) {
                            const afterIntro = containerText.substring(introIndex + 2);
                            const lines = afterIntro.split('\n')
                                .map(l => l.trim())
                                .filter(l => l && !l.includes('關於') && !l.includes('音樂人類別') && l !== '介紹');
                            if (lines.length > 0) {
                                bio = lines.join('\n').trim();
                                break;
                            }
                        }
                    }
                    container = container.parent();
                }

                // If still no bio, check next siblings
                if (!bio) {
                    let nextEl = $(el).next();
                    const bioLines = [];
                    let maxSiblings = 10;

                    while (nextEl.length && maxSiblings > 0) {
                        const siblingText = nextEl.text().trim();
                        if (siblingText && !siblingText.includes('關於')) {
                            bioLines.push(siblingText);
                        }
                        nextEl = nextEl.next();
                        maxSiblings--;

                        // Stop if we encounter another heading
                        if (nextEl.length && nextEl.get(0).tagName && nextEl.get(0).tagName.match(/^H[1-6]$/i)) {
                            break;
                        }
                    }

                    if (bioLines.length) {
                        bio = bioLines.join('\n').trim();
                    }
                }

                return false; // break the loop
            }
        });

        return {
            category: category || '未找到類別',
            bio: bio || '未找到介紹'
        };
    } catch (error) {
        console.error(`  擷取音樂人資訊失敗 ${artistUrl}:`, error.message);
        return { category: '擷取失敗', bio: '擷取失敗' };
    }
}

// ====== Parallel Processing Engine ======

async function processRowBatch(rows, options, worksheet, newColumns, startIdx, totalRows) {
    const tasks = rows.map((row, batchIdx) => {
        const globalIdx = startIdx + batchIdx;
        return processOneRow(row, options, worksheet, newColumns, globalIdx, totalRows);
    });
    return Promise.all(tasks);
}

async function processOneRow(row, options, worksheet, newColumns, idx, totalRows) {
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
                const lyrics = await scrapeLyrics(row['作品名稱_hyperlink']);
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
                    row._artistInfo = await scrapeArtistInfo(row['作者_hyperlink']);
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

    try {
        const options = JSON.parse(req.body.options);
        const filePath = req.file.path;

        console.log('開始處理檔案:', req.file.originalname);
        console.log('處理選項:', options);
        console.log(`並行數: ${CONCURRENCY}, 請求間隔: ${REQUEST_DELAY}ms`);

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
            log: `找到 ${data.length} 筆資料，使用 ${CONCURRENCY} 個並行請求開始處理`
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

            await processRowBatch(batch, options, worksheet, newColumns, i, data.length);

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

        res.download(outputPath, `processed_${req.file.originalname}`, (err) => {
            if (err) console.error('下載失敗:', err);
            fs.unlink(filePath, () => {});
            setTimeout(() => { fs.unlink(outputPath, () => {}); }, 3600000);
        });

    } catch (error) {
        console.error('處理失敗:', error);
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
    console.log('使用 HTTP 請求抓取（無需 Chrome 瀏覽器）');
    console.log('請在瀏覽器中開啟上述網址使用工具');
});

// Graceful shutdown
process.on('SIGINT', async () => {
    console.log('\n正在關閉服務器...');
    process.exit(0);
});

process.on('SIGTERM', async () => {
    console.log('\n正在關閉服務器...');
    process.exit(0);
});
