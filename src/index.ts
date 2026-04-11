import { Hono } from 'hono';
import { cors } from 'hono/cors';

type Env = {};

const REQUEST_TIMEOUT = 30000;

const FETCH_HEADERS: Record<string, string> = {
  'User-Agent':
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
  'Accept-Language': 'zh-TW,zh;q=0.9,en;q=0.8',
  DNT: '1',
  Connection: 'keep-alive',
  'Upgrade-Insecure-Requests': '1',
};

async function fetchPage(url: string): Promise<string> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);
  try {
    const res = await fetch(url, { headers: FETCH_HEADERS, signal: controller.signal });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.text();
  } finally {
    clearTimeout(timeout);
  }
}

const app = new Hono<{ Bindings: Env }>();

app.use('*', cors());

app.get('/health', (c) =>
  c.json({ status: 'ok', version: 'v4-thin-proxy', timestamp: new Date().toISOString() })
);

// Thin HTML proxy — only purpose is to bypass browser CORS for streetvoice.com.
// All HTML parsing is done in the browser (DOMParser).
app.get('/fetch', async (c) => {
  const url = c.req.query('url');
  if (!url) return c.text('Missing url', 400);
  if (!/^https?:\/\//i.test(url)) return c.text('Invalid url', 400);

  try {
    const html = await fetchPage(url);
    return new Response(html, {
      headers: {
        'Content-Type': 'text/plain; charset=utf-8',
        'Cache-Control': 'no-store',
      },
    });
  } catch (e: any) {
    return c.text('Fetch failed: ' + (e?.message || ''), 502);
  }
});

export default app;
