import { createServer } from 'node:http';
import { mkdir, readFile, rename, rm, writeFile } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { join } from 'node:path';
import vm from 'node:vm';
import { constants, createCipheriv, createHash, publicEncrypt, randomBytes, randomUUID } from 'node:crypto';
import { deflate, inflate } from 'node:zlib';

const PORT = Number(process.env.PORT || 8000);
const HOST = process.env.HOST || '0.0.0.0';
const DATA_DIR = process.env.LX_SOURCE_DATA_DIR || './data';
const SOURCE_DIR = join(DATA_DIR, 'sources');
const INDEX_PATH = join(DATA_DIR, 'sources.json');
const MAX_SCRIPT_BYTES = 1024 * 1024;
const MAX_RESPONSE_BYTES = 8 * 1024 * 1024;
const ALLOWED_SOURCE_KEYS = new Set(['kw', 'kg', 'tx', 'wy', 'mg', 'local']);

await mkdir(SOURCE_DIR, { recursive: true });

const sendJson = (res, status, value) => {
  const body = Buffer.from(JSON.stringify(value));
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8', 'content-length': body.length });
  res.end(body);
};

const readBody = async (req) => {
  const chunks = [];
  let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > MAX_SCRIPT_BYTES * 2) throw new Error('request body too large');
    chunks.push(chunk);
  }
  if (!chunks.length) return {};
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
};

const loadIndex = async () => {
  try {
    const parsed = JSON.parse(await readFile(INDEX_PATH, 'utf8'));
    return Array.isArray(parsed.sources) ? parsed : { version: 1, sources: [] };
  } catch {
    return { version: 1, sources: [] };
  }
};

const saveIndex = async (index) => {
  const tmp = `${INDEX_PATH}.tmp`;
  await writeFile(tmp, `${JSON.stringify(index, null, 2)}\n`, { mode: 0o600 });
  await rename(tmp, INDEX_PATH);
};

const safePublicSource = (source) => ({
  id: source.id,
  name: source.name,
  description: source.description || '',
  version: source.version || '',
  author: source.author || '',
  homepage: source.homepage || '',
  importUrl: source.importUrl || '',
  enabled: source.enabled !== false,
  priority: Number(source.priority || 100),
  capabilities: source.capabilities || {},
  createdAt: source.createdAt,
});

const parseHeader = (script) => {
  const field = (name) => script.match(new RegExp(`@${name}\\s+([^\\r\\n*]+)`))?.[1]?.trim() || '';
  const name = field('name');
  if (!name) throw new Error('missing required @name header');
  return { name: name.slice(0, 80), description: field('description').slice(0, 200), version: field('version').slice(0, 40), author: field('author').slice(0, 80), homepage: field('homepage').slice(0, 500) };
};

const fetchLimited = async (url, options = {}) => {
  if (!/^https?:\/\//i.test(url)) throw new Error('only http/https requests are allowed');
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), Math.min(Number(options.timeout || 15000), 30000));
  try {
    let body = options.body;
    const headers = { ...(options.headers || {}) };
    if (options.form && typeof options.form === 'object') {
      body = new URLSearchParams(options.form).toString();
      if (!Object.keys(headers).some((key) => key.toLowerCase() === 'content-type')) headers['content-type'] = 'application/x-www-form-urlencoded';
    }
    const response = await fetch(url, {
      method: options.method || 'GET',
      headers,
      body,
      redirect: 'follow',
      signal: controller.signal,
    });
    const buffer = Buffer.from(await response.arrayBuffer());
    if (buffer.length > MAX_RESPONSE_BYTES) throw new Error('source response too large');
    const contentType = response.headers.get('content-type') || '';
    let parsedBody = options.responseType === 'arraybuffer' ? buffer : buffer.toString('utf8');
    if (typeof parsedBody === 'string' && /(^|\b)(application\/json|text\/json)(\b|;)/i.test(contentType)) {
      try { parsedBody = JSON.parse(parsedBody); } catch { /* preserve text */ }
    }
    return { statusCode: response.status, headers: Object.fromEntries(response.headers), body: parsedBody };
  } finally {
    clearTimeout(timer);
  }
};

const initializeScript = async (script, filename) => {
  let requestHandler;
  let initPayload;
  const EVENT_NAMES = { request: 'request', inited: 'inited' };
  const scriptMeta = parseHeader(script);
  const lxRequest = (url, options, callback) => {
    let cancelled = false;
    fetchLimited(String(url), options || {}).then((resp) => {
      if (!cancelled) callback(null, resp, resp.body);
    }).catch((error) => { if (!cancelled) callback(error); });
    return () => { cancelled = true; };
  };
  const zlibPromise = (fn, value) => new Promise((resolve, reject) => fn(value, (error, data) => error ? reject(error) : resolve(data)));
  const lx = Object.freeze({
    version: '2',
    env: 'desktop',
    EVENT_NAMES,
    currentScriptInfo: Object.freeze({ ...scriptMeta, rawScript: script }),
    request: lxRequest,
    on: (event, handler) => { if (event === EVENT_NAMES.request && typeof handler === 'function') requestHandler = handler; return Promise.resolve(); },
    send: (event, payload) => { if (event === EVENT_NAMES.inited) initPayload = payload; return Promise.resolve(); },
    utils: Object.freeze({
      crypto: Object.freeze({
        aesEncrypt(buffer, mode, key, iv) {
          const cipher = createCipheriv(mode, key, iv);
          return Buffer.concat([cipher.update(buffer), cipher.final()]);
        },
        rsaEncrypt(buffer, key) {
          const input = Buffer.from(buffer);
          return publicEncrypt({ key, padding: constants.RSA_NO_PADDING }, Buffer.concat([Buffer.alloc(128 - input.length), input]));
        },
        randomBytes,
        md5: (value) => createHash('md5').update(value).digest('hex'),
      }),
      buffer: Object.freeze({
        from: (...args) => Buffer.from(...args),
        bufToString: (buf, format) => Buffer.from(buf, 'binary').toString(format),
      }),
      zlib: Object.freeze({
        inflate: (value) => zlibPromise(inflate, value),
        deflate: (value) => zlibPromise(deflate, value),
      }),
    }),
  });
  const context = vm.createContext({ globalThis: {}, console: Object.freeze({ log() {}, warn() {}, error() {} }), setTimeout, clearTimeout, URL, TextEncoder, TextDecoder });
  context.globalThis = context;
  context.globalThis.lx = lx;
  const compiled = new vm.Script(`'use strict';\n${script}`, { filename });
  compiled.runInContext(context, { timeout: 1500, breakOnSigint: true });
  await new Promise((resolve) => setTimeout(resolve, 0));
  if (!requestHandler || !initPayload?.sources) throw new Error('script did not register LX request/inited events');
  const capabilities = {};
  for (const [key, value] of Object.entries(initPayload.sources)) {
    if (!ALLOWED_SOURCE_KEYS.has(key) || !value || typeof value !== 'object') continue;
    capabilities[key] = { name: String(value.name || key), actions: Array.isArray(value.actions) ? Array.from(value.actions, String) : [], qualitys: Array.isArray(value.qualitys) ? Array.from(value.qualitys, String) : [] };
  }
  return { requestHandler, capabilities };
};

const importScript = async ({ script, url }) => {
  let raw = typeof script === 'string' ? script : '';
  let importUrl = '';
  if (!raw && url) {
    importUrl = String(url);
    const response = await fetchLimited(importUrl, { timeout: 20000 });
    if (response.statusCode < 200 || response.statusCode >= 300) throw new Error(`download failed: HTTP ${response.statusCode}`);
    raw = response.body;
  }
  if (!raw || Buffer.byteLength(raw) > MAX_SCRIPT_BYTES) throw new Error('script is empty or exceeds 1 MiB');
  const meta = parseHeader(raw);
  const id = randomUUID();
  const initialized = await initializeScript(raw, `${id}.js`);
  const index = await loadIndex();
  if (index.sources.length >= 20) throw new Error('at most 20 LX sources can be installed');
  const filename = `${id}.js`;
  await writeFile(join(SOURCE_DIR, filename), raw, { mode: 0o600 });
  const record = { id, ...meta, importUrl, filename, enabled: true, priority: 100, capabilities: initialized.capabilities, createdAt: new Date().toISOString() };
  index.sources.push(record);
  await saveIndex(index);
  return safePublicSource(record);
};

const resolveAction = async (body) => {
  const source = String(body.source || '');
  const action = String(body.action || 'musicUrl');
  if (!ALLOWED_SOURCE_KEYS.has(source)) throw new Error('unsupported LX source key');
  const index = await loadIndex();
  const sources = index.sources.filter((item) => item.enabled !== false).sort((a, b) => Number(a.priority || 100) - Number(b.priority || 100));
  const errors = [];
  for (const item of sources) {
    const capability = item.capabilities?.[source];
    if (!capability?.actions?.includes(action)) continue;
    try {
      const script = await readFile(join(SOURCE_DIR, item.filename), 'utf8');
      const runtime = await initializeScript(script, item.filename);
      const qualities = Array.isArray(capability.qualitys) ? capability.qualitys : [];
      const requestedQuality = String(body.quality || '320k');
      const quality = qualities.includes(requestedQuality) ? requestedQuality : ['flac24bit', 'flac', '320k', '128k'].find(value => qualities.includes(value)) || requestedQuality;
      const info = action === 'musicUrl' ? { type: quality, musicInfo: body.musicInfo || {} } : { musicInfo: body.musicInfo || {} };
      const value = await Promise.race([
        Promise.resolve(runtime.requestHandler({ source, action, info })),
        new Promise((_, reject) => setTimeout(() => reject(new Error('source action timed out')), 20000)),
      ]);
      if (value) return { sourceId: item.id, value };
    } catch (error) {
      errors.push({ sourceId: item.id, message: String(error?.message || error) });
    }
  }
  const error = new Error('no enabled LX source returned a result');
  error.details = errors;
  throw error;
};

const server = createServer(async (req, res) => {
  try {
    const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    if (req.method === 'GET' && url.pathname === '/healthz') return sendJson(res, 200, { ok: true });
    if (req.method === 'GET' && url.pathname === '/api/v1/sources') {
      const index = await loadIndex();
      return sendJson(res, 200, { ok: true, sources: index.sources.map(safePublicSource) });
    }
    if (req.method === 'POST' && url.pathname === '/api/v1/sources') return sendJson(res, 201, { ok: true, source: await importScript(await readBody(req)) });
    const match = url.pathname.match(/^\/api\/v1\/sources\/([^/]+)$/);
    if (match && req.method === 'PATCH') {
      const body = await readBody(req);
      const index = await loadIndex();
      const source = index.sources.find((item) => item.id === match[1]);
      if (!source) return sendJson(res, 404, { ok: false, error: 'source not found' });
      if (typeof body.enabled === 'boolean') source.enabled = body.enabled;
      if (Number.isFinite(Number(body.priority))) source.priority = Math.max(0, Math.min(999, Number(body.priority)));
      await saveIndex(index);
      return sendJson(res, 200, { ok: true, source: safePublicSource(source) });
    }
    if (match && req.method === 'DELETE') {
      const index = await loadIndex();
      const position = index.sources.findIndex((item) => item.id === match[1]);
      if (position < 0) return sendJson(res, 404, { ok: false, error: 'source not found' });
      const [source] = index.sources.splice(position, 1);
      await saveIndex(index);
      await rm(join(SOURCE_DIR, source.filename), { force: true });
      return sendJson(res, 200, { ok: true });
    }
    if (req.method === 'POST' && url.pathname === '/api/v1/resolve') return sendJson(res, 200, { ok: true, ...(await resolveAction(await readBody(req))) });
    return sendJson(res, 404, { ok: false, error: 'not found' });
  } catch (error) {
    return sendJson(res, 400, { ok: false, error: String(error?.message || error), details: error?.details || [] });
  }
});

if (process.argv[1]?.endsWith('/server.js')) server.listen(PORT, HOST);

export { initializeScript, parseHeader };
