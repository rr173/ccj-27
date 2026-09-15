// Tiny zero-dependency HTTP JSON server with a small route table.
import http from 'node:http';
import { logger } from './log.js';

const log = logger('http');

// routes: [{ method, pattern: '/path/:id', handler: (req, res, params, body) => any }]
export function createServer(routes, bindPort) {
  const compiled = routes.map((r) => ({
    method: r.method,
    segments: r.pattern.split('/').filter(Boolean).map((s) =>
      s.startsWith(':') ? { param: s.slice(1) } : { literal: s },
    ),
    handler: r.handler,
  }));

  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url, 'http://localhost');
    const parts = url.pathname.split('/').filter(Boolean);
    const route = compiled.find(
      (r) => r.method === req.method && r.segments.length === parts.length &&
        r.segments.every((seg, i) => (seg.param ? true : seg.literal === parts[i])),
    );

    const send = (status, payload) => {
      const body = JSON.stringify(payload);
      res.writeHead(status, { 'content-type': 'application/json' });
      res.end(body);
    };

    if (!route) return send(404, { error: 'not_found', path: url.pathname });

    let body = null;
    if (req.method !== 'GET') {
      const raw = await readAll(req);
      if (raw.length) {
        try {
          body = JSON.parse(raw.toString('utf8'));
        } catch (err) {
          return send(400, { error: 'invalid_json', detail: err.message });
        }
      }
    }

    const params = {};
    route.segments.forEach((seg, i) => {
      if (seg.param) params[seg.param] = decodeURIComponent(parts[i]);
    });
    req.query = Object.fromEntries(url.searchParams);

    try {
      const result = await route.handler(req, res, params, body);
      if (!res.writableEnded && result !== undefined) send(200, result);
      else if (!res.writableEnded) send(200, { ok: true });
    } catch (err) {
      if (err instanceof HttpError) {
        send(err.status, { error: err.code, detail: err.message });
      } else {
        log.error('handler_failed', { path: url.pathname, err: err.stack || String(err) });
        if (!res.writableEnded) send(500, { error: 'internal', detail: err.message });
      }
    }
  });

  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(bindPort, '0.0.0.0', () => {
      log.info('listening', { port: server.address().port });
      resolve(server);
    });
  });
}

export class HttpError extends Error {
  constructor(status, code, detail) {
    super(detail || code);
    this.status = status;
    this.code = code;
  }
}

function readAll(req, limit = 8 * 1024 * 1024) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', (c) => {
      size += c.length;
      if (size > limit) {
        reject(new HttpError(413, 'payload_too_large'));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}
