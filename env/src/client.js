// Small JSON HTTP client wrapper around global fetch.
export class Http {
  constructor(baseUrl) {
    this.baseUrl = baseUrl.replace(/\/$/, '');
  }

  async req(method, path, body, { timeoutMs = 10000, retries = 0, retryDelayMs = 200 } = {}) {
    const url = this.baseUrl + path;
    let attempt = 0;
    let lastErr;
    while (attempt <= retries) {
      const ctrl = new AbortController();
      const timer = setTimeout(() => ctrl.abort(), timeoutMs);
      try {
        const res = await fetch(url, {
          method,
          signal: ctrl.signal,
          headers: body === undefined ? undefined : { 'content-type': 'application/json' },
          body: body === undefined ? undefined : JSON.stringify(body),
        });
        const text = await res.text();
        const parsed = text ? safeJson(text) : null;
        if (!res.ok) {
          const err = new Error(`http ${res.status} on ${method} ${path}: ${text.slice(0, 300)}`);
          err.status = res.status;
          err.body = parsed;
          // Do not retry 4xx (except 408/429)
          if (res.status >= 400 && res.status < 500 && res.status !== 408 && res.status !== 429) throw err;
          lastErr = err;
        } else {
          return parsed;
        }
      } catch (err) {
        lastErr = err;
      } finally {
        clearTimeout(timer);
      }
      attempt += 1;
      if (attempt <= retries) {
        await new Promise((r) => setTimeout(r, retryDelayMs * attempt));
      }
    }
    throw lastErr;
  }

  get(path, opts) {
    return this.req('GET', path, undefined, opts);
  }
  post(path, body, opts) {
    return this.req('POST', path, body, opts);
  }
}

function safeJson(text) {
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}
