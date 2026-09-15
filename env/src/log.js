// Minimal structured stdout logger.
import { nowIso, pid } from './util.js';

const LEVELS = { debug: 10, info: 20, warn: 30, error: 40, silent: 99 };
const threshold = LEVELS[(process.env.LOG_LEVEL || 'info').toLowerCase()] ?? 20;

function emit(level, component, msg, fields) {
  if (LEVELS[level] < threshold) return;
  const line = {
    ts: nowIso(),
    level,
    component,
    pid: pid(),
    msg,
    ...(fields && Object.keys(fields).length ? { fields } : {}),
  };
  process.stdout.write(JSON.stringify(line) + '\n');
}

export function logger(component) {
  return {
    debug: (msg, fields) => emit('debug', component, msg, fields),
    info: (msg, fields) => emit('info', component, msg, fields),
    warn: (msg, fields) => emit('warn', component, msg, fields),
    error: (msg, fields) => emit('error', component, msg, fields),
  };
}
