#!/usr/bin/env node
// Operational CLI over the quota scheduler HTTP API (port 8701 by default).
//
// Examples:
//   quotactl overview
//   quotactl tenants
//   quotactl tenant t1                       # occupancy + waiting order + ETAs
//   quotactl configure-tenant t1 --revision 1 \
//     --in '{"ratePerSec":20,"burst":40,"maxInflight":10}' \
//     --out '{"ratePerSec":10,"burst":20,"maxInflight":5}' \
//     --weight 2 --wait 1000 --hold-ms 30000
//   quotactl configure-class gold --in '{"ratePerSec":100,"burst":100,"maxInflight":50}'
//   quotactl reserve t1 in --request r-1 [--class gold]
//   quotactl settle rsv_... --complete | --abort
//   quotactl request r-1                     # status / position / ETA
//   quotactl waiting [--tenant t1] [--class gold]
//   quotactl records t1 [--type GRANTED]
import { Http } from '../src/client.js';

const quota = new Http(process.env.QUOTA_URL || 'http://localhost:8701');
const [, , cmd, ...args] = process.argv;

function flag(name, fallback) {
  const i = args.indexOf(`--${name}`);
  if (i === -1) return fallback;
  const v = args[i + 1];
  return v && !v.startsWith('--') ? v : true;
}

function parseJsonFlag(name) {
  const v = flag(name);
  if (v === undefined || v === true) return undefined;
  return JSON.parse(v);
}

async function main() {
  switch (cmd) {
    case 'overview':
      print(await quota.get('/overview'));
      break;

    case 'tenants':
      print(await quota.get('/tenants'));
      break;

    case 'classes':
      print(await quota.get('/classes'));
      break;

    case 'tenant':
      print(await quota.get(`/tenants/${encodeURIComponent(args[0])}`));
      break;

    case 'class':
      print(await quota.get(`/classes/${encodeURIComponent(args[0])}`));
      break;

    case 'configure-tenant': {
      const tenantId = args[0];
      if (!tenantId) usage();
      const body = {
        tenantId,
        in: parseJsonFlag('in'),
        out: parseJsonFlag('out'),
      };
      if (flag('revision') !== undefined) body.expectedRevision = Number(flag('revision'));
      if (flag('weight') !== undefined) body.weight = Number(flag('weight'));
      if (flag('wait') !== undefined) body.waitCapacity = Number(flag('wait'));
      if (flag('hold-ms') !== undefined) body.holdTtlMs = Number(flag('hold-ms'));
      if (flag('wait-timeout-ms') !== undefined) body.waitTimeoutMs = Number(flag('wait-timeout-ms'));
      print(await quota.post('/admin/tenants', body));
      break;
    }

    case 'configure-class': {
      const classId = args[0];
      if (!classId) usage();
      const body = { classId, in: parseJsonFlag('in'), out: parseJsonFlag('out') };
      if (flag('revision') !== undefined) body.expectedRevision = Number(flag('revision'));
      print(await quota.post('/admin/classes', body));
      break;
    }

    case 'reserve': {
      const [tenantId, direction] = args;
      const body = { tenantId, direction, requestId: flag('request') };
      if (flag('class')) body.class = flag('class');
      if (flag('cost')) body.cost = Number(flag('cost'));
      if (flag('hold-ms')) body.holdTtlMs = Number(flag('hold-ms'));
      print(await quota.post('/reserve', body));
      break;
    }

    case 'settle': {
      const id = args[0];
      const outcome = flag('abort') ? 'abort' : 'complete';
      print(await quota.post(`/reservations/${encodeURIComponent(id)}/settle`, { outcome }));
      break;
    }

    case 'request':
      print(await quota.get(`/requests/${encodeURIComponent(args[0])}`));
      break;

    case 'reservation':
      print(await quota.get(`/reservations/${encodeURIComponent(args[0])}`));
      break;

    case 'waiting': {
      const q = new URLSearchParams();
      if (flag('tenant')) q.set('tenantId', flag('tenant'));
      if (flag('class')) q.set('cls', flag('class'));
      if (flag('direction')) q.set('direction', flag('direction'));
      print(await quota.get(`/waiting?${q}`));
      break;
    }

    case 'records': {
      const tenantId = args[0];
      const q = new URLSearchParams();
      if (flag('type')) q.set('type', flag('type'));
      if (flag('limit')) q.set('limit', flag('limit'));
      print(await quota.get(`/tenants/${encodeURIComponent(tenantId)}/records?${q}`));
      break;
    }

    case 'events': {
      const q = new URLSearchParams();
      if (flag('type')) q.set('type', flag('type'));
      if (flag('tenant')) q.set('tenantId', flag('tenant'));
      if (flag('limit')) q.set('limit', flag('limit'));
      print(await quota.get(`/events?${q}`));
      break;
    }

    default:
      usage();
  }
}

function usage() {
  console.error(`usage:
  quotactl overview
  quotactl tenants | classes
  quotactl tenant <tenantId> | class <classId>
  quotactl configure-tenant <tenantId> [--revision N] [--in JSON] [--out JSON]
             [--weight W] [--wait N] [--hold-ms MS] [--wait-timeout-ms MS]
  quotactl configure-class <classId> [--revision N] [--in JSON] [--out JSON]
  quotactl reserve <tenantId> <in|out> --request <id> [--class C] [--cost N] [--hold-ms MS]
  quotactl settle <reservationId> (--complete|--abort)
  quotactl request <requestId> | reservation <reservationId>
  quotactl waiting [--tenant T] [--class C] [--direction in|out]
  quotactl records <tenantId> [--type GRANTED|SETTLED|EXPIRY_RECLAIMED] [--limit N]
  quotactl events [--type T] [--tenant T] [--limit N]`);
  process.exit(1);
}

function print(v) {
  process.stdout.write(JSON.stringify(v, null, 2) + '\n');
}

main().catch((err) => {
  console.error('error:', err.message);
  process.exit(1);
});
