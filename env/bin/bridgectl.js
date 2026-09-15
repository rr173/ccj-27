#!/usr/bin/env node
// Operational CLI over the mapper/deliverer/connector HTTP APIs.
//
// Examples:
//   bridgectl overview
//   bridgectl chain m_ab12...
//   bridgectl mappings --status MAP_FAILED
//   bridgectl freezes
//   bridgectl freeze fz_x resolve skip
//   bridgectl freeze fz_x resolve override
//   bridgectl retry m_ab12 --body '{"id":"o-1",...}'
//   bridgectl events [--type SOURCE_ACK_DUPLICATE]
import { Http } from '../src/client.js';

const base = process.env.MAPPER_URL || 'http://localhost:8301';
const delivererUrl = process.env.DELIVERER_URL || 'http://localhost:8401';
const mapper = new Http(base);
const deliverer = new Http(delivererUrl);

const [, , cmd, ...args] = process.argv;

function flag(name, fallback) {
  const i = args.indexOf(`--${name}`);
  if (i === -1) return fallback;
  const v = args[i + 1];
  return v && !v.startsWith('--') ? v : true;
}

async function main() {
  switch (cmd) {
    case 'overview': {
      const [m, d] = await Promise.all([mapper.get('/overview'), deliverer.get('/overview')]);
      print({ mapper: m, deliverer: d });
      break;
    }
    case 'mappings': {
      const q = new URLSearchParams();
      if (flag('status')) q.set('status', flag('status'));
      if (flag('side')) q.set('side', flag('side'));
      if (flag('egressStatus')) q.set('egressStatus', flag('egressStatus'));
      if (flag('limit')) q.set('limit', flag('limit'));
      print(await mapper.get(`/mappings?${q}`));
      break;
    }
    case 'chain': {
      print(await mapper.get(`/chain/${encodeURIComponent(args[0])}`));
      break;
    }
    case 'events': {
      const q = new URLSearchParams();
      if (flag('type')) q.set('type', flag('type'));
      if (flag('since')) q.set('since', flag('since'));
      print(await mapper.get(`/events?${q}`));
      break;
    }
    case 'freezes': {
      const q = new URLSearchParams();
      if (flag('status')) q.set('status', flag('status'));
      print(await mapper.get(`/freezes?${q}`));
      break;
    }
    case 'freeze': {
      // freeze <id> resolve <skip|override|keep_local>
      const [id, action, decision] = args;
      if (action !== 'resolve') usage();
      const res = await mapper.post(`/freeze/${encodeURIComponent(id)}/resolve`, { decision });
      print(res);
      // Release the connector-side view of the segment if asked.
      const side = res.freeze?.side;
      const seq = res.freeze?.ingressSeq;
      if (side && seq && (decision === 'skip' || decision === 'override')) {
        const connector = new Http(
          side === 'A'
            ? process.env.CONNECTOR_A_URL || 'http://localhost:8501'
            : process.env.CONNECTOR_B_URL || 'http://localhost:8601',
        );
        print(await connector.post('/ingress/release', {
          seq, action: decision === 'skip' ? 'skip' : 'override',
          mappingId: res.freeze?.mappingId,
        }));
      }
      break;
    }
    case 'retry': {
      // retry <mappingId> [--body '<json>']
      const id = args[0];
      let body;
      const rawBody = flag('body');
      if (rawBody && rawBody !== true) body = JSON.parse(rawBody);
      const remap = await mapper.post(`/mappings/${encodeURIComponent(id)}/retry`, {
        body, operator: process.env.USER || 'cli',
      });
      print(remap);
      if (remap.result === 'REMAPPED') {
        const enq = await deliverer.post('/deliver', {
          mappingId: remap.mappingId,
          originSide: remap.originSide,
          targetSide: remap.originSide === 'A' ? 'B' : 'A',
          direction: remap.direction,
          ingressSeq: remap.ingressSeq,
          headers: remap.mappedHeaders,
          body: remap.mappedBody,
        });
        print(enq);
      }
      break;
    }
    case 'deliveries': {
      const q = new URLSearchParams();
      if (flag('status')) q.set('status', flag('status'));
      print(await deliverer.get(`/deliveries?${q}`));
      break;
    }
    default:
      usage();
  }
}

function usage() {
  console.error(`usage:
  bridgectl overview
  bridgectl mappings [--status MAPPED|MAP_FAILED] [--side A|B] [--egressStatus QUEUED|PUBLISHED|FAILED]
  bridgectl chain <mappingId>
  bridgectl events [--type TYPE] [--since SEQ]
  bridgectl freezes [--status FROZEN]
  bridgectl freeze <freezeId> resolve skip|override|keep_local
  bridgectl retry <mappingId> [--body '<json>']
  bridgectl deliveries [--status QUEUED|PUBLISHED|DONE|FAILED]`);
  process.exit(1);
}

function print(v) {
  process.stdout.write(JSON.stringify(v, null, 2) + '\n');
}

main().catch((err) => {
  console.error('error:', err.message);
  process.exit(1);
});
