// Run the whole reference deployment in one Node process (dev/test convenience).
// In production every component is its own container — see docker-compose.yml.
import { startBrokerA } from '../src/systems/broker-a.js';
import { startBrokerB } from '../src/systems/broker-b.js';
import { startMapper } from '../src/mapper/index.js';
import { startDeliverer } from '../src/deliverer/index.js';
import { startConnector } from '../src/connector/index.js';
import { logger } from '../src/log.js';

const log = logger('stack');

export const DEFAULT_PORTS = {
  brokerA: Number(process.env.PORT_BROKER_A || 8101),
  brokerB: Number(process.env.PORT_BROKER_B || 8201),
  mapper: Number(process.env.PORT_MAPPER || 8301),
  deliverer: Number(process.env.PORT_DELIVERER || 8401),
  connectorA: Number(process.env.PORT_CONNECTOR_A || 8501),
  connectorB: Number(process.env.PORT_CONNECTOR_B || 8601),
};

const DATA_ROOT = process.env.DATA_ROOT || './data';

export async function startAll(dataRoot = DATA_ROOT, ports = DEFAULT_PORTS) {
  await startBrokerA(ports.brokerA);
  await startBrokerB(ports.brokerB);
  await startMapper(ports.mapper, `${dataRoot}/mapper`);
  const deliverer = await startDeliverer(ports.deliverer, `${dataRoot}/deliverer`, {
    connectorA: `http://localhost:${ports.connectorA}`,
    connectorB: `http://localhost:${ports.connectorB}`,
    mapper: `http://localhost:${ports.mapper}`,
  });
  const connA = await startConnector('A', ports.connectorA, `${dataRoot}/connector-a`, {
    broker: `http://localhost:${ports.brokerA}`,
    mapper: `http://localhost:${ports.mapper}`,
    deliverer: `http://localhost:${ports.deliverer}`,
  });
  let connB = await startConnector('B', ports.connectorB, `${dataRoot}/connector-b`, {
    broker: `http://localhost:${ports.brokerB}`,
    mapper: `http://localhost:${ports.mapper}`,
    deliverer: `http://localhost:${ports.deliverer}`,
  });

  log.info('stack_ready', { ports });

  // Selective restart of one connector (the other components keep running),
  // replaying state from the same durable data directory.
  return {
    ports,
    deliverer,
    connA,
    async stopB() {
      await connB.stop();
      connB = null;
      log.info('connector_b_stopped');
    },
    async startB() {
      if (connB) return connB;
      connB = await startConnector('B', ports.connectorB, `${dataRoot}/connector-b`, {
        broker: `http://localhost:${ports.brokerB}`,
        mapper: `http://localhost:${ports.mapper}`,
        deliverer: `http://localhost:${ports.deliverer}`,
      });
      log.info('connector_b_started');
      return connB;
    },
  };
}

const isMain = process.argv[1] && process.argv[1].endsWith('start-all.js');
if (isMain) {
  startAll().catch((err) => {
    log.error('stack_failed', { err: err.stack || String(err) });
    process.exit(1);
  });
}
