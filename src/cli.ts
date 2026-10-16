#!/usr/bin/env node

import { parseArgs } from 'node:util';
import { ensureConfigDirectories, loadConfig, redactConfig } from './config.js';
import { GatewayServer } from './gateway.js';
import { VERSION } from './version.js';

const { values } = parseArgs({
  options: {
    config: { type: 'string', short: 'c' },
    'print-config': { type: 'boolean', default: false },
    help: { type: 'boolean', short: 'h', default: false },
    version: { type: 'boolean', short: 'v', default: false },
  },
  strict: true,
});

if (values.help) {
  process.stdout.write(`AHP-ACP Gateway ${VERSION}

Usage:
  ahp-acp-gateway [options]

Options:
  -c, --config <path>  Load a versioned JSON config overlay
      --print-config   Print the resolved config with secrets redacted
  -h, --help           Show this help
  -v, --version        Print the package version
`);
  process.exit(0);
}

if (values.version) {
  process.stdout.write(`${VERSION}\n`);
  process.exit(0);
}

const config = await loadConfig(values.config);
if (values['print-config']) {
  process.stdout.write(`${JSON.stringify(redactConfig(config), null, 2)}\n`);
  process.exit(0);
}

await ensureConfigDirectories(config);
const server = new GatewayServer(config);
const address = await server.listen();
process.stdout.write(`AHP-ACP Gateway listening on ws://${address.host}:${address.port}${address.path}\n`);

let closing = false;
async function shutdown(signal: string): Promise<void> {
  if (closing) return;
  closing = true;
  process.stderr.write(`Received ${signal}; shutting down\n`);
  await server.close();
}

process.on('SIGINT', () => {
  void shutdown('SIGINT');
});
process.on('SIGTERM', () => {
  void shutdown('SIGTERM');
});
