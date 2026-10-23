#!/bin/sh
set -eu

CONFIG_PATH="${AHP_CONFIG_FILE:-/tmp/ahp-acp-gateway.container.json}"
CLI_PATH="${AHP_CLI_PATH:-/app/dist/src/cli.js}"

# If the caller passes explicit CLI flags (e.g. --config, --help, --version, --print-config),
# forward them directly to the gateway CLI without generating a container config overlay.
if [ "$#" -gt 0 ]; then
  exec node "$CLI_PATH" "$@"
fi

node --input-type=module - "$CONFIG_PATH" <<'EOF'
import { readFileSync, writeFileSync } from 'node:fs';

const outPath = process.argv[2];
const defaultConfigPath = process.env.AHP_DEFAULT_CONFIG_PATH || '/app/ahp-acp-gateway.default.json';
const defaultConfig = JSON.parse(
  readFileSync(defaultConfigPath, 'utf8'),
);

const host = process.env.AHP_HOST || '0.0.0.0';
const port = Number.parseInt(process.env.PORT || process.env.AHP_PORT || '8765', 10);
const path = process.env.AHP_PATH || '/ahp';
const isLoopback = host === '127.0.0.1' || host === 'localhost' || host === '::1';
const tlsTerminated = process.env.AHP_TLS_TERMINATED !== undefined
  ? process.env.AHP_TLS_TERMINATED === 'true' || process.env.AHP_TLS_TERMINATED === '1'
  : !isLoopback;
const allowAnyOrigin = process.env.AHP_ALLOW_ANY_ORIGIN !== undefined
  ? process.env.AHP_ALLOW_ANY_ORIGIN === 'true' || process.env.AHP_ALLOW_ANY_ORIGIN === '1'
  : true;
const allowedOrigins = (process.env.AHP_ALLOWED_ORIGINS || '')
  .split(',')
  .map((item) => item.trim())
  .filter(Boolean);
const dbPath = process.env.AHP_DB_PATH || '/data/ahp-acp-gateway.db';
const defaultCwd = process.env.AHP_DEFAULT_CWD || '/workspace/demo';
const token = process.env.AHP_AUTH_TOKEN?.trim();

if (!isLoopback && !token) {
  console.error(
    'Error: AHP_AUTH_TOKEN environment variable is required when listening on a non-loopback address (' +
      host +
      ').',
  );
  process.exit(1);
}

const enabledEnv = process.env.AHP_ENABLED_PROVIDERS ?? 'opencode';
const enableAll = enabledEnv.trim() === '*' || enabledEnv.trim().toLowerCase() === 'all';
const enabledIds = new Set(
  enabledEnv
    .split(',')
    .map((id) => id.trim())
    .filter(Boolean),
);

const providers = [];
for (const provider of defaultConfig.providers || []) {
  const enabled = enableAll || enabledIds.has(provider.id);
  if (!enabled) {
    providers.push({ id: provider.id, enabled: false });
    continue;
  }
  const override = {
    id: provider.id,
    enabled: true,
    cwd: defaultCwd,
  };
  // Use the pre-installed global `opencode` binary in the container image for instant startup.
  if (provider.id === 'opencode') {
    override.command = 'opencode';
    override.args = ['acp'];
  }
  providers.push(override);
}

const overlay = {
  version: 1,
  listener: {
    host,
    port,
    path,
    tlsTerminated,
    allowedOrigins,
    allowAnyOrigin,
  },
  auth: token ? { token } : {},
  database: {
    path: dbPath,
  },
  providers,
};

writeFileSync(outPath, JSON.stringify(overlay, null, 2) + '\n', 'utf8');
EOF

if [ "${AHP_PRINT_GENERATED_CONFIG:-}" = "true" ] || [ "${AHP_PRINT_GENERATED_CONFIG:-}" = "1" ]; then
  exec node "$CLI_PATH" --config "$CONFIG_PATH" --print-config
fi

exec node "$CLI_PATH" --config "$CONFIG_PATH"
