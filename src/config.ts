import { readFileSync } from 'node:fs';
import { readFile, mkdir } from 'node:fs/promises';
import { homedir } from 'node:os';
import { dirname, resolve } from 'node:path';
import { isIP } from 'node:net';
import { fileURLToPath } from 'node:url';

export interface ProviderConfig {
  id: string;
  displayName: string;
  description: string;
  command: string;
  args: string[];
  cwd?: string;
  env?: Record<string, string>;
  startupTimeoutMs: number;
  requestTimeoutMs: number;
  shutdownTimeoutMs: number;
  stderrTailBytes: number;
  builtIn?: boolean;
  installHint?: string;
  authHint?: string;
}

export interface GatewayConfig {
  version: 1;
  listener: {
    host: string;
    port: number;
    path: string;
    tlsTerminated: boolean;
    allowedOrigins: string[];
    allowAnyOrigin: boolean;
  };
  auth: {
    token?: string;
  };
  database: {
    path: string;
  };
  replay: {
    maxActions: number;
  };
  providerDefaults: ProviderDefaults;
  providers: ProviderConfig[];
}

export interface ProviderDefaults {
  startupTimeoutMs: number;
  requestTimeoutMs: number;
  shutdownTimeoutMs: number;
  stderrTailBytes: number;
}

export interface DefaultConfigOptions {
  homeDir?: string;
}

const DEFAULT_CONFIG_FILENAME = 'ahp-acp-gateway.default.json';
const bundledDefaultConfig = readBundledDefaultConfig();

export function defaultConfig(options: DefaultConfigOptions = {}): GatewayConfig {
  return parseBundledDefaultConfig(
    bundledDefaultConfig,
    options.homeDir ?? homedir(),
  );
}

function readBundledDefaultConfig(): unknown {
  const candidates = [
    new URL(`../${DEFAULT_CONFIG_FILENAME}`, import.meta.url),
    new URL(`../../${DEFAULT_CONFIG_FILENAME}`, import.meta.url),
  ];
  for (const candidate of candidates) {
    try {
      return JSON.parse(readFileSync(fileURLToPath(candidate), 'utf8')) as unknown;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
    }
  }
  throw new Error(`Unable to locate bundled ${DEFAULT_CONFIG_FILENAME}`);
}

function parseBundledDefaultConfig(raw: unknown, home: string): GatewayConfig {
  const source = DEFAULT_CONFIG_FILENAME;
  if (!isRecord(raw) || raw.version !== 1) {
    throw new Error(`${source}: version must be 1`);
  }
  const listener = requireRecord(raw.listener, `${source}.listener`);
  const auth = requireRecord(raw.auth, `${source}.auth`);
  const database = requireRecord(raw.database, `${source}.database`);
  const replay = requireRecord(raw.replay, `${source}.replay`);
  const rawProviderDefaults = requireRecord(
    raw.providerDefaults,
    `${source}.providerDefaults`,
  );
  const providerDefaults: ProviderDefaults = {
    startupTimeoutMs: requireInteger(
      rawProviderDefaults.startupTimeoutMs,
      `${source}.providerDefaults.startupTimeoutMs`,
      1,
    ),
    requestTimeoutMs: requireInteger(
      rawProviderDefaults.requestTimeoutMs,
      `${source}.providerDefaults.requestTimeoutMs`,
      1,
    ),
    shutdownTimeoutMs: requireInteger(
      rawProviderDefaults.shutdownTimeoutMs,
      `${source}.providerDefaults.shutdownTimeoutMs`,
      1,
    ),
    stderrTailBytes: requireInteger(
      rawProviderDefaults.stderrTailBytes,
      `${source}.providerDefaults.stderrTailBytes`,
      1,
    ),
  };
  const config: GatewayConfig = {
    version: 1,
    listener: {
      host: requireString(listener.host, `${source}.listener.host`),
      port: requireInteger(listener.port, `${source}.listener.port`, 1, 65_535),
      path: normalizePath(requireString(listener.path, `${source}.listener.path`)),
      tlsTerminated: requireBoolean(
        listener.tlsTerminated,
        `${source}.listener.tlsTerminated`,
      ),
      allowedOrigins: requireOrigins(
        listener.allowedOrigins,
        `${source}.listener.allowedOrigins`,
      ),
      allowAnyOrigin: requireBoolean(
        listener.allowAnyOrigin,
        `${source}.listener.allowAnyOrigin`,
      ),
    },
    auth: {
      ...(auth.token === undefined
        ? {}
        : { token: requireString(auth.token, `${source}.auth.token`) }),
    },
    database: {
      path: expandHomePath(
        requireString(database.path, `${source}.database.path`),
        home,
      ),
    },
    replay: {
      maxActions: requireInteger(
        replay.maxActions,
        `${source}.replay.maxActions`,
        1,
        1_000_000,
      ),
    },
    providerDefaults,
    providers: resolveProviders(
      raw.providers,
      [],
      providerDefaults,
      source,
    ).map((provider) => ({ ...provider, builtIn: true })),
  };
  validateResolvedConfig(config, source);
  return config;
}

export async function loadConfig(path?: string): Promise<GatewayConfig> {
  if (!path) return defaultConfig();
  const absolutePath = resolve(path);
  const raw = JSON.parse(await readFile(absolutePath, 'utf8')) as unknown;
  return validateConfig(raw, absolutePath);
}

function resolveProviderDefaults(
  raw: unknown,
  defaults: ProviderDefaults,
  source: string,
): ProviderDefaults {
  const value = raw === undefined
    ? {}
    : requireRecord(raw, `${source}.providerDefaults`);
  return {
    startupTimeoutMs: optionalInteger(
      value.startupTimeoutMs,
      defaults.startupTimeoutMs,
      1,
    ),
    requestTimeoutMs: optionalInteger(
      value.requestTimeoutMs,
      defaults.requestTimeoutMs,
      1,
    ),
    shutdownTimeoutMs: optionalInteger(
      value.shutdownTimeoutMs,
      defaults.shutdownTimeoutMs,
      1,
    ),
    stderrTailBytes: optionalInteger(
      value.stderrTailBytes,
      defaults.stderrTailBytes,
      1,
    ),
  };
}

export async function ensureConfigDirectories(config: GatewayConfig): Promise<void> {
  await mkdir(dirname(resolve(config.database.path)), { recursive: true });
}

export function validateConfig(raw: unknown, source = '<config>'): GatewayConfig {
  if (!isRecord(raw) || raw.version !== 1) {
    throw new Error(`${source}: version must be 1`);
  }

  const defaults = defaultConfig();
  const listener = raw.listener === undefined
    ? {}
    : requireRecord(raw.listener, `${source}.listener`);
  const auth = raw.auth === undefined ? {} : requireRecord(raw.auth, `${source}.auth`);
  const database = raw.database === undefined
    ? {}
    : requireRecord(raw.database, `${source}.database`);
  const replay = raw.replay === undefined ? {} : requireRecord(raw.replay, `${source}.replay`);
  const providerDefaults = resolveProviderDefaults(
    raw.providerDefaults,
    defaults.providerDefaults,
    source,
  );

  const config: GatewayConfig = {
    version: 1,
    listener: {
      host: optionalString(listener.host, defaults.listener.host, `${source}.listener.host`),
      port: optionalInteger(listener.port, defaults.listener.port, 1, 65_535),
      path: normalizePath(optionalString(listener.path, defaults.listener.path, `${source}.listener.path`)),
      tlsTerminated: optionalBoolean(listener.tlsTerminated, defaults.listener.tlsTerminated),
      allowedOrigins: optionalOrigins(listener.allowedOrigins, defaults.listener.allowedOrigins, `${source}.listener.allowedOrigins`),
      allowAnyOrigin: optionalBoolean(listener.allowAnyOrigin, defaults.listener.allowAnyOrigin),
    },
    auth: {
      ...(auth.token === undefined ? {} : { token: requireString(auth.token, `${source}.auth.token`) }),
    },
    database: {
      path: expandHomePath(
        optionalString(database.path, defaults.database.path, `${source}.database.path`),
        homedir(),
      ),
    },
    replay: {
      maxActions: optionalInteger(replay.maxActions, defaults.replay.maxActions, 1, 1_000_000),
    },
    providerDefaults,
    providers: resolveProviders(raw.providers, defaults.providers, providerDefaults, source),
  };

  validateResolvedConfig(config, source);
  return config;
}

function validateResolvedConfig(config: GatewayConfig, source: string): void {
  const loopback = isLoopback(config.listener.host);
  if (!loopback && !config.auth.token) {
    throw new Error(`${source}: non-loopback listeners require auth.token`);
  }
  if (!loopback && !config.listener.tlsTerminated) {
    throw new Error(`${source}: non-loopback listeners require listener.tlsTerminated=true`);
  }
  if (config.providers.length === 0) {
    throw new Error(`${source}: at least one provider must be enabled`);
  }
}

export function redactConfig(config: GatewayConfig): GatewayConfig {
  return {
    ...config,
    auth: config.auth.token ? { token: '<redacted>' } : {},
    providers: config.providers.map((provider) => ({
      ...provider,
      ...(provider.env
        ? { env: Object.fromEntries(Object.keys(provider.env).map((key) => [key, '<redacted>'])) }
        : {}),
    })),
  };
}

function resolveProviders(
  raw: unknown,
  defaults: ProviderConfig[],
  providerDefaults: ProviderDefaults,
  source: string,
): ProviderConfig[] {
  if (raw === undefined) return defaults;
  if (!Array.isArray(raw)) throw new Error(`${source}.providers must be an array`);

  const entries = raw.map((entry, index) => ({
    value: requireRecord(entry, `${source}.providers[${index}]`),
    path: `${source}.providers[${index}]`,
  }));
  const ids = entries.map(({ value, path }) => requireString(value.id, `${path}.id`));
  if (new Set(ids).size !== ids.length) {
    throw new Error(`${source}: provider ids must be unique`);
  }

  const merged = new Map(defaults.map((provider) => [provider.id, provider]));
  for (const { value, path } of entries) {
    const id = requireString(value.id, `${path}.id`);
    const existing = merged.get(id);
    const enabled = optionalBoolean(value.enabled, true);
    if (!enabled) {
      merged.delete(id);
      continue;
    }
    merged.set(id, resolveProvider(value, path, providerDefaults, existing));
  }
  return [...merged.values()];
}

function resolveProvider(
  provider: Record<string, unknown>,
  path: string,
  providerDefaults: ProviderDefaults,
  existing?: ProviderConfig,
): ProviderConfig {
  const args = provider.args === undefined ? existing?.args ?? [] : provider.args;
  if (!Array.isArray(args) || !args.every((arg) => typeof arg === 'string')) {
    throw new Error(`${path}.args must be an array of strings`);
  }
  const envOverride = provider.env === undefined ? undefined : requireRecord(provider.env, `${path}.env`);
  if (envOverride && !Object.values(envOverride).every((value) => typeof value === 'string')) {
    throw new Error(`${path}.env values must be strings`);
  }
  const env = existing?.env || envOverride
    ? { ...existing?.env, ...(envOverride as Record<string, string> | undefined) }
    : undefined;
  const id = requireString(provider.id, `${path}.id`);
  const command = provider.command === undefined
    ? existing?.command
    : requireString(provider.command, `${path}.command`);
  const displayName = provider.displayName === undefined
    ? existing?.displayName
    : requireString(provider.displayName, `${path}.displayName`);
  if (!command) throw new Error(`${path}.command is required for custom provider ${id}`);
  if (!displayName) throw new Error(`${path}.displayName is required for custom provider ${id}`);

  return {
    id,
    displayName,
    description: provider.description === undefined
      ? existing?.description ?? ''
      : requireStringValue(provider.description, `${path}.description`),
    command,
    args: [...args],
    ...(provider.cwd === undefined
      ? existing?.cwd === undefined ? {} : { cwd: existing.cwd }
      : { cwd: resolve(requireString(provider.cwd, `${path}.cwd`)) }),
    ...(env ? { env } : {}),
    startupTimeoutMs: optionalInteger(
      provider.startupTimeoutMs,
      existing?.startupTimeoutMs ?? providerDefaults.startupTimeoutMs,
      1,
    ),
    requestTimeoutMs: optionalInteger(
      provider.requestTimeoutMs,
      existing?.requestTimeoutMs ?? providerDefaults.requestTimeoutMs,
      1,
    ),
    shutdownTimeoutMs: optionalInteger(
      provider.shutdownTimeoutMs,
      existing?.shutdownTimeoutMs ?? providerDefaults.shutdownTimeoutMs,
      1,
    ),
    stderrTailBytes: optionalInteger(
      provider.stderrTailBytes,
      existing?.stderrTailBytes ?? providerDefaults.stderrTailBytes,
      1,
    ),
    ...(existing?.builtIn === undefined ? {} : { builtIn: existing.builtIn }),
    ...(provider.installHint === undefined
      ? existing?.installHint === undefined ? {} : { installHint: existing.installHint }
      : { installHint: requireString(provider.installHint, `${path}.installHint`) }),
    ...(provider.authHint === undefined
      ? existing?.authHint === undefined ? {} : { authHint: existing.authHint }
      : { authHint: requireString(provider.authHint, `${path}.authHint`) }),
  };
}

function isLoopback(host: string): boolean {
  if (host === 'localhost' || host === '::1') return true;
  if (isIP(host) === 4) return host.startsWith('127.');
  return false;
}

function normalizePath(path: string): string {
  return path.startsWith('/') ? path : `/${path}`;
}

function expandHomePath(path: string, home: string): string {
  if (path === '~') return home;
  if (path.startsWith('~/') || path.startsWith('~\\')) {
    return resolve(home, path.slice(2));
  }
  return path;
}

function requireRecord(value: unknown, path: string): Record<string, unknown> {
  if (!isRecord(value)) throw new Error(`${path} must be an object`);
  return value;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function requireString(value: unknown, path: string): string {
  if (typeof value !== 'string' || value.length === 0) {
    throw new Error(`${path} must be a non-empty string`);
  }
  return value;
}

function requireStringValue(value: unknown, path: string): string {
  if (typeof value !== 'string') throw new Error(`${path} must be a string`);
  return value;
}

function requireBoolean(value: unknown, path: string): boolean {
  if (typeof value !== 'boolean') throw new Error(`${path} must be a boolean`);
  return value;
}

function requireInteger(value: unknown, path: string, min: number, max = Number.MAX_SAFE_INTEGER): number {
  if (!Number.isInteger(value) || (value as number) < min || (value as number) > max) {
    throw new Error(`${path} must be an integer between ${min} and ${max}`);
  }
  return value as number;
}

function optionalString(value: unknown, fallback: string, path: string): string {
  return value === undefined ? fallback : requireString(value, path);
}

function optionalBoolean(value: unknown, fallback: boolean): boolean {
  if (value === undefined) return fallback;
  if (typeof value !== 'boolean') throw new Error('value must be a boolean');
  return value;
}

function optionalOrigins(value: unknown, fallback: string[], path: string): string[] {
  if (value === undefined) return [...fallback];
  return requireOrigins(value, path);
}

function requireOrigins(value: unknown, path: string): string[] {
  if (!Array.isArray(value) || !value.every((item) => typeof item === 'string')) {
    throw new Error(`${path} must be an array of origins`);
  }
  return value.map((item, index) => {
    let url: URL;
    try {
      url = new URL(item);
    } catch {
      throw new Error(`${path}[${index}] must be a valid origin`);
    }
    if (
      !url.protocol
      || !url.host
      || url.username
      || url.password
      || url.search
      || url.hash
      || (url.pathname !== '' && url.pathname !== '/')
    ) {
      throw new Error(`${path}[${index}] must be an exact origin without credentials, path, query, or fragment`);
    }
    const normalized = url.origin === 'null' ? `${url.protocol}//${url.host}` : url.origin;
    if (item !== normalized && item !== `${normalized}/`) {
      throw new Error(`${path}[${index}] must be an exact origin`);
    }
    return normalized;
  });
}

function optionalInteger(value: unknown, fallback: number, min: number, max = Number.MAX_SAFE_INTEGER): number {
  if (value === undefined) return fallback;
  if (!Number.isInteger(value) || (value as number) < min || (value as number) > max) {
    throw new Error(`value must be an integer between ${min} and ${max}`);
  }
  return value as number;
}
