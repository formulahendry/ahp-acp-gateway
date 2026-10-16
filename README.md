# AHP-ACP Gateway

[![npm version](https://img.shields.io/npm/v/ahp-acp-gateway.svg)](https://www.npmjs.com/package/ahp-acp-gateway)
[![CI](https://github.com/formulahendry/ahp-acp-gateway/actions/workflows/ci.yml/badge.svg)](https://github.com/formulahendry/ahp-acp-gateway/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Connect Agent Host Protocol (AHP) clients to ACP v1 agents through one
stateful, protocol-compliant gateway.

AHP-ACP Gateway provides the AHP WebSocket server, persistent session state,
reconnect/replay, and ACP subprocess lifecycle that ACP agents do not provide
on their own. It uses the official AHP and ACP SDKs for protocol types and
behavior.

## Quick start

Requires Node.js 24 or newer.

```sh
npx --yes ahp-acp-gateway
```

The gateway starts with no config file:

- AHP WebSocket: `ws://127.0.0.1:8765/ahp`
- Health check: `http://127.0.0.1:8765/healthz`
- SQLite database: `~/.ahp-acp-gateway/ahp-acp-gateway.db`

Point an AHP client, such as the VS Code Agent Window, at the WebSocket URL.
Choose an ACP provider in the agent picker, then choose the provider's model in
the native model picker. The provider process starts lazily when the session is
created.

No ACP provider is downloaded or executed merely by starting the gateway.
Selecting a built-in `npx` provider may download that provider package on first
use.

## Install globally

```sh
npm install --global ahp-acp-gateway
ahp-acp-gateway
```

## Features

- AHP JSON-RPC over WebSocket
- Multiple configured ACP providers
- One lazy ACP subprocess per AHP chat
- Streaming text, reasoning, plans, tools, permissions, cancellation, stop
  reasons, and failures
- ACP Session Config Options mapped to native AHP model and session controls
- Multi-client observation with monotonic `serverSeq`
- SQLite-backed sessions, transcripts, action log, snapshots, and ACP bindings
- Bounded reconnect replay with snapshot fallback
- Restart repair for interrupted turns
- ACP `session/load` after restart when supported
- Read-only remote folder discovery and file opening for AHP clients
- Basic pipe-backed Agent Host terminal lifecycle
- ACP commands exposed as AHP slash completions
- Explicit errors for unsupported protocol methods

## Built-in ACP providers

Built-ins are advertised immediately but launched only after a user creates a
session for that provider.

| ID | Agent | Default launcher |
|---|---|---|
| `copilot` | GitHub Copilot | `npx --yes @github/copilot --acp` |
| `claude` | Claude Code | `npx --yes @agentclientprotocol/claude-agent-acp` |
| `gemini` | Gemini CLI | `npx --yes @google/gemini-cli --experimental-acp` |
| `qwen` | Qwen Code | `npx --yes @qwen-code/qwen-code --acp --experimental-skills` |
| `auggie` | Auggie | `npx --yes @augmentcode/auggie --acp` |
| `qoder` | Qoder CLI | `npx --yes @qoder-ai/qodercli --acp` |
| `codex` | Codex CLI | `npx --yes @agentclientprotocol/codex-acp` |
| `opencode` | OpenCode | `npx --yes @opencode/cli acp` |
| `openclaw` | OpenClaw | `npx --yes openclaw acp` |
| `kiro` | Kiro CLI | `kiro-cli acp` |
| `hermes` | Hermes Agent | `hermes acp` |
| `kimi` | Kimi CLI | `kimi acp` |
| `pi` | Pi ACP | `npx --yes pi-acp` |

Kiro, Hermes, and Kimi must already be installed on `PATH`. Other providers can
also require their own authentication. For example:

```sh
npx --yes @qoder-ai/qodercli login
```

Built-in commands intentionally contain no `--yolo`, auto-approval, or similar
permission-bypass flags.

## Configuration

The complete zero-config configuration is
[`ahp-acp-gateway.default.json`](./ahp-acp-gateway.default.json). It is loaded
by the CLI and shipped in the npm package. Copy it when you want a complete,
standalone configuration.

For smaller changes, start from
[`ahp-acp-gateway.example.json`](./ahp-acp-gateway.example.json). It is a
minimal overlay demonstrating:

- a strict browser Origin allowlist;
- disabling a built-in provider;
- pinning a provider package version;
- adding a custom ACP provider.

Run an overlay directly with `npx`:

```sh
npx --yes ahp-acp-gateway --config ./ahp-acp-gateway.example.json
```

Configuration version `1` uses these merge rules:

- `listener`, `auth`, `database`, `replay`, and `providerDefaults` override
  only supplied fields.
- Providers merge by `id`.
- `enabled: false` disables a built-in.
- A built-in override inherits fields it does not specify.
- A new provider must supply at least `id`, `displayName`, and `command`.
- `~` in the database path expands to the current user's home directory.
- Relative provider `cwd` values resolve to absolute paths at startup.

Inspect the resolved configuration:

```sh
npx --yes ahp-acp-gateway --print-config
npx --yes ahp-acp-gateway --config ./gateway.json --print-config
```

Tokens and provider environment values are redacted in printed output.

Show CLI help or the installed version:

```sh
npx --yes ahp-acp-gateway --help
npx --yes ahp-acp-gateway --version
```

For reproducible deployments, pin package versions in provider `args`, as
shown in the example configuration.

## Client behavior

The provider picker selects the ACP agent. ACP's `model` Session Config Option
is presented through the native AHP model picker, while mode, reasoning,
permission, and other options remain in the session configuration controls.
Before each prompt, the selected model is applied through ACP
`session/set_config_option`.

AHP `workingDirectories` must be absolute `file:` URIs. The gateway converts
them to native paths before calling ACP.

## Security

AHP-ACP Gateway is a protocol bridge, not a sandbox.

- It binds to loopback by default.
- Non-loopback listeners require both `auth.token` and
  `listener.tlsTerminated: true`.
- Browser Origins are allowed by default for local zero-config compatibility.
  Set `listener.allowAnyOrigin` to `false` and configure exact
  `listener.allowedOrigins` for stricter deployments.
- Authentication tokens can be supplied through the standard HTTP Bearer
  authorization header or the WebSocket `token` query parameter. Query tokens
  should only be used over TLS because URLs may be logged.
- Permission requests are never auto-approved.
- Provider stderr remains server-side; clients receive only a correlation
  reference and safe guidance.
- Provider processes inherit the gateway account's filesystem and operating
  system permissions. Run untrusted agents in an external sandbox or
  container.

## Current limitations

- ACP v1 only.
- One public chat per session; `createChat` is not implemented.
- Resource access is read-only: folder listing, metadata resolution, and file
  reads are implemented; writes, copies, moves, deletes, and watches are not.
- Terminals are pipe-backed rather than true PTYs; resize propagation and
  command detection are not implemented.
- No changesets, MCP forwarding, attachments/images, forks, multi-user policy,
  or built-in sandbox.
- Filesystem RPC requested by ACP agents is not advertised.
- AHP windowed subscriptions are not implemented.
- The provider catalog is bundled and updated manually rather than synchronized
  automatically with the ACP Registry.

## Development

```sh
git clone https://github.com/formulahendry/ahp-acp-gateway.git
cd ahp-acp-gateway
npm install
npm run check
npm run dev
```

Build and run the compiled CLI:

```sh
npm run build
node dist/src/cli.js
```

The integration suite uses the official AHP WebSocket client against the real
gateway and a fake ACP subprocess implemented with the official ACP SDK.

See [Architecture](docs/architecture.md) for protocol mapping, persistence,
ordering, replay, and restart behavior.

## License

[MIT](LICENSE)
