# Changelog

All notable changes to this project will be documented in this file.

## 0.2.0

- Inline `fileEdit` tool call results mapped from ACP native `diff` payloads
  with SQLite-persisted `session-db:` before/after snapshots
- Native AHP changeset channels (`uncommitted`, `session`, and per-turn
  changesets), review state tracking, and `discard-changes` operation across
  both Git repositories (`git-blob:` resources) and non-Git workspaces
- File and directory watching via `createResourceWatch` and
  `resourceWatch/changed` notifications
- Fixed VS Code Agent Window tool confirmation when `selectedOptionId` is
  omitted and unwrapped `vscode-agent-host:` resource URIs
- Fixed streaming markdown response part ordering around tool calls and
  reasoning blocks
- Fixed root `activeClients` tracking, dynamic model catalogue propagation, and
  `session/load` restoration metadata

## 0.1.0

Initial public MVP release.

- Universal AHP WebSocket host for configured ACP v1 agents
- Zero-config startup with a bundled provider catalog
- Native AHP provider, model, and Session Config Option integration
- SQLite persistence, sequencing, replay, reconnect, and restart repair
- Streaming responses, reasoning, plans, tools, permissions, and cancellation
- VS Code Agent Window folder browsing, file opening, and basic terminal
  compatibility
- Cross-platform lazy provider startup, including Windows `npx.cmd` handling
- Security controls for remote listeners, Origins, authentication, and stderr
