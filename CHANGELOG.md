# Changelog

All notable changes to this project will be documented in this file.

## 0.2.2

- Explicit missing-executable diagnostics (`ENOENT`) and zero-config
  authentication guidance when ACP providers fail to start or exit with
  unauthenticated errors (deriving login hints from ACP `authMethods` or the
  provider launcher without requiring custom `authHint` configuration)
- Reduced SQLite write amplification by keeping ephemeral channels (`terminal`,
  `resourceWatch`) and the action replay ring buffer in memory, and coalescing
  streaming `chat/delta` and `chat/reasoning` updates until non-streaming
  checkpoints or shutdown

## 0.2.1

- Multimodal message attachment support (`Message.attachments` -> ACP
  `ContentBlock[]`), covering workspace file/directory/image resources,
  code selection slicing (`#Lstart-Lend`), clipboard images and pasted text
  (`embeddedResource`), `simple` context, `annotations`, and prior `chat`
  transcripts
- Workspace path boundary enforcement on local file attachments and automatic
  capability fallback based on ACP `promptCapabilities` (`image`, `audio`,
  `embeddedContext`)

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
