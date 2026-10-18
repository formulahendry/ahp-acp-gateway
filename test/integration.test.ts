import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { isAbsolute, join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { promisify } from 'node:util';
import { fileURLToPath, pathToFileURL } from 'node:url';
import test from 'node:test';
import WebSocket from 'ws';
import {
  ActionType,
  ChangesetOperationTargetKind,
  ChangesetStatus,
  CompletionItemKind,
  ContentEncoding,
  MessageAttachmentKind,
  MessageKind,
  PROTOCOL_VERSION,
   ResourceChangeType,
  ResponsePartKind,
  SessionStatus,
  TerminalClaimKind,
  ToolCallConfirmationReason,
  type ActionEnvelope,
  type ChangesetState,
  type ChatState,
  type ResourceWatchState,
  type SessionState,
  type StateAction,
} from '@microsoft/agent-host-protocol';
import { AhpClient, type Subscription } from '@microsoft/agent-host-protocol/client';
import { WebSocketTransport } from '@microsoft/agent-host-protocol/ws';
import {
  formatSelectionRangeFragment,
  GatewayServer,
  sliceTextBySelection,
} from '../src/gateway.js';
import { GatewayStore } from '../src/store.js';
import {
  adaptPromptBlocksToCapabilities,
  createSetConfigOptionRequest,
  requiresCommandShell,
  resolveProviderExecutable,
  toNativeWorkingDirectory,
} from '../src/acp.js';
import {
  defaultConfig,
  redactConfig,
  validateConfig,
  type GatewayConfig,
} from '../src/config.js';

const execFileAsync = promisify(execFile);
const fakeAgent = fileURLToPath(new URL('./fake-agent.ts', import.meta.url));
const fakeAgentNoLoad = fileURLToPath(new URL('./fake-agent-no-load.ts', import.meta.url));

test('official AHP clients observe one ACP-backed turn in order', async (t) => {
  const fixture = await startFixture();
  t.after(fixture.close);
  const first = await connectClient(fixture.url, 'first');
  const second = await connectClient(fixture.url, 'second');
  t.after(() => first.shutdown());
  t.after(() => second.shutdown());

  const { session, chat } = await createSession(first, 'multi-client');
  const firstChat = (await first.subscribe(chat)).subscription;
  const secondChat = (await second.subscribe(chat)).subscription;

  const turnId = crypto.randomUUID();
  first.dispatch(chat, userTurn(turnId, 'hello'));
  const [firstActions, secondActions] = await Promise.all([
    collectUntil(firstChat, ActionType.ChatTurnComplete),
    collectUntil(secondChat, ActionType.ChatTurnComplete),
  ]);

  assert.deepEqual(
    firstActions.map((item) => item.serverSeq),
    secondActions.map((item) => item.serverSeq),
  );
  assert.ok(firstActions.some((item) =>
    item.action.type === ActionType.ChatDelta && item.action.content === 'echo:hello'));
  const snapshot = fixture.server.store.getSnapshot(chat);
  const state = snapshot?.state as ChatState;
  assert.equal(state.turns.length, 1);
  assert.equal(state.turns[0]?.id, turnId);
  assert.equal(state.activeTurn, undefined);
  assert.equal((await first.request('listSessions', { channel: 'ahp-root://' })).items[0]?.resource, session);
});

test('reconnect replays retained actions and snapshots after replay overflow', async (t) => {
  const fixture = await startFixture(3);
  t.after(fixture.close);
  const client = await connectClient(fixture.url, 'replay-client');
  const { chat } = await createSession(client, 'replay');
  const subscription = (await client.subscribe(chat)).subscription;
  client.dispatch(chat, userTurn(crypto.randomUUID(), 'hello'));
  await collectUntil(subscription, ActionType.ChatTurnComplete);
  const replayFrom = fixture.server.store.serverSeq;
  client.dispatch(chat, {
    type: ActionType.ChatIsReadChanged,
    isRead: true,
  } as StateAction);
  await nextAction(subscription, (item) => item.action.type === ActionType.ChatIsReadChanged);
  await client.shutdown();

  const replayClient = new AhpClient(await WebSocketTransport.connect(fixture.url));
  replayClient.connect();
  const replay = await replayClient.reconnect({
    clientId: 'replay-client',
    lastSeenServerSeq: replayFrom,
    subscriptions: [chat],
  });
  assert.equal(replay.type, 'replay');
  if (replay.type === 'replay') assert.ok(replay.actions.length >= 1);
  await replayClient.shutdown();

  const snapshotClient = new AhpClient(await WebSocketTransport.connect(fixture.url));
  snapshotClient.connect();
  const snapshot = await snapshotClient.reconnect({
    clientId: 'replay-client',
    lastSeenServerSeq: 0,
    subscriptions: [chat],
  });
  assert.equal(snapshot.type, 'snapshot');
  if (snapshot.type === 'snapshot') assert.equal(snapshot.snapshots[0]?.resource, chat);
  await snapshotClient.shutdown();
});

test('reconnect with no missed actions returns snapshots for a fresh window', async (t) => {
  const fixture = await startFixture();
  t.after(fixture.close);
  const first = new AhpClient(await WebSocketTransport.connect(fixture.url));
  first.connect();
  const initialized = await first.initialize({
    clientId: 'fresh-window-client',
    protocolVersions: [PROTOCOL_VERSION],
    initialSubscriptions: ['ahp-root://'],
  });
  await first.shutdown();

  const reopened = new AhpClient(await WebSocketTransport.connect(fixture.url));
  reopened.connect();
  const result = await reopened.reconnect({
    clientId: 'fresh-window-client',
    lastSeenServerSeq: initialized.serverSeq,
    subscriptions: ['ahp-root://'],
  });
  assert.equal(result.type, 'snapshot');
  if (result.type === 'snapshot') {
    const root = result.snapshots[0]?.state;
    assert.ok(root && 'agents' in root);
    assert.equal(root.agents[0]?.provider, 'fake');
    assert.equal(root.agents[0]?.models[0]?.id, 'default');
  }
  await reopened.shutdown();
});

test('gateway shutdown terminates open WebSockets without waiting for clients to exit', async () => {
  const fixture = await startFixture();
  const socket = await new Promise<WebSocket>((resolve, reject) => {
    const candidate = new WebSocket(fixture.url);
    candidate.once('open', () => resolve(candidate));
    candidate.once('error', reject);
  });
  const started = Date.now();
  await Promise.race([
    fixture.server.close(),
    new Promise<never>((_, reject) =>
      setTimeout(() => reject(new Error('Gateway shutdown timed out')), 3_000)),
  ]);
  assert.ok(Date.now() - started < 3_000);
  assert.ok(socket.readyState === WebSocket.CLOSED || socket.readyState === WebSocket.CLOSING);
  await rm(fixture.directory, { recursive: true, force: true });
});

test('permission requests are first-answer-wins', async (t) => {
  const fixture = await startFixture();
  t.after(fixture.close);
  const first = await connectClient(fixture.url, 'permission-first');
  const second = await connectClient(fixture.url, 'permission-second');
  t.after(() => first.shutdown());
  t.after(() => second.shutdown());
  const { chat } = await createSession(first, 'permission');
  const firstSub = (await first.subscribe(chat)).subscription;
  const secondSub = (await second.subscribe(chat)).subscription;
  const turnId = crypto.randomUUID();
  first.dispatch(chat, userTurn(turnId, 'permission'));

  const ready = await nextAction(firstSub, (item) => item.action.type === ActionType.ChatToolCallReady);
  const readyAction = ready.action as Extract<StateAction, { type: ActionType.ChatToolCallReady }>;
  second.dispatch(chat, {
    type: ActionType.ChatToolCallConfirmed,
    turnId,
    toolCallId: readyAction.toolCallId,
    approved: true,
    confirmed: ToolCallConfirmationReason.UserAction,
    selectedOptionId: 'reject',
  } as StateAction);
  const contradictory = await nextAction(firstSub, (item) => item.rejectionReason !== undefined);
  await nextAction(secondSub, (item) => item.rejectionReason !== undefined);
  assert.match(contradictory.rejectionReason ?? '', /contradicts/);

  second.dispatch(chat, {
    type: ActionType.ChatToolCallConfirmed,
    turnId,
    toolCallId: readyAction.toolCallId,
    approved: true,
    confirmed: ToolCallConfirmationReason.UserAction,
    selectedOptionId: 'missing',
  } as StateAction);
  const unknown = await nextAction(firstSub, (item) => item.rejectionReason !== undefined);
  await nextAction(secondSub, (item) => item.rejectionReason !== undefined);
  assert.match(unknown.rejectionReason ?? '', /Unknown confirmation option/);

  const confirmation = {
    type: ActionType.ChatToolCallConfirmed,
    turnId,
    toolCallId: readyAction.toolCallId,
    approved: true,
    confirmed: ToolCallConfirmationReason.UserAction,
    selectedOptionId: 'allow',
  } as StateAction;
  first.dispatch(chat, confirmation);
  second.dispatch(chat, confirmation);

  const [complete, rejected] = await Promise.all([
    nextAction(firstSub, (item) => item.action.type === ActionType.ChatTurnComplete),
    nextAction(secondSub, (item) => item.rejectionReason !== undefined),
  ]);
  assert.equal(complete.action.type, ActionType.ChatTurnComplete);
  assert.match(rejected.rejectionReason ?? '', /not pending|already resolved|unknown/);
  const state = fixture.server.store.getSnapshot(chat)?.state as ChatState;
  const text = state.turns[0]?.responseParts
    .filter((part) => part.kind === 'markdown')
    .map((part) => part.content)
    .join('');
  assert.match(text ?? '', /permission:allow/);
});

test('browser origins are rejected unless explicitly allowed', async (t) => {
  const fixture = await startFixture();
  t.after(fixture.close);
  fixture.server.config.listener.allowAnyOrigin = false;
  await assert.rejects(
    connectRawWebSocket(fixture.url, 'https://evil.example'),
    /HTTP 403/,
  );

  fixture.server.config.listener.allowedOrigins.push('vscode-file://vscode-app');
  const trusted = await connectRawWebSocket(fixture.url, 'vscode-file://vscode-app');
  trusted.close();
});

test('protocol negotiation accepts compatible versions and rejects malformed or unsupported versions', async (t) => {
  const fixture = await startFixture();
  t.after(fixture.close);
  const versionParts = PROTOCOL_VERSION.split('.').map(Number);
  const [major, minor, patch] = [versionParts[0]!, versionParts[1]!, versionParts[2]!];
  const compatible = `${major}.${minor}.${patch + 1}`;
  const compatibleClient = new AhpClient(await WebSocketTransport.connect(fixture.url));
  compatibleClient.connect();
  const initialized = await compatibleClient.initialize({
    clientId: 'compatible-version',
    protocolVersions: [compatible],
  });
  assert.equal(initialized.protocolVersion, compatible);
  await compatibleClient.shutdown();

  const malformedClient = new AhpClient(await WebSocketTransport.connect(fixture.url));
  malformedClient.connect();
  await assert.rejects(
    malformedClient.initialize({ clientId: 'malformed-version', protocolVersions: ['1'] }),
    /Invalid protocol version/i,
  );
  await malformedClient.shutdown();

  const unsupportedClient = new AhpClient(await WebSocketTransport.connect(fixture.url));
  unsupportedClient.connect();
  await assert.rejects(
    unsupportedClient.initialize({ clientId: 'unsupported-version', protocolVersions: ['99.0.0'] }),
    /Unsupported protocol version/,
  );
  await unsupportedClient.shutdown();
});

test('VS Code remote folder browsing and session config resolution surfaces are available', async (t) => {
  const fixture = await startFixture();
  t.after(fixture.close);
  const workspace = join(fixture.directory, 'workspace');
  await mkdir(workspace);
  await writeFile(join(fixture.directory, 'outside.txt'), 'outside');
  await writeFile(join(workspace, 'qsort.js'), 'export const qsort = (items) => [...items].sort();\n');
  await writeFile(join(workspace, 'binary.dat'), Buffer.from([0, 255, 1, 254]));
  const client = new AhpClient(await WebSocketTransport.connect(fixture.url));
  client.connect();
  t.after(() => client.shutdown());
  const initialized = await client.initialize({
    clientId: 'vscode-folder-client',
    protocolVersions: [PROTOCOL_VERSION],
  });
  assert.equal(initialized.defaultDirectory, pathToFileURL(process.cwd()).href);

  const rootUri = pathToFileURL(fixture.directory).href;
  const listed = await client.request('resourceList', {
    channel: 'ahp-root://',
    uri: rootUri,
  });
  assert.deepEqual(
    listed.entries.find((entry) => entry.name === 'workspace'),
    { name: 'workspace', type: 'directory' },
  );
  assert.deepEqual(
    listed.entries.find((entry) => entry.name === 'outside.txt'),
    { name: 'outside.txt', type: 'file' },
  );
  const resolved = await client.request('resourceResolve', {
    channel: 'ahp-root://',
    uri: pathToFileURL(workspace).href,
  });
  assert.equal(resolved.type, 'directory');
  const sessionConfig = await client.request('resolveSessionConfig', {
    channel: 'ahp-root://',
    provider: 'fake',
    workingDirectory: pathToFileURL(workspace).href,
  });
  assert.deepEqual(sessionConfig, {
    schema: { type: 'object', properties: {} },
    values: {},
  });
  const session = `ahp-session:/resource-read-${crypto.randomUUID()}`;
  await client.request('createSession', {
    channel: session,
    provider: 'fake',
    workingDirectories: [pathToFileURL(workspace).href],
  });
  const textUri = pathToFileURL(join(workspace, 'qsort.js')).href;
  const text = await client.request('resourceRead', {
    channel: 'ahp-root://',
    uri: process.platform === 'win32'
      ? textUri.replace(/^file:\/\/\/([A-Za-z]):/, (_, drive: string) =>
          `file:///${drive.toLowerCase()}%3A`)
      : textUri,
  });
  assert.deepEqual(text, {
    data: 'export const qsort = (items) => [...items].sort();\n',
    encoding: 'utf-8',
    contentType: 'text/javascript',
  });
  const binary = await client.request('resourceRead', {
    channel: 'ahp-root://',
    uri: pathToFileURL(join(workspace, 'binary.dat')).href,
  });
  assert.deepEqual(binary, {
    data: Buffer.from([0, 255, 1, 254]).toString('base64'),
    encoding: 'base64',
    contentType: 'text/plain',
  });
  await assert.rejects(
    client.request('resourceRead', {
      channel: 'ahp-root://',
      uri: pathToFileURL(join(fixture.directory, 'outside.txt')).href,
    }),
    /Permission denied/,
  );
});

test('client-chosen non-ahp session URI schemes are supported', async (t) => {
  const fixture = await startFixture();
  t.after(fixture.close);
  const client = await connectClient(fixture.url, 'custom-session-uri-client');
  t.after(() => client.shutdown());
  const session = `remote-agent-session:/${crypto.randomUUID()}`;
  await client.request('createSession', {
    channel: session,
    provider: 'fake',
    workingDirectories: [pathToFileURL(process.cwd()).href],
    config: {
      mode: 'unsupported-client-default-mode',
      unknownKey: 'ignored',
    },
  });
  const snapshot = (await client.subscribe(session)).result.snapshot?.state as SessionState | undefined;
  assert.ok(snapshot && 'provider' in snapshot);
  assert.equal(snapshot.config?.values?.mode, 'interactive');
  assert.equal(snapshot.config?.values?.unknownKey, undefined);

  const validConfigSession = `remote-agent-session:/${crypto.randomUUID()}`;
  await client.request('createSession', {
    channel: validConfigSession,
    provider: 'fake',
    config: {
      mode: 'plan',
    },
  });
  const validSnapshot = (await client.subscribe(validConfigSession)).result.snapshot?.state as SessionState | undefined;
  assert.equal(validSnapshot?.config?.values?.mode, 'plan');
  assert.deepEqual(validSnapshot?.workingDirectories, [pathToFileURL(process.cwd()).href]);
  await client.request('disposeSession', { channel: validConfigSession });
  assert.equal(snapshot.provider, 'fake');
  client.dispatch(session, {
    type: ActionType.SessionConfigChanged,
    config: { mode: 'plan' },
  } as StateAction);
  await waitForCondition(() =>
    fixture.server.store.getSession(session)?.state.config?.values.mode === 'plan');
  await client.request('disposeSession', { channel: session });
});

test('VS Code agent-host terminal lifecycle supports create, input, output, and dispose', async (t) => {
  const fixture = await startFixture();
  t.after(fixture.close);
  const client = await connectClient(fixture.url, 'terminal-api-client');
  t.after(() => client.shutdown());
  const channel = `agenthost-terminal:/${crypto.randomUUID()}`;
  await client.request('createTerminal', {
    channel,
    claim: { kind: TerminalClaimKind.Client, clientId: 'terminal-api-client' },
    name: 'Test Terminal',
    cwd: pathToFileURL(process.cwd()).href,
    cols: 80,
    rows: 24,
  });
  const subscription = (await client.subscribe(channel)).subscription;
  const terminalProcess = fixture.server.terminals.get(channel);
  assert.ok(terminalProcess);
  const marker = `TERMINAL_${crypto.randomUUID().replaceAll('-', '')}`;
  client.dispatch(channel, {
    type: ActionType.TerminalInput,
    data: process.platform === 'win32' ? `echo ${marker}\r\n` : `echo ${marker}\n`,
  } as StateAction);
  const output = await nextAction(subscription, (item) =>
    item.action.type === ActionType.TerminalData && item.action.data.includes(marker));
  assert.equal(output.action.type, ActionType.TerminalData);
  await client.request('disposeTerminal', { channel });
  assert.equal(fixture.server.store.getChannel(channel), undefined);
  assert.ok(terminalProcess.exitCode !== null || terminalProcess.signalCode !== null);
  assert.equal(terminalProcess.stdin.destroyed, true);
  assert.equal(terminalProcess.stdout.destroyed, true);
  assert.equal(terminalProcess.stderr.destroyed, true);
});

test('cancellation and process crash become explicit terminal states', async (t) => {
  const fixture = await startFixture();
  t.after(fixture.close);
  const client = await connectClient(fixture.url, 'terminal-client');
  t.after(() => client.shutdown());
  const { chat } = await createSession(client, 'terminal');
  const subscription = (await client.subscribe(chat)).subscription;

  const cancelledTurn = crypto.randomUUID();
  client.dispatch(chat, userTurn(cancelledTurn, 'slow'));
  await nextAction(subscription, (item) =>
    item.action.type === ActionType.ChatTurnStarted && item.action.turnId === cancelledTurn);
  client.dispatch(chat, {
    type: ActionType.ChatTurnCancelled,
    turnId: cancelledTurn,
    duration: 1,
  } as StateAction);
  await nextAction(subscription, (item) =>
    item.action.type === ActionType.ChatTurnCancelled && item.action.turnId === cancelledTurn);

  const crashedTurn = crypto.randomUUID();
  client.dispatch(chat, userTurn(crashedTurn, 'crash'));
  const crashed = await nextAction(subscription, (item) =>
    item.action.type === ActionType.ChatError && item.action.turnId === crashedTurn);
  assert.match(JSON.stringify(crashed.action), /acp_process_crash|acp_error/);
});

test('cancellation is sequenced before bounded cleanup of an unresponsive agent', async (t) => {
  const fixture = await startFixture();
  t.after(fixture.close);
  const provider = fixture.server.config.providers[0]!;
  provider.shutdownTimeoutMs = 100;
  const client = await connectClient(fixture.url, 'unresponsive-cancel-client');
  t.after(() => client.shutdown());
  const { chat } = await createSession(client, 'unresponsive-cancel');
  const subscription = (await client.subscribe(chat)).subscription;
  const turnId = crypto.randomUUID();
  client.dispatch(chat, userTurn(turnId, 'ignore-cancel'));
  await nextAction(subscription, (item) =>
    item.action.type === ActionType.ChatTurnStarted && item.action.turnId === turnId);

  const started = Date.now();
  client.dispatch(chat, {
    type: ActionType.ChatTurnCancelled,
    turnId,
    duration: 1,
  } as StateAction);
  await nextAction(subscription, (item) =>
    item.action.type === ActionType.ChatTurnCancelled && item.action.turnId === turnId);
  assert.ok(Date.now() - started < 500);
  await waitForCondition(() => fixture.server.adapters.size === 0);

  const nextTurn = crypto.randomUUID();
  client.dispatch(chat, userTurn(nextTurn, 'hello'));
  await nextAction(subscription, (item) =>
    item.action.type === ActionType.ChatTurnComplete && item.action.turnId === nextTurn);
});

test('idle process exits are evicted and the next turn starts a fresh adapter', async (t) => {
  const fixture = await startFixture();
  t.after(fixture.close);
  const client = await connectClient(fixture.url, 'idle-exit-client');
  t.after(() => client.shutdown());
  const { chat } = await createSession(client, 'idle-exit');
  const subscription = (await client.subscribe(chat)).subscription;
  const firstTurn = crypto.randomUUID();
  client.dispatch(chat, userTurn(firstTurn, 'exit-after'));
  await nextAction(subscription, (item) =>
    item.action.type === ActionType.ChatTurnComplete && item.action.turnId === firstTurn);
  await waitForCondition(() => fixture.server.adapters.size === 0);

  const secondTurn = crypto.randomUUID();
  client.dispatch(chat, userTurn(secondTurn, 'hello'));
  await nextAction(subscription, (item) =>
    item.action.type === ActionType.ChatTurnComplete && item.action.turnId === secondTurn);
});

test('prompt timeout evicts the process and isolates late notifications from the next turn', async (t) => {
  const fixture = await startFixture();
  t.after(fixture.close);
  const provider = fixture.server.config.providers[0]!;
  provider.requestTimeoutMs = 100;
  provider.shutdownTimeoutMs = 100;
  const client = await connectClient(fixture.url, 'prompt-timeout-client');
  t.after(() => client.shutdown());
  const { chat } = await createSession(client, 'prompt-timeout');
  const subscription = (await client.subscribe(chat)).subscription;
  const timedOutTurn = crypto.randomUUID();
  client.dispatch(chat, userTurn(timedOutTurn, 'late'));
  await nextAction(subscription, (item) =>
    item.action.type === ActionType.ChatError && item.action.turnId === timedOutTurn);
  await waitForCondition(() => fixture.server.adapters.size === 0);

  provider.requestTimeoutMs = 10_000;
  const nextTurn = crypto.randomUUID();
  client.dispatch(chat, userTurn(nextTurn, 'hello'));
  await nextAction(subscription, (item) =>
    item.action.type === ActionType.ChatTurnComplete && item.action.turnId === nextTurn);
  await new Promise((resolve) => setTimeout(resolve, 500));
  assert.doesNotMatch(
    JSON.stringify(fixture.server.store.getSnapshot(chat)),
    /LATE_NOTIFICATION_SHOULD_NOT_APPEAR/,
  );
});

test('provider stderr remains server-side and is absent from persisted client payloads', async (t) => {
  const fixture = await startFixture();
  t.after(fixture.close);
  const client = await connectClient(fixture.url, 'stderr-client');
  t.after(() => client.shutdown());
  const { chat } = await createSession(client, 'stderr');
  const subscription = (await client.subscribe(chat)).subscription;
  const logs: string[] = [];
  const originalError = console.error;
  console.error = (...args: unknown[]) => logs.push(args.map(String).join(' '));
  try {
    const turnId = crypto.randomUUID();
    client.dispatch(chat, userTurn(turnId, 'crash-secret'));
    const failed = await nextAction(subscription, (item) =>
      item.action.type === ActionType.ChatError && item.action.turnId === turnId);
    assert.doesNotMatch(JSON.stringify(failed), /SUPER_SECRET_STDERR_VALUE/);
    assert.match(JSON.stringify(failed), /reference/);
    assert.doesNotMatch(JSON.stringify(fixture.server.store.getSnapshot(chat)), /SUPER_SECRET_STDERR_VALUE/);
    assert.doesNotMatch(
      JSON.stringify(fixture.server.store.actionsAfter(0, new Set([chat]))),
      /SUPER_SECRET_STDERR_VALUE/,
    );
    assert.match(logs.join('\n'), /SUPER_SECRET_STDERR_VALUE/);
  } finally {
    console.error = originalError;
  }
});

test('ACP context usage is namespaced without false turn token accounting', async (t) => {
  const fixture = await startFixture();
  t.after(fixture.close);
  const client = await connectClient(fixture.url, 'usage-client');
  t.after(() => client.shutdown());
  const { chat } = await createSession(client, 'usage');
  const subscription = (await client.subscribe(chat)).subscription;
  const turnId = crypto.randomUUID();
  client.dispatch(chat, userTurn(turnId, 'usage'));
  await nextAction(subscription, (item) =>
    item.action.type === ActionType.ChatTurnComplete && item.action.turnId === turnId);
  const usage = (fixture.server.store.getChat(chat)?.state.turns[0]?.usage ?? {}) as Record<string, unknown>;
  assert.equal(usage.inputTokens, undefined);
  assert.equal(usage.outputTokens, undefined);
  assert.deepEqual(usage._meta, { acpContext: { used: 123, size: 456 } });
});

test('ACP control updates do not pollute transcript and commands become slash completions', async (t) => {
  const fixture = await startFixture();
  t.after(fixture.close);
  const client = await connectClient(fixture.url, 'control-update-client');
  t.after(() => client.shutdown());
  const { session, chat } = await createSession(client, 'control-updates');
  const subscription = (await client.subscribe(chat)).subscription;
  const initialConfig = fixture.server.store.getSession(session)?.state.config;
  assert.equal(initialConfig?.schema.properties.model, undefined);
  assert.equal(initialConfig?.values.model, undefined);
  assert.deepEqual(initialConfig?.schema.properties.mode?.enum, ['interactive', 'plan']);
  const root = fixture.server.store.getSnapshot('ahp-root://')?.state;
  assert.ok(root && 'agents' in root);
  assert.deepEqual(
    root.agents.find((agent) => agent.provider === 'fake')?.models.map((model) => model.id),
    ['fake-model', 'fake-model-2'],
  );
  const selectedModelTurn = crypto.randomUUID();
  client.dispatch(chat, {
    ...userTurn(selectedModelTurn, 'report-model'),
    message: {
      text: 'report-model',
      origin: { kind: MessageKind.User },
      model: { id: 'fake-model-2' },
    },
  } as StateAction);
  const modelResponse = await nextAction(subscription, (item) =>
    item.action.type === ActionType.ChatDelta
    && item.action.turnId === selectedModelTurn
    && item.action.content.startsWith('model:'));
  assert.equal(modelResponse.action.type, ActionType.ChatDelta);
  assert.equal(modelResponse.action.content, 'model:fake-model-2');

  const resolvedConfig = await client.request('resolveSessionConfig', {
    channel: 'ahp-root://',
    provider: 'fake',
    workingDirectory: pathToFileURL(process.cwd()).href,
  });
  assert.equal(resolvedConfig.schema.properties.model, undefined);
  assert.deepEqual(resolvedConfig.schema.properties.mode?.enum, ['interactive', 'plan']);

  const turnId = crypto.randomUUID();
  client.dispatch(chat, userTurn(turnId, 'control-updates'));
  await nextAction(subscription, (item) =>
    item.action.type === ActionType.ChatTurnComplete && item.action.turnId === turnId);

  const chatState = fixture.server.store.getChat(chat)?.state;
  assert.doesNotMatch(JSON.stringify(chatState?.turns), /available_commands_update|config_option_update|session_info_update/);
  assert.equal(fixture.server.store.getSession(session)?.state.title, 'Control Title');
  const completions = await client.request('completions', {
    kind: CompletionItemKind.UserMessage,
    channel: chat,
    text: '/co',
    offset: 3,
  });
  assert.deepEqual(completions.items[0], {
    insertText: '/compact ',
    rangeStart: 0,
    rangeEnd: 3,
    attachment: {
      type: 'simple',
      label: 'Compact conversation history',
      _meta: {
        command: 'compact',
        description: 'Compact conversation history',
        argumentHint: 'optional focus',
        retriggerSuggestions: true,
      },
    },
  });
});

test('restart repair persists transcript and interrupts active turns', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'ahp-acp-gateway-repair-'));
  const database = join(directory, 'gateway.db');
  try {
    const store = new GatewayStore(database, 100);
    store.initializeRoot({ agents: [], activeSessions: 1 });
    const now = new Date().toISOString();
    const session = 'ahp-session:/repair';
    const chat = 'ahp-chat:/repair';
    store.putSession({
      resource: session,
      provider: 'fake',
      createdAt: now,
      modifiedAt: now,
      state: {
        provider: 'fake',
        title: 'Repair',
        status: SessionStatus.InProgress,
        lifecycle: 'ready',
        activeClients: [],
        chats: [{ resource: chat, title: 'Chat', status: SessionStatus.InProgress, modifiedAt: now }],
        defaultChat: chat,
      } as any,
    });
    store.putChat({
      resource: chat,
      session,
      state: {
        resource: chat,
        title: 'Chat',
        status: SessionStatus.Idle,
        modifiedAt: now,
        turns: [],
      },
      binding: { provider: 'fake', acpSessionId: 'persisted' },
    });
    store.apply(chat, userTurn('active', 'unfinished'));
    store.close();

    const reopened = new GatewayStore(database, 100);
    const repaired = reopened.repairInterruptedTurns();
    assert.equal(repaired.length, 1);
    const state = reopened.getChat(chat)?.state;
    assert.equal(state?.activeTurn, undefined);
    assert.equal(state?.turns[0]?.state, 'error');
    assert.equal(reopened.getChat(chat)?.binding?.acpSessionId, 'persisted');
    reopened.close();
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('root provider catalog changes advance serverSeq and remain replayable', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'ahp-acp-gateway-root-seq-'));
  const database = join(directory, 'gateway.db');
  try {
    const first = new GatewayStore(database, 100);
    first.ensureRoot({
      agents: [{ provider: 'one', displayName: 'One', description: '', models: [] }],
      activeSessions: 0,
    });
    const before = first.serverSeq;
    first.close();

    const second = new GatewayStore(database, 100);
    const changes = second.ensureRoot({
      agents: [{ provider: 'two', displayName: 'Two', description: '', models: [] }],
      activeSessions: 0,
    });
    assert.equal(changes.length, 1);
    assert.ok(second.serverSeq > before);
    const replay = second.actionsAfter(before, new Set(['ahp-root://']));
    assert.equal(replay?.[0]?.action.type, ActionType.RootAgentsChanged);
    assert.equal((second.getSnapshot('ahp-root://')?.state as any).agents[0].provider, 'two');
    second.close();
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('session and default chat creation is atomic on constraint failure', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'ahp-acp-gateway-atomic-'));
  const database = join(directory, 'gateway.db');
  try {
    const store = new GatewayStore(database, 100);
    store.ensureRoot({ agents: [], activeSessions: 0 });
    const now = new Date().toISOString();
    const firstSession = storedSession('ahp-session:/atomic-one', 'ahp-chat:/atomic-shared', now);
    const firstChat = storedChat(firstSession.resource, 'ahp-chat:/atomic-shared', now);
    store.createSession(firstSession, firstChat);
    const beforeSeq = store.serverSeq;

    const secondSession = storedSession('ahp-session:/atomic-two', 'ahp-chat:/atomic-shared', now);
    const conflictingChat = storedChat(secondSession.resource, 'ahp-chat:/atomic-shared', now);
    assert.throws(() => store.createSession(secondSession, conflictingChat));
    assert.equal(store.getSession(secondSession.resource), undefined);
    assert.equal(store.sessionCount(), 1);
    assert.equal(store.serverSeq, beforeSeq);
    assert.equal((store.getSnapshot('ahp-root://')?.state as any).activeSessions, 1);
    store.close();
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('ephemeral channels and streaming chat deltas avoid per-token SQLite writes while flushing on completion or close', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'ahp-acp-gateway-store-ephemeral-'));
  const database = join(directory, 'gateway.db');
  try {
    const store = new GatewayStore(database, 100);
    store.ensureRoot({ agents: [], activeSessions: 0 });
    const now = new Date().toISOString();
    const session = storedSession('ahp-session:/streaming-one', 'ahp-chat:/streaming-one', now);
    const chat = storedChat(session.resource, 'ahp-chat:/streaming-one', now);
    store.createSession(session, chat);

    const terminalUri = 'ahp-terminal:/term-1';
    store.createTerminal(terminalUri, {
      cols: 80,
      rows: 24,
      content: [],
      lifecycle: 'running' as any,
      claim: { kind: TerminalClaimKind.Client, clientId: 'test-client' } as any,
      supportsCommandDetection: false,
      isPty: false,
    } as never, {
      resource: terminalUri,
      title: 'echo',
      claim: { kind: TerminalClaimKind.Client, clientId: 'test-client' } as any,
      lifecycle: 'running' as any,
    });
    store.apply(terminalUri, {
      type: ActionType.TerminalData,
      data: 'hello\n',
    } as StateAction);

    const watchUri = 'ahp-watch:/watch-1';
    store.putChannel(watchUri, 'resourceWatch', {
      root: 'file:///tmp',
      recursive: true,
    } satisfies ResourceWatchState);

    store.apply(chat.resource, userTurn('turn-1', 'stream something'));
    store.apply(chat.resource, {
      type: ActionType.ChatResponsePart,
      turnId: 'turn-1',
      part: { kind: ResponsePartKind.Reasoning, id: 'part-r1', content: '' },
    } as StateAction);
    store.apply(chat.resource, {
      type: ActionType.ChatReasoning,
      turnId: 'turn-1',
      partId: 'part-r1',
      content: 'thinking...',
    } as StateAction);
    store.apply(chat.resource, {
      type: ActionType.ChatResponsePart,
      turnId: 'turn-1',
      part: { kind: ResponsePartKind.Markdown, id: 'part-m1', content: '' },
    } as StateAction);
    store.apply(chat.resource, {
      type: ActionType.ChatDelta,
      turnId: 'turn-1',
      partId: 'part-m1',
      content: 'Hello ',
    } as StateAction);
    store.apply(chat.resource, {
      type: ActionType.ChatDelta,
      turnId: 'turn-1',
      partId: 'part-m1',
      content: 'world!',
    } as StateAction);

    // Live in-memory reads reflect every streaming chunk immediately
    const liveChat = store.getChat(chat.resource)?.state;
    assert.equal(liveChat?.activeTurn?.responseParts.length, 2);
    assert.equal((liveChat?.activeTurn?.responseParts[1] as any)?.content, 'Hello world!');

    // Inspect raw SQLite file before turn completion / close:
    // 1. Ephemeral channels (terminal / resourceWatch) are not in `channels` table
    // 2. Streaming ChatDelta / ChatReasoning have not been flushed to `chats` table yet
    const rawDb = new DatabaseSync(database, { readOnly: true });
    const channelRows = rawDb.prepare(`SELECT uri, kind FROM channels`).all() as Array<{ uri: string; kind: string }>;
    assert.equal(channelRows.some((r) => r.uri === terminalUri || r.kind === 'terminal'), false);
    assert.equal(channelRows.some((r) => r.uri === watchUri || r.kind === 'resourceWatch'), false);

    const diskChatBeforeFlush = JSON.parse(
      (rawDb.prepare(`SELECT state_json FROM chats WHERE resource=?`).get(chat.resource) as { state_json: string }).state_json,
    ) as ChatState;
    assert.equal((diskChatBeforeFlush.activeTurn?.responseParts[1] as any)?.content, '');
    const actionRows = rawDb.prepare(`SELECT channel FROM actions`).all() as Array<{ channel: string }>;
    assert.equal(actionRows.some((r) => r.channel === chat.resource || r.channel === terminalUri), false);

    // Non-streaming action (ChatTurnComplete) flushes accumulated streaming state to SQLite
    store.apply(chat.resource, {
      type: ActionType.ChatTurnComplete,
      turnId: 'turn-1',
      duration: 12,
    } as StateAction);

    const diskChatAfterComplete = JSON.parse(
      (rawDb.prepare(`SELECT state_json FROM chats WHERE resource=?`).get(chat.resource) as { state_json: string }).state_json,
    ) as ChatState;
    assert.equal(diskChatAfterComplete.turns.length, 1);
    assert.equal((diskChatAfterComplete.turns[0]?.responseParts[1] as any)?.content, 'Hello world!');

    // Also verify store.close() flushes mid-turn dirty state if closed during streaming
    store.apply(chat.resource, userTurn('turn-2', 'second turn'));
    store.apply(chat.resource, {
      type: ActionType.ChatResponsePart,
      turnId: 'turn-2',
      part: { kind: ResponsePartKind.Markdown, id: 'part-m2', content: '' },
    } as StateAction);
    store.apply(chat.resource, {
      type: ActionType.ChatDelta,
      turnId: 'turn-2',
      partId: 'part-m2',
      content: 'partial before close',
    } as StateAction);
    rawDb.close();
    store.close();

    // Simulate legacy DB rows for terminal / resourceWatch to verify startup cleanup
    const legacyDb = new DatabaseSync(database);
    legacyDb.prepare(`INSERT INTO channels(uri, kind, state_json) VALUES (?, 'terminal', '{}')`).run('ahp-terminal:/legacy');
    legacyDb.prepare(`INSERT INTO channels(uri, kind, state_json) VALUES (?, 'resourceWatch', '{}')`).run('ahp-watch:/legacy');
    legacyDb.close();

    const reopened = new GatewayStore(database, 100);
    assert.equal(reopened.getChannel(terminalUri), undefined);
    assert.equal(reopened.getChannel(watchUri), undefined);
    assert.equal(reopened.getChannel('ahp-terminal:/legacy'), undefined);
    assert.equal(reopened.getChannel('ahp-watch:/legacy'), undefined);
    const reopenedChat = reopened.getChat(chat.resource)?.state;
    assert.equal((reopenedChat?.activeTurn?.responseParts[0] as any)?.content, 'partial before close');
    reopened.close();
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('restart uses ACP load when available and marks context reset otherwise', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'ahp-acp-gateway-context-'));
  const firstServer = new GatewayServer(testConfig(directory));
  const firstAddress = await firstServer.listen();
  const firstClient = await connectClient(
    `ws://${firstAddress.host}:${firstAddress.port}${firstAddress.path}`,
    'context-first',
  );
  const { session, chat } = await createSession(firstClient, 'context');
  const firstSub = (await firstClient.subscribe(chat)).subscription;
  firstClient.dispatch(chat, userTurn(crypto.randomUUID(), 'hello'));
  await collectUntil(firstSub, ActionType.ChatTurnComplete);
  assert.ok(firstServer.store.getChat(chat)?.binding?.acpSessionId);
  await firstClient.shutdown();
  await firstServer.close();

  const secondConfig = testConfig(directory);
  secondConfig.providers[0]!.args = ['--import', 'tsx', fakeAgentNoLoad];
  const secondServer = new GatewayServer(secondConfig);
  assert.ok(secondServer.store.getChat(chat)?.binding?.acpSessionId);
  const secondAddress = await secondServer.listen();
  try {
    const secondClient = await connectClient(
      `ws://${secondAddress.host}:${secondAddress.port}${secondAddress.path}`,
      'context-second',
    );
    const listed = await secondClient.request('listSessions', { channel: 'ahp-root://' });
    assert.equal(listed.items[0]?.resource, session);
    const secondSub = (await secondClient.subscribe(chat)).subscription;
    secondClient.dispatch(chat, userTurn(crypto.randomUUID(), 'after restart'));
    const actions = await collectUntil(secondSub, ActionType.ChatTurnComplete);
    assert.equal(secondServer.store.getChat(chat)?.binding?.loadSupported, false);
    assert.ok(actions.some((item) =>
      item.action.type === ActionType.ChatResponsePart
      && item.action.part.kind === 'systemNotification'
      && JSON.stringify(item.action.part).includes('context was reset')), JSON.stringify(actions));
    await secondClient.shutdown();
  } finally {
    await secondServer.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test('invalid actions and unsupported methods fail explicitly', async (t) => {
  const fixture = await startFixture();
  t.after(fixture.close);
  const client = await connectClient(fixture.url, 'invalid-client');
  t.after(() => client.shutdown());
  const { chat } = await createSession(client, 'invalid');
  const subscription = (await client.subscribe(chat)).subscription;
  client.dispatch(chat, {
    type: ActionType.ChatDelta,
    turnId: 'x',
    partId: 'x',
    content: 'forged',
  } as StateAction);
  const rejected = await nextAction(subscription, (item) => item.rejectionReason !== undefined);
  assert.match(rejected.rejectionReason ?? '', /server-only/);
  const missingUri = pathToFileURL(join(fixture.directory, 'missing.txt')).href;
  await assert.rejects(
    client.request('resourceRead', { channel: 'ahp-root://', uri: missingUri }),
    /Resource not found/,
  );
  await assert.rejects(
    client.request('resourceWrite', {
      channel: 'ahp-root://',
      uri: missingUri,
      data: '',
      encoding: ContentEncoding.Utf8,
    }),
    /Method not implemented/,
  );
});

test('configuration enforces remote authentication and redacts secrets', () => {
  assert.throws(() => validateConfig({
    version: 1,
    listener: { host: '0.0.0.0', port: 8765, tlsTerminated: false },
    database: { path: 'gateway.db' },
    providers: [{
      id: 'fake',
      displayName: 'Fake',
      command: 'fake',
    }],
  }), /require auth.token/);

  const config = validateConfig({
    version: 1,
    listener: { host: '0.0.0.0', port: 8765, tlsTerminated: true },
    auth: { token: 'secret-token' },
    database: { path: 'gateway.db' },
    providers: [{
      id: 'fake',
      displayName: 'Fake',
      command: 'fake',
      env: { API_KEY: 'secret-key' },
    }],
  });
  const redacted = redactConfig(config);
  assert.equal(redacted.auth.token, '<redacted>');
  assert.equal(
    redacted.providers.find((provider) => provider.id === 'fake')?.env?.API_KEY,
    '<redacted>',
  );
  const vscodeOrigin = validateConfig({
    version: 1,
    listener: { allowedOrigins: ['vscode-file://vscode-app'] },
  });
  assert.deepEqual(vscodeOrigin.listener.allowedOrigins, ['vscode-file://vscode-app']);
});

test('working directories require file URIs and provider cwd resolves absolutely', async (t) => {
  const cwdUri = pathToFileURL(process.cwd()).href;
  assert.equal(isAbsolute(toNativeWorkingDirectory(cwdUri)), true);
  assert.throws(() => toNativeWorkingDirectory('https://example.com/workspace'), /must use file/);
  assert.throws(() => toNativeWorkingDirectory(process.cwd()), /absolute file URI|must use file/);
  const config = validateConfig({
    version: 1,
    providers: [{
      id: 'custom-path',
      displayName: 'Custom Path',
      command: 'custom-agent',
      cwd: '.',
    }],
  });
  assert.equal(isAbsolute(config.providers.find((provider) => provider.id === 'custom-path')?.cwd ?? ''), true);

  const fixture = await startFixture();
  t.after(fixture.close);
  const client = await connectClient(fixture.url, 'invalid-working-directory');
  t.after(() => client.shutdown());
  await assert.rejects(
    client.request('createSession', {
      channel: `ahp-session:/invalid-uri-${crypto.randomUUID()}`,
      provider: 'fake',
      workingDirectories: ['https://example.com/workspace'],
    }),
    /must use file/,
  );
});

test('provider executable resolution uses npx.cmd only on Windows', () => {
  assert.equal(resolveProviderExecutable({ command: 'npx' }, 'win32'), 'npx.cmd');
  assert.equal(resolveProviderExecutable({ command: 'npx' }, 'linux'), 'npx');
  assert.equal(resolveProviderExecutable({ command: 'uvx' }, 'win32'), 'uvx');
  assert.equal(requiresCommandShell('npx.cmd', 'win32'), true);
  assert.equal(requiresCommandShell('agent.exe', 'win32'), false);
  assert.equal(requiresCommandShell('npx.cmd', 'linux'), false);
});

test('ACP select config requests omit the boolean-only type discriminator', () => {
  assert.deepEqual(
    createSetConfigOptionRequest('session-1', 'model', 'select', 'model-2'),
    {
      sessionId: 'session-1',
      configId: 'model',
      value: 'model-2',
    },
  );
  assert.deepEqual(
    createSetConfigOptionRequest('session-1', 'allow-all', 'boolean', true),
    {
      sessionId: 'session-1',
      configId: 'allow-all',
      type: 'boolean',
      value: true,
    },
  );
});

test('bundled default JSON is the canonical zero-config source', async () => {
  const path = fileURLToPath(new URL('../ahp-acp-gateway.default.json', import.meta.url));
  const raw = JSON.parse(await readFile(path, 'utf8')) as unknown;
  assert.deepEqual(validateConfig(raw, path), defaultConfig());
});

test('example config is a valid minimal overlay', async () => {
  const path = fileURLToPath(new URL('../ahp-acp-gateway.example.json', import.meta.url));
  const raw = JSON.parse(await readFile(path, 'utf8')) as unknown;
  const config = validateConfig(raw, path);
  assert.equal(config.listener.port, 9876);
  assert.equal(config.providers.some((provider) => provider.id === 'gemini'), false);
  assert.deepEqual(
    config.providers.find((provider) => provider.id === 'codex')?.args,
    ['--yes', '@agentclientprotocol/codex-acp@2.2.2'],
  );
  assert.equal(config.providers.find((provider) => provider.id === 'codex')?.command, 'npx');
  assert.equal(config.providers.find((provider) => provider.id === 'my-agent')?.command, 'my-agent');
});

test('zero-config defaults advertise safe built-ins without starting providers', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'ahp-acp-gateway-defaults-'));
  const config = defaultConfig({ homeDir: directory });
  assert.equal(
    config.database.path,
    join(directory, '.ahp-acp-gateway', 'ahp-acp-gateway.db'),
  );
  config.listener.port = 0;
  config.database.path = join(directory, 'gateway.db');
  assert.equal(config.providers.length, 13);
  assert.equal(config.listener.allowAnyOrigin, true);
  assert.deepEqual(
    config.providers.map((provider) => provider.id),
    [
      'copilot',
      'claude',
      'gemini',
      'qwen',
      'auggie',
      'qoder',
      'codex',
      'opencode',
      'openclaw',
      'kiro',
      'hermes',
      'kimi',
      'pi',
    ],
  );
  for (const provider of config.providers) {
    assert.ok(!provider.args.some((arg) => /yolo|auto.?approve/i.test(arg)));
    if (provider.command === 'npx') {
      assert.equal(provider.command, 'npx');
      assert.equal(provider.args[0], '--yes');
      assert.doesNotMatch(provider.args[1] ?? '', /@latest$/);
      assert.equal(provider.startupTimeoutMs, 120_000);
    }
  }
  const claude = config.providers.find((provider) => provider.id === 'claude')!;
  claude.command = process.execPath;
  claude.args = ['--import', 'tsx', fakeAgent];
  claude.startupTimeoutMs = 10_000;

  const server = new GatewayServer(config);
  const address = await server.listen();
  try {
    assert.equal(server.adapters.size, 0);
    const root = server.store.getSnapshot('ahp-root://')?.state;
    assert.ok(root && 'agents' in root);
    assert.equal(root.agents.length, 13);
    assert.deepEqual(root.agents[0]?.models, [{
      id: 'default',
      provider: 'copilot',
      name: 'Agent default',
    }]);
    const response = await fetch(`http://${address.host}:${address.port}/healthz`);
    assert.equal(response.status, 200);

    const client = await connectClient(
      `ws://${address.host}:${address.port}${address.path}`,
      'provider-selection-client',
    );
    const session = `ahp-session:/provider-selection-${crypto.randomUUID()}`;
    await client.request('createSession', {
      channel: session,
      provider: 'claude',
      workingDirectories: [pathToFileURL(process.cwd()).href],
    });
    await waitForCondition(() =>
      server.store.getSession(session)?.state.lifecycle === 'ready');
    const sessionState = (await client.subscribe(session)).result.snapshot?.state;
    assert.ok(sessionState && 'provider' in sessionState);
    assert.equal(sessionState.provider, 'claude');
    assert.equal(sessionState.config?.schema.properties.model, undefined);
    const updatedRoot = server.store.getSnapshot('ahp-root://')?.state;
    assert.ok(updatedRoot && 'agents' in updatedRoot);
    assert.deepEqual(
      updatedRoot.agents.find((agent) => agent.provider === 'claude')?.models.map((model) => model.id),
      ['fake-model', 'fake-model-2'],
    );
    assert.equal(server.adapters.size, 1);
    await client.shutdown();
  } finally {
    await server.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test('provider overlays merge, override, add, and disable by id', () => {
  const config = validateConfig({
    version: 1,
    listener: { port: 9999 },
    providers: [
      { id: 'gemini', enabled: false },
      {
        id: 'codex',
        args: ['--yes', '@agentclientprotocol/codex-acp@2.2.2'],
        env: { CODEX_PATH: '/custom/codex' },
      },
      {
        id: 'custom',
        displayName: 'Custom Agent',
        description: 'Local test agent',
        command: 'custom-agent',
        args: ['acp'],
      },
    ],
  });

  assert.equal(config.listener.port, 9999);
  assert.equal(config.providers.some((provider) => provider.id === 'gemini'), false);
  assert.deepEqual(
    config.providers.find((provider) => provider.id === 'codex')?.args,
    ['--yes', '@agentclientprotocol/codex-acp@2.2.2'],
  );
  assert.equal(
    config.providers.find((provider) => provider.id === 'codex')?.env?.CODEX_PATH,
    '/custom/codex',
  );
  assert.equal(config.providers.find((provider) => provider.id === 'custom')?.command, 'custom-agent');
  assert.equal(config.providers.length, 13);
});

test('missing provider commands surface installation guidance', async (t) => {
  const fixture = await startFixture();
  t.after(fixture.close);
  const missing = fixture.server.config.providers[0]!;
  missing.command = 'definitely-missing-ahp-acp-provider';
  missing.args = [];
  missing.installHint = 'Install the test provider first.';
  missing.startupTimeoutMs = 1_000;
  const client = await connectClient(fixture.url, 'missing-provider-client');
  t.after(() => client.shutdown());
  const failedSession = `ahp-session:/missing-provider-${crypto.randomUUID()}`;
  await assert.rejects(
    client.request('createSession', {
      channel: failedSession,
      provider: 'fake',
      workingDirectories: [pathToFileURL(process.cwd()).href],
    }),
    /executable "definitely-missing-ahp-acp-provider" was not found on PATH.*Install the test provider first/,
  );
  assert.equal(
    fixture.server.store.getSession(failedSession)?.state.lifecycle,
    'failed',
  );
  await waitForCondition(() => fixture.server.adapters.size === 0);

  missing.command = process.execPath;
  missing.args = ['--import', 'tsx', fakeAgent];
  missing.startupTimeoutMs = 10_000;
  const retried = await createSession(client, 'provider-retry');
  assert.equal(fixture.server.store.getSession(retried.session)?.state.lifecycle, 'ready');
});

test('provider authentication failures show login guidance instead of install guidance', async (t) => {
  const fixture = await startFixture();
  t.after(fixture.close);
  const provider = fixture.server.config.providers[0]!;
  const client = await connectClient(fixture.url, 'auth-required-client');
  t.after(() => client.shutdown());

  // 1. Default automatic guidance when authHint is NOT configured and no authMethods are advertised
  delete provider.authHint;
  provider.args = ['--import', 'tsx', fakeAgent, '--auth-required'];
  await assert.rejects(
    client.request('createSession', {
      channel: `ahp-session:/auth-default-${crypto.randomUUID()}`,
      provider: 'fake',
      workingDirectories: [pathToFileURL(process.cwd()).href],
    }),
    /requires authentication\. Sign in with ".*" in a terminal or configure the required API key\/credentials/,
  );

  // 2. Automatic guidance using ACP InitializeResponse.authMethods when authHint is NOT configured
  provider.args = ['--import', 'tsx', fakeAgent, '--auth-required', '--auth-methods'];
  await assert.rejects(
    client.request('createSession', {
      channel: `ahp-session:/auth-methods-${crypto.randomUUID()}`,
      provider: 'fake',
      workingDirectories: [pathToFileURL(process.cwd()).href],
    }),
    /requires authentication\. Sign in or configure credentials for Fake ACP \(available auth methods: OAuth Login \(Browser sign-in\)\)/,
  );

  // 3. Explicit authHint override when configured
  provider.args = ['--import', 'tsx', fakeAgent, '--auth-required'];
  provider.authHint = 'Run the provider login command.';
  await assert.rejects(
    client.request('createSession', {
      channel: `ahp-session:/auth-required-${crypto.randomUUID()}`,
      provider: 'fake',
      workingDirectories: [pathToFileURL(process.cwd()).href],
    }),
    /requires authentication.*Run the provider login command/,
  );

  // 4. Stderr-based authentication exit before JSON-RPC response (without authHint)
  delete provider.authHint;
  provider.args = ['-e', 'process.stderr.write("Error: missing API_KEY / please login\\n"); process.exit(1);'];
  await assert.rejects(
    client.request('createSession', {
      channel: `ahp-session:/auth-stderr-${crypto.randomUUID()}`,
      provider: 'fake',
      workingDirectories: [pathToFileURL(process.cwd()).href],
    }),
    /requires authentication\. Sign in with ".*" in a terminal or configure the required API key\/credentials/,
  );
});

test('ChatTurnStarted rejects missing or invalid startedAt and repairInterruptedTurns heals legacy corrupt rows', async (t) => {
  const fixture = await startFixture();
  t.after(fixture.close);
  const client = await connectClient(fixture.url, 'timestamp-client');
  t.after(() => client.shutdown());

  const { session, chat } = await createSession(client, 'timestamp-validation');
  const chatSub = (await client.subscribe(chat)).subscription;

  // Missing startedAt must be rejected without crashing or starting a turn
  client.dispatch(chat, {
    type: ActionType.ChatTurnStarted,
    turnId: 'bad-turn-1',
    message: { text: 'hello', origin: { kind: MessageKind.User } },
  } as unknown as StateAction);
  const rejectedMissing = await nextAction(chatSub, (item) => item.rejectionReason !== undefined);
  assert.match(rejectedMissing.rejectionReason ?? '', /startedAt must be a valid ISO timestamp/);
  assert.equal((fixture.server.store.getChat(chat)?.state as ChatState).activeTurn, undefined);

  // Invalid startedAt string must also be rejected
  client.dispatch(chat, {
    type: ActionType.ChatTurnStarted,
    turnId: 'bad-turn-2',
    startedAt: 'not-a-timestamp',
    message: { text: 'hello', origin: { kind: MessageKind.User } },
  } as unknown as StateAction);
  const rejectedInvalid = await nextAction(chatSub, (item) => item.rejectionReason !== undefined);
  assert.match(rejectedInvalid.rejectionReason ?? '', /startedAt must be a valid ISO timestamp/);

  // Empty turnId must be rejected
  client.dispatch(chat, {
    type: ActionType.ChatTurnStarted,
    turnId: '',
    startedAt: new Date().toISOString(),
    message: { text: 'hello', origin: { kind: MessageKind.User } },
  } as unknown as StateAction);
  const rejectedTurnId = await nextAction(chatSub, (item) => item.rejectionReason !== undefined);
  assert.match(rejectedTurnId.rejectionReason ?? '', /turnId must be a non-empty string/);

  // Simulate legacy corrupt activeTurn in SQLite (missing startedAt) and verify repairInterruptedTurns does not throw RangeError
  const stored = fixture.server.store.getChat(chat)!;
  stored.state.activeTurn = {
    id: 'corrupt-turn',
    startedAt: 'invalid-date',
    userMessage: { text: 'corrupt', origin: { kind: MessageKind.User } },
    responseParts: [],
    citations: [],
  } as any;
  fixture.server.store.putChat(stored);
  const repaired = fixture.server.store.repairInterruptedTurns();
  assert.equal(repaired.length, 1);
  assert.equal(fixture.server.store.getChat(chat)?.state.activeTurn, undefined);
  assert.equal(fixture.server.store.getSession(session)?.state.status, SessionStatus.Idle);
});

test('VS Code client-dispatchable actions and multi-client activeClients lifecycle work across concurrent clients', async (t) => {
  const fixture = await startFixture();
  t.after(fixture.close);
  const clientA = await connectClient(fixture.url, 'vscode-client-a');
  const clientB = await connectClient(fixture.url, 'vscode-client-b');
  t.after(() => clientA.shutdown());
  t.after(() => clientB.shutdown());

  // 1. RootConfigChanged (dispatched by VS Code on connect)
  const rootSubA = (await clientA.subscribe('ahp-root://')).subscription;
  clientA.dispatch('ahp-root://', {
    type: ActionType.RootConfigChanged,
    config: {
      'github.copilot.chat.claudeCode.models': [],
      'chat.autopilot.enabled': true,
    },
  } as StateAction);
  const rootAccepted = await nextAction(rootSubA, (item) => item.action.type === ActionType.RootConfigChanged);
  assert.equal(rootAccepted.rejectionReason, undefined);

  // 2. Session & Chat client-dispatchable actions + activeClients lifecycle
  const { session, chat } = await createSession(clientA, 'collab-session');
  const sessionSubA = (await clientA.subscribe(session)).subscription;
  const sessionSubB = (await clientB.subscribe(session)).subscription;
  const chatSubA = (await clientA.subscribe(chat)).subscription;

  clientA.dispatch(session, {
    type: ActionType.SessionActiveClientSet,
    activeClient: {
      clientId: 'vscode-client-a',
      tools: [{ name: 'vscode_get_diagnostics', description: 'Get diagnostics', inputSchema: { type: 'object' } }],
    },
  } as StateAction);
  await nextAction(sessionSubB, (item) => item.action.type === ActionType.SessionActiveClientSet);

  clientB.dispatch(session, {
    type: ActionType.SessionActiveClientSet,
    activeClient: {
      clientId: 'vscode-client-b',
      tools: [],
    },
  } as StateAction);
  await nextAction(sessionSubA, (item) =>
    item.action.type === ActionType.SessionActiveClientSet
    && (item.action as any).activeClient.clientId === 'vscode-client-b');

  // Spoofing another clientId must be rejected
  clientA.dispatch(session, {
    type: ActionType.SessionActiveClientRemoved,
    clientId: 'vscode-client-b',
  } as StateAction);
  const spoofRejected = await nextAction(sessionSubA, (item) => item.rejectionReason !== undefined);
  assert.match(spoofRejected.rejectionReason ?? '', /clientId must match connection clientId/);

  // SessionTitleChanged, SessionIsReadChanged, SessionIsArchivedChanged
  clientA.dispatch(session, {
    type: ActionType.SessionTitleChanged,
    title: 'Renamed Session',
  } as StateAction);
  await nextAction(sessionSubB, (item) => item.action.type === ActionType.SessionTitleChanged);
  assert.equal(fixture.server.store.getSession(session)?.state.title, 'Renamed Session');

  clientA.dispatch(session, {
    type: ActionType.SessionIsReadChanged,
    isRead: false,
  } as StateAction);
  await nextAction(sessionSubB, (item) => item.action.type === ActionType.SessionIsReadChanged);

  clientA.dispatch(session, {
    type: ActionType.SessionIsArchivedChanged,
    isArchived: true,
  } as StateAction);
  await nextAction(sessionSubB, (item) => item.action.type === ActionType.SessionIsArchivedChanged);

  // ChatDraftChanged & ChatTruncated
  clientA.dispatch(chat, {
    type: ActionType.ChatDraftChanged,
    draft: { text: 'work in progress' },
  } as StateAction);
  await nextAction(chatSubA, (item) => item.action.type === ActionType.ChatDraftChanged);
  assert.equal(fixture.server.store.getChat(chat)?.state.draft?.text, 'work in progress');

  // Disconnecting clientB automatically removes clientB from activeClients
  await clientB.shutdown();
  await nextAction(sessionSubA, (item) =>
    item.action.type === ActionType.SessionActiveClientRemoved
    && (item.action as any).clientId === 'vscode-client-b');
  const activeClients = (fixture.server.store.getSession(session)?.state as SessionState).activeClients;
  assert.deepEqual(activeClients.map((item) => item.clientId), ['vscode-client-a']);
});

test('interleaved markdown chunks and tool calls preserve chronological responseParts order', async (t) => {
  const fixture = await startFixture();
  t.after(fixture.close);
  const client = await connectClient(fixture.url, 'interleaved-client');
  t.after(() => client.shutdown());

  const { chat } = await createSession(client, 'interleaved');
  const chatSub = (await client.subscribe(chat)).subscription;
  const turnId = crypto.randomUUID();
  client.dispatch(chat, userTurn(turnId, 'interleaved-tools'));
  await collectUntil(chatSub, ActionType.ChatTurnComplete);

  const state = fixture.server.store.getChat(chat)?.state as ChatState;
  const turn = state.turns.find((item) => item.id === turnId);
  assert.ok(turn);
  assert.deepEqual(
    turn.responseParts.map((part) =>
      part.kind === 'markdown' ? `md:${part.content}` : part.kind),
    ['reasoning', 'md:before-tool', 'toolCall', 'md:after-tool'],
  );
});

test('restored sessions via ACP session/load retain configOptionTypes and syncChatSummary on model error', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'ahp-acp-gateway-load-config-'));

  const firstServer = new GatewayServer(testConfig(directory));
  const firstAddress = await firstServer.listen();
  const firstUrl = `ws://${firstAddress.host}:${firstAddress.port}${firstAddress.path}`;
  const firstClient = await connectClient(firstUrl, 'first-load-client');
  const { session, chat } = await createSession(firstClient, 'restorable-config');
  await firstClient.shutdown();
  await firstServer.close();

  const secondConfig = testConfig(directory);
  secondConfig.providers[0]!.env = {
    ...secondConfig.providers[0]!.env,
    ACP_REPLAY_ON_LOAD: '1',
  };
  const secondServer = new GatewayServer(secondConfig);
  const secondAddress = await secondServer.listen();
  const secondUrl = `ws://${secondAddress.host}:${secondAddress.port}${secondAddress.path}`;
  const secondClient = await connectClient(secondUrl, 'second-load-client');
  try {
    const chatSub = (await secondClient.subscribe(chat)).subscription;
    const sessionSub = (await secondClient.subscribe(session)).subscription;

    // Switching model on a restored session must succeed because configOptionTypes is restored
    const turnId = crypto.randomUUID();
    secondClient.dispatch(chat, {
      type: ActionType.ChatTurnStarted,
      turnId,
      startedAt: new Date().toISOString(),
      message: {
        text: 'report-model',
        origin: { kind: MessageKind.User },
        model: { id: 'fake-model-2', provider: 'fake' },
      },
    } as StateAction);
    const actions = await collectUntil(chatSub, ActionType.ChatTurnComplete);
    assert.ok(actions.some((item) =>
      item.action.type === ActionType.ChatDelta && item.action.content === 'model:fake-model-2'));
    assert.doesNotMatch(JSON.stringify(actions), /HISTORICAL_LOAD_REPLAY/);
    const restoredTurn = (secondServer.store.getChat(chat)?.state as ChatState)
      .turns.find((turn) => turn.id === turnId);
    assert.doesNotMatch(JSON.stringify(restoredTurn?.responseParts), /HISTORICAL_LOAD_REPLAY/);

    // SessionConfigChanged on restored session also works
    secondClient.dispatch(session, {
      type: ActionType.SessionConfigChanged,
      config: { mode: 'plan' },
    } as StateAction);
    const configAccepted = await nextAction(sessionSub, (item) => item.action.type === ActionType.SessionConfigChanged);
    assert.equal(configAccepted.rejectionReason, undefined);
  } finally {
    await secondClient.shutdown();
    await secondServer.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test('startup repairs changeset catalogs for sessions persisted by older versions', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'ahp-acp-legacy-changesets-'));
  const config = testConfig(directory);
  const store = new GatewayStore(config.database.path, config.replay.maxActions);
  const now = new Date().toISOString();
  const sessionResource = `ahp-session:/legacy-${crypto.randomUUID()}`;
  const chatResource = `ahp-chat:/legacy-${crypto.randomUUID()}`;
  const session = storedSession(sessionResource, chatResource, now);
  session.state.workingDirectories = [pathToFileURL(process.cwd()).href];
  store.putSession(session);
  store.putChat(storedChat(sessionResource, chatResource, now));
  store.close();

  const server = new GatewayServer(config);
  try {
    const repairedSession = server.store.getSession(sessionResource)?.state as SessionState;
    const repairedChat = server.store.getChat(chatResource)?.state as ChatState;
    assert.deepEqual(
      repairedSession.changesets?.map((changeset) => changeset.changeKind),
      ['session'],
    );
    assert.deepEqual(
      repairedChat.changesets?.map((changeset) => changeset.changeKind),
      ['uncommitted', 'session', 'turn'],
    );
    assert.ok(server.store.getChannel(`${chatResource}/changeset/uncommitted`));
    assert.ok(server.store.getChannel(`${sessionResource}/changeset/session`));
  } finally {
    await server.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test('Changeset channels, git-blob resourceRead, review state, discard-changes, and createResourceWatch work end-to-end', async (t) => {
  const fixture = await startFixture();
  const repoDir = await mkdtemp(join(tmpdir(), 'ahp-acp-git-repo-'));
  const client = await connectClient(fixture.url, 'changeset-client');
  t.after(async () => {
    await client.shutdown();
    await fixture.close();
    await rm(repoDir, { recursive: true, force: true });
  });

  await execFileAsync('git', ['init'], { cwd: repoDir });
  await execFileAsync('git', ['config', 'user.email', 'test@example.com'], { cwd: repoDir });
  await execFileAsync('git', ['config', 'user.name', 'Test User'], { cwd: repoDir });
  await execFileAsync('git', ['config', 'core.autocrlf', 'false'], { cwd: repoDir });
  const trackedFile = join(repoDir, 'hello.txt');
  await writeFile(trackedFile, 'line1\nline2\n', 'utf8');
  await execFileAsync('git', ['add', 'hello.txt'], { cwd: repoDir });
  await execFileAsync('git', ['commit', '-m', 'Initial commit'], { cwd: repoDir });

  const session = `ahp-session:/changeset-test-${crypto.randomUUID()}`;
  const repoUri = pathToFileURL(repoDir).href;
  await client.request('createSession', {
    channel: session,
    provider: 'fake',
    workingDirectories: [repoUri],
  });
  const sessionSnap = (await client.subscribe(session)).result.snapshot?.state as SessionState;
  assert.ok(sessionSnap.changesets && sessionSnap.changesets.length >= 1);
  const chat = sessionSnap.defaultChat!;
  const chatSnap = (await client.subscribe(chat)).result.snapshot?.state as ChatState;
  assert.ok(chatSnap.changesets && chatSnap.changesets.some((cs) => cs.changeKind === 'uncommitted'));

  // 1. Setup resourceWatch on the repo directory
  const watchRes = await client.request('createResourceWatch', {
    channel: 'ahp-root://',
    uri: repoUri,
    recursive: true,
    excludes: { items: ['**/.git/**'] },
  }) as { channel: string };
  assert.match(watchRes.channel, /^ahp-resource-watch:\/\/r\//);
  const watchSubscribed = await client.subscribe(watchRes.channel);
  const watchState = watchSubscribed.result.snapshot?.state as ResourceWatchState;
  assert.equal(watchState.recursive, true);

  // Modifying an existing file reports Updated.
  await writeFile(trackedFile, 'line1\nline2-modified\nline3\n', 'utf8');
  const updatedWatchAction = await nextAction(
    watchSubscribed.subscription,
    (item) =>
      item.action.type === ActionType.ResourceWatchChanged
      && item.action.changes.items.some((change) =>
        change.uri.endsWith('/hello.txt') && change.type === ResourceChangeType.Updated),
  );
  assert.ok(
    updatedWatchAction.action.type === ActionType.ResourceWatchChanged
    && updatedWatchAction.action.changes.items.some((change) =>
      change.uri.endsWith('/hello.txt') && change.type === ResourceChangeType.Updated),
  );

  // Creating and deleting a file report Added and Deleted.
  const untrackedFile = join(repoDir, 'added.txt');
  await writeFile(untrackedFile, 'new file content\n', 'utf8');
  await nextAction(
    watchSubscribed.subscription,
    (item) =>
      item.action.type === ActionType.ResourceWatchChanged
      && item.action.changes.items.some((change) =>
        change.uri.endsWith('/added.txt') && change.type === ResourceChangeType.Added),
  );
  await rm(untrackedFile);
  await nextAction(
    watchSubscribed.subscription,
    (item) =>
      item.action.type === ActionType.ResourceWatchChanged
      && item.action.changes.items.some((change) =>
        change.uri.endsWith('/added.txt') && change.type === ResourceChangeType.Deleted),
  );
  const secondUntrackedFile = join(repoDir, 'added2.txt');
  await writeFile(secondUntrackedFile, 'new file content\n', 'utf8');
  await nextAction(
    watchSubscribed.subscription,
    (item) =>
      item.action.type === ActionType.ResourceWatchChanged
      && item.action.changes.items.some((change) =>
        change.uri.endsWith('/added2.txt') && change.type === ResourceChangeType.Added),
  );

  // 2. Run a turn so changesets refresh automatically
  const chatSub = (await client.subscribe(chat)).subscription;
  const turnId = crypto.randomUUID();
  client.dispatch(chat, userTurn(turnId, 'hello'));
  await collectUntil(chatSub, ActionType.ChatTurnComplete);

  // 3. Subscribe to uncommitted changeset, session changeset, and turn changeset
  const uncommittedChannel = `${chat}/changeset/uncommitted`;
  const uncommittedSub = await client.subscribe(uncommittedChannel);
  await waitForCondition(() => {
    const cs = fixture.server.store.getChannel(uncommittedChannel) as ChangesetState | undefined;
    return cs?.status === ChangesetStatus.Ready && (cs.files?.length ?? 0) === 2;
  });
  const uncommittedState = fixture.server.store.getChannel(uncommittedChannel) as ChangesetState;
  assert.equal(uncommittedState.files.length, 2);
  assert.ok(uncommittedState.operations?.some((op) => op.id === 'discard-changes'));

  const modifiedEntry = uncommittedState.files.find((f) => f.id.endsWith('/hello.txt'));
  assert.ok(modifiedEntry?.edit.before?.content?.uri);
  assert.ok(modifiedEntry.edit.after?.content?.uri);
  assert.equal(modifiedEntry.edit.diff?.added, 2);
  assert.equal(modifiedEntry.edit.diff?.removed, 1);

  // 4. Read before content via git-blob: URI in resourceRead
  const beforeBlob = await client.request('resourceRead', {
    channel: 'ahp-root://',
    uri: modifiedEntry.edit.before.content.uri,
  });
  assert.equal(beforeBlob.data, 'line1\nline2\n');
  assert.equal(beforeBlob.encoding, ContentEncoding.Utf8);

  // 5. Mark file as reviewed via ChangesetFilesReviewChanged
  client.dispatch(uncommittedChannel, {
    type: ActionType.ChangesetFilesReviewChanged,
    files: [modifiedEntry.id],
    reviewed: true,
  } as StateAction);
  await nextAction(
    uncommittedSub.subscription,
    (item) => item.action.type === ActionType.ChangesetFilesReviewChanged,
  );
  const afterReview = fixture.server.store.getChannel(uncommittedChannel) as ChangesetState;
  assert.equal(afterReview.files.find((f) => f.id === modifiedEntry.id)?.reviewed, true);

  // 6. Verify Turn changeset subscription on demand
  const turnChangesetChannel = `${chat}/changeset/turn/${turnId}`;
  const turnChangesetSnap = (await client.subscribe(turnChangesetChannel)).result.snapshot?.state as ChangesetState;
  assert.equal(turnChangesetSnap.status, ChangesetStatus.Ready);
  assert.equal(turnChangesetSnap.files.length, 2);

  // 7. Invoke discard-changes operation on hello.txt
  await client.request('invokeChangesetOperation', {
    channel: uncommittedChannel,
    operationId: 'discard-changes',
    target: { kind: ChangesetOperationTargetKind.Resource, resource: modifiedEntry.id },
  });
  assert.equal(await readFile(trackedFile, 'utf8'), 'line1\nline2\n');
  const afterDiscard = fixture.server.store.getChannel(uncommittedChannel) as ChangesetState;
  assert.equal(afterDiscard.files.length, 1);
  assert.ok(afterDiscard.files[0]?.id.endsWith('/added2.txt'));

  // 8. Unsubscribe from resourceWatch cleans up the channel
  await client.unsubscribe(watchRes.channel);
  await waitForCondition(() => fixture.server.store.getChannel(watchRes.channel) === undefined);
  assert.equal(fixture.server.store.getChannel(watchRes.channel), undefined);
});

test('Changesets and git-blob reads stay inside a subdirectory working root', async (t) => {
  const fixture = await startFixture();
  const repoDir = await mkdtemp(join(tmpdir(), 'ahp-acp-subdir-repo-'));
  const client = await connectClient(fixture.url, 'changeset-subdir-client');
  t.after(async () => {
    await client.shutdown();
    await fixture.close();
    await rm(repoDir, { recursive: true, force: true });
  });

  const allowedDir = join(repoDir, 'allowed');
  const outsideDir = join(repoDir, 'outside');
  await mkdir(allowedDir);
  await mkdir(outsideDir);
  await execFileAsync('git', ['init'], { cwd: repoDir });
  await execFileAsync('git', ['config', 'user.email', 'test@example.com'], { cwd: repoDir });
  await execFileAsync('git', ['config', 'user.name', 'Test User'], { cwd: repoDir });
  await execFileAsync('git', ['config', 'core.autocrlf', 'false'], { cwd: repoDir });
  const insideFile = join(allowedDir, 'inside.txt');
  const outsideFile = join(outsideDir, 'secret.txt');
  await writeFile(insideFile, 'inside base\n', 'utf8');
  await writeFile(outsideFile, 'outside base\n', 'utf8');
  await execFileAsync('git', ['add', '.'], { cwd: repoDir });
  await execFileAsync('git', ['commit', '-m', 'Initial commit'], { cwd: repoDir });

  const session = `ahp-session:/changeset-subdir-${crypto.randomUUID()}`;
  await client.request('createSession', {
    channel: session,
    provider: 'fake',
    workingDirectories: [pathToFileURL(allowedDir).href],
  });
  const sessionState = (await client.subscribe(session)).result.snapshot?.state as SessionState;
  const chat = sessionState.defaultChat!;

  await writeFile(insideFile, 'inside changed\n', 'utf8');
  await writeFile(outsideFile, 'outside changed\n', 'utf8');

  const uncommittedChannel = `${chat}/changeset/uncommitted`;
  await client.subscribe(uncommittedChannel);
  await waitForCondition(() => {
    const state = fixture.server.store.getChannel(uncommittedChannel) as ChangesetState | undefined;
    return state?.status === ChangesetStatus.Ready && state.files.length > 0;
  });
  const changeset = fixture.server.store.getChannel(uncommittedChannel) as ChangesetState;
  assert.deepEqual(
    changeset.files.map((file) => file.id),
    [pathToFileURL(await realpath(insideFile)).href],
  );

  const outsideBlobUri = `git-blob:/outside/secret.txt?${encodeURIComponent(JSON.stringify({
    sessionUri: session,
    sha: 'HEAD',
    repoRelativePath: 'outside/secret.txt',
  }))}`;
  await assert.rejects(
    client.request('resourceRead', {
      channel: 'ahp-root://',
      uri: outsideBlobUri,
    }),
    /Permission denied/,
  );
});

test('VS Code UI tool confirmation without selectedOptionId and wrapped vscode-agent-host:// URIs work end-to-end', async (t) => {
  const fixture = await startFixture();
  const repoDir = await mkdtemp(join(tmpdir(), 'ahp-acp-vscode-ui-'));
  const client = await connectClient(fixture.url, 'vscode-ui-client');
  t.after(async () => {
    await client.shutdown();
    await fixture.close();
    await rm(repoDir, { recursive: true, force: true });
  });

  await execFileAsync('git', ['init'], { cwd: repoDir });
  await execFileAsync('git', ['config', 'user.email', 'test@example.com'], { cwd: repoDir });
  await execFileAsync('git', ['config', 'user.name', 'Test User'], { cwd: repoDir });
  await execFileAsync('git', ['config', 'core.autocrlf', 'false'], { cwd: repoDir });
  const trackedFile = join(repoDir, 'math.js');
  await writeFile(trackedFile, 'function add(a, b) { return a + b; }\n', 'utf8');
  await execFileAsync('git', ['add', 'math.js'], { cwd: repoDir });
  await execFileAsync('git', ['commit', '-m', 'Initial commit'], { cwd: repoDir });

  const session = `ahp-session:/vscode-ui-${crypto.randomUUID()}`;
  const repoUri = pathToFileURL(repoDir).href;
  await client.request('createSession', {
    channel: session,
    provider: 'fake',
    workingDirectories: [repoUri],
  });
  const sessionState = (await client.subscribe(session)).result.snapshot?.state as SessionState;
  const chat = sessionState.defaultChat!;
  const chatSub = (await client.subscribe(chat)).subscription;

  // 1. Trigger a tool call that requires confirmation, and confirm it WITHOUT selectedOptionId
  // (matching VS Code sidebar Allow button / allowAll / modifiedFilesConfirmation behavior).
  const turnId = crypto.randomUUID();
  client.dispatch(chat, userTurn(turnId, 'permission'));
  const ready = await nextAction(
    chatSub,
    (item) =>
      item.action.type === ActionType.ChatToolCallReady
      && Array.isArray((item.action as any).options)
      && (item.action as any).options.length > 0,
  );
  const toolCallId = (ready.action as any).toolCallId as string;

  client.dispatch(chat, {
    type: ActionType.ChatToolCallConfirmed,
    turnId,
    toolCallId,
    approved: true,
    confirmed: ToolCallConfirmationReason.UserAction,
  } as StateAction);

  const confirmed = await nextAction(
    chatSub,
    (item) => item.action.type === ActionType.ChatToolCallConfirmed && (item.action as any).toolCallId === toolCallId,
  );
  assert.equal((confirmed.action as any).selectedOptionId, 'allow');
  await collectUntil(chatSub, ActionType.ChatTurnComplete);

  // 2. Modify tracked file and verify wrapped vscode-agent-host:// URI works in invokeChangesetOperation
  await writeFile(trackedFile, 'function add(a, b) { return a + b; }\nfunction mul(a, b) { return a * b; }\n', 'utf8');
  const uncommittedChannel = `${chat}/changeset/uncommitted`;
  await client.subscribe(uncommittedChannel);
  await client.request('createSession', {
    channel: `ahp-session:/refresh-${crypto.randomUUID()}`,
    provider: 'fake',
    workingDirectories: [repoUri],
  }).catch(() => {});

  // Run another quick turn to refresh changesets
  client.dispatch(chat, userTurn(crypto.randomUUID(), 'hello'));
  await collectUntil(chatSub, ActionType.ChatTurnComplete);

  const canonicalTrackedUri = pathToFileURL(await realpath(trackedFile)).href;
  const parsedTrackedUrl = new URL(canonicalTrackedUri);
  const ahPayload = Buffer.from(JSON.stringify({ scheme: 'file' }), 'utf8').toString('base64url');
  const wrappedResourceUri = `vscode-agent-host://hex-3132372e302e302e313a31383039302f616870${parsedTrackedUrl.pathname}?_ah=${ahPayload}`;

  await client.request('invokeChangesetOperation', {
    channel: uncommittedChannel,
    operationId: 'discard-changes',
    target: {
      kind: ChangesetOperationTargetKind.Resource,
      resource: wrappedResourceUri,
    },
  });

  assert.equal(await readFile(trackedFile, 'utf8'), 'function add(a, b) { return a + b; }\n');
});

test('ACP native diff content produces fileEdit tool results, session-db resourceRead, and non-Git folder changesets with discard-changes', async (t) => {
  const fixture = await startFixture();
  const nonGitDir = await mkdtemp(join(tmpdir(), 'ahp-acp-non-git-'));
  const client = await connectClient(fixture.url, 'non-git-client');
  t.after(async () => {
    await client.shutdown();
    await fixture.close();
    await rm(nonGitDir, { recursive: true, force: true });
  });

  const existingFile = join(nonGitDir, 'notes.txt');
  const createdFile = join(nonGitDir, 'created.txt');
  await writeFile(existingFile, 'alpha\nbeta\n', 'utf8');

  const session = `ahp-session:/non-git-${crypto.randomUUID()}`;
  const nonGitUri = pathToFileURL(nonGitDir).href;
  await client.request('createSession', {
    channel: session,
    provider: 'fake',
    workingDirectories: [nonGitUri],
  });
  const sessionState = (await client.subscribe(session)).result.snapshot?.state as SessionState;
  const chat = sessionState.defaultChat!;
  const chatSub = (await client.subscribe(chat)).subscription;

  // 1. Edit an existing file via ACP diff-edit in a non-Git folder
  const turn1 = crypto.randomUUID();
  client.dispatch(
    chat,
    userTurn(
      turn1,
      `diff-edit:${JSON.stringify({
        path: existingFile,
        newText: 'alpha\nbeta-updated\ngamma\n',
        toolCallId: 'edit-existing-1',
      })}`,
    ),
  );
  const turn1Actions = await collectUntil(chatSub, ActionType.ChatTurnComplete);
  const completeAction = turn1Actions.find(
    (item) => item.action.type === ActionType.ChatToolCallComplete,
  )?.action as any;
  assert.ok(completeAction);
  assert.equal(completeAction.result.pastTenseMessage, `Edited ${existingFile}`);
  const fileEditItem = completeAction.result.content?.find((c: any) => c.type === 'fileEdit');
  assert.ok(fileEditItem);
  assert.equal(fileEditItem.diff.added, 2);
  assert.equal(fileEditItem.diff.removed, 1);
  assert.match(fileEditItem.before.content.uri, /^session-db:/);
  assert.match(fileEditItem.after.content.uri, /^session-db:/);

  // 2. Verify session-db: before and after snapshots can be read via resourceRead
  const beforeSnap = await client.request('resourceRead', {
    channel: 'ahp-root://',
    uri: fileEditItem.before.content.uri,
  });
  assert.equal(beforeSnap.data, 'alpha\nbeta\n');
  const afterSnap = await client.request('resourceRead', {
    channel: 'ahp-root://',
    uri: fileEditItem.after.content.uri,
  });
  assert.equal(afterSnap.data, 'alpha\nbeta-updated\ngamma\n');

  // 3. Create a brand new file via ACP diff-edit in the same non-Git folder
  const turn2 = crypto.randomUUID();
  client.dispatch(
    chat,
    userTurn(
      turn2,
      `diff-edit:${JSON.stringify({
        path: createdFile,
        newText: 'brand new line 1\nbrand new line 2\n',
        toolCallId: 'create-new-1',
      })}`,
    ),
  );
  await collectUntil(chatSub, ActionType.ChatTurnComplete);

  // 4. Verify non-Git uncommitted changeset and turn changeset
  const uncommittedChannel = `${chat}/changeset/uncommitted`;
  await client.subscribe(uncommittedChannel);
  await waitForCondition(() => {
    const cs = fixture.server.store.getChannel(uncommittedChannel) as ChangesetState | undefined;
    return cs?.status === ChangesetStatus.Ready && (cs.files?.length ?? 0) === 2;
  });
  const uncommittedState = fixture.server.store.getChannel(uncommittedChannel) as ChangesetState;
  assert.equal(uncommittedState.files.length, 2);

  const turn2ChangesetChannel = `${chat}/changeset/turn/${turn2}`;
  const turn2Changeset = (await client.subscribe(turn2ChangesetChannel)).result.snapshot?.state as ChangesetState;
  assert.equal(turn2Changeset.files.length, 1);
  assert.ok(turn2Changeset.files[0]?.id.endsWith('/created.txt'));
  assert.equal(turn2Changeset.files[0]?.edit.before, undefined);

  // 5. Discard changes on the modified file in the non-Git folder -> restores original content
  const existingEntry = uncommittedState.files.find((f) => f.id.endsWith('/notes.txt'))!;
  await client.request('invokeChangesetOperation', {
    channel: uncommittedChannel,
    operationId: 'discard-changes',
    target: { kind: ChangesetOperationTargetKind.Resource, resource: existingEntry.id },
  });
  assert.equal(await readFile(existingFile, 'utf8'), 'alpha\nbeta\n');

  // 6. Discard changes on the newly created file in the non-Git folder -> deletes the file
  const createdEntry = uncommittedState.files.find((f) => f.id.endsWith('/created.txt'))!;
  await client.request('invokeChangesetOperation', {
    channel: uncommittedChannel,
    operationId: 'discard-changes',
    target: { kind: ChangesetOperationTargetKind.Resource, resource: createdEntry.id },
  });
  await assert.rejects(readFile(createdFile, 'utf8'));

  const finalUncommitted = fixture.server.store.getChannel(uncommittedChannel) as ChangesetState;
  assert.equal(finalUncommitted.files.length, 0);
});

test('Message attachments (resource file/selection/image, embeddedResource image/text, simple, chat) translate into ACP prompt ContentBlocks and respect path boundaries', async (t) => {
  const fixture = await startFixture();
  const workDir = await mkdtemp(join(tmpdir(), 'ahp-acp-attach-work-'));
  const outsideDir = await mkdtemp(join(tmpdir(), 'ahp-acp-attach-outside-'));
  const client = await connectClient(fixture.url, 'attach-client');
  t.after(async () => {
    await client.shutdown();
    await fixture.close();
    await rm(workDir, { recursive: true, force: true });
    await rm(outsideDir, { recursive: true, force: true });
  });

  const codeFile = join(workDir, 'sample.ts');
  await writeFile(codeFile, 'const line1 = 1;\nconst line2 = 2;\nconst line3 = 3;\n', 'utf8');
  const outsideFile = join(outsideDir, 'secret.txt');
  await writeFile(outsideFile, 'TOP_SECRET_OUTSIDE_CONTENT\n', 'utf8');

  const session = `ahp-session:/attach-${crypto.randomUUID()}`;
  const workDirUri = pathToFileURL(workDir).href;
  await client.request('createSession', {
    channel: session,
    provider: 'fake',
    workingDirectories: [workDirUri],
  });
  const sessionState = (await client.subscribe(session)).result.snapshot?.state as SessionState;
  const chat = sessionState.defaultChat!;
  const chatSub = (await client.subscribe(chat)).subscription;

  // Run an initial turn so we can also test Chat attachment referencing a prior turn
  const seedTurnId = crypto.randomUUID();
  client.dispatch(chat, userTurn(seedTurnId, 'seed message'));
  await collectUntil(chatSub, ActionType.ChatTurnComplete);

  const canonicalCodeUri = pathToFileURL(await realpath(codeFile)).href;
  const parsedCodeUrl = new URL(canonicalCodeUri);
  const ahPayload = Buffer.from(JSON.stringify({ scheme: 'file' }), 'utf8').toString('base64url');
  const wrappedCodeUri = `vscode-agent-host://hex-3132372e302e302e313a31383039302f616870${parsedCodeUrl.pathname}?_ah=${ahPayload}`;

  const pngBase64 = Buffer.from('fake-png-bytes').toString('base64');
  const pastedTextBase64 = Buffer.from('pasted snippet line', 'utf8').toString('base64');

  const turnId = crypto.randomUUID();
  client.dispatch(chat, {
    type: ActionType.ChatTurnStarted,
    turnId,
    startedAt: new Date().toISOString(),
    message: {
      text: 'inspect-prompt',
      origin: { kind: MessageKind.User },
      attachments: [
        {
          type: MessageAttachmentKind.Resource,
          label: 'sample.ts',
          displayKind: 'selection',
          uri: wrappedCodeUri,
          selection: {
            range: {
              start: { line: 1, character: 0 },
              end: { line: 1, character: 16 },
            },
          },
        },
        {
          type: MessageAttachmentKind.Resource,
          label: 'secret.txt',
          displayKind: 'document',
          uri: pathToFileURL(outsideFile).href,
        },
        {
          type: MessageAttachmentKind.EmbeddedResource,
          label: 'screenshot.png',
          displayKind: 'image',
          contentType: 'image/png',
          data: pngBase64,
        },
        {
          type: MessageAttachmentKind.EmbeddedResource,
          label: 'unsaved.txt',
          displayKind: 'document',
          contentType: 'text/plain',
          data: pastedTextBase64,
        },
        {
          type: MessageAttachmentKind.Simple,
          label: 'Workspace context',
          modelRepresentation: 'Workspace has 1 file.',
        },
        {
          type: MessageAttachmentKind.Chat,
          label: 'Prior Chat',
          resource: chat,
          endTurn: seedTurnId,
        },
      ],
    },
  } as StateAction);

  await collectUntil(chatSub, ActionType.ChatTurnComplete);
  const finalChat = (await client.subscribe(chat)).result.snapshot?.state as ChatState;
  const turn = finalChat.turns.find((item) => item.id === turnId)!;
  const markdownText = turn.responseParts
    .flatMap((part) => part.kind === 'markdown' ? [part.content] : [])
    .join('');

  assert.match(markdownText, /^prompt-blocks:/);
  const promptBlocks = JSON.parse(markdownText.slice('prompt-blocks:'.length)) as any[];

  // 1. Wrapped file URI inside workspace with selection -> embedded resource with line 2 only and #L2 fragment
  const codeBlock = promptBlocks.find(
    (b) => b.type === 'resource' && typeof b.resource?.uri === 'string' && b.resource.uri.endsWith('/sample.ts#L2'),
  );
  assert.ok(codeBlock);
  assert.equal(codeBlock.resource.text, 'const line2 = 2;');

  // 2. Outside file is rejected by path boundary check
  assert.equal(
    promptBlocks.some((b) => JSON.stringify(b).includes('TOP_SECRET_OUTSIDE_CONTENT') || JSON.stringify(b).includes('secret.txt')),
    false,
  );

  // 3. Embedded image -> ACP image block
  const imageBlock = promptBlocks.find((b) => b.type === 'image');
  assert.ok(imageBlock);
  assert.equal(imageBlock.mimeType, 'image/png');
  assert.equal(imageBlock.data, pngBase64);

  // 4. Embedded text -> ACP resource block
  const unsavedBlock = promptBlocks.find(
    (b) => b.type === 'resource' && b.resource?.uri === 'attachment:///unsaved.txt',
  );
  assert.ok(unsavedBlock);
  assert.equal(unsavedBlock.resource.text, 'pasted snippet line');

  // 5. Simple attachment -> text block
  const simpleBlock = promptBlocks.find(
    (b) => b.type === 'text' && typeof b.text === 'string' && b.text.includes('Workspace has 1 file.'),
  );
  assert.ok(simpleBlock);

  // 6. Chat attachment -> text block with prior transcript
  const chatBlock = promptBlocks.find(
    (b) => b.type === 'text' && typeof b.text === 'string' && b.text.includes('User: seed message'),
  );
  assert.ok(chatBlock);

  // 7. User message text is preserved at the end
  assert.deepEqual(promptBlocks[promptBlocks.length - 1], {
    type: 'text',
    text: 'inspect-prompt',
  });

  // 8. Verify capability fallback when agent does not advertise image/embeddedContext
  const downgraded = adaptPromptBlocksToCapabilities(promptBlocks, {
    image: false,
    audio: false,
    embeddedContext: false,
  });
  assert.equal(downgraded.some((b) => b.type === 'image' || b.type === 'resource'), false);
  assert.ok(downgraded.some((b) => b.type === 'text' && b.text.includes('const line2 = 2;')));
  assert.ok(downgraded.some((b) => b.type === 'text' && b.text.includes('[Attached image (image/png)]')));
});

test('Attachment unit helpers slice text selections and adapt prompt ContentBlocks across capability matrices', () => {
  const sample = 'alpha-0123\nbeta-4567\ngamma-89';

  assert.equal(sliceTextBySelection(sample, undefined), sample);
  assert.equal(formatSelectionRangeFragment(undefined), '');

  // Single-line character range
  assert.equal(
    sliceTextBySelection(sample, {
      range: {
        start: { line: 1, character: 5 },
        end: { line: 1, character: 9 },
      },
    }),
    '4567',
  );
  assert.equal(
    formatSelectionRangeFragment({
      range: {
        start: { line: 1, character: 5 },
        end: { line: 1, character: 9 },
      },
    }),
    '#L2',
  );

  // Multi-line selection range
  assert.equal(
    sliceTextBySelection(sample, {
      range: {
        start: { line: 0, character: 6 },
        end: { line: 2, character: 5 },
      },
    }),
    '0123\nbeta-4567\ngamma',
  );
  assert.equal(
    formatSelectionRangeFragment({
      range: {
        start: { line: 0, character: 6 },
        end: { line: 2, character: 5 },
      },
    }),
    '#L1-L3',
  );

  // Capability matrix: empty blocks fallback, audio fallback, and binary resource_link fallback
  assert.deepEqual(adaptPromptBlocksToCapabilities([], {}), [{ type: 'text', text: '' }]);

  const adapted = adaptPromptBlocksToCapabilities(
    [
      { type: 'audio', data: 'AAAA', mimeType: 'audio/wav' },
      {
        type: 'resource',
        resource: {
          uri: 'file:///workspace/archive.bin',
          mimeType: 'application/octet-stream',
          blob: 'AAAA',
        },
      },
      {
        type: 'resource',
        resource: {
          uri: 'attachment:///raw.bin',
          mimeType: 'application/octet-stream',
          blob: 'BBBB',
        },
      },
    ],
    { image: false, audio: false, embeddedContext: false },
  );

  assert.deepEqual(adapted, [
    { type: 'text', text: '[Attached audio (audio/wav)]' },
    {
      type: 'resource_link',
      uri: 'file:///workspace/archive.bin',
      name: 'archive.bin',
      mimeType: 'application/octet-stream',
    },
    { type: 'text', text: '[Attached binary resource: attachment:///raw.bin]' },
  ]);
});

function storedSession(resource: string, chat: string, now: string) {
  return {
    resource,
    provider: 'fake',
    createdAt: now,
    modifiedAt: now,
    state: {
      provider: 'fake',
      title: 'Atomic',
      status: SessionStatus.Idle,
      lifecycle: 'ready',
      activeClients: [],
      chats: [{ resource: chat, title: 'Chat', status: SessionStatus.Idle, modifiedAt: now }],
      defaultChat: chat,
    } as any,
  };
}

function storedChat(session: string, resource: string, now: string) {
  return {
    resource,
    session,
    state: {
      resource,
      title: 'Chat',
      status: SessionStatus.Idle,
      modifiedAt: now,
      turns: [],
    },
    binding: { provider: 'fake' },
  };
}

async function startFixture(replayLimit = 100) {
  const directory = await mkdtemp(join(tmpdir(), 'ahp-acp-gateway-test-'));
  const server = new GatewayServer(testConfig(directory, replayLimit));
  const address = await server.listen();
  const url = `ws://${address.host}:${address.port}${address.path}`;
  return {
    server,
    url,
    directory,
    close: async () => {
      await server.close();
      await rm(directory, { recursive: true, force: true });
    },
  };
}

function testConfig(directory: string, replayLimit = 100): GatewayConfig {
  return {
    version: 1,
    listener: {
      host: '127.0.0.1',
      port: 0,
      path: '/ahp',
      tlsTerminated: false,
      allowedOrigins: [],
      allowAnyOrigin: false,
    },
    auth: {},
    database: { path: join(directory, 'gateway.db') },
    replay: { maxActions: replayLimit },
    providerDefaults: {
      startupTimeoutMs: 15_000,
      requestTimeoutMs: 300_000,
      shutdownTimeoutMs: 5_000,
      stderrTailBytes: 16_384,
    },
    providers: [{
      id: 'fake',
      displayName: 'Fake ACP',
      description: 'Official ACP SDK test agent',
      command: process.execPath,
      args: ['--import', 'tsx', fakeAgent],
      startupTimeoutMs: 10_000,
      requestTimeoutMs: 10_000,
      shutdownTimeoutMs: 2_000,
      stderrTailBytes: 8_192,
    }],
  };
}

async function connectClient(url: string, id: string): Promise<AhpClient> {
  const client = new AhpClient(await WebSocketTransport.connect(url), { requestTimeoutMs: 10_000 });
  client.connect();
  await client.initialize({
    clientId: id,
    protocolVersions: [PROTOCOL_VERSION],
    initialSubscriptions: ['ahp-root://'],
  });
  return client;
}

async function createSession(client: AhpClient, name: string): Promise<{ session: string; chat: string }> {
  const session = `ahp-session:/${name}-${crypto.randomUUID()}`;
  await client.request('createSession', {
    channel: session,
    provider: 'fake',
    workingDirectories: [pathToFileURL(process.cwd()).href],
  });
  const subscribed = await client.subscribe(session);
  const state = subscribed.result.snapshot?.state;
  assert.ok(state && 'chats' in state);
  const chat = state.defaultChat;
  assert.ok(chat);
  return { session, chat };
}

function userTurn(turnId: string, text: string): StateAction {
  return {
    type: ActionType.ChatTurnStarted,
    turnId,
    startedAt: new Date().toISOString(),
    message: {
      text,
      origin: { kind: MessageKind.User },
    },
  } as StateAction;
}

async function collectUntil(subscription: Subscription, type: ActionType): Promise<ActionEnvelope[]> {
  const actions: ActionEnvelope[] = [];
  for (;;) {
    const action = await nextAction(subscription, () => true);
    actions.push(action);
    if (action.action.type === type) return actions;
  }
}

async function nextAction(
  subscription: Subscription,
  predicate: (envelope: ActionEnvelope) => boolean,
): Promise<ActionEnvelope> {
  const timeout = AbortSignal.timeout(15_000);
  while (!timeout.aborted) {
    const result = await Promise.race([
      subscription.next(),
      new Promise<never>((_, reject) => {
        timeout.addEventListener('abort', () => reject(new Error('Timed out waiting for AHP action')), { once: true });
      }),
    ]);
    if (result.done) throw new Error('Subscription closed');
    if (result.value.type === 'action' && predicate(result.value.params)) return result.value.params;
  }
  throw new Error('Timed out waiting for AHP action');
}

function connectRawWebSocket(url: string, origin: string): Promise<WebSocket> {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(url, { origin });
    socket.once('open', () => resolve(socket));
    socket.once('unexpected-response', (_request, response) => {
      reject(new Error(`HTTP ${response.statusCode}`));
      response.resume();
    });
    socket.once('error', reject);
  });
}

async function waitForCondition(predicate: () => boolean, timeoutMs = 5_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error('Timed out waiting for condition');
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}
