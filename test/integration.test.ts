import assert from 'node:assert/strict';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { isAbsolute, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import test from 'node:test';
import WebSocket from 'ws';
import {
  ActionType,
  CompletionItemKind,
  MessageKind,
  PROTOCOL_VERSION,
  SessionStatus,
  TerminalClaimKind,
  ToolCallConfirmationReason,
  type ActionEnvelope,
  type ChatState,
  type StateAction,
} from '@microsoft/agent-host-protocol';
import { AhpClient, type Subscription } from '@microsoft/agent-host-protocol/client';
import { WebSocketTransport } from '@microsoft/agent-host-protocol/ws';
import { GatewayServer } from '../src/gateway.js';
import { GatewayStore } from '../src/store.js';
import {
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
  await writeFile(join(fixture.directory, 'readme.txt'), 'test');
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
    listed.entries.find((entry) => entry.name === 'readme.txt'),
    { name: 'readme.txt', type: 'file' },
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
  });
  const snapshot = (await client.subscribe(session)).result.snapshot?.state;
  assert.ok(snapshot && 'provider' in snapshot);
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
  await assert.rejects(
    client.request('resourceRead', { channel: 'ahp-root://', uri: 'file:///nope' }),
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
    /Install the test provider first/,
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
  provider.args = ['--import', 'tsx', fakeAgent, '--auth-required'];
  provider.authHint = 'Run the provider login command.';
  const client = await connectClient(fixture.url, 'auth-required-client');
  t.after(() => client.shutdown());
  await assert.rejects(
    client.request('createSession', {
      channel: `ahp-session:/auth-required-${crypto.randomUUID()}`,
      provider: 'fake',
      workingDirectories: [pathToFileURL(process.cwd()).href],
    }),
    /requires authentication.*Run the provider login command/,
  );
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
