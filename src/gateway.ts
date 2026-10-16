import { timingSafeEqual } from 'node:crypto';
import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { lstat, readFile, readdir, realpath, stat } from 'node:fs/promises';
import { createServer, type IncomingMessage, type Server } from 'node:http';
import { extname, isAbsolute, relative } from 'node:path';
import { pathToFileURL } from 'node:url';
import {
  ActionType,
  ChatOriginKind,
  ConfirmationOptionKind,
  MessageKind,
  SUPPORTED_PROTOCOL_VERSIONS,
  SessionLifecycle,
  SessionStatus,
  TerminalLifecycleStatus,
  ToolCallCancellationReason,
  ToolCallConfirmationReason,
  ToolCallStatus,
  negotiateProtocolVersion,
  type ActionEnvelope,
  type ChatState,
  type SessionState,
  type StateAction,
} from '@microsoft/agent-host-protocol';
import { WebSocketServer, WebSocket, type RawData } from 'ws';
import type { GatewayConfig, ProviderConfig } from './config.js';
import {
  AcpV1AdapterFactory,
  toNativeWorkingDirectory,
  type AcpAdapter,
  type AcpAdapterFactory,
} from './acp.js';
import { GatewayStore, type AcpBinding, type StoredSession } from './store.js';
import { VERSION } from './version.js';

interface JsonRpcRequest {
  jsonrpc: '2.0';
  id?: string | number | null;
  method: string;
  params?: unknown;
}

interface Connection {
  socket: WebSocket;
  initialized: boolean;
  clientId?: string;
  subscriptions: Set<string>;
}

export class GatewayServer {
  readonly config: GatewayConfig;
  readonly store: GatewayStore;
  readonly adapters = new Map<string, AcpAdapter>();
  readonly terminals = new Map<string, ChildProcessWithoutNullStreams>();
  readonly commands = new Map<string, Array<{ name: string; description: string; argumentHint?: string }>>();
  readonly providerSessionConfigs = new Map<string, {
    schema: { type: 'object'; properties: Record<string, Record<string, unknown>> };
    values: Record<string, unknown>;
  }>();
  private readonly adapterFactory: AcpAdapterFactory;
  private readonly connections = new Set<Connection>();
  private readonly httpServer: Server;
  private readonly websocketServer: WebSocketServer;
  private closing = false;

  constructor(config: GatewayConfig, adapterFactory: AcpAdapterFactory = new AcpV1AdapterFactory()) {
    this.config = config;
    this.adapterFactory = adapterFactory;
    this.store = new GatewayStore(config.database.path, config.replay.maxActions);
    this.store.clearTerminals();
    this.store.ensureRoot({
      agents: config.providers.map(providerInfo),
      activeSessions: this.store.sessionCount(),
      terminals: [],
      _meta: {
        product: 'AHP-ACP Gateway',
        acpVersion: 1,
        unsupported: [
          'resourceWrites',
          'terminalPty',
          'terminalCommandDetection',
          'changesets',
          'attachments',
          'multipleChats',
          'acpV2',
        ],
      },
    });
    this.store.repairInterruptedTurns();
    this.httpServer = createServer((request, response) => {
      if (request.url === '/healthz') {
        response.writeHead(200, { 'content-type': 'application/json' });
        response.end(JSON.stringify({ ok: true, serverSeq: this.store.serverSeq }));
        return;
      }
      response.writeHead(404).end();
    });
    this.websocketServer = new WebSocketServer({ noServer: true, maxPayload: 16 * 1024 * 1024 });
    this.httpServer.on('upgrade', (request, socket, head) => {
      if (this.closing) {
        socket.write('HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\n');
        socket.destroy();
        return;
      }
      const url = new URL(request.url ?? '/', 'http://localhost');
      if (url.pathname !== config.listener.path) {
        socket.write('HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n');
        socket.destroy();
        return;
      }
      if (!this.originAllowed(request)) {
        socket.write('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n');
        socket.destroy();
        return;
      }
      if (!this.authorized(request, url)) {
        socket.write('HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n');
        socket.destroy();
        return;
      }
      this.websocketServer.handleUpgrade(request, socket, head, (ws) => {
        this.websocketServer.emit('connection', ws, request);
      });
    });
    this.websocketServer.on('connection', (socket) => this.accept(socket));
  }

  async listen(): Promise<{ host: string; port: number; path: string }> {
    await new Promise<void>((resolve, reject) => {
      this.httpServer.once('error', reject);
      this.httpServer.listen(this.config.listener.port, this.config.listener.host, () => {
        this.httpServer.off('error', reject);
        resolve();
      });
    });
    const address = this.httpServer.address();
    return {
      host: this.config.listener.host,
      port: typeof address === 'object' && address ? address.port : this.config.listener.port,
      path: this.config.listener.path,
    };
  }

  async close(): Promise<void> {
    if (this.closing) return;
    this.closing = true;
    const httpClosed = new Promise<void>((resolve, reject) => {
      this.httpServer.close((error) => {
        if (error && (error as NodeJS.ErrnoException).code !== 'ERR_SERVER_NOT_RUNNING') reject(error);
        else resolve();
      });
    });
    for (const connection of this.connections) connection.socket.close(1001, 'Server shutting down');
    for (const terminal of this.terminals.values()) terminal.kill();
    this.terminals.clear();
    await Promise.allSettled([...this.adapters.values()].map((adapter) => adapter.stop()));
    this.adapters.clear();
    await delay(250);
    for (const connection of this.connections) connection.socket.terminate();
    await new Promise<void>((resolve) => this.websocketServer.close(() => resolve()));
    await httpClosed;
    this.store.close();
  }

  private authorized(request: IncomingMessage, url: URL): boolean {
    const expected = this.config.auth.token;
    if (!expected) return true;
    const header = request.headers.authorization;
    const actual = header?.startsWith('Bearer ') ? header.slice(7) : url.searchParams.get('token');
    if (!actual) return false;
    const expectedBytes = Buffer.from(expected);
    const actualBytes = Buffer.from(actual);
    return expectedBytes.length === actualBytes.length && timingSafeEqual(expectedBytes, actualBytes);
  }

  private originAllowed(request: IncomingMessage): boolean {
    const origin = request.headers.origin;
    if (!origin) return true;
    if (this.config.listener.allowAnyOrigin) return true;
    return this.config.listener.allowedOrigins.includes(origin);
  }

  private accept(socket: WebSocket): void {
    const connection: Connection = {
      socket,
      initialized: false,
      subscriptions: new Set(),
    };
    this.connections.add(connection);
    socket.on('message', (data) => {
      void this.onMessage(connection, data);
    });
    socket.on('close', () => {
      this.connections.delete(connection);
    });
    socket.on('error', () => {
      this.connections.delete(connection);
    });
  }

  private async onMessage(connection: Connection, data: RawData): Promise<void> {
    let request: JsonRpcRequest;
    try {
      request = JSON.parse(data.toString()) as JsonRpcRequest;
      if (request.jsonrpc !== '2.0' || typeof request.method !== 'string') throw new Error('Invalid JSON-RPC request');
    } catch (error) {
      this.sendError(connection, null, -32700, asError(error).message);
      return;
    }

    try {
      if (!connection.initialized && request.method !== 'initialize' && request.method !== 'reconnect') {
        throw new RpcError(-32000, 'initialize or reconnect must be the first method');
      }
      const result = await this.handle(connection, request);
      if (request.id !== undefined) this.sendResult(connection, request.id, result);
    } catch (error) {
      if (request.id === undefined) return;
      const rpcError = error instanceof RpcError ? error : new RpcError(-32603, asError(error).message);
      this.sendError(connection, request.id, rpcError.code, rpcError.message, rpcError.data);
    }
  }

  private async handle(connection: Connection, request: JsonRpcRequest): Promise<unknown> {
    const params = asRecord(request.params);
    switch (request.method) {
      case 'initialize':
        return this.initialize(connection, params);
      case 'reconnect':
        return this.reconnect(connection, params);
      case 'ping':
        this.requireRoot(params);
        return null;
      case 'subscribe':
        return this.subscribe(connection, params);
      case 'unsubscribe':
        connection.subscriptions.delete(requireString(params.channel, 'channel'));
        return null;
      case 'listSessions':
        this.requireRoot(params);
        return this.listSessions(params);
      case 'resolveSessionConfig':
        return this.resolveSessionConfig(params);
      case 'resourceRead':
        return this.resourceRead(params);
      case 'resourceList':
        return this.resourceList(params);
      case 'resourceResolve':
        return this.resourceResolve(params);
      case 'completions':
        return this.completions(params);
      case 'createTerminal':
        return this.createTerminal(params);
      case 'disposeTerminal':
        return this.disposeTerminal(params);
      case 'createSession':
        return this.createSession(connection, params);
      case 'disposeSession':
        return this.disposeSession(params);
      case 'createChat':
        throw new RpcError(-32601, 'Multiple chats are not implemented; each session has one default chat');
      case 'disposeChat':
        return this.disposeChat(params);
      case 'fetchTurns':
        return this.fetchTurns(params);
      case 'dispatchAction':
        await this.dispatch(connection, params);
        return null;
      default:
        throw new RpcError(-32601, `Method not implemented: ${request.method}`);
    }
  }

  private initialize(connection: Connection, params: Record<string, unknown>): unknown {
    const clientId = requireString(params.clientId, 'clientId');
    const offered = requireStringArray(params.protocolVersions, 'protocolVersions');
    let protocolVersion: string | undefined;
    try {
      protocolVersion = negotiateProtocolVersion(offered);
    } catch (error) {
      throw new RpcError(-32602, asError(error).message);
    }
    if (!protocolVersion) {
      throw new RpcError(-32005, 'Unsupported protocol version', {
        supportedVersions: [...SUPPORTED_PROTOCOL_VERSIONS],
      });
    }
    connection.initialized = true;
    connection.clientId = clientId;
    const initialSubscriptions = optionalStringArray(params.initialSubscriptions);
    for (const channel of initialSubscriptions) {
      if (!this.store.getChannel(channel)) throw new RpcError(-32001, `Unknown channel ${channel}`);
      connection.subscriptions.add(channel);
    }
    return {
      protocolVersion,
      serverSeq: this.store.serverSeq,
      serverInfo: { name: 'AHP-ACP Gateway', version: VERSION },
      snapshots: initialSubscriptions.flatMap((channel) => {
        const snapshot = this.store.getSnapshot(channel);
        return snapshot ? [snapshot] : [];
      }),
      defaultDirectory: pathToFileURL(process.cwd()).href,
      completionTriggerCharacters: ['/'],
      _meta: {
        acpVersion: 1,
        processModel: 'per-chat',
      },
    };
  }

  private reconnect(connection: Connection, params: Record<string, unknown>): unknown {
    this.requireRoot(params);
    const clientId = requireString(params.clientId, 'clientId');
    const lastSeenServerSeq = requireInteger(params.lastSeenServerSeq, 'lastSeenServerSeq', 0);
    const requested = new Set(requireStringArray(params.subscriptions, 'subscriptions'));
    connection.initialized = true;
    connection.clientId = clientId;
    connection.subscriptions = new Set([...requested].filter((channel) => this.store.getChannel(channel)));
    const actions = this.store.actionsAfter(lastSeenServerSeq, connection.subscriptions);
    if (actions && actions.length > 0) {
      return {
        type: 'replay',
        actions,
        missing: [...requested].filter((channel) => !connection.subscriptions.has(channel)),
      };
    }
    return {
      type: 'snapshot',
      snapshots: [...connection.subscriptions].flatMap((channel) => {
        const snapshot = this.store.getSnapshot(channel);
        return snapshot ? [snapshot] : [];
      }),
    };
  }

  private subscribe(connection: Connection, params: Record<string, unknown>): unknown {
    const channel = requireString(params.channel, 'channel');
    if (params.flowControl !== undefined) {
      throw new RpcError(-32602, 'Windowed subscriptions are not implemented');
    }
    const snapshot = this.store.getSnapshot(channel);
    if (!snapshot) throw new RpcError(-32001, `Unknown channel ${channel}`);
    connection.subscriptions.add(channel);
    return { channel, snapshot };
  }

  private listSessions(params: Record<string, unknown>): unknown {
    const limit = params.limit === undefined ? 100 : requireInteger(params.limit, 'limit', 1, 500);
    const offset = params.cursor === undefined ? 0 : decodeCursor(requireString(params.cursor, 'cursor'));
    const sessions = this.store.listSessions(limit + 1, offset);
    const hasMore = sessions.length > limit;
    return {
      items: sessions.slice(0, limit).map((session) => summary(session)),
      ...(hasMore ? { nextCursor: encodeCursor(offset + limit) } : {}),
    };
  }

  private resolveSessionConfig(params: Record<string, unknown>): unknown {
    this.requireRoot(params);
    const provider = this.resolveProvider(params.provider);
    if (params.workingDirectory !== undefined) {
      requireWorkingDirectoryUri(requireString(params.workingDirectory, 'workingDirectory'));
    }
    const cached = this.providerSessionConfigs.get(provider.id);
    const values = {
      ...cached?.values,
      ...(params.config === undefined ? {} : asRecord(params.config)),
    };
    return {
      schema: cached?.schema ?? { type: 'object', properties: {} },
      values,
    };
  }

  private async resourceList(params: Record<string, unknown>): Promise<unknown> {
    this.requireRoot(params);
    const uri = requireString(params.uri, 'uri');
    const path = resourcePath(uri);
    try {
      const metadata = await stat(path);
      if (!metadata.isDirectory()) throw new RpcError(-32008, `Not a directory: ${uri}`);
      const entries = await readdir(path, { withFileTypes: true });
      return {
        entries: entries
          .filter((entry) => entry.isDirectory() || entry.isFile())
          .map((entry) => ({
            name: entry.name,
            type: entry.isDirectory() ? 'directory' : 'file',
          }))
          .sort((left, right) => left.name.localeCompare(right.name)),
      };
    } catch (error) {
      throw resourceError(error, uri);
    }
  }

  private async resourceRead(params: Record<string, unknown>): Promise<unknown> {
    this.requireRoot(params);
    const uri = requireString(params.uri, 'uri');
    const requestedEncoding = params.encoding;
    if (
      requestedEncoding !== undefined
      && requestedEncoding !== 'utf-8'
      && requestedEncoding !== 'base64'
    ) {
      throw new RpcError(-32602, 'encoding must be utf-8 or base64');
    }
    const path = resourcePath(uri);
    try {
      const canonicalPath = await realpath(path);
      const metadata = await stat(canonicalPath);
      if (!metadata.isFile()) throw new RpcError(-32008, `Not a file: ${uri}`);
      if (!await this.resourceReadAllowed(canonicalPath)) {
        throw new RpcError(-32009, `Permission denied: ${uri}`);
      }
      if (metadata.size > 16 * 1024 * 1024) {
        throw new RpcError(-32602, `Resource exceeds the 16 MiB read limit: ${uri}`);
      }
      const buffer = await readFile(canonicalPath);
      const encoding = requestedEncoding ?? (isUtf8(buffer) ? 'utf-8' : 'base64');
      return {
        data: encoding === 'utf-8' ? buffer.toString('utf8') : buffer.toString('base64'),
        encoding,
        contentType: contentType(canonicalPath),
      };
    } catch (error) {
      throw resourceError(error, uri);
    }
  }

  private async resourceReadAllowed(canonicalPath: string): Promise<boolean> {
    const roots = new Set<string>([process.cwd()]);
    for (const session of this.store.listSessions(10_000, 0)) {
      for (const uri of session.state.workingDirectories ?? []) {
        roots.add(resourcePath(uri));
      }
    }
    for (const root of roots) {
      try {
        if (isPathWithin(await realpath(root), canonicalPath)) return true;
      } catch {
        continue;
      }
    }
    return false;
  }

  private async resourceResolve(params: Record<string, unknown>): Promise<unknown> {
    this.requireRoot(params);
    const uri = requireString(params.uri, 'uri');
    const path = resourcePath(uri);
    const followSymlinks = params.followSymlinks !== false;
    try {
      const metadata = followSymlinks ? await stat(path) : await lstat(path);
      const canonicalPath = followSymlinks ? await realpath(path) : path;
      return {
        uri: pathToFileURL(canonicalPath).href,
        type: metadata.isSymbolicLink()
          ? 'symlink'
          : metadata.isDirectory() ? 'directory' : 'file',
        ...(metadata.isDirectory() ? {} : { size: metadata.size }),
        mtime: metadata.mtime.toISOString(),
        ctime: metadata.birthtime.toISOString(),
      };
    } catch (error) {
      throw resourceError(error, uri);
    }
  }

  private completions(params: Record<string, unknown>): unknown {
    const channel = requireString(params.channel, 'channel');
    if (!this.store.getChat(channel)) throw new RpcError(-32001, `Unknown chat ${channel}`);
    if (params.kind !== 'userMessage') return { items: [] };
    const text = requireStringValue(params.text, 'text');
    const offset = requireInteger(params.offset, 'offset', 0, text.length);
    const before = text.slice(0, offset);
    const match = /\/([\p{L}0-9_.:-]*)$/u.exec(before);
    if (!match) return { items: [] };
    const query = (match[1] ?? '').toLocaleLowerCase();
    const rangeStart = offset - match[0].length;
    const items = (this.commands.get(channel) ?? [])
      .filter((command) => command.name.toLocaleLowerCase().includes(query))
      .map((command) => {
        const takesInput = Boolean(command.argumentHint);
        return {
          insertText: `/${command.name}${takesInput ? ' ' : ''}`,
          rangeStart,
          rangeEnd: offset,
          attachment: {
            type: 'simple',
            label: command.description,
            _meta: {
              command: command.name,
              description: command.description,
              ...(command.argumentHint ? { argumentHint: command.argumentHint } : {}),
              ...(takesInput ? { retriggerSuggestions: true } : { submitOnAccept: true }),
            },
          },
        };
      });
    return { items };
  }

  private createTerminal(params: Record<string, unknown>): null {
    const channel = requireTerminalUri(params.channel);
    if (this.store.getChannel(channel)) throw new RpcError(-32003, `Terminal already exists: ${channel}`);
    const claim = asRecord(params.claim);
    const cwdUri = params.cwd === undefined
      ? pathToFileURL(process.cwd()).href
      : requireWorkingDirectoryUri(requireString(params.cwd, 'cwd'));
    const cwd = resourcePath(cwdUri);
    const title = params.name === undefined ? 'Agent Host Terminal' : requireString(params.name, 'name');
    const cols = params.cols === undefined ? 80 : requireInteger(params.cols, 'cols', 1);
    const rows = params.rows === undefined ? 24 : requireInteger(params.rows, 'rows', 1);
    const shell = process.platform === 'win32'
      ? process.env.ComSpec ?? 'cmd.exe'
      : process.env.SHELL ?? '/bin/sh';
    const child = spawn(shell, process.platform === 'win32' ? [] : ['-i'], {
      cwd,
      env: { ...process.env, TERM: 'xterm-256color' },
      stdio: ['pipe', 'pipe', 'pipe'],
      windowsHide: true,
    });
    const lifecycle = { status: TerminalLifecycleStatus.Running };
    const state = {
      title,
      cwd: cwdUri,
      cols,
      rows,
      content: [],
      lifecycle,
      claim,
      supportsCommandDetection: false,
      isPty: false,
    };
    const rootEnvelope = this.store.createTerminal(channel, state as never, {
      resource: channel,
      title,
      claim: claim as never,
      lifecycle,
    });
    this.terminals.set(channel, child);
    this.broadcastAction(rootEnvelope);
    child.stdout.setEncoding('utf8');
    child.stderr.setEncoding('utf8');
    child.stdout.on('data', (data: string) => this.emitTerminalData(channel, data));
    child.stderr.on('data', (data: string) => this.emitTerminalData(channel, data));
    child.once('error', (error) => this.emitTerminalData(channel, `${error.message}\r\n`));
    child.once('exit', (code) => this.terminalExited(channel, code));
    return null;
  }

  private disposeTerminal(params: Record<string, unknown>): null {
    const channel = requireTerminalUri(params.channel);
    if (!this.store.getChannel(channel)) throw new RpcError(-32001, `Unknown terminal ${channel}`);
    const child = this.terminals.get(channel);
    this.terminals.delete(channel);
    child?.kill();
    const rootEnvelope = this.store.deleteTerminal(channel);
    this.broadcastAction(rootEnvelope);
    return null;
  }

  private async createSession(connection: Connection, params: Record<string, unknown>): Promise<null> {
    const resource = requireSessionUri(params.channel);
    if (this.store.getSession(resource)) throw new RpcError(-32003, 'Session already exists');
    const provider = this.resolveProvider(params.provider);
    const requestedDirectories = optionalStringArray(params.workingDirectories);
    const workingDirectories = requestedDirectories.length > 0
      ? requestedDirectories.slice(0, 1).map(requireWorkingDirectoryUri)
      : undefined;
    const now = new Date().toISOString();
    const chatResource = `ahp-chat:/${crypto.randomUUID()}`;
    const chat: ChatState = {
      resource: chatResource,
      title: 'Chat',
      status: SessionStatus.Idle,
      modifiedAt: now,
      origin: { kind: ChatOriginKind.User },
      ...(workingDirectories ? { workingDirectories } : {}),
      turns: [],
    };
    const sessionState: SessionState = {
      provider: provider.id,
      title: provider.displayName,
      status: SessionStatus.Idle,
      lifecycle: SessionLifecycle.Creating,
      ...(workingDirectories ? { workingDirectories } : {}),
      chats: [chatSummary(chat)],
      defaultChat: chatResource,
      activeClients: [],
      config: {
        schema: {
          type: 'object',
          properties: {},
        },
        values: params.config === undefined ? {} : asRecord(params.config),
      },
      _meta: { acpVersion: 1 },
    };
    const session: StoredSession = {
      resource,
      provider: provider.id,
      createdAt: now,
      modifiedAt: now,
      state: sessionState,
    };
    const rootEnvelope = this.store.createSession(session, {
      resource: chatResource,
      session: resource,
      state: chat,
      binding: { provider: provider.id },
    });
    this.broadcastAction(rootEnvelope);
    this.broadcast('ahp-root://', 'root/sessionAdded', {
      channel: 'ahp-root://',
      summary: summary(session),
    });
    const adapter = this.getAdapter(chatResource);
    try {
      await adapter.start();
      if (params.config !== undefined) {
        for (const [configId, value] of Object.entries(asRecord(params.config))) {
          if (typeof value === 'string' || typeof value === 'boolean') {
            await adapter.setConfigOption(configId, value);
          }
        }
      }
      return null;
    } catch (error) {
      const current = this.store.getSession(resource);
      if (current?.state.lifecycle === SessionLifecycle.Creating) {
        const envelope = this.store.apply(resource, {
          type: ActionType.SessionCreationFailed,
          error: {
            errorType: 'provider_start_failed',
            message: asError(error).message,
          },
        } as StateAction);
        this.broadcastAction(envelope);
      }
      throw new RpcError(-32603, asError(error).message);
    }
  }

  private async disposeSession(params: Record<string, unknown>): Promise<null> {
    const resource = requireSessionUri(params.channel);
    const session = this.store.getSession(resource);
    if (!session) throw new RpcError(-32001, `Unknown session ${resource}`);
    for (const chat of session.state.chats) {
      const adapter = this.adapters.get(chat.resource);
      if (adapter) await adapter.stop();
      this.adapters.delete(chat.resource);
      this.commands.delete(chat.resource);
    }
    this.store.deleteSession(resource);
    const rootEnvelope = this.store.apply('ahp-root://', {
      type: ActionType.RootActiveSessionsChanged,
      activeSessions: this.store.sessionCount(),
    } as StateAction);
    this.broadcastAction(rootEnvelope);
    this.broadcast('ahp-root://', 'root/sessionRemoved', {
      channel: 'ahp-root://',
      session: resource,
    });
    return null;
  }

  private async disposeChat(params: Record<string, unknown>): Promise<null> {
    const resource = requireUri(params.channel, 'ahp-chat:');
    const chat = this.store.getChat(resource);
    if (!chat) throw new RpcError(-32001, `Unknown chat ${resource}`);
    const session = this.store.getSession(chat.session);
    if (!session) throw new RpcError(-32001, `Unknown session ${chat.session}`);
    if (session.state.chats.length === 1) {
      throw new RpcError(-32602, 'The default chat cannot be disposed; dispose the session instead');
    }
    const adapter = this.adapters.get(resource);
    if (adapter) await adapter.stop();
    this.adapters.delete(resource);
    this.commands.delete(resource);
    this.store.deleteChat(resource);
    const envelope = this.store.apply(chat.session, {
      type: ActionType.SessionChatRemoved,
      chat: resource,
    } as StateAction);
    this.broadcastAction(envelope);
    return null;
  }

  private fetchTurns(params: Record<string, unknown>): Record<string, never> {
    const resource = requireUri(params.channel, 'ahp-chat:');
    const chat = this.store.getChat(resource);
    if (!chat) throw new RpcError(-32001, `Unknown chat ${resource}`);
    if (params.cursor !== undefined) throw new RpcError(-32602, 'No older turn page exists for this chat');
    return {};
  }

  private async dispatch(connection: Connection, params: Record<string, unknown>): Promise<void> {
    const channel = requireString(params.channel, 'channel');
    const clientSeq = requireInteger(params.clientSeq, 'clientSeq', 0);
    const action = requireAction(params.action) as any;
    const origin = { clientId: connection.clientId ?? 'unknown', clientSeq };
    if (!isAllowedClientAction(action.type)) {
      this.broadcastAction(this.store.reject(channel, action, origin, 'Action is server-only'));
      return;
    }
    if (this.store.getSession(channel) && action.type === ActionType.SessionConfigChanged) {
      await this.dispatchSessionConfig(channel, action, origin);
      return;
    }
    if (isTerminalChannel(channel)) {
      this.dispatchTerminalAction(channel, action, origin);
      return;
    }
    const chat = this.store.getChat(channel);
    if (!chat) {
      this.broadcastAction(this.store.reject(channel, action, origin, 'Unknown or unsupported channel'));
      return;
    }
    switch (action.type) {
      case ActionType.ChatTurnStarted: {
        if (action.message.origin.kind !== MessageKind.User) {
          this.broadcastAction(this.store.reject(channel, action, origin, 'Only user messages may start a turn'));
          return;
        }
        if (chat.state.activeTurn) {
          this.broadcastAction(this.store.reject(channel, action, origin, 'A turn is already active'));
          return;
        }
        const accepted = this.store.apply(channel, action, origin);
        this.broadcastAction(accepted);
        this.syncChatSummary(chat.session, channel);
        const adapter = this.getAdapter(channel);
        void this.runPrompt(channel, adapter, action);
        return;
      }
      case ActionType.ChatTurnCancelled: {
        if (!chat.state.activeTurn || chat.state.activeTurn.id !== action.turnId) {
          this.broadcastAction(this.store.reject(channel, action, origin, 'Turn is not active'));
          return;
        }
        const accepted = this.store.apply(channel, action, origin);
        this.broadcastAction(accepted);
        this.syncChatSummary(chat.session, channel);
        const adapter = this.getAdapter(channel);
        void this.cleanupCancelledAdapter(channel, adapter);
        return;
      }
      case ActionType.ChatToolCallConfirmed: {
        const stateError = validateToolConfirmation(chat.state, action);
        if (stateError) {
          this.broadcastAction(this.store.reject(channel, action, origin, stateError));
          return;
        }
        const adapter = this.getAdapter(channel);
        const adapterError = adapter.validatePermission(
          action.turnId,
          action.toolCallId,
          action.approved,
          action.selectedOptionId,
        );
        if (adapterError) {
          this.broadcastAction(this.store.reject(channel, action, origin, adapterError));
          return;
        }
        const accepted = this.store.apply(channel, action, origin);
        this.broadcastAction(accepted);
        adapter.resolvePermission(
          action.turnId,
          action.toolCallId,
          action.approved,
          action.selectedOptionId,
        );
        return;
      }
      default: {
        const accepted = this.store.apply(channel, action, origin);
        this.broadcastAction(accepted);
      }
    }
  }

  private getAdapter(chatResource: string): AcpAdapter {
    const existing = this.adapters.get(chatResource);
    if (existing) return existing;
    const chat = this.store.getChat(chatResource);
    if (!chat) throw new RpcError(-32001, `Unknown chat ${chatResource}`);
    const session = this.store.getSession(chat.session);
    if (!session) throw new RpcError(-32001, `Unknown session ${chat.session}`);
    const provider = this.config.providers.find((item) => item.id === session.provider);
    if (!provider) throw new RpcError(-32002, `Unknown provider ${session.provider}`);
    const binding: AcpBinding = chat.binding ?? { provider: provider.id };
    const holder: { adapter?: AcpAdapter } = {};
    const adapter = this.adapterFactory.create(
      provider,
      binding,
      session.state.workingDirectories ?? [],
      {
        emit: (action) => {
          const envelope = this.store.apply(chatResource, action);
          this.broadcastAction(envelope);
          this.syncChatSummary(chat.session, chatResource);
        },
        bindingChanged: (nextBinding) => {
          this.store.updateBinding(chatResource, nextBinding);
          if (nextBinding.contextReset) {
            const state = this.store.getChat(chatResource)?.state;
            if (state?.activeTurn) {
              const envelope = this.store.apply(chatResource, {
                type: ActionType.ChatResponsePart,
                turnId: state.activeTurn.id,
                part: {
                  kind: 'systemNotification',
                  content: 'Agent context was reset because this provider cannot load an ACP session after restart.',
                },
              } as StateAction);
              this.broadcastAction(envelope);
            }
          }
        },
        ready: () => {
          const current = this.store.getSession(chat.session);
          if (!current || current.state.lifecycle === SessionLifecycle.Ready) return;
          const envelope = this.store.apply(chat.session, {
            type: ActionType.SessionReady,
          } as StateAction);
          this.broadcastAction(envelope);
        },
        modelsChanged: (models) => {
          const root = this.store.getChannel('ahp-root://') as any;
          const agents = root.agents.map((agent: any) =>
            agent.provider === provider.id
              ? {
                  ...agent,
                  models: models.map((model) => ({
                    id: model.id,
                    provider: provider.id,
                    name: model.name,
                    ...(model.description === undefined ? {} : { _meta: { description: model.description, ...model._meta } }),
                  })),
                }
              : agent);
          if (JSON.stringify(root.agents) === JSON.stringify(agents)) return;
          const envelope = this.store.apply('ahp-root://', {
            type: ActionType.RootAgentsChanged,
            agents,
          } as StateAction);
          this.broadcastAction(envelope);
        },
        commandsChanged: (commands) => {
          this.commands.set(chatResource, commands);
        },
        configChanged: (config) => {
          this.providerSessionConfigs.set(provider.id, config);
          const envelope = this.store.replaceSessionConfig(chat.session, config as never);
          this.broadcastAction(envelope);
        },
        sessionTitleChanged: (title) => {
          const current = this.store.getSession(chat.session);
          if (!current || current.state.title === title) return;
          const envelope = this.store.apply(chat.session, {
            type: ActionType.SessionTitleChanged,
            title,
          } as StateAction);
          this.broadcastAction(envelope);
          this.broadcast('ahp-root://', 'root/sessionSummaryChanged', {
            channel: 'ahp-root://',
            session: chat.session,
            changes: {
              title,
              modifiedAt: new Date().toISOString(),
            },
          });
        },
        controlUpdate: (kind, value) => {
          const current = this.store.getSession(chat.session);
          if (!current) return;
          const existingMeta = current.state._meta ?? {};
          const acpMeta = typeof existingMeta.acp === 'object' && existingMeta.acp !== null
            ? existingMeta.acp as Record<string, unknown>
            : {};
          const envelope = this.store.apply(chat.session, {
            type: ActionType.SessionMetaChanged,
            _meta: {
              ...existingMeta,
              acp: {
                ...acpMeta,
                [kind]: value,
              },
            },
          } as StateAction);
          this.broadcastAction(envelope);
        },
        crashed: (error) => {
          if (this.adapters.get(chatResource) === holder.adapter) this.adapters.delete(chatResource);
          const state = this.store.getChat(chatResource)?.state;
          if (!state?.activeTurn) return;
          const envelope = this.store.apply(chatResource, {
            type: ActionType.ChatError,
            turnId: state.activeTurn.id,
            duration: Math.max(0, Date.now() - Date.parse(state.activeTurn.startedAt)),
            part: {
              kind: 'error',
              error: { errorType: 'acp_process_crash', message: error.message },
              resumable: false,
            },
          } as StateAction);
          this.broadcastAction(envelope);
          this.syncChatSummary(chat.session, chatResource);
        },
        invalidated: () => {
          if (this.adapters.get(chatResource) === holder.adapter) this.adapters.delete(chatResource);
          this.commands.delete(chatResource);
        },
      },
    );
    holder.adapter = adapter;
    this.adapters.set(chatResource, adapter);
    return adapter;
  }

  private async cleanupCancelledAdapter(chatResource: string, adapter: AcpAdapter): Promise<void> {
    const reusable = await adapter.cancel().catch(() => false);
    if (reusable) return;
    await adapter.stop().catch(() => undefined);
    if (this.adapters.get(chatResource) === adapter) this.adapters.delete(chatResource);
  }

  private async dispatchSessionConfig(
    sessionResource: string,
    action: any,
    origin: { clientId: string; clientSeq: number },
  ): Promise<void> {
    const session = this.store.getSession(sessionResource);
    if (!session?.state.defaultChat || !session.state.config) {
      this.broadcastAction(this.store.reject(sessionResource, action, origin, 'Session config is unavailable'));
      return;
    }
    const values = asRecord(action.config);
    for (const [configId, value] of Object.entries(values)) {
      const property = session.state.config.schema.properties[configId];
      if (!property || property.sessionMutable !== true) {
        this.broadcastAction(this.store.reject(sessionResource, action, origin, `Unknown or immutable session config ${configId}`));
        return;
      }
      if (property.type === 'boolean') {
        if (typeof value !== 'boolean') {
          this.broadcastAction(this.store.reject(sessionResource, action, origin, `Session config ${configId} requires a boolean`));
          return;
        }
      } else if (
        property.type !== 'string'
        || typeof value !== 'string'
        || (property.enum && !property.enum.includes(value))
      ) {
        this.broadcastAction(this.store.reject(sessionResource, action, origin, `Invalid value for session config ${configId}`));
        return;
      }
    }
    const adapter = this.getAdapter(session.state.defaultChat);
    try {
      for (const [configId, value] of Object.entries(values)) {
        await adapter.setConfigOption(configId, value as string | boolean);
      }
      const accepted = this.store.apply(sessionResource, action, origin);
      this.broadcastAction(accepted);
    } catch (error) {
      this.broadcastAction(this.store.reject(sessionResource, action, origin, asError(error).message));
    }
  }

  private async runPrompt(chatResource: string, adapter: AcpAdapter, action: any): Promise<void> {
    try {
      const modelId = action.message?.model?.id;
      if (typeof modelId === 'string' && modelId !== 'default') {
        await adapter.setConfigOption('model', modelId);
      }
      await adapter.prompt(action.turnId, action.message.text);
    } catch (error) {
      const chat = this.store.getChat(chatResource);
      if (!chat?.state.activeTurn || chat.state.activeTurn.id !== action.turnId) return;
      const envelope = this.store.apply(chatResource, {
        type: ActionType.ChatError,
        turnId: action.turnId,
        duration: 0,
        part: {
          kind: 'error',
          error: {
            errorType: 'acp_config_error',
            message: asError(error).message,
          },
          resumable: false,
        },
      } as StateAction);
      this.broadcastAction(envelope);
    }
  }

  private dispatchTerminalAction(
    channel: string,
    action: any,
    origin: { clientId: string; clientSeq: number },
  ): void {
    if (!this.store.getChannel(channel)) {
      this.broadcastAction(this.store.reject(channel, action, origin, 'Unknown terminal'));
      return;
    }
    if (action.type === ActionType.TerminalInput && typeof action.data !== 'string') {
      this.broadcastAction(this.store.reject(channel, action, origin, 'Terminal input data must be a string'));
      return;
    }
    if (
      action.type === ActionType.TerminalResized
      && (!Number.isInteger(action.cols) || action.cols < 1 || !Number.isInteger(action.rows) || action.rows < 1)
    ) {
      this.broadcastAction(this.store.reject(channel, action, origin, 'Terminal dimensions must be positive integers'));
      return;
    }
    const accepted = this.store.apply(channel, action, origin);
    this.broadcastAction(accepted);
    if (action.type === ActionType.TerminalInput) {
      this.terminals.get(channel)?.stdin.write(action.data);
    }
  }

  private emitTerminalData(channel: string, data: string): void {
    if (this.closing || !data || !this.store.getChannel(channel)) return;
    const envelope = this.store.apply(channel, {
      type: ActionType.TerminalData,
      data,
    } as StateAction);
    this.broadcastAction(envelope);
  }

  private terminalExited(channel: string, code: number | null): void {
    this.terminals.delete(channel);
    if (this.closing) return;
    if (!this.store.getChannel(channel)) return;
    const terminalEnvelope = this.store.apply(channel, {
      type: ActionType.TerminalExited,
      ...(code === null ? {} : { exitCode: code }),
    } as StateAction);
    this.broadcastAction(terminalEnvelope);
    const root = this.store.getChannel('ahp-root://') as any;
    const terminals = (root.terminals ?? []).map((terminal: any) =>
      terminal.resource === channel
        ? {
            ...terminal,
            lifecycle: {
              status: TerminalLifecycleStatus.Exited,
              ...(code === null ? {} : { exitCode: code }),
            },
          }
        : terminal);
    const rootEnvelope = this.store.apply('ahp-root://', {
      type: ActionType.RootTerminalsChanged,
      terminals,
    } as StateAction);
    this.broadcastAction(rootEnvelope);
  }

  private syncChatSummary(sessionResource: string, chatResource: string): void {
    const chat = this.store.getChat(chatResource);
    if (!chat) return;
    const modifiedAt = new Date().toISOString();
    const envelope = this.store.apply(sessionResource, {
      type: ActionType.SessionChatUpdated,
      chat: chatResource,
      changes: {
        status: chat.state.status,
        activity: chat.state.activity,
        modifiedAt,
      },
    } as StateAction);
    this.broadcastAction(envelope);
    const session = this.store.getSession(sessionResource);
    if (session) {
      this.broadcast('ahp-root://', 'root/sessionSummaryChanged', {
        channel: 'ahp-root://',
        session: sessionResource,
        changes: {
          status: session.state.status,
          activity: session.state.activity,
          modifiedAt,
          chats: session.state.chats.map((item) => ({
            resource: item.resource,
            title: item.title,
            status: item.status,
            modifiedAt: item.modifiedAt,
          })),
        },
      });
    }
  }

  private resolveProvider(value: unknown): ProviderConfig {
    const id = value === undefined ? this.config.providers[0]?.id : requireString(value, 'provider');
    const provider = this.config.providers.find((item) => item.id === id);
    if (!provider) throw new RpcError(-32002, `No agent for provider ${id}`);
    return provider;
  }

  private requireRoot(params: Record<string, unknown>): void {
    if (params.channel !== 'ahp-root://') throw new RpcError(-32602, 'channel must be ahp-root://');
  }

  private broadcastAction(envelope: ActionEnvelope): void {
    this.broadcast(envelope.channel, 'action', envelope);
  }

  private broadcast(channel: string, method: string, params: unknown): void {
    const payload = JSON.stringify({ jsonrpc: '2.0', method, params });
    for (const connection of this.connections) {
      if (
        connection.initialized
        && connection.subscriptions.has(channel)
        && connection.socket.readyState === WebSocket.OPEN
      ) {
        connection.socket.send(payload);
      }
    }
  }

  private sendResult(connection: Connection, id: string | number | null, result: unknown): void {
    connection.socket.send(JSON.stringify({ jsonrpc: '2.0', id, result }));
  }

  private sendError(
    connection: Connection,
    id: string | number | null,
    code: number,
    message: string,
    data?: unknown,
  ): void {
    connection.socket.send(JSON.stringify({
      jsonrpc: '2.0',
      id,
      error: { code, message, ...(data === undefined ? {} : { data }) },
    }));
  }
}

class RpcError extends Error {
  constructor(
    readonly code: number,
    message: string,
    readonly data?: unknown,
  ) {
    super(message);
  }
}

function providerInfo(provider: ProviderConfig): {
  provider: string;
  displayName: string;
  description: string;
  models: Array<{ id: string; provider: string; name: string }>;
} {
  return {
    provider: provider.id,
    displayName: provider.displayName,
    description: provider.description,
    models: [{
      id: 'default',
      provider: provider.id,
      name: 'Agent default',
    }],
  };
}

function chatSummary(state: ChatState) {
  return {
    resource: state.resource,
    title: state.title,
    status: state.status,
    ...(state.activity === undefined ? {} : { activity: state.activity }),
    modifiedAt: state.modifiedAt,
    ...(state.origin === undefined ? {} : { origin: state.origin }),
    ...(state.workingDirectories === undefined ? {} : { workingDirectories: state.workingDirectories }),
  };
}

function summary(session: StoredSession) {
  return {
    resource: session.resource,
    provider: session.provider,
    title: session.state.title,
    status: session.state.status,
    ...(session.state.activity === undefined ? {} : { activity: session.state.activity }),
    ...(session.state.workingDirectories === undefined
      ? {}
      : { workingDirectories: session.state.workingDirectories }),
    createdAt: session.createdAt,
    modifiedAt: session.modifiedAt,
    chats: session.state.chats.map((chat) => ({
      resource: chat.resource,
      title: chat.title,
      status: chat.status,
      modifiedAt: chat.modifiedAt,
    })),
    ...(session.state.defaultChat === undefined ? {} : { defaultChat: session.state.defaultChat }),
    _meta: session.state._meta,
  };
}

function asRecord(value: unknown): Record<string, unknown> {
  if (value === undefined) return {};
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new RpcError(-32602, 'params must be an object');
  }
  return value as Record<string, unknown>;
}

function requireAction(value: unknown): StateAction {
  const action = asRecord(value);
  if (typeof action.type !== 'string') throw new RpcError(-32602, 'action.type must be a string');
  return action as unknown as StateAction;
}

function requireString(value: unknown, field: string): string {
  if (typeof value !== 'string' || value.length === 0) {
    throw new RpcError(-32602, `${field} must be a non-empty string`);
  }
  return value;
}

function requireStringValue(value: unknown, field: string): string {
  if (typeof value !== 'string') {
    throw new RpcError(-32602, `${field} must be a string`);
  }
  return value;
}

function requireStringArray(value: unknown, field: string): string[] {
  if (!Array.isArray(value) || !value.every((item) => typeof item === 'string')) {
    throw new RpcError(-32602, `${field} must be an array of strings`);
  }
  return value;
}

function optionalStringArray(value: unknown): string[] {
  return value === undefined ? [] : requireStringArray(value, 'value');
}

function requireInteger(value: unknown, field: string, min: number, max = Number.MAX_SAFE_INTEGER): number {
  if (!Number.isInteger(value) || (value as number) < min || (value as number) > max) {
    throw new RpcError(-32602, `${field} must be an integer between ${min} and ${max}`);
  }
  return value as number;
}

function requireUri(value: unknown, prefix: string): string {
  const uri = requireString(value, 'channel');
  if (!uri.startsWith(prefix)) throw new RpcError(-32602, `channel must start with ${prefix}`);
  return uri;
}

function requireSessionUri(value: unknown): string {
  const uri = requireString(value, 'channel');
  let parsed: URL;
  try {
    parsed = new URL(uri);
  } catch {
    throw new RpcError(-32602, 'session channel must be a valid URI');
  }
  if (
    uri === 'ahp-root://'
    || parsed.protocol === 'ahp-chat:'
    || parsed.protocol === 'ahp-terminal:'
    || parsed.protocol === 'agenthost-terminal:'
  ) {
    throw new RpcError(-32602, 'session channel uses a reserved URI scheme');
  }
  return uri;
}

function requireTerminalUri(value: unknown): string {
  const uri = requireString(value, 'channel');
  if (!isTerminalChannel(uri)) {
    throw new RpcError(-32602, 'terminal channel must use ahp-terminal: or agenthost-terminal:');
  }
  return uri;
}

function isTerminalChannel(uri: string): boolean {
  return uri.startsWith('ahp-terminal:') || uri.startsWith('agenthost-terminal:');
}

function requireWorkingDirectoryUri(value: string): string {
  try {
    toNativeWorkingDirectory(value);
    return new URL(value).href;
  } catch (error) {
    throw new RpcError(-32602, asError(error).message);
  }
}

function resourcePath(uri: string): string {
  try {
    return toNativeWorkingDirectory(uri);
  } catch (error) {
    throw new RpcError(-32602, asError(error).message);
  }
}

function resourceError(error: unknown, uri: string): RpcError {
  if (error instanceof RpcError) return error;
  const code = (error as NodeJS.ErrnoException | undefined)?.code;
  if (code === 'EACCES' || code === 'EPERM') {
    return new RpcError(-32009, `Permission denied: ${uri}`);
  }
  return new RpcError(-32008, `Resource not found: ${uri}`);
}

function isPathWithin(root: string, candidate: string): boolean {
  const path = relative(root, candidate);
  return path === '' || (!path.startsWith('..') && !isAbsolute(path));
}

function isUtf8(buffer: Buffer): boolean {
  try {
    new TextDecoder('utf-8', { fatal: true }).decode(buffer);
    return true;
  } catch {
    return false;
  }
}

function contentType(path: string): string {
  switch (extname(path).toLowerCase()) {
    case '.css':
      return 'text/css';
    case '.htm':
    case '.html':
      return 'text/html';
    case '.js':
    case '.cjs':
    case '.mjs':
      return 'text/javascript';
    case '.json':
      return 'application/json';
    case '.md':
    case '.markdown':
      return 'text/markdown';
    case '.svg':
      return 'image/svg+xml';
    case '.png':
      return 'image/png';
    case '.jpg':
    case '.jpeg':
      return 'image/jpeg';
    case '.gif':
      return 'image/gif';
    case '.webp':
      return 'image/webp';
    default:
      return 'text/plain';
  }
}

function validateToolConfirmation(state: ChatState, action: any): string | undefined {
  if (typeof action.approved !== 'boolean') return 'approved must be a boolean';
  const turn = state.activeTurn;
  if (!turn || turn.id !== action.turnId) return 'Permission turn is not active';
  const part = turn.responseParts.find((candidate) =>
    candidate.kind === 'toolCall' && candidate.toolCall.toolCallId === action.toolCallId);
  if (!part || part.kind !== 'toolCall') return 'Tool call is not part of the active turn';
  if (part.toolCall.status !== ToolCallStatus.PendingConfirmation) {
    return 'Tool call is not pending confirmation';
  }
  const selected = action.selectedOptionId === undefined
    ? undefined
    : part.toolCall.options?.find((option) => option.id === action.selectedOptionId);
  if (part.toolCall.options?.length && !selected) {
    return action.selectedOptionId
      ? `Unknown confirmation option ${action.selectedOptionId}`
      : 'selectedOptionId is required';
  }
  if (selected) {
    const optionApproves = selected.kind === ConfirmationOptionKind.Approve;
    if (optionApproves !== action.approved) {
      return `Confirmation option ${selected.id} contradicts approved=${action.approved}`;
    }
  }
  if (action.approved) {
    if (
      action.confirmed !== ToolCallConfirmationReason.UserAction
      && action.confirmed !== ToolCallConfirmationReason.Setting
      && action.confirmed !== ToolCallConfirmationReason.NotNeeded
    ) {
      return 'Approved confirmations require a valid confirmed reason';
    }
    if (action.reason !== undefined) return 'Approved confirmations cannot include a denial reason';
  } else {
    if (
      action.reason !== ToolCallCancellationReason.Denied
      && action.reason !== ToolCallCancellationReason.Skipped
    ) {
      return 'Denied confirmations require reason denied or skipped';
    }
    if (action.confirmed !== undefined) return 'Denied confirmations cannot include confirmed';
  }
  return undefined;
}

function encodeCursor(offset: number): string {
  return Buffer.from(String(offset), 'utf8').toString('base64url');
}

function decodeCursor(cursor: string): number {
  const value = Number(Buffer.from(cursor, 'base64url').toString('utf8'));
  if (!Number.isInteger(value) || value < 0) throw new RpcError(-32602, 'Invalid cursor');
  return value;
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isAllowedClientAction(type: unknown): boolean {
  return type === ActionType.ChatTurnStarted
    || type === ActionType.ChatTurnCancelled
    || type === ActionType.ChatToolCallConfirmed
    || type === ActionType.ChatToolCallComplete
    || type === ActionType.ChatToolCallResultConfirmed
    || type === ActionType.ChatToolCallContentChanged
    || type === ActionType.ChatDraftChanged
    || type === ActionType.ChatIsReadChanged
    || type === ActionType.ChatIsArchivedChanged
    || type === ActionType.SessionConfigChanged
    || type === ActionType.TerminalInput
    || type === ActionType.TerminalResized;
}
