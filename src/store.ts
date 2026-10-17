import { DatabaseSync } from 'node:sqlite';
import {
  ActionType,
  ResponsePartKind,
  changesetReducer,
  chatReducer,
  resourceWatchReducer,
  rootReducer,
  sessionReducer,
  terminalReducer,
  type ActionEnvelope,
  type ChangesetState,
  type ChatState,
  type ResourceWatchState,
  type RootState,
  type SessionState,
  type StateAction,
  type TerminalState,
} from '@microsoft/agent-host-protocol';

export type ChannelState =
  | RootState
  | SessionState
  | ChatState
  | TerminalState
  | ChangesetState
  | ResourceWatchState;
export type ChannelKind =
  | 'root'
  | 'session'
  | 'chat'
  | 'terminal'
  | 'changeset'
  | 'resourceWatch';

export interface StoredSession {
  resource: string;
  provider: string;
  createdAt: string;
  modifiedAt: string;
  state: SessionState;
}

export interface StoredChat {
  resource: string;
  session: string;
  state: ChatState;
  binding: AcpBinding | undefined;
}

export interface AcpBinding {
  provider: string;
  acpSessionId?: string;
  loadSupported?: boolean;
  contextReset?: boolean;
  configOptionTypes?: Record<string, 'select' | 'boolean'>;
}

export interface StoredFileEditSnapshot {
  sessionUri: string;
  turnId: string;
  toolCallId: string;
  fileUri: string;
  oldText: string | null;
  newText: string | null;
  added: number;
  removed: number;
}

export class GatewayStore {
  readonly db: DatabaseSync;
  readonly replayLimit: number;

  constructor(path: string, replayLimit: number) {
    this.db = new DatabaseSync(path);
    this.replayLimit = replayLimit;
    this.db.exec(`
      PRAGMA journal_mode=WAL;
      PRAGMA foreign_keys=ON;
      CREATE TABLE IF NOT EXISTS metadata (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS channels (
        uri TEXT PRIMARY KEY,
        kind TEXT NOT NULL CHECK (kind IN ('root', 'session', 'chat', 'terminal', 'changeset', 'resourceWatch')),
        state_json TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS actions (
        server_seq INTEGER PRIMARY KEY,
        channel TEXT NOT NULL,
        envelope_json TEXT NOT NULL,
        created_at TEXT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS actions_channel_seq ON actions(channel, server_seq);
      CREATE TABLE IF NOT EXISTS sessions (
        resource TEXT PRIMARY KEY,
        provider TEXT NOT NULL,
        created_at TEXT NOT NULL,
        modified_at TEXT NOT NULL,
        state_json TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS chats (
        resource TEXT PRIMARY KEY,
        session_uri TEXT NOT NULL REFERENCES sessions(resource) ON DELETE CASCADE,
        state_json TEXT NOT NULL,
        binding_json TEXT
      );
      CREATE TABLE IF NOT EXISTS file_edit_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_uri TEXT NOT NULL REFERENCES sessions(resource) ON DELETE CASCADE,
        turn_id TEXT NOT NULL,
        tool_call_id TEXT NOT NULL,
        file_uri TEXT NOT NULL,
        old_text TEXT,
        new_text TEXT,
        added INTEGER NOT NULL DEFAULT 0,
        removed INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS file_edit_snapshots_session ON file_edit_snapshots(session_uri, id);
      CREATE INDEX IF NOT EXISTS file_edit_snapshots_lookup ON file_edit_snapshots(session_uri, tool_call_id, file_uri, id);
      INSERT OR IGNORE INTO metadata(key, value) VALUES ('serverSeq', '0');
    `);
    this.migrateTerminalChannelKind();
  }

  close(): void {
    this.db.close();
  }

  initializeRoot(state: RootState): void {
    this.ensureRoot(state);
  }

  ensureRoot(state: RootState): ActionEnvelope[] {
    const current = this.getChannel('ahp-root://') as RootState | undefined;
    if (!current) {
      this.db.prepare(
        `INSERT INTO channels(uri, kind, state_json) VALUES ('ahp-root://', 'root', ?)`,
      ).run(JSON.stringify(state));
      return [];
    }

    const envelopes: ActionEnvelope[] = [];
    if (JSON.stringify(current.agents) !== JSON.stringify(state.agents)) {
      envelopes.push(this.apply('ahp-root://', {
        type: ActionType.RootAgentsChanged,
        agents: state.agents,
      } as StateAction));
    }
    const refreshed = this.getChannel('ahp-root://') as RootState;
    if (refreshed.activeSessions !== state.activeSessions) {
      envelopes.push(this.apply('ahp-root://', {
        type: ActionType.RootActiveSessionsChanged,
        activeSessions: state.activeSessions ?? 0,
      } as StateAction));
    }
    const withSessions = this.getChannel('ahp-root://') as RootState;
    if (JSON.stringify(withSessions.terminals ?? []) !== JSON.stringify(state.terminals ?? [])) {
      envelopes.push(this.apply('ahp-root://', {
        type: ActionType.RootTerminalsChanged,
        terminals: state.terminals ?? [],
      } as StateAction));
    }
    return envelopes;
  }

  get serverSeq(): number {
    const row = this.db.prepare(`SELECT value FROM metadata WHERE key='serverSeq'`).get() as { value: string };
    return Number(row.value);
  }

  getChannel(uri: string): ChannelState | undefined {
    const row = this.db.prepare(`SELECT state_json FROM channels WHERE uri=?`).get(uri) as
      | { state_json: string }
      | undefined;
    return row ? JSON.parse(row.state_json) as ChannelState : undefined;
  }

  getChannelKind(uri: string): ChannelKind | undefined {
    const row = this.db.prepare(`SELECT kind FROM channels WHERE uri=?`).get(uri) as
      | { kind: ChannelKind }
      | undefined;
    return row?.kind;
  }

  getSnapshot(uri: string): { resource: string; state: ChannelState; fromSeq: number } | undefined {
    const state = this.getChannel(uri);
    return state ? { resource: uri, state, fromSeq: this.serverSeq } : undefined;
  }

  putSession(session: StoredSession): void {
    const json = JSON.stringify(session.state);
    this.transaction(() => {
      this.db.prepare(
        `INSERT INTO sessions(resource, provider, created_at, modified_at, state_json)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(resource) DO UPDATE SET
           provider=excluded.provider, modified_at=excluded.modified_at, state_json=excluded.state_json`,
      ).run(session.resource, session.provider, session.createdAt, session.modifiedAt, json);
      this.db.prepare(
        `INSERT INTO channels(uri, kind, state_json) VALUES (?, 'session', ?)
         ON CONFLICT(uri) DO UPDATE SET state_json=excluded.state_json`,
      ).run(session.resource, json);
    });
  }

  putChat(chat: StoredChat): void {
    const json = JSON.stringify(chat.state);
    this.transaction(() => {
      this.db.prepare(
        `INSERT INTO chats(resource, session_uri, state_json, binding_json)
         VALUES (?, ?, ?, ?)
         ON CONFLICT(resource) DO UPDATE SET
           session_uri=excluded.session_uri, state_json=excluded.state_json, binding_json=excluded.binding_json`,
      ).run(chat.resource, chat.session, json, chat.binding ? JSON.stringify(chat.binding) : null);
      this.db.prepare(
        `INSERT INTO channels(uri, kind, state_json) VALUES (?, 'chat', ?)
         ON CONFLICT(uri) DO UPDATE SET state_json=excluded.state_json`,
      ).run(chat.resource, json);
    });
  }

  createSession(session: StoredSession, chat: StoredChat): ActionEnvelope {
    if (chat.session !== session.resource) {
      throw new Error(`Chat ${chat.resource} does not belong to session ${session.resource}`);
    }
    const root = this.getChannel('ahp-root://') as RootState | undefined;
    if (!root) throw new Error('Root channel is not initialized');
    const action = {
      type: ActionType.RootActiveSessionsChanged,
      activeSessions: this.sessionCount() + 1,
    } as StateAction;
    const nextRoot = rootReducer(root, action as never);
    const nextSeq = this.serverSeq + 1;
    const envelope: ActionEnvelope = {
      channel: 'ahp-root://',
      action,
      serverSeq: nextSeq,
      origin: undefined,
    };
    const sessionJson = JSON.stringify(session.state);
    const chatJson = JSON.stringify(chat.state);
    const now = new Date().toISOString();

    this.transaction(() => {
      this.db.prepare(
        `INSERT INTO sessions(resource, provider, created_at, modified_at, state_json)
         VALUES (?, ?, ?, ?, ?)`,
      ).run(session.resource, session.provider, session.createdAt, session.modifiedAt, sessionJson);
      this.db.prepare(
        `INSERT INTO channels(uri, kind, state_json) VALUES (?, 'session', ?)`,
      ).run(session.resource, sessionJson);
      this.db.prepare(
        `INSERT INTO chats(resource, session_uri, state_json, binding_json) VALUES (?, ?, ?, ?)`,
      ).run(chat.resource, chat.session, chatJson, chat.binding ? JSON.stringify(chat.binding) : null);
      this.db.prepare(
        `INSERT INTO channels(uri, kind, state_json) VALUES (?, 'chat', ?)`,
      ).run(chat.resource, chatJson);
      this.db.prepare(`UPDATE channels SET state_json=? WHERE uri='ahp-root://'`).run(JSON.stringify(nextRoot));
      this.db.prepare(`UPDATE metadata SET value=? WHERE key='serverSeq'`).run(String(nextSeq));
      this.db.prepare(
        `INSERT INTO actions(server_seq, channel, envelope_json, created_at) VALUES (?, ?, ?, ?)`,
      ).run(nextSeq, 'ahp-root://', JSON.stringify(envelope), now);
      this.trimActions();
    });
    return envelope;
  }

  clearTerminals(): void {
    this.db.prepare(`DELETE FROM channels WHERE kind IN ('terminal', 'resourceWatch')`).run();
  }

  putChannel(uri: string, kind: ChannelKind, state: ChannelState): void {
    this.db.prepare(
      `INSERT INTO channels(uri, kind, state_json) VALUES (?, ?, ?)
       ON CONFLICT(uri) DO UPDATE SET kind=excluded.kind, state_json=excluded.state_json`,
    ).run(uri, kind, JSON.stringify(state));
  }

  deleteChannel(uri: string): void {
    this.db.prepare(`DELETE FROM channels WHERE uri=?`).run(uri);
  }

  listChannelsByKind(kind: ChannelKind): Array<{ uri: string; state: ChannelState }> {
    const rows = this.db.prepare(`SELECT uri, state_json FROM channels WHERE kind=?`).all(kind) as Array<{
      uri: string;
      state_json: string;
    }>;
    return rows.map((row) => ({
      uri: row.uri,
      state: JSON.parse(row.state_json) as ChannelState,
    }));
  }

  createTerminal(
    uri: string,
    state: TerminalState,
    info: NonNullable<RootState['terminals']>[number],
  ): ActionEnvelope {
    const root = this.getChannel('ahp-root://') as RootState | undefined;
    if (!root) throw new Error('Root channel is not initialized');
    const action = {
      type: ActionType.RootTerminalsChanged,
      terminals: [...(root.terminals ?? []), info],
    } as StateAction;
    const nextRoot = rootReducer(root, action as never);
    const nextSeq = this.serverSeq + 1;
    const envelope: ActionEnvelope = {
      channel: 'ahp-root://',
      action,
      serverSeq: nextSeq,
      origin: undefined,
    };
    this.transaction(() => {
      this.db.prepare(
        `INSERT INTO channels(uri, kind, state_json) VALUES (?, 'terminal', ?)`,
      ).run(uri, JSON.stringify(state));
      this.db.prepare(`UPDATE channels SET state_json=? WHERE uri='ahp-root://'`).run(JSON.stringify(nextRoot));
      this.db.prepare(`UPDATE metadata SET value=? WHERE key='serverSeq'`).run(String(nextSeq));
      this.db.prepare(
        `INSERT INTO actions(server_seq, channel, envelope_json, created_at) VALUES (?, ?, ?, ?)`,
      ).run(nextSeq, 'ahp-root://', JSON.stringify(envelope), new Date().toISOString());
      this.trimActions();
    });
    return envelope;
  }

  deleteTerminal(uri: string): ActionEnvelope {
    const root = this.getChannel('ahp-root://') as RootState | undefined;
    if (!root) throw new Error('Root channel is not initialized');
    const action = {
      type: ActionType.RootTerminalsChanged,
      terminals: (root.terminals ?? []).filter((terminal) => terminal.resource !== uri),
    } as StateAction;
    const nextRoot = rootReducer(root, action as never);
    const nextSeq = this.serverSeq + 1;
    const envelope: ActionEnvelope = {
      channel: 'ahp-root://',
      action,
      serverSeq: nextSeq,
      origin: undefined,
    };
    this.transaction(() => {
      this.db.prepare(`DELETE FROM channels WHERE uri=? AND kind='terminal'`).run(uri);
      this.db.prepare(`UPDATE channels SET state_json=? WHERE uri='ahp-root://'`).run(JSON.stringify(nextRoot));
      this.db.prepare(`UPDATE metadata SET value=? WHERE key='serverSeq'`).run(String(nextSeq));
      this.db.prepare(
        `INSERT INTO actions(server_seq, channel, envelope_json, created_at) VALUES (?, ?, ?, ?)`,
      ).run(nextSeq, 'ahp-root://', JSON.stringify(envelope), new Date().toISOString());
      this.trimActions();
    });
    return envelope;
  }

  updateBinding(chat: string, binding: AcpBinding): void {
    this.db.prepare(`UPDATE chats SET binding_json=? WHERE resource=?`).run(JSON.stringify(binding), chat);
  }

  replaceSessionConfig(
    uri: string,
    config: NonNullable<SessionState['config']>,
  ): ActionEnvelope {
    const current = this.getSession(uri);
    if (!current) throw new Error(`Unknown session ${uri}`);
    const action = {
      type: ActionType.SessionConfigChanged,
      config: config.values,
      replace: true,
    } as StateAction;
    const nextState: SessionState = {
      ...current.state,
      config,
    };
    const nextSeq = this.serverSeq + 1;
    const envelope: ActionEnvelope = {
      channel: uri,
      action,
      serverSeq: nextSeq,
      origin: undefined,
    };
    const now = new Date().toISOString();
    const json = JSON.stringify(nextState);
    this.transaction(() => {
      this.db.prepare(`UPDATE metadata SET value=? WHERE key='serverSeq'`).run(String(nextSeq));
      this.db.prepare(`UPDATE channels SET state_json=? WHERE uri=?`).run(json, uri);
      this.db.prepare(`UPDATE sessions SET state_json=?, modified_at=? WHERE resource=?`).run(json, now, uri);
      this.db.prepare(
        `INSERT INTO actions(server_seq, channel, envelope_json, created_at) VALUES (?, ?, ?, ?)`,
      ).run(nextSeq, uri, JSON.stringify(envelope), now);
      this.trimActions();
    });
    return envelope;
  }

  getChat(uri: string): StoredChat | undefined {
    const row = this.db.prepare(
      `SELECT resource, session_uri, state_json, binding_json FROM chats WHERE resource=?`,
    ).get(uri) as
      | { resource: string; session_uri: string; state_json: string; binding_json: string | null }
      | undefined;
    return row
      ? {
          resource: row.resource,
          session: row.session_uri,
          state: JSON.parse(row.state_json) as ChatState,
          binding: row.binding_json ? JSON.parse(row.binding_json) as AcpBinding : undefined,
        }
      : undefined;
  }

  getSession(uri: string): StoredSession | undefined {
    const row = this.db.prepare(
      `SELECT resource, provider, created_at, modified_at, state_json FROM sessions WHERE resource=?`,
    ).get(uri) as
      | { resource: string; provider: string; created_at: string; modified_at: string; state_json: string }
      | undefined;
    return row
      ? {
          resource: row.resource,
          provider: row.provider,
          createdAt: row.created_at,
          modifiedAt: row.modified_at,
          state: JSON.parse(row.state_json) as SessionState,
        }
      : undefined;
  }

  listSessions(limit = 100, offset = 0): StoredSession[] {
    const rows = this.db.prepare(
      `SELECT resource, provider, created_at, modified_at, state_json
       FROM sessions ORDER BY modified_at DESC LIMIT ? OFFSET ?`,
    ).all(limit, offset) as Array<{
      resource: string;
      provider: string;
      created_at: string;
      modified_at: string;
      state_json: string;
    }>;
    return rows.map((row) => ({
      resource: row.resource,
      provider: row.provider,
      createdAt: row.created_at,
      modifiedAt: row.modified_at,
      state: JSON.parse(row.state_json) as SessionState,
    }));
  }

  sessionCount(): number {
    return Number((this.db.prepare(`SELECT COUNT(*) AS count FROM sessions`).get() as { count: number }).count);
  }

  deleteSession(uri: string): void {
    this.transaction(() => {
      const chatRows = this.db.prepare(`SELECT resource FROM chats WHERE session_uri=?`).all(uri) as Array<{
        resource: string;
      }>;
      for (const chatRow of chatRows) {
        this.db.prepare(`DELETE FROM channels WHERE uri LIKE ?`).run(`${chatRow.resource}/changeset/%`);
      }
      this.db.prepare(`DELETE FROM channels WHERE uri LIKE ?`).run(`${uri}/changeset/%`);
      this.db.prepare(
        `DELETE FROM channels WHERE uri IN (SELECT resource FROM chats WHERE session_uri=?)`,
      ).run(uri);
      this.db.prepare(`DELETE FROM sessions WHERE resource=?`).run(uri);
      this.db.prepare(`DELETE FROM channels WHERE uri=?`).run(uri);
    });
  }

  recordFileEditSnapshot(snapshot: StoredFileEditSnapshot): void {
    this.db.prepare(
      `INSERT INTO file_edit_snapshots(session_uri, turn_id, tool_call_id, file_uri, old_text, new_text, added, removed, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    ).run(
      snapshot.sessionUri,
      snapshot.turnId,
      snapshot.toolCallId,
      snapshot.fileUri,
      snapshot.oldText,
      snapshot.newText,
      snapshot.added,
      snapshot.removed,
      new Date().toISOString(),
    );
  }

  getFileEditSnapshot(
    sessionUri: string,
    toolCallId: string,
    fileUri: string,
  ): StoredFileEditSnapshot | undefined {
    const rows = this.db.prepare(
      `SELECT session_uri, turn_id, tool_call_id, file_uri, old_text, new_text, added, removed
       FROM file_edit_snapshots
       WHERE session_uri=? AND tool_call_id=? AND file_uri=?
       ORDER BY id ASC`,
    ).all(sessionUri, toolCallId, fileUri) as Array<{
      session_uri: string;
      turn_id: string;
      tool_call_id: string;
      file_uri: string;
      old_text: string | null;
      new_text: string | null;
      added: number;
      removed: number;
    }>;
    if (rows.length === 0) return undefined;
    const first = rows[0]!;
    const last = rows[rows.length - 1]!;
    return {
      sessionUri: first.session_uri,
      turnId: first.turn_id,
      toolCallId: first.tool_call_id,
      fileUri: first.file_uri,
      oldText: first.old_text,
      newText: last.new_text,
      added: rows.reduce((sum, row) => sum + row.added, 0),
      removed: rows.reduce((sum, row) => sum + row.removed, 0),
    };
  }

  listFileEditSnapshots(
    sessionUri: string,
    turnId?: string,
  ): StoredFileEditSnapshot[] {
    const rows = (turnId
      ? this.db.prepare(
          `SELECT session_uri, turn_id, tool_call_id, file_uri, old_text, new_text, added, removed
           FROM file_edit_snapshots
           WHERE session_uri=? AND turn_id=?
           ORDER BY id ASC`,
        ).all(sessionUri, turnId)
      : this.db.prepare(
          `SELECT session_uri, turn_id, tool_call_id, file_uri, old_text, new_text, added, removed
           FROM file_edit_snapshots
           WHERE session_uri=?
           ORDER BY id ASC`,
        ).all(sessionUri)) as Array<{
      session_uri: string;
      turn_id: string;
      tool_call_id: string;
      file_uri: string;
      old_text: string | null;
      new_text: string | null;
      added: number;
      removed: number;
    }>;
    return rows.map((row) => ({
      sessionUri: row.session_uri,
      turnId: row.turn_id,
      toolCallId: row.tool_call_id,
      fileUri: row.file_uri,
      oldText: row.old_text,
      newText: row.new_text,
      added: row.added,
      removed: row.removed,
    }));
  }

  deleteFileEditSnapshotsForFile(sessionUri: string, fileUri: string): void {
    this.db.prepare(
      `DELETE FROM file_edit_snapshots WHERE session_uri=? AND file_uri=?`,
    ).run(sessionUri, fileUri);
  }

  deleteChat(uri: string): void {
    this.transaction(() => {
      this.db.prepare(`DELETE FROM channels WHERE uri LIKE ?`).run(`${uri}/changeset/%`);
      this.db.prepare(`DELETE FROM chats WHERE resource=?`).run(uri);
      this.db.prepare(`DELETE FROM channels WHERE uri=?`).run(uri);
    });
  }

  apply(channel: string, action: StateAction, origin?: { clientId: string; clientSeq: number }): ActionEnvelope {
    const current = this.getChannel(channel);
    if (!current) throw new Error(`Unknown channel ${channel}`);
    const kind = this.getChannelKind(channel);
    if (!kind) throw new Error(`Unknown channel ${channel}`);
    const next = reduce(kind, current, action);
    const nextSeq = this.serverSeq + 1;
    const envelope: ActionEnvelope = { channel, action, serverSeq: nextSeq, origin };
    const now = new Date().toISOString();
    const nextJson = JSON.stringify(next);

    this.transaction(() => {
      this.db.prepare(`UPDATE metadata SET value=? WHERE key='serverSeq'`).run(String(nextSeq));
      this.db.prepare(`UPDATE channels SET state_json=? WHERE uri=?`).run(nextJson, channel);
      this.db.prepare(
        `INSERT INTO actions(server_seq, channel, envelope_json, created_at) VALUES (?, ?, ?, ?)`,
      ).run(nextSeq, channel, JSON.stringify(envelope), now);
      if (kind === 'session') {
        this.db.prepare(`UPDATE sessions SET state_json=?, modified_at=? WHERE resource=?`).run(nextJson, now, channel);
      } else if (kind === 'chat') {
        this.db.prepare(`UPDATE chats SET state_json=? WHERE resource=?`).run(nextJson, channel);
      }
      this.trimActions();
    });
    return envelope;
  }

  reject(
    channel: string,
    action: StateAction,
    origin: { clientId: string; clientSeq: number },
    rejectionReason: string,
  ): ActionEnvelope {
    const nextSeq = this.serverSeq + 1;
    const envelope: ActionEnvelope = {
      channel,
      action,
      serverSeq: nextSeq,
      origin,
      rejectionReason,
    };
    this.transaction(() => {
      this.db.prepare(`UPDATE metadata SET value=? WHERE key='serverSeq'`).run(String(nextSeq));
      this.db.prepare(
        `INSERT INTO actions(server_seq, channel, envelope_json, created_at) VALUES (?, ?, ?, ?)`,
      ).run(nextSeq, channel, JSON.stringify(envelope), new Date().toISOString());
      this.trimActions();
    });
    return envelope;
  }

  actionsAfter(serverSeq: number, subscriptions: Set<string>): ActionEnvelope[] | undefined {
    const oldest = this.db.prepare(`SELECT MIN(server_seq) AS seq FROM actions`).get() as { seq: number | null };
    if (oldest.seq !== null && serverSeq < oldest.seq - 1) return undefined;
    const rows = this.db.prepare(
      `SELECT envelope_json FROM actions WHERE server_seq > ? ORDER BY server_seq`,
    ).all(serverSeq) as Array<{ envelope_json: string }>;
    return rows
      .map((row) => JSON.parse(row.envelope_json) as ActionEnvelope)
      .filter((envelope) => subscriptions.has(envelope.channel));
  }

  repairInterruptedTurns(): ActionEnvelope[] {
    const rows = this.db.prepare(
      `SELECT resource, session_uri, state_json FROM chats`,
    ).all() as Array<{ resource: string; session_uri: string; state_json: string }>;
    const repaired: ActionEnvelope[] = [];
    for (const row of rows) {
      const state = JSON.parse(row.state_json) as ChatState;
      if (!state.activeTurn) continue;
      const parsedStartedAt = typeof state.activeTurn.startedAt === 'string'
        ? Date.parse(state.activeTurn.startedAt)
        : Number.NaN;
      if (!Number.isFinite(parsedStartedAt)) {
        state.activeTurn.startedAt = new Date().toISOString();
        const sanitizedJson = JSON.stringify(state);
        this.transaction(() => {
          this.db.prepare(`UPDATE channels SET state_json=? WHERE uri=?`).run(sanitizedJson, row.resource);
          this.db.prepare(`UPDATE chats SET state_json=? WHERE resource=?`).run(sanitizedJson, row.resource);
        });
      }
      const duration = Number.isFinite(parsedStartedAt)
        ? Math.max(0, Date.now() - parsedStartedAt)
        : 0;
      repaired.push(this.apply(row.resource, {
        type: ActionType.ChatError,
        turnId: state.activeTurn.id,
        duration,
        part: {
          kind: ResponsePartKind.Error,
          error: {
            errorType: 'host_restarted',
            message: 'The host restarted while this turn was active.',
          },
          resumable: false,
        },
      } as StateAction));
      const repairedChat = this.getChat(row.resource);
      if (repairedChat) {
        this.apply(row.session_uri, {
          type: ActionType.SessionChatUpdated,
          chat: row.resource,
          changes: {
            status: repairedChat.state.status,
            activity: repairedChat.state.activity,
            modifiedAt: new Date().toISOString(),
          },
        } as StateAction);
      }
    }
    return repaired;
  }

  private transaction<T>(fn: () => T): T {
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const result = fn();
      this.db.exec('COMMIT');
      return result;
    } catch (error) {
      this.db.exec('ROLLBACK');
      throw error;
    }
  }

  private trimActions(): void {
    this.db.prepare(
      `DELETE FROM actions WHERE server_seq <= (SELECT COALESCE(MAX(server_seq), 0) - ? FROM actions)`,
    ).run(this.replayLimit);
  }

  private migrateTerminalChannelKind(): void {
    const row = this.db.prepare(
      `SELECT sql FROM sqlite_master WHERE type='table' AND name='channels'`,
    ).get() as { sql: string } | undefined;
    if (
      !row
      || (row.sql.includes("'terminal'")
        && row.sql.includes("'changeset'")
        && row.sql.includes("'resourceWatch'"))
    ) {
      return;
    }
    this.db.exec(`
      BEGIN IMMEDIATE;
      ALTER TABLE channels RENAME TO channels_legacy;
      CREATE TABLE channels (
        uri TEXT PRIMARY KEY,
        kind TEXT NOT NULL CHECK (kind IN ('root', 'session', 'chat', 'terminal', 'changeset', 'resourceWatch')),
        state_json TEXT NOT NULL
      );
      INSERT INTO channels(uri, kind, state_json)
        SELECT uri, kind, state_json FROM channels_legacy;
      DROP TABLE channels_legacy;
      COMMIT;
    `);
  }
}

function reduce(kind: ChannelKind, state: ChannelState, action: StateAction): ChannelState {
  if (kind === 'root') return rootReducer(state as RootState, action as never);
  if (kind === 'session') return sessionReducer(state as SessionState, action as never);
  if (kind === 'chat') return chatReducer(state as ChatState, action as never);
  if (kind === 'terminal') return terminalReducer(state as TerminalState, action as never);
  if (kind === 'changeset') return changesetReducer(state as ChangesetState, action as never);
  if (kind === 'resourceWatch') return resourceWatchReducer(state as ResourceWatchState, action as never);
  throw new Error(`Unsupported channel kind ${kind}`);
}
