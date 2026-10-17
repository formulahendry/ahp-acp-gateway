import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';
import { Readable, Writable } from 'node:stream';
import * as acp from '@agentclientprotocol/sdk';
import {
  ActionType,
  ConfirmationOptionKind,
  ResponsePartKind,
  ToolCallConfirmationReason,
  type StateAction,
} from '@microsoft/agent-host-protocol';
import type { ProviderConfig } from './config.js';
import type { AcpBinding } from './store.js';

export interface AcpAdapterCallbacks {
  emit(action: StateAction): void;
  bindingChanged(binding: AcpBinding): void;
  ready(): void;
  modelsChanged(models: Array<{ id: string; name: string; description?: string; _meta?: Record<string, unknown> | null }>): void;
  commandsChanged(commands: Array<{ name: string; description: string; argumentHint?: string }>): void;
  configChanged(config: {
    schema: {
      type: 'object';
      properties: Record<string, Record<string, unknown>>;
    };
    values: Record<string, unknown>;
  }): void;
  sessionTitleChanged(title: string): void;
  controlUpdate(kind: string, value: unknown): void;
  crashed(error: Error): void;
  invalidated(): void;
}

export interface AcpAdapter {
  readonly binding: AcpBinding;
  start(): Promise<void>;
  prompt(turnId: string, text: string): Promise<void>;
  cancel(): Promise<boolean>;
  validatePermission(
    turnId: string,
    toolCallId: string,
    approved: boolean,
    selectedOptionId: string | undefined,
  ): string | undefined;
  resolvePermission(
    turnId: string,
    toolCallId: string,
    approved: boolean,
    selectedOptionId: string | undefined,
  ): boolean;
  setConfigOption(configId: string, value: string | boolean): Promise<void>;
  stop(): Promise<void>;
}

export interface AcpAdapterFactory {
  create(
    provider: ProviderConfig,
    binding: AcpBinding,
    workingDirectories: string[],
    callbacks: AcpAdapterCallbacks,
  ): AcpAdapter;
}

export class AcpV1AdapterFactory implements AcpAdapterFactory {
  create(
    provider: ProviderConfig,
    binding: AcpBinding,
    workingDirectories: string[],
    callbacks: AcpAdapterCallbacks,
  ): AcpAdapter {
    return new AcpV1Process(provider, binding, workingDirectories, callbacks);
  }
}

interface PendingPermission {
  turnId: string;
  resolve: (response: acp.RequestPermissionResponse) => void;
  options: acp.PermissionOption[];
}

class AcpV1Process implements AcpAdapter {
  readonly binding: AcpBinding;
  private readonly provider: ProviderConfig;
  private readonly workingDirectories: string[];
  private readonly callbacks: AcpAdapterCallbacks;
  private readonly permissions = new Map<string, PendingPermission>();
  private readonly toolTitles = new Map<string, string>();
  private readonly configOptionTypes = new Map<string, 'select' | 'boolean'>();
  private child: ChildProcessWithoutNullStreams | undefined;
  private context: acp.ClientContext | undefined;
  private connectPromise: Promise<unknown> | undefined;
  private startupError: Error | undefined;
  private ready = false;
  private stopResolve: (() => void) | undefined;
  private stderrTail = '';
  private currentTurn: {
    id: string;
    startedAt: number;
    markdownPart?: string | undefined;
    reasoningPart?: string | undefined;
  } | undefined;
  private cancelRequested = false;
  private promptSettled: Promise<void> | undefined;
  private resolvePromptSettled: (() => void) | undefined;
  private stopping = false;
  private invalidated = false;

  constructor(
    provider: ProviderConfig,
    binding: AcpBinding,
    workingDirectories: string[],
    callbacks: AcpAdapterCallbacks,
  ) {
    this.provider = provider;
    this.binding = { ...binding, provider: provider.id };
    this.workingDirectories = workingDirectories;
    this.callbacks = callbacks;
    if (binding.configOptionTypes) {
      for (const [key, optionType] of Object.entries(binding.configOptionTypes)) {
        if (optionType === 'select' || optionType === 'boolean') {
          this.configOptionTypes.set(key, optionType);
        }
      }
    }
  }

  async start(): Promise<void> {
    if (this.ready) return;
    if (this.connectPromise) {
      try {
        await withTimeout(this.waitForReady(), this.provider.startupTimeoutMs, 'ACP startup');
      } catch (error) {
        const clientError = this.reportStartupFailure(error);
        await this.invalidateProcess();
        throw clientError;
      }
      return;
    }

    const env = { ...process.env, ...this.provider.env };
    const executable = resolveProviderExecutable(this.provider);
    this.child = spawn(executable, this.provider.args, {
      cwd: this.provider.cwd ? resolve(this.provider.cwd) : undefined,
      env,
      stdio: ['pipe', 'pipe', 'pipe'],
      windowsHide: true,
      detached: process.platform !== 'win32',
      shell: requiresCommandShell(executable),
    });
    this.child.stderr.setEncoding('utf8');
    this.child.stderr.on('data', (chunk: string) => {
      this.stderrTail = (this.stderrTail + chunk).slice(-this.provider.stderrTailBytes);
    });
    this.child.once('error', (error) => {
      this.startupError = error;
    });
    this.child.once('exit', (code, signal) => {
      const wasReady = this.ready;
      this.context = undefined;
      this.ready = false;
      for (const permission of this.permissions.values()) {
        permission.resolve({ outcome: { outcome: 'cancelled' } });
      }
      this.permissions.clear();
      if (!this.stopping && !this.invalidated) {
        const error = this.reportFailure(`exited (${signal ?? code ?? 'unknown'})`);
        this.notifyInvalidated();
        if (wasReady) {
          this.callbacks.crashed(error);
        } else {
          this.startupError = error;
        }
      }
      this.stopResolve?.();
    });

    const stream = acp.ndJsonStream(
      Writable.toWeb(this.child.stdin),
      Readable.toWeb(this.child.stdout) as ReadableStream<Uint8Array>,
    );
    const stopPromise = new Promise<void>((resolve) => {
      this.stopResolve = resolve;
    });
    this.connectPromise = acp.client({ name: 'ahp-acp-gateway' })
      .onRequest(acp.methods.client.session.requestPermission, (ctx) =>
        this.handlePermission(ctx.params),
      )
      .onNotification(acp.methods.client.session.update, (ctx) => {
        this.handleUpdate(ctx.params);
      })
      .connectWith(stream, async (ctx) => {
        this.context = ctx;
        const initialized = await ctx.request(acp.methods.agent.initialize, {
          protocolVersion: acp.PROTOCOL_VERSION,
          clientCapabilities: {
            session: {
              configOptions: {
                boolean: {},
              },
            },
          },
        });
        const loadSupported = initialized.agentCapabilities?.loadSession === true;
        let sessionId = this.binding.acpSessionId;
        let contextReset = false;

        if (sessionId && loadSupported) {
          const loaded = await ctx.request(acp.methods.agent.session.load, {
            sessionId,
            ...this.sessionDirectories(),
            mcpServers: [],
          });
          const loadedState = loaded as unknown as { configOptions?: unknown[] } | undefined;
          if (loadedState?.configOptions) {
            this.publishConfigOptions(loadedState.configOptions);
          }
        } else {
          const created = await ctx.request(acp.methods.agent.session.new, {
            ...this.sessionDirectories(),
            mcpServers: [],
          });
          const createdState = created as unknown as { configOptions?: unknown[] };
          this.publishConfigOptions(createdState.configOptions);
          contextReset = Boolean(sessionId);
          sessionId = created.sessionId;
        }
        Object.assign(this.binding, {
          acpSessionId: sessionId,
          loadSupported,
          contextReset,
          configOptionTypes: Object.fromEntries(this.configOptionTypes),
        });
        this.callbacks.bindingChanged({ ...this.binding });
        this.ready = true;
        this.callbacks.ready();
        await stopPromise;
        return null;
      });
    this.connectPromise.catch((error: unknown) => {
      this.startupError = asError(error);
    });
    try {
      await withTimeout(this.waitForReady(), this.provider.startupTimeoutMs, 'ACP startup');
    } catch (error) {
      const clientError = this.reportStartupFailure(error);
      await this.invalidateProcess();
      throw clientError;
    }
  }

  async prompt(turnId: string, text: string): Promise<void> {
    const startedAt = Date.now();
    let requestStarted = false;
    this.cancelRequested = false;
    try {
      await this.start();
      if (this.cancelRequested) return;
      if (!this.context || !this.binding.acpSessionId) {
        throw new Error('ACP process is not initialized');
      }
      this.currentTurn = { id: turnId, startedAt };
      this.promptSettled = new Promise((resolve) => {
        this.resolvePromptSettled = resolve;
      });
      requestStarted = true;
      const result = await withTimeout(
        this.context.request(acp.methods.agent.session.prompt, {
          sessionId: this.binding.acpSessionId,
          prompt: [{ type: 'text', text }],
        }),
        this.provider.requestTimeoutMs,
        'ACP prompt',
      );
      const duration = Date.now() - startedAt;
      if (result.stopReason === 'cancelled') {
        if (!this.cancelRequested) {
          this.callbacks.emit({
            type: ActionType.ChatTurnCancelled,
            turnId,
            duration,
          } as StateAction);
        }
      } else {
        this.callbacks.emit({
          type: ActionType.ChatTurnComplete,
          turnId,
          duration,
          _meta: { acpStopReason: result.stopReason },
        } as StateAction);
      }
    } catch (error) {
      const duration = Date.now() - startedAt;
      const timedOut = error instanceof TimeoutError;
      if (timedOut) {
        this.currentTurn = undefined;
        await this.invalidateProcess();
      }
      if (this.cancelRequested) return;
      if (this.invalidated && requestStarted && !timedOut) return;
      const clientError = timedOut
        ? this.reportFailure('timed out while processing a prompt')
        : this.reportFailure('failed while processing a prompt', error);
      this.callbacks.emit({
        type: ActionType.ChatError,
        turnId,
        duration,
        part: {
          kind: ResponsePartKind.Error,
          error: {
            errorType: timedOut ? 'acp_prompt_timeout' : 'acp_error',
            message: clientError.message,
          },
          resumable: false,
        },
      } as StateAction);
    } finally {
      if (this.currentTurn?.id === turnId) this.currentTurn = undefined;
      this.resolvePromptSettled?.();
      this.resolvePromptSettled = undefined;
    }
  }

  async cancel(): Promise<boolean> {
    this.cancelRequested = true;
    this.closePermissions();
    if (this.context && this.binding.acpSessionId) {
      try {
        await withTimeout(
          this.context.notify(acp.methods.agent.session.cancel, {
            sessionId: this.binding.acpSessionId,
          }),
          Math.min(this.provider.shutdownTimeoutMs, 1_000),
          'ACP cancel notification',
        );
      } catch {
        await this.invalidateProcess();
        return false;
      }
    }
    if (this.promptSettled) {
      try {
        await withTimeout(this.promptSettled, this.provider.shutdownTimeoutMs, 'ACP cancellation');
      } catch {
        await this.invalidateProcess();
        return false;
      }
      this.promptSettled = undefined;
    }
    return true;
  }

  validatePermission(
    turnId: string,
    toolCallId: string,
    approved: boolean,
    selectedOptionId: string | undefined,
  ): string | undefined {
    const pending = this.permissions.get(toolCallId);
    if (!pending || pending.turnId !== turnId) return 'Permission request is unknown or no longer pending';
    if (!selectedOptionId) {
      const defaultOption = pending.options.find((option) =>
        approved ? option.kind.startsWith('allow') : !option.kind.startsWith('allow'));
      if (!defaultOption) return 'A selectedOptionId is required for this permission request';
      return undefined;
    }
    const selected = pending.options.find((option) => option.optionId === selectedOptionId);
    if (!selected) return `Unknown permission option ${selectedOptionId}`;
    const optionApproves = selected.kind.startsWith('allow');
    if (optionApproves !== approved) {
      return `Permission option ${selectedOptionId} contradicts approved=${approved}`;
    }
    return undefined;
  }

  resolvePermission(
    turnId: string,
    toolCallId: string,
    approved: boolean,
    selectedOptionId: string | undefined,
  ): boolean {
    if (this.validatePermission(turnId, toolCallId, approved, selectedOptionId)) return false;
    const pending = this.permissions.get(toolCallId);
    if (!pending) return false;
    this.permissions.delete(toolCallId);
    const selected = selectedOptionId
      ? pending.options.find((option) => option.optionId === selectedOptionId)
      : pending.options.find((option) =>
        approved ? option.kind.startsWith('allow') : !option.kind.startsWith('allow'));
    pending.resolve(selected
      ? { outcome: { outcome: 'selected', optionId: selected.optionId } }
      : { outcome: { outcome: 'cancelled' } });
    return true;
  }

  async setConfigOption(configId: string, value: string | boolean): Promise<void> {
    await this.start();
    if (!this.context || !this.binding.acpSessionId) throw new Error('ACP process is not initialized');
    const optionType = this.configOptionTypes.get(configId);
    if (!optionType) throw new Error(`Unknown ACP session config option ${configId}`);
    if (optionType === 'boolean' && typeof value !== 'boolean') {
      throw new Error(`ACP session config option ${configId} requires a boolean`);
    }
    if (optionType === 'select' && typeof value !== 'string') {
      throw new Error(`ACP session config option ${configId} requires a string value`);
    }
    const response = await this.context.request(
      acp.methods.agent.session.setConfigOption,
      createSetConfigOptionRequest(
        this.binding.acpSessionId,
        configId,
        optionType,
        value,
      ),
    );
    this.publishConfigOptions((response as { configOptions?: unknown[] }).configOptions);
  }

  async stop(): Promise<void> {
    this.stopping = true;
    await this.cancel().catch(() => undefined);
    this.stopResolve?.();
    const child = this.child;
    if (!child || child.exitCode !== null) return;

    terminateProcessTree(child, false);
    await Promise.race([
      new Promise<void>((resolve) => child.once('exit', () => resolve())),
      delay(this.provider.shutdownTimeoutMs),
    ]);
    if (child.exitCode === null) terminateProcessTree(child, true);
  }

  private sessionDirectories(): { cwd: string; additionalDirectories?: string[] } {
    const nativeDirectories = this.workingDirectories.map(toNativeWorkingDirectory);
    const cwd = nativeDirectories[0] ?? (this.provider.cwd ? resolve(this.provider.cwd) : process.cwd());
    const additionalDirectories = nativeDirectories.slice(1);
    return additionalDirectories.length > 0 ? { cwd, additionalDirectories } : { cwd };
  }

  private async waitForReady(): Promise<void> {
    while (!this.ready) {
      if (this.startupError) throw this.startupError;
      if (this.child?.exitCode !== null && this.child?.exitCode !== undefined) {
        throw new Error(`ACP provider exited during startup${this.stderrTail ? `: ${this.stderrTail}` : ''}`);
      }
      await delay(10);
    }
  }

  private handlePermission(params: acp.RequestPermissionRequest): Promise<acp.RequestPermissionResponse> {
    const turn = this.requireTurn();
    turn.markdownPart = undefined;
    turn.reasoningPart = undefined;
    const toolCallId = params.toolCall.toolCallId;
    const title = params.toolCall.title ?? this.toolTitles.get(toolCallId) ?? toolCallId;
    if (!this.toolTitles.has(toolCallId)) {
      this.callbacks.emit({
        type: ActionType.ChatToolCallStart,
        turnId: turn.id,
        toolCallId,
        toolName: params.toolCall.name ?? toolCallId,
        displayName: title,
        intention: title,
      } as StateAction);
      this.toolTitles.set(toolCallId, title);
    }
    this.callbacks.emit({
      type: ActionType.ChatToolCallReady,
      turnId: turn.id,
      toolCallId,
      invocationMessage: title,
      toolInput: params.toolCall.rawInput === undefined ? undefined : JSON.stringify(params.toolCall.rawInput),
      options: params.options.map((option) => ({
        id: option.optionId,
        label: option.name,
        kind: option.kind.startsWith('allow')
          ? ConfirmationOptionKind.Approve
          : ConfirmationOptionKind.Deny,
      })),
    } as StateAction);
    return new Promise((resolve) => {
      this.permissions.set(toolCallId, { turnId: turn.id, resolve, options: params.options });
    });
  }

  private handleUpdate(params: acp.SessionNotification): void {
    if (params.sessionId !== this.binding.acpSessionId) return;
    const update = params.update;
    switch (update.sessionUpdate) {
      case 'available_commands_update':
        this.callbacks.commandsChanged(update.availableCommands.map((command) => ({
          name: command.name,
          description: command.description,
          ...(command.input?.hint ? { argumentHint: command.input.hint } : {}),
        })));
        return;
      case 'current_mode_update':
        this.callbacks.controlUpdate('currentMode', { id: update.currentModeId });
        return;
      case 'config_option_update': {
        this.publishConfigOptions(update.configOptions);
        return;
      }
      case 'session_info_update':
        if (update.title) this.callbacks.sessionTitleChanged(update.title);
        this.callbacks.controlUpdate('sessionInfo', {
          ...(update.updatedAt ? { updatedAt: update.updatedAt } : {}),
        });
        return;
      case 'compaction_update':
      case 'compaction_summary_chunk':
      case 'subagent_update':
        this.callbacks.controlUpdate(update.sessionUpdate, update);
        return;
      default:
        break;
    }

    const turn = this.currentTurn;
    if (!turn) {
      this.callbacks.controlUpdate('lastUnmappedUpdate', {
        kind: update.sessionUpdate,
      });
      return;
    }
    switch (update.sessionUpdate) {
      case 'agent_message_chunk': {
        if (update.content.type !== 'text') return;
        turn.reasoningPart = undefined;
        if (!turn.markdownPart) {
          turn.markdownPart = crypto.randomUUID();
          this.callbacks.emit({
            type: ActionType.ChatResponsePart,
            turnId: turn.id,
            part: { kind: ResponsePartKind.Markdown, id: turn.markdownPart, content: '' },
          } as StateAction);
        }
        this.callbacks.emit({
          type: ActionType.ChatDelta,
          turnId: turn.id,
          partId: turn.markdownPart,
          content: update.content.text,
        } as StateAction);
        break;
      }
      case 'agent_thought_chunk': {
        if (update.content.type !== 'text') return;
        turn.markdownPart = undefined;
        if (!turn.reasoningPart) {
          turn.reasoningPart = crypto.randomUUID();
          this.callbacks.emit({
            type: ActionType.ChatResponsePart,
            turnId: turn.id,
            part: { kind: ResponsePartKind.Reasoning, id: turn.reasoningPart, content: '' },
          } as StateAction);
        }
        this.callbacks.emit({
          type: ActionType.ChatReasoning,
          turnId: turn.id,
          partId: turn.reasoningPart,
          content: update.content.text,
        } as StateAction);
        break;
      }
      case 'plan':
      case 'plan_update': {
        const entries = 'entries' in update ? update.entries : [];
        const markdown = entries.map((entry) => `- [${entry.status === 'completed' ? 'x' : ' '}] ${entry.content}`).join('\n');
        if (markdown) this.appendMarkdown(turn, `\n\n**Plan**\n${markdown}\n`);
        break;
      }
      case 'tool_call': {
        turn.markdownPart = undefined;
        turn.reasoningPart = undefined;
        this.toolTitles.set(update.toolCallId, update.title);
        this.callbacks.emit({
          type: ActionType.ChatToolCallStart,
          turnId: turn.id,
          toolCallId: update.toolCallId,
          toolName: update.name ?? update.toolCallId,
          displayName: update.title,
          intention: update.title,
        } as StateAction);
        if (update.status === 'in_progress' || update.status === 'completed' || update.status === 'failed') {
          this.callbacks.emit({
            type: ActionType.ChatToolCallReady,
            turnId: turn.id,
            toolCallId: update.toolCallId,
            invocationMessage: update.title,
            toolInput: update.rawInput === undefined ? undefined : JSON.stringify(update.rawInput),
            confirmed: ToolCallConfirmationReason.NotNeeded,
          } as StateAction);
        }
        if (update.status === 'completed' || update.status === 'failed') {
          this.completeTool(turn.id, update.toolCallId, update.status === 'completed', update.rawOutput);
        }
        break;
      }
      case 'tool_call_update': {
        const title = update.title ?? this.toolTitles.get(update.toolCallId) ?? update.toolCallId;
        if (update.status === 'in_progress') {
          this.callbacks.emit({
            type: ActionType.ChatToolCallReady,
            turnId: turn.id,
            toolCallId: update.toolCallId,
            invocationMessage: title,
            toolInput: update.rawInput === undefined ? undefined : JSON.stringify(update.rawInput),
            confirmed: ToolCallConfirmationReason.NotNeeded,
          } as StateAction);
        } else if (update.status === 'completed' || update.status === 'failed') {
          this.completeTool(turn.id, update.toolCallId, update.status === 'completed', update.rawOutput);
        }
        break;
      }
      case 'usage_update':
        this.callbacks.emit({
          type: ActionType.ChatUsage,
          turnId: turn.id,
          usage: {
            _meta: {
              acpContext: {
                used: update.used,
                size: update.size,
                ...(update.cost ? { cost: update.cost } : {}),
              },
            },
          },
        } as StateAction);
        break;
      case 'notice':
        turn.markdownPart = undefined;
        turn.reasoningPart = undefined;
        this.callbacks.emit({
          type: ActionType.ChatResponsePart,
          turnId: turn.id,
          part: {
            kind: ResponsePartKind.SystemNotification,
            content: update.description ? `${update.title}: ${update.description}` : update.title,
            _meta: {
              acpNotice: {
                severity: update.severity,
                ...update._meta,
              },
            },
          },
        } as StateAction);
        break;
      default:
        this.callbacks.controlUpdate('lastUnmappedUpdate', {
          kind: update.sessionUpdate,
        });
        break;
    }
  }

  private appendMarkdown(
    turn: { id: string; markdownPart?: string | undefined },
    content: string,
  ): void {
    if (!turn.markdownPart) {
      turn.markdownPart = crypto.randomUUID();
      this.callbacks.emit({
        type: ActionType.ChatResponsePart,
        turnId: turn.id,
        part: { kind: ResponsePartKind.Markdown, id: turn.markdownPart, content: '' },
      } as StateAction);
    }
    this.callbacks.emit({
      type: ActionType.ChatDelta,
      turnId: turn.id,
      partId: turn.markdownPart,
      content,
    } as StateAction);
  }

  private completeTool(turnId: string, toolCallId: string, success: boolean, rawOutput: unknown): void {
    this.callbacks.emit({
      type: ActionType.ChatToolCallComplete,
      turnId,
      toolCallId,
      result: {
        success,
        pastTenseMessage: success ? 'Tool completed' : 'Tool failed',
        ...(rawOutput === undefined ? {} : { structuredContent: { rawOutput } }),
        ...(success ? {} : { error: { message: 'ACP tool reported failure' } }),
      },
    } as StateAction);
  }

  private publishConfigOptions(configOptions: unknown[] | undefined): void {
    if (!configOptions) return;
    const options = configOptions.filter((option): option is Record<string, unknown> =>
      typeof option === 'object' && option !== null && !Array.isArray(option));
    const values = Object.fromEntries(options.flatMap((option) =>
      typeof option.id === 'string'
      && option.id !== 'model'
      && (typeof option.currentValue === 'string' || typeof option.currentValue === 'boolean')
        ? [[option.id, option.currentValue]]
        : []));
    const properties: Record<string, Record<string, unknown>> = {};
    this.configOptionTypes.clear();
    for (const option of options) {
      if (typeof option.id !== 'string' || typeof option.name !== 'string') continue;
      if (option.id === 'model') {
        if (option.type !== 'select' || !Array.isArray(option.options) || typeof option.currentValue !== 'string') {
          continue;
        }
        this.configOptionTypes.set(option.id, 'select');
        const flattenedOptions = flattenConfigSelectOptions(option.options);
        const currentIndex = flattenedOptions.findIndex((item) => item.value === option.currentValue);
        const orderedOptions = currentIndex > 0
          ? [flattenedOptions[currentIndex]!, ...flattenedOptions.slice(0, currentIndex), ...flattenedOptions.slice(currentIndex + 1)]
          : flattenedOptions;
        this.callbacks.modelsChanged(orderedOptions.map((item) => ({
          id: item.value,
          name: item.name,
          ...(item.description ? { description: item.description } : {}),
          ...(item._meta ? { _meta: item._meta } : {}),
        })));
        continue;
      }
      if (option.type === 'boolean' && typeof option.currentValue === 'boolean') {
        this.configOptionTypes.set(option.id, 'boolean');
        properties[option.id] = {
          type: 'boolean',
          title: option.name,
          ...(typeof option.description === 'string' ? { description: option.description } : {}),
          default: option.currentValue,
          sessionMutable: true,
        };
        continue;
      }
      if (option.type !== 'select' || !Array.isArray(option.options) || typeof option.currentValue !== 'string') {
        continue;
      }
      const flattenedOptions = flattenConfigSelectOptions(option.options);
      this.configOptionTypes.set(option.id, 'select');
      properties[option.id] = {
        type: 'string',
        title: option.name,
        ...(typeof option.description === 'string' ? { description: option.description } : {}),
        default: option.currentValue,
        enum: flattenedOptions.map((item) => item.value),
        enumLabels: flattenedOptions.map((item) => item.name),
        enumDescriptions: flattenedOptions.map((item) => item.description ?? ''),
        sessionMutable: true,
      };
    }
    this.binding.configOptionTypes = Object.fromEntries(this.configOptionTypes);
    this.callbacks.bindingChanged({ ...this.binding });
    this.callbacks.configChanged({
      schema: {
        type: 'object',
        properties,
      },
      values,
    });
    this.callbacks.controlUpdate('configOptions', { values });
  }

  private requireTurn(): {
    id: string;
    startedAt: number;
    markdownPart?: string | undefined;
    reasoningPart?: string | undefined;
  } {
    if (!this.currentTurn) throw new Error('ACP requested permission outside an active turn');
    return this.currentTurn;
  }

  private reportFailure(detail: string, cause?: unknown): Error {
    const correlationId = crypto.randomUUID();
    const causeMessage = cause ? `: ${asError(cause).message}` : '';
    const stderr = this.stderrTail ? `\nstderr tail:\n${this.stderrTail}` : '';
    console.error(`[${correlationId}] ACP provider ${this.provider.displayName} (${this.provider.id}) ${detail}${causeMessage}${stderr}`);
    const hint = this.provider.installHint ? ` ${this.provider.installHint}` : '';
    return new Error(`ACP provider ${this.provider.displayName} (${this.provider.id}) failed (reference ${correlationId}).${hint}`);
  }

  private reportStartupFailure(cause: unknown): Error {
    const message = asError(cause).message;
    if (/auth(?:entication)?(?: is)? required|auth_required/i.test(message)) {
      const correlationId = crypto.randomUUID();
      console.error(`[${correlationId}] ACP provider ${this.provider.displayName} (${this.provider.id}) requires authentication: ${message}`);
      const hint = this.provider.authHint ? ` ${this.provider.authHint}` : '';
      return new Error(`ACP provider ${this.provider.displayName} (${this.provider.id}) requires authentication.${hint}`);
    }
    return this.reportFailure('failed during startup', cause);
  }

  private closePermissions(): void {
    for (const permission of this.permissions.values()) {
      permission.resolve({ outcome: { outcome: 'cancelled' } });
    }
    this.permissions.clear();
  }

  private notifyInvalidated(): void {
    if (this.invalidated) return;
    this.invalidated = true;
    this.callbacks.invalidated();
  }

  private async invalidateProcess(): Promise<void> {
    this.currentTurn = undefined;
    this.closePermissions();
    this.notifyInvalidated();
    this.stopResolve?.();
    const child = this.child;
    if (!child || child.exitCode !== null) return;
    terminateProcessTree(child, true);
    await Promise.race([
      new Promise<void>((resolveExit) => child.once('exit', () => resolveExit())),
      delay(Math.min(this.provider.shutdownTimeoutMs, 1_000)),
    ]);
  }
}

export function resolveProviderExecutable(
  provider: Pick<ProviderConfig, 'command'>,
  platform: NodeJS.Platform = process.platform,
): string {
  if (platform === 'win32' && provider.command.toLowerCase() === 'npx') {
    return 'npx.cmd';
  }
  return provider.command;
}

export function requiresCommandShell(
  executable: string,
  platform: NodeJS.Platform = process.platform,
): boolean {
  return platform === 'win32' && /\.(?:cmd|bat)$/i.test(executable);
}

export function unwrapAgentHostUri(uri: string): string {
  let url: URL;
  try {
    url = new URL(uri);
  } catch {
    return uri;
  }
  if (url.protocol !== 'vscode-agent-host:') {
    return uri;
  }
  const encoded = url.searchParams.get('_ah');
  if (!encoded) {
    return `file://${url.pathname}${url.hash}`;
  }
  try {
    const metadata = JSON.parse(Buffer.from(encoded, 'base64url').toString('utf8')) as {
      scheme?: string;
      authority?: string;
      path?: string;
      query?: string;
    };
    const scheme = typeof metadata.scheme === 'string' && metadata.scheme ? metadata.scheme : 'file';
    const authority = typeof metadata.authority === 'string' && metadata.authority ? `//${metadata.authority}` : scheme === 'file' ? '//' : '';
    const path = typeof metadata.path === 'string' ? metadata.path : url.pathname;
    const query = typeof metadata.query === 'string' && metadata.query ? `?${metadata.query}` : '';
    return `${scheme}:${authority}${path}${query}${url.hash}`;
  } catch {
    return `file://${url.pathname}${url.hash}`;
  }
}

export function toNativeWorkingDirectory(uri: string): string {
  const unwrapped = unwrapAgentHostUri(uri);
  let url: URL;
  try {
    url = new URL(unwrapped);
  } catch {
    throw new Error(`Working directory must be an absolute file URI: ${uri}`);
  }
  if (url.protocol !== 'file:') {
    throw new Error(`Working directory URI must use file: ${uri}`);
  }
  return fileURLToPath(url);
}

export function createSetConfigOptionRequest(
  sessionId: string,
  configId: string,
  optionType: 'select' | 'boolean',
  value: string | boolean,
): acp.SetSessionConfigOptionRequest {
  return optionType === 'boolean'
    ? {
        sessionId,
        configId,
        type: 'boolean',
        value: value as boolean,
      }
    : {
        sessionId,
        configId,
        value: value as string,
      };
}

export function terminateProcessTree(child: ChildProcessWithoutNullStreams, force: boolean): void {
  if (!child.pid) return;
  if (process.platform === 'win32') {
    const killer = spawn('taskkill', ['/pid', String(child.pid), '/t', ...(force ? ['/f'] : [])], {
      stdio: 'ignore',
      windowsHide: true,
    });
    killer.unref();
  } else {
    try {
      process.kill(-child.pid, force ? 'SIGKILL' : 'SIGTERM');
    } catch {
      child.kill(force ? 'SIGKILL' : 'SIGTERM');
    }
  }
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new TimeoutError(`${label} timed out after ${timeoutMs}ms`)), timeoutMs);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

class TimeoutError extends Error {}

function flattenConfigSelectOptions(options: unknown[]): Array<{
  value: string;
  name: string;
  description?: string;
  _meta?: Record<string, unknown>;
}> {
  const records = options.flatMap((entry) => {
    if (typeof entry !== 'object' || entry === null || Array.isArray(entry)) return [];
    const record = entry as Record<string, unknown>;
    if (typeof record.value === 'string') return [record];
    return Array.isArray(record.options)
      ? record.options.filter((item): item is Record<string, unknown> =>
          typeof item === 'object' && item !== null && !Array.isArray(item))
      : [];
  });
  return records.flatMap((option) =>
    typeof option.value === 'string' && typeof option.name === 'string'
      ? [{
          value: option.value,
          name: option.name,
          ...(typeof option.description === 'string' ? { description: option.description } : {}),
          ...(typeof option._meta === 'object' && option._meta !== null
            ? { _meta: option._meta as Record<string, unknown> }
            : {}),
        }]
      : []);
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}
