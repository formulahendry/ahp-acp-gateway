#!/usr/bin/env node

import { readFile, writeFile } from 'node:fs/promises';
import { Readable, Writable } from 'node:stream';
import * as acp from '@agentclientprotocol/sdk';

const sessions = new Map<string, AbortController | undefined>();
const ignoresCancel = new Set<string>();
const sessionConfigValues = new Map<string, Record<string, string | boolean>>();

function configOptions(sessionId: string) {
  const values = sessionConfigValues.get(sessionId) ?? {
    model: 'fake-model',
    mode: 'interactive',
  };
  return [{
    id: 'model',
    name: 'Model',
    category: 'model',
    type: 'select',
    currentValue: values.model,
    options: [
      { value: 'fake-model', name: 'Fake Model' },
      { value: 'fake-model-2', name: 'Fake Model 2' },
    ],
  }, {
    id: 'mode',
    name: 'Mode',
    category: 'mode',
    type: 'select',
    currentValue: values.mode,
    options: [
      { value: 'interactive', name: 'Interactive' },
      { value: 'plan', name: 'Plan' },
    ],
  }];
}

const stream = acp.ndJsonStream(
  Writable.toWeb(process.stdout),
  Readable.toWeb(process.stdin) as ReadableStream<Uint8Array>,
);

acp.agent({ name: 'ahp-acp-gateway-test-agent' })
  .onRequest(acp.methods.agent.initialize, (ctx) => ({
    protocolVersion: ctx.params.protocolVersion,
    agentCapabilities: {
      loadSession: !process.argv.includes('--no-load') && process.env.ACP_NO_LOAD !== '1',
    },
    authMethods: [],
  }))
  .onRequest(acp.methods.agent.session.new, () => {
    if (process.argv.includes('--auth-required')) {
      throw acp.RequestError.authRequired(undefined, 'Authentication is required.');
    }
    const sessionId = crypto.randomUUID();
    sessions.set(sessionId, undefined);
    sessionConfigValues.set(sessionId, {
      model: 'fake-model',
      mode: 'interactive',
    });
    return {
      sessionId,
      models: {
        availableModels: [{
          modelId: 'fake-model',
          name: 'Fake Model',
          description: 'Model discovered during ACP session creation.',
        }],
        currentModelId: 'fake-model',
      },
      configOptions: configOptions(sessionId),
    } as any;
  })
  .onRequest(acp.methods.agent.session.load, async (ctx) => {
    sessions.set(ctx.params.sessionId, undefined);
    if (!sessionConfigValues.has(ctx.params.sessionId)) {
      sessionConfigValues.set(ctx.params.sessionId, {
        model: 'fake-model',
        mode: 'interactive',
      });
    }
    if (process.env.ACP_REPLAY_ON_LOAD === '1') {
      await ctx.client.notify(acp.methods.client.session.update, {
        sessionId: ctx.params.sessionId,
        update: {
          sessionUpdate: 'agent_message_chunk',
          content: { type: 'text', text: 'HISTORICAL_LOAD_REPLAY' },
        },
      });
    }
    return {
      configOptions: configOptions(ctx.params.sessionId),
    } as any;
  })
  .onRequest(acp.methods.agent.session.setConfigOption, (ctx) => {
    const values = sessionConfigValues.get(ctx.params.sessionId) ?? {};
    values[ctx.params.configId] = ctx.params.value;
    sessionConfigValues.set(ctx.params.sessionId, values);
    return {
      configOptions: configOptions(ctx.params.sessionId),
    } as any;
  })
  .onRequest(acp.methods.agent.session.prompt, async (ctx) => {
    const controller = new AbortController();
    sessions.set(ctx.params.sessionId, controller);
    const text = ctx.params.prompt
      .flatMap((part) => part.type === 'text' ? [part.text] : [])
      .join('');

    if (text === 'crash') {
      process.exit(7);
    }

    if (text === 'crash-secret') {
      process.stderr.write('SUPER_SECRET_STDERR_VALUE\n');
      process.exit(8);
    }

    if (text === 'ignore-cancel') {
      ignoresCancel.add(ctx.params.sessionId);
      await new Promise(() => {});
    }

    if (text === 'late') {
      setTimeout(() => {
        void ctx.client.notify(acp.methods.client.session.update, {
          sessionId: ctx.params.sessionId,
          update: {
            sessionUpdate: 'agent_message_chunk',
            content: { type: 'text', text: 'LATE_NOTIFICATION_SHOULD_NOT_APPEAR' },
          },
        }).catch(() => undefined);
      }, 400);
      await delay(2_000);
    }

    if (text === 'slow') {
      await waitForAbort(controller.signal);
      return { stopReason: 'cancelled' };
    }

    if (text === 'control-updates') {
      await ctx.client.notify(acp.methods.client.session.update, {
        sessionId: ctx.params.sessionId,
        update: {
          sessionUpdate: 'available_commands_update',
          availableCommands: [{
            name: 'compact',
            description: 'Compact conversation history',
            input: { hint: 'optional focus' },
          }],
        },
      });
      await ctx.client.notify(acp.methods.client.session.update, {
        sessionId: ctx.params.sessionId,
        update: {
          sessionUpdate: 'config_option_update',
          configOptions: [{
            type: 'select',
            id: 'model',
            name: 'Model',
            category: 'model',
            currentValue: 'control-model',
            options: [{
              value: 'control-model',
              name: 'Control Model',
              description: 'Model from a control update.',
            }],
          }],
        },
      });
      await ctx.client.notify(acp.methods.client.session.update, {
        sessionId: ctx.params.sessionId,
        update: {
          sessionUpdate: 'session_info_update',
          title: 'Control Title',
          updatedAt: new Date().toISOString(),
        },
      });
    }

    await ctx.client.notify(acp.methods.client.session.update, {
      sessionId: ctx.params.sessionId,
      update: {
        sessionUpdate: 'agent_thought_chunk',
        content: { type: 'text', text: 'thinking' },
      },
    });

    if (text === 'interleaved-tools') {
      await ctx.client.notify(acp.methods.client.session.update, {
        sessionId: ctx.params.sessionId,
        update: {
          sessionUpdate: 'agent_message_chunk',
          content: { type: 'text', text: 'before-tool' },
        },
      });
      await ctx.client.notify(acp.methods.client.session.update, {
        sessionId: ctx.params.sessionId,
        update: {
          sessionUpdate: 'tool_call',
          toolCallId: 'tool-1',
          title: 'Write file',
          name: 'write_file',
          kind: 'edit',
          status: 'completed',
          rawInput: { path: 'a.txt' },
          rawOutput: { ok: true },
        },
      });
      await ctx.client.notify(acp.methods.client.session.update, {
        sessionId: ctx.params.sessionId,
        update: {
          sessionUpdate: 'agent_message_chunk',
          content: { type: 'text', text: 'after-tool' },
        },
      });
      sessions.set(ctx.params.sessionId, undefined);
      return { stopReason: 'end_turn' };
    }

    if (text.startsWith('diff-edit:')) {
      const payload = JSON.parse(text.slice('diff-edit:'.length)) as {
        path: string;
        newText: string;
        toolCallId?: string;
      };
      const toolCallId = payload.toolCallId ?? `diff-tool-${crypto.randomUUID()}`;
      let oldText: string | null = null;
      try {
        oldText = await readFile(payload.path, 'utf8');
      } catch {
        oldText = null;
      }
      await writeFile(payload.path, payload.newText, 'utf8');
      await ctx.client.notify(acp.methods.client.session.update, {
        sessionId: ctx.params.sessionId,
        update: {
          sessionUpdate: 'tool_call',
          toolCallId,
          title: 'edit',
          name: 'edit',
          kind: 'edit',
          status: 'pending',
          rawInput: {},
        },
      });
      await ctx.client.notify(acp.methods.client.session.update, {
        sessionId: ctx.params.sessionId,
        update: {
          sessionUpdate: 'tool_call_update',
          toolCallId,
          title: payload.path,
          status: 'in_progress',
          rawInput: { path: payload.path, newText: payload.newText },
        },
      });
      await ctx.client.notify(acp.methods.client.session.update, {
        sessionId: ctx.params.sessionId,
        update: {
          sessionUpdate: 'tool_call_update',
          toolCallId,
          status: 'completed',
          rawOutput: { ok: true },
          content: [
            {
              type: 'diff',
              path: payload.path,
              ...(oldText !== null ? { oldText } : {}),
              newText: payload.newText,
            },
            {
              type: 'content',
              content: {
                type: 'text',
                text: `Edited ${payload.path}`,
              },
            },
          ],
        } as any,
      });
      await ctx.client.notify(acp.methods.client.session.update, {
        sessionId: ctx.params.sessionId,
        update: {
          sessionUpdate: 'agent_message_chunk',
          content: { type: 'text', text: 'diff-done' },
        },
      });
      sessions.set(ctx.params.sessionId, undefined);
      return { stopReason: 'end_turn' };
    }

    if (text === 'usage') {
      await ctx.client.notify(acp.methods.client.session.update, {
        sessionId: ctx.params.sessionId,
        update: {
          sessionUpdate: 'usage_update',
          used: 123,
          size: 456,
        },
      });
    }
    await ctx.client.notify(acp.methods.client.session.update, {
      sessionId: ctx.params.sessionId,
      update: {
        sessionUpdate: 'agent_message_chunk',
        content: {
          type: 'text',
          text: text === 'report-model'
            ? `model:${sessionConfigValues.get(ctx.params.sessionId)?.model ?? 'unset'}`
            : `echo:${text}`,
        },
      },
    });

    if (text === 'permission') {
      await ctx.client.notify(acp.methods.client.session.update, {
        sessionId: ctx.params.sessionId,
        update: {
          sessionUpdate: 'tool_call',
          toolCallId: 'permission-tool',
          title: 'Dangerous test tool',
          name: 'dangerous_test',
          kind: 'execute',
          status: 'pending',
          rawInput: { command: 'echo test' },
        },
      });
      const permission = await ctx.client.request(acp.methods.client.session.requestPermission, {
        sessionId: ctx.params.sessionId,
        toolCall: {
          toolCallId: 'permission-tool',
          title: 'Dangerous test tool',
          name: 'dangerous_test',
          kind: 'execute',
          status: 'pending',
          rawInput: { command: 'echo test' },
        },
        options: [
          { optionId: 'allow', name: 'Allow once', kind: 'allow_once' },
          { optionId: 'reject', name: 'Reject once', kind: 'reject_once' },
        ],
      });
      await ctx.client.notify(acp.methods.client.session.update, {
        sessionId: ctx.params.sessionId,
        update: {
          sessionUpdate: 'agent_message_chunk',
          content: {
            type: 'text',
            text: ` permission:${permission.outcome.outcome === 'selected' ? permission.outcome.optionId : 'cancelled'}`,
          },
        },
      });
    }

    sessions.set(ctx.params.sessionId, undefined);
    if (text === 'exit-after') setTimeout(() => process.exit(0), 250);
    return { stopReason: 'end_turn' };
  })
  .onNotification(acp.methods.agent.session.cancel, (ctx) => {
    if (ignoresCancel.has(ctx.params.sessionId)) return;
    sessions.get(ctx.params.sessionId)?.abort();
  })
  .connect(stream);

function waitForAbort(signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return new Promise((resolve) => signal.addEventListener('abort', () => resolve(), { once: true }));
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
