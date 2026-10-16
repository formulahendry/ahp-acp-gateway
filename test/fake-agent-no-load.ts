#!/usr/bin/env node

import { Readable, Writable } from 'node:stream';
import * as acp from '@agentclientprotocol/sdk';

const stream = acp.ndJsonStream(
  Writable.toWeb(process.stdout),
  Readable.toWeb(process.stdin) as ReadableStream<Uint8Array>,
);

acp.agent({ name: 'ahp-acp-gateway-no-load-test-agent' })
  .onRequest(acp.methods.agent.initialize, (ctx) => ({
    protocolVersion: ctx.params.protocolVersion,
    agentCapabilities: { loadSession: false },
    authMethods: [],
  }))
  .onRequest(acp.methods.agent.session.new, () => ({
    sessionId: crypto.randomUUID(),
  }))
  .onRequest(acp.methods.agent.session.load, () => ({}))
  .onRequest(acp.methods.agent.session.prompt, async (ctx) => {
    const text = ctx.params.prompt
      .flatMap((part) => part.type === 'text' ? [part.text] : [])
      .join('');
    await ctx.client.notify(acp.methods.client.session.update, {
      sessionId: ctx.params.sessionId,
      update: {
        sessionUpdate: 'agent_message_chunk',
        content: { type: 'text', text: `echo:${text}` },
      },
    });
    return { stopReason: 'end_turn' };
  })
  .onNotification(acp.methods.agent.session.cancel, () => {})
  .connect(stream);
