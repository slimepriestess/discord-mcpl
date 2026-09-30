/**
 * Review probes for dm#33 (not for merge). Both assert the behavior the PR
 * promises — gap messages delivered after a fresh gateway session — in two
 * orderings the implementation doesn't cover:
 *
 * Probe 1 — a live message that lands after the fresh session but before the
 * sweep's fetch reaches that channel advances the watermark past the gap
 * (server.ts:3193), so the sweep's `after: watermark` fetch skips it.
 *
 * Probe 2 — a fresh gateway session while NO host is connected returns early
 * from queueSweep without re-arming sweepDone; the next host connection's
 * sweep is once-per-process-guarded, so the gap is never swept.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as net from 'node:net';
import { writeFileSync, unlinkSync, existsSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { McplConnection, method } from '@animalabs/mcpl-core';
import type { McplInitializeParams, PushEventParams } from '@animalabs/mcpl-core';
import { DiscordMcplServer } from '../src/server.js';
import type { DiscordAdapter, DiscordMessageData } from '../src/discord-adapter.js';

type HistoryMsg = {
  id: string; authorId: string; authorName: string; isBot: boolean;
  content: string; cleanContent: string; attachments: never[];
  mentionsBot: boolean; timestamp: Date; reactions?: never[];
};

/** Minimal mock, mirroring test/server.test.ts, with an `after`-aware,
 *  per-channel-holdable fetchHistory so the sweep's interleave with live
 *  traffic can be pinned deterministically. */
class ProbeAdapter {
  private _messageHandler?: (msg: DiscordMessageData) => void;
  sessionRestoredHandler?: () => void;
  sentMessages: Array<{ channelId: string; content: string }> = [];
  /** channelId -> full history (ids numeric-comparable). */
  history: Record<string, HistoryMsg[]> = {};
  /** channelId -> deferred gate the next fetchHistory call awaits. */
  holds: Record<string, Promise<void>> = {};
  fetchLog: Array<{ channelId: string; after?: string }> = [];

  get isConnected(): boolean { return true; }
  whenReady(): Promise<void> { return Promise.resolve(); }
  onSessionRestored(h: () => void): void { this.sessionRestoredHandler = h; }
  get botUserId(): string | null { return 'bot_123'; }
  onMessage(h: (msg: DiscordMessageData) => void): void { this._messageHandler = h; }
  onMessageEdit(): void {} onMessageDelete(): void {} onReaction(): void {}
  onReady(): void {} onChannelCreate(): void {} onChannelDelete(): void {}
  onGuildCreate(): void {} onChannelAvailable(): void {}
  getGuildName(): string { return 'Test Guild'; }
  async sendMessage(channelId: string, content: string): Promise<{ messageId: string }> {
    this.sentMessages.push({ channelId, content });
    return { messageId: `m_${this.sentMessages.length}` };
  }
  async sendDM(): Promise<{ messageId: string }> { return { messageId: 'dm_1' }; }
  async editMessage(): Promise<void> {} async deleteMessage(): Promise<void> {}
  async addReaction(): Promise<void> {} async removeReaction(): Promise<void> {}
  async fetchHistory(channelId: string, opts?: { after?: string; limit?: number }): Promise<HistoryMsg[]> {
    this.fetchLog.push({ channelId, after: opts?.after });
    const hold = this.holds[channelId];
    if (hold) { delete this.holds[channelId]; await hold; }
    const all = this.history[channelId] ?? [];
    return opts?.after ? all.filter((m) => Number(m.id) > Number(opts.after)) : all;
  }
  async fetchAround(): Promise<HistoryMsg[]> { return []; }
  channelMeta = { name: 'general', guildId: 'g1', guildName: 'Test Guild', isDM: false };
  async getChannelMeta(): Promise<ProbeAdapter['channelMeta']> { return this.channelMeta; }
  getCachedChannelMeta(): ProbeAdapter['channelMeta'] | null { return this.channelMeta; }
  async listGuilds(): Promise<Array<{ id: string; name: string; memberCount: number }>> {
    return [{ id: 'g1', name: 'Test Guild', memberCount: 10 }];
  }
  async listChannels(): Promise<Array<{ id: string; name: string; type: string; label: string }>> {
    return [
      { id: 'c0', name: 'quiet', type: 'text', label: '#quiet (TestGuild)' },
      { id: 'c1', name: 'general', type: 'text', label: '#general (TestGuild)' },
    ];
  }
  getTextChannels(): Array<{ guildId: string; guildName: string; channel: { id: string; name: string; type: string; label: string } }> {
    return [
      { guildId: 'g1', guildName: 'Test Guild', channel: { id: 'c0', name: 'quiet', type: 'text', label: '#quiet (TestGuild)' } },
      { guildId: 'g1', guildName: 'Test Guild', channel: { id: 'c1', name: 'general', type: 'text', label: '#general (TestGuild)' } },
    ];
  }
  async createTextChannel(): Promise<{ id: string; name: string; type: string; label: string }> {
    return { id: 'c_new', name: 'new-channel', type: 'text', label: '#new-channel (TestGuild)' };
  }
  async deleteChannel(): Promise<void> {}
  channelMembersToReturn = { channelId: 'c1', members: [], total: 0 };
  async listChannelMembers(): Promise<ProbeAdapter['channelMembersToReturn']> { return this.channelMembersToReturn; }
  simulateMessage(msg: DiscordMessageData): void { this._messageHandler?.(msg); }
}

async function createTestPair(): Promise<{ client: McplConnection; serverConn: McplConnection; discord: ProbeAdapter }> {
  const tcpServer = net.createServer();
  tcpServer.listen(0, '127.0.0.1');
  await new Promise<void>((r) => tcpServer.once('listening', r));
  const addr = tcpServer.address() as net.AddressInfo;
  const [serverConn, clientSocket] = await Promise.all([
    McplConnection.acceptTcp(tcpServer),
    new Promise<net.Socket>((resolve, reject) => {
      const s = net.createConnection({ host: '127.0.0.1', port: addr.port }, () => resolve(s));
      s.once('error', reject);
    }),
  ]);
  const client = McplConnection.fromTcp(clientSocket);
  tcpServer.close();
  return { client, serverConn, discord: new ProbeAdapter() };
}

async function mcplHandshake(client: McplConnection): Promise<void> {
  const params: McplInitializeParams = {
    protocolVersion: '2024-11-05',
    capabilities: { experimental: { mcpl: { version: '0.4', pushEvents: true, channels: true, rollback: true } } },
    clientInfo: { name: 'probe-client', version: '0.1.0' },
  };
  const result = await client.sendRequest('initialize', params as unknown as Record<string, unknown>);
  void result;
  client.sendNotification('notifications/initialized');
  // 0.5 host behavior: settle the §5.3 policy — this releases the server's
  // policy-gated channel registration + reconnect sweep (see serve()).
  const receipt = (await client.sendRequest('featureSets/update', {
    enabled: ['discord.messaging', 'discord.channels', 'discord.history', 'discord.subscriptions'],
  })) as { accepted: boolean };
  assert.equal(receipt.accepted, true);
}

const gapMention = (id: string): HistoryMsg => ({
  id, authorId: 'u2', authorName: 'Bob', isBot: false,
  content: '<@bot_123> missed you', cleanContent: '@bot missed you',
  attachments: [], mentionsBot: true, timestamp: new Date(),
});

const liveMsg = (id: string, channelId: string): DiscordMessageData => ({
  id, content: '<@bot_123> live one', cleanContent: '@bot live one',
  authorId: 'u3', authorName: 'Carol', isBot: false,
  channelId, channelName: 'general', guildId: 'g1', guildName: 'Test Guild',
  mentions: ['bot_123'], attachments: [], timestamp: new Date(),
} as unknown as DiscordMessageData);

/** Drain until a push/event with the given eventId substring, or fail after
 *  `max` host-bound messages. Responds to every request along the way. */
async function expectPush(client: McplConnection, idPart: string, max = 6): Promise<PushEventParams> {
  for (let i = 0; i < max; i++) {
    const m = await client.nextMessage();
    if (m.type !== 'request') continue;
    client.sendResponse(m.request.id, {});
    if (m.request.method === method.PUSH_EVENT) {
      const p = m.request.params as PushEventParams;
      if (p.eventId?.includes(idPart)) return p;
    }
  }
  assert.fail(`no push/event containing "${idPart}" within ${max} messages`);
}

describe('probe: dm#33 gap coverage', () => {
  it('probe 1: gap survives a live message racing the gateway-session sweep', async () => {
    const wmPath = join(tmpdir(), `dm33-probe1-${process.pid}.json`);
    writeFileSync(wmPath, JSON.stringify({ watermarks: { c0: '50', c1: '100' }, dmChannels: [] }));
    process.env.DISCORD_WATERMARK_FILE = wmPath;
    try {
      const { client, serverConn, discord } = await createTestPair();
      const server = new DiscordMcplServer(discord as unknown as DiscordAdapter);
      const serverPromise = server.serve(serverConn);
      await mcplHandshake(client);
      const reg = await client.nextMessage();
      if (reg.type === 'request') client.sendResponse(reg.request.id, {});
      // Let the host-connect sweep finish over empty history.
      await new Promise((r) => setTimeout(r, 50));

      // The Mac slept: mentions 101/102 landed in #general during the gap.
      discord.fetchLog.length = 0; // isolate the session sweep's fetches
      discord.history['c1'] = [gapMention('101'), gapMention('102')];
      // Fresh gateway session. The sweep will reach c0 first — hold it there,
      // exactly long enough for one live message to arrive in c1.
      let releaseC0!: () => void;
      discord.holds['c0'] = new Promise<void>((r) => { releaseC0 = r; });
      discord.sessionRestoredHandler!();
      await new Promise((r) => setTimeout(r, 20)); // sweep is now parked on c0
      discord.simulateMessage(liveMsg('105', 'c1')); // live traffic on the new session
      const live = await Promise.race([
        expectPush(client, '105'),
        new Promise<never>((_, rej) => setTimeout(() => rej(new Error('live message never forwarded')), 3_000)),
      ]); // forwarded; watermark c1 -> 105
      assert.ok(live);
      releaseC0();

      // The PR's promise: the gap (101/102) is delivered by the session sweep.
      const gap = await Promise.race([
        expectPush(client, '_c1_', 4).then(() => true),
        new Promise<false>((r) => setTimeout(() => r(false), 1_500)),
      ]);
      assert.equal(discord.fetchLog.some((f) => f.channelId === 'c1' && Number(f.after) <= 100), true,
        `the sweep must scan c1 from the pre-gap watermark (fetches: ${JSON.stringify(discord.fetchLog)})`);
      assert.equal(gap, true, 'gap mentions 101/102 must be delivered despite the live message');

      client.close();
      await serverPromise;
    } finally {
      delete process.env.DISCORD_WATERMARK_FILE;
      if (existsSync(wmPath)) unlinkSync(wmPath);
    }
  });

  it('probe 2: a fresh session with no host attached still re-arms the sweep', async () => {
    const wmPath = join(tmpdir(), `dm33-probe2-${process.pid}.json`);
    writeFileSync(wmPath, JSON.stringify({ watermarks: { c1: '100' }, dmChannels: [] }));
    process.env.DISCORD_WATERMARK_FILE = wmPath;
    try {
      const first = await createTestPair();
      const server = new DiscordMcplServer(first.discord as unknown as DiscordAdapter);
      const serve1 = server.serve(first.serverConn);
      await mcplHandshake(first.client);
      const reg1 = await first.client.nextMessage();
      if (reg1.type === 'request') first.client.sendResponse(reg1.request.id, {});
      await new Promise((r) => setTimeout(r, 50)); // host-connect sweep done (empty)

      // Host goes away; then the Mac sleeps and wakes: fresh session, no host.
      first.client.close();
      await serve1;
      first.discord.history['c1'] = [gapMention('105')];
      first.discord.sessionRestoredHandler!();
      await new Promise((r) => setTimeout(r, 50)); // early-return path runs

      // Host comes back on a new connection (same process, same server).
      const pair2 = await createTestPair();
      const serve2 = server.serve(pair2.serverConn);
      // Rewire the SAME adapter events is not needed: server keeps first.discord.
      await mcplHandshake(pair2.client);
      const reg2 = await pair2.client.nextMessage();
      if (reg2.type === 'request') pair2.client.sendResponse(reg2.request.id, {});

      const gap = await Promise.race([
        expectPush(pair2.client, '_c1_105', 4).then(() => true),
        new Promise<false>((r) => setTimeout(() => r(false), 1_500)),
      ]);
      assert.equal(gap, true,
        'the reconnecting host must receive the gap that opened while no host was attached');

      pair2.client.close();
      await serve2;
    } finally {
      delete process.env.DISCORD_WATERMARK_FILE;
      if (existsSync(wmPath)) unlinkSync(wmPath);
    }
  });
});
