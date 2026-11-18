import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { channel as diagnosticsChannel } from "node:diagnostics_channel";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import http from "node:http";
import net from "node:net";
import { test } from "node:test";
import { parse } from "dotenv";
import { Live } from "../../src/application/live.js";
import {
  closeRabbit,
  connectRabbit,
  publishConfirmed,
  RabbitConsumer,
} from "../../src/infra/rabbit.js";
import { LiveRuntime } from "../../src/infra/runtime.js";
import { connectRedis, RedisProjection } from "../../src/infra/redis.js";
import { serverDefaults } from "../../src/infra/server.js";
import { delay } from "../../src/infra/time.js";
import { event, MemoryProjection, silo } from "../helpers.js";
import { finalStatus, settleCleanup } from "../teardown.js";

// AMQP method IDs are class-id and method-id packed into a 32-bit word.
const openOk = 0x000a0029;
const close = 0x000a0032;
const closeOk = 0x000a0033;
const channelOpenOk = 0x0014000b;
const cancel = 0x003c001e;
const cancelOk = 0x003c001f;
const exchangeDeclare = 0x0028000a;
const consume = 0x003c0014;
const ack = 0x003c0050;

class RabbitGate {
  readonly sockets = new Set<net.Socket>();
  readonly timers = new Set<NodeJS.Timeout>();
  readonly sent: number[] = [];
  readonly held: number[] = [];
  connections = 0;
  forwardedLate = 0;
  private server = net.createServer((client) => {
    this.connections++;
    const upstream = net.connect(
      Number(this.target.port || 5672),
      this.target.hostname,
    );
    this.track(client, upstream);
    this.track(upstream, client, true);
    this.frames(client, upstream, true);
    this.frames(upstream, client, false);
  });

  constructor(
    private target: URL,
    private hold: (method: number) => number,
  ) {}

  private track(socket: net.Socket, peer: net.Socket, upstream = false) {
    this.sockets.add(socket);
    socket.on("error", () => socket.destroy());
    socket.on("close", () => {
      this.sockets.delete(socket);
      // Simulate a peer that neither replies with CloseOk nor closes TCP.
      if (!upstream || !this.held.includes(closeOk)) peer.destroy();
    });
  }

  private frames(source: net.Socket, target: net.Socket, outgoing: boolean) {
    let header = outgoing;
    let buffer: Buffer<ArrayBufferLike> = Buffer.alloc(0);
    source.on("data", (chunk) => {
      buffer = Buffer.concat([buffer, chunk]);
      if (header) {
        if (buffer.length < 8) return;
        target.write(buffer.subarray(0, 8));
        buffer = buffer.subarray(8);
        header = false;
      }
      while (buffer.length >= 8) {
        const size = buffer.readUInt32BE(3) + 8;
        if (buffer.length < size) return;
        const frame = buffer.subarray(0, size);
        buffer = buffer.subarray(size);
        const method = frame[0] === 1 ? frame.readUInt32BE(7) : 0;
        if (outgoing && method) this.sent.push(method);
        const wait = outgoing ? 0 : this.hold(method);
        if (!wait) target.write(frame);
        else {
          this.held.push(method);
          if (wait < 0) continue;
          const timer = setTimeout(() => {
            this.timers.delete(timer);
            if (target.destroyed) return;
            this.forwardedLate++;
            target.write(frame);
          }, wait);
          this.timers.add(timer);
        }
      }
    });
  }

  async start() {
    await new Promise<void>((r) => this.server.listen(0, "127.0.0.1", r));
    const url = new URL(this.target);
    url.hostname = "127.0.0.1";
    url.port = String((this.server.address() as net.AddressInfo).port);
    return url.href;
  }

  async close() {
    for (const timer of this.timers) clearTimeout(timer);
    for (const socket of this.sockets) socket.destroy();
    await new Promise<void>((r) => this.server.close(() => r()));
  }
}

async function until(check: () => boolean) {
  const end = Date.now() + 6000;
  while (!check()) {
    assert.ok(Date.now() < end, "FIXTURE_TIMEOUT");
    await delay(5);
  }
}

for (const phase of [
  "handshake",
  "runtime-handshake",
  "runtime-read",
  "channel",
  "close",
  "connected",
  "cancel",
  "confirm",
  "post-commit",
] as const)
  test(
    `Rabbit shutdown during ${phase}`,
    {
      skip: process.env.LIVE_INTEGRATION !== "1",
      timeout: 16000,
    },
    async () => {
      assert.ok(process.env.LIVE_INFRA_ENV_PATH);
      const env = {
        ...parse(readFileSync(process.env.LIVE_INFRA_ENV_PATH)),
        ...process.env,
      };
      const target = new URL(
        env.RABBITMQ_URL ??
          `amqp://${encodeURIComponent(env.RABBITMQ_DEFAULT_USER ?? "")}:${encodeURIComponent(env.RABBITMQ_DEFAULT_PASS ?? "")}@127.0.0.1:5672`,
      );
      assert.equal(target.protocol, "amqp:");
      const run = randomUUID().replaceAll("-", "");
      const base = `agrotrac.test.live.shutdown.${run}`;
      const prefix = `agrotrac:live:test:shutdown:${run}:`;
      const redisUrl = env.LIVE_REDIS_URL ?? "redis://127.0.0.1:6379";
      const handshake = phase.endsWith("handshake");
      const reading = phase === "runtime-read";
      const subscribed = !handshake && phase !== "channel" && phase !== "close";
      const expires = phase === "cancel" || phase === "post-commit";
      const ms = handshake ? 5000 : 300;
      const gate = new RabbitGate(target, (method) => {
        if (
          method === closeOk ||
          (phase === "cancel" && method === cancelOk) ||
          (phase === "confirm" && method === ack)
        )
          return -1;
        if (handshake && method === openOk) return 4800;
        if (phase === "channel" && method === channelOpenOk) return 250;
        if (phase === "close" && method === channelOpenOk) return -1;
        return 0;
      });
      const projection = new MemoryProjection();
      const live = new Live(projection, {
        async collect() {
          return { silos: [], windows: [] };
        },
      });
      const url = await gate.start();
      const names = {
        exchange: base,
        queue: base + ".events",
        quarantineExchange: base + ".quarantine",
        quarantineQueue: base + ".rejected",
      };
      let release = () => {};
      let committed = false;
      const heldCommit = new Promise<void>((resolve) => {
        release = resolve;
      });
      const consumer = new RabbitConsumer(
        url,
        live,
        names,
        ms,
        10,
        phase === "post-commit"
          ? async () => {
              committed = true;
              await heldCommit;
            }
          : undefined,
      );
      let reads = 0;
      let readerClosed = false;
      const core = http.createServer((_, response) => {
        reads++;
        response.on("close", () => {
          readerClosed = true;
        });
        response.writeHead(200);
        response.write("[");
      });
      if (reading)
        await new Promise<void>((r) => core.listen(0, "127.0.0.1", r));
      const runtime =
        phase === "runtime-handshake" || reading
          ? new LiveRuntime(
              {
                rabbit: url,
                redis: redisUrl,
                core: `http://127.0.0.1:${(core.address() as net.AddressInfo | null)?.port ?? 1}`,
                prefix,
                port: 0,
                ioMs: ms,
                maxEntities: 10000,
                maxBytes: 10485760,
                scanMs: 60000,
                limits: serverDefaults,
              },
              names,
            )
          : undefined;
      const subject = runtime ?? consumer;
      const clientSockets: net.Socket[] = [];
      const socketEvents = diagnosticsChannel("net.client.socket");
      const proxyPort = Number(new URL(url).port);
      const onSocket = (event: unknown) => {
        const { socket } = event as { socket: net.Socket };
        socket.once("connect", () => {
          if (socket.remotePort === proxyPort) clientSockets.push(socket);
        });
      };
      let publisher: Awaited<ReturnType<typeof connectRabbit>> | undefined;
      let redis: Awaited<ReturnType<typeof connectRedis>> | undefined;
      let redisProjection: RedisProjection | undefined;
      const report: Record<string, unknown> = { run, phase, io_ms: ms };
      let bodyCompleted = false;
      let stopVerified = false;
      socketEvents.subscribe(onSocket);
      try {
        if (phase === "post-commit") {
          redis = await connectRedis(redisUrl);
          redisProjection = new RedisProjection(redis, prefix);
          await redisProjection.initialize();
          live.projection = redisProjection;
        }
        await subject.start();
        await until(() =>
          reading
            ? reads === 1
            : phase === "close"
              ? gate.held.includes(closeOk)
              : subscribed
                ? live.broker
                : gate.held.includes(handshake ? openOk : channelOpenOk),
        );
        if (phase === "confirm" || phase === "post-commit") {
          publisher = await connectRabbit(target.href);
          const channel = await publisher.createConfirmChannel();
          channel.on("error", () => {});
          const input = event(silo());
          const payload = phase === "confirm" ? "{" : JSON.stringify(input);
          await publishConfirmed(
            channel,
            names.exchange,
            "agrotrac.core.silo.created",
            Buffer.from(payload),
            500,
            input.event_id,
          );
          await until(() =>
            phase === "confirm" ? gate.held.includes(ack) : committed,
          );
        }
        if (!runtime) live.drain();
        const before = Date.now();
        const stopped = subject.stop();
        assert.equal(subject.stop(), stopped);
        if (expires) await assert.rejects(stopped, { message: "IO_TIMEOUT" });
        else await stopped;
        stopVerified = true;
        report.stop_ms = Date.now() - before;
        assert.equal(clientSockets.length, 1);
        report.local_sockets_open_at_stop = clientSockets.filter(
          (socket) => !socket.closed,
        ).length;
        assert.equal(report.local_sockets_open_at_stop, 0);
        report.sockets_at_stop = gate.sockets.size;
        // A peer sees TCP shutdown on a later I/O turn than the local socket.
        await until(
          () => gate.sockets.size === 0 && (!reading || readerClosed),
        );
        report.peer_closed_ms = Date.now() - before;
        assert.ok(Number(report.peer_closed_ms) < (handshake ? 500 : ms + 250));
        report.close_sent = gate.sent.includes(close);
        report.close_ok_held = gate.held.includes(closeOk);
        report.cancel_sent = gate.sent.includes(cancel);
        const sentAtStop = gate.sent.length;
        release();
        const observed = handshake ? 10200 : 800;
        await delay(Math.max(0, observed - (Date.now() - before)));
        report.observed_ms = Date.now() - before;
        report.sockets_after_window = gate.sockets.size;
        report.connections = gate.connections;
        report.late_frames = gate.forwardedLate;
        report.proxy_timers = gate.timers.size;
        report.topology = gate.sent.filter((m) => m === exchangeDeclare).length;
        report.consume = gate.sent.filter((m) => m === consume).length;
        if (reading) {
          assert.equal(reads, 1);
          report.reads = reads;
          report.read_closed = readerClosed;
        }
        if (publisher) {
          const channel = await publisher.createChannel();
          const queued = await channel.checkQueue(names.queue);
          assert.equal(queued.messageCount, 1);
          report.ready_after_disconnect = queued.messageCount;
        }
        assert.equal(gate.sent.length, sentAtStop);
        assert.equal(report.sockets_after_window, 0);
        assert.equal(report.connections, 1);
        assert.equal(clientSockets.length, 1);
        assert.equal(report.late_frames, 0);
        assert.equal(report.proxy_timers, 0);
        assert.ok(Number(report.stop_ms) < 10000);
        assert.equal(report.close_sent, phase === "close");
        assert.equal(report.close_ok_held, phase === "close");
        assert.equal(report.topology, subscribed ? 2 : 0);
        assert.equal(report.consume, subscribed ? 1 : 0);
        assert.equal(report.cancel_sent, subscribed);
        assert.equal(consumer.acknowledgements, 0);
        if (redis && redisProjection) {
          report.redis_prefix = prefix;
          report.redis_revision = Number(
            await redis.hget(redisProjection.key, "revision"),
          );
          assert.equal(report.redis_revision, 1);
        }
        assert.equal((runtime?.live ?? live).state, "DRAINING");
        report.acknowledgements = consumer.acknowledgements;
        report.deadline_rejected = expires;
        bodyCompleted = true;
      } finally {
        socketEvents.unsubscribe(onSocket);
        release();
        const cleanup = await settleCleanup([
          {
            name: "consumer",
            async close() {
              if (!stopVerified) await subject.stop();
            },
          },
          { name: "proxy", close: () => gate.close() },
          {
            name: "publisher",
            async close() {
              if (publisher) await closeRabbit(publisher);
            },
          },
          {
            name: "redis",
            async close() {
              await redisProjection?.close();
              redis?.disconnect();
            },
          },
          {
            name: "http",
            async close() {
              if (!reading) return;
              core.closeAllConnections();
              await new Promise<void>((r) => core.close(() => r()));
            },
          },
        ]);
        report.cleanup = cleanup;
        report.status = finalStatus(bodyCompleted, [], cleanup);
        const dir = `tests/fixtures/${run}`;
        mkdirSync(dir, { recursive: true });
        writeFileSync(
          `${dir}/rabbit-shutdown.json`,
          JSON.stringify(report, null, 2),
        );
        console.log(JSON.stringify(report));
      }
      assert.equal(report.status, "passed");
    },
  );
