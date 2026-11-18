import { test } from "node:test";
import assert from "node:assert/strict";
import net from "node:net";
import { randomUUID } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { LiveRuntime } from "../../src/infra/runtime.js";
import { serverDefaults } from "../../src/infra/server.js";
import { delay } from "../../src/infra/time.js";
import { RedisGate } from "../redis-gate.js";
import { LiveServer } from "../../src/infra/server.js";
import { Live } from "../../src/application/live.js";
import { MemoryProjection } from "../helpers.js";
import { WebSocket } from "ws";

async function until(predicate: () => boolean, ms = 6000) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    if (predicate()) return;
    await delay(5);
  }
  throw new Error("FIXTURE_TIMEOUT");
}

test(
  "stop during delayed Redis READY and CONFIG owns and cancels all late resources",
  { skip: process.env.LIVE_INTEGRATION !== "1", timeout: 25000 },
  async () => {
    const run = randomUUID().replaceAll("-", ""),
      start = Date.now();
    const gate = await new RedisGate((name) =>
      name === "INFO" || name === "CONFIG" ? 4900 : 0,
    ).start();
    const amqpSockets = new Set<net.Socket>();
    let amqpConnections = 0;
    const broker = net.createServer((socket) => {
      amqpConnections++;
      amqpSockets.add(socket);
      socket.on("error", () => {});
      socket.on("close", () => amqpSockets.delete(socket));
    });
    await new Promise<void>((r) => broker.listen(0, "127.0.0.1", r));
    const runtime = new LiveRuntime({
      redis: `redis://127.0.0.1:${gate.port}`,
      rabbit: `amqp://127.0.0.1:${(broker.address() as net.AddressInfo).port}`,
      core: "http://127.0.0.1:1",
      prefix: `agrotrac:live:test:shutdown:${run}:`,
      port: 0,
      ioMs: 5000,
      maxEntities: 10000,
      maxBytes: 10485760,
      scanMs: 60000,
      limits: { ...serverDefaults, checkpointMs: 100 },
    });
    const observation: Record<string, unknown> = {
      run,
      kind:
        process.env.LIVE_SHUTDOWN_REPRO === "1"
          ? "before_fix_reproduction"
          : "shutdown_regression",
      info_delay_ms: 4900,
      config_delay_ms: 4900,
    };
    try {
      await runtime.start();
      await until(() => gate.commands.some((c) => c.name === "INFO"));
      const stoppedAt = Date.now();
      await runtime.stop();
      observation.stop_ms = Date.now() - stoppedAt;
      // Keep both controlled endpoints alive beyond both original 4.9 s phases.
      await delay(Math.max(0, 10350 - (Date.now() - start)));
      observation.commands = gate.commands;
      observation.redis_sockets = gate.sockets.size;
      observation.proxy_timers = gate.timers.size;
      observation.amqp_connections = amqpConnections;
      observation.amqp_sockets = amqpSockets.size;
      observation.state = runtime.live.state;
      assert.ok(Number(observation.stop_ms) < 10000);
      assert.equal(
        gate.commands.filter((c) => c.name === "EVAL").length,
        0,
        "initialize must never run after stop",
      );
      assert.equal(amqpConnections, 0, "consumer must never start after stop");
      assert.equal(gate.sockets.size, 0);
      assert.equal(gate.timers.size, 0);
      assert.equal(amqpSockets.size, 0);
      assert.equal(runtime.live.state, "DRAINING");
      observation.status = "passed";
    } catch (e) {
      observation.status = "failed";
      observation.error = e instanceof Error ? e.message : "ASSERTION_FAILED";
      throw e;
    } finally {
      await runtime.stop();
      await gate.close();
      for (const socket of amqpSockets) socket.destroy();
      await new Promise<void>((r) => broker.close(() => r()));
      observation.duration_ms = Date.now() - start;
      const dir = `tests/fixtures/${run}`;
      mkdirSync(dir, { recursive: true });
      writeFileSync(
        `${dir}/shutdown.json`,
        JSON.stringify(observation, null, 2),
      );
      console.log(JSON.stringify(observation));
    }
  },
);

for (const phase of ["CONFIG", "EVAL"])
  test(
    `stop during pending Redis ${phase} cancels initialization without late writes`,
    { skip: process.env.LIVE_INTEGRATION !== "1", timeout: 15000 },
    async () => {
      const run = randomUUID().replaceAll("-", ""),
        gate = await new RedisGate((name) =>
          name === phase ? 4900 : 0,
        ).start(),
        start = Date.now();
      let connections = 0;
      const sockets = new Set<net.Socket>();
      const broker = net.createServer((s) => {
        connections++;
        sockets.add(s);
        s.on("error", () => {});
        s.on("close", () => sockets.delete(s));
      });
      await new Promise<void>((r) => broker.listen(0, "127.0.0.1", r));
      const runtime = new LiveRuntime({
        redis: `redis://127.0.0.1:${gate.port}`,
        rabbit: `amqp://127.0.0.1:${(broker.address() as net.AddressInfo).port}`,
        core: "http://127.0.0.1:1",
        prefix: `agrotrac:live:test:shutdown:${run}:`,
        port: 0,
        ioMs: 5000,
        maxEntities: 10000,
        maxBytes: 10485760,
        scanMs: 60000,
        limits: serverDefaults,
      });
      const observation: Record<string, unknown> = {
        run,
        phase,
        delay_ms: 4900,
      };
      try {
        await runtime.start();
        await until(() => gate.commands.some((c) => c.name === phase));
        const before = gate.commands.length,
          at = Date.now();
        await runtime.stop();
        observation.stop_ms = Date.now() - at;
        await delay(5200);
        observation.commands = gate.commands;
        observation.redis_sockets = gate.sockets.size;
        observation.proxy_timers = gate.timers.size;
        observation.amqp_connections = connections;
        assert.ok(Number(observation.stop_ms) < 10000);
        assert.equal(gate.commands.length, before);
        assert.equal(
          gate.commands.filter((c) => c.name === "EVAL" && c.forwarded).length,
          0,
        );
        assert.equal(connections, 0);
        assert.equal(gate.sockets.size, 0);
        assert.equal(gate.timers.size, 0);
        assert.equal(sockets.size, 0);
        assert.equal(runtime.live.state, "DRAINING");
        observation.status = "passed";
      } catch (e) {
        observation.status = "failed";
        throw e;
      } finally {
        await runtime.stop();
        await gate.close();
        for (const s of sockets) s.destroy();
        await new Promise<void>((r) => broker.close(() => r()));
        observation.duration_ms = Date.now() - start;
        const dir = `tests/fixtures/${run}`;
        mkdirSync(dir, { recursive: true });
        writeFileSync(
          `${dir}/shutdown.json`,
          JSON.stringify(observation, null, 2),
        );
        console.log(JSON.stringify(observation));
      }
    },
  );

test(
  "heartbeat schedules each due ping without 15/10 tick quantization",
  { skip: process.env.LIVE_INTEGRATION !== "1", timeout: 3000 },
  async () => {
    const p = new MemoryProjection(),
      live = new Live(p, {
        async collect() {
          return { silos: [], windows: [] };
        },
      });
    live.connected(true);
    await live.reconcile();
    const server = new LiveServer(live, {
      ...serverDefaults,
      pingMs: 250,
      pongMs: 170,
      checkpointMs: 60000,
    });
    const url = await server.listen(0);
    const ws = new WebSocket(
      url.replace("http:", "ws:") + "/live",
      "agrotrac.live.v1",
    );
    ws.on("error", () => {});
    let opened = 0;
    const pings: number[] = [];
    ws.on("open", () => {
      opened = Date.now();
    });
    ws.on("ping", () => pings.push(Date.now() - opened));
    try {
      await until(() => pings.length >= 2, 2000);
      assert.ok(pings[0] >= 220 && pings[0] < 310, JSON.stringify(pings));
      assert.ok(
        pings[1] - pings[0] >= 220 && pings[1] - pings[0] < 310,
        JSON.stringify(pings),
      );
      console.log(
        JSON.stringify({
          heartbeat_ping_ms: 250,
          pong_ms: 170,
          ping_observations_ms: pings,
        }),
      );
    } finally {
      ws.terminate();
      await server.close();
    }
  },
);
