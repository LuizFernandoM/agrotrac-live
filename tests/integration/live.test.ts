import { test } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFileSync, mkdirSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import net from "node:net";
import http from "node:http";
import { parse } from "dotenv";
import { Pool } from "pg";
import { WebSocket } from "ws";
import { Live } from "../../src/application/live.js";
import { connectRedis, RedisProjection } from "../../src/infra/redis.js";
import { HttpCoreReader } from "../../src/infra/core-http.js";
import { LiveServer, serverDefaults } from "../../src/infra/server.js";
import { LiveRuntime } from "../../src/infra/runtime.js";
import {
  RabbitConsumer,
  RabbitQuarantine,
  closeRabbit,
  connectRabbit,
  publishConfirmed,
  topology,
  type Topology,
} from "../../src/infra/rabbit.js";
import { deadline, delay } from "../../src/infra/time.js";
import {
  canonical,
  type Applied,
  type Snapshot,
} from "../../src/domain/model.js";
import { digest, event, silo, window } from "../helpers.js";
import { settleCleanup, finalStatus } from "../teardown.js";
import { RedisGate } from "../redis-gate.js";

async function until(predicate: () => boolean | Promise<boolean>, ms = 5000) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    if (await predicate()) return;
    await delay(10);
  }
  throw new Error("ASSERTION_TIMEOUT");
}
class Client {
  ws: WebSocket;
  frames: any[] = [];
  revision = 0;
  generation = "";
  autoResync = false;
  closed?: number;
  constructor(url: string) {
    this.ws = new WebSocket(
      url.replace("http:", "ws:") + "/live",
      "agrotrac.live.v1",
    );
    this.ws.on("error", () => {});
    this.ws.on("close", (code) => {
      this.closed = code;
    });
    this.ws.on("message", (bytes) => {
      const f = JSON.parse(bytes.toString());
      this.frames.push(f);
      if (f.type === "snapshot") {
        this.revision = f.data.revision;
        this.generation = f.data.generation;
      } else if (f.type === "update") {
        assert.equal(f.generation, this.generation);
        assert.equal(f.previous_revision, this.revision);
        this.revision = f.revision;
      } else if (
        f.type === "checkpoint" &&
        this.autoResync &&
        (f.generation !== this.generation || f.revision > this.revision)
      )
        this.ws.send(
          JSON.stringify({ protocol_version: "1.0.0", type: "resync" }),
        );
    });
  }
  async initial() {
    await until(() => this.frames.some((f) => f.type === "snapshot"));
    return this.frames.find((f) => f.type === "snapshot").data as Snapshot;
  }
  async close() {
    this.ws.terminate();
    await delay(5);
  }
}
class Proxy {
  connections = 0;
  server: net.Server;
  sockets = new Set<net.Socket>();
  frozen = false;
  port = 0;
  constructor(private target: number) {
    this.server = net.createServer((client) => {
      this.connections++;
      this.sockets.add(client);
      client.on("error", () => {});
      client.on("close", () => this.sockets.delete(client));
      const upstream = net.connect(target, "127.0.0.1");
      this.sockets.add(upstream);
      upstream.on("error", () => client.destroy());
      upstream.on("close", () => {
        this.sockets.delete(upstream);
        client.destroy();
      });
      client.on("data", (data) => {
        if (!this.frozen) upstream.write(data);
      });
      upstream.on("data", (data) => {
        if (!this.frozen) client.write(data);
      });
      client.on("close", () => upstream.destroy());
    });
  }
  async start() {
    await new Promise<void>((r) =>
      this.server.listen(0, "127.0.0.1", () => r()),
    );
    this.port = (this.server.address() as net.AddressInfo).port;
    return this;
  }
  cut() {
    for (const s of this.sockets) s.destroy();
  }
  async close() {
    this.cut();
    await new Promise<void>((r) => this.server.close(() => r()));
  }
}

test(
  "isolated real Core HTTP/outbox → Rabbit → Redis → WebSocket",
  { timeout: 180000, skip: process.env.LIVE_INTEGRATION !== "1" },
  async (t) => {
    assert.ok(
      process.env.LIVE_INFRA_ENV_PATH,
      "explicit LIVE_INFRA_ENV_PATH required",
    );
    const env = {
      ...parse(readFileSync(process.env.LIVE_INFRA_ENV_PATH)),
      ...process.env,
    };
    const run = randomUUID().replaceAll("-", ""),
      schema = `live_it_${Date.now()}_${run.slice(0, 8)}`,
      prefix = `agrotrac:live:test:${run}:`;
    const names: Topology = {
      exchange: `agrotrac.test.live.${run}`,
      queue: `agrotrac.test.live.${run}.events`,
      quarantineExchange: `agrotrac.test.live.${run}.quarantine`,
      quarantineQueue: `agrotrac.test.live.${run}.rejected`,
    };
    const rabbit =
      env.RABBITMQ_URL ??
      `amqp://${encodeURIComponent(env.RABBITMQ_DEFAULT_USER ?? "")}:${encodeURIComponent(env.RABBITMQ_DEFAULT_PASS ?? "")}@127.0.0.1:5672`;
    const pool = new Pool({
      host: "127.0.0.1",
      port: 15432,
      user: env.PGUSER ?? env.POSTGRES_USER,
      password: env.PGPASSWORD ?? env.POSTGRES_PASSWORD,
      database: env.PGDATABASE ?? env.POSTGRES_DB,
      max: 5,
      connectionTimeoutMillis: 3000,
    });
    pool.on("error", () => {});
    const coreImport = (path: string) =>
      import(pathToFileURL(resolve("../agrotrac-core/src", path)).href);
    const [
      { Core },
      { PgStore },
      { migrate },
      { http: coreHttp },
      { RabbitPublisher },
      { OutboxWorker },
    ] = await Promise.all(
      [
        "application/core.ts",
        "infra/postgres.ts",
        "infra/migration.ts",
        "infra/http.ts",
        "infra/publisher.ts",
        "infra/outbox.ts",
      ].map(coreImport),
    );
    const redis = await connectRedis(
      env.LIVE_REDIS_URL ?? "redis://127.0.0.1:6379",
      600,
    );
    const connection = await connectRabbit(rabbit, 1000);
    const channel = await connection.createChannel(),
      confirm = await connection.createConfirmChannel();
    channel.on("error", () => {});
    confirm.on("error", () => {});
    const publisher = new RabbitPublisher(
      rabbit,
      {
        exchange: names.exchange,
        liveQueue: names.queue,
        auditQueue: `agrotrac.test.live.${run}.audit`,
      },
      1000,
    );
    const worker = new OutboxWorker(pool, schema, publisher);
    const coreApp = coreHttp(
      new Core(new PgStore(pool, schema), {
        id: randomUUID,
        now: () => new Date().toISOString(),
      }),
    );
    const extras: { close(): Promise<unknown> }[] = [],
      clients: Client[] = [];
    let consumer: RabbitConsumer | undefined, server: LiveServer | undefined;
    const started = Date.now();
    const outcomes: { name: string; status: string }[] = [];
    const scenario = (name: string, work: () => Promise<void>) =>
      t.test(name, async () => {
        try {
          await work();
          outcomes.push({ name, status: "passed" });
        } catch (e) {
          outcomes.push({ name, status: "failed" });
          throw e;
        }
      });
    const report: Record<string, unknown> = {
      run,
      schema,
      prefix,
      names,
      started_at: new Date().toISOString(),
      node: process.version,
      scenarios: outcomes,
    };
    let bodyCompleted = false;
    try {
      assert.equal(
        ((await redis.config("GET", "maxmemory-policy")) as string[])[1],
        "noeviction",
      );
      report.redis_maxmemory_policy = "noeviction";
      await migrate(pool, schema, true);
      const coreUrl = await coreApp.listen({ host: "127.0.0.1", port: 0 });
      await topology(channel, names);
      const projection = new RedisProjection(redis, prefix);
      await projection.initialize();
      const live = new Live(projection, new HttpCoreReader(coreUrl, 500));
      server = new LiveServer(live, {
        ...serverDefaults,
        checkpointMs: 100,
        pingMs: 500,
        pongMs: 300,
        sendMs: 500,
      });
      const liveUrl = await server.listen(0);
      consumer = new RabbitConsumer(rabbit, live, names, 700, 50);
      consumer.start();
      await until(() => live.broker);
      assert.equal((await live.reconcile()).ok, true);
      async function request(method: string, route: string, body?: unknown) {
        const r = await fetch(`${coreUrl}/v1/core/${route}`, {
          method,
          headers:
            body === undefined ? {} : { "content-type": "application/json" },
          body: body === undefined ? undefined : JSON.stringify(body),
          signal: AbortSignal.timeout(3000),
        });
        const data = (await r.json()) as any;
        assert.ok(r.ok, `${r.status} ${data.error?.code}`);
        return data;
      }
      async function drain() {
        while (await worker.dispatchOne()) {}
        await until(
          async () =>
            Number((await channel.checkQueue(names.queue)).messageCount) === 0,
        );
        await until(() => consumer!.acknowledgements >= consumer!.deliveries);
      }
      async function restore() {
        await until(() => live.broker);
        assert.equal((await live.reconcile()).ok, true);
      }
      async function publish(e: any) {
        await publishConfirmed(
          confirm,
          names.exchange,
          e.type,
          Buffer.from(JSON.stringify(e)),
          700,
          e.event_id,
        );
        await until(() => consumer!.acknowledgements >= consumer!.deliveries);
      }
      let s: any, w: any, b: any;
      let captured: any[] = [];
      await scenario(
        "HTTP creates/reserves/cancels/blocks: two clients receive snapshot first and updates without polling",
        async () => {
          const a = new Client(liveUrl),
            c = new Client(liveUrl);
          clients.push(a, c);
          await Promise.all([a.initial(), c.initial()]);
          s = await request("POST", "silos", {
            codigo: randomUUID(),
            nome: "Live Integration",
            tipo_grao: "SOJA",
            capacidade_total_kg: 10000,
          });
          w = await request("POST", "windows", {
            silo_id: s.id,
            inicio_em: new Date(Date.now() + 60000).toISOString(),
            termino_em: new Date(Date.now() + 3600000).toISOString(),
            capacidade_maxima_kg: 10000,
          });
          await drain();
          b = await request("POST", "bookings", {
            window_id: w.id,
            peso_estimado_kg: 2000,
            placa: "ABC1D23",
            motorista_nome: "TEST PERSONAL",
            motorista_documento: "PRIVATE DOCUMENT",
          });
          await drain();
          await until(() =>
            a.frames.some(
              (f) =>
                f.type === "update" &&
                f.silos.some((x: any) => x.saldo_reservado_kg === 2000),
            ),
          );
          await until(() => c.revision === a.revision);
          assert.equal((await live.view()).silos[0].saldo_reservado_kg, 2000);
          await request("DELETE", `bookings/${b.id}`);
          await drain();
          await request("PATCH", `windows/${w.id}/status`, {
            status: "BLOCKED",
          });
          await drain();
          await until(() =>
            a.frames.some(
              (f) =>
                f.type === "update" &&
                f.windows.some((x: any) => x.status === "BLOCKED"),
            ),
          );
          const snap = await live.view();
          assert.equal(snap.silos[0].saldo_reservado_kg, 0);
          assert.equal(snap.silos[0].version, 3);
          assert.equal(snap.windows[0].version, 4);
          captured = (
            await pool.query(
              `SELECT payload FROM "${schema}".outbox_events ORDER BY created_at,event_id`,
            )
          ).rows.map((row) => row.payload);
          assert.equal(captured.length, 5);
          const cache = JSON.stringify(await redis.hgetall(projection.key)),
            frames = JSON.stringify(a.frames);
          for (const data of [cache, frames])
            for (const forbidden of [
              "PRIVATE DOCUMENT",
              "TEST PERSONAL",
              "motorista_documento",
              "motorista_nome",
              "placa",
              "booking_id",
            ])
              assert.equal(data.includes(forbidden), false);
          const rest = await fetch(liveUrl + "/v1/live/snapshot");
          assert.equal(rest.status, 200);
          assert.equal(rest.headers.get("cache-control"), "no-store");
          assert.equal(
            (await fetch(liveUrl + "/v1/live/snapshot?x=1")).status,
            400,
          );
          assert.equal((await fetch(liveUrl + "/health/ready")).status, 200);
        },
      );
      await scenario(
        "Lua real: replay/stale, mixed independent versions and aggregate conflict have no partial writes",
        async () => {
          const original = await live.view(),
            rev = original.revision;
          for (const e of [...captured].reverse()) await publish(e);
          await delay(80);
          assert.equal((await live.view()).revision, rev);
          const p = new RedisProjection(redis, prefix);
          await p.initialize();
          const a = silo(8),
            x = window(a, 2);
          assert.equal((await p.apply({ silo: a, window: x })).ok, true);
          const mixed = await p.apply({
            silo: { ...a, version: 7 },
            window: { ...x, version: 3 },
          });
          assert.ok(mixed.ok);
          assert.equal(mixed.value.silos.length, 0);
          assert.equal(mixed.value.windows[0].version, 3);
          const before = await redis.hgetall(p.key);
          const conflict = await p.apply({
            silo: { ...a, version: 9 },
            window: { ...x, version: 3, status: "BLOCKED" },
          });
          assert.equal(conflict.ok, false);
          assert.deepEqual(await redis.hgetall(p.key), before);
          const e = event(a),
            change = { silo: a, event_id: e.event_id, digest: digest(e) };
          await p.apply(change);
          assert.equal((await p.apply(change)).ok, true);
          const bad = await p.apply({
            ...change,
            digest: digest({ ...e, occurred_at: "2026-01-01T00:00:00Z" }),
          });
          assert.deepEqual(bad, { ok: false, code: "EVENT_ID_CONFLICT" });
          const operations = await Promise.all(
            Array.from({ length: 25 }, (_, i) =>
              p.apply({ silo: { ...a, version: 10 + i } }),
            ),
          );
          assert.ok(operations.every((r) => r.ok));
          assert.equal(
            JSON.parse((await redis.hgetall(p.key))[`s:${a.id}`]).version,
            34,
          );
          await redis.hset(p.key, `v:w:${x.id}`, "bad");
          const corruptBefore = await redis.hgetall(p.key);
          assert.equal(
            (
              await p.apply({
                silo: { ...a, version: 35 },
                window: { ...x, version: 4 },
              })
            ).ok,
            false,
          );
          assert.deepEqual(await redis.hgetall(p.key), corruptBefore);
          const wrong = new RedisProjection(redis, prefix);
          await redis.set(wrong.key, "fixture-wrong-type");
          assert.deepEqual(await wrong.apply({ silo: a }), {
            ok: false,
            code: "CORRUPT_PROJECTION",
          });
          assert.equal(await redis.get(wrong.key), "fixture-wrong-type");
          const precise = new RedisProjection(redis, prefix);
          await precise.initialize();
          await precise.apply({
            silo: {
              ...a,
              version: Number.MAX_SAFE_INTEGER,
              capacidade_total_kg: Number.MAX_SAFE_INTEGER,
              saldo_disponivel_kg: Number.MAX_SAFE_INTEGER,
            },
          });
          assert.equal(
            JSON.parse((await redis.hgetall(precise.key))[`s:${a.id}`]).version,
            Number.MAX_SAFE_INTEGER,
          );
          await redis.hset(
            precise.key,
            "revision",
            String(Number.MAX_SAFE_INTEGER),
          );
          const atLimit = await redis.hgetall(precise.key);
          assert.equal((await precise.apply({ window: x })).ok, false);
          assert.deepEqual(await redis.hgetall(precise.key), atLimit);
        },
      );
      await scenario(
        "poison and event ID conflict quarantined durably; original state preserved and valid messages still drain",
        async () => {
          const before = await redis.hgetall(projection.key),
            first = captured[0];
          await publish({ ...first, occurred_at: "2026-01-01T00:00:00Z" });
          await until(() => !live.ready());
          await until(
            async () =>
              Number(
                (await channel.checkQueue(names.quarantineQueue)).messageCount,
              ) >= 1,
          );
          assert.deepEqual(await redis.hgetall(projection.key), before);
          const poison = Buffer.from('{"bad":');
          await publishConfirmed(
            confirm,
            names.exchange,
            "agrotrac.core.bad",
            poison,
            700,
          );
          await until(
            async () =>
              Number(
                (await channel.checkQueue(names.quarantineQueue)).messageCount,
              ) >= 2,
          );
          await drain();
          await restore();
          assert.equal((await live.view()).silos[0].version, 3);
          assert.equal((await fetch(liveUrl + "/health/live")).status, 200);
        },
      );
      await scenario(
        "mandatory return is failure even with broker confirm; poison is not ACKed before durable quarantine",
        async () => {
          await assert.rejects(
            publishConfirmed(
              confirm,
              names.quarantineExchange,
              "no-binding",
              Buffer.from("{}"),
              500,
            ),
            /UNROUTED/,
          );
          let ack = false;
          const q = new RabbitQuarantine(
            confirm,
            { ...names, quarantineExchange: "missing.exchange" },
            500,
          );
          await assert.rejects(
            (async () => {
              await live.ingest(
                Buffer.from("{"),
                "agrotrac.core.bad",
                undefined,
                q,
              );
              ack = true;
            })(),
          );
          assert.equal(ack, false);
          await restore();
        },
      );
      // The prior missing exchange intentionally closes its own confirm channel.
      await scenario(
        "consumer with unrouted quarantine retains original, recovers route and then drains valid event",
        async () => {
          const n = {
            exchange: `${names.exchange}.poison`,
            queue: `${names.queue}.poison`,
            quarantineExchange: `${names.quarantineExchange}.poison`,
            quarantineQueue: `${names.quarantineQueue}.poison`,
          };
          const p = new RedisProjection(redis, prefix);
          await p.initialize();
          const l = new Live(p, new HttpCoreReader(coreUrl, 500));
          const c = new RabbitConsumer(rabbit, l, n, 150, 50);
          extras.push({ close: () => c.stop() });
          c.start();
          await until(() => l.broker);
          await channel.unbindQueue(
            n.quarantineQueue,
            n.quarantineExchange,
            "rejected",
          );
          const pub = await connection.createConfirmChannel();
          pub.on("error", () => {});
          await publishConfirmed(
            pub,
            n.exchange,
            "agrotrac.core.bad",
            Buffer.from("{"),
            300,
          );
          await until(() => c.deliveries > 0);
          await delay(100);
          assert.equal(c.acknowledgements, 0);
          assert.equal(
            (await channel.checkQueue(n.quarantineQueue)).messageCount,
            0,
          );
          await channel.bindQueue(
            n.quarantineQueue,
            n.quarantineExchange,
            "rejected",
          );
          await until(() => c.acknowledgements === 1, 5000);
          assert.equal(
            (await channel.checkQueue(n.quarantineQueue)).messageCount,
            1,
          );
          const e = event(silo());
          await publishConfirmed(
            pub,
            n.exchange,
            e.type,
            Buffer.from(JSON.stringify(e)),
            300,
            e.event_id,
          );
          await until(() => c.acknowledgements === 2);
          assert.equal(
            (await redis.hgetall(p.key))[`s:${e.payload.silo_id}`] !==
              undefined,
            true,
          );
          await c.stop();
          await pub.close();
        },
      );
      await scenario(
        "commit then crash hook: no broadcast; reconnect snapshot repairs committed state",
        async () => {
          const c = new Client(liveUrl);
          clients.push(c);
          await c.initial();
          const a = await request("GET", `silos/${s.id}`),
            e = event({ ...a, version: a.version + 1 });
          let crashed = false;
          await assert.rejects(
            live.ingest(
              Buffer.from(JSON.stringify(e)),
              e.type,
              e.event_id,
              {
                async publish() {
                  assert.fail();
                },
              },
              async () => {
                crashed = true;
                throw new Error("INJECTED_CRASH_AFTER_COMMIT");
              },
            ),
          );
          assert.equal(crashed, true);
          const revision = (await projection.read()).revision;
          assert.ok(revision > c.revision);
          await c.close();
          const reconnect = new Client(liveUrl);
          clients.push(reconnect);
          assert.equal((await reconnect.initial()).revision, revision);
          // Restore the fixture's Core truth into a new generation; no data removal.
          const next = new RedisProjection(redis, prefix);
          await next.initialize();
          live.invalidate("REBUILD");
          live.projection = next;
          await restore();
        },
      );
      await scenario(
        "last broadcast lost without crash: checkpoint causes client resync, no successor event or REST polling",
        async () => {
          const c = new Client(liveUrl);
          clients.push(c);
          await c.initial();
          c.autoResync = true;
          const a = await request("GET", `silos/${s.id}`),
            e = event({ ...a, version: a.version + 1 });
          const notify = live.notifications;
          live.notifications = { ...notify, update() {} };
          await live.ingest(
            Buffer.from(JSON.stringify(e)),
            e.type,
            e.event_id,
            {
              async publish() {
                assert.fail();
              },
            },
          );
          live.notifications = notify;
          await until(
            () => c.frames.filter((f) => f.type === "snapshot").length === 2,
          );
          assert.equal(c.revision, (await live.projection.read()).revision);
          const next = new RedisProjection(redis, prefix);
          await next.initialize();
          live.invalidate("REBUILD");
          live.projection = next;
          await restore();
        },
      );
      await scenario(
        "opening snapshot and concurrent update are consistently ordered",
        async () => {
          const p = live.projection,
            read = p.read.bind(p);
          let release!: () => void,
            entered = false;
          const block = new Promise<void>((r) => (release = r));
          let once = true;
          p.read = async () => {
            const snap = await read();
            if (once) {
              once = false;
              entered = true;
              await block;
            }
            return snap;
          };
          const c = new Client(liveUrl);
          clients.push(c);
          await until(() => entered);
          const a = await request("GET", `silos/${s.id}`),
            e = event({ ...a, version: a.version + 1 });
          await live.ingest(
            Buffer.from(JSON.stringify(e)),
            e.type,
            e.event_id,
            {
              async publish() {
                assert.fail();
              },
            },
          );
          release();
          await c.initial();
          await until(() => c.frames.some((f) => f.type === "update"));
          assert.equal(c.revision, (await p.read()).revision);
          assert.equal(c.frames[0].type, "snapshot");
          p.read = read;
          const next = new RedisProjection(redis, prefix);
          await next.initialize();
          live.invalidate("REBUILD");
          live.projection = next;
          await restore();
        },
      );
      await scenario(
        "Redis loss is a fresh namespace rebuild from real Core HTTP, incomplete and oversize never ready",
        async () => {
          const next = new RedisProjection(redis, prefix);
          await next.initialize();
          live.invalidate("REDIS_LOSS");
          live.projection = next;
          assert.equal(
            (await fetch(liveUrl + "/v1/live/snapshot")).status,
            503,
          );
          await restore();
          const state = await live.view();
          assert.equal(state.silos.length, 1);
          assert.equal(state.windows.length, 1);
          assert.equal(state.windows[0].status, "BLOCKED");
          assert.notEqual(state.generation, projection.generation);
          const bounded = new Live(next, new HttpCoreReader(coreUrl, 300, 2));
          bounded.connected(true);
          assert.equal((await bounded.reconcile()).ok, false);
          assert.equal(bounded.ready(), false);
          const partial = new Live(next, {
            async collect() {
              throw new Error("PARTIAL_HTTP");
            },
          });
          partial.connected(true);
          assert.equal((await partial.reconcile()).ok, false);
          assert.equal(partial.ready(), false);
          let v = 1;
          const changing = new Live(next, {
            async collect() {
              const a = silo(v++);
              return { silos: [a], windows: [] };
            },
          });
          changing.connected(true);
          assert.equal((await changing.reconcile()).ok, false);
          assert.equal(changing.ready(), false);
        },
      );
      await scenario(
        "real Redis proxy failure: timeout before ACK, explicit 503, reconnect and snapshot",
        async () => {
          const proxy = await new Proxy(6379).start();
          extras.push(proxy);
          const r = await connectRedis(`redis://127.0.0.1:${proxy.port}`, 120);
          extras.push({
            async close() {
              r.disconnect();
            },
          });
          const p = new RedisProjection(r, prefix);
          await p.initialize();
          const isolated = new Live(p, new HttpCoreReader(coreUrl, 300));
          isolated.connected(true);
          assert.equal((await isolated.reconcile()).ok, true);
          proxy.frozen = true;
          const e = event(silo());
          let ack = false;
          await assert.rejects(
            (async () => {
              await isolated.ingest(
                Buffer.from(JSON.stringify(e)),
                e.type,
                e.event_id,
                {
                  async publish() {
                    assert.fail();
                  },
                },
              );
              ack = true;
            })(),
          );
          assert.equal(ack, false);
          await assert.rejects(isolated.view());
          assert.equal(isolated.ready(), false);
          proxy.frozen = false;
          const recovered = await connectRedis(
            `redis://127.0.0.1:${proxy.port}`,
            300,
          );
          extras.push({
            async close() {
              recovered.disconnect();
            },
          });
          const replacement = new RedisProjection(recovered, prefix);
          await replacement.initialize();
          isolated.projection = replacement;
          await isolated.reconcile();
          assert.equal(isolated.ready(), true);
        },
      );
      await scenario(
        "Rabbit proxy failure/reconnection keeps delivery and deadlines; silent handshake bounded",
        async () => {
          const proxy = await new Proxy(5672).start();
          extras.push(proxy);
          const via = new URL(rabbit);
          via.port = String(proxy.port);
          const n = {
            exchange: `${names.exchange}.recover`,
            queue: `${names.queue}.recover`,
            quarantineExchange: `${names.quarantineExchange}.recover`,
            quarantineQueue: `${names.quarantineQueue}.recover`,
          };
          const p = new RedisProjection(redis, prefix);
          await p.initialize();
          const l = new Live(p, new HttpCoreReader(coreUrl, 300));
          const c = new RabbitConsumer(via.href, l, n, 150, 50);
          extras.push({ close: () => c.stop() });
          c.start();
          await until(() => l.broker);
          await l.reconcile();
          proxy.frozen = true;
          proxy.cut();
          await until(() => !l.ready());
          assert.equal(l.broker, false);
          await delay(250);
          proxy.frozen = false;
          await until(() => l.broker, 5000);
          assert.ok(c.reconnections > 0);
          await c.stop();
          const silent = net.createServer((socket) => {
            socket.on("error", () => {});
            silentSockets.add(socket);
            socket.on("close", () => silentSockets.delete(socket));
          });
          const silentSockets = new Set<net.Socket>();
          await new Promise<void>((r) =>
            silent.listen(0, "127.0.0.1", () => r()),
          );
          extras.push({
            async close() {
              for (const s of silentSockets) s.destroy();
              await new Promise<void>((r) => silent.close(() => r()));
            },
          });
          const u = new URL(rabbit);
          u.port = String((silent.address() as net.AddressInfo).port);
          const time = Date.now();
          await assert.rejects(connectRabbit(u.href, 120));
          assert.ok(Date.now() - time < 1000);
          await delay(150);
        },
      );
      await scenario(
        "Core HTTP proxy timeout never promotes candidate; late close and shutdown bounded",
        async () => {
          const proxy = await new Proxy(Number(new URL(coreUrl).port)).start();
          extras.push(proxy);
          proxy.frozen = true;
          const candidate = new RedisProjection(redis, prefix);
          await candidate.initialize();
          const l = new Live(
            candidate,
            new HttpCoreReader(`http://127.0.0.1:${proxy.port}`, 120),
          );
          l.connected(true);
          const time = Date.now();
          assert.equal((await l.reconcile()).ok, false);
          assert.ok(Date.now() - time < 1000);
          assert.equal(l.ready(), false);
          proxy.frozen = false;
          assert.equal((await l.reconcile()).ok, true);
          const h = http.createServer((_q, _r) => {});
          await new Promise<void>((r) => h.listen(0, "127.0.0.1", () => r()));
          const sockets = new Set<net.Socket>();
          h.on("connection", (s) => {
            sockets.add(s);
            s.on("close", () => sockets.delete(s));
          });
          extras.push({
            async close() {
              for (const s of sockets) s.destroy();
              await new Promise<void>((r) => h.close(() => r()));
            },
          });
          await assert.rejects(
            new HttpCoreReader(
              `http://127.0.0.1:${(h.address() as net.AddressInfo).port}`,
              100,
            ).collect(),
          );
        },
      );
      await scenario(
        "production runtime automatically rebuilds a missing generation and reconnects Redis, shutdown under 10 seconds",
        async () => {
          const proxy = await new Proxy(6379).start();
          extras.push(proxy);
          const n = {
            exchange: `${names.exchange}.runtime`,
            queue: `${names.queue}.runtime`,
            quarantineExchange: `${names.quarantineExchange}.runtime`,
            quarantineQueue: `${names.quarantineQueue}.runtime`,
          };
          const runtime = new LiveRuntime(
            {
              redis: `redis://127.0.0.1:${proxy.port}`,
              rabbit,
              core: coreUrl,
              prefix: `${prefix}runtime:`,
              port: 0,
              ioMs: 200,
              maxEntities: 10000,
              maxBytes: 10485760,
              scanMs: 500,
              limits: { ...serverDefaults, checkpointMs: 100, sendMs: 200 },
            },
            n,
          );
          extras.push({ close: () => runtime.stop() });
          const url = await runtime.start();
          await until(() => runtime.live.ready(), 8000);
          const initial = runtime.live.projection.generation;
          // Switch this fixture to a nonexistent hash (no deletion of Redis data).
          const missing = new RedisProjection(redis, `${prefix}runtime:`);
          runtime.live.projection = missing;
          await assert.rejects(runtime.live.view());
          assert.equal((await fetch(url + "/health/ready")).status, 503);
          await until(() => runtime.live.ready(), 8000);
          assert.notEqual(runtime.live.projection.generation, initial);
          assert.notEqual(
            runtime.live.projection.generation,
            missing.generation,
          );
          const generation = runtime.live.projection.generation;
          proxy.frozen = true;
          await assert.rejects(runtime.live.view());
          assert.equal((await fetch(url + "/v1/live/snapshot")).status, 503);
          proxy.frozen = false;
          await until(
            () =>
              runtime.live.ready() &&
              runtime.live.projection.generation !== generation,
            8000,
          );
          const time = Date.now();
          await runtime.stop();
          assert.ok(Date.now() - time < 10000);
          await assert.rejects(fetch(url + "/health/live"));
        },
      );
      await scenario(
        "stop during rebuild aborts pending Redis apply and does not start late work",
        async () => {
          let hold = false;
          const gate = await new RedisGate((name, args) =>
            hold && name === "EVAL" && args[1].includes("local t=") ? 4900 : 0,
          ).start();
          extras.push(gate);
          const rabbitProxy = await new Proxy(5672).start();
          extras.push(rabbitProxy);
          const rabbitUrl = new URL(rabbit);
          rabbitUrl.port = String(rabbitProxy.port);
          const n = {
            exchange: `${names.exchange}.stoprebuild`,
            queue: `${names.queue}.stoprebuild`,
            quarantineExchange: `${names.quarantineExchange}.stoprebuild`,
            quarantineQueue: `${names.quarantineQueue}.stoprebuild`,
          };
          const runtime = new LiveRuntime(
            {
              redis: `redis://127.0.0.1:${gate.port}`,
              rabbit: rabbitUrl.href,
              core: coreUrl,
              prefix: `${prefix}stoprebuild:`,
              port: 0,
              ioMs: 5000,
              maxEntities: 10000,
              maxBytes: 10485760,
              scanMs: 500,
              limits: { ...serverDefaults, checkpointMs: 100 },
            },
            n,
          );
          extras.push({ close: () => runtime.stop() });
          await runtime.start();
          await until(() => runtime.live.ready());
          hold = true;
          runtime.live.invalidate("FIXTURE_REBUILD");
          await until(() =>
            gate.commands.some((c) => c.name === "EVAL" && !c.forwarded),
          );
          const commands = gate.commands.length,
            connections = rabbitProxy.connections,
            forwarded = gate.commands.filter((c) => c.forwarded).length,
            at = Date.now();
          await runtime.stop();
          const stopMs = Date.now() - at;
          await delay(5200);
          assert.ok(stopMs < 10000);
          assert.equal(gate.commands.length, commands);
          assert.equal(
            gate.commands.filter((c) => c.forwarded).length,
            forwarded,
          );
          assert.equal(rabbitProxy.connections, connections);
          assert.equal(gate.sockets.size, 0);
          assert.equal(gate.timers.size, 0);
          assert.equal(rabbitProxy.sockets.size, 0);
          assert.equal(runtime.live.state, "DRAINING");
          report.rebuild_stop = {
            stop_ms: stopMs,
            observation_after_stop_ms: 5200,
            late_commands: 0,
            late_amqp_connections: 0,
            redis_sockets: 0,
            amqp_sockets: 0,
          };
        },
      );
      await scenario(
        "silent Rabbit confirm expires and own channel reconnects without stopping broker",
        async () => {
          const proxy = await new Proxy(5672).start();
          extras.push(proxy);
          const u = new URL(rabbit);
          u.port = String(proxy.port);
          const c = await connectRabbit(u.href, 200);
          const ch = await c.createConfirmChannel();
          ch.on("error", () => {});
          proxy.frozen = true;
          const time = Date.now();
          await assert.rejects(
            publishConfirmed(
              ch,
              names.exchange,
              "agrotrac.core.silo.created",
              Buffer.from("{}"),
              120,
            ),
          );
          assert.ok(Date.now() - time < 1000);
          await closeRabbit(c, 100);
          proxy.frozen = false;
          const recovered = await connectRabbit(u.href, 300);
          const rc = await recovered.createConfirmChannel();
          rc.on("error", () => {});
          await publishConfirmed(
            rc,
            names.exchange + ".runtime",
            "agrotrac.core.bad",
            Buffer.from("{"),
            300,
          );
          await closeRabbit(recovered, 100);
        },
      );
      await scenario(
        "WS subprotocol refusal, gap resync, oversized frame and heartbeat timeout",
        async () => {
          const transport = new LiveServer(live, {
            ...serverDefaults,
            checkpointMs: 100,
            pingMs: 100,
            pongMs: 70,
            sendMs: 100,
          });
          extras.push(transport);
          const url = await transport.listen(0);
          const refused = new WebSocket(
            url.replace("http:", "ws:") + "/live",
            "unsupported",
          );
          refused.on("error", () => {});
          let status = 0;
          refused.on("unexpected-response", (_req, response) => {
            status = response.statusCode ?? 0;
            response.resume();
            refused.terminate();
          });
          await until(() => status === 400);
          const c = new Client(url);
          clients.push(c);
          const snap = await c.initial();
          transport.broadcast({
            kind: "APPLIED",
            generation: snap.generation,
            previous_revision: snap.revision + 1,
            revision: snap.revision + 2,
            silos: [],
            windows: [],
          });
          await until(
            () =>
              c.frames.some((f) => f.type === "resync_required") &&
              c.frames.filter((f) => f.type === "snapshot").length === 2,
          );
          const big = new Client(url);
          clients.push(big);
          await big.initial();
          big.ws.send("x".repeat(4097));
          await until(() => big.closed === 1009);
          const noPong = new WebSocket(
            url.replace("http:", "ws:") + "/live",
            "agrotrac.live.v1",
            { autoPong: false },
          );
          noPong.on("error", () => {});
          let dead = false;
          noPong.on("close", () => {
            dead = true;
          });
          await until(() => dead, 2000);
          await transport.close();
        },
      );
      await scenario(
        "slow socket, independent healthy client, invalid frames and WS deadlines",
        async () => {
          const transport = new LiveServer(live, {
            ...serverDefaults,
            checkpointMs: 60000,
            pingMs: 60000,
            pongMs: 60000,
            sendMs: 100,
            bufferBytes: 4096,
          });
          const url = await transport.listen(0);
          extras.push(transport);
          const slow = new Client(url),
            healthy = new Client(url);
          clients.push(slow, healthy);
          await Promise.all([slow.initial(), healthy.initial()]);
          (slow.ws as unknown as { _socket: net.Socket })._socket.pause();
          const current = await live.view();
          let revision = current.revision;
          let closed = false;
          slow.ws.once("close", () => {
            closed = true;
          });
          // Transport-only load injection, not fake Core events or cache mutations.
          for (let i = 0; i < 5000; i++) {
            const update: Applied = {
              kind: "APPLIED",
              generation: current.generation,
              previous_revision: revision,
              revision: ++revision,
              silos: [
                { ...current.silos[0], nome: "x".repeat(100), version: i + 10 },
              ],
              windows: [],
            };
            transport.broadcast(update);
            if (i % 10 === 0) await delay(1);
          }
          await until(
            () => healthy.frames.filter((f) => f.type === "update").length > 0,
          );
          (slow.ws as unknown as { _socket: net.Socket })._socket.resume();
          await until(() => closed || slow.closed !== undefined);
          assert.equal(healthy.closed, undefined);
          const invalid = new Client(url);
          clients.push(invalid);
          await invalid.initial();
          invalid.ws.send("{");
          await until(() => invalid.closed === 1007);
          const unsupported = new Client(url);
          clients.push(unsupported);
          await unsupported.initial();
          unsupported.ws.send(
            JSON.stringify({ protocol_version: "9", type: "resync" }),
          );
          await until(() => unsupported.closed === 1008);
          const shutdown = Date.now();
          await transport.close();
          assert.ok(Date.now() - shutdown < 1000);
        },
      );
      report.outbox = (
        await pool.query(
          `SELECT count(*)::int total,count(*) FILTER(WHERE status='PUBLISHED')::int published FROM "${schema}".outbox_events`,
        )
      ).rows[0];
      report.deliveries = consumer.deliveries;
      report.acknowledgements = consumer.acknowledgements;
      report.quarantine_messages = (
        await channel.checkQueue(names.quarantineQueue)
      ).messageCount;
      bodyCompleted = true;
    } finally {
      const shutdown = Date.now();
      const cleanup = await settleCleanup([
        ...clients.map((c, i) => ({
          name: `client_${i}`,
          close: () => c.close(),
        })),
        { name: "consumer", close: async () => consumer?.stop() },
        { name: "server", close: async () => server?.close() },
        ...extras
          .reverse()
          .map((e, i) => ({ name: `extra_${i}`, close: () => e.close() })),
        { name: "publisher", close: () => publisher.close() },
        { name: "core_http", close: () => coreApp.close() },
        { name: "rabbit", close: () => closeRabbit(connection, 500) },
        { name: "redis", close: async () => redis.disconnect() },
        { name: "pool", close: () => pool.end() },
      ]);
      report.body_completed = bodyCompleted;
      report.cleanup = cleanup;
      report.status = finalStatus(bodyCompleted, outcomes, cleanup);
      report.runner_exit = "only_observable_by_parent_runner";
      report.shutdown_ms = Date.now() - shutdown;
      report.duration_ms = Date.now() - started;
      report.finished_at = new Date().toISOString();
      const dir = `tests/fixtures/${run}`;
      mkdirSync(dir, { recursive: true });
      writeFileSync(`${dir}/result.json`, JSON.stringify(report, null, 2));
      console.log(JSON.stringify(report));
      if (cleanup.some((c) => c.status === "failed"))
        throw new Error("TEARDOWN_FAILED");
    }
  },
);
