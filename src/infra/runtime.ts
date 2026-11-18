import type { Redis } from "ioredis";
import { Live } from "../application/live.js";
import type { Projection } from "../application/ports.js";
import type { configuration } from "./config.js";
import { HttpCoreReader } from "./core-http.js";
import { createRedis, openRedis, RedisProjection } from "./redis.js";
import { RabbitConsumer, type Topology } from "./rabbit.js";
import { LiveServer } from "./server.js";
import { deadline, delay } from "./time.js";
const unavailable: Projection = {
  generation: "",
  async apply() {
    throw new Error("REDIS_UNAVAILABLE");
  },
  async read() {
    throw new Error("REDIS_UNAVAILABLE");
  },
  async mark() {
    throw new Error("REDIS_UNAVAILABLE");
  },
  async close() {},
};
export class LiveRuntime {
  readonly abort = new AbortController();
  readonly live: Live;
  readonly server: LiveServer;
  private redis?: Redis;
  private consumer?: RabbitConsumer;
  private loop?: Promise<void>;
  private stopping?: Promise<void>;
  constructor(
    private config: ReturnType<typeof configuration>,
    private names?: Topology,
  ) {
    this.live = new Live(
      unavailable,
      new HttpCoreReader(
        config.core,
        config.ioMs,
        config.maxBytes,
        config.maxEntities,
        this.abort.signal,
      ),
      config.maxEntities,
    );
    this.server = new LiveServer(this.live, {
      ...config.limits,
      maxSnapshotBytes: config.maxBytes,
    });
  }
  async start() {
    const url = await this.server.listen(this.config.port);
    this.loop = this.run();
    void this.loop.catch(() => {});
    return url;
  }
  private async run() {
    let backoff = 1000;
    const { config, live, abort } = this;
    while (!abort.signal.aborted) {
      try {
        abort.signal.throwIfAborted();
        const redis = createRedis(config.redis, config.ioMs);
        this.redis = redis;
        await openRedis(redis, config.ioMs, abort.signal);
        abort.signal.throwIfAborted();
        redis.on("close", () => live.invalidate("REDIS_UNAVAILABLE"));
        const projection = new RedisProjection(
          redis,
          config.prefix,
          undefined,
          config.maxEntities,
          config.maxBytes,
          abort.signal,
        );
        await projection.initialize();
        abort.signal.throwIfAborted();
        live.projection = projection;
        this.consumer = new RabbitConsumer(
          config.rabbit,
          live,
          this.names,
          config.ioMs,
        );
        this.consumer.start();
        while (!abort.signal.aborted && redis.status === "ready") {
          if (
            live.broker &&
            (!live.ready() || Date.now() - live.lastVerified >= config.scanMs)
          ) {
            const result = await live.reconcile();
            abort.signal.throwIfAborted();
            if (result.ok) backoff = 1000;
            else {
              await delay(backoff, abort.signal);
              backoff = Math.min(backoff * 2, 30000);
              if (!abort.signal.aborted && redis.status === "ready")
                await live.barrier(async () => {
                  abort.signal.throwIfAborted();
                  const next = new RedisProjection(
                    redis,
                    config.prefix,
                    undefined,
                    config.maxEntities,
                    config.maxBytes,
                    abort.signal,
                  );
                  await next.initialize();
                  abort.signal.throwIfAborted();
                  live.projection = next;
                });
            }
          }
          await delay(500, abort.signal);
        }
      } catch {
        live.invalidate("DEPENDENCY_UNAVAILABLE");
      } finally {
        try {
          await this.consumer?.stop();
        } finally {
          this.consumer = undefined;
          this.redis?.disconnect();
          this.redis = undefined;
        }
      }
      if (!abort.signal.aborted) {
        await delay(backoff, abort.signal);
        backoff = Math.min(30000, backoff * 2);
      }
    }
  }
  stop() {
    if (!this.stopping) this.stopping = this.shutdown();
    return this.stopping;
  }
  private async shutdown() {
    this.live.drain();
    this.abort.abort();
    // The resource is owned even while connect/CONFIG/initialize is pending.
    this.redis?.disconnect();
    const results = await deadline(
      Promise.allSettled([
        this.server.close(),
        this.consumer?.stop(),
        this.loop,
      ]),
      9500,
    );
    const failures = results.filter(
      (r): r is PromiseRejectedResult => r.status === "rejected",
    );
    if (failures.length)
      throw new AggregateError(
        failures.map((r) => r.reason),
        "SHUTDOWN_FAILED",
      );
  }
}
