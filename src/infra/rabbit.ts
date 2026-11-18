import amqp, {
  type Channel,
  type ChannelModel,
  type ConfirmChannel,
  type ConsumeMessage,
} from "amqplib";
import { randomUUID } from "node:crypto";
import type { Socket } from "node:net";
import type { Live } from "../application/live.js";
import type { Quarantine } from "../application/ports.js";
import type { Applied } from "../domain/model.js";
import { abortable, deadline, delay, Serial } from "./time.js";
export type Topology = {
  exchange: string;
  queue: string;
  quarantineExchange: string;
  quarantineQueue: string;
};
export const canonicalTopology: Topology = {
  exchange: "agrotrac.events",
  queue: "agrotrac.live.events",
  quarantineExchange: "agrotrac.live.quarantine",
  quarantineQueue: "agrotrac.live.quarantine.events",
};
function connectionSocket(c: ChannelModel) {
  return (c as unknown as { connection: { stream: Socket } }).connection.stream;
}
export function destroyConnection(c: ChannelModel) {
  connectionSocket(c).destroy(new Error("OWN_CONNECTION_CLOSED"));
}
function disconnectRabbit(c: ChannelModel) {
  const socket = connectionSocket(c);
  if (socket.closed) return Promise.resolve();
  return new Promise<void>((resolve) => {
    socket.once("close", resolve);
    destroyConnection(c);
  });
}
export async function closeRabbit(c: ChannelModel, ms = 5000) {
  await deadline(c.close(), ms).catch(() => {});
  await disconnectRabbit(c);
}
export async function connectRabbit(
  url: string,
  ms = 5000,
  signal?: AbortSignal,
) {
  signal?.throwIfAborted();
  const abort = new AbortController();
  const cancel = () => abort.abort();
  signal?.addEventListener("abort", cancel, { once: true });
  try {
    // amqplib passes this signal to net/tls before the AMQP handshake starts.
    const opening = amqp
      .connect(url, { timeout: ms, signal: abort.signal })
      .then((c) => {
        c.on("error", () => {});
        return c;
      });
    const c = await deadline(opening, ms, destroyConnection);
    if (abort.signal.aborted) {
      await disconnectRabbit(c);
      abort.signal.throwIfAborted();
    }
    return c;
  } catch {
    abort.abort();
    throw new Error("BROKER_UNAVAILABLE");
  } finally {
    signal?.removeEventListener("abort", cancel);
  }
}
export async function topology(ch: Channel, t: Topology, signal?: AbortSignal) {
  const options = { durable: true, autoDelete: false };
  signal?.throwIfAborted();
  await ch.assertExchange(t.exchange, "topic", options);
  signal?.throwIfAborted();
  await ch.assertQueue(t.queue, options);
  signal?.throwIfAborted();
  await ch.bindQueue(t.queue, t.exchange, "agrotrac.core.#");
  signal?.throwIfAborted();
  await ch.assertExchange(t.quarantineExchange, "direct", options);
  signal?.throwIfAborted();
  await ch.assertQueue(t.quarantineQueue, options);
  signal?.throwIfAborted();
  await ch.bindQueue(t.quarantineQueue, t.quarantineExchange, "rejected");
}
export async function publishConfirmed(
  ch: ConfirmChannel,
  exchange: string,
  key: string,
  bytes: Buffer,
  ms = 5000,
  id = randomUUID(),
) {
  let returned = false;
  const onReturn = () => {
    returned = true;
  };
  ch.on("return", onReturn);
  try {
    await deadline(
      new Promise<void>((resolve, reject) => {
        ch.publish(
          exchange,
          key,
          bytes,
          {
            persistent: true,
            mandatory: true,
            contentType: "application/json",
            messageId: id,
          },
          (e) =>
            e
              ? reject(new Error("BROKER_NACK"))
              : returned
                ? reject(new Error("BROKER_UNROUTED"))
                : resolve(),
        );
      }),
      ms,
    );
  } finally {
    ch.removeListener("return", onReturn);
  }
}
export class RabbitQuarantine implements Quarantine {
  private serial = new Serial();
  constructor(
    private ch: ConfirmChannel,
    private t: Topology,
    private ms = 5000,
  ) {}
  publish(
    bytes: Uint8Array,
    routing: string,
    messageId: string | undefined,
    code: string,
  ) {
    return this.serial.run(() =>
      publishConfirmed(
        this.ch,
        this.t.quarantineExchange,
        "rejected",
        Buffer.from(
          JSON.stringify({
            quarantine_id: randomUUID(),
            error: { code },
            quarantined_at: new Date().toISOString(),
            source: {
              exchange: this.t.exchange,
              routing_key: routing,
              message_id: messageId ?? null,
            },
            raw_base64: Buffer.from(bytes).toString("base64"),
          }),
        ),
        this.ms,
      ),
    );
  }
}
export class RabbitConsumer {
  private connection?: ChannelModel;
  private channel?: Channel;
  private tag?: string;
  private abort = new AbortController();
  private loop?: Promise<void>;
  private stopping?: Promise<void>;
  private pending = new Set<Promise<void>>();
  acknowledgements = 0;
  deliveries = 0;
  reconnections = 0;
  constructor(
    private url: string,
    private live: Live,
    readonly names = canonicalTopology,
    private ms = 5000,
    private retryMs = 1000,
    private afterCommit?: (change: Applied) => Promise<void>,
  ) {}
  start() {
    if (!this.loop && !this.abort.signal.aborted) {
      this.loop = this.run();
      // A shutdown failure is observed by stop(), even if start() was fire-and-forget.
      void this.loop.catch(() => {});
    }
  }
  private async run() {
    let backoff = this.retryMs;
    while (!this.abort.signal.aborted) {
      const progress = this.acknowledgements;
      try {
        await this.session();
      } catch {
        this.live.connected(false);
      } finally {
        await this.finishSession();
      }
      if (this.acknowledgements > progress) backoff = this.retryMs;
      if (!this.abort.signal.aborted) {
        await delay(
          backoff + Math.floor((Math.random() * backoff) / 5),
          this.abort.signal,
        );
        backoff = Math.min(30000, backoff * 2);
        if (!this.abort.signal.aborted) this.reconnections++;
      }
    }
  }
  private async finishSession() {
    const signal = this.abort.signal;
    const c = this.connection;
    const ch = this.channel;
    const tag = this.tag;
    const drain = deadline(Promise.allSettled([...this.pending]), this.ms);
    const close = async () => {
      if (!c) return;
      try {
        if (signal.aborted) {
          if (ch && tag && !connectionSocket(c).destroyed)
            await deadline(ch.cancel(tag), this.ms);
        } else {
          await deadline(
            abortable(c.close(), signal, () => destroyConnection(c)),
            this.ms,
          ).catch(() => {});
        }
      } finally {
        await disconnectRabbit(c);
      }
    };
    const results = await Promise.allSettled([drain, close()]);
    this.connection = undefined;
    this.channel = undefined;
    this.tag = undefined;
    if (signal.aborted) {
      for (const result of results) {
        if (result.status === "rejected") throw result.reason;
      }
    }
  }
  private async session() {
    const signal = this.abort.signal;
    const c = await connectRabbit(this.url, this.ms, signal);
    this.connection = c;
    signal.throwIfAborted();
    let failSession!: (e: Error) => void;
    const ended = new Promise<never>((_, reject) => {
      failSession = reject;
    });
    ended.catch(() => {});
    const sessionAbort = new AbortController();
    c.once("close", () => {
      sessionAbort.abort();
      this.live.connected(false);
      failSession(new Error("BROKER_CLOSED"));
    });
    const ch = await deadline(abortable(c.createChannel(), signal), this.ms);
    ch.on("error", () => {});
    ch.once("close", () => failSession(new Error("CHANNEL_CLOSED")));
    signal.throwIfAborted();
    const q = await deadline(
      abortable(c.createConfirmChannel(), signal),
      this.ms,
    );
    q.on("error", () => {});
    q.once("close", () => failSession(new Error("CHANNEL_CLOSED")));
    signal.throwIfAborted();
    this.channel = ch;
    await deadline(
      abortable(topology(ch, this.names, signal), signal),
      this.ms,
    );
    signal.throwIfAborted();
    await deadline(abortable(ch.prefetch(1), signal), this.ms);
    signal.throwIfAborted();
    const quarantine = new RabbitQuarantine(q, this.names, this.ms);
    const subscription = await deadline(
      abortable(
        ch.consume(
          this.names.queue,
          (msg) => {
            if (!msg) {
              failSession(new Error("CONSUMER_CANCELLED"));
              return;
            }
            if (signal.aborted || sessionAbort.signal.aborted) return;
            this.deliveries++;
            const task = this.delivery(
              msg,
              ch,
              quarantine,
              sessionAbort.signal,
            ).catch((e) => failSession(e));
            this.pending.add(task);
            void task.finally(() => this.pending.delete(task));
          },
          { noAck: false },
        ),
        signal,
      ),
      this.ms,
    );
    signal.throwIfAborted();
    this.tag = subscription.consumerTag;
    this.live.connected(true);
    await abortable(ended, signal);
  }
  private async delivery(
    msg: ConsumeMessage,
    ch: Channel,
    q: Quarantine,
    session: AbortSignal,
  ) {
    const signal = AbortSignal.any([session, this.abort.signal]);
    for (let attempt = 0; attempt < 3 && !signal.aborted; attempt++) {
      try {
        await this.live.ingest(
          msg.content,
          msg.fields.routingKey,
          msg.properties.messageId,
          q,
          this.afterCommit,
        );
        if (!signal.aborted) {
          ch.ack(msg);
          this.acknowledgements++;
        }
        return;
      } catch {
        if (signal.aborted) return;
        this.live.invalidate("PROJECTION_UNAVAILABLE");
        if (attempt === 2) throw new Error("DELIVERY_FAILED");
        await delay(250 * 2 ** attempt, signal);
      }
    }
  }
  stop() {
    if (!this.stopping) this.stopping = this.shutdown();
    return this.stopping;
  }
  private async shutdown() {
    this.abort.abort();
    this.live.connected(false);
    await this.loop;
  }
}
