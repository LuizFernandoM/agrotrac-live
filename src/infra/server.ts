import http from "node:http";
import { randomUUID } from "node:crypto";
import { WebSocket, WebSocketServer } from "ws";
import type { Live } from "../application/live.js";
import {
  integer,
  object,
  uuid,
  type Applied,
  type Snapshot,
} from "../domain/model.js";
import { strictJson } from "../domain/json.js";
import { deadline } from "./time.js";
export type ServerLimits = {
  checkpointMs: number;
  pingMs: number;
  pongMs: number;
  sendMs: number;
  bufferBytes: number;
  bufferUpdates: number;
  maxSnapshotBytes: number;
  shutdownMs: number;
};
export const serverDefaults: ServerLimits = {
  checkpointMs: 10000,
  pingMs: 15000,
  pongMs: 10000,
  sendMs: 5000,
  bufferBytes: 2097152,
  bufferUpdates: 100,
  maxSnapshotBytes: 10485760,
  shutdownMs: 10000,
};
type Client = {
  ws: WebSocket;
  syncing: boolean;
  syncInFlight: boolean;
  queue: Applied[];
  queuedBytes: number;
  generation: string;
  revision: number;
  lastResync: number;
  lastPong: number;
  pingAt: number;
  closed: boolean;
};
const frame = (type: string, data: object) => ({
  protocol_version: "1.0.0",
  type,
  ...data,
});
export class LiveServer {
  readonly server: http.Server;
  private ws: WebSocketServer;
  private clients = new Set<Client>();
  private checkpoint: NodeJS.Timeout;
  private heartbeat?: NodeJS.Timeout;
  private closing = false;
  private checking = false;
  constructor(
    private live: Live,
    readonly limits: ServerLimits = serverDefaults,
  ) {
    this.server = http.createServer((req, res) => {
      void this.request(req, res).catch(() => {
        if (!res.destroyed) {
          res.statusCode = 500;
          res.end();
        }
      });
    });
    this.server.requestTimeout = 5000;
    this.server.headersTimeout = 5000;
    this.server.keepAliveTimeout = 1000;
    this.ws = new WebSocketServer({
      noServer: true,
      maxPayload: 4096,
      handleProtocols: (protocols) =>
        protocols.has("agrotrac.live.v1") ? "agrotrac.live.v1" : false,
      perMessageDeflate: false,
    });
    this.server.on("upgrade", (req, socket, head) => {
      socket.on("error", () => {});
      const reject = (status: number) => {
        socket.end(
          `HTTP/1.1 ${status} Rejected\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`,
        );
        setTimeout(() => socket.destroy(), 100).unref();
      };
      if (req.url !== "/live") return reject(404);
      if (!this.live.ready()) return reject(503);
      if (
        !req.headers["sec-websocket-protocol"]
          ?.split(",")
          .map((p) => p.trim())
          .includes("agrotrac.live.v1")
      )
        return reject(400);
      this.ws.handleUpgrade(req, socket, head, (ws) => this.open(ws));
    });
    live.notifications = {
      update: (c) => this.broadcast(c),
      invalidate: (code) => this.invalidate(code),
    };
    this.checkpoint = setInterval(
      () => void this.checkpoints(),
      limits.checkpointMs,
    );
  }
  private async request(req: http.IncomingMessage, res: http.ServerResponse) {
    let correlation: string = randomUUID();
    const supplied = req.headers["x-correlation-id"];
    res.setHeader("content-type", "application/json");
    res.setHeader("cache-control", "no-store");
    const send = (status: number, data: unknown) => {
      if (res.destroyed) return;
      res.statusCode = status;
      res.setHeader("x-correlation-id", correlation);
      if (status === 503) res.setHeader("retry-after", "1");
      res.end(JSON.stringify(data));
    };
    const error = (status: number, code: string) =>
      send(status, {
        error: {
          code,
          message: code,
          details: null,
          correlation_id: correlation,
          timestamp: new Date().toISOString(),
        },
      });
    if (supplied !== undefined && !uuid(supplied))
      return error(400, "INVALID_CORRELATION_ID");
    if (uuid(supplied)) correlation = supplied;
    const url = new URL(req.url ?? "/", "http://127.0.0.1");
    if (req.method !== "GET") return error(404, "NOT_FOUND");
    if (url.pathname === "/health/live") return send(200, { status: "alive" });
    if (url.pathname === "/health/ready")
      return this.live.ready()
        ? send(200, { status: "ready", consistency: "per_aggregate_bounded" })
        : error(503, "LIVE_NOT_READY");
    if (url.pathname !== "/v1/live/snapshot") return error(404, "NOT_FOUND");
    if (url.search) return error(400, "INVALID_QUERY");
    try {
      send(200, await this.live.view());
    } catch (e) {
      error(
        503,
        e instanceof Error && e.message === "SNAPSHOT_TOO_LARGE"
          ? "SNAPSHOT_TOO_LARGE"
          : "LIVE_NOT_READY",
      );
    }
  }
  private open(ws: WebSocket) {
    const c: Client = {
      ws,
      syncing: true,
      syncInFlight: false,
      queue: [],
      queuedBytes: 0,
      generation: "",
      revision: 0,
      lastResync: 0,
      lastPong: Date.now(),
      pingAt: 0,
      closed: false,
    };
    this.clients.add(c);
    this.scheduleHeartbeat();
    ws.on("error", () => {});
    ws.on("close", () => {
      c.closed = true;
      this.clients.delete(c);
      this.scheduleHeartbeat();
    });
    ws.on("pong", () => {
      c.lastPong = Date.now();
      c.pingAt = 0;
      this.scheduleHeartbeat();
    });
    ws.on("message", (data) => {
      try {
        const v = strictJson(Buffer.from(data as Buffer), 4096);
        if (
          !object(v) ||
          Object.keys(v).sort().join(",") !== "protocol_version,type" ||
          v.protocol_version !== "1.0.0" ||
          v.type !== "resync"
        )
          return this.closeClient(c, 1008, "INVALID_FRAME");
        if (Date.now() - c.lastResync < 1000)
          return this.closeClient(c, 1008, "RESYNC_RATE_LIMIT");
        c.lastResync = Date.now();
        void this.sync(c);
      } catch {
        this.closeClient(c, 1007, "INVALID_JSON");
      }
    });
    void this.sync(c);
  }
  private send(c: Client, data: object, snapshot = false): Promise<void> {
    const body = JSON.stringify(data);
    if (c.closed || c.ws.readyState !== WebSocket.OPEN)
      return Promise.reject(new Error("SOCKET_CLOSED"));
    if (
      Buffer.byteLength(body) >
        (snapshot ? this.limits.maxSnapshotBytes : this.limits.bufferBytes) ||
      (!snapshot &&
        c.ws.bufferedAmount + Buffer.byteLength(body) > this.limits.bufferBytes)
    ) {
      this.closeClient(c, 1013, "SLOW_CLIENT");
      return Promise.reject(new Error("SLOW_CLIENT"));
    }
    return deadline(
      new Promise<void>((resolve, reject) =>
        c.ws.send(body, (e) =>
          e ? reject(new Error("SEND_FAILED")) : resolve(),
        ),
      ),
      this.limits.sendMs,
    ).catch((e) => {
      this.closeClient(c, 1013, "SEND_TIMEOUT");
      throw e;
    });
  }
  private async sync(c: Client) {
    if (c.closed || c.syncInFlight) return;
    c.syncInFlight = true;
    c.syncing = true;
    try {
      const snap = await this.live.view();
      await this.send(c, frame("snapshot", { data: snap }), true);
      if (!this.live.ready() || c.closed)
        return this.closeClient(c, 1012, "LIVE_NOT_READY");
      c.generation = snap.generation;
      c.revision = snap.revision;
      const queued = c.queue;
      c.queue = [];
      c.queuedBytes = 0;
      c.syncing = false;
      for (const update of queued)
        if (update.revision > c.revision) this.update(c, update);
    } catch {
      this.closeClient(c, 1013, "LIVE_NOT_READY");
    } finally {
      c.syncInFlight = false;
    }
  }
  private update(c: Client, change: Applied) {
    if (c.closed) return;
    if (c.syncing) {
      c.queue.push(change);
      c.queuedBytes += Buffer.byteLength(JSON.stringify(change));
      if (
        c.queue.length > this.limits.bufferUpdates ||
        c.queuedBytes > this.limits.bufferBytes
      )
        this.closeClient(c, 1013, "SLOW_CLIENT");
      return;
    }
    if (change.revision <= c.revision) return;
    if (
      c.generation !== change.generation ||
      change.previous_revision !== c.revision
    ) {
      void this.send(
        c,
        frame("resync_required", { code: "VERSION_GAP" }),
      ).catch(() => {});
      void this.sync(c);
      return;
    }
    c.revision = change.revision;
    void this.send(
      c,
      frame("update", {
        generation: change.generation,
        previous_revision: change.previous_revision,
        revision: change.revision,
        ...(change.event_id ? { event_id: change.event_id } : {}),
        silos: change.silos,
        windows: change.windows,
      }),
    ).catch(() => {});
  }
  broadcast(change: Applied) {
    for (const c of this.clients) this.update(c, change);
  }
  private invalidate(code: string) {
    for (const c of this.clients)
      this.closeClient(c, code === "SHUTDOWN" ? 1001 : 1012, code);
  }
  private closeClient(c: Client, code: number, reason: string) {
    if (c.closed) return;
    c.closed = true;
    this.clients.delete(c);
    this.scheduleHeartbeat();
    c.ws.close(code, reason);
    const timer = setTimeout(
      () => c.ws.terminate(),
      Math.min(250, this.limits.sendMs),
    );
    timer.unref();
  }
  private async checkpoints() {
    if (this.checking) return;
    this.checking = true;
    try {
      if (!this.live.ready()) {
        this.invalidate("LIVE_NOT_READY");
        return;
      }
      const snap = await this.live.view();
      for (const c of this.clients)
        if (!c.syncing)
          void this.send(
            c,
            frame("checkpoint", {
              generation: snap.generation,
              revision: snap.revision,
            }),
          ).catch(() => {});
    } catch {
      this.live.invalidate("REDIS_UNAVAILABLE");
    } finally {
      this.checking = false;
    }
  }
  private scheduleHeartbeat() {
    clearTimeout(this.heartbeat);
    this.heartbeat = undefined;
    if (this.closing || this.clients.size === 0) return;
    const due = Math.min(
      ...[...this.clients].map((c) =>
        c.pingAt
          ? c.pingAt + this.limits.pongMs
          : c.lastPong + this.limits.pingMs,
      ),
    );
    this.heartbeat = setTimeout(
      () => this.ping(),
      Math.max(1, due - Date.now()),
    );
  }
  private ping() {
    const now = Date.now();
    for (const c of this.clients) {
      if (c.pingAt && now - c.pingAt >= this.limits.pongMs) {
        c.ws.terminate();
        this.clients.delete(c);
        continue;
      }
      if (!c.pingAt && now - c.lastPong >= this.limits.pingMs) {
        c.pingAt = now;
        c.ws.ping();
      }
    }
    this.scheduleHeartbeat();
  }
  async listen(port = 3002) {
    await new Promise<void>((resolve, reject) => {
      this.server.once("error", reject);
      this.server.listen(port, "127.0.0.1", () => {
        this.server.removeListener("error", reject);
        resolve();
      });
    });
    const address = this.server.address();
    return `http://127.0.0.1:${typeof address === "object" && address ? address.port : port}`;
  }
  async close() {
    this.closing = true;
    clearInterval(this.checkpoint);
    clearTimeout(this.heartbeat);
    this.invalidate("SHUTDOWN");
    for (const ws of this.ws.clients) ws.terminate();
    await deadline(
      Promise.all([
        new Promise<void>((r) => this.ws.close(() => r())),
        new Promise<void>((r) => {
          this.server.close(() => r());
          this.server.closeAllConnections();
        }),
      ]),
      this.limits.shutdownMs,
    );
  }
}
