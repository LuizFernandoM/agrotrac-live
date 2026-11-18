import { createHash } from "node:crypto";
import {
  canonical,
  fail,
  ok,
  snapshot,
  validateEvent,
  type Applied,
  type Change,
  type Result,
  type Snapshot,
  type Silo,
  type Window,
} from "../domain/model.js";
import { strictJson } from "../domain/json.js";
import type {
  CoreReader,
  Notifications,
  Projection,
  Quarantine,
} from "./ports.js";
// This class has no HTTP/Redis/AMQP/WS framework dependency.
export class Live {
  private tail: Promise<unknown> = Promise.resolve();
  barrier<T>(work: () => Promise<T>): Promise<T> {
    const task = this.tail.then(work);
    this.tail = task.catch(() => {});
    return task;
  }
  state: "BOOTSTRAPPING" | "READY" | "UNAVAILABLE" | "DRAINING" =
    "BOOTSTRAPPING";
  broker = false;
  lastVerified = 0;
  fault = 0;
  notifications: Notifications = { update() {}, invalidate() {} };
  constructor(
    public projection: Projection,
    private core: CoreReader,
    private maxEntities = 10000,
    private now = () => Date.now(),
  ) {}
  invalidate(code = "LIVE_NOT_READY") {
    if (this.state === "DRAINING") return;
    this.state = "UNAVAILABLE";
    this.fault++;
    this.notifications.invalidate(code);
  }
  connected(value: boolean) {
    this.broker = value;
    if (!value) this.invalidate("BROKER_UNAVAILABLE");
  }
  ready() {
    return (
      this.state === "READY" &&
      this.broker &&
      this.now() - this.lastVerified <= 120000
    );
  }
  async view(): Promise<Snapshot> {
    if (!this.ready()) throw new Error("LIVE_NOT_READY");
    const p = this.projection,
      fault = this.fault;
    try {
      const value = await p.read();
      if (!this.ready() || p !== this.projection || fault !== this.fault)
        throw new Error("LIVE_NOT_READY");
      return value;
    } catch (e) {
      this.invalidate("REDIS_UNAVAILABLE");
      throw e;
    }
  }
  async ingest(
    bytes: Uint8Array,
    routing: string,
    messageId: string | undefined,
    quarantine: Quarantine,
    afterCommit?: (change: Applied) => Promise<void>,
  ): Promise<void> {
    let checked: ReturnType<typeof validateEvent>;
    try {
      checked = validateEvent(strictJson(bytes), routing, messageId);
    } catch (e) {
      checked = fail(
        e instanceof Error && e.message === "MESSAGE_TOO_LARGE"
          ? "MESSAGE_TOO_LARGE"
          : "INVALID_JSON",
      );
    }
    if (!checked.ok) {
      this.invalidate(checked.code);
      await quarantine.publish(bytes, routing, messageId, checked.code);
      return;
    }
    const change: Change = {
      ...checked.value.change,
      digest: createHash("sha256")
        .update(canonical(checked.value.event))
        .digest("hex"),
    };
    await this.barrier(async () => {
      const applied = await this.projection.apply(change);
      if (!applied.ok) {
        this.invalidate(applied.code);
        if (
          ["EVENT_ID_CONFLICT", "AGGREGATE_VERSION_CONFLICT"].includes(
            applied.code,
          )
        ) {
          await quarantine.publish(bytes, routing, messageId, applied.code);
          return;
        }
        throw new Error(applied.code);
      }
      await afterCommit?.(applied.value);
      if (applied.value.kind === "APPLIED" && this.ready())
        this.notifications.update(applied.value);
    });
  }
  async reconcile(): Promise<Result<void>> {
    const fault = this.fault,
      p = this.projection,
      started = new Date(this.now()).toISOString();
    let prior = "";
    try {
      for (let round = 0; round < 6; round++) {
        if (
          this.state === "DRAINING" ||
          this.now() - Date.parse(started) > 60000
        )
          throw new Error("REBUILD_TIMEOUT");
        const data = await this.core.collect();
        if (
          data.silos.length > this.maxEntities ||
          data.windows.length > this.maxEntities
        )
          throw new Error("SNAPSHOT_TOO_LARGE");
        const silos: Silo[] = [],
          windows: Window[] = [];
        for (const [kind, values] of [
          ["silo", data.silos],
          ["window", data.windows],
        ] as const) {
          const ids = new Set<string>();
          for (const value of values) {
            const valid = snapshot(value, kind);
            if (!valid.ok || ids.has(valid.value.id))
              throw new Error("INVALID_CORE_SNAPSHOT");
            ids.add(valid.value.id);
            if (kind === "silo") silos.push(valid.value as Silo);
            else windows.push(valid.value as Window);
          }
        }
        const ids = new Set(silos.map((s) => s.id));
        if (windows.some((w) => !ids.has(w.silo_id)))
          throw new Error("INVALID_CORE_SNAPSHOT");
        for (const change of [
          ...silos.map((silo) => ({ silo })),
          ...windows.map((window) => ({ window })),
        ])
          await this.barrier(async () => {
            if (this.isDraining() || this.now() - Date.parse(started) > 60000)
              throw new Error("REBUILD_TIMEOUT");
            const r = await p.apply(change);
            if (!r.ok) throw new Error(r.code);
            if (this.ready() && r.value.kind === "APPLIED")
              this.notifications.update(r.value);
          });
        const signature = canonical({
          silos: silos.sort((a, b) => a.id.localeCompare(b.id)),
          windows: windows.sort((a, b) => a.id.localeCompare(b.id)),
        });
        if (signature === prior) {
          if (this.now() - Date.parse(started) > 60000)
            throw new Error("REBUILD_TIMEOUT");
          if (!this.broker || this.fault !== fault || p !== this.projection)
            throw new Error("LIVE_NOT_READY");
          await p.mark({
            core_scan_started_at: started,
            core_scan_completed_at: new Date(this.now()).toISOString(),
          });
          await p.read();
          if (
            !this.broker ||
            this.fault !== fault ||
            p !== this.projection ||
            this.isDraining()
          )
            throw new Error("LIVE_NOT_READY");
          this.lastVerified = this.now();
          this.state = "READY";
          return ok(undefined);
        }
        prior = signature;
      }
      throw new Error("CORE_SNAPSHOT_UNSTABLE");
    } catch {
      this.invalidate("REBUILD_FAILED");
      return fail("REBUILD_FAILED");
    }
  }
  drain() {
    this.state = "DRAINING";
    this.notifications.invalidate("SHUTDOWN");
  }
  private isDraining() {
    return this.state === "DRAINING";
  }
}
