import { Redis } from "ioredis";
import { randomUUID } from "node:crypto";
import type { Projection } from "../application/ports.js";
import {
  canonical,
  fail,
  integer,
  object,
  ok,
  snapshot,
  utc,
  uuid,
  type Applied,
  type Change,
  type Result,
  type Scan,
  type Snapshot,
  type Silo,
  type Window,
} from "../domain/model.js";
import { strictJson } from "../domain/json.js";
import { abortable, deadline } from "./time.js";
// One hash is both the catalogue and its index. A single HSET is the only mutating
// command; all foreseeable failures (including wrong key type) precede it.
export const APPLY = `
local t=redis.call('TYPE',KEYS[1]).ok
if t~='hash' then return {'CORRUPT_PROJECTION'} end
if redis.call('HGET',KEYS[1],'generation')~=ARGV[1] then return {'GENERATION_MISMATCH'} end
local rev=tonumber(redis.call('HGET',KEYS[1],'revision'))
if not rev or rev<0 or rev%1~=0 or rev>9007199254740991 then return {'CORRUPT_PROJECTION'} end
local sc=tonumber(redis.call('HGET',KEYS[1],'s_count'));local wc=tonumber(redis.call('HGET',KEYS[1],'w_count'))
if not sc or not wc or sc<0 or wc<0 or sc%1~=0 or wc%1~=0 then return {'CORRUPT_PROJECTION'} end
if ARGV[2]~='' then
 local old=redis.call('HGET',KEYS[1],'d:'..ARGV[2])
 if old then if old==ARGV[3] then return {'DUPLICATE',string.format('%.0f',rev)} else return {'EVENT_ID_CONFLICT'} end end
end
local updates={};local out={'APPLIED',string.format('%.0f',rev)}
for n=4,8,4 do
 if ARGV[n]~='' then
  local field=ARGV[n];local data=ARGV[n+1];local version=tonumber(ARGV[n+2]);local current=redis.call('HGET',KEYS[1],field);local oldVersion=redis.call('HGET',KEYS[1],'v:'..field)
  if (current and not oldVersion) or (oldVersion and not current) then return {'CORRUPT_PROJECTION'} end
  if oldVersion and (not tonumber(oldVersion) or tonumber(oldVersion)%1~=0 or tonumber(oldVersion)<1 or tonumber(oldVersion)>9007199254740991) then return {'CORRUPT_PROJECTION'} end
  if oldVersion and tonumber(oldVersion)==version and current~=data then return {'AGGREGATE_VERSION_CONFLICT'} end
  if not oldVersion or version>tonumber(oldVersion) then
   if not oldVersion then if n==4 then sc=sc+1 else wc=wc+1 end end
   table.insert(updates,field);table.insert(updates,data);table.insert(updates,'v:'..field);table.insert(updates,ARGV[n+2]);table.insert(out,field);table.insert(out,data)
  end
 end
end
if #updates>0 then
 if sc>tonumber(ARGV[12]) or wc>tonumber(ARGV[12]) then return {'SNAPSHOT_TOO_LARGE'} end
 if rev>=9007199254740991 then return {'REVISION_EXHAUSTED'} end
 rev=rev+1;table.insert(updates,'revision');table.insert(updates,string.format('%.0f',rev))
 table.insert(updates,'s_count');table.insert(updates,string.format('%.0f',sc));table.insert(updates,'w_count');table.insert(updates,string.format('%.0f',wc))
else out[1]='STALE' end
if ARGV[2]~='' then table.insert(updates,'d:'..ARGV[2]);table.insert(updates,ARGV[3]) end
if #updates>0 then redis.call('HSET',KEYS[1],unpack(updates)) end
table.insert(out,3,string.format('%.0f',rev));return out
`;
export class RedisProjection implements Projection {
  readonly generation: string;
  readonly key: string;
  constructor(
    readonly client: Redis,
    readonly prefix: string,
    generation = randomUUID(),
    private maxEntities = 10000,
    private maxBytes = 10485760,
    private signal?: AbortSignal,
  ) {
    this.generation = generation;
    this.key = `${prefix}${generation}`;
  }
  private async io<T>(work: Promise<T>): Promise<T> {
    try {
      return await abortable(work, this.signal, () => this.client.disconnect());
    } catch {
      this.client.disconnect();
      throw new Error("REDIS_UNAVAILABLE");
    }
  }
  async initialize() {
    this.signal?.throwIfAborted();
    const result = await this.io(
      this.client.eval(
        `if redis.call('EXISTS',KEYS[1])==1 then return 0 end redis.call('HSET',KEYS[1],'generation',ARGV[1],'revision','0','s_count','0','w_count','0');return 1`,
        1,
        this.key,
        this.generation,
      ),
    );
    if (result !== 1) throw new Error("GENERATION_EXISTS");
  }
  async apply(change: Change): Promise<Result<Applied>> {
    this.signal?.throwIfAborted();
    if (
      (change.silo && !snapshot(change.silo, "silo").ok) ||
      (change.window && !snapshot(change.window, "window").ok) ||
      (change.event_id &&
        (!uuid(change.event_id) ||
          !change.digest ||
          !/^[0-9a-f]{64}$/.test(change.digest)))
    )
      return fail("INVALID_PAYLOAD");
    const args = [
      this.generation,
      change.event_id ?? "",
      change.digest ?? "",
      change.silo ? `s:${change.silo.id}` : "",
      change.silo ? canonical(change.silo) : "",
      String(change.silo?.version ?? ""),
      "",
      change.window ? `w:${change.window.id}` : "",
      change.window ? canonical(change.window) : "",
      String(change.window?.version ?? ""),
      "",
      String(this.maxEntities),
    ];
    const r = (await this.io(
      this.client.eval(APPLY, 1, this.key, ...args),
    )) as string[];
    if (!["APPLIED", "STALE", "DUPLICATE"].includes(r[0])) return fail(r[0]);
    const previous = Number(r[1]),
      revision = r[0] === "DUPLICATE" ? previous : Number(r[2]);
    if (!integer(previous) || !integer(revision))
      throw new Error("CORRUPT_PROJECTION");
    const silos: Silo[] = [],
      windows: Window[] = [];
    for (let i = 3; i < r.length; i += 2) {
      const v = JSON.parse(r[i + 1]);
      if (r[i].startsWith("s:")) silos.push(v);
      else windows.push(v);
    }
    return ok({
      kind: r[0] as Applied["kind"],
      generation: this.generation,
      previous_revision: previous,
      revision,
      event_id: change.event_id,
      silos,
      windows,
    });
  }
  async mark(scan: Scan) {
    this.signal?.throwIfAborted();
    await this.io(
      this.client.eval(
        `if redis.call('TYPE',KEYS[1]).ok~='hash' or redis.call('HGET',KEYS[1],'generation')~=ARGV[1] then return redis.error_reply('GENERATION_MISMATCH') end return redis.call('HSET',KEYS[1],'scan',ARGV[2])`,
        1,
        this.key,
        this.generation,
        JSON.stringify(scan),
      ),
    );
  }
  async read(): Promise<Snapshot> {
    this.signal?.throwIfAborted();
    const rows = await this.io(this.client.hgetall(this.key));
    if (
      rows.generation !== this.generation ||
      !rows.scan ||
      !integer(Number(rows.revision))
    )
      throw new Error("CORRUPT_PROJECTION");
    const scan = JSON.parse(rows.scan) as Scan,
      silos: Silo[] = [],
      windows: Window[] = [];
    if (
      !object(scan) ||
      Object.keys(scan).sort().join(",") !==
        "core_scan_completed_at,core_scan_started_at" ||
      !utc(scan.core_scan_started_at) ||
      !utc(scan.core_scan_completed_at) ||
      Date.parse(scan.core_scan_started_at) >
        Date.parse(scan.core_scan_completed_at)
    )
      throw new Error("CORRUPT_PROJECTION");
    for (const [key, raw] of Object.entries(rows)) {
      if (key.startsWith("s:") || key.startsWith("w:")) {
        const kind = key.startsWith("s:") ? "silo" : "window";
        const v = strictJson(Buffer.from(raw));
        const checked = snapshot(v, kind);
        if (
          !checked.ok ||
          checked.value.id !== key.slice(2) ||
          String(checked.value.version) !== rows[`v:${key}`]
        )
          throw new Error("CORRUPT_PROJECTION");
        if (kind === "silo") silos.push(checked.value as Silo);
        else windows.push(checked.value as Window);
      } else if (key.startsWith("v:")) {
        if (!rows[key.slice(2)]) throw new Error("CORRUPT_PROJECTION");
      } else if (key.startsWith("d:")) {
        if (!uuid(key.slice(2)) || !/^[0-9a-f]{64}$/.test(raw))
          throw new Error("CORRUPT_PROJECTION");
      } else if (
        !["generation", "revision", "scan", "s_count", "w_count"].includes(key)
      )
        throw new Error("CORRUPT_PROJECTION");
    }
    const ids = new Set(silos.map((s) => s.id));
    if (windows.some((w) => !ids.has(w.silo_id)))
      throw new Error("CORRUPT_PROJECTION");
    if (
      !integer(Number(rows.s_count)) ||
      !integer(Number(rows.w_count)) ||
      silos.length !== Number(rows.s_count) ||
      windows.length !== Number(rows.w_count)
    )
      throw new Error("CORRUPT_PROJECTION");
    if (silos.length > this.maxEntities || windows.length > this.maxEntities)
      throw new Error("SNAPSHOT_TOO_LARGE");
    const result: Snapshot = {
      protocol_version: "1.0.0",
      generation: this.generation,
      revision: Number(rows.revision),
      consistency: "per_aggregate_bounded",
      ...scan,
      observed_at: new Date().toISOString(),
      broker_connected: true,
      silos: silos.sort((a, b) => a.id.localeCompare(b.id)),
      windows: windows.sort((a, b) => a.id.localeCompare(b.id)),
    };
    if (Buffer.byteLength(JSON.stringify(result)) > this.maxBytes)
      throw new Error("SNAPSHOT_TOO_LARGE");
    return result;
  }
  async close() {
    /* Client belongs to the runtime; candidate generations are preserved. */
  }
}
export function createRedis(url: string, ms = 5000) {
  const client = new Redis(url, {
    lazyConnect: true,
    connectTimeout: ms,
    commandTimeout: ms,
    enableOfflineQueue: false,
    retryStrategy: () => null,
    maxRetriesPerRequest: 0,
  });
  client.on("error", () => {});
  return client;
}
export async function openRedis(
  client: Redis,
  ms = 5000,
  signal?: AbortSignal,
) {
  try {
    signal?.throwIfAborted();
    await abortable(
      deadline(client.connect(), ms, () => client.disconnect()),
      signal,
      () => client.disconnect(),
    );
    signal?.throwIfAborted();
    const policy = (await abortable(
      client.config("GET", "maxmemory-policy"),
      signal,
      () => client.disconnect(),
    )) as string[];
    signal?.throwIfAborted();
    if (policy[1] !== "noeviction")
      throw new Error("REDIS_EVICTION_POLICY_INCOMPATIBLE");
    return client;
  } catch (e) {
    client.disconnect();
    throw e;
  }
}
export async function connectRedis(
  url: string,
  ms = 5000,
  signal?: AbortSignal,
) {
  return openRedis(createRedis(url, ms), ms, signal);
}
