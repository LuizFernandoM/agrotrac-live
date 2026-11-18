import { test } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { Live } from "../../src/application/live.js";
import { strictJson } from "../../src/domain/json.js";
import { canonical, snapshot, validateEvent } from "../../src/domain/model.js";
import { deadline, delay } from "../../src/infra/time.js";
import { MemoryProjection, silo, window, event } from "../helpers.js";
import { settleCleanup, finalStatus } from "../teardown.js";
const bytes = (v: unknown) => Buffer.from(JSON.stringify(v));
test("teardown failure cannot produce passed and later cleanup still runs", async () => {
  let later = false;
  const cleanup = await settleCleanup([
    {
      name: "broken",
      async close() {
        throw new Error("INJECTED");
      },
    },
    {
      name: "later",
      async close() {
        later = true;
      },
    },
  ]);
  assert.equal(later, true);
  assert.equal(finalStatus(true, [{ status: "passed" }], cleanup), "failed");
  assert.equal(cleanup[0].status, "failed");
  assert.equal(finalStatus(false, [], []), "failed");
  assert.equal(
    finalStatus(true, [{ status: "passed" }], [{ status: "passed" }]),
    "passed",
  );
});
test("strict JSON rejects duplicate/escaped keys, malformed UTF8, NUL, unsafe numbers and depth", () => {
  for (const v of [
    '{"a":1,"\\u0061":2}',
    '{"x":"\\u0000"}',
    '{"x":"\\ud800"}',
    "9007199254740992",
    "1e999",
    "[1,]",
    "true x",
    "\u00a0null",
    '["unterminated]',
  ])
    assert.throws(() => strictJson(Buffer.from(v)));
  assert.throws(() => strictJson(Buffer.from([0xc0, 0xaf])));
  assert.throws(() =>
    strictJson(Buffer.from("[".repeat(34) + "0" + "]".repeat(34))),
  );
  assert.throws(() => strictJson(Buffer.from("{}"), 1), /MESSAGE_TOO_LARGE/);
  assert.equal(
    canonical(strictJson(Buffer.from('{"b":[true,null],"a":1}'))),
    '{"a":1,"b":[true,null]}',
  );
});
test("Core envelope and redundant fields; masses and versions are safe", () => {
  const s = silo(),
    w = window(s),
    e = event(s, w);
  assert.equal(validateEvent(e, e.type).ok, true);
  for (const bad of [-1, 1.1, "1", null, 9007199254740992])
    assert.equal(snapshot({ ...s, version: bad }, "silo").ok, false);
  assert.equal(
    snapshot(
      {
        ...s,
        capacidade_total_kg: Number.MAX_SAFE_INTEGER,
        saldo_disponivel_kg: Number.MAX_SAFE_INTEGER,
      },
      "silo",
    ).ok,
    true,
  );
  for (const altered of [
    { ...e, version: "2.0.0" },
    { ...e, payload: { ...e.payload, window_version: 2 } },
    { ...e, payload: { ...e.payload, status: "CANCELLED" } },
    { ...e, payload: { ...e.payload, extra: true } },
  ])
    assert.equal(validateEvent(altered, e.type).ok, false);
  assert.equal(validateEvent(e, "wrong").ok, false);
  assert.equal(validateEvent(e, e.type, randomUUID()).ok, false);
});
test("reconcile only READY with two stable complete rounds and broker", async () => {
  const s = silo(),
    w = window(s),
    p = new MemoryProjection(),
    live = new Live(p, {
      async collect() {
        return { silos: [s], windows: [w] };
      },
    });
  assert.equal((await live.reconcile()).ok, false);
  assert.equal(live.ready(), false);
  live.connected(true);
  assert.equal((await live.reconcile()).ok, true);
  assert.equal((await live.view()).consistency, "per_aggregate_bounded");
  live.connected(false);
  await assert.rejects(live.view());
});
test("partial/orphan/oversized/unstable snapshots never become ready", async () => {
  const s = silo(),
    w = window(s);
  for (const mode of ["partial", "orphan", "oversized", "unstable"]) {
    let n = 0;
    const live = new Live(
      new MemoryProjection(),
      {
        async collect() {
          if (mode === "partial") throw new Error("HTTP_FAILED");
          return {
            silos:
              mode === "orphan"
                ? []
                : mode === "oversized"
                  ? [s, { ...s, id: randomUUID() }]
                  : [{ ...s, version: mode === "unstable" ? ++n : 1 }],
            windows: mode === "orphan" ? [w] : [],
          };
        },
      },
      1,
    );
    live.connected(true);
    assert.equal((await live.reconcile()).ok, false, mode);
    assert.equal(live.ready(), false);
  }
});
test("independent versions, replay and conflicts preserve the original", async () => {
  const s = silo(8),
    w = window(s, 2),
    p = new MemoryProjection();
  await p.apply({ silo: s, window: w });
  const r = await p.apply({
    silo: { ...s, version: 7 },
    window: { ...w, version: 3 },
  });
  assert.ok(r.ok);
  assert.equal(r.value.silos.length, 0);
  assert.equal(r.value.windows[0].version, 3);
  const live = new Live(p, {
    async collect() {
      return { silos: [s], windows: [{ ...w, version: 3 }] };
    },
  });
  live.connected(true);
  await live.reconcile();
  const e = event({ ...s, version: 9 });
  let quarantine = 0;
  const q = {
    async publish() {
      quarantine++;
    },
  };
  await live.ingest(bytes(e), e.type, e.event_id, q);
  const rev = p.revision;
  await live.ingest(bytes(e), e.type, e.event_id, q);
  assert.equal(p.revision, rev);
  await live.ingest(
    bytes({ ...e, occurred_at: "2026-01-01T00:00:00Z" }),
    e.type,
    e.event_id,
    q,
  );
  assert.equal(quarantine, 1);
  assert.equal(live.ready(), false);
  assert.equal(p.silos.get(s.id)?.version, 9);
});
test("quarantine failure and persistence failure propagate: caller cannot ACK", async () => {
  const p = new MemoryProjection(),
    live = new Live(p, {
      async collect() {
        return { silos: [], windows: [] };
      },
    });
  await assert.rejects(
    live.ingest(Buffer.from("{"), "x", undefined, {
      async publish() {
        throw new Error("CONFIRM_FAILED");
      },
    }),
  );
  p.apply = async () => {
    throw new Error("REDIS_FAILED");
  };
  const e = event(silo());
  await assert.rejects(
    live.ingest(bytes(e), e.type, e.event_id, {
      async publish() {
        assert.fail();
      },
    }),
  );
});
test("fault during collection cannot reactivate readiness; freshness expires", async () => {
  let clock = 0;
  const p = new MemoryProjection();
  let live: Live;
  live = new Live(
    p,
    {
      async collect() {
        live.invalidate();
        return { silos: [], windows: [] };
      },
    },
    10000,
    () => clock,
  );
  live.connected(true);
  assert.equal((await live.reconcile()).ok, false);
  const valid = new Live(
    p,
    {
      async collect() {
        return { silos: [], windows: [] };
      },
    },
    10000,
    () => clock,
  );
  valid.connected(true);
  await valid.reconcile();
  assert.equal(valid.ready(), true);
  clock = 120001;
  assert.equal(valid.ready(), false);
  valid.drain();
  assert.equal(valid.state, "DRAINING");
});
test("deadline cleans up late connection; abortable backoff releases timer", async () => {
  let cleaned = false;
  await assert.rejects(
    deadline(
      delay(20).then(() => 1),
      2,
      () => {
        cleaned = true;
      },
    ),
  );
  await delay(25);
  assert.equal(cleaned, true);
  const abort = new AbortController();
  const wait = delay(60000, abort.signal);
  abort.abort();
  await wait;
});
