import { randomUUID, createHash } from "node:crypto";
import {
  canonical,
  fail,
  ok,
  type Applied,
  type Change,
  type Scan,
  type Snapshot,
  type Silo,
  type Window,
} from "../src/domain/model.js";
import type { Projection } from "../src/application/ports.js";
export function silo(version = 1): Silo {
  const now = new Date().toISOString();
  return {
    id: randomUUID(),
    codigo: "TEST",
    nome: "Teste",
    tipo_grao: "SOJA",
    capacidade_total_kg: 10000,
    ocupacao_fisica_kg: 0,
    saldo_reservado_kg: 0,
    saldo_disponivel_kg: 10000,
    status: "ACTIVE",
    version,
    created_at: now,
    updated_at: now,
  };
}
export function window(s: Silo, version = 1): Window {
  return {
    id: randomUUID(),
    silo_id: s.id,
    inicio_em: new Date(Date.now() + 60000).toISOString(),
    termino_em: new Date(Date.now() + 3600000).toISOString(),
    capacidade_maxima_kg: 10000,
    saldo_reservado_kg: 0,
    saldo_disponivel_kg: 10000,
    status: "OPEN",
    version,
    created_at: s.created_at,
    updated_at: s.updated_at,
  };
}
export function event(s: Silo, w?: Window) {
  return {
    event_id: randomUUID(),
    type: w ? "agrotrac.core.window.created" : "agrotrac.core.silo.created",
    version: "1.0.0",
    producer: "agrotrac-core",
    occurred_at: new Date().toISOString(),
    correlation_id: randomUUID(),
    payload: {
      silo_id: s.id,
      silo_version: s.version,
      silo: s,
      saldo_disponivel_silo_kg: s.saldo_disponivel_kg,
      ...(w
        ? {
            window_id: w.id,
            window_version: w.version,
            window: w,
            inicio_em: w.inicio_em,
            termino_em: w.termino_em,
            status: w.status,
            updated_at: w.updated_at,
            motivo: null,
            saldo_disponivel_janela_kg: w.saldo_disponivel_kg,
          }
        : {}),
    },
  };
}
export const digest = (v: unknown) =>
  createHash("sha256").update(canonical(v)).digest("hex");
export class MemoryProjection implements Projection {
  generation = randomUUID();
  revision = 0;
  silos = new Map<string, Silo>();
  windows = new Map<string, Window>();
  dedup = new Map<string, string>();
  scan: Scan = {
    core_scan_started_at: new Date().toISOString(),
    core_scan_completed_at: new Date().toISOString(),
  };
  async apply(c: Change) {
    if (c.event_id && this.dedup.has(c.event_id))
      return this.dedup.get(c.event_id) === c.digest
        ? ok({
            kind: "DUPLICATE",
            generation: this.generation,
            previous_revision: this.revision,
            revision: this.revision,
            silos: [],
            windows: [],
          } as Applied)
        : fail("EVENT_ID_CONFLICT");
    const next: { silo?: Silo; window?: Window } = {};
    for (const [kind, v, map] of [
      ["silo", c.silo, this.silos],
      ["window", c.window, this.windows],
    ] as const) {
      if (!v) continue;
      const old = map.get(v.id);
      if (old?.version === v.version && canonical(old) !== canonical(v))
        return fail("AGGREGATE_VERSION_CONFLICT");
      if (!old || v.version > old.version) Object.assign(next, { [kind]: v });
    }
    const previous = this.revision;
    if (next.silo) this.silos.set(next.silo.id, next.silo);
    if (next.window) this.windows.set(next.window.id, next.window);
    if (next.silo || next.window) this.revision++;
    if (c.event_id) this.dedup.set(c.event_id, c.digest!);
    return ok({
      kind: this.revision === previous ? "STALE" : "APPLIED",
      generation: this.generation,
      previous_revision: previous,
      revision: this.revision,
      silos: next.silo ? [next.silo] : [],
      windows: next.window ? [next.window] : [],
    } as Applied);
  }
  async read(): Promise<Snapshot> {
    return {
      protocol_version: "1.0.0",
      generation: this.generation,
      revision: this.revision,
      consistency: "per_aggregate_bounded",
      ...this.scan,
      observed_at: new Date().toISOString(),
      broker_connected: true,
      silos: [...this.silos.values()],
      windows: [...this.windows.values()],
    };
  }
  async mark(s: Scan) {
    this.scan = s;
  }
  async close() {}
}
