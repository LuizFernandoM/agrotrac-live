export type Result<T> = { ok: true; value: T } | { ok: false; code: string };
export const ok = <T>(value: T): Result<T> => ({ ok: true, value });
export const fail = (code: string): Result<never> => ({ ok: false, code });
export type Silo = {
  id: string;
  codigo: string;
  nome: string;
  tipo_grao: string;
  capacidade_total_kg: number;
  ocupacao_fisica_kg: number;
  saldo_reservado_kg: number;
  saldo_disponivel_kg: number;
  status: "ACTIVE" | "INACTIVE";
  version: number;
  created_at: string;
  updated_at: string;
};
export type Window = {
  id: string;
  silo_id: string;
  inicio_em: string;
  termino_em: string;
  capacidade_maxima_kg: number;
  saldo_reservado_kg: number;
  saldo_disponivel_kg: number;
  status: "OPEN" | "BLOCKED" | "CLOSED";
  version: number;
  created_at: string;
  updated_at: string;
};
export type CoreEvent = {
  event_id: string;
  type: string;
  version: "1.0.0";
  producer: "agrotrac-core";
  occurred_at: string;
  correlation_id: string;
  payload: Record<string, unknown>;
};
export type Change = {
  event_id?: string;
  digest?: string;
  silo?: Silo;
  window?: Window;
};
export type Applied = {
  kind: "APPLIED" | "STALE" | "DUPLICATE";
  generation: string;
  previous_revision: number;
  revision: number;
  event_id?: string;
  silos: Silo[];
  windows: Window[];
};
export type Scan = {
  core_scan_started_at: string;
  core_scan_completed_at: string;
};
export type Snapshot = Scan & {
  protocol_version: "1.0.0";
  generation: string;
  revision: number;
  consistency: "per_aggregate_bounded";
  observed_at: string;
  broker_connected: true;
  silos: Silo[];
  windows: Window[];
};
export const uuid = (v: unknown): v is string =>
  typeof v === "string" &&
  /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(
    v,
  );
export const integer = (v: unknown, min = 0): v is number =>
  Number.isSafeInteger(v) && (v as number) >= min;
export const object = (v: unknown): v is Record<string, unknown> =>
  !!v && typeof v === "object" && !Array.isArray(v);
export const utc = (v: unknown): v is string => {
  if (
    typeof v !== "string" ||
    !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3})?Z$/.test(v)
  )
    return false;
  const d = new Date(v);
  return (
    Number.isFinite(d.getTime()) &&
    d.toISOString() === (v.length === 20 ? v.replace("Z", ".000Z") : v)
  );
};
export const canonical = (v: unknown): string =>
  Array.isArray(v)
    ? `[${v.map(canonical).join(",")}]`
    : object(v)
      ? `{${Object.keys(v)
          .sort()
          .map((k) => `${JSON.stringify(k)}:${canonical(v[k])}`)
          .join(",")}}`
      : JSON.stringify(v);
export const text = (v: unknown, min: number, max: number): v is string =>
  typeof v === "string" && v.trim().length >= min && v.length <= max;
const keys = (v: Record<string, unknown>, expected: string[]) =>
  Object.keys(v).sort().join("|") === [...expected].sort().join("|");
const common = [
  "id",
  "version",
  "created_at",
  "updated_at",
  "saldo_reservado_kg",
  "saldo_disponivel_kg",
  "status",
];
export function snapshot(
  v: unknown,
  kind: "silo" | "window",
): Result<Silo | Window> {
  if (
    !object(v) ||
    !uuid(v.id) ||
    !integer(v.version, 1) ||
    !utc(v.created_at) ||
    !utc(v.updated_at) ||
    Date.parse(v.created_at) > Date.parse(v.updated_at) ||
    !integer(v.saldo_reservado_kg) ||
    !integer(v.saldo_disponivel_kg)
  )
    return fail("INVALID_PAYLOAD");
  if (kind === "silo") {
    if (
      !keys(v, [
        ...common,
        "codigo",
        "nome",
        "tipo_grao",
        "capacidade_total_kg",
        "ocupacao_fisica_kg",
      ]) ||
      !text(v.codigo, 1, 50) ||
      !text(v.nome, 1, 100) ||
      !text(v.tipo_grao, 1, 50) ||
      !integer(v.capacidade_total_kg, 1) ||
      !integer(v.ocupacao_fisica_kg) ||
      !["ACTIVE", "INACTIVE"].includes(v.status as string) ||
      v.ocupacao_fisica_kg > v.capacidade_total_kg ||
      v.saldo_reservado_kg > v.capacidade_total_kg - v.ocupacao_fisica_kg ||
      v.saldo_disponivel_kg !==
        v.capacidade_total_kg - v.ocupacao_fisica_kg - v.saldo_reservado_kg
    )
      return fail("INVALID_PAYLOAD");
  } else if (
    !keys(v, [
      ...common,
      "silo_id",
      "inicio_em",
      "termino_em",
      "capacidade_maxima_kg",
    ]) ||
    !uuid(v.silo_id) ||
    !utc(v.inicio_em) ||
    !utc(v.termino_em) ||
    Date.parse(v.inicio_em) >= Date.parse(v.termino_em) ||
    !integer(v.capacidade_maxima_kg, 1) ||
    !["OPEN", "BLOCKED", "CLOSED"].includes(v.status as string) ||
    v.saldo_reservado_kg > v.capacidade_maxima_kg ||
    v.saldo_disponivel_kg !== v.capacidade_maxima_kg - v.saldo_reservado_kg
  )
    return fail("INVALID_PAYLOAD");
  return ok(v as unknown as Silo | Window);
}
export function validateEvent(
  v: unknown,
  routing: string,
  messageId?: string,
): Result<{ event: CoreEvent; change: Change }> {
  if (
    !object(v) ||
    !keys(v, [
      "event_id",
      "type",
      "version",
      "producer",
      "occurred_at",
      "correlation_id",
      "payload",
    ]) ||
    !uuid(v.event_id) ||
    !uuid(v.correlation_id) ||
    !utc(v.occurred_at) ||
    v.producer !== "agrotrac-core" ||
    !object(v.payload)
  )
    return fail("INVALID_ENVELOPE");
  if (v.version !== "1.0.0") return fail("UNSUPPORTED_VERSION");
  if (
    v.type !== routing ||
    (messageId !== undefined && messageId !== v.event_id)
  )
    return fail("ROUTING_KEY_MISMATCH");
  const kinds = [
    "silo.created",
    "window.created",
    "window.updated",
    "booking.created",
    "booking.cancelled",
  ];
  if (
    typeof v.type !== "string" ||
    !kinds.some((k) => v.type === `agrotrac.core.${k}`)
  )
    return fail("UNSUPPORTED_EVENT_TYPE");
  const p = v.payload,
    s = snapshot(p.silo, "silo");
  if (
    !s.ok ||
    p.silo_id !== s.value.id ||
    p.silo_version !== s.value.version ||
    p.saldo_disponivel_silo_kg !== s.value.saldo_disponivel_kg
  )
    return fail("INVALID_PAYLOAD");
  const expected = [
    "silo_id",
    "silo_version",
    "silo",
    "saldo_disponivel_silo_kg",
  ];
  let w: Window | undefined;
  if (v.type !== "agrotrac.core.silo.created") {
    expected.push(
      "window_id",
      "window_version",
      "window",
      "inicio_em",
      "termino_em",
      "status",
      "updated_at",
      "motivo",
      "saldo_disponivel_janela_kg",
    );
    const checked = snapshot(p.window, "window");
    if (!checked.ok) return checked;
    w = checked.value as Window;
    if (
      p.window_id !== w.id ||
      w.silo_id !== s.value.id ||
      p.window_version !== w.version ||
      p.inicio_em !== w.inicio_em ||
      p.termino_em !== w.termino_em ||
      p.status !== w.status ||
      p.updated_at !== w.updated_at ||
      p.saldo_disponivel_janela_kg !== w.saldo_disponivel_kg ||
      !(
        p.motivo === null ||
        (typeof p.motivo === "string" && p.motivo.length <= 500)
      )
    )
      return fail("INVALID_PAYLOAD");
  }
  if (v.type.startsWith("agrotrac.core.booking.")) {
    expected.push(
      "booking_id",
      "booking",
      "placa",
      "motorista_nome",
      "peso_estimado_kg",
    );
    const b = p.booking;
    if (
      !object(b) ||
      !keys(b, [
        "id",
        "silo_id",
        "window_id",
        "placa",
        "motorista_nome",
        "motorista_documento",
        "peso_estimado_kg",
        "status",
        "cancelled_at",
        "motivo",
        "created_at",
        "updated_at",
      ]) ||
      !uuid(b.id) ||
      b.id !== p.booking_id ||
      b.silo_id !== s.value.id ||
      b.window_id !== w?.id ||
      typeof b.placa !== "string" ||
      !/^[A-Z]{3}[0-9][A-Z0-9][0-9]{2}$/.test(b.placa) ||
      !text(b.motorista_nome, 2, 100) ||
      !(
        b.motorista_documento === null || text(b.motorista_documento, 1, 100)
      ) ||
      !integer(b.peso_estimado_kg, 1) ||
      !utc(b.created_at) ||
      !utc(b.updated_at) ||
      Date.parse(b.created_at) > Date.parse(b.updated_at) ||
      p.placa !== b.placa ||
      p.motorista_nome !== b.motorista_nome ||
      p.peso_estimado_kg !== b.peso_estimado_kg
    )
      return fail("INVALID_PAYLOAD");
    if (v.type.endsWith("cancelled")) {
      expected.push("peso_estornado_kg", "cancelled_at");
      if (
        b.status !== "CANCELLED" ||
        !utc(b.cancelled_at) ||
        p.cancelled_at !== b.cancelled_at ||
        p.peso_estornado_kg !== b.peso_estimado_kg ||
        p.motivo !== b.motivo
      )
        return fail("INVALID_PAYLOAD");
    } else if (
      b.status !== "CONFIRMED" ||
      b.cancelled_at !== null ||
      b.motivo !== null
    )
      return fail("INVALID_PAYLOAD");
  }
  if (!keys(p, expected)) return fail("INVALID_PAYLOAD");
  return ok({
    event: v as unknown as CoreEvent,
    change: { event_id: v.event_id, silo: s.value as Silo, window: w },
  });
}
