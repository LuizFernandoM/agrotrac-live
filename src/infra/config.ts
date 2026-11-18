import { readFileSync } from "node:fs";
import { parse } from "dotenv";
import { serverDefaults } from "./server.js";
export function configuration() {
  const file = process.env.LIVE_INFRA_ENV_PATH
    ? parse(readFileSync(process.env.LIVE_INFRA_ENV_PATH))
    : {};
  const env = { ...file, ...process.env };
  const positive = (key: string, fallback: number, max: number) => {
    const raw = env[key];
    if (raw === undefined) return fallback;
    if (
      !/^[1-9]\d*$/.test(raw) ||
      !Number.isSafeInteger(Number(raw)) ||
      Number(raw) > max
    )
      throw new Error("INVALID_CONFIGURATION");
    return Number(raw);
  };
  if (env.LIVE_HOST && env.LIVE_HOST !== "127.0.0.1")
    throw new Error("LOCAL_BIND_REQUIRED");
  if (env.LIVE_PORT && env.LIVE_PORT !== "3002")
    throw new Error("LOCAL_PORT_REQUIRED");
  const core = env.LIVE_CORE_URL ?? "http://127.0.0.1:3001";
  const parsed = new URL(core);
  if (
    !["http:", "https:"].includes(parsed.protocol) ||
    parsed.username ||
    parsed.password ||
    parsed.search ||
    parsed.hash
  )
    throw new Error("INVALID_CONFIGURATION");
  const prefix = env.LIVE_REDIS_PREFIX ?? "agrotrac:live:v1:";
  if (!/^[a-zA-Z0-9:_-]{1,150}:$/.test(prefix))
    throw new Error("INVALID_CONFIGURATION");
  return {
    redis: env.LIVE_REDIS_URL ?? "redis://127.0.0.1:6379",
    rabbit:
      env.RABBITMQ_URL ??
      `amqp://${encodeURIComponent(env.RABBITMQ_DEFAULT_USER ?? "")}:${encodeURIComponent(env.RABBITMQ_DEFAULT_PASS ?? "")}@127.0.0.1:5672`,
    core,
    prefix,
    port: 3002,
    ioMs: positive("LIVE_IO_TIMEOUT_MS", 5000, 5000),
    maxEntities: positive("LIVE_MAX_ENTITIES", 10000, 10000),
    maxBytes: positive("LIVE_MAX_SNAPSHOT_BYTES", 10485760, 10485760),
    scanMs: positive("LIVE_SCAN_INTERVAL_MS", 60000, 60000),
    limits: {
      ...serverDefaults,
      checkpointMs: positive("LIVE_CHECKPOINT_MS", 10000, 10000),
      sendMs: positive("LIVE_SEND_TIMEOUT_MS", 5000, 5000),
      bufferBytes: positive("LIVE_BUFFER_BYTES", 2097152, 2097152),
      bufferUpdates: positive("LIVE_BUFFER_UPDATES", 100, 100),
    },
  };
}
