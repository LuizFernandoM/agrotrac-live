import type {
  Applied,
  Change,
  Result,
  Scan,
  Snapshot,
} from "../domain/model.js";
export interface Projection {
  generation: string;
  apply(change: Change): Promise<Result<Applied>>;
  read(): Promise<Snapshot>;
  mark(scan: Scan): Promise<void>;
  close(): Promise<void>;
}
export interface CoreReader {
  collect(): Promise<{ silos: unknown[]; windows: unknown[] }>;
}
export interface Notifications {
  update(change: Applied): void;
  invalidate(code: string): void;
}
export interface Quarantine {
  publish(
    bytes: Uint8Array,
    routing: string,
    messageId: string | undefined,
    code: string,
  ): Promise<void>;
}
