import type { CoreReader } from "../application/ports.js";
import { strictJson } from "../domain/json.js";
export class HttpCoreReader implements CoreReader {
  constructor(
    private url: string,
    private ms = 5000,
    private maxBytes = 10485760,
    private maxEntities = 10000,
    private signal?: AbortSignal,
  ) {}
  private async list(kind: string): Promise<unknown[]> {
    const response = await fetch(`${this.url}/v1/core/${kind}`, {
      signal: AbortSignal.any([
        AbortSignal.timeout(this.ms),
        ...(this.signal ? [this.signal] : []),
      ]),
      redirect: "error",
    });
    if (!response.ok || !response.body) throw new Error("CORE_UNAVAILABLE");
    const chunks: Uint8Array[] = [];
    let size = 0;
    const reader = response.body.getReader();
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        size += value.length;
        if (size > this.maxBytes) throw new Error("SNAPSHOT_TOO_LARGE");
        chunks.push(value);
      }
      const data = strictJson(Buffer.concat(chunks), this.maxBytes);
      if (!Array.isArray(data) || data.length > this.maxEntities)
        throw new Error("SNAPSHOT_TOO_LARGE");
      return data;
    } finally {
      await reader.cancel().catch(() => {});
    }
  }
  async collect() {
    // bounded pair; no claim that these endpoints share a transaction
    const silos = await this.list("silos");
    const windows = await this.list("windows");
    return { silos, windows };
  }
}
