import net from "node:net";

/** RESP request gate backed by the real Redis. Delays only fixture commands. */
export class RedisGate {
  readonly sockets = new Set<net.Socket>();
  readonly timers = new Set<NodeJS.Timeout>();
  readonly commands: { name: string; at: number; forwarded: boolean }[] = [];
  readonly server: net.Server;
  port = 0;
  constructor(private delays: (name: string, args: string[]) => number) {
    this.server = net.createServer((client) => {
      const upstream = net.connect(6379, "127.0.0.1");
      const owned = new Set<NodeJS.Timeout>();
      for (const socket of [client, upstream]) {
        this.sockets.add(socket);
        socket.on("error", () => {});
        socket.on("close", () => {
          this.sockets.delete(socket);
          for (const timer of owned) {
            clearTimeout(timer);
            this.timers.delete(timer);
          }
          owned.clear();
          client.destroy();
          upstream.destroy();
        });
      }
      upstream.on("data", (data) => client.write(data));
      let buffer = Buffer.alloc(0);
      client.on("data", (data) => {
        buffer = Buffer.concat([buffer, data]);
        for (;;) {
          const parsed = request(buffer);
          if (!parsed) break;
          const raw = buffer.subarray(0, parsed.end);
          buffer = buffer.subarray(parsed.end);
          const name = parsed.args[0].toUpperCase();
          const entry = { name, at: Date.now(), forwarded: false };
          this.commands.push(entry);
          const forward = () => {
            if (!upstream.destroyed && !client.destroyed) {
              entry.forwarded = true;
              upstream.write(raw);
            }
          };
          const ms = this.delays(name, parsed.args);
          if (ms) {
            const timer = setTimeout(() => {
              owned.delete(timer);
              this.timers.delete(timer);
              forward();
            }, ms);
            owned.add(timer);
            this.timers.add(timer);
          } else forward();
        }
      });
    });
  }
  async start() {
    await new Promise<void>((r) => this.server.listen(0, "127.0.0.1", r));
    this.port = (this.server.address() as net.AddressInfo).port;
    return this;
  }
  async close() {
    for (const timer of this.timers) clearTimeout(timer);
    this.timers.clear();
    for (const socket of this.sockets) socket.destroy();
    await new Promise<void>((r) => this.server.close(() => r()));
  }
}
function request(buffer: Buffer): { args: string[]; end: number } | undefined {
  const line = (start: number) => {
    const end = buffer.indexOf("\r\n", start);
    return end < 0
      ? undefined
      : { text: buffer.toString("utf8", start, end), next: end + 2 };
  };
  const head = line(0);
  if (!head) return;
  if (!/^\*\d+$/.test(head.text)) throw new Error("FIXTURE_RESP_ERROR");
  let offset = head.next;
  const args: string[] = [];
  for (let i = 0; i < Number(head.text.slice(1)); i++) {
    const size = line(offset);
    if (!size) return;
    if (!/^\$\d+$/.test(size.text)) throw new Error("FIXTURE_RESP_ERROR");
    const end = size.next + Number(size.text.slice(1));
    if (buffer.length < end + 2) return;
    args.push(buffer.toString("utf8", size.next, end));
    offset = end + 2;
  }
  return { args, end: offset };
}
