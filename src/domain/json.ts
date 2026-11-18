// Small recursive JSON reader: duplicate keys (including escaped equivalents) are rejected.
export function strictJson(bytes: Uint8Array, max = 1048576): unknown {
  if (bytes.byteLength > max) throw new Error("MESSAGE_TOO_LARGE");
  let s: string;
  try {
    s = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch {
    throw new Error("INVALID_JSON");
  }
  let i = 0;
  const ws = () => {
    while (/\s/.test(s[i] ?? "") && i < s.length) {
      if (!" \t\r\n".includes(s[i])) throw 0;
      i++;
    }
  };
  const string = (): string => {
    const start = i++;
    while (i < s.length) {
      const c = s[i++];
      if (c === '"') {
        const v = JSON.parse(s.slice(start, i));
        for (let j = 0; j < v.length; j++) {
          const n = v.charCodeAt(j);
          if (n === 0) throw 0;
          if (n >= 0xd800 && n <= 0xdbff) {
            const next = v.charCodeAt(++j);
            if (!(next >= 0xdc00 && next <= 0xdfff)) throw 0;
          } else if (n >= 0xdc00 && n <= 0xdfff) throw 0;
        }
        return v;
      }
      if (c === "\\") i++;
    }
    throw 0;
  };
  const value = (depth: number): unknown => {
    if (depth > 32) throw 0;
    ws();
    const c = s[i];
    if (c === '"') return string();
    if (c === "{") {
      i++;
      const o: Record<string, unknown> = Object.create(null),
        seen = new Set<string>();
      ws();
      if (s[i] === "}") {
        i++;
        return o;
      }
      for (;;) {
        ws();
        if (s[i] !== '"') throw 0;
        const k = string();
        if (seen.has(k)) throw 0;
        seen.add(k);
        ws();
        if (s[i++] !== ":") throw 0;
        o[k] = value(depth + 1);
        ws();
        const end = s[i++];
        if (end === "}") return o;
        if (end !== ",") throw 0;
      }
    }
    if (c === "[") {
      i++;
      const a: unknown[] = [];
      ws();
      if (s[i] === "]") {
        i++;
        return a;
      }
      for (;;) {
        a.push(value(depth + 1));
        ws();
        const end = s[i++];
        if (end === "]") return a;
        if (end !== ",") throw 0;
      }
    }
    const match =
      /^(?:true|false|null|-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?)/.exec(
        s.slice(i),
      );
    if (!match) throw 0;
    i += match[0].length;
    const v = JSON.parse(match[0]);
    if (
      typeof v === "number" &&
      (!Number.isFinite(v) || (Number.isInteger(v) && !Number.isSafeInteger(v)))
    )
      throw 0;
    return v;
  };
  try {
    const v = value(0);
    ws();
    if (i !== s.length) throw 0;
    return v;
  } catch {
    throw new Error("INVALID_JSON");
  }
}
