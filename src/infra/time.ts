export async function deadline<T>(
  work: Promise<T>,
  ms: number,
  late?: (value: T) => void,
): Promise<T> {
  let expired = false;
  let timer: NodeJS.Timeout | undefined;
  const tracked = work.then((v) => {
    if (expired) late?.(v);
    return v;
  });
  try {
    return await Promise.race([
      tracked,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => {
          expired = true;
          reject(new Error("IO_TIMEOUT"));
        }, ms);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}
/** Cancels the wait and its owned transport, and always removes its listener. */
export async function abortable<T>(
  work: Promise<T>,
  signal?: AbortSignal,
  cancel?: () => void,
): Promise<T> {
  if (!signal) return work;
  let onAbort = () => {};
  const aborted = new Promise<never>((_, reject) => {
    onAbort = () => {
      cancel?.();
      reject(new Error("ABORTED"));
    };
    signal.addEventListener("abort", onAbort, { once: true });
    if (signal.aborted) onAbort();
  });
  try {
    const value = await Promise.race([work, aborted]);
    signal.throwIfAborted();
    return value;
  } finally {
    signal.removeEventListener("abort", onAbort);
  }
}
export function delay(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal?.aborted) return resolve();
    const done = () => {
      clearTimeout(timer);
      signal?.removeEventListener("abort", done);
      resolve();
    };
    const timer = setTimeout(done, ms);
    signal?.addEventListener("abort", done, { once: true });
  });
}
export class Serial {
  private tail: Promise<unknown> = Promise.resolve();
  run<T>(work: () => Promise<T>): Promise<T> {
    const task = this.tail.then(work);
    this.tail = task.catch(() => {});
    return task;
  }
}
