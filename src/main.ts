import { configuration } from "./infra/config.js";
import { LiveRuntime } from "./infra/runtime.js";
async function main() {
  const runtime = new LiveRuntime(configuration());
  const ended = new Promise<void>((resolve) => {
    const stop = () =>
      void runtime.stop().then(resolve, () => {
        console.error("LIVE_SHUTDOWN_FAILED");
        process.exitCode = 1;
        resolve();
      });
    process.once("SIGINT", stop);
    process.once("SIGTERM", stop);
  });
  try {
    await runtime.start();
    console.log("LIVE_LOCAL_LISTENING");
    await ended;
  } catch {
    await runtime.stop();
    throw new Error("LIVE_START_FAILED");
  }
}
main().catch(() => {
  console.error("LIVE_START_FAILED");
  process.exitCode = 1;
});
