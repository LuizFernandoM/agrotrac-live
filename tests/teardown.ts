export type Cleanup = { name: string; close: () => Promise<unknown> };
export async function settleCleanup(steps: Cleanup[]) {
  const results: {
    name: string;
    status: "passed" | "failed";
    duration_ms: number;
  }[] = [];
  for (const step of steps) {
    const started = Date.now();
    try {
      await step.close();
      results.push({
        name: step.name,
        status: "passed",
        duration_ms: Date.now() - started,
      });
    } catch {
      results.push({
        name: step.name,
        status: "failed",
        duration_ms: Date.now() - started,
      });
    }
  }
  return results;
}
export function finalStatus(
  bodyCompleted: boolean,
  scenarios: { status: string }[],
  cleanup: { status: string }[],
) {
  return bodyCompleted &&
    scenarios.every((s) => s.status === "passed") &&
    cleanup.every((s) => s.status === "passed")
    ? "passed"
    : "failed";
}
