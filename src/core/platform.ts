import { Effect, Fiber } from "effect"

export type RuntimeKind = "deno" | "bun" | "node"

/** Detect the current JS runtime without touching runtime-specific globals. */
export const runtimeKind = (): RuntimeKind => {
  if ("Deno" in globalThis) {
    return "deno"
  }
  if ("Bun" in globalThis) {
    return "bun"
  }
  return "node"
}

interface DenoSignalApi {
  readonly addSignalListener: (signal: string, handler: () => void) => void
  readonly removeSignalListener: (signal: string, handler: () => void) => void
}

const denoApi = (): DenoSignalApi | undefined =>
  (globalThis as { readonly Deno?: DenoSignalApi }).Deno

/**
 * `NodeRuntime.runMain` equivalent for Deno (no `@effect/platform-node`
 * dependency): forks the effect and interrupts the fiber on SIGINT/SIGTERM,
 * so scope finalizers (drain, OTel flush) run exactly like on Node/Bun.
 */
export const runWithDenoSignals = async <A, E>(effect: Effect.Effect<A, E>): Promise<void> => {
  const fiber = Effect.runFork(effect)
  const api = denoApi()
  if (api === undefined) {
    await Effect.runPromise(Fiber.await(fiber))
    return
  }
  const interrupt = (): void => {
    Effect.runFork(Fiber.interrupt(fiber))
  }
  api.addSignalListener("SIGINT", interrupt)
  api.addSignalListener("SIGTERM", interrupt)
  try {
    await Effect.runPromise(Fiber.await(fiber))
  } finally {
    api.removeSignalListener("SIGINT", interrupt)
    api.removeSignalListener("SIGTERM", interrupt)
  }
}
