import { describe, expect, it } from "@effect/vitest"
import { Effect, Layer } from "effect"
import { FunctionRegistryLive, makeFunction } from "@/core/functions/registry"
import {
  WorkerPool,
  WorkerPoolLive,
  WorkerPoolError,
  WorkerPoolTimeoutError,
  allocRequestId,
} from "@/core/runtime/worker-pool"
import { runWorkflow } from "@/core/workflows/runner"
import type { WorkflowDef } from "@/core/workflows/definition"

const fixtureUrl = new URL("./fixtures/echo-worker.mjs", import.meta.url)
const missingUrl = new URL("./fixtures/does-not-exist.mjs", import.meta.url)

const poolLive = (overrides?: Partial<{ size: number; timeoutMs: number; entryUrl: URL }>) =>
  WorkerPoolLive({
    enabled: true,
    size: 2,
    entryUrl: fixtureUrl,
    timeoutMs: 5000,
    ...overrides,
  })

// NB: it.live (real clock + real threads) — pool timeouts need live timers.
describe("WorkerPool", () => {
  it.live("executes through worker threads", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const pool = yield* WorkerPool
        const output = yield* pool.execute("any-fn", { hello: "world" })
        expect(output).toEqual({ echo: { hello: "world" } })
      }),
    ).pipe(Effect.provide(poolLive())),
  )

  it.live("propagates worker failures", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const pool = yield* WorkerPool
        const error = yield* Effect.flip(pool.execute("fails", {}))
        expect(error).toBeInstanceOf(WorkerPoolError)
      }),
    ).pipe(Effect.provide(poolLive())),
  )

  it.live("times out slow workers", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const pool = yield* WorkerPool
        const error = yield* Effect.flip(pool.execute("never-replies", {}))
        expect(error).toBeInstanceOf(WorkerPoolTimeoutError)
      }),
    ).pipe(Effect.provide(poolLive({ timeoutMs: 200 }))),
  )

  it.live("routes runWorkflow through the pool when provided", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const def: WorkflowDef = {
          name: "echo-flow",
          nodes: [{ id: "only", fn: "anything", input: { x: 1 } }],
        }
        const outputs = yield* runWorkflow(def)
        // Echo fixture wraps input instead of running a real function.
        expect(outputs.get("only")).toEqual({ echo: { x: 1 } })
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(
          poolLive(),
          FunctionRegistryLive([makeFunction("anything", () => Effect.succeed("local"))]),
        ),
      ),
    ),
  )

  it.live("falls back to in-process when no live workers", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const def: WorkflowDef = {
          name: "ping",
          nodes: [{ id: "ping", fn: "ping" }],
        }
        const outputs = yield* runWorkflow(def)
        expect(outputs.get("ping")).toBe("pong")
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(
          WorkerPoolLive({ enabled: true, size: 1, entryUrl: missingUrl, timeoutMs: 1000 }),
          FunctionRegistryLive([makeFunction("ping", () => Effect.succeed("pong"))]),
        ),
      ),
    ),
  )

  it.live("disabled pool falls back to in-process", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const def: WorkflowDef = {
          name: "ping",
          nodes: [{ id: "ping", fn: "ping" }],
        }
        const outputs = yield* runWorkflow(def)
        expect(outputs.get("ping")).toBe("pong")
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(
          WorkerPoolLive({ enabled: false, size: 0, entryUrl: fixtureUrl, timeoutMs: 1000 }),
          FunctionRegistryLive([makeFunction("ping", () => Effect.succeed("pong"))]),
        ),
      ),
    ),
  )

  it.live("fails fast on unserializable input", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const pool = yield* WorkerPool
        const error = yield* Effect.flip(pool.execute("any-fn", () => {}))
        expect(error).toBeInstanceOf(WorkerPoolError)
        expect(error._tag).toBe("WorkerPoolError")
        if (error._tag === "WorkerPoolError") {
          expect(error.reason).toContain("unserializable input")
        }
      }),
    ).pipe(Effect.provide(poolLive())),
  )
})

describe("allocRequestId", () => {
  it.effect("wraps around MAX_SAFE_INTEGER without colliding", () =>
    Effect.gen(function* () {
      const pending = new Map<number, unknown>([[1, "taken"]])
      const slot = { nextId: Number.MAX_SAFE_INTEGER, pending }
      expect(allocRequestId(slot)).toBe(2)
    }),
  )

  it.effect("returns -1 when the id space is exhausted", () =>
    Effect.gen(function* () {
      const pending = new Map<number, unknown>()
      for (let i = 1; i <= 1001; i++) {
        pending.set(i, "x")
      }
      const slot = { nextId: 1, pending }
      expect(allocRequestId(slot)).toBe(-1)
    }),
  )
})
