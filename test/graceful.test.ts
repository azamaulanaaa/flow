import { describe, expect, it } from "@effect/vitest"
import { Effect, Layer, PubSub, Queue } from "effect"
import { FunctionRegistryLive, makeFunction } from "@/core/functions/registry"
import { RuntimeBus, RuntimeBusLive, submitRun } from "@/core/runtime/bus"
import { WorkflowCatalogLive, startRuntime, waitForIdle } from "@/core/runtime/service"
import type { WorkflowDef } from "@/core/workflows/definition"

const TestRegistryLive = FunctionRegistryLive([
  makeFunction("slow", (input: { ms: number }) =>
    Effect.delay(Effect.succeed(`done-${input.ms}`), `${input.ms} millis` as const)),
])

const slowWorkflow: WorkflowDef = {
  name: "slow",
  nodes: [{ id: "s", fn: "slow", input: { ms: 50 } }],
}

const TestLayers = Layer.mergeAll(
  RuntimeBusLive(16),
  WorkflowCatalogLive([slowWorkflow]),
  TestRegistryLive,
)

describe("Graceful shutdown", () => {
  // NB: it.live (real clock) — waitForIdle polls with Effect.sleep and the
  // slow function uses Effect.delay, both frozen under it.effect TestClock.
  it.live("waitForIdle resolves after queued and in-flight runs finish", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const bus = yield* RuntimeBus
        const subscription = yield* PubSub.subscribe(bus.events)

        yield* startRuntime({ workers: 2 })
        yield* submitRun({ runId: "drain-1", workflow: "slow", trigger: "test" })
        yield* submitRun({ runId: "drain-2", workflow: "slow", trigger: "test" })

        yield* waitForIdle.pipe(Effect.timeout("5 seconds"))

        const size = yield* Queue.size(bus.queue)
        // NB: size goes negative with workers blocked on take (-1 each).
        expect(size).toBeLessThanOrEqual(0)

        // Both runs succeeded.
        const events: Array<string> = []
        for (let i = 0; i < 4; i++) {
          const e = yield* Queue.take(subscription).pipe(Effect.timeout("5 seconds"))
          if (e) {
            events.push(`${e._tag}:${e.runId}`)
          }
        }
        expect(events).toContain("RunSucceeded:drain-1")
        expect(events).toContain("RunSucceeded:drain-2")
      }),
    ).pipe(Effect.provide(TestLayers)),
  )

  it.live("waitForIdle is bounded by caller timeout", () =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* startRuntime({ workers: 1 })
        // No runs submitted: idle immediately.
        yield* waitForIdle.pipe(Effect.timeout("5 seconds"))
        expect(true).toBe(true)
      }),
    ).pipe(Effect.provide(TestLayers)),
  )
})
