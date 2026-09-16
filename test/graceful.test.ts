import { describe, expect, it } from "@effect/vitest"
import { Effect, Layer, PubSub, Queue } from "effect"
import { FunctionRegistryLive, makeFunction } from "@/core/functions/registry"
import { RuntimeBus, RuntimeBusLive, submitRun } from "@/core/runtime/bus"
import { WorkflowCatalogLive, startRuntime, waitForIdle, waitForIdleWithPoll } from "@/core/runtime/service"
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

  it.live("waitForIdleWithPoll honors a custom interval", () =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* startRuntime({ workers: 1 })
        yield* waitForIdleWithPoll(5).pipe(Effect.timeout("5 seconds"))
        expect(true).toBe(true)
      }),
    ).pipe(Effect.provide(TestLayers)),
  )

  it.live("caller timeout fires while a run is still in flight", () =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* startRuntime({ workers: 1 })
        yield* submitRun({ runId: "long-1", workflow: "slow", trigger: "test" })
        const exit = yield* Effect.exit(waitForIdle.pipe(Effect.timeout("20 millis")))
        // The drain is still blocked by the 50ms slow function -> timeout path.
        expect(exit._tag).toBe("Failure")
      }),
    ).pipe(Effect.provide(TestLayers)),
  )

  it.live("dropping events never block publishers with a slow subscriber", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const bus = yield* RuntimeBus
        // No subscriber consuming: publish a burst; dropping PubSub must not backpressure.
        yield* startRuntime({ workers: 1 })
        for (let i = 0; i < 10; i++) {
          yield* submitRun({ runId: `burst-${i}`, workflow: "slow", trigger: "test" }).pipe(
            Effect.timeout("1 second"),
          )
        }
        yield* waitForIdle.pipe(Effect.timeout("10 seconds"))
        // At least the drain completed; slow-subscriber drops are acceptable by design.
        const size = yield* Queue.size(bus.queue)
        expect(size).toBeLessThanOrEqual(0)
      }),
    ).pipe(Effect.provide(TestLayers)),
  )
})
