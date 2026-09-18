import { describe, expect, it } from "@effect/vitest"
import { Effect, Layer, PubSub, Queue } from "effect"
import { FunctionRegistryLive, makeFunction } from "@/core/functions/registry"
import { RuntimeBus, RuntimeBusLive, submitRun } from "@/core/runtime/bus"
import { DuplicateWorkflowError, WorkflowCatalogLive, startRuntime } from "@/core/runtime/service"
import type { WorkflowDef } from "@/core/workflows/definition"

const TestRegistryLive = FunctionRegistryLive([makeFunction("ping", () => Effect.succeed("pong"))])

const pingWorkflow: WorkflowDef = {
  name: "ping",
  nodes: [{ id: "ping", fn: "ping" }],
}

const TestLayers = Layer.mergeAll(
  RuntimeBusLive(16),
  WorkflowCatalogLive([pingWorkflow]),
  TestRegistryLive,
)

describe("Runtime", () => {
  it.effect("executes a submitted run and publishes lifecycle events", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const bus = yield* RuntimeBus
        const subscription = yield* PubSub.subscribe(bus.events)

        yield* startRuntime({ workers: 2 })

        yield* submitRun({ runId: "run-1", workflow: "ping", trigger: "test" })

        const started = yield* Queue.take(subscription).pipe(Effect.timeout("5 seconds"))
        expect(started?._tag).toBe("RunStarted")

        const finished = yield* Queue.take(subscription).pipe(Effect.timeout("5 seconds"))
        expect(finished?._tag).toBe("RunSucceeded")
        if (finished?._tag === "RunSucceeded") {
          expect(finished.runId).toBe("run-1")
          expect(finished.outputs.get("ping")).toBe("pong")
        }
      }),
    ).pipe(Effect.provide(TestLayers)),
  )

  it.effect("publishes RunFailed for unknown workflows without killing workers", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const bus = yield* RuntimeBus
        const subscription = yield* PubSub.subscribe(bus.events)

        yield* startRuntime({ workers: 1 })

        yield* submitRun({ runId: "run-bad", workflow: "ghost", trigger: "test" })
        const failed = yield* Queue.take(subscription).pipe(Effect.timeout("5 seconds"))
        // First event for this run is RunStarted; drain until RunFailed.
        const second =
          failed?._tag === "RunStarted"
            ? yield* Queue.take(subscription).pipe(Effect.timeout("5 seconds"))
            : failed
        expect(second?._tag).toBe("RunFailed")

        // Worker still alive: a valid run completes afterwards.
        yield* submitRun({ runId: "run-2", workflow: "ping", trigger: "test" })
        let event = yield* Queue.take(subscription).pipe(Effect.timeout("5 seconds"))
        while (
          event?._tag === "RunStarted" ||
          (event?._tag !== undefined && (event as { runId?: string }).runId !== "run-2")
        ) {
          event = yield* Queue.take(subscription).pipe(Effect.timeout("5 seconds"))
        }
        expect(event?._tag).toBe("RunSucceeded")
      }),
    ).pipe(Effect.provide(TestLayers)),
  )

  it.effect("fails fast on duplicate workflow names", () =>
    Effect.gen(function* () {
      const bad = WorkflowCatalogLive([pingWorkflow, { ...pingWorkflow }])
      const error = yield* Effect.flip(Layer.build(bad).pipe(Effect.scoped))
      expect(error).toBeInstanceOf(DuplicateWorkflowError)
    }),
  )
})
