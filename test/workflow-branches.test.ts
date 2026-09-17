import { describe, expect, it } from "@effect/vitest"
import { Effect, Layer, PubSub, Queue } from "effect"
import { FunctionRegistryLive } from "@/core/functions/registry"
import { RuntimeBus, RuntimeBusLive, submitRun } from "@/core/runtime/bus"
import { WorkflowCatalogLive, startRuntime } from "@/core/runtime/service"
import { planWorkflow } from "@/core/workflows/definition"
import { exampleFunctions, welcomeWorkflow } from "@/workflows"

const TestLayers = Layer.mergeAll(
  RuntimeBusLive(16),
  WorkflowCatalogLive([welcomeWorkflow]),
  FunctionRegistryLive([...exampleFunctions]),
)

describe("Branched workflow", () => {
  it.effect("plans diverge-then-converge levels", () =>
    Effect.gen(function* () {
      const levels = yield* planWorkflow(welcomeWorkflow)
      expect(levels.map((level) => level.map((node) => node.id))).toEqual([
        ["greet"],
        ["upper", "count"],
        ["report"],
      ])
    }),
  )

  it.effect("runs every tracked branch and joins outputs", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const bus = yield* RuntimeBus
        const subscription = yield* PubSub.subscribe(bus.events)

        yield* startRuntime({ workers: 2 })

        yield* submitRun({ runId: "branch-1", workflow: "welcome", trigger: "test" })

        let event = yield* Queue.take(subscription).pipe(Effect.timeout("5 seconds"))
        while (event?._tag === "RunStarted") {
          event = yield* Queue.take(subscription).pipe(Effect.timeout("5 seconds"))
        }
        expect(event?._tag).toBe("RunSucceeded")
        if (event?._tag === "RunSucceeded") {
          expect(event.runId).toBe("branch-1")
          expect(event.outputs.get("greet")).toBe("Hello, world!")
          expect(event.outputs.get("upper")).toBe("HELLO, WORLD!")
          expect(event.outputs.get("count")).toBe(13)
          expect(event.outputs.get("report")).toBe("HELLO, WORLD! (13 chars)")
        }
      }),
    ).pipe(Effect.provide(TestLayers)),
  )
})
