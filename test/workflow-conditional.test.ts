import { describe, expect, it } from "@effect/vitest"
import { Effect, Layer, PubSub, Queue } from "effect"
import { FunctionRegistryLive, makeFunction } from "@/core/functions/registry"
import { RuntimeBus, RuntimeBusLive, submitRun } from "@/core/runtime/bus"
import { WorkflowCatalogLive, startRuntime } from "@/core/runtime/service"
import type { WorkflowDef } from "@/core/workflows/definition"
import { runWorkflow } from "@/core/workflows/runner"

const condRegistry = FunctionRegistryLive([
  makeFunction("flag", () => Effect.succeed("off")),
  makeFunction("record", (input: unknown) => Effect.succeed(input)),
  makeFunction("summarize", (input: unknown) => Effect.succeed(input)),
])

const condWorkflow: WorkflowDef = {
  name: "cond",
  nodes: [
    { id: "flag", fn: "flag" },
    {
      id: "hot",
      fn: "record",
      dependsOn: ["flag"],
      when: (outputs) => outputs.get("flag") === "on",
      input: (outputs: ReadonlyMap<string, unknown>) => outputs.get("flag"),
    },
    {
      id: "cold",
      fn: "record",
      dependsOn: ["flag"],
      when: (outputs) => outputs.get("flag") === "off",
      input: (outputs: ReadonlyMap<string, unknown>) => outputs.get("flag"),
    },
    {
      id: "summary",
      fn: "summarize",
      dependsOn: ["hot", "cold"],
      input: (outputs: ReadonlyMap<string, unknown>) => ({
        hot: outputs.get("hot"),
        cold: outputs.get("cold"),
      }),
    },
  ],
}

const flippedRegistry = FunctionRegistryLive([
  makeFunction("flag-on", () => Effect.succeed("on")),
  makeFunction("record", (input: unknown) => Effect.succeed(input)),
  makeFunction("summarize", (input: unknown) => Effect.succeed(input)),
])

const TestLayers = Layer.mergeAll(
  RuntimeBusLive(16),
  WorkflowCatalogLive([condWorkflow]),
  condRegistry,
)

describe("Conditional branching", () => {
  it.effect("skips gated nodes and propagates the skip transitively", () =>
    Effect.gen(function* () {
      const { outputs, skipped } = yield* runWorkflow(condWorkflow)
      // "hot" gated off; "summary" skipped via its skipped dependency.
      expect(skipped).toEqual(["hot", "summary"])
      expect(outputs.get("flag")).toBe("off")
      expect(outputs.get("cold")).toBe("off")
      expect(outputs.has("hot")).toBe(false)
      expect(outputs.has("summary")).toBe(false)
    }).pipe(Effect.provide(condRegistry)),
  )

  it.effect("runs the other branch when the gate flips", () =>
    Effect.gen(function* () {
      const flipped: WorkflowDef = {
        ...condWorkflow,
        nodes: condWorkflow.nodes.map((node) =>
          node.id === "flag" ? { ...node, fn: "flag-on" } : node,
        ),
      }
      const registry = flippedRegistry
      const { outputs, skipped } = yield* runWorkflow(flipped)
      expect(skipped).toEqual(["cold", "summary"])
      expect(outputs.get("hot")).toBe("on")
      expect(outputs.has("cold")).toBe(false)
    }).pipe(Effect.provide(flippedRegistry)),
  )

  it.effect("publishes skipped ids on RunSucceeded", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const bus = yield* RuntimeBus
        const subscription = yield* PubSub.subscribe(bus.events)

        yield* startRuntime({ workers: 1 })

        yield* submitRun({ runId: "cond-1", workflow: "cond", trigger: "test" })

        let event = yield* Queue.take(subscription).pipe(Effect.timeout("5 seconds"))
        while (event?._tag === "RunStarted") {
          event = yield* Queue.take(subscription).pipe(Effect.timeout("5 seconds"))
        }
        expect(event?._tag).toBe("RunSucceeded")
        if (event?._tag === "RunSucceeded") {
          expect(event.runId).toBe("cond-1")
          expect(event.skipped).toEqual(["hot", "summary"])
          expect(event.outputs.get("cold")).toBe("off")
        }
      }),
    ).pipe(Effect.provide(TestLayers)),
  )
})
