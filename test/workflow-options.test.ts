import { describe, expect, it } from "@effect/vitest"
import { Effect, Layer, PubSub, Queue } from "effect"
import { FunctionRegistryLive, makeFunction } from "@/core/functions/registry"
import { RuntimeBus, RuntimeBusLive, submitRun } from "@/core/runtime/bus"
import { WorkflowCatalogLive, startRuntime } from "@/core/runtime/service"
import { runWorkflow } from "@/core/workflows/runner"
import type { WorkflowDef } from "@/core/workflows/definition"

const echoRegistry = FunctionRegistryLive([
  makeFunction("echo", (input: unknown) => Effect.succeed(input)),
  makeFunction("const-a", () => Effect.succeed("A")),
  makeFunction("fail", () => Effect.fail(new Error("boom"))),
])

describe("runWorkflow options", () => {
  it.effect("uses defaultInput for nodes without explicit input", () =>
    Effect.gen(function* () {
      const def: WorkflowDef = { name: "echo-flow", nodes: [{ id: "e", fn: "echo" }] }
      const outputs = yield* runWorkflow(def, { defaultInput: { hello: "world" } })
      expect(outputs.get("e")).toEqual({ hello: "world" })
    }).pipe(Effect.provide(echoRegistry)),
  )

  it.effect("prefers explicit node input over defaultInput", () =>
    Effect.gen(function* () {
      const def: WorkflowDef = {
        name: "explicit",
        nodes: [{ id: "e", fn: "echo", input: "static" }],
      }
      const outputs = yield* runWorkflow(def, { defaultInput: "fallback" })
      expect(outputs.get("e")).toBe("static")
    }).pipe(Effect.provide(echoRegistry)),
  )

  it.effect("respects bounded concurrency and still completes", () =>
    Effect.gen(function* () {
      const def: WorkflowDef = {
        name: "fanout",
        nodes: [
          { id: "a", fn: "const-a" },
          { id: "b", fn: "const-a" },
          { id: "c", fn: "const-a" },
        ],
      }
      const outputs = yield* runWorkflow(def, { concurrency: 1 })
      expect(outputs.get("a")).toBe("A")
      expect(outputs.get("b")).toBe("A")
      expect(outputs.get("c")).toBe("A")
    }).pipe(Effect.provide(echoRegistry)),
  )

  it.effect("propagates node failure as RunFailed reason", () =>
    Effect.gen(function* () {
      const def: WorkflowDef = { name: "broken", nodes: [{ id: "f", fn: "fail" }] }
      const error = yield* Effect.flip(runWorkflow(def).pipe(Effect.provide(echoRegistry)))
      expect(String(error)).toContain("boom")
    }),
  )
})

describe("RunRequest input threading", () => {
  it.effect("threads submitRun input into nodes without explicit input", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const bus = yield* RuntimeBus
        const subscription = yield* PubSub.subscribe(bus.events)
        yield* startRuntime({ workers: 1, workflowConcurrency: 4 })
        yield* submitRun({ runId: "input-1", workflow: "echo-wf", trigger: "test", input: "run-data" })
        const started = yield* Queue.take(subscription).pipe(Effect.timeout("5 seconds"))
        expect(started?._tag).toBe("RunStarted")
        const finished = yield* Queue.take(subscription).pipe(Effect.timeout("5 seconds"))
        expect(finished?._tag).toBe("RunSucceeded")
        if (finished?._tag === "RunSucceeded") {
          expect(finished.outputs.get("e")).toBe("run-data")
        }
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(
          RuntimeBusLive(16),
          WorkflowCatalogLive([{ name: "echo-wf", nodes: [{ id: "e", fn: "echo" }] }]),
          echoRegistry,
        ),
      ),
    ),
  )
})
