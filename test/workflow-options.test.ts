import { describe, expect, it } from "@effect/vitest"
import { Cause, Effect, Layer, PubSub, Queue, Ref } from "effect"
import { FunctionRegistryLive, makeFunction } from "@/core/functions/registry"
import { RuntimeBus, RuntimeBusLive, submitRun } from "@/core/runtime/bus"
import { WorkflowCatalogLive, causeTagOf, startRuntime } from "@/core/runtime/service"
import { runWorkflow, WorkflowNodeTimeoutError } from "@/core/workflows/runner"
import type { WorkflowDef } from "@/core/workflows/definition"

const echoRegistry = FunctionRegistryLive([
  makeFunction("echo", (input: unknown) => Effect.succeed(input)),
  makeFunction("const-a", () => Effect.succeed("A")),
  makeFunction("fail", () => Effect.fail(new Error("boom"))),
  makeFunction("slow", () => Effect.sleep("500 millis").pipe(Effect.as("slow-done"))),
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

  it.live("times out slow nodes with WorkflowNodeTimeoutError", () =>
    Effect.gen(function* () {
      const def: WorkflowDef = { name: "slow-flow", nodes: [{ id: "s", fn: "slow" }] }
      const error = yield* Effect.flip(runWorkflow(def, { nodeTimeoutMs: 50 }).pipe(Effect.provide(echoRegistry)))
      expect(error).toBeInstanceOf(WorkflowNodeTimeoutError)
    }),
  )

  it.effect("retries failing nodes and succeeds on second attempt", () =>
    Effect.gen(function* () {
      const calls = yield* Ref.make(0)
      const flakyRegistry = FunctionRegistryLive([
        makeFunction("flaky", () =>
          Effect.gen(function* () {
            const n = yield* Ref.updateAndGet(calls, (c) => c + 1)
            if (n < 2) {
              return yield* Effect.fail(new Error("first-try-boom"))
            }
            return "recovered"
          }),
        ),
      ])
      const def: WorkflowDef = { name: "flaky-flow", nodes: [{ id: "f", fn: "flaky" }] }
      const outputs = yield* runWorkflow(def, { retryAttempts: 1 }).pipe(Effect.provide(flakyRegistry))
      expect(outputs.get("f")).toBe("recovered")
      expect(yield* Ref.get(calls)).toBe(2)
    }),
  )
})

describe("RunFailed causeTag", () => {
  it.effect("tags unknown workflows", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const bus = yield* RuntimeBus
        const subscription = yield* PubSub.subscribe(bus.events)
        yield* startRuntime({ workers: 1 })
        yield* submitRun({ runId: "tag-bad", workflow: "ghost", trigger: "test" })
        let event = yield* Queue.take(subscription).pipe(Effect.timeout("5 seconds"))
        while (event?._tag === "RunStarted") {
          event = yield* Queue.take(subscription).pipe(Effect.timeout("5 seconds"))
        }
        expect(event?._tag).toBe("RunFailed")
        if (event?._tag === "RunFailed") {
          expect(event.causeTag).toBe("UnknownWorkflow")
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

  it.effect("tags workflow failures with the underlying error tag", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const bus = yield* RuntimeBus
        const subscription = yield* PubSub.subscribe(bus.events)
        yield* startRuntime({ workers: 1 })
        yield* submitRun({ runId: "tag-fail", workflow: "missing-fn", trigger: "test" })
        let event = yield* Queue.take(subscription).pipe(Effect.timeout("5 seconds"))
        while (event?._tag === "RunStarted") {
          event = yield* Queue.take(subscription).pipe(Effect.timeout("5 seconds"))
        }
        expect(event?._tag).toBe("RunFailed")
        if (event?._tag === "RunFailed") {
          expect(event.causeTag).toBe("UnknownFunctionError")
          expect(event.reason).toContain("nope")
        }
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(
          RuntimeBusLive(16),
          WorkflowCatalogLive([{ name: "missing-fn", nodes: [{ id: "x", fn: "nope" }] }]),
          echoRegistry,
        ),
      ),
    ),
  )

  it.effect("causeTagOf maps tagged errors, Errors and dies", () =>
    Effect.gen(function* () {
      expect(causeTagOf(Cause.fail(new WorkflowNodeTimeoutError({ workflow: "w", node: "n", timeoutMs: 10 })))).toBe(
        "WorkflowNodeTimeoutError",
      )
      expect(causeTagOf(Cause.fail(new Error("boom")))).toBe("Error")
      expect(causeTagOf(Cause.die(new Error("die")))).toBe("Die")
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
