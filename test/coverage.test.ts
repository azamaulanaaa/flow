import { describe, expect, it } from "@effect/vitest"
import { Deferred, Effect, Fiber, Queue } from "effect"
import { AppConfigService, AppConfigLive } from "@/core/config"
import { FUNCTION_WORKER_MODE, isFunctionWorkerThread } from "@/core/runtime/function-worker"
import { RuntimeBus, RuntimeBusLive, submitRun } from "@/core/runtime/bus"
import { runtimeKind } from "@/core/platform"

describe("RuntimeBus backpressure", () => {
  it.live("bounded queue blocks submitRun when full until a take frees space", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const bus = yield* RuntimeBus
        yield* submitRun({ runId: "fill-1", workflow: "w", trigger: "test" })
        // Queue capacity is 1 and now full: a second submit must block.
        const done = yield* Deferred.make<void>()
        const fiber = yield* Effect.fork(
          submitRun({ runId: "fill-2", workflow: "w", trigger: "test" }).pipe(
            Effect.ensuring(Deferred.succeed(done, undefined as void)),
          ),
        )
        yield* Effect.sleep("50 millis")
        expect(yield* Deferred.isDone(done)).toBe(false)
        // Free one slot: the blocked submit completes.
        expect((yield* Queue.take(bus.queue)).runId).toBe("fill-1")
        yield* Fiber.join(fiber)
        expect(yield* Deferred.isDone(done)).toBe(true)
        expect((yield* Queue.take(bus.queue)).runId).toBe("fill-2")
      }),
    ).pipe(Effect.provide(RuntimeBusLive(1))),
  )
})

describe("platform", () => {
  it.effect("detects node runtime under vitest", () =>
    Effect.gen(function* () {
      expect(runtimeKind()).toBe("node")
    }),
  )
})

describe("config defaults", () => {
  it.effect("provides sane runtime tuning values", () =>
    Effect.gen(function* () {
      const config = yield* AppConfigService
      expect(config.serviceName.length).toBeGreaterThan(0)
      expect(config.queueCapacity).toBeGreaterThan(0)
      expect(config.runtimeWorkers).toBeGreaterThanOrEqual(1)
      expect(config.workflowConcurrency).toBeGreaterThanOrEqual(1)
      expect(config.workflowNodeTimeoutMs).toBeGreaterThanOrEqual(0)
      expect(config.workflowRetryAttempts).toBeGreaterThanOrEqual(0)
      expect(config.shutdownTimeoutMs).toBeGreaterThan(0)
      expect(config.workerPoolTimeoutMs).toBeGreaterThan(0)
    }).pipe(Effect.provide(AppConfigLive)),
  )
})

describe("function-worker entry", () => {
  it.effect("reports main-thread context in tests", () =>
    Effect.gen(function* () {
      expect(FUNCTION_WORKER_MODE).toBe("function-worker")
      expect(isFunctionWorkerThread()).toBe(false)
    }),
  )
})
