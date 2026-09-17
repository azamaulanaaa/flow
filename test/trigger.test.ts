import { describe, expect, it } from "@effect/vitest"
import { Effect, Fiber, Layer, Queue, TestClock } from "effect"
import { RuntimeBus, RuntimeBusLive } from "@/core/runtime/bus"
import { newRunId } from "@/core/runtime/run-id"
import { makeCronTrigger } from "@/triggers/cron"
import { makeOnceTrigger } from "@/triggers/once"
import { welcomeWorkflow } from "@/workflows/welcome"

const TestLayers = RuntimeBusLive(16)

describe("CronTrigger", () => {
  it.effect("publishes a run request on schedule", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const bus = yield* RuntimeBus
        const trigger = yield* makeCronTrigger({ schedule: "* * * * * *", workflow: "welcome" })
        expect(trigger.tag).toBe("cron:* * * * * *")
        yield* trigger.start

        const take = yield* Effect.fork(Queue.take(bus.queue))
        yield* TestClock.adjust("2 minutes")
        const request = yield* Fiber.join(take)

        expect(request.workflow).toBe("welcome")
        expect(request.trigger).toBe("cron:* * * * * *")
        expect(request.runId).toContain("welcome-")
      }),
    ).pipe(Effect.provide(TestLayers)),
  )

  it.effect("accepts a workflow definition instead of a magic string", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const bus = yield* RuntimeBus
        const trigger = yield* makeCronTrigger({
          schedule: "* * * * * *",
          workflow: welcomeWorkflow,
        })
        yield* trigger.start

        const take = yield* Effect.fork(Queue.take(bus.queue))
        yield* TestClock.adjust("2 minutes")
        const request = yield* Fiber.join(take)

        expect(request.workflow).toBe("welcome")
      }),
    ).pipe(Effect.provide(TestLayers)),
  )

  it.effect("fails fast on invalid cron expressions", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(makeCronTrigger({ schedule: "not-a-cron", workflow: "x" }))
      expect(error._tag).toBe("CronParseError")
    }),
  )
})

describe("OnceTrigger", () => {
  it.effect("fires a single run immediately", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const bus = yield* RuntimeBus
        const trigger = makeOnceTrigger({ workflow: welcomeWorkflow })
        expect(trigger.tag).toBe("once")
        yield* trigger.start

        const request = yield* Queue.take(bus.queue).pipe(Effect.timeout("5 seconds"))
        expect(request?.workflow).toBe("welcome")
        expect(request?.trigger).toBe("once")
        expect(request?.runId).toContain("welcome-")
      }),
    ).pipe(Effect.provide(TestLayers)),
  )
})

describe("newRunId", () => {
  it.effect("generates unique prefixed ids", () =>
    Effect.gen(function* () {
      const ids = new Set([newRunId("welcome"), newRunId("welcome"), newRunId("welcome")])
      expect(ids.size).toBe(3)
      for (const id of ids) {
        expect(id.startsWith("welcome-")).toBe(true)
        expect(id.length).toBeGreaterThan("welcome-".length + 8)
      }
      expect(newRunId("  ")).toContain("run-")
    }),
  )
})
