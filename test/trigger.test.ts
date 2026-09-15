import { describe, expect, it } from "@effect/vitest"
import { Effect, Fiber, Layer, Queue, TestClock } from "effect"
import { RuntimeBus, RuntimeBusLive } from "@/core/runtime/bus"
import { makeCronTrigger } from "@/core/triggers/cron"

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

  it.effect("fails fast on invalid cron expressions", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(makeCronTrigger({ schedule: "not-a-cron", workflow: "x" }))
      expect(error._tag).toBe("CronParseError")
    }),
  )
})
