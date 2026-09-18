import { describe, expect, it } from "@effect/vitest"
import { Effect, Exit, Option, Queue, Scope } from "effect"
import { RuntimeBus, RuntimeBusLive } from "@/core/runtime/bus"
import { makeSubscriptionTrigger } from "@/core/triggers/trigger"
import { welcomeWorkflow } from "@/workflows/welcome"

const TestLayers = RuntimeBusLive(16)

describe("SubscriptionTrigger", () => {
  it.effect("fires runs from external events and unsubscribes on scope close", () =>
    Effect.gen(function* () {
      const bus = yield* RuntimeBus
      const emits: Array<(input?: unknown) => void> = []
      let released = 0
      // Fake external source (PocketBase subscribe shape): gated by a live
      // flag so post-unsubscribe events never reach the bus.
      let live = false
      const trigger = makeSubscriptionTrigger({
        tag: "pocketbase:notes",
        workflow: welcomeWorkflow,
        subscribe: (fire) =>
          Effect.gen(function* () {
            live = true
            emits.push((input?: unknown) => {
              if (live) {
                fire(input)
              }
            })
            return Effect.sync(() => {
              live = false
              released += 1
            })
          }),
      })
      expect(trigger.tag).toBe("pocketbase:notes")

      const scope = yield* Scope.make()
      yield* trigger.start.pipe(Scope.extend(scope))

      // `start` forks: wait until the subscription is live (no clock
      // dependency — bounded yield loop works under TestClock too).
      yield* Effect.gen(function* () {
        let n = 0
        while (emits.length === 0 && n < 1000) {
          yield* Effect.yieldNow()
          n += 1
        }
        return yield* emits.length === 0
          ? Effect.fail(new Error("subscribe never ran"))
          : Effect.void
      })

      emits[0]?.({ note: "hello" })
      const request = yield* Queue.take(bus.queue).pipe(Effect.timeout("5 seconds"))
      expect(request?.workflow).toBe("welcome")
      expect(request?.trigger).toBe("pocketbase:notes")
      expect(request?.input).toEqual({ note: "hello" })

      // Scope close (SIGINT/SIGTERM path) runs the release: the source is
      // detached and late events are dropped.
      yield* Scope.close(scope, Exit.void)
      expect(released).toBe(1)
      emits[0]?.({ note: "late" })
      expect(Option.isNone(yield* Queue.poll(bus.queue))).toBe(true)
    }).pipe(Effect.provide(TestLayers), Effect.scoped),
  )
})
