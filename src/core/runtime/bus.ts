import { Context, Effect, Layer, PubSub, Queue, Ref } from "effect"

/** Request to execute a workflow, produced by triggers. */
export interface RunRequest {
  readonly runId: string
  readonly workflow: string
  readonly trigger: string
  readonly input?: unknown
}

export type RunEvent =
  | {
      readonly _tag: "RunStarted"
      readonly runId: string
      readonly workflow: string
      readonly trigger: string
    }
  | {
      readonly _tag: "RunSucceeded"
      readonly runId: string
      readonly workflow: string
      readonly outputs: ReadonlyMap<string, unknown>
      /** Node ids skipped via `when` gates (empty when everything ran). */
      readonly skipped: ReadonlyArray<string>
    }
  | {
      readonly _tag: "RunFailed"
      readonly runId: string
      readonly workflow: string
      readonly reason: string
      /** Machine-readable failure tag (e.g. `UnknownFunctionError`, `WorkflowNodeTimeoutError`, `UnknownWorkflow`). Optional for backward compatibility. */
      readonly causeTag?: string
    }

export interface RuntimeBusShape {
  readonly queue: Queue.Queue<RunRequest>
  readonly events: PubSub.PubSub<RunEvent>
  /** In-flight run count, for graceful drain on shutdown. */
  readonly inflight: Ref.Ref<number>
}

export class RuntimeBus extends Context.Tag("RuntimeBus")<RuntimeBus, RuntimeBusShape>() {}

/**
 * In-process bus: bounded queue with backpressure for run requests,
 * dropping pubsub for lifecycle events (slow subscribers never block workers).
 */
export const RuntimeBusLive = (queueCapacity = 128): Layer.Layer<RuntimeBus> =>
  Layer.effect(
    RuntimeBus,
    Effect.gen(function* () {
      const queue = yield* Queue.bounded<RunRequest>(queueCapacity)
      const events = yield* PubSub.dropping<RunEvent>(256)
      const inflight = yield* Ref.make(0)
      return { queue, events, inflight }
    }),
  )

/** Submit a run request (applies backpressure when the queue is full). */
export const submitRun = (request: RunRequest): Effect.Effect<void, never, RuntimeBus> =>
  Effect.gen(function* () {
    const bus = yield* RuntimeBus
    yield* Queue.offer(bus.queue, request)
  })

/** Scoped subscription to run lifecycle events. */
export const subscribeRunEvents = Effect.gen(function* () {
  const bus = yield* RuntimeBus
  return yield* PubSub.subscribe(bus.events)
})
