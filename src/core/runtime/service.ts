import { Cause, Context, Effect, Layer, PubSub, Queue, Ref } from "effect"
import type { Scope } from "effect/Scope"
import { FunctionRegistry } from "@/core/functions/registry"
import { runWorkflow, type WorkflowOutputs } from "@/core/workflows/runner"
import type { WorkflowDef } from "@/core/workflows/definition"
import { RuntimeBus, type RunEvent, type RunRequest } from "@/core/runtime/bus"

export class WorkflowCatalog extends Context.Tag("WorkflowCatalog")<
  WorkflowCatalog,
  ReadonlyMap<string, WorkflowDef>
>() {}

export const WorkflowCatalogLive = (
  defs: ReadonlyArray<WorkflowDef>,
): Layer.Layer<WorkflowCatalog> => Layer.succeed(WorkflowCatalog, new Map(defs.map((d) => [d.name, d])))

const publishEvent = (event: RunEvent): Effect.Effect<void, never, RuntimeBus> =>
  Effect.gen(function* () {
    const bus = yield* RuntimeBus
    yield* PubSub.publish(bus.events, event)
  })

const failureReason = (cause: Cause.Cause<unknown>): string => Cause.pretty(cause, { renderErrorCause: true })

/** Best-effort machine-readable tag for a workflow failure cause. */
export const causeTagOf = (cause: Cause.Cause<unknown>): string => {
  const failure = Cause.failureOption(cause)
  if (failure._tag === "Some") {
    const error = failure.value
    if (typeof error === "object" && error !== null && "_tag" in error) {
      const tag = (error as { readonly _tag?: unknown })._tag
      if (typeof tag === "string" && tag.length > 0) {
        return tag
      }
    }
    if (error instanceof Error) {
      return error.name || "Error"
    }
    return "UnknownError"
  }
  if (Cause.isDie(cause)) {
    return "Die"
  }
  return "Interrupt"
}

export interface RuntimeWorkerOptions {
  readonly workflowConcurrency?: number
  readonly nodeTimeoutMs?: number
  readonly retryAttempts?: number
}

const handleRequest = (
  request: RunRequest,
  options: RuntimeWorkerOptions = {},
): Effect.Effect<void, never, RuntimeBus | WorkflowCatalog | FunctionRegistry> =>
  Effect.gen(function* () {
    const bus = yield* RuntimeBus
    yield* Ref.update(bus.inflight, (n) => n + 1)
    return yield* Effect.gen(function* () {
    yield* publishEvent({
      _tag: "RunStarted",
      runId: request.runId,
      workflow: request.workflow,
      trigger: request.trigger,
    })
    const catalog = yield* WorkflowCatalog
    const def = catalog.get(request.workflow)
    if (def === undefined) {
      yield* publishEvent({
        _tag: "RunFailed",
        runId: request.runId,
        workflow: request.workflow,
        reason: `unknown workflow: ${request.workflow}`,
        causeTag: "UnknownWorkflow",
      })
      return
    }
    const exit = yield* Effect.exit(
      runWorkflow(def, {
        defaultInput: request.input,
        concurrency: options.workflowConcurrency,
        nodeTimeoutMs: options.nodeTimeoutMs,
        retryAttempts: options.retryAttempts,
      }),
    )
    if (exit._tag === "Success") {
      const outputs = exit.value as WorkflowOutputs
      yield* publishEvent({ _tag: "RunSucceeded", runId: request.runId, workflow: request.workflow, outputs })
    } else {
      yield* publishEvent({
        _tag: "RunFailed",
        runId: request.runId,
        workflow: request.workflow,
        reason: failureReason(exit.cause),
        causeTag: causeTagOf(exit.cause),
      })
    }
    }).pipe(Effect.ensuring(Ref.update(bus.inflight, (n) => Math.max(0, n - 1))))
  }).pipe(
    Effect.withSpan(`run.${request.workflow}`, {
      attributes: {
        "run.id": request.runId,
        "workflow.name": request.workflow,
        "trigger": request.trigger,
      },
    }),
  )

const makeWorker = (options: RuntimeWorkerOptions = {}) =>
  Effect.gen(function* () {
    const bus = yield* RuntimeBus
    while (true) {
      const request = yield* Queue.take(bus.queue)
      yield* handleRequest(request, options)
    }
  })

/**
 * Start worker fibers that drain the run queue.
 *
 * Scoped: fibers are supervised by the caller's scope, so shutting the scope
 * down stops the runtime gracefully. Workers never die on workflow failure —
 * every outcome is captured into a `RunEvent`.
 */
export const startRuntime = (
  options: { readonly workers?: number } & RuntimeWorkerOptions = {},
): Effect.Effect<void, never, RuntimeBus | WorkflowCatalog | FunctionRegistry | Scope> =>
  Effect.gen(function* () {
    const count = options.workers ?? 4
    for (let i = 0; i < count; i++) {
      yield* Effect.forkScoped(
        Effect.forever(
          makeWorker({
            workflowConcurrency: options.workflowConcurrency,
            nodeTimeoutMs: options.nodeTimeoutMs,
            retryAttempts: options.retryAttempts,
          }),
        ).pipe(Effect.withSpan("runtime.worker")),
      )
    }
    yield* Effect.log(`Runtime started with ${count} workers`)
  })

/**
 * Wait until the run queue is empty and no runs are in flight.
 * Used during graceful shutdown so in-flight workflows finish before
 * layers (OTel flush) are released. Caller should bound with a timeout.
 *
 * `pollMs` controls the idle-check interval (default 25ms). Lower values
 * wake faster under load; higher values reduce timer churn when idle.
 */
export const waitForIdleWithPoll = (
  pollMs = 25,
): Effect.Effect<void, never, RuntimeBus> =>
  Effect.gen(function* () {
    const bus = yield* RuntimeBus
    const interval = Math.max(1, Math.floor(pollMs))
    while (true) {
      const size = yield* Queue.size(bus.queue)
      const active = yield* Ref.get(bus.inflight)
      // NB: Queue.size goes negative when workers are blocked on take
      // (one pending taker counts as -1), so idle is size <= 0.
      if (size <= 0 && active === 0) {
        return
      }
      yield* Effect.sleep(`${interval} millis`)
    }
  })

/** Default idle wait (25ms poll). Prefer {@link waitForIdleWithPoll} to tune. */
export const waitForIdle: Effect.Effect<void, never, RuntimeBus> = waitForIdleWithPoll(25)
