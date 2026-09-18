import { Effect } from "effect"
import type { Scope } from "effect/Scope"
import { RuntimeBus, submitRun } from "@/core/runtime/bus"
import { newRunId } from "@/core/runtime/run-id"
import type { WorkflowDef } from "@/core/workflows/definition"

/**
 * A trigger produces {@link RunRequest}s from the outside world
 * (cron, webhooks, queue consumers, reactive stores, ...).
 *
 * `start` is scoped: the trigger runs until its scope is closed,
 * so shutting the app scope down stops all triggers gracefully.
 */
export interface Trigger {
  readonly tag: string
  readonly start: Effect.Effect<void, never, Scope | RuntimeBus>
}

export interface SubscriptionTriggerOptions {
  /** Human-readable tag, e.g. `pocketbase:notes`. */
  readonly tag: string
  /**
   * Workflow to run on each event. Pass the workflow definition object
   * (preferred — typo-proof and rename-safe) or a registered name.
   */
  readonly workflow: string | WorkflowDef
  /** Static run input when the event carries none (event payload wins). */
  readonly input?: unknown
  /**
   * Subscribe to the outside world. `fire` enqueues a run; call it from
   * callbacks (PocketBase `subscribe`, `EventSource.onmessage`, ...).
   * Return the release Effect — it runs as the scope finalizer, so
   * `unsubscribe`/`close` is guaranteed on shutdown.
   */
  readonly subscribe: (
    fire: (input?: unknown) => void,
  ) => Effect.Effect<Effect.Effect<void, never>, unknown>
}

const workflowNameOf = (workflow: string | WorkflowDef): string =>
  typeof workflow === "string" ? workflow : workflow.name

/**
 * Build a trigger around an external subscription with guaranteed
 * cleanup (the PocketBase-reactive shape).
 *
 * `subscribe` runs once in a forked scope; its returned release Effect
 * is the `acquireRelease` finalizer, so scope close (SIGINT/SIGTERM,
 * LIFO with workers) always unsubscribes before the run-queue drain.
 * Callback invocations are bridged via `Effect.runFork` with the bus
 * captured at `start`, so `fire` stays a plain sync function.
 *
 * @example
 * ```ts
 * makeSubscriptionTrigger({
 *   tag: "pocketbase:notes",
 *   workflow: notesWorkflow,
 *   subscribe: (fire) =>
 *     Effect.sync(() =>
 *       pb.collection("notes").subscribe("*", (e) => fire(e)),
 *     ).pipe(Effect.map((unsub) => Effect.sync(() => unsub()))),
 * })
 * ```
 */
export const makeSubscriptionTrigger = (options: SubscriptionTriggerOptions): Trigger => {
  const workflow = workflowNameOf(options.workflow)
  return {
    tag: options.tag,
    start: Effect.asVoid(
      Effect.forkScoped(
        Effect.gen(function* () {
          const bus = yield* RuntimeBus
          const fire = (input?: unknown): void => {
            Effect.runFork(
              submitRun({
                runId: newRunId(workflow),
                workflow,
                trigger: options.tag,
                input: input ?? options.input,
              }).pipe(
                Effect.provideService(RuntimeBus, bus),
                Effect.withSpan(`trigger.subscription.${workflow}`, {
                  attributes: { "workflow.name": workflow, trigger: options.tag },
                }),
              ),
            )
          }
          const release = yield* options.subscribe(fire)
          yield* Effect.addFinalizer(() => release)
          yield* Effect.never
        }),
      ),
    ),
  }
}
