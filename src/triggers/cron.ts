import { Cron, Effect, Schedule } from "effect"
import { submitRun } from "../runtime/bus.js"
import type { Trigger } from "./trigger.js"

export interface CronTriggerOptions {
  /** Cron expression, e.g. `* * * * *` or with seconds `* * * * * *`. */
  readonly schedule: string
  /** Workflow to run on each tick. */
  readonly workflow: string
  readonly input?: unknown
  readonly runIdPrefix?: string
}

/**
 * Build a cron trigger. Parsing happens at construction (fallible);
 * the returned trigger's `start` is infallible and runs forever in a scope.
 */
export const makeCronTrigger = (
  options: CronTriggerOptions,
): Effect.Effect<Trigger, Cron.ParseError> =>
  Effect.gen(function* () {
    const parsed = Cron.parse(options.schedule)
    if (parsed._tag === "Left") {
      return yield* Effect.fail(parsed.left)
    }
    const cron = parsed.right
    let tick = 0

    const fire = Effect.gen(function* () {
      tick += 1
      const runId = `${options.runIdPrefix ?? options.workflow}-${Date.now()}-${tick}`
      yield* submitRun({
        runId,
        workflow: options.workflow,
        trigger: `cron:${options.schedule}`,
        input: options.input,
      })
      yield* Effect.log(`Cron trigger fired run ${runId}`)
    }).pipe(
      Effect.withSpan(`trigger.cron.${options.workflow}`, {
        attributes: { "workflow.name": options.workflow, "trigger": "cron" },
      }),
    )

    return {
      tag: `cron:${options.schedule}`,
      start: Effect.asVoid(Effect.forkScoped(Effect.repeat(fire, Schedule.cron(cron)))),
    } satisfies Trigger
  })

/** Fire a single run immediately (useful for boot hooks, tests, manual runs). */
export const makeOnceTrigger = (
  options: { readonly workflow: string; readonly input?: unknown; readonly runId?: string },
): Trigger => ({
  tag: "once",
  start: Effect.asVoid(
    Effect.forkScoped(
      submitRun({
        runId: options.runId ?? `${options.workflow}-${Date.now()}`,
        workflow: options.workflow,
        trigger: "once",
        input: options.input,
      }).pipe(Effect.withSpan(`trigger.once.${options.workflow}`)),
    ),
  ),
})
