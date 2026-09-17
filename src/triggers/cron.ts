import { Cron, Effect, Schedule } from "effect"
import { submitRun } from "@/core/runtime/bus"
import { newRunId } from "@/core/runtime/run-id"
import type { Trigger } from "@/core/triggers/trigger"
import type { WorkflowDef } from "@/core/workflows/definition"

export interface CronTriggerOptions {
  /** Cron expression, e.g. `* * * * *` or with seconds `* * * * * *`. */
  readonly schedule: string
  /**
   * Workflow to run on each tick. Pass the workflow definition object
   * (preferred — typo-proof and rename-safe) or a registered name.
   */
  readonly workflow: string | WorkflowDef
  readonly input?: unknown
  readonly runIdPrefix?: string
}

const workflowNameOf = (workflow: string | WorkflowDef): string =>
  typeof workflow === "string" ? workflow : workflow.name

/**
 * Built-in cron trigger: fires a run on a schedule.
 *
 * This file is both library and example — a new trigger kind is just a
 * module that follows the {@link Trigger} signature (`tag` + scoped
 * `start`): build it with `Effect.forkScoped` and `submitRun`.
 *
 * @example Wire it into a workflow bundle (OR with other triggers):
 * ```ts
 * makeTriggers: (config) =>
 *   Effect.all([
 *     makeCronTrigger({ schedule: config.cronExpression, workflow: myWorkflow }),
 *     Effect.succeed(makeOnceTrigger({ workflow: myWorkflow })),
 *   ])
 * ```
 *
 * Parsing happens at construction (fallible — the `Cron.ParseError`
 * channel forces callers to handle bad expressions); the returned
 * trigger's `start` is infallible and runs forever in a scope.
 */
export const makeCronTrigger = (
  options: CronTriggerOptions,
): Effect.Effect<Trigger, Cron.ParseError> =>
  Effect.gen(function* () {
    const workflow = workflowNameOf(options.workflow)
    const parsed = Cron.parse(options.schedule)
    if (parsed._tag === "Left") {
      return yield* Effect.fail(parsed.left)
    }
    const cron = parsed.right

    const fire = Effect.gen(function* () {
      const runId = newRunId(options.runIdPrefix ?? workflow)
      yield* submitRun({
        runId,
        workflow,
        trigger: `cron:${options.schedule}`,
        input: options.input,
      })
      yield* Effect.log(`Cron trigger fired run ${runId}`)
    }).pipe(
      Effect.withSpan(`trigger.cron.${workflow}`, {
        attributes: { "workflow.name": workflow, trigger: "cron" },
      }),
    )

    return {
      tag: `cron:${options.schedule}`,
      start: Effect.asVoid(Effect.forkScoped(Effect.repeat(fire, Schedule.cron(cron)))),
    } satisfies Trigger
  })
