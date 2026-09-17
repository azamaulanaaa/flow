import { Effect } from "effect"
import { submitRun } from "@/core/runtime/bus"
import { newRunId } from "@/core/runtime/run-id"
import type { Trigger } from "@/core/triggers/trigger"
import type { WorkflowDef } from "@/core/workflows/definition"

export interface OnceTriggerOptions {
  /**
   * Workflow to run once. Pass the workflow definition object
   * (preferred — typo-proof and rename-safe) or a registered name.
   */
  readonly workflow: string | WorkflowDef
  readonly input?: unknown
  readonly runId?: string
}

const workflowNameOf = (workflow: string | WorkflowDef): string =>
  typeof workflow === "string" ? workflow : workflow.name

/**
 * Built-in once trigger: fires a single run immediately when started
 * (useful for boot hooks, tests, manual runs).
 *
 * Like every trigger, this follows the {@link Trigger} signature (`tag` +
 * scoped `start`). Combine it with other triggers in a workflow bundle's
 * `makeTriggers` — triggers act as an OR: the function sequence runs
 * whichever way fires first.
 *
 * @example
 * ```ts
 * Effect.succeed(makeOnceTrigger({ workflow: myWorkflow }))
 * ```
 */
export const makeOnceTrigger = (options: OnceTriggerOptions): Trigger => {
  const workflow = workflowNameOf(options.workflow)
  return {
    tag: "once",
    start: Effect.asVoid(
      Effect.forkScoped(
        submitRun({
          runId: options.runId ?? newRunId(workflow),
          workflow,
          trigger: "once",
          input: options.input,
        }).pipe(Effect.withSpan(`trigger.once.${workflow}`)),
      ),
    ),
  }
}
