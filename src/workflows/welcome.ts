import { Effect } from "effect"
import { makeBundle } from "@/core/workflows/bundle"
import type { WorkflowDef } from "@/core/workflows/definition"
import { greetFunction } from "@/functions/greet"
import { makeCronTrigger } from "@/triggers/cron"
import { makeOnceTrigger } from "@/triggers/once"

export const welcomeWorkflow = {
  name: "welcome",
  nodes: [{ id: "greet", fn: "greet", input: { name: "world" } }],
} as const satisfies WorkflowDef

/**
 * The `welcome` workflow owns what it runs and what starts it:
 * functions from `src/functions/*`, triggers from `src/triggers/*`.
 *
 * Triggers act as an OR — the sequence below runs on every cron tick
 * and once at boot. Add new functions/triggers here; `src/index.ts`
 * boots every bundle generically, so no per-workflow wiring lives
 * in `main`.
 */
export const welcomeBundle = makeBundle({
  workflow: welcomeWorkflow,
  functions: [greetFunction],
  makeTriggers: (config) =>
    Effect.all([
      makeCronTrigger({ schedule: config.cronExpression, workflow: welcomeWorkflow }),
      Effect.succeed(makeOnceTrigger({ workflow: welcomeWorkflow })),
    ]),
})
