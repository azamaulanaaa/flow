import { Effect } from "effect"
import { makeBundle } from "@/core/workflows/bundle"
import type { WorkflowDef } from "@/core/workflows/definition"
import { countFunction } from "@/functions/count"
import { greetFunction } from "@/functions/greet"
import { reportFunction } from "@/functions/report"
import { upperFunction } from "@/functions/upper"
import { makeCronTrigger } from "@/triggers/cron"
import { makeOnceTrigger } from "@/triggers/once"

export const welcomeWorkflow = {
  name: "welcome",
  nodes: [
    { id: "greet", fn: "greet", input: { name: "world" } },
    {
      id: "upper",
      fn: "upper",
      dependsOn: ["greet"],
      input: (outputs: ReadonlyMap<string, unknown>) => ({ text: outputs.get("greet") }),
    },
    {
      id: "count",
      fn: "count",
      dependsOn: ["greet"],
      input: (outputs: ReadonlyMap<string, unknown>) => ({ text: outputs.get("greet") }),
    },
    {
      id: "report",
      fn: "report",
      dependsOn: ["upper", "count"],
      input: (outputs: ReadonlyMap<string, unknown>) => ({ upper: outputs.get("upper"), count: outputs.get("count") }),
    },
  ],
} as const satisfies WorkflowDef

/**
 * The `welcome` workflow owns what it runs and what starts it:
 * functions from `src/functions/*`, triggers from `src/triggers/*`.
 *
 * Every step is a tracked registry function — workflows declare DAG
 * wiring and input plumbing only, never inline work, so nothing escapes
 * tracing. Branches diverge (`greet` → `upper` + `count` in parallel)
 * and converge (`report` joins both) via `dependsOn`.
 *
 * Triggers act as an OR — the sequence below runs on every cron tick
 * and once at boot. Add new functions/triggers here; `src/index.ts`
 * boots every bundle generically, so no per-workflow wiring lives
 * in `main`.
 */
export const welcomeBundle = makeBundle({
  workflow: welcomeWorkflow,
  functions: [greetFunction, upperFunction, countFunction, reportFunction],
  makeTriggers: (config) =>
    Effect.all([
      makeCronTrigger({ schedule: config.cronExpression, workflow: welcomeWorkflow }),
      Effect.succeed(makeOnceTrigger({ workflow: welcomeWorkflow })),
    ]),
})
