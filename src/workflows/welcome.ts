import { Effect } from "effect"
import type { WorkflowDef } from "@/core/workflows/definition"
import type { WorkflowBundle } from "@/core/workflows/bundle"
import { greetFunction } from "@/functions/greet"
import { makeWelcomeCronTrigger } from "@/triggers/welcome-cron"

export const welcomeWorkflow: WorkflowDef = {
  name: "welcome",
  nodes: [{ id: "greet", fn: "greet", input: { name: "world" } }],
}

/**
 * The `welcome` workflow owns what it runs and what starts it:
 * functions from `src/functions/*`, triggers from `src/triggers/*`.
 * Add new functions/triggers here — `src/index.ts` boots every bundle
 * generically, so no per-workflow wiring lives in `main`.
 */
export const welcomeBundle: WorkflowBundle = {
  workflow: welcomeWorkflow,
  functions: [greetFunction],
  makeTriggers: (config) =>
    Effect.map(makeWelcomeCronTrigger(config), (trigger) => [trigger]),
}
