import { Effect } from "effect"
import type { AppConfig } from "@/core/config"
import type { FunctionDef } from "@/core/functions/registry"
import type { Trigger } from "@/core/triggers/trigger"
import type { WorkflowDef } from "@/core/workflows/definition"

/**
 * App-level bundle: a workflow owns the functions it calls and the
 * triggers that start it.
 *
 * Framework code (`src/core/*`) only knows this interface. Concrete
 * bundles live in `src/workflows/*` and import their functions from
 * `src/functions/*` plus their trigger factories from `src/triggers/*`.
 * The composition root (`src/index.ts`) just boots every bundle —
 * no per-workflow stitching in `main`.
 */
export interface WorkflowBundle {
  readonly workflow: WorkflowDef
  /** Functions this workflow may call (registered by name). */
  readonly functions: ReadonlyArray<FunctionDef<any, any, any>>
  /** Build this workflow's triggers from env config (usually cron). */
  readonly makeTriggers: (
    config: AppConfig,
  ) => Effect.Effect<ReadonlyArray<Trigger>, unknown>
}

/** All workflow definitions across bundles (for `WorkflowCatalogLive`). */
export const bundleWorkflows = (
  bundles: ReadonlyArray<WorkflowBundle>,
): ReadonlyArray<WorkflowDef> => bundles.map((b) => b.workflow)

/** All functions across bundles, deduped by name (for `FunctionRegistryLive`). */
export const bundleFunctions = (
  bundles: ReadonlyArray<WorkflowBundle>,
): ReadonlyArray<FunctionDef<any, any, any>> => {
  const seen = new Set<string>()
  const out: Array<FunctionDef<any, any, any>> = []
  for (const bundle of bundles) {
    for (const fn of bundle.functions) {
      if (!seen.has(fn.name)) {
        seen.add(fn.name)
        out.push(fn)
      }
    }
  }
  return out
}

/** Build every bundle's triggers (flattened, definition order preserved). */
export const makeBundleTriggers = (
  bundles: ReadonlyArray<WorkflowBundle>,
  config: AppConfig,
): Effect.Effect<ReadonlyArray<Trigger>, unknown> =>
  Effect.map(
    Effect.all(bundles.map((b) => b.makeTriggers(config))),
    (lists) => lists.flat(),
  )
