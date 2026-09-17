import { Effect } from "effect"
import type { AppConfig } from "@/core/config"
import type { FunctionDef } from "@/core/functions/registry"
import type { Trigger } from "@/core/triggers/trigger"
import type { WorkflowDef, WorkflowNode } from "@/core/workflows/definition"

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

/**
 * All functions across bundles (for `FunctionRegistryLive`).
 *
 * The same `FunctionDef` reference shared across bundles is deduped.
 * A different implementation under an already-seen name is kept so
 * `FunctionRegistryLive` fails fast with `DuplicateFunctionError`
 * instead of silently shadowing one implementation.
 */
export const bundleFunctions = (
  bundles: ReadonlyArray<WorkflowBundle>,
): ReadonlyArray<FunctionDef<any, any, any>> => {
  const seen = new Map<string, FunctionDef<any, any, any>>()
  const out: Array<FunctionDef<any, any, any>> = []
  for (const bundle of bundles) {
    for (const fn of bundle.functions) {
      const existing = seen.get(fn.name)
      if (existing === undefined) {
        seen.set(fn.name, fn)
        out.push(fn)
      } else if (existing !== fn) {
        // Conflicting implementation — surface downstream as a duplicate.
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

/**
 * Define a workflow bundle with static checks.
 *
 * TypeScript rejects the bundle when a node references a function that is
 * not listed in `functions` — the `fn:` field is constrained to the literal
 * names captured by {@link makeFunction}:
 *
 * ```ts
 * // @ts-expect-error "ghost" is not in functions
 * makeBundle({
 *   workflow: { name: "x", nodes: [{ id: "a", fn: "ghost" }] },
 *   functions: [greetFunction],
 *   makeTriggers: () => Effect.succeed([]),
 * })
 * ```
 */
export const makeBundle = <const Fns extends ReadonlyArray<FunctionDef<any, any, any>>>(
  bundle: {
    readonly workflow: WorkflowDef & {
      readonly nodes: ReadonlyArray<WorkflowNode & { readonly fn: Fns[number]["name"] }>
    }
    readonly functions: Fns
    readonly makeTriggers: (
      config: AppConfig,
    ) => Effect.Effect<ReadonlyArray<Trigger>, unknown>
  },
): WorkflowBundle => bundle
