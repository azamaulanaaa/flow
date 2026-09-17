export * from "./welcome"
import { bundleFunctions, bundleWorkflows } from "@/core/workflows/bundle"
import type { WorkflowBundle } from "@/core/workflows/bundle"
import { welcomeBundle } from "./welcome"

/**
 * All workflow bundles registered with the runtime. Add yours here.
 *
 * Each bundle owns its workflow definition plus the functions it calls
 * and the triggers that start it — the composition root (`src/index.ts`)
 * just boots this list, with no per-workflow stitching in `main`.
 */
export const workflowBundles: ReadonlyArray<WorkflowBundle> = [welcomeBundle]

/** Derived registries — kept so `main` and the worker pool share one source. */
export const exampleWorkflows = bundleWorkflows(workflowBundles)
export const exampleFunctions = bundleFunctions(workflowBundles)
