import { Effect, Layer } from "effect"
import { AppConfigLive } from "@/core/config"
import {
  FunctionRegistry,
  FunctionRegistryLive,
  UnknownFunctionError,
} from "@/core/functions/registry"
import { WorkflowCatalog, WorkflowCatalogLive } from "@/core/runtime/service"
import { makeBundleTriggers } from "@/core/workflows/bundle"
import { planWorkflow } from "@/core/workflows/definition"
import { exampleFunctions, exampleWorkflows, workflowBundles } from "@/workflows"

const ValidateLive = Layer.mergeAll(
  AppConfigLive,
  FunctionRegistryLive([...exampleFunctions]),
  WorkflowCatalogLive([...exampleWorkflows]),
)

const program = Effect.gen(function* () {
  const catalog = yield* WorkflowCatalog
  const registry = yield* FunctionRegistry
  let triggerCount = 0
  for (const bundle of workflowBundles) {
    // DAG shape: duplicates, unknown deps, self-deps, cycles.
    yield* planWorkflow(bundle.workflow)
    // Every `fn:` resolves ( Bundle types check this statically;
    // this is the runtime backstop for string-built defs).
    for (const node of bundle.workflow.nodes) {
      if (!registry.has(node.fn)) {
        return yield* Effect.fail(new UnknownFunctionError({ name: node.fn }))
      }
    }
    // Trigger construction without starting: parses cron expressions.
    const triggers = yield* makeBundleTriggers([bundle])
    triggerCount += triggers.length
  }
  yield* Effect.log(
    `Validation OK: ${catalog.size} workflow(s), ${registry.size} function(s), ${triggerCount} trigger(s)`,
  )
})

const main = async (): Promise<void> => {
  try {
    await Effect.runPromise(program.pipe(Effect.scoped, Effect.provide(ValidateLive)))
  } catch (error) {
    console.error(
      `Validation failed: ${error instanceof Error ? (error.stack ?? error.message) : String(error)}`,
    )
    process.exitCode = 1
  }
}

await main()
