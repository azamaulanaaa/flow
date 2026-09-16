import { Effect } from "effect"
import { FunctionRegistry, runFunction } from "@/core/functions/registry"
import { WorkerPool } from "@/core/runtime/worker-pool"
import { planWorkflow, resolveNodeInput, type WorkflowDef, type WorkflowNode } from "@/core/workflows/definition"

export type WorkflowOutputs = ReadonlyMap<string, unknown>

/**
 * Run a function through the worker-thread pool when one is provided,
 * falling back to in-process execution on any pool failure (disabled pool,
 * dead workers, timeouts, unserializable payloads).
 *
 * NB: fallback on timeout is at-least-once — the worker may still finish the
 * task after we gave up waiting. Keep functions idempotent when the pool is on.
 */
const runFunctionPooled = (
  fn: string,
  input: unknown,
): Effect.Effect<unknown, unknown, FunctionRegistry> =>
  Effect.gen(function* () {
    const poolOpt = yield* Effect.serviceOption(WorkerPool)
    if (poolOpt._tag === "None") {
      return yield* runFunction<unknown, unknown>(fn, input)
    }
    return yield* poolOpt.value.execute(fn, input).pipe(
      Effect.orElse(() => runFunction<unknown, unknown>(fn, input)),
    )
  })

const runNode = (
  workflowName: string,
  node: WorkflowNode,
  outputs: Map<string, unknown>,
): Effect.Effect<[string, unknown], unknown, FunctionRegistry> =>
  Effect.gen(function* () {
    yield* Effect.annotateCurrentSpan("workflow.name", workflowName)
    yield* Effect.annotateCurrentSpan("workflow.node", node.id)
    yield* Effect.annotateCurrentSpan("function.name", node.fn)
    const input = resolveNodeInput(node.input, outputs)
    const output = yield* runFunctionPooled(node.fn, input)
    return [node.id, output] as [string, unknown]
  }).pipe(Effect.withSpan(`workflow.${workflowName}.node.${node.id}`))

/**
 * Execute a workflow DAG.
 *
 * Levels run sequentially; nodes within a level run concurrently.
 * Returns node outputs keyed by node id. The whole run is wrapped in a
 * `workflow.<name>` span, so OTel shows trigger → workflow → function nesting.
 */
export const runWorkflow = (
  def: WorkflowDef,
): Effect.Effect<WorkflowOutputs, unknown, FunctionRegistry> =>
  Effect.gen(function* () {
    const levels = yield* planWorkflow(def)
    const outputs = new Map<string, unknown>()
    for (const level of levels) {
      const results = yield* Effect.all(
        level.map((node) => runNode(def.name, node, outputs)),
        { concurrency: "unbounded" },
      )
      for (const [id, output] of results) {
        outputs.set(id, output)
      }
    }
    return outputs as WorkflowOutputs
  }).pipe(Effect.withSpan(`workflow.${def.name}`, { attributes: { "workflow.name": def.name } }))
