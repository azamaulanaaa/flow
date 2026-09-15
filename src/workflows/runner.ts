import { Effect } from "effect"
import { FunctionRegistry, runFunction } from "../functions/registry.js"
import { planWorkflow, resolveNodeInput, type WorkflowDef, type WorkflowNode } from "./definition.js"

export type WorkflowOutputs = ReadonlyMap<string, unknown>

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
    const output = yield* runFunction<unknown, unknown>(node.fn, input)
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
