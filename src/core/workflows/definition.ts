import { Data, Effect } from "effect"

/** Static input value, or derived from already-completed node outputs. */
export type NodeInput = unknown | ((outputs: ReadonlyMap<string, unknown>) => unknown)

export interface WorkflowNode {
  readonly id: string
  /** Name of a function in the {@link FunctionRegistry}. */
  readonly fn: string
  readonly input?: NodeInput
  readonly dependsOn?: ReadonlyArray<string>
  /**
   * Optional gate over already-completed node outputs. Evaluated after all
   * previous levels finish, before the node's level runs — so it only sees
   * earlier levels (same-level nodes run in parallel). `false` skips the
   * node, and skipping propagates transitively to its dependents.
   *
   * Like `input` mappers this is data plumbing, not work: keep it pure and
   * total (a throw fails the run).
   */
  readonly when?: (outputs: ReadonlyMap<string, unknown>) => boolean
}

export interface WorkflowDef {
  readonly name: string
  readonly nodes: ReadonlyArray<WorkflowNode>
}

export class WorkflowDefinitionError extends Data.TaggedError("WorkflowDefinitionError")<{
  readonly reason: string
  readonly workflow: string
}> {}

/** Topological execution levels. Nodes within a level have no inter-dependencies. */
export type ExecutionLevels = ReadonlyArray<ReadonlyArray<WorkflowNode>>

const resolveInput = (
  input: NodeInput | undefined,
  outputs: ReadonlyMap<string, unknown>,
): unknown =>
  typeof input === "function"
    ? (input as (o: ReadonlyMap<string, unknown>) => unknown)(outputs)
    : input

export const resolveNodeInput = resolveInput

/**
 * Validate a workflow DAG and compute topological execution levels.
 *
 * Fails with {@link WorkflowDefinitionError} on duplicate ids,
 * unknown dependencies, or cycles.
 */
export const planWorkflow = (
  def: WorkflowDef,
): Effect.Effect<ExecutionLevels, WorkflowDefinitionError> =>
  Effect.gen(function* () {
    const fail = (reason: string) => new WorkflowDefinitionError({ reason, workflow: def.name })

    if (def.nodes.length === 0) {
      return yield* Effect.fail(fail("workflow has no nodes"))
    }

    const ids = new Set<string>()
    for (const node of def.nodes) {
      if (ids.has(node.id)) {
        return yield* Effect.fail(fail(`duplicate node id: ${node.id}`))
      }
      ids.add(node.id)
    }

    for (const node of def.nodes) {
      for (const dep of node.dependsOn ?? []) {
        if (!ids.has(dep)) {
          return yield* Effect.fail(fail(`node ${node.id} depends on unknown node: ${dep}`))
        }
        if (dep === node.id) {
          return yield* Effect.fail(fail(`node ${node.id} depends on itself`))
        }
      }
    }

    // Kahn's algorithm, preserving definition order within levels.
    const indegree = new Map<string, number>()
    const dependents = new Map<string, Array<string>>()
    for (const node of def.nodes) {
      indegree.set(node.id, node.dependsOn?.length ?? 0)
      for (const dep of node.dependsOn ?? []) {
        const list = dependents.get(dep) ?? []
        list.push(node.id)
        dependents.set(dep, list)
      }
    }

    const byId = new Map(def.nodes.map((n) => [n.id, n]))
    const levels: Array<ReadonlyArray<WorkflowNode>> = []
    let ready = def.nodes.filter((n) => (indegree.get(n.id) ?? 0) === 0)
    let visited = 0

    while (ready.length > 0) {
      levels.push(ready)
      visited += ready.length
      const next: Array<WorkflowNode> = []
      for (const node of ready) {
        for (const dependent of dependents.get(node.id) ?? []) {
          const remaining = (indegree.get(dependent) ?? 1) - 1
          indegree.set(dependent, remaining)
          if (remaining === 0) {
            next.push(byId.get(dependent)!)
          }
        }
      }
      ready = next
    }

    if (visited !== def.nodes.length) {
      return yield* Effect.fail(fail("workflow contains a dependency cycle"))
    }

    return levels
  })
