import { Data, Duration, Effect, Schedule } from "effect"
import { FunctionRegistry, runFunction } from "@/core/functions/registry"
import { WorkerPool } from "@/core/runtime/worker-pool"
import {
  planWorkflow,
  resolveNodeInput,
  type WorkflowDef,
  type WorkflowNode,
} from "@/core/workflows/definition"

export type WorkflowOutputs = ReadonlyMap<string, unknown>

/** Default cap for parallel nodes within a DAG level (avoids unbounded fan-out). */
export const DEFAULT_LEVEL_CONCURRENCY = 32

export class WorkflowNodeTimeoutError extends Data.TaggedError("WorkflowNodeTimeoutError")<{
  readonly workflow: string
  readonly node: string
  readonly timeoutMs: number
}> {}

export interface RunWorkflowOptions {
  /** Max parallel nodes per level. Defaults to {@link DEFAULT_LEVEL_CONCURRENCY}. */
  readonly concurrency?: number
  /**
   * Fallback input for nodes without explicit `input`.
   * Wired from `RunRequest.input` so triggers can supply run-scoped data.
   */
  readonly defaultInput?: unknown
  /** Per-node timeout in ms. `0` or unset = disabled. */
  readonly nodeTimeoutMs?: number
  /** Retry attempts per node (in addition to the first try). Defaults to 0. */
  readonly retryAttempts?: number
}

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
    return yield* poolOpt.value
      .execute(fn, input)
      .pipe(Effect.orElse(() => runFunction<unknown, unknown>(fn, input)))
  })

const runNode = (
  workflowName: string,
  node: WorkflowNode,
  outputs: Map<string, unknown>,
  options: RunWorkflowOptions = {},
): Effect.Effect<[string, unknown], unknown, FunctionRegistry> =>
  Effect.gen(function* () {
    yield* Effect.annotateCurrentSpan("workflow.name", workflowName)
    yield* Effect.annotateCurrentSpan("workflow.node", node.id)
    yield* Effect.annotateCurrentSpan("function.name", node.fn)
    const input =
      node.input === undefined ? options.defaultInput : resolveNodeInput(node.input, outputs)
    const attempts = Math.max(0, Math.floor(options.retryAttempts ?? 0))
    const timeoutMs = Math.floor(options.nodeTimeoutMs ?? 0)
    let task = runFunctionPooled(node.fn, input)
    if (timeoutMs > 0) {
      task = task.pipe(
        Effect.timeoutFail({
          duration: Duration.millis(timeoutMs),
          onTimeout: () =>
            new WorkflowNodeTimeoutError({ workflow: workflowName, node: node.id, timeoutMs }),
        }),
      )
    }
    if (attempts > 0) {
      task = task.pipe(Effect.retry(Schedule.recurs(attempts)))
    }
    const output = yield* task
    return [node.id, output] as [string, unknown]
  }).pipe(Effect.withSpan(`workflow.${workflowName}.node.${node.id}`))

/**
 * Execute a workflow DAG.
 *
 * Levels run sequentially; nodes within a level run concurrently (bounded).
 * Nodes without explicit `input` receive `options.defaultInput`
 * (populated from `RunRequest.input` by the runtime service).
 * `nodeTimeoutMs` bounds each node; `retryAttempts` retries failures/timeouts.
 * Returns node outputs keyed by node id. The whole run is wrapped in a
 * `workflow.<name>` span, so OTel shows trigger → workflow → function nesting.
 */
export const runWorkflow = (
  def: WorkflowDef,
  options: RunWorkflowOptions = {},
): Effect.Effect<WorkflowOutputs, unknown, FunctionRegistry> =>
  Effect.gen(function* () {
    const levels = yield* planWorkflow(def)
    const concurrency = Math.max(1, Math.floor(options.concurrency ?? DEFAULT_LEVEL_CONCURRENCY))
    const outputs = new Map<string, unknown>()
    for (const level of levels) {
      const results = yield* Effect.all(
        level.map((node) => runNode(def.name, node, outputs, options)),
        { concurrency },
      )
      for (const [id, output] of results) {
        outputs.set(id, output)
      }
    }
    return outputs as WorkflowOutputs
  }).pipe(Effect.withSpan(`workflow.${def.name}`, { attributes: { "workflow.name": def.name } }))
