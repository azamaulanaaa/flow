import * as NodeRuntime from "@effect/platform-node/NodeRuntime"
import { Effect, Layer } from "effect"
import type { Scope } from "effect/Scope"
import { isMainThread, workerData } from "node:worker_threads"
import { AppConfigLive, AppConfigService } from "@/core/config"
import { FunctionRegistry, FunctionRegistryLive } from "@/core/functions/registry"
import { OtelLive } from "@/core/otel"
import { RuntimeBus, RuntimeBusLive } from "@/core/runtime/bus"
import { FUNCTION_WORKER_MODE, runFunctionWorkerEntry } from "@/core/runtime/function-worker"
import { startRuntime, waitForIdle, WorkflowCatalog, WorkflowCatalogLive } from "@/core/runtime/service"
import { WorkerPool, WorkerPoolLive } from "@/core/runtime/worker-pool"
import { makeCronTrigger } from "@/core/triggers/cron"
import { exampleFunctions } from "@/functions"
import { exampleWorkflows } from "@/workflows"

/** Bus capacity comes from env config so deploys can tune backpressure. */
const RuntimeBusFromConfigLive: Layer.Layer<RuntimeBus, never, AppConfigService> = Layer.unwrapEffect(
  Effect.map(AppConfigService, (config) => RuntimeBusLive(config.queueCapacity)),
)

const WorkerPoolFromConfigLive: Layer.Layer<WorkerPool, never, AppConfigService> = Layer.unwrapEffect(
  Effect.map(
    AppConfigService,
    (config) =>
      WorkerPoolLive({
        enabled: config.workerPoolEnabled,
        size: config.workerPoolSize,
        entryUrl: new URL(import.meta.url),
        timeoutMs: config.workerPoolTimeoutMs,
      }),
  ),
)

const MainLive = Layer.mergeAll(
  AppConfigLive,
  FunctionRegistryLive([...exampleFunctions]),
  WorkflowCatalogLive([...exampleWorkflows]),
  RuntimeBusFromConfigLive.pipe(Layer.provide(AppConfigLive)),
  OtelLive.pipe(Layer.provide(AppConfigLive)),
  WorkerPoolFromConfigLive.pipe(Layer.provide(AppConfigLive)),
)

const program: Effect.Effect<
  void,
  unknown,
  AppConfigService | FunctionRegistry | RuntimeBus | WorkflowCatalog | Scope
> = Effect.gen(function* () {
  const config = yield* AppConfigService

  yield* startRuntime({ workers: 4 })

  const trigger = yield* makeCronTrigger({
    schedule: config.cronExpression,
    workflow: "welcome",
  })
  yield* trigger.start

  yield* Effect.log(
    `Service ${config.serviceName} started: cron (${config.cronExpression}) -> welcome workflow`,
  )
  // Graceful exit: NodeRuntime interrupts Effect.never on SIGINT/SIGTERM.
  // forkScoped children (trigger, workers) stop in LIFO order, then this
  // finalizer drains queued + in-flight runs (bounded by shutdownTimeoutMs)
  // before the scope releases layers (OTel flush).
  yield* Effect.addFinalizer((exit) =>
    Effect.gen(function* () {
      yield* Effect.log(`Shutdown requested (${exit._tag}), draining runs...`)
      yield* waitForIdle.pipe(
        Effect.timeout(`${config.shutdownTimeoutMs} millis`),
        Effect.ignore,
      )
      yield* Effect.log("Shutdown complete, flushing telemetry")
    }),
  )
  // Run until the process receives SIGINT/SIGTERM (handled by NodeRuntime).
  yield* Effect.never
})

const main = program.pipe(
  Effect.scoped,
  Effect.provide(MainLive),
  Effect.catchAllCause((cause) => Effect.logError(cause)),
)

// Worker threads boot this same entrypoint in dispatcher mode (same bundle,
// no extra build artifact). Main thread runs the service.
if (!isMainThread && (workerData as { readonly mode?: unknown } | undefined)?.mode === FUNCTION_WORKER_MODE) {
  await runFunctionWorkerEntry()
} else {
  main.pipe(NodeRuntime.runMain)
}
