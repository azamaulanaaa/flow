import { NodeRuntime } from "@effect/platform-node"
import { Effect, Layer } from "effect"
import type { Scope } from "effect/Scope"
import { AppConfigLive, AppConfigService } from "@/config"
import { exampleFunctions } from "@/functions/examples"
import { FunctionRegistry, FunctionRegistryLive } from "@/functions/registry"
import { OtelLive } from "@/otel"
import { RuntimeBus, RuntimeBusLive } from "@/runtime/bus"
import { startRuntime, WorkflowCatalog, WorkflowCatalogLive } from "@/runtime/service"
import { makeCronTrigger } from "@/triggers/cron"
import { exampleWorkflows } from "@/workflows/examples"

/** Bus capacity comes from env config so deploys can tune backpressure. */
const RuntimeBusFromConfigLive: Layer.Layer<RuntimeBus, never, AppConfigService> = Layer.unwrapEffect(
  Effect.map(AppConfigService, (config) => RuntimeBusLive(config.queueCapacity)),
)

const MainLive = Layer.mergeAll(
  AppConfigLive,
  FunctionRegistryLive([...exampleFunctions]),
  WorkflowCatalogLive([...exampleWorkflows]),
  RuntimeBusFromConfigLive.pipe(Layer.provide(AppConfigLive)),
  OtelLive.pipe(Layer.provide(AppConfigLive)),
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
  // Run until the process receives SIGINT/SIGTERM (handled by NodeRuntime).
  yield* Effect.never
})

const main = program.pipe(
  Effect.scoped,
  Effect.provide(MainLive),
  Effect.catchAllCause((cause) => Effect.logError(cause)),
)

main.pipe(NodeRuntime.runMain)
