import { Config, Context, Layer } from "effect"
import { availableParallelism } from "node:os"

const cpuCount = (): number => {
  try {
    return Math.max(1, availableParallelism())
  } catch {
    return 4
  }
}

export interface AppConfig {
  readonly serviceName: string
  readonly logLevel: string
  readonly queueCapacity: number
  readonly runtimeWorkers: number
  readonly workflowConcurrency: number
  readonly workflowNodeTimeoutMs: number
  readonly workflowRetryAttempts: number
  readonly cronExpression: string
  readonly otlpEndpoint: string | undefined
  readonly shutdownTimeoutMs: number
  readonly workerPoolEnabled: boolean
  readonly workerPoolSize: number
  readonly workerPoolTimeoutMs: number
}

export class AppConfigService extends Context.Tag("AppConfigService")<
  AppConfigService,
  AppConfig
>() {}

const AppConfigFromEnv = Config.all({
  serviceName: Config.string("SERVICE_NAME").pipe(Config.withDefault("workflow-runner")),
  logLevel: Config.string("LOG_LEVEL").pipe(Config.withDefault("INFO")),
  queueCapacity: Config.integer("QUEUE_CAPACITY").pipe(Config.withDefault(128)),
  runtimeWorkers: Config.integer("RUNTIME_WORKERS").pipe(Config.withDefault(4)),
  workflowConcurrency: Config.integer("WORKFLOW_CONCURRENCY").pipe(Config.withDefault(32)),
  workflowNodeTimeoutMs: Config.integer("WORKFLOW_NODE_TIMEOUT_MS").pipe(Config.withDefault(0)),
  workflowRetryAttempts: Config.integer("WORKFLOW_RETRY_ATTEMPTS").pipe(Config.withDefault(0)),
  cronExpression: Config.string("CRON_EXPRESSION").pipe(Config.withDefault("*/1 * * * *")),
  shutdownTimeoutMs: Config.integer("SHUTDOWN_TIMEOUT_MS").pipe(Config.withDefault(10_000)),
  workerPoolEnabled: Config.boolean("WORKER_POOL_ENABLED").pipe(Config.withDefault(false)),
  workerPoolSize: Config.integer("WORKER_POOL_SIZE").pipe(Config.withDefault(cpuCount())),
  workerPoolTimeoutMs: Config.integer("WORKER_POOL_TIMEOUT_MS").pipe(Config.withDefault(30_000)),
  otlpEndpoint: Config.option(Config.string("OTEL_EXPORTER_OTLP_ENDPOINT")).pipe(
    Config.map((o) => (o._tag === "Some" ? o.value : undefined)),
  ),
})

export const AppConfigLive: Layer.Layer<AppConfigService> = Layer.effect(
  AppConfigService,
  AppConfigFromEnv as unknown as import("effect").Effect.Effect<AppConfig>,
)
