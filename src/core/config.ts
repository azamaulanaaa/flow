import { Config, Context, Effect, Layer } from "effect"
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

const VALID_LOG_LEVELS = new Set([
  "trace",
  "debug",
  "info",
  "warn",
  "warning",
  "error",
  "fatal",
  "none",
  "all",
])

/** Pure validation so bad env fails fast at boot with one readable error. */
export const configValidationErrors = (config: AppConfig): ReadonlyArray<string> => {
  const errors: Array<string> = []
  if (config.serviceName.trim().length === 0) {
    errors.push("SERVICE_NAME must be non-empty")
  }
  if (!VALID_LOG_LEVELS.has(config.logLevel.trim().toLowerCase())) {
    errors.push(
      "LOG_LEVEL must be one of TRACE, DEBUG, INFO, WARN, ERROR, FATAL, NONE, ALL (got " +
        JSON.stringify(config.logLevel) +
        ")",
    )
  }
  if (!Number.isInteger(config.queueCapacity) || config.queueCapacity < 1) {
    errors.push(`QUEUE_CAPACITY must be an integer >= 1 (got ${config.queueCapacity})`)
  }
  if (!Number.isInteger(config.runtimeWorkers) || config.runtimeWorkers < 1) {
    errors.push(`RUNTIME_WORKERS must be an integer >= 1 (got ${config.runtimeWorkers})`)
  }
  if (!Number.isInteger(config.workflowConcurrency) || config.workflowConcurrency < 1) {
    errors.push(`WORKFLOW_CONCURRENCY must be an integer >= 1 (got ${config.workflowConcurrency})`)
  }
  if (!Number.isInteger(config.workflowNodeTimeoutMs) || config.workflowNodeTimeoutMs < 0) {
    errors.push(
      `WORKFLOW_NODE_TIMEOUT_MS must be an integer >= 0 (got ${config.workflowNodeTimeoutMs})`,
    )
  }
  if (!Number.isInteger(config.workflowRetryAttempts) || config.workflowRetryAttempts < 0) {
    errors.push(
      `WORKFLOW_RETRY_ATTEMPTS must be an integer >= 0 (got ${config.workflowRetryAttempts})`,
    )
  }
  if (config.cronExpression.trim().length === 0) {
    errors.push("CRON_EXPRESSION must be non-empty")
  }
  if (!Number.isInteger(config.shutdownTimeoutMs) || config.shutdownTimeoutMs < 1) {
    errors.push(`SHUTDOWN_TIMEOUT_MS must be an integer >= 1 (got ${config.shutdownTimeoutMs})`)
  }
  if (!Number.isInteger(config.workerPoolSize) || config.workerPoolSize < 1) {
    errors.push(`WORKER_POOL_SIZE must be an integer >= 1 (got ${config.workerPoolSize})`)
  }
  if (!Number.isInteger(config.workerPoolTimeoutMs) || config.workerPoolTimeoutMs < 1) {
    errors.push(
      `WORKER_POOL_TIMEOUT_MS must be an integer >= 1 (got ${config.workerPoolTimeoutMs})`,
    )
  }
  return errors
}

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

export const AppConfigLive: Layer.Layer<AppConfigService, unknown> = Layer.effect(
  AppConfigService,
  Effect.gen(function* () {
    const config = (yield* AppConfigFromEnv) as AppConfig
    const errors = configValidationErrors(config)
    if (errors.length > 0) {
      return yield* Effect.fail(new Error(`Invalid configuration:\n- ${errors.join("\n- ")}`))
    }
    return config
  }) as Effect.Effect<AppConfig, unknown>,
)
