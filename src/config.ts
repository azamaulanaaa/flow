import { Config, Context, Layer } from "effect"

export interface AppConfig {
  readonly serviceName: string
  readonly logLevel: string
  readonly queueCapacity: number
  readonly cronExpression: string
  readonly otlpEndpoint: string | undefined
}

export class AppConfigService extends Context.Tag("AppConfigService")<
  AppConfigService,
  AppConfig
>() {}

const AppConfigFromEnv = Config.all({
  serviceName: Config.string("SERVICE_NAME").pipe(Config.withDefault("workflow-runner")),
  logLevel: Config.string("LOG_LEVEL").pipe(Config.withDefault("INFO")),
  queueCapacity: Config.integer("QUEUE_CAPACITY").pipe(Config.withDefault(128)),
  cronExpression: Config.string("CRON_EXPRESSION").pipe(Config.withDefault("*/1 * * * *")),
  otlpEndpoint: Config.option(Config.string("OTEL_EXPORTER_OTLP_ENDPOINT")).pipe(
    Config.map((o) => (o._tag === "Some" ? o.value : undefined)),
  ),
})

export const AppConfigLive: Layer.Layer<AppConfigService> = Layer.effect(
  AppConfigService,
  AppConfigFromEnv as unknown as import("effect").Effect.Effect<AppConfig>,
)
