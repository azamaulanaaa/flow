import { NodeSdk } from "@effect/opentelemetry"
import { BatchSpanProcessor, ConsoleSpanExporter } from "@opentelemetry/sdk-trace-base"
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http"
import { Effect, Layer } from "effect"
import { AppConfigService } from "./config.js"

/**
 * OpenTelemetry SDK layer driven by {@link AppConfigService}.
 *
 * Uses OTLP HTTP exporter when `OTEL_EXPORTER_OTLP_ENDPOINT` is set,
 * otherwise falls back to console exporter for local development.
 */
export const OtelLive: Layer.Layer<never, never, AppConfigService> = Layer.unwrapEffect(
  Effect.gen(function* () {
    const config = yield* AppConfigService
    const spanProcessor = config.otlpEndpoint
      ? new BatchSpanProcessor(new OTLPTraceExporter({ url: config.otlpEndpoint }))
      : new BatchSpanProcessor(new ConsoleSpanExporter())
    return NodeSdk.layer(() => ({
      resource: { serviceName: config.serviceName },
      spanProcessor,
    }))
  }),
)

/** Local-only OTel layer (console exporter), no config required. Useful for tests. */
export const OtelConsoleLive: Layer.Layer<never> = NodeSdk.layer(() => ({
  resource: { serviceName: "workflow-runner-test" },
  spanProcessor: new BatchSpanProcessor(new ConsoleSpanExporter()),
}))
