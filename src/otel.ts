import { NodeSdk } from "@effect/opentelemetry"
import { OTLPLogExporter } from "@opentelemetry/exporter-logs-otlp-http"
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http"
import { BatchLogRecordProcessor, ConsoleLogRecordExporter } from "@opentelemetry/sdk-logs"
import { BatchSpanProcessor, ConsoleSpanExporter } from "@opentelemetry/sdk-trace-base"
import { Effect, Layer } from "effect"
import { AppConfigService } from "./config.js"

/**
 * OpenTelemetry SDK layer driven by {@link AppConfigService}.
 *
 * Uses OTLP HTTP exporters when `OTEL_EXPORTER_OTLP_ENDPOINT` is set,
 * otherwise falls back to console exporters for local development.
 *
 * Wires two signals:
 * - traces via `spanProcessor` (`Effect.withSpan` creates the spans)
 * - logs via `logRecordProcessor` (`Effect.log` is exported as log records;
 *   logs inside a span are additionally attached as span events by Effect)
 */
export const OtelLive: Layer.Layer<never, never, AppConfigService> = Layer.unwrapEffect(
  Effect.gen(function* () {
    const config = yield* AppConfigService
    const spanProcessor = config.otlpEndpoint
      ? new BatchSpanProcessor(new OTLPTraceExporter({ url: config.otlpEndpoint }))
      : new BatchSpanProcessor(new ConsoleSpanExporter())
    const logRecordProcessor = config.otlpEndpoint
      ? new BatchLogRecordProcessor({ exporter: new OTLPLogExporter({ url: config.otlpEndpoint }) })
      : new BatchLogRecordProcessor({ exporter: new ConsoleLogRecordExporter() })
    return NodeSdk.layer(() => ({
      resource: { serviceName: config.serviceName },
      spanProcessor,
      logRecordProcessor,
    }))
  }),
)

/** Local-only OTel layer (console exporters), no config required. Useful for tests. */
export const OtelConsoleLive: Layer.Layer<never> = NodeSdk.layer(() => ({
  resource: { serviceName: "workflow-runner-test" },
  spanProcessor: new BatchSpanProcessor(new ConsoleSpanExporter()),
  logRecordProcessor: new BatchLogRecordProcessor({ exporter: new ConsoleLogRecordExporter() }),
}))
