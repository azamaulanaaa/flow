import * as NodeSdk from "@effect/opentelemetry/NodeSdk"
import { OTLPLogExporter } from "@opentelemetry/exporter-logs-otlp-http"
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http"
import { BatchLogRecordProcessor, ConsoleLogRecordExporter } from "@opentelemetry/sdk-logs"
import { BatchSpanProcessor, ConsoleSpanExporter } from "@opentelemetry/sdk-trace-base"
import { Effect, Layer } from "effect"
import { AppConfigService } from "@/core/config"

/**
 * Resolve per-signal OTLP HTTP URLs from a single endpoint value.
 *
 * Accepts a base (`http://collector:4318`), a signal path
 * (`.../v1/traces`, `.../v1/logs`), or a trailing-slash base.
 * OTLP HTTP exporters require full signal URLs, so passing the same
 * value to both exporters silently misroutes one signal.
 */
export const resolveOtlpUrls = (
  endpoint: string,
): { readonly tracesUrl: string; readonly logsUrl: string } => {
  const trimmed = endpoint.replace(/\/+$/, "")
  if (trimmed.endsWith("/v1/logs")) {
    return { tracesUrl: `${trimmed.slice(0, -"/v1/logs".length)}/v1/traces`, logsUrl: trimmed }
  }
  if (trimmed.endsWith("/v1/traces")) {
    return { tracesUrl: trimmed, logsUrl: `${trimmed.slice(0, -"/v1/traces".length)}/v1/logs` }
  }
  return { tracesUrl: `${trimmed}/v1/traces`, logsUrl: `${trimmed}/v1/logs` }
}

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
    const urls =
      config.otlpEndpoint !== undefined ? resolveOtlpUrls(config.otlpEndpoint) : undefined
    const spanProcessor = urls
      ? new BatchSpanProcessor(new OTLPTraceExporter({ url: urls.tracesUrl }))
      : new BatchSpanProcessor(new ConsoleSpanExporter())
    const logRecordProcessor = urls
      ? new BatchLogRecordProcessor({ exporter: new OTLPLogExporter({ url: urls.logsUrl }) })
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
