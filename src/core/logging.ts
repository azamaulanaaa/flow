import { Effect, Layer, LogLevel, Logger } from "effect"
import { AppConfigService } from "@/core/config"

/** Parse LOG_LEVEL (case-insensitive, WARN=Warning). Falls back to Info. */
export const parseLogLevel = (raw: string): LogLevel.LogLevel => {
  const normalized = raw.trim().toLowerCase()
  switch (normalized) {
    case "trace":
      return LogLevel.Trace
    case "debug":
      return LogLevel.Debug
    case "info":
      return LogLevel.Info
    case "warn":
    case "warning":
      return LogLevel.Warning
    case "error":
      return LogLevel.Error
    case "fatal":
      return LogLevel.Fatal
    case "none":
      return LogLevel.None
    case "all":
      return LogLevel.All
    default:
      return LogLevel.Info
  }
}

/**
 * Minimum log-level layer driven by {@link AppConfigService}.
 * Previously LOG_LEVEL was parsed but never wired to the runtime.
 */
export const LogLevelLive: Layer.Layer<never, never, AppConfigService> = Layer.unwrapEffect(
  Effect.map(AppConfigService, (config) => Logger.minimumLogLevel(parseLogLevel(config.logLevel))),
)
