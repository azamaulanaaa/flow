import { describe, expect, it } from "@effect/vitest"
import { Effect } from "effect"
import { resolveOtlpUrls } from "@/core/otel"

describe("resolveOtlpUrls", () => {
  it.effect("expands a base endpoint into signal URLs", () =>
    Effect.gen(function* () {
      expect(resolveOtlpUrls("http://collector:4318")).toEqual({
        tracesUrl: "http://collector:4318/v1/traces",
        logsUrl: "http://collector:4318/v1/logs",
      })
    }),
  )

  it.effect("derives the sibling signal from a signal URL", () =>
    Effect.gen(function* () {
      expect(resolveOtlpUrls("http://collector:4318/v1/logs")).toEqual({
        tracesUrl: "http://collector:4318/v1/traces",
        logsUrl: "http://collector:4318/v1/logs",
      })
      expect(resolveOtlpUrls("http://collector:4318/v1/traces")).toEqual({
        tracesUrl: "http://collector:4318/v1/traces",
        logsUrl: "http://collector:4318/v1/logs",
      })
    }),
  )

  it.effect("strips trailing slashes", () =>
    Effect.gen(function* () {
      expect(resolveOtlpUrls("http://collector:4318/")).toEqual({
        tracesUrl: "http://collector:4318/v1/traces",
        logsUrl: "http://collector:4318/v1/logs",
      })
    }),
  )
})
