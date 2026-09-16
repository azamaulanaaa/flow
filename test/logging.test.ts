import { describe, expect, it } from "@effect/vitest"
import { Effect } from "effect"
import { parseLogLevel } from "@/core/logging"

describe("parseLogLevel", () => {
  it.effect("parses canonical levels case-insensitively", () =>
    Effect.gen(function* () {
      expect(parseLogLevel("INFO").label).toBe("INFO")
      expect(parseLogLevel("debug").label).toBe("DEBUG")
      expect(parseLogLevel("Trace").label).toBe("TRACE")
      expect(parseLogLevel("ERROR").label).toBe("ERROR")
      expect(parseLogLevel("fatal").label).toBe("FATAL")
    }),
  )

  it.effect("maps WARN and WARNING to Warning", () =>
    Effect.gen(function* () {
      expect(parseLogLevel("WARN").label).toBe("WARN")
      expect(parseLogLevel("warning").label).toBe("WARN")
    }),
  )

  it.effect("falls back to Info on unknown values", () =>
    Effect.gen(function* () {
      expect(parseLogLevel("verbose").label).toBe("INFO")
      expect(parseLogLevel("").label).toBe("INFO")
    }),
  )
})
