import { describe, expect, it } from "@effect/vitest"
import { Effect } from "effect"
import { exampleFunctions } from "@/functions/examples"
import {
  FunctionRegistryLive,
  UnknownFunctionError,
  lookupFunction,
  runFunction,
} from "@/functions/registry"

const TestRegistryLive = FunctionRegistryLive([...exampleFunctions])

describe("FunctionRegistry", () => {
  it.effect("looks up a registered function by name", () =>
    Effect.gen(function* () {
      const fn = yield* lookupFunction("greet")
      expect(fn.name).toBe("greet")
    }).pipe(Effect.provide(TestRegistryLive)),
  )

  it.effect("runs a registered function with input", () =>
    Effect.gen(function* () {
      const result = yield* runFunction<{ name: string }, string>("greet", { name: "Ada" })
      expect(result).toBe("Hello, Ada!")
    }).pipe(Effect.provide(TestRegistryLive)),
  )

  it.effect("fails with UnknownFunctionError for missing names", () =>
    Effect.gen(function* () {
      const exit = yield* Effect.exit(lookupFunction("nope"))
      expect(exit._tag).toBe("Failure")
      if (exit._tag === "Failure") {
        expect(exit.cause._tag).toBe("Fail")
      }
      const error = yield* Effect.flip(lookupFunction("nope"))
      expect(error).toBeInstanceOf(UnknownFunctionError)
      expect(error.name).toBe("nope")
    }).pipe(Effect.provide(TestRegistryLive)),
  )
})
