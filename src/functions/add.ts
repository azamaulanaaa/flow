import { Effect } from "effect"
import { makeFunction } from "@/core/functions/registry"

export interface AddInput {
  readonly a: number
  readonly b: number
}

export const addFunction = makeFunction("add", (input: AddInput) =>
  Effect.gen(function* () {
    yield* Effect.log(`Adding ${input.a} + ${input.b}`)
    return input.a + input.b
  }),
)
