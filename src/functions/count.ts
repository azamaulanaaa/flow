import { Effect } from "effect"
import { makeFunction } from "@/core/functions/registry"

export interface CountInput {
  readonly text: string
}

export const countFunction = makeFunction("count", (input: CountInput) =>
  Effect.gen(function* () {
    yield* Effect.log(`Counting chars in ${input.text.length} char input`)
    return input.text.length
  }),
)
