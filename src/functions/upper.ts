import { Effect } from "effect"
import { makeFunction } from "@/core/functions/registry"

export interface UpperInput {
  readonly text: string
}

export const upperFunction = makeFunction("upper", (input: UpperInput) =>
  Effect.gen(function* () {
    yield* Effect.log(`Upcasing ${input.text.length} chars`)
    return input.text.toUpperCase()
  }),
)
