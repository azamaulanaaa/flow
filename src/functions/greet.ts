import { Effect } from "effect"
import { makeFunction } from "@/core/functions/registry"

export interface GreetInput {
  readonly name: string
}

export const greetFunction = makeFunction("greet", (input: GreetInput) =>
  Effect.gen(function* () {
    yield* Effect.log(`Greeting ${input.name}`)
    return `Hello, ${input.name}!`
  }),
)
