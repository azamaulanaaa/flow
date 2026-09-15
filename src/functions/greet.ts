import { Effect } from "effect"
import { makeFunction } from "@/core/functions/registry"

export interface GreetInput {
  readonly name: string
}

export const greetFunction = makeFunction<GreetInput, string>("greet", (input) =>
  Effect.gen(function* () {
    yield* Effect.log(`Greeting ${input.name}`)
    return `Hello, ${input.name}!`
  }),
)
