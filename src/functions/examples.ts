import { Effect } from "effect"
import { makeFunction } from "@/functions/registry"

export interface GreetInput {
  readonly name: string
}

export const greetFunction = makeFunction<GreetInput, string>("greet", (input) =>
  Effect.gen(function* () {
    yield* Effect.log(`Greeting ${input.name}`)
    return `Hello, ${input.name}!`
  }),
)

export interface AddInput {
  readonly a: number
  readonly b: number
}

export const addFunction = makeFunction<AddInput, number>("add", (input) =>
  Effect.gen(function* () {
    yield* Effect.log(`Adding ${input.a} + ${input.b}`)
    return input.a + input.b
  }),
)

export const exampleFunctions = [greetFunction, addFunction] as const
