import { Context, Data, Effect, Layer } from "effect"

/** A named, traced unit of work that workflows can stitch together. */
export interface FunctionDef<I = any, O = any, E = any> {
  readonly name: string
  readonly run: (input: I) => Effect.Effect<O, E>
}

export class UnknownFunctionError extends Data.TaggedError("UnknownFunctionError")<{
  readonly name: string
}> {}

/**
 * Define a function with automatic OpenTelemetry tracing.
 *
 * Each invocation creates a `function.<name>` span; Effect logs inside `run`
 * are exported as span events by the OTel layer.
 */
export const makeFunction = <I, O, E = never>(
  name: string,
  run: (input: I) => Effect.Effect<O, E>,
): FunctionDef<I, O, E> => ({
  name,
  run: (input: I) =>
    Effect.gen(function* () {
      yield* Effect.annotateCurrentSpan("function.name", name)
      return yield* run(input)
    }).pipe(Effect.withSpan(`function.${name}`)),
})

export class FunctionRegistry extends Context.Tag("FunctionRegistry")<
  FunctionRegistry,
  ReadonlyMap<string, FunctionDef<any, any, any>>
>() {}

/** Build a registry layer from a list of function definitions. */
export const FunctionRegistryLive = (
  defs: ReadonlyArray<FunctionDef<any, any, any>>,
): Layer.Layer<FunctionRegistry> =>
  Layer.succeed(
    FunctionRegistry,
    new Map(defs.map((d) => [d.name, d])),
  )

/** Look up a function by name, failing with {@link UnknownFunctionError}. */
export const lookupFunction = (
  name: string,
): Effect.Effect<FunctionDef<any, any, any>, UnknownFunctionError, FunctionRegistry> =>
  Effect.gen(function* () {
    const registry = yield* FunctionRegistry
    const fn = registry.get(name)
    if (fn === undefined) {
      return yield* Effect.fail(new UnknownFunctionError({ name }))
    }
    return fn
  })

/** Look up and run a function by name with the given input. */
export const runFunction = <I, O>(
  name: string,
  input: I,
): Effect.Effect<O, UnknownFunctionError | unknown, FunctionRegistry> =>
  Effect.gen(function* () {
    const fn = (yield* lookupFunction(name)) as FunctionDef<I, O, any>
    return yield* fn.run(input) as Effect.Effect<O, any>
  }).pipe(Effect.withSpan(`function.call.${name}`)) as Effect.Effect<
    O,
    UnknownFunctionError | unknown,
    FunctionRegistry
  >
