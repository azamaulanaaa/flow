export * from "./add"
export * from "./greet"
import { addFunction } from "./add"
import { greetFunction } from "./greet"

/** All user functions registered with the runtime. Add yours here. */
export const exampleFunctions = [greetFunction, addFunction] as const
