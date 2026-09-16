import { isMainThread, parentPort, workerData } from "node:worker_threads"
import { Effect } from "effect"
import { FunctionRegistryLive, runFunction } from "@/core/functions/registry"
import { exampleFunctions } from "@/functions"

/**
 * `workerData.mode` value that boots the function dispatcher instead of the app.
 * Workers are spawned on the app entrypoint itself (same bundle/file), and
 * `src/index.ts` branches here when it detects this mode in a worker thread.
 */
export const FUNCTION_WORKER_MODE = "function-worker"

export interface FunctionWorkerRequest {
  readonly id: number
  readonly fn: string
  readonly input: unknown
}

export type FunctionWorkerResponse =
  | { readonly id: number; readonly ok: true; readonly output: unknown }
  | { readonly id: number; readonly ok: false; readonly error: string }

/** True inside a worker thread booted as a function executor. */
export const isFunctionWorkerThread = (): boolean =>
  !isMainThread &&
  (workerData as { readonly mode?: unknown } | undefined)?.mode === FUNCTION_WORKER_MODE

const RegistryLayer = FunctionRegistryLive([...exampleFunctions])

const serializeError = (error: unknown): string => {
  if (typeof error === "object" && error !== null && "_tag" in error) {
    const tag = (error as { readonly _tag: unknown })._tag
    const name = (error as { readonly name?: unknown }).name
    return typeof tag === "string"
      ? `${tag}${typeof name === "string" ? `: ${name}` : ""}`
      : String(error)
  }
  return error instanceof Error ? error.message : String(error)
}

/**
 * Worker-thread entrypoint: serves `runFunction` requests over `parentPort`.
 *
 * The worker loads the same function registry modules as the main thread and
 * looks functions up by name, so only `input`/`output` cross the thread
 * boundary (structured clone). Never returns; the parent terminates the
 * worker on pool shutdown.
 *
 * No OTel layer here by design: spans stay on the main thread around
 * `WorkerPool.execute`. Worker `Effect.log` calls go to inherited stdout.
 */
export const runFunctionWorkerEntry = async (): Promise<never> => {
  const port = parentPort
  if (port === null) {
    throw new Error("function worker started without parentPort")
  }
  port.on("message", (message: FunctionWorkerRequest) => {
    void (async () => {
      let response: FunctionWorkerResponse
      try {
        const output = await Effect.runPromise(
          runFunction<unknown, unknown>(message.fn, message.input).pipe(
            Effect.provide(RegistryLayer),
          ),
        )
        response = { id: message.id, ok: true, output }
      } catch (error) {
        response = { id: message.id, ok: false, error: serializeError(error) }
      }
      try {
        port.postMessage(response)
      } catch {
        port.postMessage({
          id: message.id,
          ok: false,
          error: `unserializable output from function: ${message.fn}`,
        })
      }
    })()
  })
  return new Promise<never>(() => {})
}
