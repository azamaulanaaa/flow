import { Worker } from "node:worker_threads"
import { Context, Data, Deferred, Duration, Effect, Layer } from "effect"
import { FUNCTION_WORKER_MODE } from "@/core/runtime/function-worker"
import type { FunctionWorkerRequest, FunctionWorkerResponse } from "@/core/runtime/function-worker"

export class WorkerPoolError extends Data.TaggedError("WorkerPoolError")<{
  readonly reason: string
}> {}

export class WorkerPoolTimeoutError extends Data.TaggedError("WorkerPoolTimeoutError")<{
  readonly fn: string
  readonly timeoutMs: number
}> {}

/** Max in-flight requests per slot before failing fast (bounds pending Map growth). */
export const MAX_PENDING_PER_SLOT = 1000

/** Allocate a request id that does not collide with in-flight requests. Wraps on overflow. */
export const allocRequestId = (slot: { nextId: number; pending: Map<number, unknown> }): number => {
  for (let i = 0; i < MAX_PENDING_PER_SLOT + 1; i++) {
    if (slot.nextId > Number.MAX_SAFE_INTEGER - 1) {
      slot.nextId = 1
    }
    const id = slot.nextId++
    if (!slot.pending.has(id)) {
      return id
    }
  }
  return -1
}

export interface WorkerPoolOptions {
  /** False = stub that always fails (runner falls back to in-process). No threads spawned. */
  readonly enabled: boolean
  readonly size: number
  /** App entrypoint URL — workers boot the same bundle/file in dispatcher mode. */
  readonly entryUrl: URL | string
  readonly timeoutMs: number
}

interface Slot {
  worker: Worker | null
  pending: Map<number, Deferred.Deferred<unknown, WorkerPoolError>>
  nextId: number
  dead: boolean
}

export interface WorkerPoolShape {
  readonly execute: (
    fn: string,
    input: unknown,
  ) => Effect.Effect<unknown, WorkerPoolError | WorkerPoolTimeoutError>
}

export class WorkerPool extends Context.Tag("WorkerPool")<WorkerPool, WorkerPoolShape>() {}

/**
 * Fixed-size pool of `node:worker_threads` function executors.
 *
 * Workers boot the app entrypoint in dispatcher mode (same bundle, no extra
 * build artifact) and run registry functions by name — only `input`/`output`
 * cross the boundary via structured clone. Best-effort by design: every
 * failure mode (dead workers, timeouts, unserializable payloads) surfaces as
 * a typed error so the caller can fall back to in-process execution.
 */
export const WorkerPoolLive = (options: WorkerPoolOptions): Layer.Layer<WorkerPool> =>
  Layer.scoped(
    WorkerPool,
    Effect.gen(function* () {
      if (!options.enabled || options.size <= 0) {
        return WorkerPool.of({
          execute: () => Effect.fail(new WorkerPoolError({ reason: "worker pool disabled" })),
        })
      }

      const size = Math.max(1, Math.floor(options.size))
      const slots: Array<Slot> = []
      let cursor = 0
      let closed = false

      const complete = (effect: Effect.Effect<void>): void => {
        // EventEmitter callbacks are outside the Effect runtime — fork completion in.
        Effect.runFork(effect)
      }

      const killSlot = (slot: Slot, reason: string): void => {
        slot.dead = true
        for (const pending of slot.pending.values()) {
          complete(Deferred.fail(pending, new WorkerPoolError({ reason })))
        }
        slot.pending.clear()
      }

      const attach = (slot: Slot): void => {
        let worker: Worker
        try {
          worker = new Worker(options.entryUrl, { workerData: { mode: FUNCTION_WORKER_MODE } })
        } catch {
          slot.dead = true
          return
        }
        slot.worker = worker
        slot.dead = false
        worker.on("message", (message: FunctionWorkerResponse) => {
          const pending = slot.pending.get(message.id)
          if (pending === undefined) {
            return // late reply after a timeout; already cleaned up
          }
          slot.pending.delete(message.id)
          if (message.ok) {
            complete(Deferred.succeed(pending, message.output))
          } else {
            complete(Deferred.fail(pending, new WorkerPoolError({ reason: message.error })))
          }
        })
        worker.on("error", (error) => {
          killSlot(slot, `worker error: ${error instanceof Error ? error.message : String(error)}`)
          respawn(slot)
        })
        worker.on("exit", (code) => {
          if (!slot.dead && !closed) {
            killSlot(slot, `worker exited with code ${code}`)
            respawn(slot)
          }
        })
      }

      const respawn = (slot: Slot): void => {
        if (closed) {
          return
        }
        slot.worker = null
        try {
          attach(slot)
        } catch {
          slot.dead = true
        }
      }

      for (let i = 0; i < size; i++) {
        const slot: Slot = { worker: null, pending: new Map(), nextId: 1, dead: true }
        attach(slot)
        slots.push(slot)
      }

      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          closed = true
          for (const slot of slots) {
            const worker = slot.worker
            slot.worker = null
            slot.dead = true
            if (worker !== null) {
              yield* Effect.promise(() => worker.terminate()).pipe(Effect.ignore)
            }
          }
        }),
      )

      const execute: WorkerPoolShape["execute"] = (fn, input) =>
        Effect.gen(function* () {
          let slot: Slot | undefined
          for (let i = 0; i < slots.length; i++) {
            const candidate = slots[(cursor + i) % slots.length]!
            if (!candidate.dead && candidate.worker !== null) {
              slot = candidate
              cursor = (cursor + i + 1) % slots.length
              break
            }
          }
          if (slot === undefined || slot.worker === null) {
            return yield* Effect.fail(new WorkerPoolError({ reason: "no live workers in pool" }))
          }
          const live = slot
          const worker = live.worker
          if (worker === null) {
            return yield* Effect.fail(new WorkerPoolError({ reason: "no live workers in pool" }))
          }
          if (live.pending.size >= MAX_PENDING_PER_SLOT) {
            return yield* Effect.fail(
              new WorkerPoolError({ reason: `worker slot overloaded (${live.pending.size} pending)` }),
            )
          }
          const id = allocRequestId(live)
          if (id < 0) {
            return yield* Effect.fail(new WorkerPoolError({ reason: "worker slot id space exhausted" }))
          }
          const deferred = yield* Deferred.make<unknown, WorkerPoolError>()
          live.pending.set(id, deferred)
          try {
            worker.postMessage({ id, fn, input } satisfies FunctionWorkerRequest)
          } catch {
            live.pending.delete(id)
            return yield* Effect.fail(
              new WorkerPoolError({ reason: `unserializable input for function: ${fn}` }),
            )
          }
          return yield* Deferred.await(deferred).pipe(
            Effect.timeoutFail({
              duration: Duration.millis(options.timeoutMs),
              onTimeout: () => {
                live.pending.delete(id)
                return new WorkerPoolTimeoutError({ fn, timeoutMs: options.timeoutMs })
              },
            }),
          )
        })

      return WorkerPool.of({ execute })
    }),
  )
