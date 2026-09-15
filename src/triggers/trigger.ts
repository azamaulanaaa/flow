import type { Effect } from "effect"
import type { Scope } from "effect/Scope"
import type { RuntimeBus } from "../runtime/bus.js"

/**
 * A trigger produces {@link RunRequest}s from the outside world
 * (cron, webhooks, queue consumers, reactive stores, ...).
 *
 * `start` is scoped: the trigger runs until its scope is closed,
 * so shutting the app scope down stops all triggers gracefully.
 */
export interface Trigger {
  readonly tag: string
  readonly start: Effect.Effect<void, never, Scope | RuntimeBus>
}
