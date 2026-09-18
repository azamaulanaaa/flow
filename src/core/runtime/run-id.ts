/**
 * Unique run ids for trigger-produced {@link RunRequest}s.
 *
 * `Date.now()`-based ids collide across restarts and parallel workers, so
 * triggers use `crypto.randomUUID()` (available on Node/Bun/Deno) with a
 * workflow prefix for readability (`welcome-3f9a-...`). Falls back to a
 * timestamp + random suffix only where `randomUUID` is unavailable.
 */
export const newRunId = (prefix: string): string => {
  const clean = prefix.trim().length > 0 ? prefix : "run"
  const cryptoApi = (globalThis as { readonly crypto?: { readonly randomUUID: () => string } })
    .crypto
  // NB: call as a method — detached `randomUUID` loses its `this` (Crypto).
  if (cryptoApi !== undefined && typeof cryptoApi.randomUUID === "function") {
    return `${clean}-${cryptoApi.randomUUID()}`
  }
  const suffix = `${Date.now().toString(36)}-${Math.floor(Math.random() * 2 ** 48).toString(36)}`
  return `${clean}-${suffix}`
}
