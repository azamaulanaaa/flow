import { parentPort } from "node:worker_threads"

// Minimal FUNCTION_WORKER protocol fixture for pool mechanics tests.
// Echoes input back; never replies for "never-replies"; fails for "fails".
parentPort?.on("message", (message) => {
  if (message.fn === "never-replies") {
    return
  }
  if (message.fn === "fails") {
    parentPort?.postMessage({ id: message.id, ok: false, error: "WorkerError: boom" })
    return
  }
  parentPort?.postMessage({ id: message.id, ok: true, output: { echo: message.input } })
})
