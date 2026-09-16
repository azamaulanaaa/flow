# workflow-runner

Effect-TS workflow runner: no UI, no endpoints. Named functions stitched into
DAG workflows, driven by modular triggers, observed via OpenTelemetry.

## How it works

```
Trigger (cron | once | future: PocketBase, HTTP, ...)
  -> RuntimeBus Queue (bounded, backpressure) -> worker pool
  -> Workflow DAG (levels run in order, nodes in parallel)
  -> FunctionRegistry (each call traced as function.<name>)
  -> RunEvent PubSub (started / succeeded / failed)
```

Spans nest as `trigger.*` → `run.*` → `workflow.*` → `function.*`.
`Effect.log` is exported twice: as OTel log records (console exporter locally,
OTLP `.../v1/traces` + `.../v1/logs` derived from `OTEL_EXPORTER_OTLP_ENDPOINT`
when set) and as span events on the enclosing span — giving both searchable
streams and correlated waterfall context. `RunFailed` events carry a
machine-readable `causeTag` (e.g. `UnknownFunctionError`,
`WorkflowNodeTimeoutError`, `UnknownWorkflow`) alongside the human-readable
`reason`.

## Scripts

```sh
npm install
npm run check   # typecheck + format:check + test + build (mirrors CI)
npm start       # node dist/index.js (after build)
```

Individual steps: `npm run typecheck|test|build|format:check`,
dev via `npm run dev`, watch mode via `npm run test:watch`.

> Note: if `npm install` fails with `EPERM ... symlink` in a restricted
> sandbox, retry with `npm install --no-bin-links` and invoke the tool
> directly without relying on `.bin` symlinks, e.g.
> `node node_modules/typescript/lib/tsc.js --noEmit -p tsconfig.json`.

## Configuration (env)

| Var                           | Default           | Description                                                                                                                    |
| ----------------------------- | ----------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `SERVICE_NAME`                | `workflow-runner` | OTel service name                                                                                                              |
| `LOG_LEVEL`                   | `INFO`            | minimum log level (TRACE,DEBUG,INFO,WARN,ERROR,FATAL,NONE,ALL)                                                                 |
| `QUEUE_CAPACITY`              | `128`             | run-queue bound (backpressure)                                                                                                 |
| `RUNTIME_WORKERS`             | `4`               | runtime worker fibers draining the queue                                                                                       |
| `WORKFLOW_CONCURRENCY`        | `32`              | max parallel nodes per DAG level + default node input comes from trigger `input` when node `input` is unset                    |
| `WORKFLOW_NODE_TIMEOUT_MS`    | `0`               | per-node timeout in ms (`0` = disabled); fails the node with `WorkflowNodeTimeoutError`, retried per `WORKFLOW_RETRY_ATTEMPTS` |
| `WORKFLOW_RETRY_ATTEMPTS`     | `0`               | extra retry attempts per node after the first try                                                                              |
| `CRON_EXPRESSION`             | `*/1 * * * *`     | schedule for the bundled cron trigger                                                                                          |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | _(unset)_         | OTLP HTTP base (`http://host:4318` -> `.../v1/traces` + `.../v1/logs`); unset = console exporter                               |
| `SHUTDOWN_TIMEOUT_MS`         | `10000`           | max wait to drain queued + in-flight runs on SIGINT/SIGTERM                                                                    |
| `WORKER_POOL_ENABLED`         | `false`           | run functions in `node:worker_threads` (true multithreading for CPU-bound work)                                                |
| `WORKER_POOL_SIZE`            | CPUs              | worker thread count                                                                                                            |
| `WORKER_POOL_TIMEOUT_MS`      | `30000`           | per-function worker timeout (falls back to in-process)                                                                         |

## Shutdown

On `SIGINT`/`SIGTERM` (`NodeRuntime.runMain` interrupts `Effect.never`):

1. trigger + worker fibers stop in LIFO scope order (no new runs accepted),
2. a finalizer drains the queue and in-flight runs via `waitForIdle`,
   bounded by `SHUTDOWN_TIMEOUT_MS`,
3. scope release flushes OTel Batch processors (spans + log records).

Logs show `Shutdown requested (...)` then `Shutdown complete, flushing telemetry`.

## Runtimes — Node, Bun, Deno

- **Node** (primary): `npm run check` (`typecheck|format:check|test|build`) or `npm start`. Tests run on Node.
- **Bun** (verified): runs TS directly with tsconfig `@/` paths, no build step.
  ```sh
  bun src/index.ts            # dev (replaces `npm run dev` / tsx)
  bun dist/index.js           # built bundle (replaces `npm start`)
  bun node_modules/vitest/vitest.mjs run   # tests, ~2x faster here
  ```
  `NodeRuntime` is used on Bun
  (dynamic import in `src/index.ts`), worker pool verified working.
- **Deno** (experimental): `deno.json` maps `@/` (trailing-slash import-map
  form — Deno has no TS `paths` wildcards) and enables `sloppy-imports` for
  our extensionless + directory imports; tasks (`deno task dev|start|check`,
  needs `--allow-all` for env/net/threads). `src/index.ts` avoids evaluating
  `@effect/platform-node` on Deno (dynamic import on Node/Bun only) and runs
  via `runWithDenoSignals` (`src/core/platform.ts`), which forks and
  interrupts on `SIGINT`/`SIGTERM` so the drain + OTel flush path is shared.
  Caveats: `node:worker_threads`/`node:os` are Deno-covered for our subset —
  pool spawn failures fall back to in-process; OTel context via
  `node:async_hooks` is partial on Deno, so cross-span correlation may degrade.

## Layout — framework vs app

- `src/core/` – framework, do not put business logic here:
  - `core/functions/registry.ts` – `makeFunction`, `FunctionRegistry`
  - `core/workflows/definition.ts`, `runner.ts` – DAG validation + parallel runner
  - `core/runtime/bus.ts`, `service.ts` – Queue/PubSub bus + runtime workers, drain, typed failures
  - `core/runtime/worker-pool.ts`, `function-worker.ts` – thread pool (opt-in true parallelism)
  - `core/triggers/trigger.ts`, `cron.ts` – `Trigger` interface + impls
  - `core/config.ts`, `core/otel.ts`, `core/logging.ts` – env config, OTel SDK layer, log-level layer
  - `core/platform.ts` – runtime detection (`node`/`bun`/`deno`) + Deno signal runner
- `src/functions/` – **put your functions here**, one file per function:
  - `greet.ts`, `add.ts` – examples using `makeFunction` from `@/core/functions/registry`
  - `index.ts` – barrel, re-export + append to `exampleFunctions`
- `src/workflows/` – **put your workflows here**, one file per workflow:
  - `welcome.ts` – example referencing function names by string (`fn: "greet"`)
  - `index.ts` – barrel, re-export + append to `exampleWorkflows`
- `src/index.ts` – composition root only (wires `core/*` + `functions` + `workflows`, no logic)
- `test/` – Vitest + `@effect/vitest` suites per module

Internal imports use the `@/` alias for `src/` (extensionless, e.g.
`@/core/functions/registry`), mapped in `tsconfig.json` and resolved by Vitest,
tsup (build) and tsx (dev).

## Guides

### Add a function — `src/functions/<name>.ts`

```ts
import { Effect } from "effect"
import { makeFunction } from "@/core/functions/registry"

export const myFn = makeFunction<{ who: string }, string>("my-fn", (input) =>
  Effect.gen(function* () {
    yield* Effect.log(`hello ${input.who}`)
    return `hi ${input.who}`
  }),
)
```

Then register in `src/functions/index.ts`:
`export * from "./<name>"` + append to `exampleFunctions`.
Name must match `fn:` used in workflows.

### Add a workflow — `src/workflows/<name>.ts`

```ts
import type { WorkflowDef } from "@/core/workflows/definition"

export const myFlow: WorkflowDef = {
  name: "my-flow",
  nodes: [
    { id: "a", fn: "my-fn", input: { who: "world" } },
    { id: "b", fn: "other-fn", dependsOn: ["a"], input: (outputs) => ({ prev: outputs.get("a") }) },
  ],
}
```

Then register in `src/workflows/index.ts` + trigger it via cron/`makeOnceTrigger`.
Levels run sequentially, nodes in a level run in parallel via `Effect.all`
(bounded by `WORKFLOW_CONCURRENCY`, default 32). Nodes without explicit `input`
receive the trigger's run `input`; per-node timeout (`WORKFLOW_NODE_TIMEOUT_MS`,
`0` = disabled) and retries (`WORKFLOW_RETRY_ATTEMPTS`) are opt-in.

### Add a trigger

Use `makeCronTrigger({ schedule, workflow })` or `makeOnceTrigger({ workflow })`
from `@/core/triggers/cron`. Wire `trigger.start` in `src/index.ts` (scoped).

## Thread pool (opt-in true parallelism)

Fibers already overlap I/O-bound work. For CPU-bound functions (parse, crypto,
compute), set `WORKER_POOL_ENABLED=true`: `runNode` routes each function call
through a fixed pool of `node:worker_threads` (`src/core/runtime/worker-pool.ts`),
sized by `WORKER_POOL_SIZE`. Workers boot the same bundle in dispatcher mode
(`src/core/runtime/function-worker.ts`, no extra build artifact) and run
registry functions by name.

Rules and limits:

- Only `input`/`output` cross the boundary via structured clone — keep them
  plain data. Unserializable payloads fail fast and fall back to in-process.
- Every pool failure (disabled, dead workers, `WORKER_POOL_TIMEOUT_MS`, worker
  errors) falls back to in-process execution, so the pool is best-effort.
  Fallback on timeout is at-least-once: keep pooled functions idempotent.
- Trace spans stay on the main thread around `execute`; worker `Effect.log`
  lines go to inherited stdout without OTel enrichment.
- Threads are terminated on scope close (graceful shutdown ordering applies).
- Each slot fails fast past 1000 in-flight requests (`worker slot overloaded`)
  and request ids wrap without colliding — both fall back to in-process.

## CI

`.github/workflows/ci.yml` runs on push + pull requests, five jobs:

- `format` – `npm run format:check` (Prettier)
- `typescript` – `npm run typecheck`
- `node` – `npm run test` + `npm run build` + boot smoke test
- `bun` – `bun node_modules/vitest/vitest.mjs run` + boot smoke test
- `deno` – `deno task test` + boot smoke test

Static checks (`format`, `typescript`) run once on Node — `tsc` is a native
(Go) binary there, so per-runtime typechecking is redundant. Each runtime
job (`node`, `bun`, `deno`) runs the same Vitest suite plus a boot smoke
test: note `deno task test` invokes the suite via Node (Vitest cannot run
on the Deno runtime itself); Deno-runtime coverage comes from the smoke
boot, which runs `deno run --allow-all src/index.ts`.

Each smoke test boots the real service, waits for the `started: cron` log,
then `SIGTERM`s it and asserts exit `0` plus `Shutdown complete` (graceful
drain path).

## License

Dual-licensed under [MIT](LICENSE-MIT) and [Apache 2.0](LICENSE-APACHE).
