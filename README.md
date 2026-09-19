# workflow-runner

Effect-TS workflow runner: no UI, no endpoints. Named functions stitched into
DAG workflows, driven by modular triggers, observed via OpenTelemetry.

## How it works

```
Trigger (cron | once | subscription: makeSubscriptionTrigger for webhooks/queues/...)
  -> RuntimeBus Queue (bounded, dropping when full) -> runtime worker fibers
  -> Workflow DAG (levels run in order, nodes in parallel)
  -> per-node worker pool (opt-in) -> FunctionRegistry (each call traced)
  -> RunEvent PubSub (started / succeeded / failed)
```

Spans nest as `trigger.cron|once|subscription.<workflow>` → `run.<workflow>`
→ `workflow.<workflow>` → `workflow.<workflow>.node.<id>`
→ `function.call.<name>` → `function.<name>`.
`Effect.log` is exported as OTel log records (console exporter locally,
OTLP `.../v1/traces` + `.../v1/logs` derived from `OTEL_EXPORTER_OTLP_ENDPOINT`
when set — a bare base, a trailing-slash base, or a full signal URL are all
accepted, see `resolveOtlpUrls`) and logs inside a span are additionally
attached as span events by Effect — giving both searchable
streams and correlated waterfall context. `RunFailed` events carry a
machine-readable `causeTag` (e.g. `UnknownFunctionError`,
`WorkflowNodeTimeoutError`, `UnknownWorkflow`) alongside the human-readable
`reason`; `RunSucceeded` events carry the node `outputs` plus the `skipped`
ids from `when` gates.

## Scripts

```sh
npm install
npm run check   # typecheck + format:check + validate + test + build (CI adds per-runtime smoke tests)
npm start       # node dist/index.js (after build)
```

Individual steps: `npm run typecheck|validate|test|build|format:check`,
dev via `npm run dev`, watch mode via `npm run test:watch`.
`npm run validate` checks boot wiring (env ranges, duplicate names, empty/DAG
shape, `fn:` resolution, trigger construction including cron parsing) without
starting triggers or workers —
run it before `npm start` after editing workflows.

> Note: if `npm install` fails with `EPERM ... symlink` in a restricted
> sandbox, retry with `npm install --no-bin-links` and invoke the tool
> directly without relying on `.bin` symlinks, e.g.
> `node node_modules/typescript/lib/tsc.js --noEmit -p tsconfig.json`.

## Configuration (env)

| Var                           | Default           | Description                                                                                                                    |
| ----------------------------- | ----------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `SERVICE_NAME`                | `workflow-runner` | OTel service name (must be non-empty)                                                                                          |
| `LOG_LEVEL`                   | `INFO`            | minimum log level, case-insensitive (TRACE,DEBUG,INFO,WARN/WARNING,ERROR,FATAL,NONE,ALL)                                       |
| `QUEUE_CAPACITY`              | `128`             | run-queue bound; `submitRun` uses non-blocking `Queue.offer`, so a full queue drops the run                                    |
| `RUNTIME_WORKERS`             | `4`               | runtime worker fibers draining the queue                                                                                       |
| `WORKFLOW_CONCURRENCY`        | `32`              | max parallel nodes per DAG level                                                                                               |
| `WORKFLOW_NODE_TIMEOUT_MS`    | `0`               | per-node timeout in ms (`0` = disabled); fails the node with `WorkflowNodeTimeoutError`, retried per `WORKFLOW_RETRY_ATTEMPTS` |
| `WORKFLOW_RETRY_ATTEMPTS`     | `0`               | extra retry attempts per node after the first try                                                                              |
| `WELCOME_CRON_EXPRESSION`     | `*/1 * * * *`     | schedule for the bundled welcome cron trigger (workflow-owned env, not core config)                                            |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | _(unset)_         | OTLP HTTP base or full signal URL (`http://host:4318`, `.../v1/traces`, `.../v1/logs`); unset = console exporter               |
| `SHUTDOWN_TIMEOUT_MS`         | `10000`           | max wait to drain queued + in-flight runs on SIGINT/SIGTERM                                                                    |
| `WORKER_POOL_ENABLED`         | `false`           | run functions in `node:worker_threads` (true multithreading for CPU-bound work)                                                |
| `WORKER_POOL_SIZE`            | CPUs              | worker thread count (defaults to `node:os` `availableParallelism()`, fallback `4`)                                             |
| `WORKER_POOL_TIMEOUT_MS`      | `30000`           | per-function worker timeout (falls back to in-process)                                                                         |

Nodes without explicit `input` receive the trigger's run `input` as their
default (`RunRequest.input` → `defaultInput`).

## Shutdown

On `SIGINT`/`SIGTERM` (`NodeRuntime.runMain` on Node/Bun interrupts
`Effect.never`; `runWithDenoSignals` interrupts the forked fiber on Deno):

1. trigger + worker fibers stop in LIFO scope order (no new runs accepted),
2. a finalizer drains the queue and in-flight runs via `waitForIdle`,
   bounded by `SHUTDOWN_TIMEOUT_MS`,
3. scope release flushes OTel Batch processors (spans + log records).

Logs show `Shutdown requested (...)` then `Shutdown complete, flushing telemetry`.

## Production readiness

- **Fail-fast boot:** env ranges are validated (`SERVICE_NAME` non-empty,
  `QUEUE_CAPACITY`/`RUNTIME_WORKERS`/`WORKFLOW_CONCURRENCY`/`WORKER_POOL_SIZE`/
  `WORKER_POOL_TIMEOUT_MS`/`SHUTDOWN_TIMEOUT_MS` ≥ 1,
  `WORKFLOW_NODE_TIMEOUT_MS`/`WORKFLOW_RETRY_ATTEMPTS` ≥ 0, `LOG_LEVEL`
  allow-listed including `WARN`/`WARNING`) and duplicate workflow or
  function names abort boot with `DuplicateWorkflowError` /
  `DuplicateFunctionError` — conflicting implementations never shadow each other.
  Share the same `FunctionDef` reference across bundles to reuse a function.
- **Run ids:** triggers mint `${workflow}-${uuid}` via `crypto.randomUUID()`
  (empty prefixes fall back to `run-...`; a timestamp + random suffix is used
  only where `randomUUID` is unavailable), unique across restarts and workers
  (no `Date.now()` collisions). `once` also accepts a fixed `runId`, `cron`
  accepts a `runIdPrefix`.
- **Durability (non-goal):** the run queue is in-memory (`Queue.bounded`) and
  `submitRun` is non-blocking (`Queue.offer` — a full queue drops the run),
  while `RunEvent` delivery
  is dropping (slow subscribers never block workers). Queued runs are lost on
  crash; a clean `SIGTERM` drains via `waitForIdle` bounded by
  `SHUTDOWN_TIMEOUT_MS`. If you need durable queues or exactly-once semantics,
  put them in a trigger (queue consumer submitting runs) — the core stays
  ephemeral by design.
- **At-least-once pool:** every worker-pool failure (including timeout) falls
  back to in-process execution, so a timed-out function may still complete in
  the worker. Keep pooled functions idempotent.
- **Input validation:** node inputs are unvalidated `unknown` between steps —
  validate inside functions (e.g. Effect Schema) and keep `input`/`when`
  mappers pure and total (a throw becomes a defect and fails the run as
  `Die`).
- **Health:** no HTTP endpoints by design (nothing to probe). Supervise the
  process itself (systemd/k8s `exec` or OTel `started: ...` logs); add a trigger
  that exports health if your platform needs an endpoint.

## Runtimes — Node, Bun, Deno

- **Node** (primary): `npm run check` (`typecheck|format:check|validate|test|build`) or `npm start`. Tests run on Node.
- **Bun** (verified): runs TS directly with tsconfig `@/` paths, no build step.
  ```sh
  bun src/index.ts            # dev (replaces `npm run dev` / tsx)
  bun dist/index.js           # built bundle (replaces `npm start`)
  npm run test:bun            # tests, ~2x faster here
  npm run validate:bun        # dry-run boot wiring on Bun
  ```
  `NodeRuntime` is used on Bun
  (dynamic import in `src/index.ts`), worker pool verified working.
- **Deno** (experimental): `deno.json` maps `@/` (trailing-slash import-map
  form — Deno has no TS `paths` wildcards) and enables `sloppy-imports` for
  our extensionless + directory imports; tasks are `deno task dev|start|validate`
  (run with `--allow-all` baked in), `deno task check` (`deno check`), and
  `deno task test` (runs the Vitest suite via Node, not the Deno runtime). `src/index.ts` avoids evaluating
  `@effect/platform-node` on Deno (dynamic import on Node/Bun only) and runs
  via `runWithDenoSignals` (`src/core/platform.ts`), which forks and
  interrupts on `SIGINT`/`SIGTERM` so the drain + OTel flush path is shared.
  Caveats: `node:worker_threads`/`node:os` are Deno-covered for our subset —
  pool spawn failures fall back to in-process; OTel context via
  `node:async_hooks` is partial on Deno, so cross-span correlation may degrade.

## Layout — framework vs app

- `src/core/` – framework, do not put business logic here:
  - `core/functions/registry.ts` – `makeFunction`, `FunctionRegistry`
  - `core/workflows/definition.ts`, `runner.ts`, `bundle.ts` – DAG validation + parallel runner + `WorkflowBundle` / `makeBundle` (workflow owns its functions + triggers, statically checked)
  - `core/runtime/bus.ts`, `service.ts`, `run-id.ts` – Queue/PubSub bus + runtime workers, drain, typed failures, UUID run ids
  - `core/runtime/worker-pool.ts`, `function-worker.ts` – thread pool (opt-in true parallelism)
  - `core/triggers/trigger.ts` – the `Trigger` signature every trigger follows (`tag` + scoped `start`)
  - `core/config.ts`, `core/otel.ts`, `core/logging.ts` – env config, OTel SDK layer, log-level layer
  - `core/platform.ts` – runtime detection (`node`/`bun`/`deno`) + Deno signal runner
- `src/functions/` – **put your functions here**, one file per function:
  - `greet.ts`, `upper.ts`, `count.ts`, `report.ts` – tracked steps used by the welcome branches (`add.ts` shows the same shape)
  - `index.ts` – barrel, re-export (registration happens via workflow bundles)
- `src/triggers/` – **all triggers live here**, one file per trigger kind:
  - `cron.ts` – built-in schedule trigger (`makeCronTrigger`) + usage example
  - `once.ts` – built-in fire-once trigger (`makeOnceTrigger`) + usage example
  - `index.ts` – barrel, re-export
- `src/workflows/` – **put your workflows here**, one file per workflow bundle:
  - `welcome.ts` – example `welcomeBundle`: `greet` diverges into `upper` + `count`, `report` converges both; triggers are cron OR once
  - `index.ts` – barrel, append bundles to `workflowBundles` (derives `exampleWorkflows` / `exampleFunctions`)
- `src/index.ts` – composition root only (boots every bundle in `workflowBundles`, no per-workflow wiring)
- `test/` – Vitest + `@effect/vitest` suites per module
- **Dependency rule** (locked by `test/deps.test.ts`): code depends inward —
  `workflows` → `triggers` + `functions`, triggers/functions → nothing
  app-level (they take the workflow object as a parameter), `core` → `core`
  only. Runtime data flows the other way (trigger → bus → workflow →
  functions), decoupled by workflow name — so a trigger never imports a
  workflow and nothing ever bypasses the bundle.

Internal imports use the `@/` alias for `src/` (extensionless, e.g.
`@/core/functions/registry`), mapped in `tsconfig.json` and resolved by Vitest,
tsup (build) and tsx (dev).

## Guides

### Add a function — `src/functions/<name>.ts`

Scaffold it: `npm run scaffold -- function <name>` (creates the file +
barrel export; you still add it to your workflow bundle's `functions`).

```ts
import { Effect } from "effect"
import { makeFunction } from "@/core/functions/registry"

export const myFn = makeFunction("my-fn", (input: { who: string }) =>
  Effect.gen(function* () {
    yield* Effect.log(`hello ${input.who}`)
    return `hi ${input.who}`
  }),
)
```

Then re-export from `src/functions/index.ts` (`export * from "./<name>"`)
and list the function in your workflow's bundle (`functions: [...]`).
Name must match `fn:` used in workflows.

### Add a workflow — `src/workflows/<name>.ts`

Scaffold it: `npm run scaffold -- workflow <name> [--fn greet]` (creates the
bundle file and registers it in `workflowBundles`; you still adjust nodes).

A workflow file owns its bundle: the DAG plus the functions it calls
and the triggers that start it. Triggers act as an OR — the same
function sequence runs whichever way fires first. Use `makeBundle`
so TypeScript statically rejects nodes referencing functions that are
not listed in `functions`, and pass the workflow object (not a magic
string) to trigger factories.

Every step must be a tracked registry function: workflows declare DAG
wiring and input plumbing only — there is no place to inline work, so
nothing escapes tracing (`function.<name>` spans, registry lookup).
The `input` mappers (`(outputs) => ({...})`) only reshape data between
steps; annotate their param as `ReadonlyMap<string, unknown>` (required
alongside `as const`).

Branches diverge and converge through `dependsOn`: nodes sharing a
dependency run in parallel in the same level, and a node with several
dependencies joins them (see `welcome.ts`: `greet` → `upper` + `count`
→ `report`). DAG cycles, duplicates, and unknown dependencies fail
fast in `planWorkflow`.

Conditional branches use `when` gates over already-completed outputs:

```ts
{ id: "notify", fn: "notify", dependsOn: ["check"],
  when: (outputs) => outputs.get("check") === "ok", ... }
```

Gates run after all previous levels finish, so they only see earlier
levels (same-level nodes run in parallel). `false` skips the node —
no registry lookup, no call — but still traced via a
`workflow.<name>.node.<id>.skipped` span and log. Skipping propagates
transitively to dependents (their inputs would be incomplete), so
gating one early node early-stops the rest of the run as a Succeeded
run with partial outputs. Skipped ids are returned with outputs and on
the `RunSucceeded` event (`skipped: [...]`, empty when everything ran).
Like `input` mappers, `when` is data plumbing: keep it pure and total,
since a throw fails the run.

```ts
import { Config, Effect } from "effect"
import { makeBundle } from "@/core/workflows/bundle"
import type { WorkflowDef } from "@/core/workflows/definition"
import { myFn } from "@/functions/<name>"
import { otherFn } from "@/functions/<other-name>"
import { makeCronTrigger } from "@/triggers/cron"
import { makeOnceTrigger } from "@/triggers/once"

// Each workflow owns its trigger config (prefixed env, e.g. PocketBase URL).
const schedule = Config.string("MYFLOW_CRON_EXPRESSION").pipe(
  Config.withDefault("*/1 * * * *"),
)

export const myWorkflow = {
  name: "my-flow",
  nodes: [
    { id: "a", fn: "my-fn", input: { who: "world" } },
    { id: "b", fn: "other-fn", dependsOn: ["a"], input: (outputs) => ({ prev: outputs.get("a") }) },
  ],
} as const satisfies WorkflowDef

export const myBundle = makeBundle({
  workflow: myWorkflow,
  functions: [myFn, otherFn],
  makeTriggers: () =>
    Effect.gen(function* () {
      const cronSchedule = yield* schedule
      return yield* Effect.all([
        makeCronTrigger({ schedule: cronSchedule, workflow: myWorkflow }),
        Effect.succeed(makeOnceTrigger({ workflow: myWorkflow })),
      ])
    }),
}
```

Then append `myBundle` to `workflowBundles` in `src/workflows/index.ts`.
`src/index.ts` boots every bundle generically — no per-workflow wiring
in `main`. Levels run sequentially, nodes in a level run in parallel via
`Effect.all` (bounded by `WORKFLOW_CONCURRENCY`, default 32). Nodes
without explicit `input` receive the trigger's run `input`; per-node
timeout (`WORKFLOW_NODE_TIMEOUT_MS`, `0` = disabled) and retries
(`WORKFLOW_RETRY_ATTEMPTS`) are opt-in.

### Add a trigger — `src/triggers/<kind>.ts`

Scaffold it: `npm run scaffold -- trigger <kind>` (creates the file + barrel
export; you still reference it in your workflow bundle's `makeTriggers`).

A trigger is any module that follows the `Trigger` signature from
`@/core/triggers/trigger` (`tag` + scoped `start` that `submitRun`s).
Built-ins (`cron.ts`, `once.ts`) double as examples — copy one to add
a kind (webhook, queue consumer, ...), re-export from
`src/triggers/index.ts`, and reference it in your workflow's
`makeTriggers` alongside the others (OR semantics). For external
subscriptions (PocketBase realtime, SSE, ...), use
`makeSubscriptionTrigger` from the same module: its `subscribe`
returns the release Effect, so scope close (SIGINT/SIGTERM) always
unsubscribes before the run-queue drain — see
`test/subscription-trigger.test.ts`.

Static checks guard the wiring end to end: `makeBundle` rejects
unknown `fn:` names, trigger factories accept the workflow object so
renames stay in sync, and fallible constructors (e.g. cron parsing)
expose a typed Effect error channel (`Cron.ParseError`) instead of
throwing — `test/bundle.test.ts` locks all three in.

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
- `node` – `npm run test` + `npm run build` + `node dist/validate.js` + boot smoke test
- `bun` – `bun node_modules/vitest/vitest.mjs run` + `bun src/validate.ts` + boot smoke test
- `deno` – `deno task test` + `deno run --allow-all src/validate.ts` + boot smoke test (after `deno install`)

Static checks (`format`, `typescript`) run once on Node — `tsc` is a native
(Go) binary there, so per-runtime typechecking is redundant. Each runtime
job (`node`, `bun`, `deno`) runs the same Vitest suite plus a boot smoke
test: note `deno task test` invokes the suite via Node (Vitest cannot run
on the Deno runtime itself); Deno-runtime coverage comes from the smoke
boot, which runs `deno run --allow-all src/index.ts`.

Each smoke test boots the real service, waits for the `started: cron` log,
then `SIGTERM`s it and asserts exit `0` plus `Shutdown complete` (graceful
drain path). Smoke entrypoints differ per runtime (`node dist/index.js` on
Node, `bun src/index.ts` on Bun, `deno run --allow-all src/index.ts` on Deno;
Deno polls longer for boot).

## License

Dual-licensed under [MIT](LICENSE-MIT) and [Apache 2.0](LICENSE-APACHE).
