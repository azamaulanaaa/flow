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
OTLP `.../v1/logs` when `OTEL_EXPORTER_OTLP_ENDPOINT` is set) and as span
events on the enclosing span — giving both searchable streams and correlated
waterfall context.

## Scripts

```sh
npm install
npm run typecheck
npm run test
npm run build
npm start
```

> Note: if `npm install` fails with `EPERM ... symlink` in a restricted
> sandbox, retry with `npm install --no-bin-links` and invoke the compiler
> directly via `node node_modules/typescript/lib/tsc.js`.

## Configuration (env)

| Var | Default | Description |
| --- | ------- | ----------- |
| `SERVICE_NAME` | `workflow-runner` | OTel service name |
| `LOG_LEVEL` | `INFO` | log level |
| `QUEUE_CAPACITY` | `128` | run-queue bound (backpressure) |
| `CRON_EXPRESSION` | `*/1 * * * *` | schedule for the bundled cron trigger |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | _(unset)_ | OTLP HTTP endpoint; unset = console exporter |
| `SHUTDOWN_TIMEOUT_MS` | `10000` | max wait to drain queued + in-flight runs on SIGINT/SIGTERM |

## Shutdown

On `SIGINT`/`SIGTERM` (`NodeRuntime.runMain` interrupts `Effect.never`):
1. trigger + worker fibers stop in LIFO scope order (no new runs accepted),
2. a finalizer drains the queue and in-flight runs via `waitForIdle`,
   bounded by `SHUTDOWN_TIMEOUT_MS`,
3. scope release flushes OTel Batch processors (spans + log records).

Logs show `Shutdown requested (...)` then `Shutdown complete, flushing telemetry`.

## Layout — framework vs app

- `src/core/` – framework, do not put business logic here:
  - `core/functions/registry.ts` – `makeFunction`, `FunctionRegistry`
  - `core/workflows/definition.ts`, `runner.ts` – DAG validation + parallel runner
  - `core/runtime/bus.ts`, `service.ts` – Queue/PubSub bus + worker pool
  - `core/triggers/trigger.ts`, `cron.ts` – `Trigger` interface + impls
  - `core/config.ts`, `core/otel.ts` – env config, OTel SDK layer
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
Levels run sequentially, nodes in a level run in parallel via `Effect.all`.

### Add a trigger

Use `makeCronTrigger({ schedule, workflow })` or `makeOnceTrigger({ workflow })`
from `@/core/triggers/cron`. Wire `trigger.start` in `src/index.ts` (scoped).

## License

Dual-licensed under [MIT](LICENSE-MIT) and [Apache 2.0](LICENSE-APACHE).
