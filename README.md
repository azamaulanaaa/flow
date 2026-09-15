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

## Layout

- `src/functions/` – `makeFunction` registry (SDK surface, auto-traced)
- `src/workflows/` – DAG definition/validation + parallel runner
- `src/runtime/` – Queue/PubSub bus + worker service
- `src/triggers/` – generic `Trigger` interface, cron + once impls
- `src/config.ts`, `src/otel.ts` – env config, OTel SDK layer
- `test/` – Vitest + `@effect/vitest` suites per module

Internal imports use the `@/` alias for `src/` (extensionless, e.g.
`@/functions/registry`), mapped in `tsconfig.json` and resolved by Vitest,
tsup (build) and tsx (dev).

## License

Dual-licensed under [MIT](LICENSE-MIT) and [Apache 2.0](LICENSE-APACHE).
