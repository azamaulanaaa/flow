export * from "./add"
export * from "./greet"

// NOTE: defining a function here does not register it. Each workflow
// bundle (`src/workflows/*`) lists the functions it calls, and the
// runtime registry is derived from `workflowBundles` in
// `src/workflows/index.ts` — so adding a function means referencing it
// from the workflow bundle that uses it.
