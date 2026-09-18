import { describe, expect, it } from "@effect/vitest"
import { Effect, Layer } from "effect"
import { expectTypeOf } from "vitest"
import { FunctionRegistryLive, makeFunction } from "@/core/functions/registry"
import {
  bundleFunctions,
  bundleWorkflows,
  makeBundle,
  makeBundleTriggers,
  type WorkflowBundle,
} from "@/core/workflows/bundle"
import { greetFunction } from "@/functions/greet"
import { exampleFunctions, exampleWorkflows, workflowBundles } from "@/workflows"

describe("WorkflowBundles", () => {
  it.effect("each bundle owns its workflow, functions, and triggers", () =>
    Effect.gen(function* () {
      expect(workflowBundles.length).toBeGreaterThan(0)
      for (const bundle of workflowBundles) {
        expect(bundle.workflow.name.length).toBeGreaterThan(0)
        // Every `fn:` referenced by the workflow must be in its function list.
        const provided = new Set(bundle.functions.map((f) => f.name))
        for (const node of bundle.workflow.nodes) {
          expect(provided.has(node.fn)).toBe(true)
        }
        // Triggers build without throwing (each workflow owns its config).
        const triggers = yield* bundle.makeTriggers()
        expect(triggers.length).toBeGreaterThan(0)
      }
    }),
  )

  it.effect("triggers act as an OR over the same function sequence", () =>
    Effect.gen(function* () {
      const triggers = yield* makeBundleTriggers(workflowBundles)
      const tags = triggers.map((t) => t.tag)
      // Welcome runs on every cron tick and once at boot.
      expect(tags).toContain("cron:*/1 * * * *")
      expect(tags).toContain("once")
    }),
  )

  it.effect("derived registries stay in sync with bundles", () =>
    Effect.gen(function* () {
      expect([...exampleWorkflows].map((w) => w.name)).toEqual(
        bundleWorkflows(workflowBundles).map((w) => w.name),
      )
      expect([...exampleFunctions].map((f) => f.name)).toEqual(
        bundleFunctions(workflowBundles).map((f) => f.name),
      )
    }),
  )

  it("statically pins bundle and function-name types", () => {
    expectTypeOf(workflowBundles).toMatchTypeOf<ReadonlyArray<WorkflowBundle>>()
    // Function names stay literal, so `fn:` references are checked.
    expectTypeOf(greetFunction.name).toEqualTypeOf<"greet">()
  })

  it.effect("statically rejects nodes referencing unregistered functions", () =>
    Effect.gen(function* () {
      void makeBundle({
        workflow: {
          name: "bad",
          nodes: [
            {
              id: "a",
              // @ts-expect-error "ghost" is not listed in functions
              fn: "ghost",
            },
          ],
        },
        functions: [greetFunction],
        makeTriggers: () => Effect.succeed([]),
      })
    }),
  )

  it.effect("surfaces conflicting function implementations as duplicates", () =>
    Effect.gen(function* () {
      const b1 = makeBundle({
        workflow: { name: "w1", nodes: [{ id: "a", fn: "shared" }] },
        functions: [makeFunction("shared", () => Effect.succeed(1))],
        makeTriggers: () => Effect.succeed([]),
      })
      const b2 = makeBundle({
        workflow: { name: "w2", nodes: [{ id: "a", fn: "shared" }] },
        functions: [makeFunction("shared", () => Effect.succeed(2))],
        makeTriggers: () => Effect.succeed([]),
      })
      // Shared reference dedupes; conflicting impls are preserved for fail-fast.
      expect(bundleFunctions([b1, b1]).length).toBe(1)
      const conflicted = bundleFunctions([b1, b2])
      expect(conflicted.length).toBe(2)
      const error = yield* Effect.flip(Layer.build(FunctionRegistryLive(conflicted)).pipe(Effect.scoped))
      expect((error as { _tag: string })._tag).toBe("DuplicateFunctionError")
    }),
  )
})
