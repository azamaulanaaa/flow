import { describe, expect, it } from "@effect/vitest"
import { Effect } from "effect"
import { FunctionRegistryLive, makeFunction } from "@/core/functions/registry"
import { planWorkflow, type WorkflowDef } from "@/core/workflows/definition"
import { runWorkflow } from "@/core/workflows/runner"

const constFn = (name: string, value: unknown) => makeFunction(name, () => Effect.succeed(value))

const TestRegistryLive = FunctionRegistryLive([
  constFn("a", "A"),
  constFn("b", "B"),
  makeFunction("concat", (input: { parts: Array<string> }) =>
    Effect.succeed(input.parts.join("+")),
  ),
])

const diamond: WorkflowDef = {
  name: "diamond",
  nodes: [
    { id: "left", fn: "a" },
    { id: "right", fn: "b" },
    {
      id: "join",
      fn: "concat",
      dependsOn: ["left", "right"],
      input: (outputs: ReadonlyMap<string, unknown>) => ({
        parts: [outputs.get("left"), outputs.get("right")],
      }),
    },
  ],
}

describe("planWorkflow", () => {
  it.effect("computes parallel levels for a diamond DAG", () =>
    Effect.gen(function* () {
      const levels = yield* planWorkflow(diamond)
      expect(levels.length).toBe(2)
      expect(levels[0]!.map((n) => n.id).sort()).toEqual(["left", "right"])
      expect(levels[1]!.map((n) => n.id)).toEqual(["join"])
    }),
  )

  it.effect("rejects dependency cycles", () =>
    Effect.gen(function* () {
      const cyclic: WorkflowDef = {
        name: "cyclic",
        nodes: [
          { id: "x", fn: "a", dependsOn: ["y"] },
          { id: "y", fn: "b", dependsOn: ["x"] },
        ],
      }
      const error = yield* Effect.flip(planWorkflow(cyclic))
      expect(error._tag).toBe("WorkflowDefinitionError")
      expect(error.reason).toContain("cycle")
    }),
  )

  it.effect("rejects unknown dependencies", () =>
    Effect.gen(function* () {
      const broken: WorkflowDef = {
        name: "broken",
        nodes: [{ id: "x", fn: "a", dependsOn: ["ghost"] }],
      }
      const error = yield* Effect.flip(planWorkflow(broken))
      expect(error._tag).toBe("WorkflowDefinitionError")
      expect(error.reason).toContain("ghost")
    }),
  )
})

describe("runWorkflow", () => {
  it.effect("runs levels in order and stitches outputs into downstream input", () =>
    Effect.gen(function* () {
      const { outputs, skipped } = yield* runWorkflow(diamond)
      expect(outputs.get("left")).toBe("A")
      expect(outputs.get("right")).toBe("B")
      expect(outputs.get("join")).toBe("A+B")
      expect(skipped).toEqual([])
    }).pipe(Effect.provide(TestRegistryLive)),
  )
})
