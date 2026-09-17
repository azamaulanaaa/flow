import { describe, expect, it } from "@effect/vitest"
import { Effect } from "effect"
import type { AppConfig } from "@/core/config"
import {
  bundleFunctions,
  bundleWorkflows,
  makeBundleTriggers,
} from "@/core/workflows/bundle"
import { exampleFunctions, exampleWorkflows, workflowBundles } from "@/workflows"

const testConfig: AppConfig = {
  serviceName: "test",
  logLevel: "ERROR",
  queueCapacity: 16,
  runtimeWorkers: 1,
  workflowConcurrency: 8,
  workflowNodeTimeoutMs: 0,
  workflowRetryAttempts: 0,
  cronExpression: "* * * * * *",
  otlpEndpoint: undefined,
  shutdownTimeoutMs: 1000,
  workerPoolEnabled: false,
  workerPoolSize: 1,
  workerPoolTimeoutMs: 1000,
}

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
        // Triggers build from config without throwing.
        const triggers = yield* bundle.makeTriggers(testConfig)
        expect(triggers.length).toBeGreaterThan(0)
      }
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
      const triggers = yield* makeBundleTriggers(workflowBundles, testConfig)
      const tags = triggers.map((t) => t.tag)
      // Welcome bundle's cron trigger is present, owned by the workflow.
      expect(tags).toContain("cron:* * * * * *")
    }),
  )
})
