import type { WorkflowDef } from "@/core/workflows/definition"

export const welcomeWorkflow: WorkflowDef = {
  name: "welcome",
  nodes: [{ id: "greet", fn: "greet", input: { name: "world" } }],
}
