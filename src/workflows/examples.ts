import type { WorkflowDef } from "@/workflows/definition"

export const welcomeWorkflow: WorkflowDef = {
  name: "welcome",
  nodes: [{ id: "greet", fn: "greet", input: { name: "world" } }],
}

export const exampleWorkflows: ReadonlyArray<WorkflowDef> = [welcomeWorkflow]
