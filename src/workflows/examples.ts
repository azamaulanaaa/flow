import type { WorkflowDef } from "./definition.js"

export const welcomeWorkflow: WorkflowDef = {
  name: "welcome",
  nodes: [{ id: "greet", fn: "greet", input: { name: "world" } }],
}

export const exampleWorkflows: ReadonlyArray<WorkflowDef> = [welcomeWorkflow]
