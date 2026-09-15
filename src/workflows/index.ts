export * from "./welcome"
import type { WorkflowDef } from "@/core/workflows/definition"
import { welcomeWorkflow } from "./welcome"

/** All user workflows registered with the runtime. Add yours here. */
export const exampleWorkflows: ReadonlyArray<WorkflowDef> = [welcomeWorkflow]
