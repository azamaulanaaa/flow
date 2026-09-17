// Scaffold generator for the file + barrel + bundle checklist.
// Usage:
//   node scripts/scaffold.mjs function <kebab-name>
//   node scripts/scaffold.mjs trigger <kebab-name>
//   node scripts/scaffold.mjs workflow <kebab-name> [--fn greet]
//
// Exits non-zero without writing anything when inputs are invalid or a
// target file already exists. Generated code follows repo prettier style.

import { existsSync, readFileSync, writeFileSync } from "node:fs"
import path from "node:path"
import { fileURLToPath } from "node:url"

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..")
const src = (p) => path.join(root, "src", p)

const fail = (message) => {
  console.error(`scaffold: ${message}`)
  process.exit(1)
}

const kebab = (value) => /^[a-z][a-z0-9-]*$/.test(value)
const camel = (kebabName) =>
  kebabName.replace(/-([a-z0-9])/g, (_, c) => c.toUpperCase())
const pascal = (kebabName) => camel(kebabName).replace(/^./, (c) => c.toUpperCase())

const writeNew = (file, content) => {
  if (existsSync(file)) {
    fail(`${path.relative(root, file)} already exists`)
  }
  writeFileSync(file, content)
  console.log(`created ${path.relative(root, file)}`)
}

/** Insert `export * from "./name"` sorted among leading barrel exports. */
const addBarrelExport = (barrel, name) => {
  const lines = readFileSync(barrel, "utf8").split("\n")
  const line = `export * from "./${name}"`
  if (lines.includes(line)) {
    return
  }
  let end = 0
  while (end < lines.length && lines[end].startsWith("export * from")) {
    end += 1
  }
  const exports = [...lines.slice(0, end), line].sort()
  writeFileSync(barrel, [...exports, ...lines.slice(end)].join("\n"))
  console.log(`updated ${path.relative(root, barrel)}`)
}

const functionTemplate = (kebabName) => {
  const c = camel(kebabName)
  const p = pascal(kebabName)
  return `import { Effect } from "effect"
import { makeFunction } from "@/core/functions/registry"

export interface ${p}Input {
  readonly name: string
}

export const ${c}Function = makeFunction("${kebabName}", (input: ${p}Input) =>
  Effect.gen(function* () {
    yield* Effect.log(\`Running ${kebabName} for \${input.name}\`)
    return \`Hello, \${input.name}!\`
  }),
)
`
}

const triggerTemplate = (kebabName) => {
  const p = pascal(kebabName)
  return `import { Effect } from "effect"
import { submitRun } from "@/core/runtime/bus"
import { newRunId } from "@/core/runtime/run-id"
import type { Trigger } from "@/core/triggers/trigger"
import type { WorkflowDef } from "@/core/workflows/definition"

export interface ${p}TriggerOptions {
  /**
   * Workflow to run. Pass the workflow definition object
   * (preferred — typo-proof and rename-safe) or a registered name.
   */
  readonly workflow: string | WorkflowDef
  readonly input?: unknown
}

const workflowNameOf = (workflow: string | WorkflowDef): string =>
  typeof workflow === "string" ? workflow : workflow.name

/**
 * Built-in ${kebabName} trigger: fires a single run when started.
 * Combine it with other triggers in a workflow bundle's
 * \`makeTriggers\` — triggers act as an OR.
 */
export const make${p}Trigger = (options: ${p}TriggerOptions): Trigger => {
  const workflow = workflowNameOf(options.workflow)
  return {
    tag: "${kebabName}",
    start: Effect.asVoid(
      Effect.forkScoped(
        submitRun({
          runId: newRunId(workflow),
          workflow,
          trigger: "${kebabName}",
          input: options.input,
        }).pipe(Effect.withSpan(\`trigger.${kebabName}.\${workflow}\`)),
      ),
    ),
  }
}
`
}

/** Find the `export const X = makeFunction("name"` binding in a function file. */
const functionExportOf = (fnName) => {
  const file = src(path.join("functions", `${fnName}.ts`))
  if (!existsSync(file)) {
    fail(`--fn "${fnName}" has no file src/functions/${fnName}.ts`)
  }
  const content = readFileSync(file, "utf8")
  const match = content.match(new RegExp(`export const (\\w+) = makeFunction\\("${fnName}"`))
  if (match === null) {
    fail(`src/functions/${fnName}.ts does not export makeFunction("${fnName}")`)
  }
  return match[1]
}

const workflowTemplate = (kebabName, fnName, fnExport) => {
  const c = camel(kebabName)
  const fnImport = `@/functions/${fnName}`
  return `import { Effect } from "effect"
import { makeBundle } from "@/core/workflows/bundle"
import type { WorkflowDef } from "@/core/workflows/definition"
import { ${fnExport} } from "${fnImport}"
import { makeOnceTrigger } from "@/triggers/once"

export const ${c}Workflow = {
  name: "${kebabName}",
  nodes: [{ id: "step", fn: "${fnName}", input: { name: "world" } }],
} as const satisfies WorkflowDef

export const ${c}Bundle = makeBundle({
  workflow: ${c}Workflow,
  functions: [${fnExport}],
  makeTriggers: () => Effect.succeed([makeOnceTrigger({ workflow: ${c}Workflow })]),
})
`
}

/** Register a bundle in src/workflows/index.ts (export + import + array). */
const registerBundle = (kebabName) => {
  const c = camel(kebabName)
  const bundle = `${c}Bundle`
  const index = src(path.join("workflows", "index.ts"))
  let content = readFileSync(index, "utf8")
  const exportLine = `export * from "./${kebabName}"`
  if (!content.includes(exportLine)) {
    const lines = content.split("\n")
    let end = 0
    while (end < lines.length && lines[end].startsWith("export * from")) {
      end += 1
    }
    const exports = [...lines.slice(0, end), exportLine].sort()
    content = [...exports, ...lines.slice(end)].join("\n")
  }
  const importLine = `import { ${bundle} } from "./${kebabName}"`
  if (!content.includes(importLine)) {
    const lines = content.split("\n")
    let lastImport = -1
    lines.forEach((line, i) => {
      if (line.startsWith("import ")) {
        lastImport = i
      }
    })
    lines.splice(lastImport + 1, 0, importLine)
    content = lines.join("\n")
  }
  const arrayPattern = /(export const workflowBundles[^=]*=\s*\[)([^\]]*)(\])/
  const match = content.match(arrayPattern)
  if (match === null) {
    fail("could not find workflowBundles array in src/workflows/index.ts")
  }
  if (!match[2].split(",").map((s) => s.trim()).includes(bundle)) {
    const items = match[2].trim().length === 0 ? bundle : `${match[2].replace(/\s+$/, "")}, ${bundle}`
    content = content.replace(arrayPattern, `$1${items}$3`)
  }
  writeFileSync(index, content)
  console.log(`updated ${path.relative(root, index)}`)
}

const [kind, name, ...rest] = process.argv.slice(2)
if (kind === undefined || name === undefined) {
  fail("usage: node scripts/scaffold.mjs <function|trigger|workflow> <kebab-name> [--fn greet]")
}
if (!kebab(name)) {
  fail(`"${name}" must be kebab-case: lower-case letters, digits, dashes`)
}

if (kind === "function") {
  const file = src(path.join("functions", `${name}.ts`))
  writeNew(file, functionTemplate(name))
  addBarrelExport(src(path.join("functions", "index.ts")), name)
  console.log(`next: add ${camel(name)}Function to your workflow bundle's functions, then npm run validate`)
} else if (kind === "trigger") {
  const file = src(path.join("triggers", `${name}.ts`))
  writeNew(file, triggerTemplate(name))
  addBarrelExport(src(path.join("triggers", "index.ts")), name)
  console.log(`next: reference make${pascal(name)}Trigger in your workflow bundle's makeTriggers, then npm run validate`)
} else if (kind === "workflow") {
  const fnFlag = rest.indexOf("--fn")
  const fnName = fnFlag === -1 ? "greet" : rest[fnFlag + 1]
  if (fnName === undefined || !kebab(fnName)) {
    fail(`--fn needs a kebab-case function name (got "${rest[fnFlag + 1] ?? ""}")`)
  }
  const fnExport = functionExportOf(fnName)
  const file = src(path.join("workflows", `${name}.ts`))
  writeNew(file, workflowTemplate(name, fnName, fnExport))
  addBarrelExport(src(path.join("workflows", "index.ts")), name)
  registerBundle(name)
  console.log(`next: adjust nodes/inputs in src/workflows/${name}.ts, then npm run validate`)
} else {
  fail(`unknown kind "${kind}" (expected function, trigger, or workflow)`)
}
