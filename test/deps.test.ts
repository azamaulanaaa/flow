import { describe, expect, it } from "@effect/vitest"
import { readdirSync, readFileSync } from "node:fs"
import { join } from "node:path"

const SRC = join(process.cwd(), "src")

const listTs = (dir: string): Array<string> => {
  const out: Array<string> = []
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const full = join(dir, entry.name)
    if (entry.isDirectory()) {
      out.push(...listTs(full))
    } else if (entry.name.endsWith(".ts")) {
      out.push(full)
    }
  }
  return out
}

interface ImportStmt {
  readonly clause: string
  readonly spec: string
}

const importStmts = (source: string): Array<ImportStmt> => {
  const matches = source.matchAll(/(import|export)\s*([\s\S]*?)\s*from\s*["']([^"']+)["']/g)
  return [...matches].map((m) => ({ clause: m[2] ?? "", spec: m[3] ?? "" }))
}

/** Whole-statement `import type` is erased at runtime — not a dependency. */
const isTypeOnly = (clause: string): boolean => /^\s*type\b/.test(clause)

// Layer rule: code dependencies point inward. Workflows depend on triggers
// and functions; triggers and functions depend on nothing app-level (they
// take the workflow object as a parameter instead); core depends on core
// only. Runtime data still flows the other way: trigger -> bus -> workflow
// -> functions, decoupled by workflow name.
describe("Layer dependencies", () => {
  it("triggers and functions never import workflows", () => {
    const violations: Array<string> = []
    const files = [...listTs(join(SRC, "triggers")), ...listTs(join(SRC, "functions"))]
    for (const file of files) {
      const source = readFileSync(file, "utf8")
      for (const stmt of importStmts(source)) {
        if (isTypeOnly(stmt.clause)) {
          continue
        }
        if (/^@\/workflows(\/|$)/.test(stmt.spec) || /^\.\.?\/.*workflows/.test(stmt.spec)) {
          violations.push(`${file} -> ${stmt.spec}`)
        }
      }
    }
    expect(violations).toEqual([])
  })

  it("triggers and functions stay independent of each other", () => {
    const violations: Array<string> = []
    const check = (dir: string, forbidden: RegExp): void => {
      for (const file of listTs(join(SRC, dir))) {
        const source = readFileSync(file, "utf8")
        for (const stmt of importStmts(source)) {
          if (isTypeOnly(stmt.clause)) {
            continue
          }
          if (forbidden.test(stmt.spec)) {
            violations.push(`${file} -> ${stmt.spec}`)
          }
        }
      }
    }
    check("triggers", /^@\/functions(\/|$)/)
    check("functions", /^@\/triggers(\/|$)/)
    expect(violations).toEqual([])
  })

  it("core never imports app layers", () => {
    const violations: Array<string> = []
    for (const file of listTs(join(SRC, "core"))) {
      const source = readFileSync(file, "utf8")
      for (const stmt of importStmts(source)) {
        if (isTypeOnly(stmt.clause)) {
          continue
        }
        if (/^@\/(functions|triggers|workflows)(\/|$)/.test(stmt.spec)) {
          violations.push(`${file} -> ${stmt.spec}`)
        }
      }
    }
    expect(violations).toEqual([])
  })
})
