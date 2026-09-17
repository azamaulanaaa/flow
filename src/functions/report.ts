import { Effect } from "effect"
import { makeFunction } from "@/core/functions/registry"

export interface ReportInput {
  readonly upper: string
  readonly count: number
}

export const reportFunction = makeFunction("report", (input: ReportInput) =>
  Effect.gen(function* () {
    yield* Effect.log(`Reporting ${input.count} chars`)
    return `${input.upper} (${input.count} chars)`
  }),
)
