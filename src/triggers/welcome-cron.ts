import type { Effect } from "effect"
import type { Cron } from "effect"
import type { AppConfig } from "@/core/config"
import { makeCronTrigger } from "@/core/triggers/cron"
import type { Trigger } from "@/core/triggers/trigger"

/**
 * Cron trigger for the `welcome` workflow.
 *
 * Put your triggers here, one file per trigger — mirroring `src/functions/`.
 * The owning workflow (`src/workflows/welcome.ts`) references this factory
 * in its bundle; `src/index.ts` never imports triggers directly.
 */
export const makeWelcomeCronTrigger = (
  config: AppConfig,
): Effect.Effect<Trigger, Cron.ParseError> =>
  makeCronTrigger({
    schedule: config.cronExpression,
    workflow: "welcome",
  })
