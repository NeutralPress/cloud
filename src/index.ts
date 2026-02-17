import { app } from "./app";
import { handleQueue, handleScheduled } from "./worker-handlers";
import type { DispatchMessage, EnvBindings } from "./types";

export default {
  fetch: app.fetch,

  async scheduled(controller: ScheduledController, env: EnvBindings, ctx: ExecutionContext) {
    ctx.waitUntil(handleScheduled(controller, env));
  },

  async queue(batch: MessageBatch<DispatchMessage>, env: EnvBindings) {
    await handleQueue(batch, env);
  },
} satisfies ExportedHandler<EnvBindings, DispatchMessage>;
