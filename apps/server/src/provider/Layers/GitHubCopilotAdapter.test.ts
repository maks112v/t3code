import * as NodeAssert from "node:assert/strict";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import {
  type CopilotClient,
  type CopilotSession,
  type MessageOptions,
  type SessionConfig,
  type SessionEvent,
} from "@github/copilot-sdk";
import {
  GitHubCopilotSettings,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { ServerConfig } from "../../config.ts";
import { makeGitHubCopilotAdapter } from "./GitHubCopilotAdapter.ts";

const decodeSettings = Schema.decodeSync(GitHubCopilotSettings);

it.effect("starts a session, sends a turn, and maps streaming events", () =>
  Effect.scoped(
    Effect.gen(function* () {
      let sessionConfig: SessionConfig | undefined;
      let sentMessage: MessageOptions | undefined;
      let handler: ((event: SessionEvent) => void) | undefined;

      const copilotSession = {
        sessionId: "copilot-session-1",
        on: (next: (event: SessionEvent) => void) => {
          handler = next;
          return () => {
            handler = undefined;
          };
        },
        send: async (message: MessageOptions) => {
          sentMessage = message;
          handler?.({
            type: "assistant.message_delta",
            id: "event-delta",
            timestamp: "2026-08-09T12:00:00.000Z",
            data: { messageId: "message-1", deltaContent: "Hello" },
          } as SessionEvent);
          handler?.({
            type: "assistant.message",
            id: "event-message",
            timestamp: "2026-08-09T12:00:00.001Z",
            data: { messageId: "message-1", content: "Hello" },
          } as SessionEvent);
          handler?.({
            type: "session.idle",
            id: "event-idle",
            timestamp: "2026-08-09T12:00:00.002Z",
            data: { aborted: false },
          } as SessionEvent);
          return "message-1";
        },
        abort: async () => undefined,
        disconnect: async () => undefined,
        getEvents: async () => [],
      } as unknown as CopilotSession;
      const client = {
        start: async () => undefined,
        stop: async () => [],
        createSession: async (config: SessionConfig) => {
          sessionConfig = config;
          return copilotSession;
        },
      } as unknown as CopilotClient;

      const adapter = yield* makeGitHubCopilotAdapter(decodeSettings({ enabled: true }), {
        instanceId: ProviderInstanceId.make("githubCopilot"),
        clientFactory: () => client,
      });
      const eventsFiber = yield* Stream.take(adapter.streamEvents, 7).pipe(
        Stream.runCollect,
        Effect.forkChild,
      );
      const threadId = ThreadId.make("copilot-thread");

      const session = yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("githubCopilot"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: {
          instanceId: ProviderInstanceId.make("githubCopilot"),
          model: "claude-sonnet-4.6",
          options: [{ id: "reasoningEffort", value: "high" }],
        },
      });
      yield* adapter.sendTurn({ threadId, input: "Say hello", attachments: [] });

      const events = Array.from(yield* Fiber.join(eventsFiber));
      NodeAssert.equal(session.resumeCursor && typeof session.resumeCursor, "object");
      NodeAssert.equal(sessionConfig?.model, "claude-sonnet-4.6");
      NodeAssert.equal(sessionConfig?.reasoningEffort, "high");
      NodeAssert.equal(sentMessage?.prompt, "Say hello");
      NodeAssert.deepEqual(
        events.map((event) => event.type),
        [
          "session.started",
          "thread.started",
          "turn.started",
          "content.delta",
          "item.completed",
          "turn.completed",
          "session.state.changed",
        ],
      );
    }),
  ).pipe(
    Effect.provide(
      ServerConfig.layerTest(process.cwd(), process.cwd()).pipe(Layer.provide(NodeServices.layer)),
    ),
  ),
);
