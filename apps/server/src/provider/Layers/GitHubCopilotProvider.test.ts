import * as NodeAssert from "node:assert/strict";

import type { CopilotClientOptions, ModelInfo } from "@github/copilot-sdk";
import { it } from "@effect/vitest";
import { GitHubCopilotSettings } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  checkGitHubCopilotProviderStatus,
  type GitHubCopilotClientFactory,
} from "./GitHubCopilotProvider.ts";

const decodeSettings = Schema.decodeSync(GitHubCopilotSettings);

const settings = (overrides?: Partial<GitHubCopilotSettings>) =>
  decodeSettings({ enabled: true, homePath: "", customModels: [], ...overrides });

it.effect("does not start Copilot while the provider is disabled", () =>
  Effect.gen(function* () {
    let created = false;
    const snapshot = yield* checkGitHubCopilotProviderStatus(
      settings({ enabled: false }),
      {},
      (() => {
        created = true;
        throw new Error("disabled providers must not create a client");
      }) as GitHubCopilotClientFactory,
    );

    NodeAssert.equal(created, false);
    NodeAssert.equal(snapshot.enabled, false);
    NodeAssert.match(snapshot.message ?? "", /disabled/i);
  }),
);

it.effect("discovers enabled models and forwards per-instance options", () =>
  Effect.gen(function* () {
    let receivedOptions: CopilotClientOptions | undefined;
    let stopCalls = 0;
    const model = {
      id: "claude-sonnet-4.6",
      name: "Claude Sonnet 4.6",
      capabilities: {},
      policy: { state: "enabled", terms: "" },
      supportedReasoningEfforts: ["low", "high"],
      defaultReasoningEffort: "high",
    } as ModelInfo;
    const factory: GitHubCopilotClientFactory = (options) => {
      receivedOptions = options;
      return {
        start: async () => undefined,
        stop: async () => {
          stopCalls += 1;
          return [];
        },
        listModels: async () => [model],
      };
    };
    const environment = { COPILOT_GITHUB_TOKEN: "test-token" };

    const snapshot = yield* checkGitHubCopilotProviderStatus(
      settings({ homePath: "/tmp/copilot-home", customModels: ["custom-model"] }),
      environment,
      factory,
    );

    NodeAssert.equal(snapshot.status, "ready");
    NodeAssert.equal(snapshot.auth.status, "authenticated");
    NodeAssert.equal(snapshot.requiresNewThreadForModelChange, undefined);
    NodeAssert.deepEqual(
      snapshot.models.map((entry) => entry.slug),
      ["claude-sonnet-4.6", "custom-model"],
    );
    NodeAssert.equal(receivedOptions?.baseDirectory, "/tmp/copilot-home");
    NodeAssert.equal(receivedOptions?.env, environment);
    NodeAssert.equal(stopCalls, 1);

    const reasoning = snapshot.models[0]?.capabilities?.optionDescriptors?.find(
      (descriptor) => descriptor.id === "reasoningEffort" && descriptor.type === "select",
    );
    NodeAssert.ok(reasoning?.type === "select");
    NodeAssert.equal(reasoning.options.find((option) => option.isDefault)?.id, "high");
  }),
);

it.effect("reports startup failures as an authentication error and stops the client", () =>
  Effect.gen(function* () {
    let stopCalls = 0;
    const factory: GitHubCopilotClientFactory = () => ({
      start: async () => {
        throw new Error("not authenticated");
      },
      stop: async () => {
        stopCalls += 1;
        return [];
      },
      listModels: async () => [],
    });

    const snapshot = yield* checkGitHubCopilotProviderStatus(settings(), {}, factory);

    NodeAssert.equal(snapshot.status, "error");
    NodeAssert.equal(snapshot.auth.status, "unauthenticated");
    NodeAssert.equal(stopCalls, 1);
  }),
);
