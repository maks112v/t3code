import { CopilotClient, type CopilotClientOptions, type ModelInfo } from "@github/copilot-sdk";
import {
  type GitHubCopilotSettings,
  type ModelCapabilities,
  type ServerProviderModel,
} from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import * as Data from "effect/Data";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";

import {
  buildSelectOptionDescriptor,
  buildServerProvider,
  providerModelsFromSettings,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";

const PRESENTATION = {
  displayName: "GitHub Copilot",
  badgeLabel: "Proof of Concept",
  showInteractionModeToggle: true,
} as const;

const EMPTY_CAPABILITIES: ModelCapabilities = createModelCapabilities({ optionDescriptors: [] });

class GitHubCopilotProbeError extends Data.TaggedError("GitHubCopilotProbeError")<{
  readonly cause: unknown;
}> {}

export interface GitHubCopilotClientFactory {
  (options: CopilotClientOptions): Pick<CopilotClient, "start" | "stop" | "listModels">;
}

function modelCapabilities(model: ModelInfo): ModelCapabilities {
  const efforts = model.supportedReasoningEfforts ?? [];
  if (efforts.length === 0) return EMPTY_CAPABILITIES;
  return createModelCapabilities({
    optionDescriptors: [
      buildSelectOptionDescriptor({
        id: "reasoningEffort",
        label: "Reasoning effort",
        options: efforts.map((effort) => ({
          value: effort,
          label: effort,
          isDefault: effort === model.defaultReasoningEffort,
        })),
      }),
    ],
  });
}

function mapModels(
  models: ReadonlyArray<ModelInfo>,
  customModels: ReadonlyArray<string>,
): ReadonlyArray<ServerProviderModel> {
  const discovered = models
    .filter((model) => model.policy?.state !== "disabled")
    .map(
      (model) =>
        ({
          slug: model.id,
          name: model.name.trim() || model.id,
          isCustom: false,
          capabilities: modelCapabilities(model),
        }) satisfies ServerProviderModel,
    );
  return providerModelsFromSettings(discovered, customModels, EMPTY_CAPABILITIES);
}

export const makePendingGitHubCopilotProvider = Effect.fn("makePendingGitHubCopilotProvider")(
  function* (settings: GitHubCopilotSettings): Effect.fn.Return<ServerProviderDraft> {
    const checkedAt = DateTime.formatIso(yield* DateTime.now);
    return buildServerProvider({
      presentation: PRESENTATION,
      enabled: settings.enabled,
      checkedAt,
      models: providerModelsFromSettings([], settings.customModels, EMPTY_CAPABILITIES),
      probe: settings.enabled
        ? {
            installed: true,
            version: null,
            status: "warning",
            auth: { status: "unknown" },
            message: "Checking GitHub Copilot availability...",
          }
        : {
            installed: false,
            version: null,
            status: "warning",
            auth: { status: "unknown" },
            message: "GitHub Copilot is disabled in T3 Code settings.",
          },
    });
  },
);

export const checkGitHubCopilotProviderStatus = Effect.fn("checkGitHubCopilotProviderStatus")(
  function* (
    settings: GitHubCopilotSettings,
    environment: NodeJS.ProcessEnv = process.env,
    clientFactory: GitHubCopilotClientFactory = (options) => new CopilotClient(options),
  ): Effect.fn.Return<ServerProviderDraft> {
    const checkedAt = DateTime.formatIso(yield* DateTime.now);
    if (!settings.enabled) {
      return yield* makePendingGitHubCopilotProvider(settings);
    }

    const client = clientFactory({
      env: environment,
      ...(settings.homePath ? { baseDirectory: settings.homePath } : {}),
    });
    const probe = yield* Effect.tryPromise({
      try: async () => {
        await client.start();
        return await client.listModels();
      },
      catch: (cause) => new GitHubCopilotProbeError({ cause }),
    }).pipe(Effect.ensuring(Effect.promise(() => client.stop()).pipe(Effect.ignore)), Effect.exit);

    if (Exit.isFailure(probe)) {
      return buildServerProvider({
        presentation: PRESENTATION,
        enabled: true,
        checkedAt,
        models: providerModelsFromSettings([], settings.customModels, EMPTY_CAPABILITIES),
        probe: {
          installed: true,
          version: null,
          status: "error",
          auth: { status: "unauthenticated" },
          message:
            "GitHub Copilot could not start. Run `copilot login` or provide COPILOT_GITHUB_TOKEN, then refresh.",
        },
      });
    }

    return buildServerProvider({
      presentation: PRESENTATION,
      enabled: true,
      checkedAt,
      models: mapModels(probe.value, settings.customModels),
      probe: {
        installed: true,
        version: null,
        status: "ready",
        auth: { status: "authenticated" },
        message: "GitHub Copilot is ready.",
      },
    });
  },
);
