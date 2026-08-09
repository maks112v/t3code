import {
  GitHubCopilotSettings,
  ProviderDriverKind,
  TextGenerationError,
  type ServerProvider,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import type * as TextGeneration from "../../textGeneration/TextGeneration.ts";
import { ProviderDriverError } from "../Errors.ts";
import { makeGitHubCopilotAdapter } from "../Layers/GitHubCopilotAdapter.ts";
import {
  checkGitHubCopilotProviderStatus,
  makePendingGitHubCopilotProvider,
} from "../Layers/GitHubCopilotProvider.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "../ProviderDriver.ts";
import type { ServerProviderDraft } from "../providerSnapshot.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import { makeManualOnlyProviderMaintenanceCapabilities } from "../providerMaintenance.ts";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "../providerUpdateSettings.ts";

const DRIVER_KIND = ProviderDriverKind.make("githubCopilot");
const decodeSettings = Schema.decodeSync(GitHubCopilotSettings);

export type GitHubCopilotDriverEnv =
  | BackgroundPolicy.BackgroundPolicy
  | ServerConfig
  | ServerSettingsService;

const unavailableTextGeneration: TextGeneration.TextGeneration["Service"] = {
  generateCommitMessage: (input) =>
    Effect.fail(
      new TextGenerationError({
        operation: "generateCommitMessage",
        detail: `GitHub Copilot text generation is not part of this proof of concept (${input.cwd}).`,
      }),
    ),
  generatePrContent: (input) =>
    Effect.fail(
      new TextGenerationError({
        operation: "generatePrContent",
        detail: `GitHub Copilot text generation is not part of this proof of concept (${input.cwd}).`,
      }),
    ),
  generateBranchName: (input) =>
    Effect.fail(
      new TextGenerationError({
        operation: "generateBranchName",
        detail: `GitHub Copilot text generation is not part of this proof of concept (${input.cwd}).`,
      }),
    ),
  generateThreadTitle: (input) =>
    Effect.fail(
      new TextGenerationError({
        operation: "generateThreadTitle",
        detail: `GitHub Copilot text generation is not part of this proof of concept (${input.cwd}).`,
      }),
    ),
};

const withIdentity =
  (input: {
    readonly instanceId: ProviderInstance["instanceId"];
    readonly displayName: string | undefined;
    readonly accentColor: string | undefined;
    readonly continuationGroupKey: string;
  }) =>
  (snapshot: ServerProviderDraft): ServerProvider => ({
    ...snapshot,
    instanceId: input.instanceId,
    driver: DRIVER_KIND,
    ...(input.displayName ? { displayName: input.displayName } : {}),
    ...(input.accentColor ? { accentColor: input.accentColor } : {}),
    continuation: { groupKey: input.continuationGroupKey },
  });

export const GitHubCopilotDriver: ProviderDriver<GitHubCopilotSettings, GitHubCopilotDriverEnv> = {
  driverKind: DRIVER_KIND,
  metadata: {
    displayName: "GitHub Copilot",
    supportsMultipleInstances: true,
  },
  configSchema: GitHubCopilotSettings,
  defaultConfig: () => decodeSettings({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const serverSettings = yield* ServerSettingsService;
      const processEnv = mergeProviderInstanceEnvironment(environment);
      const effectiveConfig = { ...config, enabled } satisfies GitHubCopilotSettings;
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: DRIVER_KIND,
        instanceId,
      });
      const stampIdentity = withIdentity({
        instanceId,
        displayName,
        accentColor,
        continuationGroupKey: continuationIdentity.continuationKey,
      });
      const adapter = yield* makeGitHubCopilotAdapter(effectiveConfig, {
        instanceId,
        environment: processEnv,
      });
      const snapshotSettings = makeProviderSnapshotSettingsSource(effectiveConfig, serverSettings);
      const maintenanceCapabilities = makeManualOnlyProviderMaintenanceCapabilities({
        provider: DRIVER_KIND,
        packageName: "@github/copilot-sdk",
      });
      const snapshot = yield* makeManagedServerProvider<
        ProviderSnapshotSettings<GitHubCopilotSettings>
      >({
        maintenanceCapabilities,
        getSettings: snapshotSettings.getSettings,
        streamSettings: snapshotSettings.streamSettings,
        haveSettingsChanged: haveProviderSnapshotSettingsChanged,
        initialSnapshot: (settings) =>
          makePendingGitHubCopilotProvider(settings.provider).pipe(Effect.map(stampIdentity)),
        checkProvider: checkGitHubCopilotProviderStatus(effectiveConfig, processEnv).pipe(
          Effect.map(stampIdentity),
        ),
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: `Failed to build GitHub Copilot snapshot: ${cause.message}`,
              cause,
            }),
        ),
      );

      return {
        instanceId,
        driverKind: DRIVER_KIND,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        snapshot,
        adapter,
        textGeneration: unavailableTextGeneration,
      } satisfies ProviderInstance;
    }),
};
