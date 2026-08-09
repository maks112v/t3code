import { randomUUID } from "node:crypto";

import {
  CopilotClient,
  type CopilotClientOptions,
  type CopilotSession,
  type MessageOptions,
  type PermissionRequest,
  type PermissionRequestResult,
  type SessionConfig,
  type SessionEvent,
} from "@github/copilot-sdk";
import {
  ApprovalRequestId,
  EventId,
  type GitHubCopilotSettings,
  type ProviderApprovalDecision,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderRuntimeEvent,
  type ProviderSession,
  RuntimeItemId,
  RuntimeRequestId,
  type ThreadId,
  TurnId,
} from "@t3tools/contracts";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import {
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import type { ProviderAdapterShape, ProviderThreadSnapshot } from "../Services/ProviderAdapter.ts";

const PROVIDER = ProviderDriverKind.make("githubCopilot");
const USER_INPUT_QUESTION_ID = "answer";

type GitHubCopilotAdapterError =
  | ProviderAdapterRequestError
  | ProviderAdapterSessionNotFoundError
  | ProviderAdapterValidationError;
type UserInputHandler = NonNullable<SessionConfig["onUserInputRequest"]>;
type UserInputRequest = Parameters<UserInputHandler>[0];
type UserInputResponse = Awaited<ReturnType<UserInputHandler>>;
type ReasoningEffort = NonNullable<SessionConfig["reasoningEffort"]>;

export interface GitHubCopilotAdapterOptions {
  readonly instanceId?: ProviderInstanceId;
  readonly environment?: NodeJS.ProcessEnv;
  readonly clientFactory?: (options: CopilotClientOptions) => CopilotClient;
}

interface PendingApproval {
  readonly requestType:
    | "command_execution_approval"
    | "file_change_approval"
    | "file_read_approval"
    | "dynamic_tool_call"
    | "unknown";
  readonly turnId: TurnId | undefined;
  readonly resolve: (result: PermissionRequestResult) => void;
}

interface PendingUserInput {
  readonly request: UserInputRequest;
  readonly turnId: TurnId | undefined;
  readonly resolve: (result: UserInputResponse) => void;
}

interface ToolMetadata {
  readonly title: string;
  readonly itemType: "command_execution" | "mcp_tool_call" | "dynamic_tool_call";
}

interface SessionContext {
  readonly client: CopilotClient;
  readonly copilot: CopilotSession;
  readonly threadId: ThreadId;
  session: ProviderSession;
  reasoningEffort: ReasoningEffort | undefined;
  autoModelEnabled: boolean;
  activeTurnId: TurnId | undefined;
  readonly pendingApprovals: Map<ApprovalRequestId, PendingApproval>;
  readonly pendingUserInputs: Map<ApprovalRequestId, PendingUserInput>;
  readonly tools: Map<string, ToolMetadata>;
  unsubscribe: () => void;
}

function messageFromCause(cause: unknown, fallback: string): string {
  return cause instanceof Error && cause.message.trim() ? cause.message : fallback;
}

function trim(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function asReasoningEffort(value: string | undefined): ReasoningEffort | undefined {
  switch (value) {
    case "low":
    case "medium":
    case "high":
    case "xhigh":
    case "max":
      return value;
    default:
      return undefined;
  }
}

function isAutoModel(value: string | undefined): boolean {
  return value?.toLowerCase() === "auto";
}

function resumeSessionId(value: unknown): string | undefined {
  if (typeof value === "string") return trim(value);
  if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
  return trim((value as Record<string, unknown>).sessionId);
}

function permissionRequestType(request: PermissionRequest): PendingApproval["requestType"] {
  switch (request.kind) {
    case "shell":
      return "command_execution_approval";
    case "write":
      return "file_change_approval";
    case "read":
      return "file_read_approval";
    case "mcp":
    case "custom-tool":
      return "dynamic_tool_call";
    default:
      return "unknown";
  }
}

function permissionDetail(request: PermissionRequest): string | undefined {
  switch (request.kind) {
    case "shell":
      return trim(request.fullCommandText);
    case "write":
      return trim(request.fileName ?? request.intention);
    case "read":
      return trim(request.path ?? request.intention);
    case "mcp":
      return trim(request.toolTitle ?? request.toolName);
    case "url":
      return trim(request.url ?? request.intention);
    case "custom-tool":
      return trim(request.toolName ?? request.toolDescription);
    default:
      return undefined;
  }
}

function permissionResult(decision: ProviderApprovalDecision): PermissionRequestResult {
  return decision === "accept" || decision === "acceptForSession"
    ? { kind: "approved" }
    : { kind: "denied-interactively-by-user" };
}

function isAutoApproved(runtimeMode: ProviderSession["runtimeMode"], request: PermissionRequest) {
  return (
    runtimeMode === "full-access" ||
    (runtimeMode === "auto-accept-edits" && request.kind === "write")
  );
}

function eventBase(input: {
  readonly instanceId: ProviderInstanceId;
  readonly threadId: ThreadId;
  readonly turnId?: TurnId;
  readonly itemId?: string;
  readonly requestId?: string;
  readonly raw?: SessionEvent;
}): Omit<ProviderRuntimeEvent, "type" | "payload"> {
  return {
    eventId: EventId.make(randomUUID()),
    provider: PROVIDER,
    providerInstanceId: input.instanceId,
    threadId: input.threadId,
    createdAt: input.raw?.timestamp ?? DateTime.formatIso(DateTime.nowUnsafe()),
    ...(input.turnId ? { turnId: input.turnId } : {}),
    ...(input.itemId ? { itemId: RuntimeItemId.make(input.itemId) } : {}),
    ...(input.requestId ? { requestId: RuntimeRequestId.make(input.requestId) } : {}),
    ...(input.raw
      ? {
          raw: {
            source: "github-copilot.sdk.event" as const,
            method: input.raw.type,
            payload: input.raw,
          },
        }
      : {
          raw: {
            source: "github-copilot.sdk.synthetic" as const,
            payload: {},
          },
        }),
  };
}

function syntheticEvent(
  instanceId: ProviderInstanceId,
  threadId: ThreadId,
  type: ProviderRuntimeEvent["type"],
  payload: ProviderRuntimeEvent["payload"],
  refs?: { readonly turnId?: TurnId; readonly requestId?: string },
): ProviderRuntimeEvent {
  return {
    ...eventBase({ instanceId, threadId, ...refs }),
    type,
    payload,
  } as ProviderRuntimeEvent;
}

function mapHistory(
  threadId: ThreadId,
  events: ReadonlyArray<SessionEvent>,
): ProviderThreadSnapshot {
  const turns: ProviderThreadSnapshot["turns"][number][] = [];
  let current: { id: TurnId; items: unknown[] } | undefined;
  for (const event of events) {
    if (event.type === "assistant.turn_start") {
      current = { id: TurnId.make(event.data.turnId), items: [event] };
      turns.push(current);
    } else if (current) {
      current.items.push(event);
      if (event.type === "assistant.turn_end" || event.type === "abort") current = undefined;
    }
  }
  return { threadId, turns };
}

export function makeGitHubCopilotAdapter(
  settings: GitHubCopilotSettings,
  options?: GitHubCopilotAdapterOptions,
) {
  return Effect.gen(function* () {
    const instanceId = options?.instanceId ?? ProviderInstanceId.make("githubCopilot");
    const serverConfig = yield* ServerConfig;
    const events = yield* Queue.unbounded<ProviderRuntimeEvent>();
    const sessions = new Map<ThreadId, SessionContext>();
    const runtimeContext = yield* Effect.context<never>();
    const runFork = Effect.runForkWith(runtimeContext);

    const emit = (event: ProviderRuntimeEvent) => Queue.offer(events, event).pipe(Effect.asVoid);
    const emitFromCallback = (event: ProviderRuntimeEvent) => {
      runFork(emit(event));
    };

    const ensureSession = (threadId: ThreadId) => {
      const context = sessions.get(threadId);
      return context
        ? Effect.succeed(context)
        : Effect.fail(new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }));
    };

    const stopContext = (context: SessionContext) =>
      Effect.tryPromise({
        try: async () => {
          context.unsubscribe();
          await context.copilot.disconnect();
          await context.client.stop();
        },
        catch: (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "session.disconnect",
            detail: messageFromCause(cause, "Failed to stop GitHub Copilot session."),
            cause,
          }),
      });

    yield* Effect.addFinalizer(() =>
      Effect.forEach([...sessions.values()], stopContext, {
        concurrency: "unbounded",
        discard: true,
      }).pipe(Effect.ignore, Effect.ensuring(Queue.shutdown(events))),
    );

    const mapSessionEvent = (
      context: SessionContext,
      event: SessionEvent,
    ): ReadonlyArray<ProviderRuntimeEvent> => {
      const turnId = context.activeTurnId;
      const base = (refs?: { readonly itemId?: string; readonly turnId?: TurnId }) => {
        const eventTurnId = refs?.turnId ?? turnId;
        return eventBase({
          instanceId,
          threadId: context.threadId,
          ...(eventTurnId ? { turnId: eventTurnId } : {}),
          ...(refs?.itemId ? { itemId: refs.itemId } : {}),
          raw: event,
        });
      };

      switch (event.type) {
        case "assistant.turn_start": {
          const resolvedModel = trim(event.data.model);
          if (!context.autoModelEnabled || !resolvedModel || isAutoModel(resolvedModel)) {
            return [];
          }
          return [
            {
              ...base(),
              type: "model.rerouted",
              payload: {
                fromModel: "auto",
                toModel: resolvedModel,
                reason: "GitHub Copilot Auto selected this model.",
              },
            },
          ];
        }
        case "assistant.message_delta":
          return [
            {
              ...base({ itemId: event.data.messageId }),
              type: "content.delta",
              payload: { streamKind: "assistant_text", delta: event.data.deltaContent },
            },
          ];
        case "assistant.message":
          return [
            {
              ...base({ itemId: event.data.messageId }),
              type: "item.completed",
              payload: {
                itemType: "assistant_message",
                status: "completed",
                title: "Assistant message",
                ...(trim(event.data.content) ? { detail: event.data.content.trim() } : {}),
                data: event.data,
              },
            },
          ];
        case "assistant.reasoning_delta":
          return [
            {
              ...base({ itemId: event.data.reasoningId }),
              type: "content.delta",
              payload: { streamKind: "reasoning_text", delta: event.data.deltaContent },
            },
          ];
        case "assistant.reasoning":
          return [
            {
              ...base({ itemId: event.data.reasoningId }),
              type: "item.completed",
              payload: {
                itemType: "reasoning",
                status: "completed",
                title: "Reasoning",
                ...(trim(event.data.content) ? { detail: event.data.content.trim() } : {}),
                data: event.data,
              },
            },
          ];
        case "tool.execution_start": {
          const itemType = event.data.shellToolInfo
            ? "command_execution"
            : event.data.mcpToolName
              ? "mcp_tool_call"
              : "dynamic_tool_call";
          context.tools.set(event.data.toolCallId, {
            title: event.data.toolName || "Tool call",
            itemType,
          });
          return [
            {
              ...base({ itemId: event.data.toolCallId }),
              type: "item.started",
              payload: {
                itemType,
                status: "inProgress",
                title: event.data.toolName || "Tool call",
                data: event.data,
              },
            },
          ];
        }
        case "tool.execution_progress":
          return [
            {
              ...base({ itemId: event.data.toolCallId }),
              type: "tool.progress",
              payload: {
                toolUseId: event.data.toolCallId,
                summary: event.data.progressMessage,
              },
            },
          ];
        case "tool.execution_complete": {
          const tool = context.tools.get(event.data.toolCallId);
          context.tools.delete(event.data.toolCallId);
          return [
            {
              ...base({ itemId: event.data.toolCallId }),
              type: "item.completed",
              payload: {
                itemType: tool?.itemType ?? "dynamic_tool_call",
                status: event.data.success ? "completed" : "failed",
                title: tool?.title ?? "Tool call",
                data: event.data,
              },
            },
          ];
        }
        case "assistant.usage": {
          const inputTokens = Math.max(0, Math.floor(event.data.inputTokens ?? 0));
          const cachedInputTokens = Math.max(0, Math.floor(event.data.cacheReadTokens ?? 0));
          const outputTokens = Math.max(0, Math.floor(event.data.outputTokens ?? 0));
          const reasoningOutputTokens = Math.max(0, Math.floor(event.data.reasoningTokens ?? 0));
          const usedTokens = inputTokens + cachedInputTokens + outputTokens;
          return [
            {
              ...base(),
              type: "thread.token-usage.updated",
              payload: {
                usage: {
                  usedTokens,
                  totalProcessedTokens: usedTokens,
                  inputTokens,
                  cachedInputTokens,
                  outputTokens,
                  reasoningOutputTokens,
                  lastUsedTokens: usedTokens,
                  lastInputTokens: inputTokens,
                  lastCachedInputTokens: cachedInputTokens,
                  lastOutputTokens: outputTokens,
                  lastReasoningOutputTokens: reasoningOutputTokens,
                },
              },
            },
          ];
        }
        case "session.title_changed":
          return [
            {
              ...base(),
              type: "thread.metadata.updated",
              payload: { name: event.data.title, metadata: { ...event.data } },
            },
          ];
        case "session.model_change": {
          if (event.agentId) return [];
          const previousModel = trim(event.data.previousModel) ?? context.session.model;
          const newModel = trim(event.data.newModel);
          if (!newModel) return [];
          context.session = {
            ...context.session,
            model: newModel,
            updatedAt: event.timestamp,
          };
          context.reasoningEffort = asReasoningEffort(trim(event.data.reasoningEffort));
          context.autoModelEnabled = isAutoModel(newModel);
          const reason = trim(event.data.cause);
          return previousModel && previousModel !== newModel && reason
            ? [
                {
                  ...base(),
                  type: "model.rerouted",
                  payload: { fromModel: previousModel, toModel: newModel, reason },
                },
              ]
            : [];
        }
        case "session.error":
          return [
            {
              ...base(),
              type: "runtime.error",
              payload: {
                message: event.data.message,
                class: "provider_error",
                detail: event.data,
              },
            },
          ];
        case "session.warning":
          return [
            {
              ...base(),
              type: "runtime.warning",
              payload: { message: event.data.message, detail: event.data },
            },
          ];
        case "abort":
          return turnId
            ? [
                {
                  ...base(),
                  type: "turn.aborted",
                  payload: { reason: String(event.data.reason) },
                },
              ]
            : [];
        case "session.idle": {
          if (!turnId) return [];
          context.activeTurnId = undefined;
          context.session = {
            ...context.session,
            status: "ready",
            activeTurnId: undefined,
            updatedAt: event.timestamp,
          };
          return [
            {
              ...base({ turnId }),
              type: event.data.aborted ? "turn.aborted" : "turn.completed",
              payload: event.data.aborted
                ? { reason: "Interrupted by user." }
                : { state: "completed" },
            } as ProviderRuntimeEvent,
            {
              ...base(),
              type: "session.state.changed",
              payload: { state: "ready", reason: "session.idle" },
            },
          ];
        }
        default:
          return [];
      }
    };

    const startSession: ProviderAdapterShape<GitHubCopilotAdapterError>["startSession"] = Effect.fn(
      "GitHubCopilotAdapter.startSession",
    )(function* (input) {
      const existing = sessions.get(input.threadId);
      if (existing) {
        yield* stopContext(existing).pipe(Effect.ignore);
        sessions.delete(input.threadId);
      }

      const cwd = input.cwd ?? serverConfig.cwd;
      const client = (
        options?.clientFactory ?? ((clientOptions) => new CopilotClient(clientOptions))
      )({
        workingDirectory: cwd,
        env: options?.environment ?? process.env,
        ...(settings.homePath ? { baseDirectory: settings.homePath } : {}),
      });
      const pendingApprovals = new Map<ApprovalRequestId, PendingApproval>();
      const pendingUserInputs = new Map<ApprovalRequestId, PendingUserInput>();
      let context: SessionContext | undefined;

      const onPermissionRequest = (request: PermissionRequest) => {
        if (isAutoApproved(input.runtimeMode, request)) {
          return Promise.resolve<PermissionRequestResult>({ kind: "approved" });
        }
        return new Promise<PermissionRequestResult>((resolve) => {
          const requestId = ApprovalRequestId.make(`copilot-${randomUUID()}`);
          const requestType = permissionRequestType(request);
          const turnId = context?.activeTurnId;
          pendingApprovals.set(requestId, { requestType, turnId, resolve });
          emitFromCallback(
            syntheticEvent(
              instanceId,
              input.threadId,
              "request.opened",
              {
                requestType,
                ...(permissionDetail(request) ? { detail: permissionDetail(request) } : {}),
                args: request,
              },
              { requestId, ...(turnId ? { turnId } : {}) },
            ),
          );
        });
      };
      const onUserInputRequest = (request: UserInputRequest) =>
        new Promise<UserInputResponse>((resolve) => {
          const requestId = ApprovalRequestId.make(`copilot-input-${randomUUID()}`);
          const turnId = context?.activeTurnId;
          pendingUserInputs.set(requestId, { request, turnId, resolve });
          emitFromCallback(
            syntheticEvent(
              instanceId,
              input.threadId,
              "user-input.requested",
              {
                questions: [
                  {
                    id: USER_INPUT_QUESTION_ID,
                    header: "Question",
                    question: request.question,
                    options: (request.choices ?? []).map((choice) => ({
                      label: choice,
                      description: choice,
                    })),
                  },
                ],
              },
              { requestId, ...(turnId ? { turnId } : {}) },
            ),
          );
        });

      const selectedModel = input.modelSelection?.model;
      const reasoningEffort = asReasoningEffort(
        getModelSelectionStringOptionValue(input.modelSelection, "reasoningEffort"),
      );
      const copilot = yield* Effect.tryPromise({
        try: async () => {
          await client.start();
          const config = {
            workingDirectory: cwd,
            streaming: true,
            ...(selectedModel ? { model: selectedModel } : {}),
            ...(reasoningEffort ? { reasoningEffort } : {}),
            onPermissionRequest,
            onUserInputRequest,
          };
          const resumeId = resumeSessionId(input.resumeCursor);
          return resumeId
            ? await client.resumeSession(resumeId, config)
            : await client.createSession(config);
        },
        catch: (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "session.create",
            detail: messageFromCause(cause, "Failed to start GitHub Copilot session."),
            cause,
          }),
      }).pipe(Effect.tapError(() => Effect.promise(() => client.stop()).pipe(Effect.ignore)));

      const now = DateTime.formatIso(yield* DateTime.now);
      const session: ProviderSession = {
        provider: PROVIDER,
        providerInstanceId: instanceId,
        status: "ready",
        runtimeMode: input.runtimeMode,
        cwd,
        ...(selectedModel ? { model: selectedModel } : {}),
        threadId: input.threadId,
        resumeCursor: { sessionId: copilot.sessionId },
        createdAt: now,
        updatedAt: now,
      };
      context = {
        client,
        copilot,
        threadId: input.threadId,
        session,
        reasoningEffort,
        autoModelEnabled: isAutoModel(selectedModel),
        activeTurnId: undefined,
        pendingApprovals,
        pendingUserInputs,
        tools: new Map(),
        unsubscribe: () => undefined,
      };
      context.unsubscribe = copilot.on((event) => {
        for (const mapped of mapSessionEvent(context!, event)) emitFromCallback(mapped);
      });
      sessions.set(input.threadId, context);

      yield* emit(
        syntheticEvent(instanceId, input.threadId, "session.started", {
          message: resumeSessionId(input.resumeCursor)
            ? "Resumed GitHub Copilot session"
            : "Started GitHub Copilot session",
          resume: { sessionId: copilot.sessionId },
        }),
      );
      yield* emit(
        syntheticEvent(instanceId, input.threadId, "thread.started", {
          providerThreadId: copilot.sessionId,
        }),
      );
      return session;
    });

    const sendTurn: ProviderAdapterShape<GitHubCopilotAdapterError>["sendTurn"] = Effect.fn(
      "GitHubCopilotAdapter.sendTurn",
    )(function* (input) {
      const context = yield* ensureSession(input.threadId);
      if (!input.input?.trim() && !(input.attachments && input.attachments.length > 0)) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: "Turn requires non-empty text or attachments.",
        });
      }
      const modelSelection = input.modelSelection;
      if (modelSelection !== undefined && modelSelection.instanceId !== instanceId) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: `GitHub Copilot model selection is bound to instance '${modelSelection.instanceId}', expected '${instanceId}'.`,
        });
      }
      if (modelSelection) {
        const reasoningEffort = asReasoningEffort(
          getModelSelectionStringOptionValue(modelSelection, "reasoningEffort"),
        );
        const shouldSwitch =
          modelSelection.model !== context.session.model ||
          reasoningEffort !== context.reasoningEffort;
        if (shouldSwitch) {
          if (context.activeTurnId) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "sendTurn",
              issue:
                "GitHub Copilot cannot switch models while a turn is running. Interrupt or wait for it to finish, then retry.",
            });
          }
          yield* Effect.tryPromise({
            try: () =>
              context.copilot.setModel(
                modelSelection.model,
                reasoningEffort ? { reasoningEffort } : undefined,
              ),
            catch: (cause) =>
              new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: "session.setModel",
                detail: messageFromCause(cause, "Failed to switch GitHub Copilot model."),
                cause,
              }),
          });
          context.session = {
            ...context.session,
            model: modelSelection.model,
            updatedAt: DateTime.formatIso(yield* DateTime.now),
          };
          context.reasoningEffort = reasoningEffort;
        }
        context.autoModelEnabled = isAutoModel(modelSelection.model);
      }
      const turnId = TurnId.make(randomUUID());
      context.activeTurnId = turnId;
      context.session = {
        ...context.session,
        status: "running",
        activeTurnId: turnId,
        updatedAt: DateTime.formatIso(yield* DateTime.now),
      };
      yield* emit(
        syntheticEvent(
          instanceId,
          input.threadId,
          "turn.started",
          context.session.model ? { model: context.session.model } : {},
          { turnId },
        ),
      );

      const attachments: NonNullable<MessageOptions["attachments"]> = [];
      for (const attachment of input.attachments ?? []) {
        const attachmentPath = resolveAttachmentPath({
          attachmentsDir: serverConfig.attachmentsDir,
          attachment,
        });
        if (!attachmentPath) {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "session.send",
            detail: `Invalid attachment id '${attachment.id}'.`,
          });
        }
        attachments.push({
          type: "file",
          path: attachmentPath,
          displayName: attachment.name,
        });
      }
      yield* Effect.tryPromise({
        try: () =>
          context.copilot.send({
            prompt: input.input?.trim() ?? "Please inspect the attached image.",
            ...(attachments.length > 0 ? { attachments } : {}),
            agentMode: input.interactionMode === "plan" ? "plan" : "interactive",
          }),
        catch: (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "session.send",
            detail: messageFromCause(cause, "Failed to send GitHub Copilot turn."),
            cause,
          }),
      });
      return {
        threadId: input.threadId,
        turnId,
        resumeCursor: { sessionId: context.copilot.sessionId },
      };
    });

    const interruptTurn: ProviderAdapterShape<GitHubCopilotAdapterError>["interruptTurn"] =
      Effect.fn("GitHubCopilotAdapter.interruptTurn")(function* (threadId) {
        const context = yield* ensureSession(threadId);
        yield* Effect.tryPromise({
          try: () => context.copilot.abort(),
          catch: (cause) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "session.abort",
              detail: messageFromCause(cause, "Failed to interrupt GitHub Copilot."),
              cause,
            }),
        });
      });

    const respondToRequest: ProviderAdapterShape<GitHubCopilotAdapterError>["respondToRequest"] =
      Effect.fn("GitHubCopilotAdapter.respondToRequest")(function* (threadId, requestId, decision) {
        const context = yield* ensureSession(threadId);
        const pending = context.pendingApprovals.get(requestId);
        if (!pending) {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "permission.respond",
            detail: `Unknown permission request '${requestId}'.`,
          });
        }
        context.pendingApprovals.delete(requestId);
        pending.resolve(permissionResult(decision));
        yield* emit(
          syntheticEvent(
            instanceId,
            threadId,
            "request.resolved",
            { requestType: pending.requestType, decision },
            { requestId, ...(pending.turnId ? { turnId: pending.turnId } : {}) },
          ),
        );
      });

    const respondToUserInput: ProviderAdapterShape<GitHubCopilotAdapterError>["respondToUserInput"] =
      Effect.fn("GitHubCopilotAdapter.respondToUserInput")(
        function* (threadId, requestId, answers) {
          const context = yield* ensureSession(threadId);
          const pending = context.pendingUserInputs.get(requestId);
          if (!pending) {
            return yield* new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "user-input.respond",
              detail: `Unknown user-input request '${requestId}'.`,
            });
          }
          context.pendingUserInputs.delete(requestId);
          const answer =
            Object.values(answers).find((value): value is string => typeof value === "string") ??
            "";
          pending.resolve({
            answer,
            wasFreeform: !(pending.request.choices ?? []).includes(answer),
          });
          yield* emit(
            syntheticEvent(
              instanceId,
              threadId,
              "user-input.resolved",
              { answers },
              { requestId, ...(pending.turnId ? { turnId: pending.turnId } : {}) },
            ),
          );
        },
      );

    const stopSession: ProviderAdapterShape<GitHubCopilotAdapterError>["stopSession"] = Effect.fn(
      "GitHubCopilotAdapter.stopSession",
    )(function* (threadId) {
      const context = yield* ensureSession(threadId);
      yield* stopContext(context);
      sessions.delete(threadId);
      yield* emit(
        syntheticEvent(instanceId, threadId, "session.exited", {
          reason: "Session stopped.",
          recoverable: true,
          exitKind: "graceful",
        }),
      );
    });

    const readThread: ProviderAdapterShape<GitHubCopilotAdapterError>["readThread"] = Effect.fn(
      "GitHubCopilotAdapter.readThread",
    )(function* (threadId) {
      const context = yield* ensureSession(threadId);
      const history = yield* Effect.tryPromise({
        try: () => context.copilot.getEvents(),
        catch: (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "session.getEvents",
            detail: messageFromCause(cause, "Failed to read GitHub Copilot history."),
            cause,
          }),
      });
      return mapHistory(threadId, history);
    });

    const rollbackThread: ProviderAdapterShape<GitHubCopilotAdapterError>["rollbackThread"] =
      Effect.fn("GitHubCopilotAdapter.rollbackThread")(function* () {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "rollbackThread",
          issue: "GitHub Copilot rollback is not implemented in this proof of concept.",
        });
      });

    const stopAll = () =>
      Effect.gen(function* () {
        const active = [...sessions.values()];
        sessions.clear();
        yield* Effect.forEach(active, stopContext, {
          concurrency: "unbounded",
          discard: true,
        }).pipe(Effect.ignore);
      });

    return {
      provider: PROVIDER,
      capabilities: { sessionModelSwitch: "in-session" },
      startSession,
      sendTurn,
      interruptTurn,
      respondToRequest,
      respondToUserInput,
      stopSession,
      listSessions: () => Effect.sync(() => [...sessions.values()].map((entry) => entry.session)),
      hasSession: (threadId) => Effect.sync(() => sessions.has(threadId)),
      readThread,
      rollbackThread,
      stopAll,
      get streamEvents() {
        return Stream.fromQueue(events);
      },
    } satisfies ProviderAdapterShape<
      | ProviderAdapterRequestError
      | ProviderAdapterSessionNotFoundError
      | ProviderAdapterValidationError
    >;
  });
}
