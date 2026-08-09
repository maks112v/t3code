import {
  KeybindingRule as KeybindingRuleSchema,
  type KeybindingCommand,
  type KeybindingRule,
  type ResolvedKeybindingsConfig,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

export const PROJECT_SCRIPT_KEYBINDING_INVALID_MESSAGE = "Invalid keybinding.";

const decodeKeybindingRule = Schema.decodeUnknownOption(KeybindingRuleSchema);

function normalizeProjectScriptKeybindingInput(
  keybinding: string | null | undefined,
): string | null {
  const trimmed = keybinding?.trim() ?? "";
  return trimmed.length > 0 ? trimmed : null;
}

export function decodeProjectScriptKeybindingRule(input: {
  keybinding: string | null | undefined;
  command: KeybindingCommand;
}): KeybindingRule | null {
  const normalizedKey = normalizeProjectScriptKeybindingInput(input.keybinding);
  if (!normalizedKey) return null;

  const decoded = decodeKeybindingRule({
    key: normalizedKey,
    command: input.command,
  });
  if (decoded._tag === "None") {
    throw new Error(PROJECT_SCRIPT_KEYBINDING_INVALID_MESSAGE);
  }
  return decoded.value;
}

export function keybindingValueForCommand(
  keybindings: ResolvedKeybindingsConfig,
  command: KeybindingCommand,
): string | null {
  return keybindingRulesForCommand(keybindings, command).at(-1)?.key ?? null;
}

export function keybindingRulesForCommand(
  keybindings: ResolvedKeybindingsConfig,
  command: KeybindingCommand,
): ReadonlyArray<KeybindingRule> {
  const rules: KeybindingRule[] = [];
  for (const binding of keybindings) {
    if (binding.command !== command || binding.whenAst !== undefined) continue;

    const parts: string[] = [];
    if (binding.shortcut.modKey) parts.push("mod");
    if (binding.shortcut.ctrlKey) parts.push("ctrl");
    if (binding.shortcut.metaKey) parts.push("meta");
    if (binding.shortcut.altKey) parts.push("alt");
    if (binding.shortcut.shiftKey) parts.push("shift");
    const keyToken =
      binding.shortcut.key === " "
        ? "space"
        : binding.shortcut.key === "escape"
          ? "esc"
          : binding.shortcut.key;
    parts.push(keyToken);
    rules.push({ key: parts.join("+"), command });
  }
  return rules;
}
