import { ProjectId, type OrchestrationProject } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { orphanedProjectScriptCommandsOnDelete } from "./projectKeybindingCleanup.ts";

const project = (
  id: string,
  scriptIds: ReadonlyArray<string>,
  deletedAt: string | null = null,
): OrchestrationProject => ({
  id: ProjectId.make(id),
  title: id,
  workspaceRoot: `/tmp/${id}`,
  defaultModelSelection: null,
  scripts: scriptIds.map((scriptId) => ({
    id: scriptId,
    name: scriptId,
    command: `pnpm ${scriptId}`,
    icon: "play",
    runOnWorktreeCreate: false,
  })),
  createdAt: "2026-01-01T00:00:00.000Z",
  updatedAt: "2026-01-01T00:00:00.000Z",
  deletedAt,
});

describe("orphanedProjectScriptCommandsOnDelete", () => {
  it("returns commands used only by the deleted project", () => {
    expect(
      orphanedProjectScriptCommandsOnDelete(ProjectId.make("one"), [
        project("one", ["dev", "test"]),
        project("two", ["dev", "lint"]),
      ]),
    ).toEqual(["script.test.run"]);
  });

  it("ignores already deleted projects when checking shared commands", () => {
    expect(
      orphanedProjectScriptCommandsOnDelete(ProjectId.make("one"), [
        project("one", ["dev"]),
        project("old", ["dev"], "2026-01-02T00:00:00.000Z"),
      ]),
    ).toEqual(["script.dev.run"]);
  });
});
