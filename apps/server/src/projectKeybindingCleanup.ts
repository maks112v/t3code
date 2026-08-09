import {
  SCRIPT_RUN_COMMAND_PATTERN,
  type KeybindingCommand,
  type OrchestrationProject,
  type ProjectId,
} from "@t3tools/contracts";

export function orphanedProjectScriptCommandsOnDelete(
  projectId: ProjectId,
  projects: ReadonlyArray<OrchestrationProject>,
): ReadonlyArray<KeybindingCommand> {
  const deletedProject = projects.find(
    (project) => project.id === projectId && project.deletedAt === null,
  );
  if (!deletedProject) return [];

  const remainingScriptIds = new Set(
    projects
      .filter((project) => project.id !== projectId && project.deletedAt === null)
      .flatMap((project) => project.scripts.map((script) => script.id)),
  );

  return Array.from(
    new Set(
      deletedProject.scripts
        .map((script) => script.id)
        .filter((scriptId) => !remainingScriptIds.has(scriptId)),
    ),
    (scriptId) => SCRIPT_RUN_COMMAND_PATTERN.make(`script.${scriptId}.run`),
  );
}
