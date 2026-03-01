export { AcpClient } from "./client.js";
export type { SessionCreateResult } from "./client.js";
export type {
  AcpClientOptions,
  PermissionMode,
  ClientOperation,
  PermissionStats,
  AcpxEvent,
  AcpxEventDraft,
} from "./types.js";
export { AgentSpawnError, PermissionPromptUnavailableError } from "./errors.js";
export {
  resolveAgentCommand,
  mergeAgentRegistry,
  listBuiltInAgents,
} from "./agent-registry.js";
export { sessionUpdateToEventDrafts, createAcpxEvent } from "./events.js";
export { formatErrorMessage } from "./error-normalization.js";
export {
  listSessionsForAgent,
  writeSessionRecord,
  isoNow,
} from "./session-persistence.js";
export { SESSION_RECORD_SCHEMA } from "./types.js";
export type { SessionRecord } from "./types.js";
