export { AcpClient } from "./client.js";
export type { SessionCreateResult } from "./client.js";
export type { AcpClientOptions, PermissionMode, ClientOperation, PermissionStats } from "./types.js";
export { AgentSpawnError, PermissionPromptUnavailableError } from "./errors.js";
export { resolveAgentCommand, mergeAgentRegistry, listBuiltInAgents } from "./agent-registry.js";
