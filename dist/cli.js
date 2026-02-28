#!/usr/bin/env node
import {
  DEFAULT_AGENT_NAME,
  listBuiltInAgents,
  normalizeAgentName,
  resolveAgentCommand
} from "./chunk-S2W5ZAJT.js";
import {
  ACPX_EVENT_TYPES,
  AUTH_POLICIES,
  DEFAULT_HISTORY_LIMIT,
  DEFAULT_QUEUE_OWNER_TTL_MS,
  EXIT_CODES,
  InterruptedError,
  NON_INTERACTIVE_PERMISSION_POLICIES,
  OUTPUT_FORMATS,
  cancelSessionPrompt,
  clientOperationToEventDraft,
  closeSession,
  createAcpxEvent,
  createSession,
  ensureSession,
  errorToEventDraft,
  exitCodeForOutputErrorCode,
  findGitRepositoryRoot,
  findSession,
  findSessionByDirectoryWalk,
  isAcpxEvent,
  listSessionsForAgent,
  normalizeOutputError,
  probeQueueOwnerHealth,
  runOnce,
  sendSession,
  sessionUpdateToEventDrafts,
  setSessionConfigOption,
  setSessionMode
} from "./chunk-3XONKFR7.js";
import {
  normalizeRuntimeSessionId
} from "./chunk-6SQCOJO6.js";

// src/cli.ts
import { realpathSync as realpathSync2 } from "fs";
import { pathToFileURL as pathToFileURL2 } from "url";

// src/cli-core.ts
import { Command, CommanderError, InvalidArgumentError as InvalidArgumentError2 } from "commander";
import { realpathSync } from "fs";
import fs2 from "fs/promises";
import path3 from "path";
import { pathToFileURL } from "url";
import { findSkillsRoot, maybeHandleSkillflag } from "skillflag";

// src/cli/flags.ts
import { InvalidArgumentError } from "commander";
import path from "path";
function parseOutputFormat(value) {
  if (!OUTPUT_FORMATS.includes(value)) {
    throw new InvalidArgumentError(
      `Invalid format "${value}". Expected one of: ${OUTPUT_FORMATS.join(", ")}`
    );
  }
  return value;
}
function parseAuthPolicy(value) {
  if (!AUTH_POLICIES.includes(value)) {
    throw new InvalidArgumentError(
      `Invalid auth policy "${value}". Expected one of: ${AUTH_POLICIES.join(", ")}`
    );
  }
  return value;
}
function parseNonInteractivePermissionPolicy(value) {
  if (!NON_INTERACTIVE_PERMISSION_POLICIES.includes(
    value
  )) {
    throw new InvalidArgumentError(
      `Invalid non-interactive permission policy "${value}". Expected one of: ${NON_INTERACTIVE_PERMISSION_POLICIES.join(", ")}`
    );
  }
  return value;
}
function parseTimeoutSeconds(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new InvalidArgumentError("Timeout must be a positive number of seconds");
  }
  return Math.round(parsed * 1e3);
}
function parseTtlSeconds(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new InvalidArgumentError("TTL must be a non-negative number of seconds");
  }
  return Math.round(parsed * 1e3);
}
function parseSessionName(value) {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    throw new InvalidArgumentError("Session name must not be empty");
  }
  return trimmed;
}
function parseNonEmptyValue(label, value) {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    throw new InvalidArgumentError(`${label} must not be empty`);
  }
  return trimmed;
}
function parseHistoryLimit(value) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new InvalidArgumentError("Limit must be a positive integer");
  }
  return parsed;
}
function resolvePermissionMode(flags, defaultMode) {
  const selected = [flags.approveAll, flags.approveReads, flags.denyAll].filter(
    Boolean
  ).length;
  if (selected > 1) {
    throw new InvalidArgumentError(
      "Use only one permission mode: --approve-all, --approve-reads, or --deny-all"
    );
  }
  if (flags.approveAll) {
    return "approve-all";
  }
  if (flags.denyAll) {
    return "deny-all";
  }
  return defaultMode;
}
function addGlobalFlags(command) {
  return command.option("--agent <command>", "Raw ACP agent command (escape hatch)").option("--cwd <dir>", "Working directory", process.cwd()).option(
    "--auth-policy <policy>",
    "Authentication policy: skip or fail when auth is required",
    parseAuthPolicy
  ).option("--approve-all", "Auto-approve all permission requests").option(
    "--approve-reads",
    "Auto-approve read/search requests and prompt for writes"
  ).option("--deny-all", "Deny all permission requests").option(
    "--non-interactive-permissions <policy>",
    "When prompting is unavailable: deny or fail",
    parseNonInteractivePermissionPolicy
  ).option("--format <fmt>", "Output format: text, json, quiet", parseOutputFormat).option(
    "--json-strict",
    "Strict JSON mode: requires --format json and suppresses non-JSON stderr output"
  ).option(
    "--timeout <seconds>",
    "Maximum time to wait for agent response",
    parseTimeoutSeconds
  ).option(
    "--ttl <seconds>",
    "Queue owner idle TTL before shutdown (0 = keep alive forever) (default: 300)",
    parseTtlSeconds
  ).option("--verbose", "Enable verbose debug logs");
}
function addSessionOption(command) {
  return command.option(
    "-s, --session <name>",
    "Use named session instead of cwd default",
    parseSessionName
  ).option(
    "--no-wait",
    "Queue prompt and return immediately when another prompt is already running"
  );
}
function addSessionNameOption(command) {
  return command.option(
    "-s, --session <name>",
    "Use named session instead of cwd default",
    parseSessionName
  );
}
function resolveSessionNameFromFlags(flags, command) {
  if (flags.session) {
    return flags.session;
  }
  const allOpts = command.optsWithGlobals?.();
  if (allOpts && typeof allOpts.session === "string") {
    return parseSessionName(allOpts.session);
  }
  const parentOpts = command.parent?.opts?.();
  if (parentOpts && typeof parentOpts.session === "string") {
    return parseSessionName(parentOpts.session);
  }
  return void 0;
}
function addPromptInputOption(command) {
  return command.option(
    "-f, --file <path>",
    "Read prompt text from file path (use - for stdin)"
  );
}
function resolveGlobalFlags(command, config) {
  const opts = command.optsWithGlobals();
  const format = opts.format ?? config.format ?? "text";
  const jsonStrict = opts.jsonStrict === true;
  const verbose = opts.verbose === true;
  if (jsonStrict && format !== "json") {
    throw new InvalidArgumentError("--json-strict requires --format json");
  }
  if (jsonStrict && verbose) {
    throw new InvalidArgumentError("--json-strict cannot be combined with --verbose");
  }
  return {
    agent: opts.agent,
    cwd: opts.cwd ?? process.cwd(),
    authPolicy: opts.authPolicy ?? config.authPolicy,
    nonInteractivePermissions: opts.nonInteractivePermissions ?? config.nonInteractivePermissions,
    jsonStrict,
    timeout: opts.timeout ?? config.timeoutMs,
    ttl: opts.ttl ?? config.ttlMs ?? DEFAULT_QUEUE_OWNER_TTL_MS,
    verbose,
    format,
    approveAll: opts.approveAll ? true : void 0,
    approveReads: opts.approveReads ? true : void 0,
    denyAll: opts.denyAll ? true : void 0
  };
}
function resolveOutputPolicy(format, jsonStrict) {
  return {
    format,
    jsonStrict,
    suppressNonJsonStderr: jsonStrict,
    queueErrorAlreadyEmitted: format !== "quiet",
    suppressSdkConsoleErrors: jsonStrict
  };
}
function resolveAgentInvocation(explicitAgentName, globalFlags, config) {
  const override = globalFlags.agent?.trim();
  if (override && explicitAgentName) {
    throw new InvalidArgumentError(
      "Do not combine positional agent with --agent override"
    );
  }
  const agentName = explicitAgentName ?? config.defaultAgent ?? DEFAULT_AGENT_NAME;
  const agentCommand = override && override.length > 0 ? override : resolveAgentCommand(agentName, config.agents);
  return {
    agentName,
    agentCommand,
    cwd: path.resolve(globalFlags.cwd)
  };
}

// src/config.ts
import fs from "fs/promises";
import os from "os";
import path2 from "path";
var DEFAULT_TIMEOUT_MS = void 0;
var DEFAULT_TTL_MS = 3e5;
var DEFAULT_PERMISSION_MODE = "approve-reads";
var DEFAULT_NON_INTERACTIVE_PERMISSION_POLICY = "deny";
var DEFAULT_AUTH_POLICY = "skip";
var DEFAULT_OUTPUT_FORMAT = "text";
var VALID_PERMISSION_MODES = /* @__PURE__ */ new Set([
  "approve-all",
  "approve-reads",
  "deny-all"
]);
var VALID_NON_INTERACTIVE_PERMISSION_POLICIES = /* @__PURE__ */ new Set(["deny", "fail"]);
var VALID_AUTH_POLICIES = /* @__PURE__ */ new Set(["skip", "fail"]);
var VALID_OUTPUT_FORMATS = /* @__PURE__ */ new Set(["text", "json", "quiet"]);
function defaultGlobalConfigPath() {
  return path2.join(os.homedir(), ".acpx", "config.json");
}
function projectConfigPath(cwd) {
  return path2.join(path2.resolve(cwd), ".acpxrc.json");
}
function isObject(value) {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}
function parseTtlMs(value, sourcePath) {
  if (value == null) {
    return void 0;
  }
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    throw new Error(
      `Invalid config ttl in ${sourcePath}: expected non-negative seconds`
    );
  }
  return Math.round(value * 1e3);
}
function parseTimeoutMs(value, sourcePath) {
  if (value == null) {
    return void 0;
  }
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
    throw new Error(
      `Invalid config timeout in ${sourcePath}: expected positive seconds or null`
    );
  }
  return Math.round(value * 1e3);
}
function parsePermissionMode(value, sourcePath) {
  if (value == null) {
    return void 0;
  }
  if (typeof value !== "string" || !VALID_PERMISSION_MODES.has(value)) {
    throw new Error(
      `Invalid config defaultPermissions in ${sourcePath}: expected approve-all, approve-reads, or deny-all`
    );
  }
  return value;
}
function parseNonInteractivePermissionPolicy2(value, sourcePath) {
  if (value == null) {
    return void 0;
  }
  if (typeof value !== "string" || !VALID_NON_INTERACTIVE_PERMISSION_POLICIES.has(
    value
  )) {
    throw new Error(
      `Invalid config nonInteractivePermissions in ${sourcePath}: expected deny or fail`
    );
  }
  return value;
}
function parseAuthPolicy2(value, sourcePath) {
  if (value == null) {
    return void 0;
  }
  if (typeof value !== "string" || !VALID_AUTH_POLICIES.has(value)) {
    throw new Error(
      `Invalid config authPolicy in ${sourcePath}: expected skip or fail`
    );
  }
  return value;
}
function parseOutputFormat2(value, sourcePath) {
  if (value == null) {
    return void 0;
  }
  if (typeof value !== "string" || !VALID_OUTPUT_FORMATS.has(value)) {
    throw new Error(
      `Invalid config format in ${sourcePath}: expected text, json, or quiet`
    );
  }
  return value;
}
function parseDefaultAgent(value, sourcePath) {
  if (value == null) {
    return void 0;
  }
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(
      `Invalid config defaultAgent in ${sourcePath}: expected non-empty string`
    );
  }
  return normalizeAgentName(value);
}
function parseAgents(value, sourcePath) {
  if (value == null) {
    return void 0;
  }
  if (!isObject(value)) {
    throw new Error(`Invalid config agents in ${sourcePath}: expected object`);
  }
  const parsed = {};
  for (const [name, raw] of Object.entries(value)) {
    if (!isObject(raw)) {
      throw new Error(
        `Invalid config agents.${name} in ${sourcePath}: expected object with command`
      );
    }
    const command = raw.command;
    if (typeof command !== "string" || command.trim().length === 0) {
      throw new Error(
        `Invalid config agents.${name}.command in ${sourcePath}: expected non-empty string`
      );
    }
    parsed[normalizeAgentName(name)] = command.trim();
  }
  return parsed;
}
function parseAuth(value, sourcePath) {
  if (value == null) {
    return void 0;
  }
  if (!isObject(value)) {
    throw new Error(`Invalid config auth in ${sourcePath}: expected object`);
  }
  const parsed = {};
  for (const [methodId, rawCredential] of Object.entries(value)) {
    if (typeof rawCredential !== "string" || rawCredential.trim().length === 0) {
      throw new Error(
        `Invalid config auth.${methodId} in ${sourcePath}: expected non-empty string`
      );
    }
    parsed[methodId] = rawCredential;
  }
  return parsed;
}
async function readConfigFile(filePath) {
  try {
    const payload = await fs.readFile(filePath, "utf8");
    let parsed;
    try {
      parsed = JSON.parse(payload);
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      throw new Error(`Invalid JSON in ${filePath}: ${reason}`, {
        cause: error
      });
    }
    if (!isObject(parsed)) {
      throw new Error(`Invalid config in ${filePath}: expected top-level JSON object`);
    }
    return {
      config: parsed,
      exists: true
    };
  } catch (error) {
    if (error.code === "ENOENT") {
      return { exists: false };
    }
    throw error;
  }
}
function mergeAgents(globalAgents, projectAgents) {
  return {
    ...globalAgents ?? {},
    ...projectAgents ?? {}
  };
}
function mergeAuth(globalAuth, projectAuth) {
  return {
    ...globalAuth ?? {},
    ...projectAuth ?? {}
  };
}
async function loadResolvedConfig(cwd) {
  const globalPath = defaultGlobalConfigPath();
  const projectPath = projectConfigPath(cwd);
  const [globalResult, projectResult] = await Promise.all([
    readConfigFile(globalPath),
    readConfigFile(projectPath)
  ]);
  const globalConfig = globalResult.config;
  const projectConfig = projectResult.config;
  const defaultAgent = parseDefaultAgent(projectConfig?.defaultAgent, projectPath) ?? parseDefaultAgent(globalConfig?.defaultAgent, globalPath) ?? DEFAULT_AGENT_NAME;
  const defaultPermissions = parsePermissionMode(projectConfig?.defaultPermissions, projectPath) ?? parsePermissionMode(globalConfig?.defaultPermissions, globalPath) ?? DEFAULT_PERMISSION_MODE;
  const nonInteractivePermissions = parseNonInteractivePermissionPolicy2(
    projectConfig?.nonInteractivePermissions,
    projectPath
  ) ?? parseNonInteractivePermissionPolicy2(
    globalConfig?.nonInteractivePermissions,
    globalPath
  ) ?? DEFAULT_NON_INTERACTIVE_PERMISSION_POLICY;
  const authPolicy = parseAuthPolicy2(projectConfig?.authPolicy, projectPath) ?? parseAuthPolicy2(globalConfig?.authPolicy, globalPath) ?? DEFAULT_AUTH_POLICY;
  const ttlMs = parseTtlMs(projectConfig?.ttl, projectPath) ?? parseTtlMs(globalConfig?.ttl, globalPath) ?? DEFAULT_TTL_MS;
  const timeoutConfiguredInProject = projectConfig != null && Object.prototype.hasOwnProperty.call(projectConfig, "timeout");
  const timeoutConfiguredInGlobal = globalConfig != null && Object.prototype.hasOwnProperty.call(globalConfig, "timeout");
  let timeoutMs = DEFAULT_TIMEOUT_MS;
  if (timeoutConfiguredInProject) {
    timeoutMs = parseTimeoutMs(projectConfig?.timeout, projectPath);
  } else if (timeoutConfiguredInGlobal) {
    timeoutMs = parseTimeoutMs(globalConfig?.timeout, globalPath);
  }
  const format = parseOutputFormat2(projectConfig?.format, projectPath) ?? parseOutputFormat2(globalConfig?.format, globalPath) ?? DEFAULT_OUTPUT_FORMAT;
  const agents = mergeAgents(
    parseAgents(globalConfig?.agents, globalPath),
    parseAgents(projectConfig?.agents, projectPath)
  );
  const auth = mergeAuth(
    parseAuth(globalConfig?.auth, globalPath),
    parseAuth(projectConfig?.auth, projectPath)
  );
  return {
    defaultAgent,
    defaultPermissions,
    nonInteractivePermissions,
    authPolicy,
    ttlMs,
    timeoutMs,
    format,
    agents,
    auth,
    globalPath,
    projectPath,
    hasGlobalConfig: globalResult.exists,
    hasProjectConfig: projectResult.exists
  };
}
function toConfigDisplay(config) {
  const agents = {};
  for (const [name, command] of Object.entries(config.agents)) {
    agents[name] = { command };
  }
  return {
    defaultAgent: config.defaultAgent,
    defaultPermissions: config.defaultPermissions,
    nonInteractivePermissions: config.nonInteractivePermissions,
    authPolicy: config.authPolicy,
    ttl: Math.round(config.ttlMs / 1e3),
    timeout: config.timeoutMs == null ? null : config.timeoutMs / 1e3,
    format: config.format,
    agents,
    authMethods: Object.keys(config.auth).sort()
  };
}
async function initGlobalConfigFile() {
  const configPath = defaultGlobalConfigPath();
  await fs.mkdir(path2.dirname(configPath), { recursive: true });
  try {
    await fs.access(configPath);
    return {
      path: configPath,
      created: false
    };
  } catch {
  }
  const payload = {
    defaultAgent: DEFAULT_AGENT_NAME,
    defaultPermissions: "approve-all",
    nonInteractivePermissions: "deny",
    authPolicy: "skip",
    ttl: 300,
    timeout: null,
    format: "text",
    agents: {},
    auth: {}
  };
  await fs.writeFile(configPath, `${JSON.stringify(payload, null, 2)}
`, "utf8");
  return {
    path: configPath,
    created: true
  };
}

// src/output.ts
var MAX_THOUGHT_CHARS = 900;
var MAX_INLINE_CHARS = 220;
var MAX_OUTPUT_CHARS = 2e3;
var MAX_OUTPUT_LINES = 28;
var MAX_LOCATION_ITEMS = 5;
var OUTPUT_PRIORITY_KEYS = [
  "stdout",
  "stderr",
  "output",
  "content",
  "text",
  "message",
  "result",
  "response",
  "value"
];
var DEFAULT_JSON_SESSION_ID = "unknown";
function asStatus(status) {
  return status ?? "unknown";
}
function isFinalStatus(status) {
  return status === "completed" || status === "failed";
}
function toStatusLabel(status) {
  switch (status) {
    case "in_progress":
      return "running";
    case "pending":
      return "pending";
    case "completed":
      return "completed";
    case "failed":
      return "failed";
    default:
      return "running";
  }
}
function asRecord(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return void 0;
  }
  return value;
}
function collapseWhitespace(value) {
  return value.replace(/\s+/g, " ").trim();
}
function truncate(value, maxChars) {
  if (value.length <= maxChars) {
    return value;
  }
  if (maxChars <= 3) {
    return value.slice(0, maxChars);
  }
  return `${value.slice(0, maxChars - 3)}...`;
}
function toInline(value, maxChars = MAX_INLINE_CHARS) {
  return truncate(collapseWhitespace(value), maxChars);
}
function indentBlock(value, prefix) {
  return value.split("\n").map((line) => `${prefix}${line}`).join("\n");
}
function dedupeStrings(values) {
  const seen = /* @__PURE__ */ new Set();
  const result = [];
  for (const value of values) {
    if (seen.has(value)) {
      continue;
    }
    seen.add(value);
    result.push(value);
  }
  return result;
}
function safeJson(value, spacing) {
  const seen = /* @__PURE__ */ new WeakSet();
  try {
    return JSON.stringify(
      value,
      (_key, entry) => {
        if (typeof entry === "bigint") {
          return `${entry}n`;
        }
        if (typeof entry === "function") {
          return `[Function ${entry.name || "anonymous"}]`;
        }
        if (typeof entry === "symbol") {
          return entry.toString();
        }
        if (entry && typeof entry === "object") {
          if (seen.has(entry)) {
            return "[Circular]";
          }
          seen.add(entry);
        }
        return entry;
      },
      spacing
    );
  } catch {
    return void 0;
  }
}
function readFirstString(source, keys) {
  for (const key of keys) {
    const value = source[key];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return void 0;
}
function readFirstStringArray(source, keys) {
  for (const key of keys) {
    const value = source[key];
    if (!Array.isArray(value)) {
      continue;
    }
    const entries = value.map((entry) => typeof entry === "string" ? entry.trim() : "").filter((entry) => entry.length > 0);
    if (entries.length > 0) {
      return entries;
    }
  }
  return void 0;
}
function summarizeToolInput(rawInput) {
  if (rawInput == null) {
    return void 0;
  }
  if (typeof rawInput === "string" || typeof rawInput === "number" || typeof rawInput === "boolean") {
    return toInline(String(rawInput));
  }
  const record = asRecord(rawInput);
  if (record) {
    const command = readFirstString(record, ["command", "cmd", "program"]);
    const args = readFirstStringArray(record, ["args", "arguments"]);
    if (command) {
      const invocation = [command, ...args ?? []].join(" ");
      return toInline(invocation);
    }
    const location = readFirstString(record, [
      "path",
      "file",
      "filePath",
      "filepath",
      "target",
      "uri",
      "url"
    ]);
    if (location) {
      return toInline(location);
    }
    const query = readFirstString(record, ["query", "pattern", "text", "search"]);
    if (query) {
      return toInline(query);
    }
  }
  const json = safeJson(rawInput, 0);
  return json ? toInline(json) : void 0;
}
function formatLocations(locations) {
  if (!locations || locations.length === 0) {
    return void 0;
  }
  const unique = /* @__PURE__ */ new Set();
  for (const location of locations) {
    const path4 = location.path?.trim();
    if (!path4) {
      continue;
    }
    const line = typeof location.line === "number" && Number.isFinite(location.line) ? `:${Math.max(1, Math.trunc(location.line))}` : "";
    unique.add(`${path4}${line}`);
  }
  const items = [...unique];
  if (items.length === 0) {
    return void 0;
  }
  const visible = items.slice(0, MAX_LOCATION_ITEMS);
  const hidden = items.length - visible.length;
  if (hidden <= 0) {
    return visible.join(", ");
  }
  return `${visible.join(", ")}, +${hidden} more`;
}
function summarizeDiff(path4, oldText, newText) {
  const oldLines = oldText ? oldText.split("\n").length : 0;
  const newLines = newText.split("\n").length;
  const delta = newLines - oldLines;
  if (delta === 0) {
    return `diff ${path4} (line count unchanged)`;
  }
  const signedDelta = `${delta > 0 ? "+" : ""}${delta}`;
  return `diff ${path4} (${signedDelta} lines)`;
}
function textFromContentBlock(content) {
  switch (content.type) {
    case "text":
      return content.text;
    case "resource_link":
      return content.title ?? content.name ?? content.uri;
    case "resource": {
      if ("text" in content.resource && typeof content.resource.text === "string") {
        return content.resource.text;
      }
      const uri = content.resource.uri;
      const mimeType = content.resource.mimeType;
      return `[resource] ${uri}${mimeType ? ` (${mimeType})` : ""}`;
    }
    case "image":
      return `[image] ${content.mimeType}`;
    case "audio":
      return `[audio] ${content.mimeType}`;
    default:
      return void 0;
  }
}
function summarizeToolContent(content) {
  if (!content || content.length === 0) {
    return void 0;
  }
  const fragments = [];
  for (const entry of content) {
    if (entry.type === "content") {
      const text = textFromContentBlock(entry.content);
      if (text && text.trim()) {
        fragments.push(text.trimEnd());
      }
      continue;
    }
    if (entry.type === "diff") {
      fragments.push(summarizeDiff(entry.path, entry.oldText, entry.newText));
      continue;
    }
    if (entry.type === "terminal") {
      fragments.push(`[terminal] ${entry.terminalId}`);
    }
  }
  const unique = dedupeStrings(
    fragments.map((fragment) => fragment.trim()).filter((fragment) => fragment.length > 0)
  );
  if (unique.length === 0) {
    return void 0;
  }
  return unique.join("\n\n");
}
function extractOutputText(value, depth = 0, seen = /* @__PURE__ */ new Set()) {
  if (value == null) {
    return void 0;
  }
  if (typeof value === "string") {
    const trimmed = value.trimEnd();
    return trimmed.length > 0 ? trimmed : void 0;
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  if (depth >= 4) {
    return void 0;
  }
  if (Array.isArray(value)) {
    const parts = value.map((entry) => extractOutputText(entry, depth + 1, seen)).filter((entry) => Boolean(entry));
    if (parts.length === 0) {
      return void 0;
    }
    return dedupeStrings(parts).join("\n");
  }
  const record = asRecord(value);
  if (!record) {
    return void 0;
  }
  if (seen.has(record)) {
    return void 0;
  }
  seen.add(record);
  const preferred = [];
  for (const key of OUTPUT_PRIORITY_KEYS) {
    if (!(key in record)) {
      continue;
    }
    const extracted = extractOutputText(record[key], depth + 1, seen);
    if (extracted) {
      preferred.push(extracted);
    }
  }
  const uniquePreferred = dedupeStrings(preferred);
  if (uniquePreferred.length > 0) {
    return uniquePreferred.join("\n");
  }
  const json = safeJson(record, 2);
  if (!json || json === "{}") {
    return void 0;
  }
  return json;
}
function summarizeToolOutput(rawOutput, content) {
  const outputFromRaw = extractOutputText(rawOutput);
  const outputFromContent = summarizeToolContent(content);
  const fragments = dedupeStrings(
    [outputFromRaw, outputFromContent].map((fragment) => fragment?.trim()).filter((fragment) => Boolean(fragment))
  );
  if (fragments.length === 0) {
    return void 0;
  }
  return fragments.join("\n\n");
}
function limitOutputBlock(value) {
  const normalized = value.replace(/\r\n/g, "\n").trim();
  if (!normalized) {
    return "";
  }
  const lines = normalized.split("\n");
  const visible = lines.slice(0, MAX_OUTPUT_LINES);
  let result = visible.join("\n");
  if (lines.length > visible.length) {
    const hidden = lines.length - visible.length;
    result += `
... (${hidden} more lines)`;
  }
  if (result.length > MAX_OUTPUT_CHARS) {
    result = `${result.slice(0, MAX_OUTPUT_CHARS - 3)}...`;
  }
  return result;
}
var TextOutputFormatter = class {
  stdout;
  useColor;
  toolStates = /* @__PURE__ */ new Map();
  thoughtBuffer = "";
  wroteAny = false;
  atLineStart = true;
  section = null;
  constructor(stdout) {
    this.stdout = stdout;
    this.useColor = Boolean(stdout.isTTY);
  }
  setContext(_context) {
  }
  onEvent(event) {
    if (event.type === ACPX_EVENT_TYPES.OUTPUT_DELTA) {
      if (event.data.stream === "output") {
        this.flushThoughtBuffer();
        this.writeAssistantChunk(event.data.text);
        return;
      }
      this.thoughtBuffer += event.data.text;
      return;
    }
    if (event.type === ACPX_EVENT_TYPES.TOOL_CALL) {
      this.flushThoughtBuffer();
      this.renderToolUpdate({
        sessionUpdate: "tool_call_update",
        toolCallId: event.data.tool_call_id ?? "tool_call",
        title: event.data.title,
        status: event.data.status
      });
      return;
    }
    if (event.type === ACPX_EVENT_TYPES.PLAN) {
      this.flushThoughtBuffer();
      this.beginSection("plan");
      this.writeLine(this.bold("[plan]"));
      for (const entry of event.data.entries) {
        this.writeLine(`  - [${entry.status}] ${entry.content}`);
      }
      return;
    }
    if (event.type === ACPX_EVENT_TYPES.CLIENT_OPERATION) {
      this.onClientOperation({
        method: event.data.method,
        status: event.data.status,
        summary: event.data.summary,
        details: event.data.details,
        timestamp: event.ts
      });
      return;
    }
    if (event.type === ACPX_EVENT_TYPES.TURN_DONE) {
      this.onDone(event.data.stop_reason);
      return;
    }
    if (event.type === ACPX_EVENT_TYPES.ERROR) {
      this.onError({
        code: event.data.code,
        detailCode: event.data.detail_code,
        origin: event.data.origin,
        message: event.data.message,
        retryable: event.data.retryable,
        acp: event.data.acp_error,
        timestamp: event.ts
      });
    }
  }
  onSessionUpdate(notification) {
    const update = notification.update;
    if (update.sessionUpdate !== "agent_thought_chunk") {
      this.flushThoughtBuffer();
    }
    switch (update.sessionUpdate) {
      case "agent_message_chunk": {
        if (update.content.type === "text") {
          this.writeAssistantChunk(update.content.text);
        }
        return;
      }
      case "agent_thought_chunk": {
        if (update.content.type === "text") {
          this.thoughtBuffer += update.content.text;
        }
        return;
      }
      case "tool_call": {
        this.renderToolUpdate(update);
        return;
      }
      case "tool_call_update": {
        this.renderToolUpdate(update);
        return;
      }
      case "plan": {
        this.beginSection("plan");
        this.writeLine(this.bold("[plan]"));
        for (const entry of update.entries) {
          this.writeLine(`  - [${entry.status}] ${entry.content}`);
        }
        return;
      }
      default:
        return;
    }
  }
  onDone(stopReason) {
    this.flushThoughtBuffer();
    this.beginSection("done");
    this.writeLine(this.dim(`[done] ${stopReason}`));
  }
  onError(params) {
    this.flushThoughtBuffer();
    this.beginSection("done");
    this.writeLine(this.formatAnsi(`[error] ${params.code}: ${params.message}`, "31"));
  }
  onClientOperation(operation) {
    this.flushThoughtBuffer();
    this.beginSection("client");
    const normalizedStatus = operation.status === "completed" ? "completed" : operation.status === "failed" ? "failed" : "in_progress";
    const statusText = this.colorStatus(operation.status, normalizedStatus);
    this.writeLine(`${this.bold("[client]")} ${operation.summary} (${statusText})`);
    if (operation.details && operation.details.trim().length > 0) {
      this.writeLine("  details:");
      this.writeLine(indentBlock(operation.details, "    "));
    }
  }
  flush() {
    this.flushThoughtBuffer();
    if (!this.atLineStart) {
      this.write("\n");
    }
  }
  write(chunk) {
    if (!chunk) {
      return;
    }
    this.stdout.write(chunk);
    this.wroteAny = true;
    this.atLineStart = chunk.endsWith("\n");
  }
  writeLine(line) {
    this.write(`${line}
`);
  }
  beginSection(next) {
    if (!this.atLineStart) {
      this.write("\n");
    }
    if (this.wroteAny) {
      this.write("\n");
    }
    this.section = next;
  }
  writeAssistantChunk(text) {
    if (!text) {
      return;
    }
    this.section = "assistant";
    this.write(text);
  }
  flushThoughtBuffer() {
    const thought = truncate(collapseWhitespace(this.thoughtBuffer), MAX_THOUGHT_CHARS);
    this.thoughtBuffer = "";
    if (!thought) {
      return;
    }
    this.beginSection("thought");
    this.writeLine(this.dim(`[thinking] ${thought}`));
  }
  renderToolUpdate(update) {
    const state = this.getOrCreateToolState(update.toolCallId);
    this.mergeToolState(state, update);
    const status = asStatus(state.status);
    if (isFinalStatus(status)) {
      const signature = this.toolSignature(state);
      if (signature !== state.finalSignature) {
        state.finalSignature = signature;
        this.renderFinalToolState(state, status);
      }
      return;
    }
    if (state.startedPrinted) {
      return;
    }
    state.startedPrinted = true;
    this.renderStartingToolState(state, status);
  }
  getOrCreateToolState(toolCallId) {
    const existing = this.toolStates.get(toolCallId);
    if (existing) {
      return existing;
    }
    const created = {
      id: toolCallId,
      startedPrinted: false
    };
    this.toolStates.set(toolCallId, created);
    return created;
  }
  mergeToolState(state, update) {
    if (typeof update.title === "string" && update.title.trim().length > 0) {
      state.title = update.title;
    }
    if (update.status !== void 0) {
      state.status = update.status;
    }
    if (update.kind !== void 0) {
      state.kind = update.kind;
    }
    if (update.locations !== void 0) {
      state.locations = update.locations;
    }
    if (update.rawInput !== void 0) {
      state.rawInput = update.rawInput;
    }
    if (update.rawOutput !== void 0) {
      state.rawOutput = update.rawOutput;
    }
    if (update.content !== void 0) {
      state.content = update.content;
    }
  }
  toolSignature(state) {
    const signaturePayload = {
      title: state.title,
      status: state.status,
      kind: state.kind,
      input: summarizeToolInput(state.rawInput),
      files: formatLocations(state.locations),
      output: summarizeToolOutput(state.rawOutput, state.content)
    };
    return safeJson(signaturePayload, 0) ?? JSON.stringify(signaturePayload);
  }
  renderStartingToolState(state, status) {
    this.beginSection("tool");
    const title = state.title ?? state.id;
    const label = status === "pending" ? "pending" : "running";
    const statusText = this.colorStatus(label, status);
    this.writeLine(`${this.bold("[tool]")} ${title} (${statusText})`);
    const input = summarizeToolInput(state.rawInput);
    if (input) {
      this.writeLine(`  input: ${input}`);
    }
    const files = formatLocations(state.locations);
    if (files) {
      this.writeLine(`  files: ${files}`);
    }
  }
  renderFinalToolState(state, status) {
    this.beginSection("tool");
    const title = state.title ?? state.id;
    const statusText = this.colorStatus(toStatusLabel(status), status);
    this.writeLine(`${this.bold("[tool]")} ${title} (${statusText})`);
    if (state.kind) {
      this.writeLine(`  kind: ${state.kind}`);
    }
    const input = summarizeToolInput(state.rawInput);
    if (input) {
      this.writeLine(`  input: ${input}`);
    }
    const files = formatLocations(state.locations);
    if (files) {
      this.writeLine(`  files: ${files}`);
    }
    const output = summarizeToolOutput(state.rawOutput, state.content);
    if (output) {
      this.writeLine("  output:");
      this.writeLine(indentBlock(limitOutputBlock(output), "    "));
    }
  }
  formatAnsi(text, code) {
    if (!this.useColor) {
      return text;
    }
    return `\x1B[${code}m${text}\x1B[0m`;
  }
  bold(text) {
    return this.formatAnsi(text, "1");
  }
  dim(text) {
    return this.formatAnsi(text, "2");
  }
  colorStatus(text, status) {
    if (!this.useColor) {
      return text;
    }
    switch (status) {
      case "completed":
        return this.formatAnsi(text, "32");
      case "failed":
        return this.formatAnsi(text, "31");
      case "pending":
      case "in_progress":
      case "unknown":
      default:
        return this.formatAnsi(text, "33");
    }
  }
};
var JsonOutputFormatter = class {
  stdout;
  sessionId;
  acpSessionId;
  agentSessionId;
  requestId;
  nextSeq = 0;
  constructor(stdout, context) {
    this.stdout = stdout;
    this.sessionId = context?.sessionId?.trim() || DEFAULT_JSON_SESSION_ID;
    this.acpSessionId = context?.acpSessionId?.trim() || void 0;
    this.agentSessionId = context?.agentSessionId?.trim() || void 0;
    this.requestId = context?.requestId?.trim() || void 0;
    this.nextSeq = context?.nextSeq ?? 0;
  }
  setContext(context) {
    this.sessionId = context.sessionId?.trim() || this.sessionId || DEFAULT_JSON_SESSION_ID;
    this.acpSessionId = context.acpSessionId?.trim() || this.acpSessionId;
    this.agentSessionId = context.agentSessionId?.trim() || this.agentSessionId;
    this.requestId = context.requestId?.trim() || this.requestId;
    if (typeof context.nextSeq === "number" && Number.isInteger(context.nextSeq) && context.nextSeq >= 0) {
      this.nextSeq = context.nextSeq;
    }
  }
  onEvent(event) {
    if (!isAcpxEvent(event)) {
      throw new Error("Attempted to render invalid acpx.event.v1 payload");
    }
    this.sessionId = event.session_id || this.sessionId;
    this.acpSessionId = event.acp_session_id || this.acpSessionId;
    this.agentSessionId = event.agent_session_id || this.agentSessionId;
    this.requestId = event.request_id || this.requestId;
    this.nextSeq = event.seq + 1;
    this.stdout.write(JSON.stringify(event) + "\n");
  }
  onSessionUpdate(notification) {
    for (const draft of sessionUpdateToEventDrafts(notification)) {
      this.emitDraft(draft);
    }
  }
  onDone(stopReason) {
    this.emitDraft({
      type: ACPX_EVENT_TYPES.TURN_DONE,
      data: {
        stop_reason: stopReason
      }
    });
  }
  onError(params) {
    this.emitDraft(
      errorToEventDraft({
        code: params.code,
        detailCode: params.detailCode,
        origin: params.origin,
        message: params.message,
        retryable: params.retryable,
        acp: params.acp
      })
    );
  }
  onClientOperation(operation) {
    this.emitDraft(clientOperationToEventDraft(operation));
  }
  flush() {
  }
  emitDraft(draft) {
    const event = createAcpxEvent(
      {
        sessionId: this.sessionId || DEFAULT_JSON_SESSION_ID,
        acpSessionId: this.acpSessionId,
        agentSessionId: this.agentSessionId,
        requestId: this.requestId,
        seq: this.nextSeq
      },
      draft
    );
    if (!isAcpxEvent(event)) {
      throw new Error("Attempted to render invalid acpx.event.v1 payload");
    }
    this.nextSeq += 1;
    this.stdout.write(JSON.stringify(event) + "\n");
  }
};
var QuietOutputFormatter = class {
  stdout;
  chunks = [];
  sawEventOutput = false;
  sawSessionUpdateOutput = false;
  flushed = false;
  constructor(stdout) {
    this.stdout = stdout;
  }
  setContext(_context) {
  }
  onEvent(event) {
    if (event.type === ACPX_EVENT_TYPES.OUTPUT_DELTA) {
      if (event.data.stream !== "output") {
        return;
      }
      if (!this.sawEventOutput && this.sawSessionUpdateOutput) {
        this.chunks = [];
      }
      this.sawEventOutput = true;
      this.chunks.push(event.data.text);
      return;
    }
    if (event.type === ACPX_EVENT_TYPES.TURN_DONE) {
      this.flushBufferedOutput();
    }
  }
  onSessionUpdate(notification) {
    if (this.sawEventOutput) {
      return;
    }
    const update = notification.update;
    if (update.sessionUpdate !== "agent_message_chunk") {
      return;
    }
    if (update.content.type !== "text") {
      return;
    }
    this.sawSessionUpdateOutput = true;
    this.chunks.push(update.content.text);
  }
  onDone(_stopReason) {
    this.flushBufferedOutput();
  }
  onError(_params) {
  }
  onClientOperation(_operation) {
  }
  flush() {
  }
  flushBufferedOutput() {
    if (this.flushed) {
      return;
    }
    this.flushed = true;
    const text = this.chunks.join("");
    this.stdout.write(text.endsWith("\n") ? text : `${text}
`);
  }
};
function createOutputFormatter(format, options = {}) {
  const stdout = options.stdout ?? process.stdout;
  switch (format) {
    case "text":
      return new TextOutputFormatter(stdout);
    case "json":
      return new JsonOutputFormatter(stdout, options.jsonContext);
    case "quiet":
      return new QuietOutputFormatter(stdout);
    default: {
      const exhaustive = format;
      throw new Error(`Unsupported output format: ${exhaustive}`);
    }
  }
}

// src/cli-core.ts
var NoSessionError = class extends Error {
  constructor(message) {
    super(message);
    this.name = "NoSessionError";
  }
};
var TOP_LEVEL_VERBS = /* @__PURE__ */ new Set([
  "prompt",
  "exec",
  "cancel",
  "set-mode",
  "set",
  "sessions",
  "status",
  "config",
  "help"
]);
async function readPromptInputFromStdin() {
  let data = "";
  for await (const chunk of process.stdin) {
    data += String(chunk);
  }
  return data;
}
async function readPrompt(promptParts, filePath, cwd) {
  if (filePath) {
    const source = filePath === "-" ? await readPromptInputFromStdin() : await fs2.readFile(path3.resolve(cwd, filePath), "utf8");
    const pieces = [source.trim(), promptParts.join(" ").trim()].filter(
      (value) => value.length > 0
    );
    const prompt2 = pieces.join("\n\n").trim();
    if (!prompt2) {
      throw new InvalidArgumentError2("Prompt from --file is empty");
    }
    return prompt2;
  }
  const joined = promptParts.join(" ").trim();
  if (joined.length > 0) {
    return joined;
  }
  if (process.stdin.isTTY) {
    throw new InvalidArgumentError2(
      "Prompt is required (pass as argument, --file, or pipe via stdin)"
    );
  }
  const prompt = (await readPromptInputFromStdin()).trim();
  if (!prompt) {
    throw new InvalidArgumentError2("Prompt from stdin is empty");
  }
  return prompt;
}
function applyPermissionExitCode(result) {
  const stats = result.permissionStats;
  const deniedOrCancelled = stats.denied + stats.cancelled;
  if (stats.requested > 0 && stats.approved === 0 && deniedOrCancelled > 0) {
    process.exitCode = EXIT_CODES.PERMISSION_DENIED;
  }
}
function printSessionsByFormat(sessions, format) {
  if (format === "json") {
    process.stdout.write(`${JSON.stringify(sessions)}
`);
    return;
  }
  if (format === "quiet") {
    for (const session of sessions) {
      const closedMarker = session.closed ? " [closed]" : "";
      process.stdout.write(`${session.acpxRecordId}${closedMarker}
`);
    }
    return;
  }
  if (sessions.length === 0) {
    process.stdout.write("No sessions\n");
    return;
  }
  for (const session of sessions) {
    const closedMarker = session.closed ? " [closed]" : "";
    process.stdout.write(
      `${session.acpxRecordId}${closedMarker}	${session.name ?? "-"}	${session.cwd}	${session.lastUsedAt}
`
    );
  }
}
function jsonEventIdentityFromRecord(record, overrides = {}) {
  return {
    sessionId: record.acpxRecordId,
    acpSessionId: record.acpSessionId,
    agentSessionId: record.agentSessionId,
    requestId: overrides.requestId,
    nextSeq: overrides.nextSeq ?? record.lastSeq + 1
  };
}
function emitControlEvent(format, identity, draft) {
  if (format !== "json") {
    return false;
  }
  const event = createAcpxEvent(
    {
      sessionId: identity.sessionId,
      acpSessionId: identity.acpSessionId,
      agentSessionId: identity.agentSessionId,
      requestId: identity.requestId,
      seq: identity.nextSeq
    },
    draft
  );
  process.stdout.write(`${JSON.stringify(event)}
`);
  return true;
}
function printClosedSessionByFormat(record, format) {
  if (emitControlEvent(format, jsonEventIdentityFromRecord(record), {
    type: ACPX_EVENT_TYPES.SESSION_CLOSED,
    data: {
      reason: "close"
    }
  })) {
    return;
  }
  if (format === "quiet") {
    return;
  }
  process.stdout.write(`${record.acpxRecordId}
`);
}
function agentSessionIdPayload(agentSessionId) {
  const normalized = normalizeRuntimeSessionId(agentSessionId);
  if (!normalized) {
    return {};
  }
  return { agentSessionId: normalized };
}
function printNewSessionByFormat(record, replaced, format) {
  if (emitControlEvent(format, jsonEventIdentityFromRecord(record), {
    type: ACPX_EVENT_TYPES.SESSION_ENSURED,
    data: {
      created: true,
      name: record.name,
      replaced_session_id: replaced?.acpxRecordId
    }
  })) {
    return;
  }
  if (format === "quiet") {
    process.stdout.write(`${record.acpxRecordId}
`);
    return;
  }
  if (replaced) {
    process.stdout.write(
      `${record.acpxRecordId}	(replaced ${replaced.acpxRecordId})
`
    );
    return;
  }
  process.stdout.write(`${record.acpxRecordId}
`);
}
function printEnsuredSessionByFormat(record, created, format) {
  if (emitControlEvent(format, jsonEventIdentityFromRecord(record), {
    type: ACPX_EVENT_TYPES.SESSION_ENSURED,
    data: {
      created,
      name: record.name
    }
  })) {
    return;
  }
  if (format === "quiet") {
    process.stdout.write(`${record.acpxRecordId}
`);
    return;
  }
  const action = created ? "created" : "existing";
  process.stdout.write(`${record.acpxRecordId}	(${action})
`);
}
function printQueuedPromptByFormat(result, format) {
  if (emitControlEvent(
    format,
    {
      sessionId: result.sessionId,
      requestId: result.requestId,
      nextSeq: 0
    },
    {
      type: ACPX_EVENT_TYPES.PROMPT_QUEUED,
      data: {
        request_id: result.requestId
      }
    }
  )) {
    return;
  }
  if (format === "quiet") {
    return;
  }
  process.stdout.write(`[queued] ${result.requestId}
`);
}
function formatSessionLabel(record) {
  return record.name ?? "cwd";
}
function formatRoutedFrom(sessionCwd, currentCwd) {
  const relative = path3.relative(sessionCwd, currentCwd);
  if (!relative || relative === ".") {
    return void 0;
  }
  return relative.startsWith(".") ? relative : `.${path3.sep}${relative}`;
}
async function resolveSessionConnectionStatus(record) {
  const health = await probeQueueOwnerHealth(record.acpxRecordId);
  return health.healthy ? "connected" : "needs reconnect";
}
function formatPromptSessionBannerLine(record, currentCwd, connectionStatus = "needs reconnect") {
  const label = formatSessionLabel(record);
  const normalizedSessionCwd = path3.resolve(record.cwd);
  const normalizedCurrentCwd = path3.resolve(currentCwd);
  const routedFrom = normalizedSessionCwd === normalizedCurrentCwd ? void 0 : formatRoutedFrom(normalizedSessionCwd, normalizedCurrentCwd);
  const status = connectionStatus;
  if (routedFrom) {
    return `[acpx] session ${label} (${record.acpxRecordId}) \xB7 ${normalizedSessionCwd} (routed from ${routedFrom}) \xB7 agent ${status}`;
  }
  return `[acpx] session ${label} (${record.acpxRecordId}) \xB7 ${normalizedSessionCwd} \xB7 agent ${status}`;
}
async function printPromptSessionBanner(record, currentCwd, format, jsonStrict = false) {
  if (format === "quiet" || jsonStrict && format === "json") {
    return;
  }
  const status = await resolveSessionConnectionStatus(record);
  process.stderr.write(
    `${formatPromptSessionBannerLine(record, currentCwd, status)}
`
  );
}
function printCreatedSessionBanner(record, agentName, format, jsonStrict = false) {
  if (format === "quiet" || jsonStrict && format === "json") {
    return;
  }
  const label = formatSessionLabel(record);
  process.stderr.write(`[acpx] created session ${label} (${record.acpxRecordId})
`);
  process.stderr.write(`[acpx] agent: ${agentName}
`);
  process.stderr.write(`[acpx] cwd: ${record.cwd}
`);
}
async function findRoutedSessionOrThrow(agentCommand, agentName, cwd, sessionName) {
  const gitRoot = findGitRepositoryRoot(cwd);
  const walkBoundary = gitRoot ?? cwd;
  const record = await findSessionByDirectoryWalk({
    agentCommand,
    cwd,
    name: sessionName,
    boundary: walkBoundary
  });
  if (record) {
    return record;
  }
  const createCmd = sessionName ? `acpx ${agentName} sessions new --name ${sessionName}` : `acpx ${agentName} sessions new`;
  throw new NoSessionError(
    `\u26A0 No acpx session found (searched up to ${walkBoundary}).
Create one: ${createCmd}`
  );
}
async function handlePrompt(explicitAgentName, promptParts, flags, command, config) {
  const globalFlags = resolveGlobalFlags(command, config);
  const outputPolicy = resolveOutputPolicy(
    globalFlags.format,
    globalFlags.jsonStrict === true
  );
  const permissionMode = resolvePermissionMode(globalFlags, config.defaultPermissions);
  const prompt = await readPrompt(promptParts, flags.file, globalFlags.cwd);
  const agent = resolveAgentInvocation(explicitAgentName, globalFlags, config);
  const record = await findRoutedSessionOrThrow(
    agent.agentCommand,
    agent.agentName,
    agent.cwd,
    flags.session
  );
  const outputFormatter = createOutputFormatter(outputPolicy.format, {
    jsonContext: {
      sessionId: record.acpxRecordId
    }
  });
  await printPromptSessionBanner(
    record,
    agent.cwd,
    outputPolicy.format,
    outputPolicy.jsonStrict
  );
  const result = await sendSession({
    sessionId: record.acpxRecordId,
    message: prompt,
    permissionMode,
    nonInteractivePermissions: globalFlags.nonInteractivePermissions,
    authCredentials: config.auth,
    authPolicy: globalFlags.authPolicy,
    outputFormatter,
    errorEmissionPolicy: {
      queueErrorAlreadyEmitted: outputPolicy.queueErrorAlreadyEmitted
    },
    suppressSdkConsoleErrors: outputPolicy.suppressSdkConsoleErrors,
    timeoutMs: globalFlags.timeout,
    ttlMs: globalFlags.ttl,
    verbose: globalFlags.verbose,
    waitForCompletion: flags.wait !== false
  });
  if ("queued" in result) {
    printQueuedPromptByFormat(result, outputPolicy.format);
    return;
  }
  applyPermissionExitCode(result);
  if (globalFlags.verbose && result.loadError) {
    process.stderr.write(
      `[acpx] loadSession failed, started fresh session: ${result.loadError}
`
    );
  }
}
async function handleExec(explicitAgentName, promptParts, flags, command, config) {
  const globalFlags = resolveGlobalFlags(command, config);
  const outputPolicy = resolveOutputPolicy(
    globalFlags.format,
    globalFlags.jsonStrict === true
  );
  const permissionMode = resolvePermissionMode(globalFlags, config.defaultPermissions);
  const prompt = await readPrompt(promptParts, flags.file, globalFlags.cwd);
  const outputFormatter = createOutputFormatter(outputPolicy.format);
  const agent = resolveAgentInvocation(explicitAgentName, globalFlags, config);
  const result = await runOnce({
    agentCommand: agent.agentCommand,
    cwd: agent.cwd,
    message: prompt,
    permissionMode,
    nonInteractivePermissions: globalFlags.nonInteractivePermissions,
    authCredentials: config.auth,
    authPolicy: globalFlags.authPolicy,
    outputFormatter,
    suppressSdkConsoleErrors: outputPolicy.suppressSdkConsoleErrors,
    timeoutMs: globalFlags.timeout,
    verbose: globalFlags.verbose
  });
  applyPermissionExitCode(result);
}
function printCancelResultByFormat(result, format) {
  if (emitControlEvent(
    format,
    {
      sessionId: result.sessionId || "unknown",
      nextSeq: 0
    },
    {
      type: ACPX_EVENT_TYPES.CANCEL_RESULT,
      data: {
        cancelled: result.cancelled
      }
    }
  )) {
    return;
  }
  if (result.cancelled) {
    process.stdout.write("cancel requested\n");
    return;
  }
  process.stdout.write("nothing to cancel\n");
}
function printSetModeResultByFormat(modeId, result, format) {
  if (emitControlEvent(format, jsonEventIdentityFromRecord(result.record), {
    type: ACPX_EVENT_TYPES.MODE_SET,
    data: {
      mode_id: modeId,
      resumed: result.resumed
    }
  })) {
    return;
  }
  if (format === "quiet") {
    process.stdout.write(`${modeId}
`);
    return;
  }
  process.stdout.write(`mode set: ${modeId}
`);
}
function printSetConfigOptionResultByFormat(configId, value, result, format) {
  if (emitControlEvent(format, jsonEventIdentityFromRecord(result.record), {
    type: ACPX_EVENT_TYPES.CONFIG_SET,
    data: {
      config_id: configId,
      value,
      resumed: result.resumed,
      config_options: result.response.configOptions
    }
  })) {
    return;
  }
  if (format === "quiet") {
    process.stdout.write(`${value}
`);
    return;
  }
  process.stdout.write(
    `config set: ${configId}=${value} (${result.response.configOptions.length} options)
`
  );
}
async function handleCancel(explicitAgentName, flags, command, config) {
  const globalFlags = resolveGlobalFlags(command, config);
  const agent = resolveAgentInvocation(explicitAgentName, globalFlags, config);
  const gitRoot = findGitRepositoryRoot(agent.cwd);
  const walkBoundary = gitRoot ?? agent.cwd;
  const record = await findSessionByDirectoryWalk({
    agentCommand: agent.agentCommand,
    cwd: agent.cwd,
    name: resolveSessionNameFromFlags(flags, command),
    boundary: walkBoundary
  });
  if (!record) {
    printCancelResultByFormat(
      {
        sessionId: "",
        cancelled: false
      },
      globalFlags.format
    );
    return;
  }
  const result = await cancelSessionPrompt({
    sessionId: record.acpxRecordId,
    verbose: globalFlags.verbose
  });
  printCancelResultByFormat(result, globalFlags.format);
}
async function handleSetMode(explicitAgentName, modeId, flags, command, config) {
  const globalFlags = resolveGlobalFlags(command, config);
  const agent = resolveAgentInvocation(explicitAgentName, globalFlags, config);
  const record = await findRoutedSessionOrThrow(
    agent.agentCommand,
    agent.agentName,
    agent.cwd,
    resolveSessionNameFromFlags(flags, command)
  );
  const result = await setSessionMode({
    sessionId: record.acpxRecordId,
    modeId,
    nonInteractivePermissions: globalFlags.nonInteractivePermissions,
    authCredentials: config.auth,
    authPolicy: globalFlags.authPolicy,
    timeoutMs: globalFlags.timeout,
    verbose: globalFlags.verbose
  });
  if (globalFlags.verbose && result.loadError) {
    process.stderr.write(
      `[acpx] loadSession failed, started fresh session: ${result.loadError}
`
    );
  }
  printSetModeResultByFormat(modeId, result, globalFlags.format);
}
async function handleSetConfigOption(explicitAgentName, configId, value, flags, command, config) {
  const globalFlags = resolveGlobalFlags(command, config);
  const agent = resolveAgentInvocation(explicitAgentName, globalFlags, config);
  const record = await findRoutedSessionOrThrow(
    agent.agentCommand,
    agent.agentName,
    agent.cwd,
    resolveSessionNameFromFlags(flags, command)
  );
  const result = await setSessionConfigOption({
    sessionId: record.acpxRecordId,
    configId,
    value,
    nonInteractivePermissions: globalFlags.nonInteractivePermissions,
    authCredentials: config.auth,
    authPolicy: globalFlags.authPolicy,
    timeoutMs: globalFlags.timeout,
    verbose: globalFlags.verbose
  });
  if (globalFlags.verbose && result.loadError) {
    process.stderr.write(
      `[acpx] loadSession failed, started fresh session: ${result.loadError}
`
    );
  }
  printSetConfigOptionResultByFormat(configId, value, result, globalFlags.format);
}
async function handleSessionsList(explicitAgentName, command, config) {
  const globalFlags = resolveGlobalFlags(command, config);
  const agent = resolveAgentInvocation(explicitAgentName, globalFlags, config);
  const sessions = await listSessionsForAgent(agent.agentCommand);
  printSessionsByFormat(sessions, globalFlags.format);
}
async function handleSessionsClose(explicitAgentName, sessionName, command, config) {
  const globalFlags = resolveGlobalFlags(command, config);
  const agent = resolveAgentInvocation(explicitAgentName, globalFlags, config);
  const record = await findSession({
    agentCommand: agent.agentCommand,
    cwd: agent.cwd,
    name: sessionName
  });
  if (!record) {
    if (sessionName) {
      throw new Error(
        `No named session "${sessionName}" for cwd ${agent.cwd} and agent ${agent.agentName}`
      );
    }
    throw new Error(`No cwd session for ${agent.cwd} and agent ${agent.agentName}`);
  }
  const closed = await closeSession(record.acpxRecordId);
  printClosedSessionByFormat(closed, globalFlags.format);
}
async function handleSessionsNew(explicitAgentName, flags, command, config) {
  const globalFlags = resolveGlobalFlags(command, config);
  const permissionMode = resolvePermissionMode(globalFlags, config.defaultPermissions);
  const agent = resolveAgentInvocation(explicitAgentName, globalFlags, config);
  const replaced = await findSession({
    agentCommand: agent.agentCommand,
    cwd: agent.cwd,
    name: flags.name
  });
  if (replaced) {
    await closeSession(replaced.acpxRecordId);
    if (globalFlags.verbose) {
      process.stderr.write(
        `[acpx] soft-closed prior session: ${replaced.acpxRecordId}
`
      );
    }
  }
  const created = await createSession({
    agentCommand: agent.agentCommand,
    cwd: agent.cwd,
    name: flags.name,
    permissionMode,
    nonInteractivePermissions: globalFlags.nonInteractivePermissions,
    authCredentials: config.auth,
    authPolicy: globalFlags.authPolicy,
    timeoutMs: globalFlags.timeout,
    verbose: globalFlags.verbose
  });
  printCreatedSessionBanner(
    created,
    agent.agentName,
    globalFlags.format,
    globalFlags.jsonStrict
  );
  if (globalFlags.verbose) {
    const scope = flags.name ? `named session "${flags.name}"` : "cwd session";
    process.stderr.write(`[acpx] created ${scope}: ${created.acpxRecordId}
`);
  }
  printNewSessionByFormat(created, replaced, globalFlags.format);
}
async function handleSessionsEnsure(explicitAgentName, flags, command, config) {
  const globalFlags = resolveGlobalFlags(command, config);
  const permissionMode = resolvePermissionMode(globalFlags, config.defaultPermissions);
  const agent = resolveAgentInvocation(explicitAgentName, globalFlags, config);
  const result = await ensureSession({
    agentCommand: agent.agentCommand,
    cwd: agent.cwd,
    name: flags.name,
    permissionMode,
    nonInteractivePermissions: globalFlags.nonInteractivePermissions,
    authCredentials: config.auth,
    authPolicy: globalFlags.authPolicy,
    timeoutMs: globalFlags.timeout,
    verbose: globalFlags.verbose
  });
  if (result.created) {
    printCreatedSessionBanner(
      result.record,
      agent.agentName,
      globalFlags.format,
      globalFlags.jsonStrict
    );
  }
  printEnsuredSessionByFormat(result.record, result.created, globalFlags.format);
}
function userContentToText(content) {
  if ("Text" in content) {
    return content.Text;
  }
  if ("Mention" in content) {
    return content.Mention.content;
  }
  if ("Image" in content) {
    return content.Image.source || "[image]";
  }
  return "";
}
function agentContentToText(content) {
  if ("Text" in content) {
    return content.Text;
  }
  if ("Thinking" in content) {
    return content.Thinking.text;
  }
  if ("RedactedThinking" in content) {
    return "[redacted_thinking]";
  }
  if ("ToolUse" in content) {
    return `[tool:${content.ToolUse.name}]`;
  }
  return "";
}
function conversationHistoryEntries(record) {
  const entries = [];
  for (const message of record.messages) {
    if (message === "Resume") {
      continue;
    }
    if ("User" in message) {
      const text = message.User.content.map((entry) => userContentToText(entry)).join(" ").trim();
      if (!text) {
        continue;
      }
      entries.push({
        role: "user",
        timestamp: record.updated_at,
        textPreview: text
      });
      continue;
    }
    if ("Agent" in message) {
      const text = message.Agent.content.map((entry) => agentContentToText(entry)).join(" ").trim();
      if (!text) {
        continue;
      }
      entries.push({
        role: "assistant",
        timestamp: record.updated_at,
        textPreview: text
      });
    }
  }
  return entries;
}
function printSessionDetailsByFormat(record, format) {
  if (format === "json") {
    process.stdout.write(`${JSON.stringify(record)}
`);
    return;
  }
  if (format === "quiet") {
    process.stdout.write(`${record.acpxRecordId}
`);
    return;
  }
  process.stdout.write(`id: ${record.acpxRecordId}
`);
  process.stdout.write(`sessionId: ${record.acpSessionId}
`);
  process.stdout.write(`agentSessionId: ${record.agentSessionId ?? "-"}
`);
  process.stdout.write(`agent: ${record.agentCommand}
`);
  process.stdout.write(`cwd: ${record.cwd}
`);
  process.stdout.write(`name: ${record.name ?? "-"}
`);
  process.stdout.write(`created: ${record.createdAt}
`);
  process.stdout.write(`lastActivity: ${record.lastUsedAt}
`);
  process.stdout.write(`lastPrompt: ${record.lastPromptAt ?? "-"}
`);
  process.stdout.write(`closed: ${record.closed ? "yes" : "no"}
`);
  process.stdout.write(`closedAt: ${record.closedAt ?? "-"}
`);
  process.stdout.write(`pid: ${record.pid ?? "-"}
`);
  process.stdout.write(`agentStartedAt: ${record.agentStartedAt ?? "-"}
`);
  process.stdout.write(`lastExitCode: ${record.lastAgentExitCode ?? "-"}
`);
  process.stdout.write(`lastExitSignal: ${record.lastAgentExitSignal ?? "-"}
`);
  process.stdout.write(`lastExitAt: ${record.lastAgentExitAt ?? "-"}
`);
  process.stdout.write(
    `disconnectReason: ${record.lastAgentDisconnectReason ?? "-"}
`
  );
  process.stdout.write(
    `historyEntries: ${conversationHistoryEntries(record).length}
`
  );
}
function printSessionHistoryByFormat(record, limit, format) {
  const history = conversationHistoryEntries(record);
  const visible = history.slice(Math.max(0, history.length - limit));
  if (format === "json") {
    process.stdout.write(
      `${JSON.stringify({
        id: record.acpxRecordId,
        sessionId: record.acpSessionId,
        limit,
        count: visible.length,
        entries: visible
      })}
`
    );
    return;
  }
  if (format === "quiet") {
    for (const entry of visible) {
      process.stdout.write(`${entry.textPreview}
`);
    }
    return;
  }
  process.stdout.write(
    `session: ${record.acpxRecordId} (${visible.length}/${history.length} shown)
`
  );
  if (visible.length === 0) {
    process.stdout.write("No history\n");
    return;
  }
  for (const entry of visible) {
    process.stdout.write(`${entry.timestamp}	${entry.role}	${entry.textPreview}
`);
  }
}
async function handleSessionsShow(explicitAgentName, sessionName, command, config) {
  const globalFlags = resolveGlobalFlags(command, config);
  const agent = resolveAgentInvocation(explicitAgentName, globalFlags, config);
  const record = await findSession({
    agentCommand: agent.agentCommand,
    cwd: agent.cwd,
    name: sessionName,
    includeClosed: true
  });
  if (!record) {
    throw new Error(
      sessionName ? `No named session "${sessionName}" for cwd ${agent.cwd} and agent ${agent.agentName}` : `No cwd session for ${agent.cwd} and agent ${agent.agentName}`
    );
  }
  printSessionDetailsByFormat(record, globalFlags.format);
}
async function handleSessionsHistory(explicitAgentName, sessionName, flags, command, config) {
  const globalFlags = resolveGlobalFlags(command, config);
  const agent = resolveAgentInvocation(explicitAgentName, globalFlags, config);
  const record = await findSession({
    agentCommand: agent.agentCommand,
    cwd: agent.cwd,
    name: sessionName,
    includeClosed: true
  });
  if (!record) {
    throw new Error(
      sessionName ? `No named session "${sessionName}" for cwd ${agent.cwd} and agent ${agent.agentName}` : `No cwd session for ${agent.cwd} and agent ${agent.agentName}`
    );
  }
  printSessionHistoryByFormat(record, flags.limit, globalFlags.format);
}
function formatUptime(startedAt) {
  if (!startedAt) {
    return void 0;
  }
  const startedMs = Date.parse(startedAt);
  if (!Number.isFinite(startedMs)) {
    return void 0;
  }
  const elapsedMs = Math.max(0, Date.now() - startedMs);
  const seconds = Math.floor(elapsedMs / 1e3);
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor(seconds % 3600 / 60);
  const remSeconds = seconds % 60;
  return `${hours.toString().padStart(2, "0")}:${minutes.toString().padStart(2, "0")}:${remSeconds.toString().padStart(2, "0")}`;
}
async function handleStatus(explicitAgentName, flags, command, config) {
  const globalFlags = resolveGlobalFlags(command, config);
  const agent = resolveAgentInvocation(explicitAgentName, globalFlags, config);
  const record = await findSession({
    agentCommand: agent.agentCommand,
    cwd: agent.cwd,
    name: resolveSessionNameFromFlags(flags, command)
  });
  if (!record) {
    if (emitControlEvent(
      globalFlags.format,
      {
        sessionId: "unknown",
        nextSeq: 0
      },
      {
        type: ACPX_EVENT_TYPES.STATUS_SNAPSHOT,
        data: {
          status: "no-session",
          summary: "no active session"
        }
      }
    )) {
      return;
    }
    if (globalFlags.format === "quiet") {
      process.stdout.write("no-session\n");
      return;
    }
    process.stdout.write(`session: -
`);
    process.stdout.write(`agent: ${agent.agentCommand}
`);
    process.stdout.write(`pid: -
`);
    process.stdout.write(`status: no-session
`);
    process.stdout.write(`uptime: -
`);
    process.stdout.write(`lastPromptTime: -
`);
    return;
  }
  const health = await probeQueueOwnerHealth(record.acpxRecordId);
  const running = health.healthy;
  const payload = {
    sessionId: record.acpxRecordId,
    agentCommand: record.agentCommand,
    pid: health.pid ?? record.pid ?? null,
    status: running ? "running" : "dead",
    uptime: running ? formatUptime(record.agentStartedAt) ?? null : null,
    lastPromptTime: record.lastPromptAt ?? null,
    exitCode: running ? null : record.lastAgentExitCode ?? null,
    signal: running ? null : record.lastAgentExitSignal ?? null,
    ...agentSessionIdPayload(record.agentSessionId)
  };
  if (emitControlEvent(globalFlags.format, jsonEventIdentityFromRecord(record), {
    type: ACPX_EVENT_TYPES.STATUS_SNAPSHOT,
    data: {
      status: running ? "alive" : "dead",
      pid: payload.pid ?? void 0,
      summary: running ? "queue owner healthy" : "queue owner unavailable",
      uptime: payload.uptime ?? void 0,
      last_prompt_time: payload.lastPromptTime ?? void 0,
      exit_code: payload.exitCode ?? void 0,
      signal: payload.signal ?? void 0
    }
  })) {
    return;
  }
  if (globalFlags.format === "quiet") {
    process.stdout.write(`${payload.status}
`);
    return;
  }
  process.stdout.write(`session: ${payload.sessionId}
`);
  if ("agentSessionId" in payload) {
    process.stdout.write(`agentSessionId: ${payload.agentSessionId}
`);
  }
  process.stdout.write(`agent: ${payload.agentCommand}
`);
  process.stdout.write(`pid: ${payload.pid ?? "-"}
`);
  process.stdout.write(`status: ${payload.status}
`);
  process.stdout.write(`uptime: ${payload.uptime ?? "-"}
`);
  process.stdout.write(`lastPromptTime: ${payload.lastPromptTime ?? "-"}
`);
  if (payload.status === "dead") {
    process.stdout.write(`exitCode: ${payload.exitCode ?? "-"}
`);
    process.stdout.write(`signal: ${payload.signal ?? "-"}
`);
  }
}
async function handleConfigShow(command, config) {
  const globalFlags = resolveGlobalFlags(command, config);
  const payload = {
    ...toConfigDisplay(config),
    paths: {
      global: config.globalPath,
      project: config.projectPath
    },
    loaded: {
      global: config.hasGlobalConfig,
      project: config.hasProjectConfig
    }
  };
  if (globalFlags.format === "json") {
    process.stdout.write(`${JSON.stringify(payload)}
`);
    return;
  }
  process.stdout.write(`${JSON.stringify(payload, null, 2)}
`);
}
async function handleConfigInit(command, config) {
  const globalFlags = resolveGlobalFlags(command, config);
  const result = await initGlobalConfigFile();
  if (globalFlags.format === "json") {
    process.stdout.write(
      `${JSON.stringify({
        path: result.path,
        created: result.created
      })}
`
    );
    return;
  }
  if (globalFlags.format === "quiet") {
    process.stdout.write(`${result.path}
`);
    return;
  }
  if (result.created) {
    process.stdout.write(`Created ${result.path}
`);
    return;
  }
  process.stdout.write(`Config already exists: ${result.path}
`);
}
function registerSessionsCommand(parent, explicitAgentName, config) {
  const sessionsCommand = parent.command("sessions").description("List, ensure, create, or close sessions for this agent");
  sessionsCommand.action(async function() {
    await handleSessionsList(explicitAgentName, this, config);
  });
  sessionsCommand.command("list").description("List sessions").action(async function() {
    await handleSessionsList(explicitAgentName, this, config);
  });
  sessionsCommand.command("new").description("Create a fresh session for current cwd").option("--name <name>", "Session name", parseSessionName).action(async function(flags) {
    await handleSessionsNew(explicitAgentName, flags, this, config);
  });
  sessionsCommand.command("ensure").description("Ensure a session exists for current cwd or ancestor").option("--name <name>", "Session name", parseSessionName).action(async function(flags) {
    await handleSessionsEnsure(explicitAgentName, flags, this, config);
  });
  sessionsCommand.command("close").description("Close session for current cwd").argument("[name]", "Session name", parseSessionName).action(async function(name) {
    await handleSessionsClose(explicitAgentName, name, this, config);
  });
  sessionsCommand.command("show").description("Show session metadata for current cwd").argument("[name]", "Session name", parseSessionName).action(async function(name) {
    await handleSessionsShow(explicitAgentName, name, this, config);
  });
  sessionsCommand.command("history").description("Show recent session history entries").argument("[name]", "Session name", parseSessionName).option(
    "--limit <count>",
    "Maximum number of entries to show (default: 20)",
    parseHistoryLimit,
    DEFAULT_HISTORY_LIMIT
  ).action(async function(name, flags) {
    await handleSessionsHistory(explicitAgentName, name, flags, this, config);
  });
}
function registerSharedAgentSubcommands(parent, explicitAgentName, config, descriptions) {
  const promptCommand = parent.command("prompt").description(descriptions.prompt).argument("[prompt...]", "Prompt text").showHelpAfterError();
  addSessionOption(promptCommand);
  addPromptInputOption(promptCommand);
  promptCommand.action(async function(promptParts, flags) {
    await handlePrompt(explicitAgentName, promptParts, flags, this, config);
  });
  const execCommand = parent.command("exec").description(descriptions.exec).argument("[prompt...]", "Prompt text").showHelpAfterError();
  addPromptInputOption(execCommand);
  execCommand.action(async function(promptParts, flags) {
    await handleExec(explicitAgentName, promptParts, flags, this, config);
  });
  const cancelCommand = parent.command("cancel").description(descriptions.cancel);
  addSessionNameOption(cancelCommand);
  cancelCommand.action(async function(flags) {
    await handleCancel(explicitAgentName, flags, this, config);
  });
  const setModeCommand = parent.command("set-mode").description(descriptions.setMode).argument(
    "<mode>",
    "Mode id",
    (value) => parseNonEmptyValue("Mode", value)
  );
  addSessionNameOption(setModeCommand);
  setModeCommand.action(async function(modeId, flags) {
    await handleSetMode(explicitAgentName, modeId, flags, this, config);
  });
  const setConfigCommand = parent.command("set").description(descriptions.setConfig).argument(
    "<key>",
    "Config option id",
    (value) => parseNonEmptyValue("Config option key", value)
  ).argument(
    "<value>",
    "Config option value",
    (value) => parseNonEmptyValue("Config option value", value)
  );
  addSessionNameOption(setConfigCommand);
  setConfigCommand.action(async function(key, value, flags) {
    await handleSetConfigOption(explicitAgentName, key, value, flags, this, config);
  });
  const statusCommand = parent.command("status").description(descriptions.status);
  addSessionNameOption(statusCommand);
  statusCommand.action(async function(flags) {
    await handleStatus(explicitAgentName, flags, this, config);
  });
}
function registerAgentCommand(program, agentName, config) {
  const agentCommand = program.command(agentName).description(`Use ${agentName} agent`).argument("[prompt...]", "Prompt text").enablePositionalOptions().passThroughOptions().showHelpAfterError();
  addSessionOption(agentCommand);
  addPromptInputOption(agentCommand);
  agentCommand.action(async function(promptParts, flags) {
    await handlePrompt(agentName, promptParts, flags, this, config);
  });
  registerSharedAgentSubcommands(agentCommand, agentName, config, {
    prompt: "Prompt using persistent session",
    exec: "One-shot prompt without saved session",
    cancel: "Cooperatively cancel current in-flight prompt",
    setMode: "Set session mode",
    setConfig: "Set session config option",
    status: "Show local status of current session agent process"
  });
  registerSessionsCommand(agentCommand, agentName, config);
}
function registerConfigCommand(program, config) {
  const configCommand = program.command("config").description("Inspect and initialize acpx configuration");
  configCommand.command("show").description("Show resolved config").action(async function() {
    await handleConfigShow(this, config);
  });
  configCommand.command("init").description("Create global config template").action(async function() {
    await handleConfigInit(this, config);
  });
  configCommand.action(async function() {
    await handleConfigShow(this, config);
  });
}
function registerDefaultCommands(program, config) {
  registerSharedAgentSubcommands(program, void 0, config, {
    prompt: `Prompt using ${config.defaultAgent} by default`,
    exec: `One-shot prompt using ${config.defaultAgent} by default`,
    cancel: `Cancel active prompt for ${config.defaultAgent} by default`,
    setMode: `Set session mode for ${config.defaultAgent} by default`,
    setConfig: `Set session config option for ${config.defaultAgent} by default`,
    status: `Show local status for ${config.defaultAgent} by default`
  });
  registerSessionsCommand(program, void 0, config);
  registerConfigCommand(program, config);
}
function detectAgentToken(argv) {
  let hasAgentOverride = false;
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === "--") {
      break;
    }
    if (!token.startsWith("-") || token === "-") {
      return { token, hasAgentOverride };
    }
    if (token === "--agent") {
      hasAgentOverride = true;
      index += 1;
      continue;
    }
    if (token.startsWith("--agent=")) {
      hasAgentOverride = true;
      continue;
    }
    if (token === "--cwd" || token === "--auth-policy" || token === "--non-interactive-permissions" || token === "--format" || token === "--timeout" || token === "--ttl" || token === "--file") {
      index += 1;
      continue;
    }
    if (token.startsWith("--cwd=") || token.startsWith("--auth-policy=") || token.startsWith("--non-interactive-permissions=") || token.startsWith("--format=") || token.startsWith("--json-strict=") || token.startsWith("--timeout=") || token.startsWith("--ttl=") || token.startsWith("--file=")) {
      continue;
    }
    if (token === "--approve-all" || token === "--approve-reads" || token === "--deny-all" || token === "--json-strict" || token === "--verbose") {
      continue;
    }
    return { hasAgentOverride };
  }
  return { hasAgentOverride };
}
function detectInitialCwd(argv) {
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === "--cwd") {
      const next = argv[index + 1];
      if (next && next !== "--") {
        return path3.resolve(next);
      }
      break;
    }
    if (token.startsWith("--cwd=")) {
      const value = token.slice("--cwd=".length).trim();
      if (value.length > 0) {
        return path3.resolve(value);
      }
      break;
    }
    if (token === "--") {
      break;
    }
  }
  return process.cwd();
}
function detectRequestedOutputFormat(argv, fallback) {
  let detectedFormat = fallback;
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === "--") {
      break;
    }
    if (token === "--json-strict" || token.startsWith("--json-strict=")) {
      return "json";
    }
    if (token === "--format") {
      const raw = argv[index + 1];
      if (raw && OUTPUT_FORMATS.includes(raw)) {
        detectedFormat = raw;
      }
      continue;
    }
    if (token.startsWith("--format=")) {
      const raw = token.slice("--format=".length).trim();
      if (OUTPUT_FORMATS.includes(raw)) {
        detectedFormat = raw;
      }
    }
  }
  return detectedFormat;
}
function detectJsonStrict(argv) {
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === "--") {
      break;
    }
    if (token === "--json-strict") {
      return true;
    }
    if (token.startsWith("--json-strict=")) {
      return true;
    }
  }
  return false;
}
function emitJsonErrorEvent(error) {
  const formatter = createOutputFormatter("json", {
    jsonContext: {
      sessionId: "unknown"
    }
  });
  formatter.onError(error);
  formatter.flush();
}
function isOutputAlreadyEmitted(error) {
  if (!error || typeof error !== "object") {
    return false;
  }
  return error.outputAlreadyEmitted === true;
}
function emitRequestedError(error, normalized, outputPolicy) {
  if (isOutputAlreadyEmitted(error)) {
    return;
  }
  if (outputPolicy.format === "json") {
    emitJsonErrorEvent(normalized);
  } else if (!outputPolicy.suppressNonJsonStderr) {
    process.stderr.write(`${normalized.message}
`);
  }
}
async function runWithOutputPolicy(_outputPolicy, run) {
  return await run();
}
async function main(argv = process.argv) {
  await maybeHandleSkillflag(argv, {
    skillsRoot: findSkillsRoot(import.meta.url),
    includeBundledSkill: false
  });
  const config = await loadResolvedConfig(detectInitialCwd(argv.slice(2)));
  const requestedJsonStrict = detectJsonStrict(argv.slice(2));
  const requestedOutputFormat = detectRequestedOutputFormat(
    argv.slice(2),
    config.format
  );
  const requestedOutputPolicy = resolveOutputPolicy(
    requestedOutputFormat,
    requestedJsonStrict
  );
  const builtInAgents = listBuiltInAgents(config.agents);
  const program = new Command();
  program.name("acpx").description("Headless CLI client for the Agent Client Protocol").enablePositionalOptions().showHelpAfterError();
  if (requestedJsonStrict) {
    program.configureOutput({
      writeOut: () => {
      },
      writeErr: () => {
      }
    });
  }
  addGlobalFlags(program);
  for (const agentName of builtInAgents) {
    registerAgentCommand(program, agentName, config);
  }
  registerDefaultCommands(program, config);
  const scan = detectAgentToken(argv.slice(2));
  if (!scan.hasAgentOverride && scan.token && !TOP_LEVEL_VERBS.has(scan.token) && !builtInAgents.includes(scan.token)) {
    registerAgentCommand(program, scan.token, config);
  }
  program.argument("[prompt...]", "Prompt text").action(async function(promptParts) {
    if (promptParts.length === 0 && process.stdin.isTTY) {
      if (requestedJsonStrict) {
        throw new InvalidArgumentError2(
          "Prompt is required (pass as argument, --file, or pipe via stdin)"
        );
      }
      this.outputHelp();
      return;
    }
    await handlePrompt(void 0, promptParts, {}, this, config);
  });
  program.addHelpText(
    "after",
    `
Examples:
  acpx codex sessions new
  acpx codex "fix the tests"
  acpx codex prompt "fix the tests"
  acpx codex --no-wait "queue follow-up task"
  acpx codex exec "what does this repo do"
  acpx codex cancel
  acpx codex set-mode plan
  acpx codex set approval_policy conservative
  acpx codex -s backend "fix the API"
  acpx codex sessions
  acpx codex sessions new --name backend
  acpx codex sessions ensure --name backend
  acpx codex sessions close backend
  acpx codex status
  acpx config show
  acpx config init
  acpx --ttl 30 codex "investigate flaky tests"
  acpx claude "refactor auth"
  acpx gemini "add logging"
  acpx --agent ./my-custom-server "do something"`
  );
  program.exitOverride((error) => {
    throw error;
  });
  await runWithOutputPolicy(requestedOutputPolicy, async () => {
    try {
      await program.parseAsync(argv);
    } catch (error) {
      if (error instanceof CommanderError) {
        if (error.code === "commander.helpDisplayed" || error.code === "commander.version") {
          process.exit(EXIT_CODES.SUCCESS);
        }
        const normalized2 = normalizeOutputError(error, {
          defaultCode: "USAGE",
          origin: "cli"
        });
        if (requestedOutputPolicy.format === "json") {
          emitRequestedError(error, normalized2, requestedOutputPolicy);
        }
        process.exit(exitCodeForOutputErrorCode(normalized2.code));
      }
      if (error instanceof InterruptedError) {
        process.exit(EXIT_CODES.INTERRUPTED);
      }
      const normalized = normalizeOutputError(error, {
        origin: "cli"
      });
      emitRequestedError(error, normalized, requestedOutputPolicy);
      process.exit(exitCodeForOutputErrorCode(normalized.code));
    }
  });
}
function isCliEntrypoint(argv) {
  const entry = argv[1];
  if (!entry) {
    return false;
  }
  try {
    const resolved = pathToFileURL(realpathSync(entry)).href;
    return import.meta.url === resolved;
  } catch {
    return false;
  }
}
if (isCliEntrypoint(process.argv)) {
  void main(process.argv);
}

// src/cli.ts
function isCliEntrypoint2(argv) {
  const entry = argv[1];
  if (!entry) {
    return false;
  }
  try {
    const resolved = pathToFileURL2(realpathSync2(entry)).href;
    return import.meta.url === resolved;
  } catch {
    return false;
  }
}
if (isCliEntrypoint2(process.argv)) {
  void main(process.argv);
}
export {
  formatPromptSessionBannerLine,
  parseTtlSeconds
};
