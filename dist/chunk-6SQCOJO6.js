// src/errors.ts
var AcpxOperationalError = class extends Error {
  outputCode;
  detailCode;
  origin;
  retryable;
  acp;
  outputAlreadyEmitted;
  constructor(message, options) {
    super(message, options);
    this.name = new.target.name;
    this.outputCode = options?.outputCode;
    this.detailCode = options?.detailCode;
    this.origin = options?.origin;
    this.retryable = options?.retryable;
    this.acp = options?.acp;
    this.outputAlreadyEmitted = options?.outputAlreadyEmitted;
  }
};
var SessionNotFoundError = class extends AcpxOperationalError {
  sessionId;
  constructor(sessionId) {
    super(`Session not found: ${sessionId}`);
    this.sessionId = sessionId;
  }
};
var SessionResolutionError = class extends AcpxOperationalError {
};
var AgentSpawnError = class extends AcpxOperationalError {
  agentCommand;
  constructor(agentCommand, cause) {
    super(`Failed to spawn agent command: ${agentCommand}`, {
      cause: cause instanceof Error ? cause : void 0
    });
    this.agentCommand = agentCommand;
  }
};
var AuthPolicyError = class extends AcpxOperationalError {
  constructor(message, options) {
    super(message, {
      outputCode: "RUNTIME",
      detailCode: "AUTH_REQUIRED",
      origin: "acp",
      ...options
    });
  }
};
var QueueConnectionError = class extends AcpxOperationalError {
};
var QueueProtocolError = class extends AcpxOperationalError {
};
var PermissionDeniedError = class extends AcpxOperationalError {
};
var PermissionPromptUnavailableError = class extends AcpxOperationalError {
  constructor() {
    super("Permission prompt unavailable in non-interactive mode");
  }
};

// src/client.ts
import {
  ClientSideConnection,
  PROTOCOL_VERSION,
  ndJsonStream
} from "@agentclientprotocol/sdk";
import { spawn as spawn2 } from "child_process";
import path2 from "path";
import { Readable, Writable } from "stream";

// src/filesystem.ts
import fs from "fs/promises";
import path from "path";

// src/permission-prompt.ts
import readline from "readline/promises";
async function promptForPermission(options) {
  if (!process.stdin.isTTY || !process.stderr.isTTY) {
    return false;
  }
  if (options.header) {
    process.stderr.write(`
${options.header}
`);
  }
  if (options.details && options.details.trim().length > 0) {
    process.stderr.write(`${options.details}
`);
  }
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stderr
  });
  try {
    const answer = await rl.question(options.prompt);
    const normalized = answer.trim().toLowerCase();
    return normalized === "y" || normalized === "yes";
  } finally {
    rl.close();
  }
}

// src/filesystem.ts
var WRITE_PREVIEW_MAX_LINES = 16;
var WRITE_PREVIEW_MAX_CHARS = 1200;
function nowIso() {
  return (/* @__PURE__ */ new Date()).toISOString();
}
function isWithinRoot(rootDir, targetPath) {
  const relative = path.relative(rootDir, targetPath);
  return relative.length === 0 || !relative.startsWith("..") && !path.isAbsolute(relative);
}
function toWritePreview(content) {
  const normalized = content.replace(/\r\n/g, "\n");
  const lines = normalized.split("\n");
  const visibleLines = lines.slice(0, WRITE_PREVIEW_MAX_LINES);
  let preview = visibleLines.join("\n");
  if (lines.length > visibleLines.length) {
    preview += `
... (${lines.length - visibleLines.length} more lines)`;
  }
  if (preview.length > WRITE_PREVIEW_MAX_CHARS) {
    preview = `${preview.slice(0, WRITE_PREVIEW_MAX_CHARS - 3)}...`;
  }
  return preview;
}
async function defaultConfirmWrite(filePath, preview) {
  return await promptForPermission({
    header: `[permission] Allow write to ${filePath}?`,
    details: preview,
    prompt: "Allow write? (y/N) "
  });
}
function canPromptForPermission() {
  return Boolean(process.stdin.isTTY && process.stderr.isTTY);
}
var FileSystemHandlers = class {
  rootDir;
  permissionMode;
  nonInteractivePermissions;
  onOperation;
  usesDefaultConfirmWrite;
  confirmWrite;
  constructor(options) {
    this.rootDir = path.resolve(options.cwd);
    this.permissionMode = options.permissionMode;
    this.nonInteractivePermissions = options.nonInteractivePermissions ?? "deny";
    this.onOperation = options.onOperation;
    this.usesDefaultConfirmWrite = options.confirmWrite == null;
    this.confirmWrite = options.confirmWrite ?? defaultConfirmWrite;
  }
  async readTextFile(params) {
    const filePath = this.resolvePathWithinRoot(params.path);
    const summary = `read_text_file: ${filePath}`;
    this.emitOperation({
      method: "fs/read_text_file",
      status: "running",
      summary,
      details: this.readWindowDetails(params.line, params.limit),
      timestamp: nowIso()
    });
    try {
      if (this.permissionMode === "deny-all") {
        throw new PermissionDeniedError(
          "Permission denied for fs/read_text_file (--deny-all)"
        );
      }
      const content = await fs.readFile(filePath, "utf8");
      const sliced = this.sliceContent(content, params.line, params.limit);
      this.emitOperation({
        method: "fs/read_text_file",
        status: "completed",
        summary,
        details: this.readWindowDetails(params.line, params.limit),
        timestamp: nowIso()
      });
      return { content: sliced };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.emitOperation({
        method: "fs/read_text_file",
        status: "failed",
        summary,
        details: message,
        timestamp: nowIso()
      });
      throw error;
    }
  }
  async writeTextFile(params) {
    const filePath = this.resolvePathWithinRoot(params.path);
    const preview = toWritePreview(params.content);
    const summary = `write_text_file: ${filePath}`;
    this.emitOperation({
      method: "fs/write_text_file",
      status: "running",
      summary,
      details: preview,
      timestamp: nowIso()
    });
    try {
      if (!await this.isWriteApproved(filePath, preview)) {
        throw new PermissionDeniedError("Permission denied for fs/write_text_file");
      }
      await fs.mkdir(path.dirname(filePath), { recursive: true });
      await fs.writeFile(filePath, params.content, "utf8");
      this.emitOperation({
        method: "fs/write_text_file",
        status: "completed",
        summary,
        details: preview,
        timestamp: nowIso()
      });
      return {};
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.emitOperation({
        method: "fs/write_text_file",
        status: "failed",
        summary,
        details: message,
        timestamp: nowIso()
      });
      throw error;
    }
  }
  async isWriteApproved(filePath, preview) {
    if (this.permissionMode === "approve-all") {
      return true;
    }
    if (this.permissionMode === "deny-all") {
      return false;
    }
    if (this.usesDefaultConfirmWrite && this.nonInteractivePermissions === "fail" && !canPromptForPermission()) {
      throw new PermissionPromptUnavailableError();
    }
    return await this.confirmWrite(filePath, preview);
  }
  resolvePathWithinRoot(rawPath) {
    if (!path.isAbsolute(rawPath)) {
      throw new Error(`Path must be absolute: ${rawPath}`);
    }
    const resolved = path.resolve(rawPath);
    if (!isWithinRoot(this.rootDir, resolved)) {
      throw new Error(`Path is outside allowed cwd subtree: ${resolved}`);
    }
    return resolved;
  }
  sliceContent(content, line, limit) {
    if (line == null && limit == null) {
      return content;
    }
    const lines = content.split("\n");
    const startLine = line == null ? 1 : Math.max(1, Math.trunc(line));
    const startIndex = Math.max(0, startLine - 1);
    const maxLines = limit == null ? void 0 : Math.max(0, Math.trunc(limit));
    if (maxLines === 0) {
      return "";
    }
    const endIndex = maxLines == null ? lines.length : Math.min(lines.length, startIndex + maxLines);
    return lines.slice(startIndex, endIndex).join("\n");
  }
  readWindowDetails(line, limit) {
    if (line == null && limit == null) {
      return void 0;
    }
    const start = line == null ? 1 : Math.max(1, Math.trunc(line));
    const max = limit == null ? "all" : Math.max(0, Math.trunc(limit));
    return `line=${start}, limit=${max}`;
  }
  emitOperation(operation) {
    this.onOperation?.(operation);
  }
};

// src/permissions.ts
function selected(optionId) {
  return { outcome: { outcome: "selected", optionId } };
}
function cancelled() {
  return { outcome: { outcome: "cancelled" } };
}
function pickOption(options, kinds) {
  for (const kind of kinds) {
    const match = options.find((option) => option.kind === kind);
    if (match) {
      return match;
    }
  }
  return void 0;
}
function inferToolKind(params) {
  if (params.toolCall.kind) {
    return params.toolCall.kind;
  }
  const title = params.toolCall.title?.trim().toLowerCase();
  if (!title) {
    return void 0;
  }
  const head = title.split(":", 1)[0]?.trim();
  if (!head) {
    return void 0;
  }
  if (head.includes("read") || head.includes("cat")) {
    return "read";
  }
  if (head.includes("search") || head.includes("find") || head.includes("grep")) {
    return "search";
  }
  if (head.includes("write") || head.includes("edit") || head.includes("patch")) {
    return "edit";
  }
  if (head.includes("delete") || head.includes("remove")) {
    return "delete";
  }
  if (head.includes("move") || head.includes("rename")) {
    return "move";
  }
  if (head.includes("run") || head.includes("execute") || head.includes("bash")) {
    return "execute";
  }
  if (head.includes("fetch") || head.includes("http") || head.includes("url")) {
    return "fetch";
  }
  if (head.includes("think")) {
    return "think";
  }
  return "other";
}
function isAutoApprovedReadKind(kind) {
  return kind === "read" || kind === "search";
}
async function promptForToolPermission(params) {
  const toolName = params.toolCall.title ?? "tool";
  const toolKind = inferToolKind(params) ?? "other";
  return await promptForPermission({
    prompt: `
[permission] Allow ${toolName} [${toolKind}]? (y/N) `
  });
}
function canPromptForPermission2() {
  return Boolean(process.stdin.isTTY && process.stderr.isTTY);
}
async function resolvePermissionRequest(params, mode, nonInteractivePolicy = "deny") {
  const options = params.options ?? [];
  if (options.length === 0) {
    return cancelled();
  }
  const allowOption = pickOption(options, ["allow_once", "allow_always"]);
  const rejectOption = pickOption(options, ["reject_once", "reject_always"]);
  if (mode === "approve-all") {
    if (allowOption) {
      return selected(allowOption.optionId);
    }
    return selected(options[0].optionId);
  }
  if (mode === "deny-all") {
    if (rejectOption) {
      return selected(rejectOption.optionId);
    }
    return cancelled();
  }
  const kind = inferToolKind(params);
  if (isAutoApprovedReadKind(kind) && allowOption) {
    return selected(allowOption.optionId);
  }
  if (!canPromptForPermission2()) {
    if (nonInteractivePolicy === "fail") {
      throw new PermissionPromptUnavailableError();
    }
    if (rejectOption) {
      return selected(rejectOption.optionId);
    }
    return cancelled();
  }
  const approved = await promptForToolPermission(params);
  if (approved && allowOption) {
    return selected(allowOption.optionId);
  }
  if (!approved && rejectOption) {
    return selected(rejectOption.optionId);
  }
  return cancelled();
}
function classifyPermissionDecision(params, response) {
  if (response.outcome.outcome !== "selected") {
    return "cancelled";
  }
  const selectedOptionId = response.outcome.optionId;
  const selectedOption = params.options.find(
    (option) => option.optionId === selectedOptionId
  );
  if (!selectedOption) {
    return "cancelled";
  }
  if (selectedOption.kind === "allow_once" || selectedOption.kind === "allow_always") {
    return "approved";
  }
  return "denied";
}

// src/agent-session-id.ts
var AGENT_SESSION_ID_META_KEYS = ["agentSessionId", "sessionId"];
function normalizeAgentSessionId(value) {
  if (typeof value !== "string") {
    return void 0;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : void 0;
}
function asMetaRecord(meta) {
  if (!meta || typeof meta !== "object" || Array.isArray(meta)) {
    return void 0;
  }
  return meta;
}
function extractAgentSessionId(meta) {
  const record = asMetaRecord(meta);
  if (!record) {
    return void 0;
  }
  for (const key of AGENT_SESSION_ID_META_KEYS) {
    const normalized = normalizeAgentSessionId(record[key]);
    if (normalized) {
      return normalized;
    }
  }
  return void 0;
}

// src/runtime-session-id.ts
function normalizeRuntimeSessionId(value) {
  return normalizeAgentSessionId(value);
}
function extractRuntimeSessionId(meta) {
  return extractAgentSessionId(meta);
}

// src/terminal.ts
import { spawn } from "child_process";
import { randomUUID } from "crypto";
var DEFAULT_TERMINAL_OUTPUT_LIMIT_BYTES = 64 * 1024;
var DEFAULT_KILL_GRACE_MS = 1500;
function nowIso2() {
  return (/* @__PURE__ */ new Date()).toISOString();
}
function toCommandLine(command, args) {
  const renderedArgs = (args ?? []).map((arg) => JSON.stringify(arg)).join(" ");
  return renderedArgs.length > 0 ? `${command} ${renderedArgs}` : command;
}
function toEnvObject(env) {
  if (!env || env.length === 0) {
    return void 0;
  }
  const merged = { ...process.env };
  for (const entry of env) {
    merged[entry.name] = entry.value;
  }
  return merged;
}
function trimToUtf8Boundary(buffer, limit) {
  if (limit <= 0) {
    return Buffer.alloc(0);
  }
  if (buffer.length <= limit) {
    return buffer;
  }
  let start = buffer.length - limit;
  while (start < buffer.length && (buffer[start] & 192) === 128) {
    start += 1;
  }
  if (start >= buffer.length) {
    start = buffer.length - limit;
  }
  return buffer.subarray(start);
}
function waitForSpawn(process2) {
  return new Promise((resolve, reject) => {
    const onSpawn = () => {
      process2.off("error", onError);
      resolve();
    };
    const onError = (error) => {
      process2.off("spawn", onSpawn);
      reject(error);
    };
    process2.once("spawn", onSpawn);
    process2.once("error", onError);
  });
}
async function defaultConfirmExecute(commandLine) {
  return await promptForPermission({
    prompt: `
[permission] Allow terminal command "${commandLine}"? (y/N) `
  });
}
function canPromptForPermission3() {
  return Boolean(process.stdin.isTTY && process.stderr.isTTY);
}
function waitMs(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, Math.max(0, ms));
  });
}
var TerminalManager = class {
  cwd;
  permissionMode;
  nonInteractivePermissions;
  onOperation;
  usesDefaultConfirmExecute;
  confirmExecute;
  killGraceMs;
  terminals = /* @__PURE__ */ new Map();
  constructor(options) {
    this.cwd = options.cwd;
    this.permissionMode = options.permissionMode;
    this.nonInteractivePermissions = options.nonInteractivePermissions ?? "deny";
    this.onOperation = options.onOperation;
    this.usesDefaultConfirmExecute = options.confirmExecute == null;
    this.confirmExecute = options.confirmExecute ?? defaultConfirmExecute;
    this.killGraceMs = Math.max(
      0,
      Math.round(options.killGraceMs ?? DEFAULT_KILL_GRACE_MS)
    );
  }
  async createTerminal(params) {
    const commandLine = toCommandLine(params.command, params.args);
    const summary = `terminal/create: ${commandLine}`;
    this.emitOperation({
      method: "terminal/create",
      status: "running",
      summary,
      timestamp: nowIso2()
    });
    try {
      if (!await this.isExecuteApproved(commandLine)) {
        throw new PermissionDeniedError("Permission denied for terminal/create");
      }
      const outputByteLimit = Math.max(
        0,
        Math.round(params.outputByteLimit ?? DEFAULT_TERMINAL_OUTPUT_LIMIT_BYTES)
      );
      const proc = spawn(params.command, params.args ?? [], {
        cwd: params.cwd ?? this.cwd,
        env: toEnvObject(params.env),
        stdio: ["ignore", "pipe", "pipe"]
      });
      await waitForSpawn(proc);
      let resolveExit = () => {
      };
      const exitPromise = new Promise((resolve) => {
        resolveExit = resolve;
      });
      const terminal = {
        process: proc,
        output: Buffer.alloc(0),
        truncated: false,
        outputByteLimit,
        exitCode: void 0,
        signal: void 0,
        exitPromise,
        resolveExit
      };
      const appendOutput = (chunk) => {
        const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
        if (bytes.length === 0) {
          return;
        }
        terminal.output = Buffer.concat([terminal.output, bytes]);
        if (terminal.output.length > terminal.outputByteLimit) {
          terminal.output = trimToUtf8Boundary(
            terminal.output,
            terminal.outputByteLimit
          );
          terminal.truncated = true;
        }
      };
      proc.stdout.on("data", appendOutput);
      proc.stderr.on("data", appendOutput);
      proc.once("exit", (exitCode, signal) => {
        terminal.exitCode = exitCode;
        terminal.signal = signal;
        terminal.resolveExit({
          exitCode: exitCode ?? null,
          signal: signal ?? null
        });
      });
      const terminalId = randomUUID();
      this.terminals.set(terminalId, terminal);
      this.emitOperation({
        method: "terminal/create",
        status: "completed",
        summary,
        details: `terminalId=${terminalId}`,
        timestamp: nowIso2()
      });
      return { terminalId };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.emitOperation({
        method: "terminal/create",
        status: "failed",
        summary,
        details: message,
        timestamp: nowIso2()
      });
      throw error;
    }
  }
  async terminalOutput(params) {
    const terminal = this.getTerminal(params.terminalId);
    if (!terminal) {
      throw new Error(`Unknown terminal: ${params.terminalId}`);
    }
    const hasExitStatus = terminal.exitCode !== void 0 || terminal.signal !== void 0;
    this.emitOperation({
      method: "terminal/output",
      status: "completed",
      summary: `terminal/output: ${params.terminalId}`,
      timestamp: nowIso2()
    });
    return {
      output: terminal.output.toString("utf8"),
      truncated: terminal.truncated,
      exitStatus: hasExitStatus ? {
        exitCode: terminal.exitCode ?? null,
        signal: terminal.signal ?? null
      } : void 0
    };
  }
  async waitForTerminalExit(params) {
    const terminal = this.getTerminal(params.terminalId);
    if (!terminal) {
      throw new Error(`Unknown terminal: ${params.terminalId}`);
    }
    const response = await terminal.exitPromise;
    this.emitOperation({
      method: "terminal/wait_for_exit",
      status: "completed",
      summary: `terminal/wait_for_exit: ${params.terminalId}`,
      details: `exitCode=${response.exitCode ?? "null"}, signal=${response.signal ?? "null"}`,
      timestamp: nowIso2()
    });
    return response;
  }
  async killTerminal(params) {
    const terminal = this.getTerminal(params.terminalId);
    if (!terminal) {
      throw new Error(`Unknown terminal: ${params.terminalId}`);
    }
    const summary = `terminal/kill: ${params.terminalId}`;
    this.emitOperation({
      method: "terminal/kill",
      status: "running",
      summary,
      timestamp: nowIso2()
    });
    try {
      await this.killProcess(terminal);
      this.emitOperation({
        method: "terminal/kill",
        status: "completed",
        summary,
        timestamp: nowIso2()
      });
      return {};
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.emitOperation({
        method: "terminal/kill",
        status: "failed",
        summary,
        details: message,
        timestamp: nowIso2()
      });
      throw error;
    }
  }
  async releaseTerminal(params) {
    const summary = `terminal/release: ${params.terminalId}`;
    this.emitOperation({
      method: "terminal/release",
      status: "running",
      summary,
      timestamp: nowIso2()
    });
    const terminal = this.getTerminal(params.terminalId);
    if (!terminal) {
      this.emitOperation({
        method: "terminal/release",
        status: "completed",
        summary,
        details: "already released",
        timestamp: nowIso2()
      });
      return {};
    }
    try {
      await this.killProcess(terminal);
      await terminal.exitPromise.catch(() => {
      });
      terminal.output = Buffer.alloc(0);
      this.terminals.delete(params.terminalId);
      this.emitOperation({
        method: "terminal/release",
        status: "completed",
        summary,
        timestamp: nowIso2()
      });
      return {};
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.emitOperation({
        method: "terminal/release",
        status: "failed",
        summary,
        details: message,
        timestamp: nowIso2()
      });
      throw error;
    }
  }
  async shutdown() {
    for (const terminalId of [...this.terminals.keys()]) {
      await this.releaseTerminal({ terminalId, sessionId: "shutdown" });
    }
  }
  getTerminal(terminalId) {
    return this.terminals.get(terminalId);
  }
  emitOperation(operation) {
    this.onOperation?.(operation);
  }
  async isExecuteApproved(commandLine) {
    if (this.permissionMode === "approve-all") {
      return true;
    }
    if (this.permissionMode === "deny-all") {
      return false;
    }
    if (this.usesDefaultConfirmExecute && this.nonInteractivePermissions === "fail" && !canPromptForPermission3()) {
      throw new PermissionPromptUnavailableError();
    }
    return await this.confirmExecute(commandLine);
  }
  isRunning(terminal) {
    return terminal.exitCode === void 0 && terminal.signal === void 0;
  }
  async killProcess(terminal) {
    if (!this.isRunning(terminal)) {
      return;
    }
    try {
      terminal.process.kill("SIGTERM");
    } catch {
      return;
    }
    const exitedAfterTerm = await Promise.race([
      terminal.exitPromise.then(() => true),
      waitMs(this.killGraceMs).then(() => false)
    ]);
    if (exitedAfterTerm || !this.isRunning(terminal)) {
      return;
    }
    try {
      terminal.process.kill("SIGKILL");
    } catch {
      return;
    }
    await Promise.race([
      terminal.exitPromise.then(() => void 0),
      waitMs(this.killGraceMs)
    ]);
  }
};

// src/client.ts
var REPLAY_IDLE_MS = 80;
var REPLAY_DRAIN_TIMEOUT_MS = 5e3;
var DRAIN_POLL_INTERVAL_MS = 20;
function shouldSuppressSdkConsoleError(args) {
  if (args.length === 0) {
    return false;
  }
  return typeof args[0] === "string" && args[0] === "Error handling request";
}
function installSdkConsoleErrorSuppression() {
  const originalConsoleError = console.error;
  console.error = (...args) => {
    if (shouldSuppressSdkConsoleError(args)) {
      return;
    }
    originalConsoleError(...args);
  };
  return () => {
    console.error = originalConsoleError;
  };
}
function isoNow() {
  return (/* @__PURE__ */ new Date()).toISOString();
}
function waitForSpawn2(child) {
  return new Promise((resolve, reject) => {
    const onSpawn = () => {
      child.off("error", onError);
      resolve();
    };
    const onError = (error) => {
      child.off("spawn", onSpawn);
      reject(error);
    };
    child.once("spawn", onSpawn);
    child.once("error", onError);
  });
}
function splitCommandLine(value) {
  const parts = [];
  let current = "";
  let quote = null;
  let escaping = false;
  for (const ch of value) {
    if (escaping) {
      current += ch;
      escaping = false;
      continue;
    }
    if (ch === "\\" && quote !== "'") {
      escaping = true;
      continue;
    }
    if (quote) {
      if (ch === quote) {
        quote = null;
      } else {
        current += ch;
      }
      continue;
    }
    if (ch === "'" || ch === '"') {
      quote = ch;
      continue;
    }
    if (/\s/.test(ch)) {
      if (current.length > 0) {
        parts.push(current);
        current = "";
      }
      continue;
    }
    current += ch;
  }
  if (escaping) {
    current += "\\";
  }
  if (quote) {
    throw new Error("Invalid --agent command: unterminated quote");
  }
  if (current.length > 0) {
    parts.push(current);
  }
  if (parts.length === 0) {
    throw new Error("Invalid --agent command: empty command");
  }
  return {
    command: parts[0],
    args: parts.slice(1)
  };
}
function asAbsoluteCwd(cwd) {
  return path2.resolve(cwd);
}
function toEnvToken(value) {
  return value.trim().replace(/[^a-zA-Z0-9]+/g, "_").replace(/^_+|_+$/g, "").toUpperCase();
}
function authEnvKeys(methodId) {
  const token = toEnvToken(methodId);
  const keys = /* @__PURE__ */ new Set([methodId]);
  if (token) {
    keys.add(token);
    keys.add(`ACPX_AUTH_${token}`);
  }
  return [...keys];
}
function readEnvCredential(methodId) {
  for (const key of authEnvKeys(methodId)) {
    const value = process.env[key];
    if (typeof value === "string" && value.trim().length > 0) {
      return value;
    }
  }
  return void 0;
}
function buildAgentEnvironment(authCredentials, extraEnv) {
  const env = { ...process.env, ...extraEnv };
  if (!authCredentials) {
    return env;
  }
  for (const [methodId, credential] of Object.entries(authCredentials)) {
    if (typeof credential !== "string" || credential.trim().length === 0) {
      continue;
    }
    if (!methodId.includes("=") && !methodId.includes("\0") && env[methodId] == null) {
      env[methodId] = credential;
    }
    const normalized = toEnvToken(methodId);
    if (normalized) {
      const prefixed = `ACPX_AUTH_${normalized}`;
      if (env[prefixed] == null) {
        env[prefixed] = credential;
      }
      if (env[normalized] == null) {
        env[normalized] = credential;
      }
    }
  }
  return env;
}
var AcpClient = class {
  options;
  connection;
  agent;
  initResult;
  permissionStats = {
    requested: 0,
    approved: 0,
    denied: 0,
    cancelled: 0
  };
  filesystem;
  terminalManager;
  sessionUpdateChain = Promise.resolve();
  observedSessionUpdates = 0;
  processedSessionUpdates = 0;
  suppressSessionUpdates = false;
  activePrompt;
  cancellingSessionIds = /* @__PURE__ */ new Set();
  closing = false;
  agentStartedAt;
  lastAgentExit;
  lastKnownPid;
  promptPermissionFailures = /* @__PURE__ */ new Map();
  constructor(options) {
    this.options = {
      ...options,
      cwd: asAbsoluteCwd(options.cwd),
      authPolicy: options.authPolicy ?? "skip"
    };
    const emitOperation = this.options.onClientOperation;
    this.filesystem = new FileSystemHandlers({
      cwd: this.options.cwd,
      permissionMode: this.options.permissionMode,
      nonInteractivePermissions: this.options.nonInteractivePermissions,
      onOperation: emitOperation
    });
    this.terminalManager = new TerminalManager({
      cwd: this.options.cwd,
      permissionMode: this.options.permissionMode,
      nonInteractivePermissions: this.options.nonInteractivePermissions,
      onOperation: emitOperation
    });
  }
  get initializeResult() {
    return this.initResult;
  }
  getAgentPid() {
    return this.agent?.pid ?? this.lastKnownPid;
  }
  getPermissionStats() {
    return { ...this.permissionStats };
  }
  getAgentLifecycleSnapshot() {
    const pid = this.agent?.pid ?? this.lastKnownPid;
    const running = Boolean(this.agent) && this.agent?.exitCode == null && this.agent?.signalCode == null && !this.agent?.killed;
    return {
      pid,
      startedAt: this.agentStartedAt,
      running,
      lastExit: this.lastAgentExit ? { ...this.lastAgentExit } : void 0
    };
  }
  supportsLoadSession() {
    return Boolean(this.initResult?.agentCapabilities?.loadSession);
  }
  hasActivePrompt(sessionId) {
    if (!this.activePrompt) {
      return false;
    }
    if (sessionId == null) {
      return true;
    }
    return this.activePrompt.sessionId === sessionId;
  }
  async start() {
    if (this.connection && this.agent) {
      return;
    }
    const { command, args } = splitCommandLine(this.options.agentCommand);
    this.log(`spawning agent: ${command} ${args.join(" ")}`);
    const child = spawn2(command, args, {
      cwd: this.options.cwd,
      env: buildAgentEnvironment(this.options.authCredentials, this.options.extraEnv),
      stdio: ["pipe", "pipe", "pipe"]
    });
    try {
      await waitForSpawn2(child);
    } catch (error) {
      throw new AgentSpawnError(this.options.agentCommand, error);
    }
    this.closing = false;
    this.agentStartedAt = isoNow();
    this.lastAgentExit = void 0;
    this.lastKnownPid = child.pid ?? void 0;
    this.attachAgentLifecycleObservers(child);
    child.stderr.on("data", (chunk) => {
      if (this.options.verbose) {
        process.stderr.write(chunk);
      }
      if (this.options.onStderr) {
        const lines = chunk.toString().split("\n").filter(Boolean);
        for (const line of lines) {
          this.options.onStderr(line);
        }
      }
    });
    const input = Writable.toWeb(child.stdin);
    const output = Readable.toWeb(child.stdout);
    const stream = ndJsonStream(input, output);
    const connection = new ClientSideConnection(
      () => ({
        sessionUpdate: async (params) => {
          await this.handleSessionUpdate(params);
        },
        requestPermission: async (params) => {
          return this.handlePermissionRequest(params);
        },
        readTextFile: async (params) => {
          return this.handleReadTextFile(params);
        },
        writeTextFile: async (params) => {
          return this.handleWriteTextFile(params);
        },
        createTerminal: async (params) => {
          return this.handleCreateTerminal(params);
        },
        terminalOutput: async (params) => {
          return this.handleTerminalOutput(params);
        },
        waitForTerminalExit: async (params) => {
          return this.handleWaitForTerminalExit(params);
        },
        killTerminal: async (params) => {
          return this.handleKillTerminal(params);
        },
        releaseTerminal: async (params) => {
          return this.handleReleaseTerminal(params);
        }
      }),
      stream
    );
    connection.signal.addEventListener(
      "abort",
      () => {
        this.recordAgentExit(
          "connection_close",
          child.exitCode ?? null,
          child.signalCode ?? null
        );
      },
      { once: true }
    );
    try {
      const initResult = await connection.initialize({
        protocolVersion: PROTOCOL_VERSION,
        clientCapabilities: {
          fs: {
            readTextFile: true,
            writeTextFile: true
          },
          terminal: true
        },
        clientInfo: {
          name: "acpx",
          version: "0.1.0"
        }
      });
      await this.authenticateIfRequired(connection, initResult.authMethods ?? []);
      this.connection = connection;
      this.agent = child;
      this.initResult = initResult;
      this.log(`initialized protocol version ${initResult.protocolVersion}`);
    } catch (error) {
      child.kill();
      throw error;
    }
  }
  async createSession(cwd = this.options.cwd) {
    const connection = this.getConnection();
    const result = await connection.newSession({
      cwd: asAbsoluteCwd(cwd),
      mcpServers: []
    });
    return {
      sessionId: result.sessionId,
      agentSessionId: extractRuntimeSessionId(result._meta)
    };
  }
  async loadSession(sessionId, cwd = this.options.cwd) {
    this.getConnection();
    return await this.loadSessionWithOptions(sessionId, cwd, {});
  }
  async loadSessionWithOptions(sessionId, cwd = this.options.cwd, options = {}) {
    const connection = this.getConnection();
    const previousSuppression = this.suppressSessionUpdates;
    this.suppressSessionUpdates = previousSuppression || Boolean(options.suppressReplayUpdates);
    let response;
    try {
      response = await connection.loadSession({
        sessionId,
        cwd: asAbsoluteCwd(cwd),
        mcpServers: []
      });
      await this.waitForSessionUpdateDrain(
        options.replayIdleMs ?? REPLAY_IDLE_MS,
        options.replayDrainTimeoutMs ?? REPLAY_DRAIN_TIMEOUT_MS
      );
    } finally {
      this.suppressSessionUpdates = previousSuppression;
    }
    return {
      agentSessionId: extractRuntimeSessionId(response?._meta)
    };
  }
  async prompt(sessionId, text) {
    const connection = this.getConnection();
    const restoreConsoleError = this.options.suppressSdkConsoleErrors ? installSdkConsoleErrorSuppression() : void 0;
    let promptPromise;
    try {
      promptPromise = connection.prompt({
        sessionId,
        prompt: [
          {
            type: "text",
            text
          }
        ]
      });
    } catch (error) {
      restoreConsoleError?.();
      throw error;
    }
    this.activePrompt = {
      sessionId,
      promise: promptPromise
    };
    try {
      const response = await promptPromise;
      const permissionFailure = this.consumePromptPermissionFailure(sessionId);
      if (permissionFailure) {
        throw permissionFailure;
      }
      return response;
    } catch (error) {
      const permissionFailure = this.consumePromptPermissionFailure(sessionId);
      if (permissionFailure) {
        throw permissionFailure;
      }
      throw error;
    } finally {
      restoreConsoleError?.();
      if (this.activePrompt?.promise === promptPromise) {
        this.activePrompt = void 0;
      }
      this.cancellingSessionIds.delete(sessionId);
      this.promptPermissionFailures.delete(sessionId);
    }
  }
  async setSessionMode(sessionId, modeId) {
    const connection = this.getConnection();
    await connection.setSessionMode({
      sessionId,
      modeId
    });
  }
  async setSessionConfigOption(sessionId, configId, value) {
    const connection = this.getConnection();
    return await connection.setSessionConfigOption({
      sessionId,
      configId,
      value
    });
  }
  async cancel(sessionId) {
    const connection = this.getConnection();
    this.cancellingSessionIds.add(sessionId);
    await connection.cancel({
      sessionId
    });
  }
  async requestCancelActivePrompt() {
    const active = this.activePrompt;
    if (!active) {
      return false;
    }
    await this.cancel(active.sessionId);
    return true;
  }
  async cancelActivePrompt(waitMs2 = 2500) {
    const active = this.activePrompt;
    if (!active) {
      return void 0;
    }
    try {
      await this.cancel(active.sessionId);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.log(`failed to send session/cancel: ${message}`);
    }
    if (waitMs2 <= 0) {
      return void 0;
    }
    let timer;
    const timeoutPromise = new Promise((resolve) => {
      timer = setTimeout(resolve, waitMs2);
    });
    try {
      return await Promise.race([
        active.promise.then(
          (response) => response,
          () => void 0
        ),
        timeoutPromise
      ]);
    } finally {
      if (timer) {
        clearTimeout(timer);
      }
    }
  }
  async close() {
    this.closing = true;
    await this.terminalManager.shutdown();
    const agent = this.agent;
    if (agent) {
      if (!agent.killed) {
        agent.kill();
      }
      this.detachAgentHandles(agent);
    }
    this.sessionUpdateChain = Promise.resolve();
    this.observedSessionUpdates = 0;
    this.processedSessionUpdates = 0;
    this.suppressSessionUpdates = false;
    this.activePrompt = void 0;
    this.cancellingSessionIds.clear();
    this.promptPermissionFailures.clear();
    this.connection = void 0;
    this.agent = void 0;
  }
  detachAgentHandles(agent) {
    const stdin = agent.stdin;
    const stdout = agent.stdout;
    const stderr = agent.stderr;
    stdin?.destroy();
    stdout?.destroy();
    stderr?.destroy();
    try {
      agent.unref();
    } catch {
    }
  }
  getConnection() {
    if (!this.connection) {
      throw new Error("ACP client not started");
    }
    return this.connection;
  }
  log(message) {
    if (!this.options.verbose) {
      return;
    }
    process.stderr.write(`[acpx] ${message}
`);
  }
  selectAuthMethod(methods) {
    const configCredentials = this.options.authCredentials ?? {};
    for (const method of methods) {
      const envCredential = readEnvCredential(method.id);
      if (envCredential) {
        return {
          methodId: method.id,
          credential: envCredential,
          source: "env"
        };
      }
      const configCredential = configCredentials[method.id] ?? configCredentials[toEnvToken(method.id)];
      if (typeof configCredential === "string" && configCredential.trim().length > 0) {
        return {
          methodId: method.id,
          credential: configCredential,
          source: "config"
        };
      }
    }
    return void 0;
  }
  async authenticateIfRequired(connection, methods) {
    if (methods.length === 0) {
      return;
    }
    const selected2 = this.selectAuthMethod(methods);
    if (!selected2) {
      if (this.options.authPolicy === "fail") {
        throw new AuthPolicyError(
          `agent advertised auth methods [${methods.map((m) => m.id).join(", ")}] but no matching credentials found`
        );
      }
      this.log(
        `agent advertised auth methods [${methods.map((m) => m.id).join(", ")}] but no matching credentials found \u2014 skipping (agent may handle auth internally)`
      );
      return;
    }
    await connection.authenticate({
      methodId: selected2.methodId
    });
    this.log(`authenticated with method ${selected2.methodId} (${selected2.source})`);
  }
  async handlePermissionRequest(params) {
    if (this.cancellingSessionIds.has(params.sessionId)) {
      return {
        outcome: {
          outcome: "cancelled"
        }
      };
    }
    let response;
    try {
      if (this.options.onRequestPermission) {
        response = await this.options.onRequestPermission(params);
      } else {
        response = await resolvePermissionRequest(
          params,
          this.options.permissionMode,
          this.options.nonInteractivePermissions ?? "deny"
        );
      }
    } catch (error) {
      if (error instanceof PermissionPromptUnavailableError) {
        this.notePromptPermissionFailure(params.sessionId, error);
        this.permissionStats.requested += 1;
        this.permissionStats.cancelled += 1;
        return {
          outcome: {
            outcome: "cancelled"
          }
        };
      }
      throw error;
    }
    const decision = classifyPermissionDecision(params, response);
    this.permissionStats.requested += 1;
    if (decision === "approved") {
      this.permissionStats.approved += 1;
    } else if (decision === "denied") {
      this.permissionStats.denied += 1;
    } else {
      this.permissionStats.cancelled += 1;
    }
    return response;
  }
  attachAgentLifecycleObservers(child) {
    child.once("exit", (exitCode, signal) => {
      this.recordAgentExit("process_exit", exitCode, signal);
    });
    child.once("close", (exitCode, signal) => {
      this.recordAgentExit("process_close", exitCode, signal);
    });
    child.stdout.once("close", () => {
      this.recordAgentExit(
        "pipe_close",
        child.exitCode ?? null,
        child.signalCode ?? null
      );
    });
  }
  recordAgentExit(reason, exitCode, signal) {
    if (this.lastAgentExit) {
      return;
    }
    this.lastAgentExit = {
      exitCode,
      signal,
      exitedAt: isoNow(),
      reason,
      unexpectedDuringPrompt: !this.closing && Boolean(this.activePrompt)
    };
  }
  notePromptPermissionFailure(sessionId, error) {
    if (!this.promptPermissionFailures.has(sessionId)) {
      this.promptPermissionFailures.set(sessionId, error);
    }
  }
  consumePromptPermissionFailure(sessionId) {
    const error = this.promptPermissionFailures.get(sessionId);
    if (error) {
      this.promptPermissionFailures.delete(sessionId);
    }
    return error;
  }
  async handleReadTextFile(params) {
    return await this.filesystem.readTextFile(params);
  }
  async handleWriteTextFile(params) {
    try {
      return await this.filesystem.writeTextFile(params);
    } catch (error) {
      if (error instanceof PermissionPromptUnavailableError) {
        this.notePromptPermissionFailure(params.sessionId, error);
      }
      throw error;
    }
  }
  async handleCreateTerminal(params) {
    try {
      return await this.terminalManager.createTerminal(params);
    } catch (error) {
      if (error instanceof PermissionPromptUnavailableError) {
        this.notePromptPermissionFailure(params.sessionId, error);
      }
      throw error;
    }
  }
  async handleTerminalOutput(params) {
    return await this.terminalManager.terminalOutput(params);
  }
  async handleWaitForTerminalExit(params) {
    return await this.terminalManager.waitForTerminalExit(params);
  }
  async handleKillTerminal(params) {
    return await this.terminalManager.killTerminal(params);
  }
  async handleReleaseTerminal(params) {
    return await this.terminalManager.releaseTerminal(params);
  }
  async handleSessionUpdate(notification) {
    const sequence = ++this.observedSessionUpdates;
    this.sessionUpdateChain = this.sessionUpdateChain.then(async () => {
      try {
        if (!this.suppressSessionUpdates) {
          this.options.onSessionUpdate?.(notification);
        }
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        this.log(`session update handler failed: ${message}`);
      } finally {
        this.processedSessionUpdates = sequence;
      }
    });
    await this.sessionUpdateChain;
  }
  async waitForSessionUpdateDrain(idleMs, timeoutMs) {
    const normalizedIdleMs = Math.max(0, idleMs);
    const normalizedTimeoutMs = Math.max(normalizedIdleMs, timeoutMs);
    const deadline = Date.now() + normalizedTimeoutMs;
    let lastObserved = this.observedSessionUpdates;
    let idleSince = Date.now();
    while (Date.now() <= deadline) {
      const observed = this.observedSessionUpdates;
      if (observed !== lastObserved) {
        lastObserved = observed;
        idleSince = Date.now();
      }
      if (this.processedSessionUpdates === this.observedSessionUpdates && Date.now() - idleSince >= normalizedIdleMs) {
        await this.sessionUpdateChain;
        if (this.processedSessionUpdates === this.observedSessionUpdates) {
          return;
        }
      }
      await new Promise((resolve) => {
        setTimeout(resolve, DRAIN_POLL_INTERVAL_MS);
      });
    }
    throw new Error(
      `Timed out waiting for session replay drain after ${normalizedTimeoutMs}ms`
    );
  }
};

export {
  SessionNotFoundError,
  SessionResolutionError,
  AgentSpawnError,
  AuthPolicyError,
  QueueConnectionError,
  QueueProtocolError,
  PermissionDeniedError,
  PermissionPromptUnavailableError,
  normalizeRuntimeSessionId,
  AcpClient
};
