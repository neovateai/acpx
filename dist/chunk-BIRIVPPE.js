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
    const startTotal = performance.now();
    const { command, args } = splitCommandLine(this.options.agentCommand);
    this.log(`spawning agent: ${command} ${args.join(" ")}`);
    const spawnStart = performance.now();
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
    this.emitTiming("start.spawn", performance.now() - spawnStart);
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
    const output = Readable.toWeb(
      child.stdout
    );
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
      const initStart = performance.now();
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
      this.emitTiming("start.initialize", performance.now() - initStart);
      const authStart = performance.now();
      await this.authenticateIfRequired(connection, initResult.authMethods ?? []);
      this.emitTiming("start.authenticate", performance.now() - authStart);
      this.connection = connection;
      this.agent = child;
      this.initResult = initResult;
      this.log(`initialized protocol version ${initResult.protocolVersion}`);
      this.emitTiming("start.total", performance.now() - startTotal);
    } catch (error) {
      child.kill();
      throw error;
    }
  }
  async createSession(cwd = this.options.cwd) {
    const t0 = performance.now();
    const connection = this.getConnection();
    const result = await connection.newSession({
      cwd: asAbsoluteCwd(cwd),
      mcpServers: []
    });
    this.emitTiming("createSession", performance.now() - t0);
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
    const t0 = performance.now();
    const connection = this.getConnection();
    const previousSuppression = this.suppressSessionUpdates;
    this.suppressSessionUpdates = previousSuppression || Boolean(options.suppressReplayUpdates);
    let response;
    try {
      const rpcStart = performance.now();
      response = await connection.loadSession({
        sessionId,
        cwd: asAbsoluteCwd(cwd),
        mcpServers: []
      });
      this.emitTiming("loadSession.rpc", performance.now() - rpcStart);
      const drainStart = performance.now();
      await this.waitForSessionUpdateDrain(
        options.replayIdleMs ?? REPLAY_IDLE_MS,
        options.replayDrainTimeoutMs ?? REPLAY_DRAIN_TIMEOUT_MS
      );
      this.emitTiming("loadSession.drain", performance.now() - drainStart);
    } finally {
      this.suppressSessionUpdates = previousSuppression;
    }
    this.emitTiming("loadSession.total", performance.now() - t0);
    return {
      agentSessionId: extractRuntimeSessionId(response?._meta)
    };
  }
  async prompt(sessionId, text) {
    const t0 = performance.now();
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
      this.emitTiming("prompt", performance.now() - t0);
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
  emitTiming(label, durationMs) {
    this.options.onTiming?.(label, Math.round(durationMs * 100) / 100);
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

// src/types.ts
var EXIT_CODES = {
  SUCCESS: 0,
  ERROR: 1,
  USAGE: 2,
  TIMEOUT: 3,
  NO_SESSION: 4,
  PERMISSION_DENIED: 5,
  INTERRUPTED: 130
};
var OUTPUT_FORMATS = ["text", "json", "quiet"];
var AUTH_POLICIES = ["skip", "fail"];
var NON_INTERACTIVE_PERMISSION_POLICIES = ["deny", "fail"];
var ACPX_EVENT_SCHEMA = "acpx.event.v1";
var ACPX_EVENT_OUTPUT_STREAMS = ["output", "thought"];
var ACPX_EVENT_TYPES = {
  TURN_STARTED: "turn_started",
  OUTPUT_DELTA: "output_delta",
  TOOL_CALL: "tool_call",
  PLAN: "plan",
  UPDATE: "update",
  CLIENT_OPERATION: "client_operation",
  TURN_DONE: "turn_done",
  ERROR: "error",
  PROMPT_QUEUED: "prompt_queued",
  SESSION_ENSURED: "session_ensured",
  CANCEL_REQUESTED: "cancel_requested",
  CANCEL_RESULT: "cancel_result",
  MODE_SET: "mode_set",
  CONFIG_SET: "config_set",
  STATUS_SNAPSHOT: "status_snapshot",
  SESSION_CLOSED: "session_closed"
};
var ACPX_EVENT_TURN_MODES = ["prompt"];
var ACPX_EVENT_STATUS_SNAPSHOT_STATUSES = [
  "alive",
  "dead",
  "no-session"
];
var ACPX_EVENT_TOOL_CALL_STATUSES = [
  "pending",
  "in_progress",
  "completed",
  "failed"
];
var OUTPUT_ERROR_CODES = [
  "NO_SESSION",
  "TIMEOUT",
  "PERMISSION_DENIED",
  "PERMISSION_PROMPT_UNAVAILABLE",
  "RUNTIME",
  "USAGE"
];
var OUTPUT_ERROR_ORIGINS = ["cli", "runtime", "queue", "acp"];
var SESSION_RECORD_SCHEMA = "acpx.session.v1";

// src/events.ts
import { randomUUID as randomUUID2 } from "crypto";
function asRecord(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return void 0;
  }
  return value;
}
function isoNow2() {
  return (/* @__PURE__ */ new Date()).toISOString();
}
function trimNonEmpty(value) {
  if (!value) {
    return void 0;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : void 0;
}
function truncateInputPreview(message, maxChars = 200) {
  const trimmed = message.trim();
  if (trimmed.length <= maxChars) {
    return trimmed;
  }
  if (maxChars <= 3) {
    return trimmed.slice(0, maxChars);
  }
  return `${trimmed.slice(0, maxChars - 3)}...`;
}
function createAcpxEvent(identity, draft) {
  return {
    schema: ACPX_EVENT_SCHEMA,
    event_id: randomUUID2(),
    session_id: identity.sessionId,
    acp_session_id: trimNonEmpty(identity.acpSessionId),
    agent_session_id: trimNonEmpty(identity.agentSessionId),
    request_id: trimNonEmpty(draft.request_id ?? identity.requestId),
    seq: identity.seq,
    ts: identity.ts ?? isoNow2(),
    type: draft.type,
    data: draft.data
  };
}
function sessionUpdateToEventDrafts(notification) {
  const update = notification.update;
  switch (update.sessionUpdate) {
    case "agent_message_chunk": {
      if (update.content.type !== "text") {
        return [];
      }
      return [
        {
          type: ACPX_EVENT_TYPES.OUTPUT_DELTA,
          data: {
            stream: "output",
            text: update.content.text
          }
        }
      ];
    }
    case "agent_thought_chunk": {
      if (update.content.type !== "text") {
        return [];
      }
      return [
        {
          type: ACPX_EVENT_TYPES.OUTPUT_DELTA,
          data: {
            stream: "thought",
            text: update.content.text
          }
        }
      ];
    }
    case "tool_call":
    case "tool_call_update": {
      return [
        {
          type: ACPX_EVENT_TYPES.TOOL_CALL,
          data: {
            tool_call_id: update.toolCallId,
            title: update.title ?? void 0,
            status: update.status ?? void 0
          }
        }
      ];
    }
    case "plan": {
      return [
        {
          type: ACPX_EVENT_TYPES.PLAN,
          data: {
            entries: update.entries.map((entry) => ({
              content: entry.content,
              status: entry.status,
              priority: entry.priority
            }))
          }
        }
      ];
    }
    default: {
      return [
        {
          type: ACPX_EVENT_TYPES.UPDATE,
          data: {
            update: update.sessionUpdate
          }
        }
      ];
    }
  }
}
function clientOperationToEventDraft(operation) {
  return {
    type: ACPX_EVENT_TYPES.CLIENT_OPERATION,
    data: {
      method: operation.method,
      status: operation.status,
      summary: operation.summary,
      details: operation.details
    }
  };
}
function errorToEventDraft(params) {
  return {
    type: ACPX_EVENT_TYPES.ERROR,
    data: {
      code: params.code,
      detail_code: params.detailCode,
      origin: params.origin,
      message: params.message,
      retryable: params.retryable,
      acp_error: params.acp
    }
  };
}
function isAcpxEventOutputStream(value) {
  return typeof value === "string" && ACPX_EVENT_OUTPUT_STREAMS.includes(value);
}
function isOutputErrorCode(value) {
  return typeof value === "string" && OUTPUT_ERROR_CODES.includes(value);
}
function isOutputErrorOrigin(value) {
  return typeof value === "string" && OUTPUT_ERROR_ORIGINS.includes(value);
}
function isAcpError(value) {
  const record = asRecord(value);
  return !!record && typeof record.code === "number" && Number.isFinite(record.code) && typeof record.message === "string";
}
function isToolCallStatus(value) {
  return typeof value === "string" && ACPX_EVENT_TOOL_CALL_STATUSES.includes(
    value
  );
}
function isFiniteInteger(value) {
  return typeof value === "number" && Number.isInteger(value);
}
function hasOnlyKeys(data, allowed) {
  const allowedSet = new Set(allowed);
  return Object.keys(data).every((key) => allowedSet.has(key));
}
function isAcpxEvent(value) {
  const event = asRecord(value);
  if (!event) {
    return false;
  }
  if (event.schema !== ACPX_EVENT_SCHEMA || typeof event.event_id !== "string" || typeof event.session_id !== "string" || typeof event.seq !== "number" || !Number.isInteger(event.seq) || event.seq < 0 || typeof event.ts !== "string" || typeof event.type !== "string") {
    return false;
  }
  if (event.request_id !== void 0 && typeof event.request_id !== "string") {
    return false;
  }
  if (event.acp_session_id !== void 0 && typeof event.acp_session_id !== "string") {
    return false;
  }
  if (event.agent_session_id !== void 0 && typeof event.agent_session_id !== "string") {
    return false;
  }
  const data = asRecord(event.data);
  if (!data) {
    return false;
  }
  switch (event.type) {
    case ACPX_EVENT_TYPES.TURN_STARTED:
      return hasOnlyKeys(data, ["mode", "resumed", "input_preview"]) && typeof data.mode === "string" && ACPX_EVENT_TURN_MODES.includes(
        data.mode
      ) && typeof data.resumed === "boolean" && (data.input_preview === void 0 || typeof data.input_preview === "string");
    case ACPX_EVENT_TYPES.OUTPUT_DELTA:
      return hasOnlyKeys(data, ["stream", "text"]) && isAcpxEventOutputStream(data.stream) && typeof data.text === "string";
    case ACPX_EVENT_TYPES.TOOL_CALL:
      return hasOnlyKeys(data, ["tool_call_id", "title", "status"]) && typeof data.tool_call_id === "string" && data.tool_call_id.trim().length > 0 && (data.title === void 0 || typeof data.title === "string" && data.title.trim().length > 0) && (data.status === void 0 || isToolCallStatus(data.status));
    case ACPX_EVENT_TYPES.PLAN:
      return hasOnlyKeys(data, ["entries"]) && Array.isArray(data.entries) && data.entries.every((entry) => {
        const parsed = asRecord(entry);
        return !!parsed && hasOnlyKeys(parsed, ["content", "status", "priority"]) && typeof parsed.content === "string" && typeof parsed.status === "string" && typeof parsed.priority === "string";
      });
    case ACPX_EVENT_TYPES.UPDATE:
      return hasOnlyKeys(data, ["update"]) && typeof data.update === "string";
    case ACPX_EVENT_TYPES.CLIENT_OPERATION:
      return hasOnlyKeys(data, ["method", "status", "summary", "details"]) && typeof data.method === "string" && typeof data.status === "string" && typeof data.summary === "string" && (data.details === void 0 || typeof data.details === "string");
    case ACPX_EVENT_TYPES.TURN_DONE:
      return hasOnlyKeys(data, ["stop_reason", "permission_stats"]) && typeof data.stop_reason === "string" && (data.permission_stats === void 0 || (() => {
        const stats = asRecord(data.permission_stats);
        return !!stats && hasOnlyKeys(stats, ["requested", "approved", "denied", "cancelled"]) && isFiniteInteger(stats.requested) && isFiniteInteger(stats.approved) && isFiniteInteger(stats.denied) && isFiniteInteger(stats.cancelled);
      })());
    case ACPX_EVENT_TYPES.ERROR:
      return hasOnlyKeys(data, [
        "code",
        "detail_code",
        "origin",
        "message",
        "retryable",
        "acp_error"
      ]) && isOutputErrorCode(data.code) && (data.detail_code === void 0 || typeof data.detail_code === "string") && (data.origin === void 0 || isOutputErrorOrigin(data.origin)) && typeof data.message === "string" && (data.retryable === void 0 || typeof data.retryable === "boolean") && (data.acp_error === void 0 || isAcpError(data.acp_error));
    case ACPX_EVENT_TYPES.PROMPT_QUEUED:
      return hasOnlyKeys(data, ["request_id"]) && typeof data.request_id === "string" && data.request_id.trim().length > 0;
    case ACPX_EVENT_TYPES.SESSION_ENSURED:
      return hasOnlyKeys(data, ["created", "name", "replaced_session_id"]) && typeof data.created === "boolean" && (data.name === void 0 || typeof data.name === "string") && (data.replaced_session_id === void 0 || typeof data.replaced_session_id === "string");
    case ACPX_EVENT_TYPES.CANCEL_REQUESTED:
      return hasOnlyKeys(data, []);
    case ACPX_EVENT_TYPES.CANCEL_RESULT:
      return hasOnlyKeys(data, ["cancelled"]) && typeof data.cancelled === "boolean";
    case ACPX_EVENT_TYPES.MODE_SET:
      return hasOnlyKeys(data, ["mode_id", "resumed"]) && typeof data.mode_id === "string" && data.mode_id.trim().length > 0 && (data.resumed === void 0 || typeof data.resumed === "boolean");
    case ACPX_EVENT_TYPES.CONFIG_SET:
      return hasOnlyKeys(data, ["config_id", "value", "resumed", "config_options"]) && typeof data.config_id === "string" && data.config_id.trim().length > 0 && typeof data.value === "string" && (data.resumed === void 0 || typeof data.resumed === "boolean") && (data.config_options === void 0 || Array.isArray(data.config_options));
    case ACPX_EVENT_TYPES.STATUS_SNAPSHOT:
      return hasOnlyKeys(data, [
        "status",
        "pid",
        "summary",
        "uptime",
        "last_prompt_time",
        "exit_code",
        "signal"
      ]) && typeof data.status === "string" && ACPX_EVENT_STATUS_SNAPSHOT_STATUSES.includes(
        data.status
      ) && (data.pid === void 0 || isFiniteInteger(data.pid) && data.pid > 0) && (data.summary === void 0 || typeof data.summary === "string") && (data.uptime === void 0 || typeof data.uptime === "string") && (data.last_prompt_time === void 0 || typeof data.last_prompt_time === "string") && (data.exit_code === void 0 || isFiniteInteger(data.exit_code)) && (data.signal === void 0 || typeof data.signal === "string");
    case ACPX_EVENT_TYPES.SESSION_CLOSED:
      return hasOnlyKeys(data, ["reason"]) && data.reason === "close";
    default:
      return false;
  }
}

// src/error-normalization.ts
var RESOURCE_NOT_FOUND_ACP_CODES = /* @__PURE__ */ new Set([-32002]);
var AUTH_REQUIRED_ACP_CODES = /* @__PURE__ */ new Set([-32e3]);
var QUERY_CLOSED_BEFORE_RESPONSE_DETAIL = "query closed before response received";
function asRecord2(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return void 0;
  }
  return value;
}
function isAuthRequiredMessage(value) {
  if (!value) {
    return false;
  }
  const normalized = value.toLowerCase();
  return normalized.includes("auth required") || normalized.includes("authentication required") || normalized.includes("authorization required") || normalized.includes("credential required") || normalized.includes("credentials required") || normalized.includes("token required") || normalized.includes("login required");
}
function isAcpAuthRequiredPayload(acp) {
  if (!acp) {
    return false;
  }
  if (!AUTH_REQUIRED_ACP_CODES.has(acp.code)) {
    return false;
  }
  if (isAuthRequiredMessage(acp.message)) {
    return true;
  }
  const data = asRecord2(acp.data);
  if (!data) {
    return false;
  }
  if (data.authRequired === true) {
    return true;
  }
  const methodId = data.methodId;
  if (typeof methodId === "string" && methodId.trim().length > 0) {
    return true;
  }
  const methods = data.methods;
  if (Array.isArray(methods) && methods.length > 0) {
    return true;
  }
  return false;
}
function isOutputErrorCode2(value) {
  return typeof value === "string" && OUTPUT_ERROR_CODES.includes(value);
}
function isOutputErrorOrigin2(value) {
  return typeof value === "string" && OUTPUT_ERROR_ORIGINS.includes(value);
}
function readOutputErrorMeta(error) {
  const record = asRecord2(error);
  if (!record) {
    return {};
  }
  const outputCode = isOutputErrorCode2(record.outputCode) ? record.outputCode : void 0;
  const detailCode = typeof record.detailCode === "string" && record.detailCode.trim().length > 0 ? record.detailCode : void 0;
  const origin = isOutputErrorOrigin2(record.origin) ? record.origin : void 0;
  const retryable = typeof record.retryable === "boolean" ? record.retryable : void 0;
  const acp = toAcpErrorPayload(record.acp);
  return {
    outputCode,
    detailCode,
    origin,
    retryable,
    acp
  };
}
function toAcpErrorPayload(value) {
  const record = asRecord2(value);
  if (!record) {
    return void 0;
  }
  if (typeof record.code !== "number" || !Number.isFinite(record.code)) {
    return void 0;
  }
  if (typeof record.message !== "string" || record.message.length === 0) {
    return void 0;
  }
  return {
    code: record.code,
    message: record.message,
    data: record.data
  };
}
function extractAcpErrorInternal(value, depth) {
  if (depth > 5) {
    return void 0;
  }
  const direct = toAcpErrorPayload(value);
  if (direct) {
    return direct;
  }
  const record = asRecord2(value);
  if (!record) {
    return void 0;
  }
  if ("error" in record) {
    const nested = extractAcpErrorInternal(record.error, depth + 1);
    if (nested) {
      return nested;
    }
  }
  if ("cause" in record) {
    const nested = extractAcpErrorInternal(record.cause, depth + 1);
    if (nested) {
      return nested;
    }
  }
  return void 0;
}
function isTimeoutLike(error) {
  return error instanceof Error && error.name === "TimeoutError";
}
function isNoSessionLike(error) {
  return error instanceof Error && error.name === "NoSessionError";
}
function isUsageLike(error) {
  if (!(error instanceof Error)) {
    return false;
  }
  return error.name === "CommanderError" || error.name === "InvalidArgumentError" || asRecord2(error)?.code === "commander.invalidArgument";
}
function formatErrorMessage(error) {
  if (error instanceof Error) {
    return error.message;
  }
  if (error && typeof error === "object") {
    const maybeMessage = error.message;
    if (typeof maybeMessage === "string" && maybeMessage.length > 0) {
      return maybeMessage;
    }
    try {
      return JSON.stringify(error);
    } catch {
    }
  }
  return String(error);
}
function extractAcpError(error) {
  return extractAcpErrorInternal(error, 0);
}
function isAcpResourceNotFoundError(error) {
  const acp = extractAcpError(error);
  return Boolean(acp && RESOURCE_NOT_FOUND_ACP_CODES.has(acp.code));
}
function isAcpQueryClosedBeforeResponseError(error) {
  const acp = extractAcpError(error);
  if (!acp || acp.code !== -32603) {
    return false;
  }
  const data = asRecord2(acp.data);
  const details = data?.details;
  if (typeof details !== "string") {
    return false;
  }
  return details.toLowerCase().includes(QUERY_CLOSED_BEFORE_RESPONSE_DETAIL);
}
function mapErrorCode(error) {
  if (error instanceof PermissionPromptUnavailableError) {
    return "PERMISSION_PROMPT_UNAVAILABLE";
  }
  if (error instanceof PermissionDeniedError) {
    return "PERMISSION_DENIED";
  }
  if (isTimeoutLike(error)) {
    return "TIMEOUT";
  }
  if (isNoSessionLike(error) || isAcpResourceNotFoundError(error)) {
    return "NO_SESSION";
  }
  if (isUsageLike(error)) {
    return "USAGE";
  }
  return void 0;
}
function normalizeOutputError(error, options = {}) {
  const meta = readOutputErrorMeta(error);
  const mapped = mapErrorCode(error);
  let code = mapped ?? options.defaultCode ?? "RUNTIME";
  if (meta.outputCode) {
    code = meta.outputCode;
  }
  if (code === "RUNTIME" && isAcpResourceNotFoundError(error)) {
    code = "NO_SESSION";
  }
  const acp = options.acp ?? meta.acp ?? extractAcpError(error);
  const detailCode = meta.detailCode ?? options.detailCode ?? (error instanceof AuthPolicyError || isAcpAuthRequiredPayload(acp) ? "AUTH_REQUIRED" : void 0);
  return {
    code,
    message: formatErrorMessage(error),
    detailCode,
    origin: meta.origin ?? options.origin,
    retryable: meta.retryable ?? options.retryable,
    acp
  };
}
function exitCodeForOutputErrorCode(code) {
  switch (code) {
    case "USAGE":
      return EXIT_CODES.USAGE;
    case "TIMEOUT":
      return EXIT_CODES.TIMEOUT;
    case "NO_SESSION":
      return EXIT_CODES.NO_SESSION;
    case "PERMISSION_DENIED":
    case "PERMISSION_PROMPT_UNAVAILABLE":
      return EXIT_CODES.PERMISSION_DENIED;
    case "RUNTIME":
    default:
      return EXIT_CODES.ERROR;
  }
}

// src/base-dir.ts
import os from "os";
import path3 from "path";
var overrideDir;
function setBaseDir(dir) {
  overrideDir = dir;
}
function getBaseDir() {
  return overrideDir ?? process.env.ACPX_HOME ?? path3.join(os.homedir(), ".acpx");
}

// src/session-persistence/repository.ts
import { statSync } from "fs";
import fs2 from "fs/promises";
import path5 from "path";

// src/persisted-key-policy.ts
var SNAKE_CASE_KEY = /^[a-z][a-z0-9_]*$/;
var ZED_TAG_KEYS = /* @__PURE__ */ new Set([
  "User",
  "Agent",
  "Resume",
  "Text",
  "Mention",
  "Image",
  "Thinking",
  "RedactedThinking",
  "ToolUse"
]);
var MAP_OBJECT_PATHS = /* @__PURE__ */ new Set([
  "request_token_usage",
  "messages.Agent.tool_results"
]);
var OPAQUE_VALUE_PATHS = /* @__PURE__ */ new Set([
  "agent_capabilities",
  "messages.Agent.content.ToolUse.input",
  "acpx.config_options"
]);
function isRecord(value) {
  return !!value && typeof value === "object" && !Array.isArray(value);
}
function joinPath(path6) {
  return path6.join(".");
}
function isAllowedKey(_path, key) {
  if (ZED_TAG_KEYS.has(key)) {
    return true;
  }
  return false;
}
function shouldSkipKeyRule(path6) {
  return MAP_OBJECT_PATHS.has(joinPath(path6));
}
function shouldSkipDescend(path6) {
  return OPAQUE_VALUE_PATHS.has(joinPath(path6)) || isToolResultOutputPath(path6);
}
function isToolResultOutputPath(path6) {
  if (path6.length < 5 || path6[path6.length - 1] !== "output") {
    return false;
  }
  const toolResultsIndex = path6.lastIndexOf("tool_results");
  if (toolResultsIndex === -1 || toolResultsIndex + 2 !== path6.length - 1) {
    return false;
  }
  const parentPath = path6.slice(0, toolResultsIndex + 1).join(".");
  return parentPath === "messages.Agent.tool_results";
}
function collectViolations(value, path6, violations) {
  if (Array.isArray(value)) {
    for (const entry of value) {
      collectViolations(entry, path6, violations);
    }
    return;
  }
  if (!isRecord(value)) {
    return;
  }
  const skipKeyRule = shouldSkipKeyRule(path6);
  for (const [key, child] of Object.entries(value)) {
    if (!skipKeyRule && !SNAKE_CASE_KEY.test(key) && !isAllowedKey(path6, key)) {
      violations.push(`${joinPath(path6)}.${key}`.replace(/^\./, ""));
    }
    const childPath = [...path6, key];
    if (shouldSkipDescend(childPath)) {
      continue;
    }
    collectViolations(child, childPath, violations);
  }
}
function findPersistedKeyPolicyViolations(value) {
  const violations = [];
  collectViolations(value, [], violations);
  return violations;
}
function assertPersistedKeyPolicy(value) {
  const violations = findPersistedKeyPolicyViolations(value);
  if (violations.length === 0) {
    return;
  }
  throw new Error(
    `Persisted key policy violation (expected snake_case keys): ${violations.join(", ")}`
  );
}

// src/session-event-log.ts
import path4 from "path";
var DEFAULT_EVENT_SEGMENT_MAX_BYTES = 64 * 1024 * 1024;
var DEFAULT_EVENT_MAX_SEGMENTS = 5;
function sessionBaseDir() {
  return path4.join(getBaseDir(), "sessions");
}
function safeSessionId(sessionId) {
  return encodeURIComponent(sessionId);
}
function sessionEventActivePath(sessionId) {
  return path4.join(sessionBaseDir(), `${safeSessionId(sessionId)}.events.ndjson`);
}
function sessionEventSegmentPath(sessionId, segment) {
  return path4.join(
    sessionBaseDir(),
    `${safeSessionId(sessionId)}.events.${segment}.ndjson`
  );
}
function sessionEventLockPath(sessionId) {
  return path4.join(sessionBaseDir(), `${safeSessionId(sessionId)}.events.lock`);
}
function defaultSessionEventLog(sessionId) {
  return {
    active_path: sessionEventActivePath(sessionId),
    segment_count: DEFAULT_EVENT_MAX_SEGMENTS,
    max_segment_bytes: DEFAULT_EVENT_SEGMENT_MAX_BYTES,
    max_segments: DEFAULT_EVENT_MAX_SEGMENTS,
    last_write_at: void 0,
    last_write_error: null
  };
}

// src/session-persistence/parse.ts
function asRecord3(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return void 0;
  }
  return value;
}
function hasOwn(source, key) {
  return Object.prototype.hasOwnProperty.call(source, key);
}
function isStringArray(value) {
  return Array.isArray(value) && value.every((entry) => typeof entry === "string");
}
function parseTokenUsage(raw) {
  if (raw === void 0 || raw === null) {
    return void 0;
  }
  const record = asRecord3(raw);
  if (!record) {
    return null;
  }
  const usage = {};
  const fields = [
    "input_tokens",
    "output_tokens",
    "cache_creation_input_tokens",
    "cache_read_input_tokens"
  ];
  for (const field of fields) {
    const value = record[field];
    if (value === void 0) {
      continue;
    }
    if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
      return null;
    }
    usage[field] = value;
  }
  return usage;
}
function parseRequestTokenUsage(raw) {
  if (raw === void 0 || raw === null) {
    return void 0;
  }
  const record = asRecord3(raw);
  if (!record) {
    return null;
  }
  const usage = {};
  for (const [key, value] of Object.entries(record)) {
    const parsed = parseTokenUsage(value);
    if (parsed == null) {
      return null;
    }
    usage[key] = parsed;
  }
  return usage;
}
function isSessionMessageImage(raw) {
  const record = asRecord3(raw);
  if (!record || typeof record.source !== "string") {
    return false;
  }
  if (record.size === void 0 || record.size === null) {
    return true;
  }
  const size = asRecord3(record.size);
  return !!size && typeof size.width === "number" && Number.isFinite(size.width) && typeof size.height === "number" && Number.isFinite(size.height);
}
function isUserContent(raw) {
  const record = asRecord3(raw);
  if (!record) {
    return false;
  }
  if (typeof record.Text === "string") {
    return true;
  }
  if (record.Mention !== void 0) {
    const mention = asRecord3(record.Mention);
    return !!mention && typeof mention.uri === "string" && typeof mention.content === "string";
  }
  if (record.Image !== void 0) {
    return isSessionMessageImage(record.Image);
  }
  return false;
}
function isToolUse(raw) {
  const record = asRecord3(raw);
  return !!record && typeof record.id === "string" && typeof record.name === "string" && typeof record.raw_input === "string" && hasOwn(record, "input") && typeof record.is_input_complete === "boolean" && (record.thought_signature === void 0 || record.thought_signature === null || typeof record.thought_signature === "string");
}
function isToolResultContent(raw) {
  const record = asRecord3(raw);
  if (!record) {
    return false;
  }
  if (typeof record.Text === "string") {
    return true;
  }
  if (record.Image !== void 0) {
    return isSessionMessageImage(record.Image);
  }
  return false;
}
function isToolResult(raw) {
  const record = asRecord3(raw);
  return !!record && typeof record.tool_use_id === "string" && typeof record.tool_name === "string" && typeof record.is_error === "boolean" && isToolResultContent(record.content);
}
function isAgentContent(raw) {
  const record = asRecord3(raw);
  if (!record) {
    return false;
  }
  if (typeof record.Text === "string") {
    return true;
  }
  if (record.Thinking !== void 0) {
    const thinking = asRecord3(record.Thinking);
    return !!thinking && typeof thinking.text === "string" && (thinking.signature === void 0 || thinking.signature === null || typeof thinking.signature === "string");
  }
  if (typeof record.RedactedThinking === "string") {
    return true;
  }
  if (record.ToolUse !== void 0) {
    return isToolUse(record.ToolUse);
  }
  return false;
}
function isUserMessage(raw) {
  const record = asRecord3(raw);
  if (!record || record.User === void 0) {
    return false;
  }
  const user = asRecord3(record.User);
  return !!user && typeof user.id === "string" && Array.isArray(user.content) && user.content.every((entry) => isUserContent(entry));
}
function isAgentMessage(raw) {
  const record = asRecord3(raw);
  if (!record || record.Agent === void 0) {
    return false;
  }
  const agent = asRecord3(record.Agent);
  if (!agent || !Array.isArray(agent.content) || !agent.content.every(isAgentContent)) {
    return false;
  }
  const toolResults = asRecord3(agent.tool_results);
  if (!toolResults) {
    return false;
  }
  return Object.values(toolResults).every(isToolResult);
}
function isConversationMessage(raw) {
  return raw === "Resume" || isUserMessage(raw) || isAgentMessage(raw);
}
function parseConversationRecord(record) {
  if (!Array.isArray(record.messages) || !record.messages.every(isConversationMessage) || typeof record.updated_at !== "string") {
    return void 0;
  }
  if (record.title !== void 0 && record.title !== null && typeof record.title !== "string") {
    return void 0;
  }
  const cumulativeTokenUsage = parseTokenUsage(record.cumulative_token_usage);
  const requestTokenUsage = parseRequestTokenUsage(record.request_token_usage);
  if (cumulativeTokenUsage === null || requestTokenUsage === null) {
    return void 0;
  }
  return {
    title: record.title === void 0 || record.title === null || typeof record.title === "string" ? record.title : null,
    messages: record.messages,
    updated_at: record.updated_at,
    cumulative_token_usage: cumulativeTokenUsage ?? {},
    request_token_usage: requestTokenUsage ?? {}
  };
}
function parseAcpxState(raw) {
  const record = asRecord3(raw);
  if (!record) {
    return void 0;
  }
  const state = {};
  if (typeof record.current_mode_id === "string") {
    state.current_mode_id = record.current_mode_id;
  }
  if (isStringArray(record.available_commands)) {
    state.available_commands = [...record.available_commands];
  }
  if (Array.isArray(record.config_options)) {
    state.config_options = record.config_options;
  }
  return state;
}
function parseEventLog(raw, sessionId) {
  const record = asRecord3(raw);
  if (!record) {
    return defaultSessionEventLog(sessionId);
  }
  if (typeof record.active_path !== "string" || typeof record.segment_count !== "number" || !Number.isInteger(record.segment_count) || record.segment_count < 1 || typeof record.max_segment_bytes !== "number" || !Number.isInteger(record.max_segment_bytes) || record.max_segment_bytes < 1 || typeof record.max_segments !== "number" || !Number.isInteger(record.max_segments) || record.max_segments < 1) {
    return defaultSessionEventLog(sessionId);
  }
  return {
    active_path: record.active_path,
    segment_count: record.segment_count,
    max_segment_bytes: record.max_segment_bytes,
    max_segments: record.max_segments,
    last_write_at: typeof record.last_write_at === "string" ? record.last_write_at : void 0,
    last_write_error: record.last_write_error == null || typeof record.last_write_error === "string" ? record.last_write_error : null
  };
}
function normalizeOptionalName(value) {
  if (value == null) {
    return void 0;
  }
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : void 0;
}
function normalizeOptionalPid(value) {
  if (value == null) {
    return void 0;
  }
  if (!Number.isInteger(value) || value <= 0) {
    return null;
  }
  return value;
}
function normalizeOptionalBoolean(value, fallback = false) {
  if (value == null) {
    return fallback;
  }
  return typeof value === "boolean" ? value : null;
}
function normalizeOptionalString(value) {
  if (value == null) {
    return void 0;
  }
  return typeof value === "string" ? value : null;
}
function normalizeOptionalExitCode(value) {
  if (value === void 0) {
    return void 0;
  }
  if (value === null) {
    return null;
  }
  if (Number.isInteger(value)) {
    return value;
  }
  return /* @__PURE__ */ Symbol("invalid");
}
function normalizeOptionalSignal(value) {
  if (value === void 0) {
    return void 0;
  }
  if (value === null) {
    return null;
  }
  if (typeof value === "string") {
    return value;
  }
  return /* @__PURE__ */ Symbol("invalid");
}
function parseSessionRecord(raw) {
  const record = asRecord3(raw);
  if (!record) {
    return null;
  }
  if (record.schema !== SESSION_RECORD_SCHEMA) {
    return null;
  }
  const name = normalizeOptionalName(record.name);
  const pid = normalizeOptionalPid(record.pid);
  const closed = normalizeOptionalBoolean(record.closed, false);
  const closedAt = normalizeOptionalString(record.closed_at);
  const agentStartedAt = normalizeOptionalString(record.agent_started_at);
  const lastPromptAt = normalizeOptionalString(record.last_prompt_at);
  const lastAgentExitCode = normalizeOptionalExitCode(record.last_agent_exit_code);
  const lastAgentExitSignal = normalizeOptionalSignal(record.last_agent_exit_signal);
  const lastAgentExitAt = normalizeOptionalString(record.last_agent_exit_at);
  const lastAgentDisconnectReason = normalizeOptionalString(
    record.last_agent_disconnect_reason
  );
  if (typeof record.acpx_record_id !== "string" || typeof record.acp_session_id !== "string" || typeof record.agent_command !== "string" || typeof record.cwd !== "string" || typeof record.created_at !== "string" || typeof record.last_used_at !== "string" || typeof record.last_seq !== "number" || !Number.isInteger(record.last_seq) || record.last_seq < 0 || name === null || pid === null || closed === null || closedAt === null || agentStartedAt === null || lastPromptAt === null || typeof lastAgentExitCode === "symbol" || typeof lastAgentExitSignal === "symbol" || lastAgentExitAt === null || lastAgentDisconnectReason === null) {
    return null;
  }
  const conversation = parseConversationRecord(record);
  if (!conversation) {
    return null;
  }
  const eventLog = parseEventLog(record.event_log, record.acpx_record_id);
  const lastRequestId = normalizeOptionalString(record.last_request_id);
  if (lastRequestId === null) {
    return null;
  }
  return {
    schema: SESSION_RECORD_SCHEMA,
    acpxRecordId: record.acpx_record_id,
    acpSessionId: record.acp_session_id,
    agentSessionId: normalizeRuntimeSessionId(record.agent_session_id),
    agentCommand: record.agent_command,
    cwd: record.cwd,
    name,
    createdAt: record.created_at,
    lastUsedAt: record.last_used_at,
    lastSeq: record.last_seq,
    lastRequestId,
    eventLog,
    closed,
    closedAt,
    pid,
    agentStartedAt,
    lastPromptAt,
    lastAgentExitCode,
    lastAgentExitSignal: lastAgentExitSignal == null ? lastAgentExitSignal : lastAgentExitSignal,
    lastAgentExitAt,
    lastAgentDisconnectReason,
    protocolVersion: typeof record.protocol_version === "number" ? record.protocol_version : void 0,
    agentCapabilities: asRecord3(
      record.agent_capabilities
    ),
    title: conversation.title,
    messages: conversation.messages,
    updated_at: conversation.updated_at,
    cumulative_token_usage: conversation.cumulative_token_usage,
    request_token_usage: conversation.request_token_usage,
    acpx: parseAcpxState(record.acpx)
  };
}

// src/session-persistence/serialize.ts
function serializeSessionRecordForDisk(record) {
  const canonical = {
    ...record,
    schema: SESSION_RECORD_SCHEMA
  };
  return {
    schema: canonical.schema,
    acpx_record_id: canonical.acpxRecordId,
    acp_session_id: canonical.acpSessionId,
    agent_session_id: normalizeRuntimeSessionId(canonical.agentSessionId),
    agent_command: canonical.agentCommand,
    cwd: canonical.cwd,
    name: canonical.name,
    created_at: canonical.createdAt,
    last_used_at: canonical.lastUsedAt,
    last_seq: canonical.lastSeq,
    last_request_id: canonical.lastRequestId,
    event_log: canonical.eventLog,
    closed: canonical.closed,
    closed_at: canonical.closedAt,
    pid: canonical.pid,
    agent_started_at: canonical.agentStartedAt,
    last_prompt_at: canonical.lastPromptAt,
    last_agent_exit_code: canonical.lastAgentExitCode,
    last_agent_exit_signal: canonical.lastAgentExitSignal,
    last_agent_exit_at: canonical.lastAgentExitAt,
    last_agent_disconnect_reason: canonical.lastAgentDisconnectReason,
    protocol_version: canonical.protocolVersion,
    agent_capabilities: canonical.agentCapabilities,
    title: canonical.title,
    messages: canonical.messages,
    updated_at: canonical.updated_at,
    cumulative_token_usage: canonical.cumulative_token_usage,
    request_token_usage: canonical.request_token_usage,
    acpx: canonical.acpx
  };
}

// src/session-persistence/repository.ts
var DEFAULT_HISTORY_LIMIT = 20;
function sessionFilePath(acpxRecordId) {
  const safeId = encodeURIComponent(acpxRecordId);
  return path5.join(sessionBaseDir2(), `${safeId}.json`);
}
function sessionBaseDir2() {
  return path5.join(getBaseDir(), "sessions");
}
async function ensureSessionDir() {
  await fs2.mkdir(sessionBaseDir2(), { recursive: true });
}
async function writeSessionRecord(record) {
  await ensureSessionDir();
  const persisted = serializeSessionRecordForDisk(record);
  assertPersistedKeyPolicy(persisted);
  const file = sessionFilePath(record.acpxRecordId);
  const tempFile = `${file}.${process.pid}.${Date.now()}.tmp`;
  const payload = JSON.stringify(persisted, null, 2);
  await fs2.writeFile(tempFile, `${payload}
`, "utf8");
  await fs2.rename(tempFile, file);
}
async function resolveSessionRecord(sessionId) {
  await ensureSessionDir();
  const directPath = sessionFilePath(sessionId);
  try {
    const directPayload = await fs2.readFile(directPath, "utf8");
    const directRecord = parseSessionRecord(JSON.parse(directPayload));
    if (directRecord) {
      return directRecord;
    }
  } catch {
  }
  const sessions = await listSessions();
  const exact = sessions.filter(
    (session) => session.acpxRecordId === sessionId || session.acpSessionId === sessionId
  );
  if (exact.length === 1) {
    return exact[0];
  }
  if (exact.length > 1) {
    throw new SessionResolutionError(`Multiple sessions match id: ${sessionId}`);
  }
  const suffixMatches = sessions.filter(
    (session) => session.acpxRecordId.endsWith(sessionId) || session.acpSessionId.endsWith(sessionId)
  );
  if (suffixMatches.length === 1) {
    return suffixMatches[0];
  }
  if (suffixMatches.length > 1) {
    throw new SessionResolutionError(`Session id is ambiguous: ${sessionId}`);
  }
  throw new SessionNotFoundError(sessionId);
}
function hasGitDirectory(dir) {
  const gitPath = path5.join(dir, ".git");
  try {
    return statSync(gitPath).isDirectory();
  } catch {
    return false;
  }
}
function isWithinBoundary(boundary, target) {
  const relative = path5.relative(boundary, target);
  return relative.length === 0 || !relative.startsWith("..") && !path5.isAbsolute(relative);
}
function absolutePath(value) {
  return path5.resolve(value);
}
function findGitRepositoryRoot(startDir) {
  let current = absolutePath(startDir);
  const root = path5.parse(current).root;
  for (; ; ) {
    if (hasGitDirectory(current)) {
      return current;
    }
    if (current === root) {
      return void 0;
    }
    const parent = path5.dirname(current);
    if (parent === current) {
      return void 0;
    }
    current = parent;
  }
}
function normalizeName(value) {
  if (value == null) {
    return void 0;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : void 0;
}
function isoNow3() {
  return (/* @__PURE__ */ new Date()).toISOString();
}
async function listSessions() {
  await ensureSessionDir();
  const entries = await fs2.readdir(sessionBaseDir2(), { withFileTypes: true });
  const records = [];
  for (const entry of entries) {
    if (!entry.isFile() || !entry.name.endsWith(".json")) {
      continue;
    }
    const fullPath = path5.join(sessionBaseDir2(), entry.name);
    try {
      const payload = await fs2.readFile(fullPath, "utf8");
      const parsed = parseSessionRecord(JSON.parse(payload));
      if (parsed) {
        records.push(parsed);
      }
    } catch {
    }
  }
  records.sort((a, b) => b.lastUsedAt.localeCompare(a.lastUsedAt));
  return records;
}
async function listSessionsForAgent(agentCommand) {
  const sessions = await listSessions();
  return sessions.filter((session) => session.agentCommand === agentCommand);
}
async function findSession(options) {
  const normalizedCwd = absolutePath(options.cwd);
  const normalizedName = normalizeName(options.name);
  const sessions = await listSessionsForAgent(options.agentCommand);
  return sessions.find((session) => {
    if (session.cwd !== normalizedCwd) {
      return false;
    }
    if (!options.includeClosed && session.closed) {
      return false;
    }
    if (normalizedName == null) {
      return session.name == null;
    }
    return session.name === normalizedName;
  });
}
async function findSessionByDirectoryWalk(options) {
  const normalizedName = normalizeName(options.name);
  const normalizedStart = absolutePath(options.cwd);
  const normalizedBoundary = absolutePath(options.boundary ?? normalizedStart);
  const walkBoundary = isWithinBoundary(normalizedBoundary, normalizedStart) ? normalizedBoundary : normalizedStart;
  const sessions = await listSessionsForAgent(options.agentCommand);
  const matchesScope = (session, dir) => {
    if (session.cwd !== dir) {
      return false;
    }
    if (session.closed) {
      return false;
    }
    if (normalizedName == null) {
      return session.name == null;
    }
    return session.name === normalizedName;
  };
  let current = normalizedStart;
  const walkRoot = path5.parse(current).root;
  for (; ; ) {
    const match = sessions.find((session) => matchesScope(session, current));
    if (match) {
      return match;
    }
    if (current === walkBoundary || current === walkRoot) {
      return void 0;
    }
    const parent = path5.dirname(current);
    if (parent === current) {
      return void 0;
    }
    current = parent;
    if (!isWithinBoundary(walkBoundary, current)) {
      return void 0;
    }
  }
}

export {
  AgentSpawnError,
  QueueConnectionError,
  QueueProtocolError,
  PermissionPromptUnavailableError,
  normalizeRuntimeSessionId,
  AcpClient,
  EXIT_CODES,
  OUTPUT_FORMATS,
  AUTH_POLICIES,
  NON_INTERACTIVE_PERMISSION_POLICIES,
  ACPX_EVENT_TYPES,
  OUTPUT_ERROR_CODES,
  OUTPUT_ERROR_ORIGINS,
  SESSION_RECORD_SCHEMA,
  truncateInputPreview,
  createAcpxEvent,
  sessionUpdateToEventDrafts,
  clientOperationToEventDraft,
  errorToEventDraft,
  isAcpxEvent,
  formatErrorMessage,
  extractAcpError,
  isAcpResourceNotFoundError,
  isAcpQueryClosedBeforeResponseError,
  normalizeOutputError,
  exitCodeForOutputErrorCode,
  assertPersistedKeyPolicy,
  setBaseDir,
  getBaseDir,
  DEFAULT_EVENT_SEGMENT_MAX_BYTES,
  DEFAULT_EVENT_MAX_SEGMENTS,
  sessionBaseDir,
  sessionEventActivePath,
  sessionEventSegmentPath,
  sessionEventLockPath,
  defaultSessionEventLog,
  DEFAULT_HISTORY_LIMIT,
  writeSessionRecord,
  resolveSessionRecord,
  absolutePath,
  findGitRepositoryRoot,
  normalizeName,
  isoNow3 as isoNow,
  listSessionsForAgent,
  findSession,
  findSessionByDirectoryWalk
};
