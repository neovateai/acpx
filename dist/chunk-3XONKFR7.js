import {
  AcpClient,
  AuthPolicyError,
  PermissionDeniedError,
  PermissionPromptUnavailableError,
  QueueConnectionError,
  QueueProtocolError,
  SessionNotFoundError,
  SessionResolutionError,
  normalizeRuntimeSessionId
} from "./chunk-6SQCOJO6.js";

// src/session-runtime.ts
import { spawn } from "child_process";
import fs4 from "fs/promises";
import path4 from "path";
import { fileURLToPath } from "url";

// src/events.ts
import { randomUUID } from "crypto";

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
function asRecord(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return void 0;
  }
  return value;
}
function isoNow() {
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
    event_id: randomUUID(),
    session_id: identity.sessionId,
    acp_session_id: trimNonEmpty(identity.acpSessionId),
    agent_session_id: trimNonEmpty(identity.agentSessionId),
    request_id: trimNonEmpty(draft.request_id ?? identity.requestId),
    seq: identity.seq,
    ts: identity.ts ?? isoNow(),
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

// src/session-conversation-model.ts
import { randomUUID as randomUUID2 } from "crypto";
function isoNow2() {
  return (/* @__PURE__ */ new Date()).toISOString();
}
function deepClone(value) {
  try {
    return structuredClone(value);
  } catch {
    return value;
  }
}
function hasOwn(source, key) {
  return Object.prototype.hasOwnProperty.call(source, key);
}
function normalizeAgentName(value) {
  if (typeof value !== "string") {
    return void 0;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : void 0;
}
function extractText(content) {
  if (content.type === "text") {
    return content.text;
  }
  if (content.type === "resource_link") {
    return content.title ?? content.name ?? content.uri;
  }
  if (content.type === "resource") {
    if ("text" in content.resource && typeof content.resource.text === "string") {
      return content.resource.text;
    }
    return content.resource.uri;
  }
  return void 0;
}
function contentToUserContent(content) {
  if (content.type === "text") {
    return {
      Text: content.text
    };
  }
  if (content.type === "resource_link") {
    const value = content.title ?? content.name ?? content.uri;
    return {
      Mention: {
        uri: content.uri,
        content: value
      }
    };
  }
  if (content.type === "resource") {
    if ("text" in content.resource && typeof content.resource.text === "string") {
      return {
        Text: content.resource.text
      };
    }
    return {
      Mention: {
        uri: content.resource.uri,
        content: content.resource.uri
      }
    };
  }
  if (content.type === "image") {
    return {
      Image: {
        source: content.data,
        size: null
      }
    };
  }
  return void 0;
}
function nextUserMessageId() {
  return randomUUID2();
}
function isUserMessage(message) {
  return typeof message === "object" && message !== null && hasOwn(message, "User");
}
function isAgentMessage(message) {
  return typeof message === "object" && message !== null && hasOwn(message, "Agent");
}
function isAgentTextContent(content) {
  return hasOwn(content, "Text");
}
function isAgentThinkingContent(content) {
  return hasOwn(content, "Thinking");
}
function isAgentToolUseContent(content) {
  return hasOwn(content, "ToolUse");
}
function updateConversationTimestamp(conversation, timestamp) {
  conversation.updated_at = timestamp;
}
function ensureAgentMessage(conversation) {
  const last = conversation.messages.at(-1);
  if (last && isAgentMessage(last)) {
    return last.Agent;
  }
  const created = {
    content: [],
    tool_results: {}
  };
  conversation.messages.push({ Agent: created });
  return created;
}
function appendAgentText(agent, text) {
  if (!text.trim()) {
    return;
  }
  const last = agent.content.at(-1);
  if (last && isAgentTextContent(last)) {
    last.Text += text;
    return;
  }
  const next = {
    Text: text
  };
  agent.content.push(next);
}
function appendAgentThinking(agent, text) {
  if (!text.trim()) {
    return;
  }
  const last = agent.content.at(-1);
  if (last && isAgentThinkingContent(last)) {
    last.Thinking.text += text;
    return;
  }
  const next = {
    Thinking: {
      text,
      signature: null
    }
  };
  agent.content.push(next);
}
function statusIndicatesComplete(status) {
  if (typeof status !== "string") {
    return false;
  }
  const normalized = status.toLowerCase();
  return normalized.includes("complete") || normalized.includes("done") || normalized.includes("success") || normalized.includes("failed") || normalized.includes("error") || normalized.includes("cancel");
}
function statusIndicatesError(status) {
  if (typeof status !== "string") {
    return false;
  }
  const normalized = status.toLowerCase();
  return normalized.includes("fail") || normalized.includes("error");
}
function toToolResultContent(value) {
  if (typeof value === "string") {
    return { Text: value };
  }
  if (value != null) {
    try {
      return { Text: JSON.stringify(value) };
    } catch {
      return { Text: String(value) };
    }
  }
  return { Text: "" };
}
function toRawInput(value) {
  if (typeof value === "string") {
    return value;
  }
  try {
    return JSON.stringify(value ?? {});
  } catch {
    return String(value ?? "");
  }
}
function ensureToolUseContent(agent, toolCallId) {
  for (const content of agent.content) {
    if (isAgentToolUseContent(content) && content.ToolUse.id === toolCallId) {
      return content.ToolUse;
    }
  }
  const created = {
    id: toolCallId,
    name: "tool_call",
    raw_input: "{}",
    input: {},
    is_input_complete: false,
    thought_signature: null
  };
  agent.content.push({ ToolUse: created });
  return created;
}
function upsertToolResult(agent, toolCallId, patch) {
  const existing = agent.tool_results[toolCallId];
  const next = {
    tool_use_id: toolCallId,
    tool_name: patch.tool_name ?? existing?.tool_name ?? "tool_call",
    is_error: patch.is_error ?? existing?.is_error ?? false,
    content: patch.content ?? existing?.content ?? { Text: "" },
    output: patch.output ?? existing?.output
  };
  agent.tool_results[toolCallId] = next;
}
function applyToolCallUpdate(agent, update) {
  const tool = ensureToolUseContent(agent, update.toolCallId);
  if (hasOwn(update, "title")) {
    tool.name = normalizeAgentName(update.title) ?? tool.name ?? "tool_call";
  }
  if (hasOwn(update, "kind")) {
    const kindName = normalizeAgentName(update.kind);
    if (!tool.name || tool.name === "tool_call") {
      tool.name = kindName ?? tool.name;
    }
  }
  if (hasOwn(update, "rawInput")) {
    const rawInput = deepClone(update.rawInput);
    tool.input = rawInput ?? {};
    tool.raw_input = toRawInput(rawInput);
  }
  if (hasOwn(update, "status")) {
    tool.is_input_complete = statusIndicatesComplete(
      update.status
    );
  }
  if (hasOwn(update, "rawOutput") || hasOwn(update, "status") || hasOwn(update, "title") || hasOwn(update, "kind")) {
    const status = update.status;
    const output = hasOwn(update, "rawOutput") ? deepClone(update.rawOutput) : void 0;
    upsertToolResult(agent, update.toolCallId, {
      tool_name: tool.name,
      is_error: statusIndicatesError(status),
      content: output === void 0 ? void 0 : toToolResultContent(output),
      output
    });
  }
}
function asRecord3(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return void 0;
  }
  return value;
}
function numberField(source, keys) {
  for (const key of keys) {
    const value = source[key];
    if (typeof value === "number" && Number.isFinite(value) && value >= 0) {
      return value;
    }
  }
  return void 0;
}
function usageToTokenUsage(update) {
  const updateRecord = asRecord3(update);
  const usageMeta = asRecord3(updateRecord?._meta)?.usage;
  const source = asRecord3(usageMeta) ?? updateRecord;
  if (!source) {
    return void 0;
  }
  const normalized = {
    input_tokens: numberField(source, ["input_tokens", "inputTokens"]),
    output_tokens: numberField(source, ["output_tokens", "outputTokens"]),
    cache_creation_input_tokens: numberField(source, [
      "cache_creation_input_tokens",
      "cacheCreationInputTokens",
      "cachedWriteTokens"
    ]),
    cache_read_input_tokens: numberField(source, [
      "cache_read_input_tokens",
      "cacheReadInputTokens",
      "cachedReadTokens"
    ])
  };
  if (normalized.input_tokens === void 0 && normalized.output_tokens === void 0 && normalized.cache_creation_input_tokens === void 0 && normalized.cache_read_input_tokens === void 0) {
    return void 0;
  }
  return normalized;
}
function ensureAcpxState(state) {
  return state ?? {};
}
function lastUserMessageId(conversation) {
  for (let index = conversation.messages.length - 1; index >= 0; index -= 1) {
    const message = conversation.messages[index];
    if (message && isUserMessage(message)) {
      return message.User.id;
    }
  }
  return void 0;
}
function createSessionConversation(timestamp = isoNow2()) {
  return {
    title: null,
    messages: [],
    updated_at: timestamp,
    cumulative_token_usage: {},
    request_token_usage: {}
  };
}
function cloneSessionConversation(conversation) {
  if (!conversation) {
    return createSessionConversation();
  }
  return {
    title: conversation.title,
    messages: deepClone(conversation.messages ?? []),
    updated_at: conversation.updated_at,
    cumulative_token_usage: deepClone(conversation.cumulative_token_usage ?? {}),
    request_token_usage: deepClone(conversation.request_token_usage ?? {})
  };
}
function cloneSessionAcpxState(state) {
  if (!state) {
    return void 0;
  }
  return {
    current_mode_id: state.current_mode_id,
    available_commands: state.available_commands ? [...state.available_commands] : void 0,
    config_options: state.config_options ? deepClone(state.config_options) : void 0
  };
}
function recordPromptSubmission(conversation, prompt, timestamp = isoNow2()) {
  const text = prompt.trim();
  if (!text) {
    return;
  }
  conversation.messages.push({
    User: {
      id: nextUserMessageId(),
      content: [{ Text: text }]
    }
  });
  updateConversationTimestamp(conversation, timestamp);
}
function recordSessionUpdate(conversation, state, notification, timestamp = isoNow2()) {
  const acpx = ensureAcpxState(state);
  const update = notification.update;
  switch (update.sessionUpdate) {
    case "user_message_chunk": {
      const userContent = contentToUserContent(update.content);
      if (userContent) {
        conversation.messages.push({
          User: {
            id: nextUserMessageId(),
            content: [userContent]
          }
        });
      }
      break;
    }
    case "agent_message_chunk": {
      const text = extractText(update.content);
      if (text) {
        const agent = ensureAgentMessage(conversation);
        appendAgentText(agent, text);
      }
      break;
    }
    case "agent_thought_chunk": {
      const text = extractText(update.content);
      if (text) {
        const agent = ensureAgentMessage(conversation);
        appendAgentThinking(agent, text);
      }
      break;
    }
    case "tool_call":
    case "tool_call_update": {
      const agent = ensureAgentMessage(conversation);
      applyToolCallUpdate(agent, update);
      break;
    }
    case "usage_update": {
      const usage = usageToTokenUsage(update);
      if (usage) {
        conversation.cumulative_token_usage = usage;
        const userId = lastUserMessageId(conversation);
        if (userId) {
          conversation.request_token_usage[userId] = usage;
        }
      }
      break;
    }
    case "session_info_update": {
      if (hasOwn(update, "title")) {
        conversation.title = update.title ?? null;
      }
      if (hasOwn(update, "updatedAt")) {
        conversation.updated_at = update.updatedAt ?? conversation.updated_at;
      }
      break;
    }
    case "available_commands_update": {
      acpx.available_commands = update.availableCommands.map((entry) => entry.name).filter((entry) => typeof entry === "string" && entry.trim().length > 0);
      break;
    }
    case "current_mode_update": {
      acpx.current_mode_id = update.currentModeId;
      break;
    }
    case "config_option_update": {
      acpx.config_options = deepClone(update.configOptions);
      break;
    }
    default:
      break;
  }
  updateConversationTimestamp(conversation, timestamp);
  return acpx;
}
function recordClientOperation(conversation, state, operation, timestamp = isoNow2()) {
  const acpx = ensureAcpxState(state);
  updateConversationTimestamp(conversation, timestamp);
  return acpx;
}

// src/session-events.ts
import fs2 from "fs/promises";

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
function joinPath(path5) {
  return path5.join(".");
}
function isAllowedKey(path5, key) {
  if (ZED_TAG_KEYS.has(key)) {
    return true;
  }
  return false;
}
function shouldSkipKeyRule(path5) {
  return MAP_OBJECT_PATHS.has(joinPath(path5));
}
function shouldSkipDescend(path5) {
  return OPAQUE_VALUE_PATHS.has(joinPath(path5)) || isToolResultOutputPath(path5);
}
function isToolResultOutputPath(path5) {
  if (path5.length < 5 || path5[path5.length - 1] !== "output") {
    return false;
  }
  const toolResultsIndex = path5.lastIndexOf("tool_results");
  if (toolResultsIndex === -1 || toolResultsIndex + 2 !== path5.length - 1) {
    return false;
  }
  const parentPath = path5.slice(0, toolResultsIndex + 1).join(".");
  return parentPath === "messages.Agent.tool_results";
}
function collectViolations(value, path5, violations) {
  if (Array.isArray(value)) {
    for (const entry of value) {
      collectViolations(entry, path5, violations);
    }
    return;
  }
  if (!isRecord(value)) {
    return;
  }
  const skipKeyRule = shouldSkipKeyRule(path5);
  for (const [key, child] of Object.entries(value)) {
    if (!skipKeyRule && !SNAKE_CASE_KEY.test(key) && !isAllowedKey(path5, key)) {
      violations.push(`${joinPath(path5)}.${key}`.replace(/^\./, ""));
    }
    const childPath = [...path5, key];
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
import os from "os";
import path from "path";
var DEFAULT_EVENT_SEGMENT_MAX_BYTES = 64 * 1024 * 1024;
var DEFAULT_EVENT_MAX_SEGMENTS = 5;
function sessionBaseDir() {
  return path.join(os.homedir(), ".acpx", "sessions");
}
function safeSessionId(sessionId) {
  return encodeURIComponent(sessionId);
}
function sessionEventActivePath(sessionId) {
  return path.join(sessionBaseDir(), `${safeSessionId(sessionId)}.events.ndjson`);
}
function sessionEventSegmentPath(sessionId, segment) {
  return path.join(
    sessionBaseDir(),
    `${safeSessionId(sessionId)}.events.${segment}.ndjson`
  );
}
function sessionEventLockPath(sessionId) {
  return path.join(sessionBaseDir(), `${safeSessionId(sessionId)}.events.lock`);
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
import { statSync } from "fs";
import fs from "fs/promises";
import os2 from "os";
import path2 from "path";

// src/session-persistence/parse.ts
function asRecord4(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return void 0;
  }
  return value;
}
function hasOwn2(source, key) {
  return Object.prototype.hasOwnProperty.call(source, key);
}
function isStringArray(value) {
  return Array.isArray(value) && value.every((entry) => typeof entry === "string");
}
function parseTokenUsage(raw) {
  if (raw === void 0 || raw === null) {
    return void 0;
  }
  const record = asRecord4(raw);
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
  const record = asRecord4(raw);
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
  const record = asRecord4(raw);
  if (!record || typeof record.source !== "string") {
    return false;
  }
  if (record.size === void 0 || record.size === null) {
    return true;
  }
  const size = asRecord4(record.size);
  return !!size && typeof size.width === "number" && Number.isFinite(size.width) && typeof size.height === "number" && Number.isFinite(size.height);
}
function isUserContent(raw) {
  const record = asRecord4(raw);
  if (!record) {
    return false;
  }
  if (typeof record.Text === "string") {
    return true;
  }
  if (record.Mention !== void 0) {
    const mention = asRecord4(record.Mention);
    return !!mention && typeof mention.uri === "string" && typeof mention.content === "string";
  }
  if (record.Image !== void 0) {
    return isSessionMessageImage(record.Image);
  }
  return false;
}
function isToolUse(raw) {
  const record = asRecord4(raw);
  return !!record && typeof record.id === "string" && typeof record.name === "string" && typeof record.raw_input === "string" && hasOwn2(record, "input") && typeof record.is_input_complete === "boolean" && (record.thought_signature === void 0 || record.thought_signature === null || typeof record.thought_signature === "string");
}
function isToolResultContent(raw) {
  const record = asRecord4(raw);
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
  const record = asRecord4(raw);
  return !!record && typeof record.tool_use_id === "string" && typeof record.tool_name === "string" && typeof record.is_error === "boolean" && isToolResultContent(record.content);
}
function isAgentContent(raw) {
  const record = asRecord4(raw);
  if (!record) {
    return false;
  }
  if (typeof record.Text === "string") {
    return true;
  }
  if (record.Thinking !== void 0) {
    const thinking = asRecord4(record.Thinking);
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
function isUserMessage2(raw) {
  const record = asRecord4(raw);
  if (!record || record.User === void 0) {
    return false;
  }
  const user = asRecord4(record.User);
  return !!user && typeof user.id === "string" && Array.isArray(user.content) && user.content.every((entry) => isUserContent(entry));
}
function isAgentMessage2(raw) {
  const record = asRecord4(raw);
  if (!record || record.Agent === void 0) {
    return false;
  }
  const agent = asRecord4(record.Agent);
  if (!agent || !Array.isArray(agent.content) || !agent.content.every(isAgentContent)) {
    return false;
  }
  const toolResults = asRecord4(agent.tool_results);
  if (!toolResults) {
    return false;
  }
  return Object.values(toolResults).every(isToolResult);
}
function isConversationMessage(raw) {
  return raw === "Resume" || isUserMessage2(raw) || isAgentMessage2(raw);
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
  const record = asRecord4(raw);
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
  const record = asRecord4(raw);
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
  const record = asRecord4(raw);
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
    agentCapabilities: asRecord4(
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

// src/session-persistence/repository.ts
var DEFAULT_HISTORY_LIMIT = 20;
function sessionFilePath(acpxRecordId) {
  const safeId = encodeURIComponent(acpxRecordId);
  return path2.join(sessionBaseDir2(), `${safeId}.json`);
}
function sessionBaseDir2() {
  return path2.join(os2.homedir(), ".acpx", "sessions");
}
async function ensureSessionDir() {
  await fs.mkdir(sessionBaseDir2(), { recursive: true });
}
async function writeSessionRecord(record) {
  await ensureSessionDir();
  const persisted = serializeSessionRecordForDisk(record);
  assertPersistedKeyPolicy(persisted);
  const file = sessionFilePath(record.acpxRecordId);
  const tempFile = `${file}.${process.pid}.${Date.now()}.tmp`;
  const payload = JSON.stringify(persisted, null, 2);
  await fs.writeFile(tempFile, `${payload}
`, "utf8");
  await fs.rename(tempFile, file);
}
async function resolveSessionRecord(sessionId) {
  await ensureSessionDir();
  const directPath = sessionFilePath(sessionId);
  try {
    const directPayload = await fs.readFile(directPath, "utf8");
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
  const gitPath = path2.join(dir, ".git");
  try {
    return statSync(gitPath).isDirectory();
  } catch {
    return false;
  }
}
function isWithinBoundary(boundary, target) {
  const relative = path2.relative(boundary, target);
  return relative.length === 0 || !relative.startsWith("..") && !path2.isAbsolute(relative);
}
function absolutePath(value) {
  return path2.resolve(value);
}
function findGitRepositoryRoot(startDir) {
  let current = absolutePath(startDir);
  const root = path2.parse(current).root;
  for (; ; ) {
    if (hasGitDirectory(current)) {
      return current;
    }
    if (current === root) {
      return void 0;
    }
    const parent = path2.dirname(current);
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
  const entries = await fs.readdir(sessionBaseDir2(), { withFileTypes: true });
  const records = [];
  for (const entry of entries) {
    if (!entry.isFile() || !entry.name.endsWith(".json")) {
      continue;
    }
    const fullPath = path2.join(sessionBaseDir2(), entry.name);
    try {
      const payload = await fs.readFile(fullPath, "utf8");
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
  const walkRoot = path2.parse(current).root;
  for (; ; ) {
    const match = sessions.find((session) => matchesScope(session, current));
    if (match) {
      return match;
    }
    if (current === walkBoundary || current === walkRoot) {
      return void 0;
    }
    const parent = path2.dirname(current);
    if (parent === current) {
      return void 0;
    }
    current = parent;
    if (!isWithinBoundary(walkBoundary, current)) {
      return void 0;
    }
  }
}

// src/session-events.ts
var LOCK_RETRY_MS = 15;
async function ensureSessionDir2() {
  await fs2.mkdir(sessionBaseDir(), { recursive: true });
}
async function pathExists(filePath) {
  try {
    await fs2.access(filePath);
    return true;
  } catch {
    return false;
  }
}
async function statSize(filePath) {
  try {
    const stats = await fs2.stat(filePath);
    return stats.size;
  } catch {
    return 0;
  }
}
async function countExistingEventSegments(sessionId, maxSegments) {
  let count = 0;
  for (let segment = 1; segment <= maxSegments; segment += 1) {
    if (await pathExists(sessionEventSegmentPath(sessionId, segment))) {
      count += 1;
    }
  }
  if (await pathExists(sessionEventActivePath(sessionId))) {
    count += 1;
  }
  return count;
}
async function rotateSegments(sessionId, maxSegments) {
  const active = sessionEventActivePath(sessionId);
  const overflow = sessionEventSegmentPath(sessionId, maxSegments);
  await fs2.unlink(overflow).catch((error) => {
    if (error.code !== "ENOENT") {
      throw error;
    }
  });
  for (let segment = maxSegments - 1; segment >= 1; segment -= 1) {
    const from = sessionEventSegmentPath(sessionId, segment);
    const to = sessionEventSegmentPath(sessionId, segment + 1);
    if (!await pathExists(from)) {
      continue;
    }
    await fs2.rename(from, to);
  }
  if (await pathExists(active)) {
    await fs2.rename(active, sessionEventSegmentPath(sessionId, 1));
  }
}
async function acquireEventsLock(sessionId) {
  await ensureSessionDir2();
  const lockPath = sessionEventLockPath(sessionId);
  const payload = JSON.stringify(
    {
      pid: process.pid,
      created_at: (/* @__PURE__ */ new Date()).toISOString()
    },
    null,
    2
  );
  for (; ; ) {
    try {
      await fs2.writeFile(lockPath, `${payload}
`, {
        encoding: "utf8",
        flag: "wx"
      });
      return { filePath: lockPath };
    } catch (error) {
      const code = error.code;
      if (code !== "EEXIST") {
        throw error;
      }
      await new Promise((resolve) => {
        setTimeout(resolve, LOCK_RETRY_MS);
      });
    }
  }
}
async function releaseEventsLock(lock) {
  await fs2.unlink(lock.filePath).catch((error) => {
    if (error.code !== "ENOENT") {
      throw error;
    }
  });
}
var SessionEventWriter = class _SessionEventWriter {
  record;
  lock;
  maxSegmentBytes;
  maxSegments;
  nextSeq;
  closed = false;
  constructor(record, lock, options) {
    this.record = record;
    this.lock = lock;
    this.maxSegmentBytes = options.maxSegmentBytes;
    this.maxSegments = options.maxSegments;
    this.nextSeq = record.lastSeq + 1;
  }
  static async open(record, options = {}) {
    const lock = await acquireEventsLock(record.acpxRecordId);
    return new _SessionEventWriter(record, lock, {
      maxSegmentBytes: options.maxSegmentBytes ?? record.eventLog.max_segment_bytes ?? DEFAULT_EVENT_SEGMENT_MAX_BYTES,
      maxSegments: options.maxSegments ?? record.eventLog.max_segments ?? DEFAULT_EVENT_MAX_SEGMENTS
    });
  }
  getRecord() {
    return this.record;
  }
  createEvent(draft) {
    const event = createAcpxEvent(
      {
        sessionId: this.record.acpxRecordId,
        acpSessionId: this.record.acpSessionId,
        agentSessionId: this.record.agentSessionId,
        requestId: draft.request_id,
        seq: this.nextSeq
      },
      draft
    );
    this.nextSeq += 1;
    return event;
  }
  async appendEvent(event, options = {}) {
    await this.appendEvents([event], options);
  }
  async appendEvents(events, options = {}) {
    if (this.closed) {
      throw new Error("SessionEventWriter is closed");
    }
    if (events.length === 0) {
      return;
    }
    await ensureSessionDir2();
    let activePath = sessionEventActivePath(this.record.acpxRecordId);
    for (const event of events) {
      if (!isAcpxEvent(event)) {
        throw new Error("Attempted to persist invalid acpx.event.v1 payload");
      }
      if (event.seq !== this.record.lastSeq + 1) {
        throw new Error(
          `acpx event sequence mismatch: expected ${this.record.lastSeq + 1}, got ${event.seq}`
        );
      }
      assertPersistedKeyPolicy(event);
      const line = `${JSON.stringify(event)}
`;
      const lineBytes = Buffer.byteLength(line);
      const currentSize = await statSize(activePath);
      if (currentSize > 0 && currentSize + lineBytes > this.maxSegmentBytes) {
        await rotateSegments(this.record.acpxRecordId, this.maxSegments);
        activePath = sessionEventActivePath(this.record.acpxRecordId);
      }
      await fs2.appendFile(activePath, line, "utf8");
      this.record.lastSeq = event.seq;
      if (event.seq >= this.nextSeq) {
        this.nextSeq = event.seq + 1;
      }
      this.record.lastRequestId = event.request_id ?? this.record.lastRequestId;
      this.record.lastUsedAt = event.ts;
      this.record.eventLog = {
        active_path: activePath,
        segment_count: await countExistingEventSegments(
          this.record.acpxRecordId,
          this.maxSegments
        ),
        max_segment_bytes: this.maxSegmentBytes,
        max_segments: this.maxSegments,
        last_write_at: event.ts,
        last_write_error: null
      };
    }
    if (options.checkpoint === true) {
      await writeSessionRecord(this.record);
    }
  }
  async appendDraft(draft, options = {}) {
    const event = this.createEvent(draft);
    await this.appendEvent(event, options);
    return event;
  }
  async appendDrafts(drafts, options = {}) {
    const events = drafts.map((draft) => this.createEvent(draft));
    await this.appendEvents(events, options);
    return events;
  }
  async checkpoint() {
    if (this.closed) {
      throw new Error("SessionEventWriter is closed");
    }
    await writeSessionRecord(this.record);
  }
  async close(options = {}) {
    if (this.closed) {
      return;
    }
    try {
      if (options.checkpoint !== false) {
        await writeSessionRecord(this.record);
      }
    } finally {
      this.closed = true;
      await releaseEventsLock(this.lock);
    }
  }
};

// src/session-runtime-helpers.ts
var TimeoutError = class extends Error {
  constructor(timeoutMs) {
    super(`Timed out after ${timeoutMs}ms`);
    this.name = "TimeoutError";
  }
};
var InterruptedError = class extends Error {
  constructor() {
    super("Interrupted");
    this.name = "InterruptedError";
  }
};
async function withTimeout(promise, timeoutMs) {
  if (timeoutMs == null || timeoutMs <= 0) {
    return await promise;
  }
  let timer;
  const timeoutPromise = new Promise((_resolve, reject) => {
    timer = setTimeout(() => {
      reject(new TimeoutError(timeoutMs));
    }, timeoutMs);
  });
  try {
    return await Promise.race([promise, timeoutPromise]);
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
}
async function withInterrupt(run, onInterrupt) {
  return await new Promise((resolve, reject) => {
    let settled = false;
    const finish = (cb) => {
      if (settled) {
        return;
      }
      settled = true;
      process.off("SIGINT", onSigint);
      process.off("SIGTERM", onSigterm);
      cb();
    };
    const onSigint = () => {
      void onInterrupt().finally(() => {
        finish(() => reject(new InterruptedError()));
      });
    };
    const onSigterm = () => {
      void onInterrupt().finally(() => {
        finish(() => reject(new InterruptedError()));
      });
    };
    process.once("SIGINT", onSigint);
    process.once("SIGTERM", onSigterm);
    void run().then(
      (result) => finish(() => resolve(result)),
      (error) => finish(() => reject(error))
    );
  });
}

// src/queue-owner-turn-controller.ts
var QueueOwnerTurnController = class {
  options;
  state = "idle";
  pendingCancel = false;
  activeController;
  constructor(options) {
    this.options = options;
  }
  get lifecycleState() {
    return this.state;
  }
  get hasPendingCancel() {
    return this.pendingCancel;
  }
  beginTurn() {
    this.state = "starting";
    this.pendingCancel = false;
  }
  markPromptActive() {
    if (this.state === "starting" || this.state === "active") {
      this.state = "active";
    }
  }
  endTurn() {
    this.state = "idle";
    this.pendingCancel = false;
  }
  beginClosing() {
    this.state = "closing";
    this.pendingCancel = false;
    this.activeController = void 0;
  }
  setActiveController(controller) {
    this.activeController = controller;
  }
  clearActiveController() {
    this.activeController = void 0;
  }
  assertCanHandleControlRequest() {
    if (this.state === "closing") {
      throw new QueueConnectionError("Queue owner is closing", {
        detailCode: "QUEUE_OWNER_SHUTTING_DOWN",
        origin: "queue",
        retryable: true
      });
    }
  }
  async requestCancel() {
    const activeController = this.activeController;
    if (activeController?.hasActivePrompt()) {
      const cancelled = await activeController.requestCancelActivePrompt();
      if (cancelled) {
        this.pendingCancel = false;
      }
      return cancelled;
    }
    if (this.state === "starting" || this.state === "active") {
      this.pendingCancel = true;
      return true;
    }
    return false;
  }
  async applyPendingCancel() {
    const activeController = this.activeController;
    if (!this.pendingCancel || !activeController || !activeController.hasActivePrompt()) {
      return false;
    }
    const cancelled = await activeController.requestCancelActivePrompt();
    if (cancelled) {
      this.pendingCancel = false;
    }
    return cancelled;
  }
  async setSessionMode(modeId, timeoutMs) {
    this.assertCanHandleControlRequest();
    const activeController = this.activeController;
    if (activeController) {
      await this.options.withTimeout(
        async () => await activeController.setSessionMode(modeId),
        timeoutMs
      );
      return;
    }
    await this.options.setSessionModeFallback(modeId, timeoutMs);
  }
  async setSessionConfigOption(configId, value, timeoutMs) {
    this.assertCanHandleControlRequest();
    const activeController = this.activeController;
    if (activeController) {
      return await this.options.withTimeout(
        async () => await activeController.setSessionConfigOption(configId, value),
        timeoutMs
      );
    }
    return await this.options.setSessionConfigOptionFallback(
      configId,
      value,
      timeoutMs
    );
  }
};

// src/queue-ipc.ts
import { createHash, randomUUID as randomUUID3 } from "crypto";
import fs3 from "fs/promises";
import net from "net";
import os3 from "os";
import path3 from "path";

// src/queue-messages.ts
function asRecord5(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return void 0;
  }
  return value;
}
function isPermissionMode(value) {
  return value === "approve-all" || value === "approve-reads" || value === "deny-all";
}
function isNonInteractivePermissionPolicy(value) {
  return value === "deny" || value === "fail";
}
function isOutputErrorCode3(value) {
  return typeof value === "string" && OUTPUT_ERROR_CODES.includes(value);
}
function isOutputErrorOrigin3(value) {
  return typeof value === "string" && OUTPUT_ERROR_ORIGINS.includes(value);
}
function parseAcpError(value) {
  const record = asRecord5(value);
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
function parseQueueRequest(raw) {
  const request = asRecord5(raw);
  if (!request) {
    return null;
  }
  if (typeof request.type !== "string" || typeof request.requestId !== "string") {
    return null;
  }
  const timeoutRaw = request.timeoutMs;
  const timeoutMs = typeof timeoutRaw === "number" && Number.isFinite(timeoutRaw) && timeoutRaw > 0 ? Math.round(timeoutRaw) : void 0;
  if (request.type === "submit_prompt") {
    const nonInteractivePermissions = request.nonInteractivePermissions == null ? void 0 : isNonInteractivePermissionPolicy(request.nonInteractivePermissions) ? request.nonInteractivePermissions : null;
    const suppressSdkConsoleErrors = request.suppressSdkConsoleErrors == null ? void 0 : typeof request.suppressSdkConsoleErrors === "boolean" ? request.suppressSdkConsoleErrors : null;
    if (typeof request.message !== "string" || !isPermissionMode(request.permissionMode) || nonInteractivePermissions === null || suppressSdkConsoleErrors === null || typeof request.waitForCompletion !== "boolean") {
      return null;
    }
    return {
      type: "submit_prompt",
      requestId: request.requestId,
      message: request.message,
      permissionMode: request.permissionMode,
      nonInteractivePermissions,
      timeoutMs,
      ...suppressSdkConsoleErrors !== void 0 ? { suppressSdkConsoleErrors } : {},
      waitForCompletion: request.waitForCompletion
    };
  }
  if (request.type === "cancel_prompt") {
    return {
      type: "cancel_prompt",
      requestId: request.requestId
    };
  }
  if (request.type === "set_mode") {
    if (typeof request.modeId !== "string" || request.modeId.trim().length === 0) {
      return null;
    }
    return {
      type: "set_mode",
      requestId: request.requestId,
      modeId: request.modeId,
      timeoutMs
    };
  }
  if (request.type === "set_config_option") {
    if (typeof request.configId !== "string" || request.configId.trim().length === 0 || typeof request.value !== "string" || request.value.trim().length === 0) {
      return null;
    }
    return {
      type: "set_config_option",
      requestId: request.requestId,
      configId: request.configId,
      value: request.value,
      timeoutMs
    };
  }
  return null;
}
function parseSessionSendResult(raw) {
  const result = asRecord5(raw);
  if (!result) {
    return null;
  }
  if (typeof result.stopReason !== "string" || typeof result.sessionId !== "string" || typeof result.resumed !== "boolean") {
    return null;
  }
  const permissionStats = asRecord5(result.permissionStats);
  const record = asRecord5(result.record);
  if (!permissionStats || !record) {
    return null;
  }
  const statsValid = typeof permissionStats.requested === "number" && typeof permissionStats.approved === "number" && typeof permissionStats.denied === "number" && typeof permissionStats.cancelled === "number";
  if (!statsValid) {
    return null;
  }
  const recordValid = typeof record.acpxRecordId === "string" && typeof record.acpSessionId === "string" && typeof record.agentCommand === "string" && typeof record.cwd === "string" && typeof record.createdAt === "string" && typeof record.lastUsedAt === "string" && Array.isArray(record.messages) && typeof record.updated_at === "string" && typeof record.lastSeq === "number" && Number.isInteger(record.lastSeq) && !!record.eventLog && typeof record.eventLog === "object";
  if (!recordValid) {
    return null;
  }
  return result;
}
function parseQueueOwnerMessage(raw) {
  const message = asRecord5(raw);
  if (!message || typeof message.type !== "string") {
    return null;
  }
  if (typeof message.requestId !== "string") {
    return null;
  }
  if (message.type === "accepted") {
    return {
      type: "accepted",
      requestId: message.requestId
    };
  }
  if (message.type === "event") {
    if (!isAcpxEvent(message.event)) {
      return null;
    }
    return {
      type: "event",
      requestId: message.requestId,
      event: message.event
    };
  }
  if (message.type === "result") {
    const parsedResult = parseSessionSendResult(message.result);
    if (!parsedResult) {
      return null;
    }
    return {
      type: "result",
      requestId: message.requestId,
      result: parsedResult
    };
  }
  if (message.type === "cancel_result") {
    if (typeof message.cancelled !== "boolean") {
      return null;
    }
    return {
      type: "cancel_result",
      requestId: message.requestId,
      cancelled: message.cancelled
    };
  }
  if (message.type === "set_mode_result") {
    if (typeof message.modeId !== "string") {
      return null;
    }
    return {
      type: "set_mode_result",
      requestId: message.requestId,
      modeId: message.modeId
    };
  }
  if (message.type === "set_config_option_result") {
    const response = asRecord5(message.response);
    if (!response || !Array.isArray(response.configOptions)) {
      return null;
    }
    return {
      type: "set_config_option_result",
      requestId: message.requestId,
      response
    };
  }
  if (message.type === "error") {
    if (typeof message.message !== "string" || !isOutputErrorCode3(message.code) || !isOutputErrorOrigin3(message.origin)) {
      return null;
    }
    const detailCode = typeof message.detailCode === "string" && message.detailCode.trim().length > 0 ? message.detailCode : void 0;
    const retryable = typeof message.retryable === "boolean" ? message.retryable : void 0;
    const acp = parseAcpError(message.acp);
    const outputAlreadyEmitted = typeof message.outputAlreadyEmitted === "boolean" ? message.outputAlreadyEmitted : void 0;
    return {
      type: "error",
      requestId: message.requestId,
      code: message.code,
      detailCode,
      origin: message.origin,
      message: message.message,
      retryable,
      acp,
      ...outputAlreadyEmitted === void 0 ? {} : { outputAlreadyEmitted }
    };
  }
  return null;
}

// src/queue-ipc.ts
var PROCESS_EXIT_GRACE_MS = 1500;
var PROCESS_POLL_MS = 50;
var QUEUE_CONNECT_ATTEMPTS = 40;
var QUEUE_CONNECT_RETRY_MS = 50;
function queueBaseDir() {
  return path3.join(os3.homedir(), ".acpx", "queues");
}
function makeQueueOwnerError(requestId, message, detailCode, options = {}) {
  return {
    type: "error",
    requestId,
    code: "RUNTIME",
    detailCode,
    origin: "queue",
    retryable: options.retryable,
    message
  };
}
function makeQueueOwnerErrorFromUnknown(requestId, error, detailCode, options = {}) {
  const normalized = normalizeOutputError(error, {
    defaultCode: "RUNTIME",
    origin: "queue",
    detailCode,
    retryable: options.retryable
  });
  return {
    type: "error",
    requestId,
    code: normalized.code,
    detailCode: normalized.detailCode,
    origin: normalized.origin,
    message: normalized.message,
    retryable: normalized.retryable,
    acp: normalized.acp
  };
}
var STALE_OWNER_PROTOCOL_DETAIL_CODES = /* @__PURE__ */ new Set([
  "QUEUE_PROTOCOL_MALFORMED_MESSAGE",
  "QUEUE_PROTOCOL_UNEXPECTED_RESPONSE"
]);
async function maybeRecoverStaleOwnerAfterProtocolMismatch(params) {
  if (!(params.error instanceof QueueProtocolError)) {
    return false;
  }
  const detailCode = params.error.detailCode;
  if (!detailCode || !STALE_OWNER_PROTOCOL_DETAIL_CODES.has(detailCode)) {
    return false;
  }
  await cleanupStaleQueueOwner(params.sessionId, params.owner).catch(() => {
  });
  if (params.verbose) {
    process.stderr.write(
      `[acpx] dropped stale queue owner metadata after protocol mismatch for session ${params.sessionId} (${detailCode})
`
    );
  }
  return true;
}
function isProcessAlive(pid) {
  if (!pid || !Number.isInteger(pid) || pid <= 0 || pid === process.pid) {
    return false;
  }
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}
async function waitForProcessExit(pid, timeoutMs) {
  const deadline = Date.now() + Math.max(0, timeoutMs);
  while (Date.now() <= deadline) {
    if (!isProcessAlive(pid)) {
      return true;
    }
    await new Promise((resolve) => {
      setTimeout(resolve, PROCESS_POLL_MS);
    });
  }
  return !isProcessAlive(pid);
}
async function terminateProcess(pid) {
  if (!isProcessAlive(pid)) {
    return false;
  }
  try {
    process.kill(pid, "SIGTERM");
  } catch {
    return false;
  }
  if (await waitForProcessExit(pid, PROCESS_EXIT_GRACE_MS)) {
    return true;
  }
  try {
    process.kill(pid, "SIGKILL");
  } catch {
    return false;
  }
  await waitForProcessExit(pid, PROCESS_EXIT_GRACE_MS);
  return true;
}
function parseQueueOwnerRecord(raw) {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    return null;
  }
  const record = raw;
  if (!Number.isInteger(record.pid) || record.pid <= 0 || typeof record.sessionId !== "string" || typeof record.socketPath !== "string") {
    return null;
  }
  return {
    pid: record.pid,
    sessionId: record.sessionId,
    socketPath: record.socketPath
  };
}
function queueKeyForSession(sessionId) {
  return createHash("sha256").update(sessionId).digest("hex").slice(0, 24);
}
function queueLockFilePath(sessionId) {
  return path3.join(queueBaseDir(), `${queueKeyForSession(sessionId)}.lock`);
}
function queueSocketPath(sessionId) {
  const key = queueKeyForSession(sessionId);
  if (process.platform === "win32") {
    return `\\\\.\\pipe\\acpx-${key}`;
  }
  return path3.join(queueBaseDir(), `${key}.sock`);
}
async function ensureQueueDir() {
  await fs3.mkdir(queueBaseDir(), { recursive: true });
}
async function removeSocketFile(socketPath) {
  if (process.platform === "win32") {
    return;
  }
  try {
    await fs3.unlink(socketPath);
  } catch (error) {
    if (error.code !== "ENOENT") {
      throw error;
    }
  }
}
async function readQueueOwnerRecord(sessionId) {
  const lockPath = queueLockFilePath(sessionId);
  try {
    const payload = await fs3.readFile(lockPath, "utf8");
    const parsed = parseQueueOwnerRecord(JSON.parse(payload));
    return parsed ?? void 0;
  } catch {
    return void 0;
  }
}
async function cleanupStaleQueueOwner(sessionId, owner) {
  const lockPath = queueLockFilePath(sessionId);
  const socketPath = owner?.socketPath ?? queueSocketPath(sessionId);
  await removeSocketFile(socketPath).catch(() => {
  });
  await fs3.unlink(lockPath).catch((error) => {
    if (error.code !== "ENOENT") {
      throw error;
    }
  });
}
async function tryAcquireQueueOwnerLease(sessionId, nowIso = () => (/* @__PURE__ */ new Date()).toISOString()) {
  await ensureQueueDir();
  const lockPath = queueLockFilePath(sessionId);
  const socketPath = queueSocketPath(sessionId);
  const payload = JSON.stringify(
    {
      pid: process.pid,
      sessionId,
      socketPath,
      createdAt: nowIso()
    },
    null,
    2
  );
  try {
    await fs3.writeFile(lockPath, `${payload}
`, {
      encoding: "utf8",
      flag: "wx"
    });
    await removeSocketFile(socketPath).catch(() => {
    });
    return { lockPath, socketPath };
  } catch (error) {
    if (error.code !== "EEXIST") {
      throw error;
    }
    const owner = await readQueueOwnerRecord(sessionId);
    if (!owner || !isProcessAlive(owner.pid)) {
      await cleanupStaleQueueOwner(sessionId, owner);
    }
    return void 0;
  }
}
async function releaseQueueOwnerLease(lease) {
  await removeSocketFile(lease.socketPath).catch(() => {
  });
  await fs3.unlink(lease.lockPath).catch((error) => {
    if (error.code !== "ENOENT") {
      throw error;
    }
  });
}
function shouldRetryQueueConnect(error) {
  const code = error.code;
  return code === "ENOENT" || code === "ECONNREFUSED";
}
async function waitMs(ms) {
  await new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}
async function connectToSocket(socketPath) {
  return await new Promise((resolve, reject) => {
    const socket = net.createConnection(socketPath);
    const onConnect = () => {
      socket.off("error", onError);
      resolve(socket);
    };
    const onError = (error) => {
      socket.off("connect", onConnect);
      reject(error);
    };
    socket.once("connect", onConnect);
    socket.once("error", onError);
  });
}
async function connectToQueueOwner(owner, maxAttempts = QUEUE_CONNECT_ATTEMPTS) {
  let lastError;
  const attempts = Math.max(1, Math.trunc(maxAttempts));
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    try {
      return await connectToSocket(owner.socketPath);
    } catch (error) {
      lastError = error;
      if (!shouldRetryQueueConnect(error)) {
        throw error;
      }
      await waitMs(QUEUE_CONNECT_RETRY_MS);
    }
  }
  if (lastError && !shouldRetryQueueConnect(lastError)) {
    throw lastError;
  }
  return void 0;
}
async function probeQueueOwnerHealth(sessionId) {
  const owner = await readQueueOwnerRecord(sessionId);
  if (!owner) {
    return {
      sessionId,
      hasLease: false,
      healthy: false,
      socketReachable: false,
      pidAlive: false
    };
  }
  const pidAlive = isProcessAlive(owner.pid);
  let socketReachable = false;
  try {
    const socket = await connectToQueueOwner(owner, 2);
    if (socket) {
      socketReachable = true;
      if (!socket.destroyed) {
        socket.end();
      }
    }
  } catch {
    socketReachable = false;
  }
  if (!socketReachable && !pidAlive) {
    await cleanupStaleQueueOwner(sessionId, owner);
    return {
      sessionId,
      hasLease: false,
      healthy: false,
      socketReachable: false,
      pidAlive: false
    };
  }
  return {
    sessionId,
    hasLease: true,
    healthy: socketReachable,
    socketReachable,
    pidAlive,
    pid: owner.pid,
    socketPath: owner.socketPath
  };
}
function writeQueueMessage(socket, message) {
  if (socket.destroyed || !socket.writable) {
    return;
  }
  socket.write(`${JSON.stringify(message)}
`);
}
var SessionQueueOwner = class _SessionQueueOwner {
  server;
  controlHandlers;
  pending = [];
  waiters = [];
  closed = false;
  constructor(server, controlHandlers) {
    this.server = server;
    this.controlHandlers = controlHandlers;
  }
  static async start(lease, controlHandlers) {
    const ownerRef = { current: void 0 };
    const server = net.createServer((socket) => {
      ownerRef.current?.handleConnection(socket);
    });
    ownerRef.current = new _SessionQueueOwner(server, controlHandlers);
    await new Promise((resolve, reject) => {
      const onListening = () => {
        server.off("error", onError);
        resolve();
      };
      const onError = (error) => {
        server.off("listening", onListening);
        reject(error);
      };
      server.once("listening", onListening);
      server.once("error", onError);
      server.listen(lease.socketPath);
    });
    return ownerRef.current;
  }
  async close() {
    if (this.closed) {
      return;
    }
    this.closed = true;
    for (const waiter of this.waiters.splice(0)) {
      waiter(void 0);
    }
    for (const task of this.pending.splice(0)) {
      if (task.waitForCompletion) {
        task.send(
          makeQueueOwnerError(
            task.requestId,
            "Queue owner shutting down before prompt execution",
            "QUEUE_OWNER_SHUTTING_DOWN",
            {
              retryable: true
            }
          )
        );
      }
      task.close();
    }
    await new Promise((resolve) => {
      this.server.close(() => resolve());
    });
  }
  async nextTask(timeoutMs) {
    if (this.pending.length > 0) {
      return this.pending.shift();
    }
    if (this.closed) {
      return void 0;
    }
    return await new Promise((resolve) => {
      const shouldTimeout = timeoutMs != null;
      const timer = shouldTimeout && setTimeout(
        () => {
          const index = this.waiters.indexOf(waiter);
          if (index >= 0) {
            this.waiters.splice(index, 1);
          }
          resolve(void 0);
        },
        Math.max(0, timeoutMs)
      );
      const waiter = (task) => {
        if (timer) {
          clearTimeout(timer);
        }
        resolve(task);
      };
      this.waiters.push(waiter);
    });
  }
  enqueue(task) {
    if (this.closed) {
      if (task.waitForCompletion) {
        task.send(
          makeQueueOwnerError(
            task.requestId,
            "Queue owner is shutting down",
            "QUEUE_OWNER_SHUTTING_DOWN",
            {
              retryable: true
            }
          )
        );
      }
      task.close();
      return;
    }
    const waiter = this.waiters.shift();
    if (waiter) {
      waiter(task);
      return;
    }
    this.pending.push(task);
  }
  handleConnection(socket) {
    socket.setEncoding("utf8");
    if (this.closed) {
      writeQueueMessage(
        socket,
        makeQueueOwnerError("unknown", "Queue owner is closed", "QUEUE_OWNER_CLOSED", {
          retryable: true
        })
      );
      socket.end();
      return;
    }
    let buffer = "";
    let handled = false;
    const fail = (requestId, message, detailCode) => {
      writeQueueMessage(
        socket,
        makeQueueOwnerError(requestId, message, detailCode, {
          retryable: false
        })
      );
      socket.end();
    };
    const processLine = (line) => {
      if (handled) {
        return;
      }
      handled = true;
      let parsed;
      try {
        parsed = JSON.parse(line);
      } catch {
        fail(
          "unknown",
          "Invalid queue request payload",
          "QUEUE_REQUEST_PAYLOAD_INVALID_JSON"
        );
        return;
      }
      const request = parseQueueRequest(parsed);
      if (!request) {
        fail("unknown", "Invalid queue request", "QUEUE_REQUEST_INVALID");
        return;
      }
      if (request.type === "cancel_prompt") {
        writeQueueMessage(socket, {
          type: "accepted",
          requestId: request.requestId
        });
        void this.controlHandlers.cancelPrompt().then((cancelled) => {
          writeQueueMessage(socket, {
            type: "cancel_result",
            requestId: request.requestId,
            cancelled
          });
        }).catch((error) => {
          writeQueueMessage(
            socket,
            makeQueueOwnerErrorFromUnknown(
              request.requestId,
              error,
              "QUEUE_CONTROL_REQUEST_FAILED"
            )
          );
        }).finally(() => {
          if (!socket.destroyed) {
            socket.end();
          }
        });
        return;
      }
      if (request.type === "set_mode") {
        writeQueueMessage(socket, {
          type: "accepted",
          requestId: request.requestId
        });
        void this.controlHandlers.setSessionMode(request.modeId, request.timeoutMs).then(() => {
          writeQueueMessage(socket, {
            type: "set_mode_result",
            requestId: request.requestId,
            modeId: request.modeId
          });
        }).catch((error) => {
          writeQueueMessage(
            socket,
            makeQueueOwnerErrorFromUnknown(
              request.requestId,
              error,
              "QUEUE_CONTROL_REQUEST_FAILED"
            )
          );
        }).finally(() => {
          if (!socket.destroyed) {
            socket.end();
          }
        });
        return;
      }
      if (request.type === "set_config_option") {
        writeQueueMessage(socket, {
          type: "accepted",
          requestId: request.requestId
        });
        void this.controlHandlers.setSessionConfigOption(request.configId, request.value, request.timeoutMs).then((response) => {
          writeQueueMessage(socket, {
            type: "set_config_option_result",
            requestId: request.requestId,
            response
          });
        }).catch((error) => {
          writeQueueMessage(
            socket,
            makeQueueOwnerErrorFromUnknown(
              request.requestId,
              error,
              "QUEUE_CONTROL_REQUEST_FAILED"
            )
          );
        }).finally(() => {
          if (!socket.destroyed) {
            socket.end();
          }
        });
        return;
      }
      const task = {
        requestId: request.requestId,
        message: request.message,
        permissionMode: request.permissionMode,
        nonInteractivePermissions: request.nonInteractivePermissions,
        timeoutMs: request.timeoutMs,
        suppressSdkConsoleErrors: request.suppressSdkConsoleErrors,
        waitForCompletion: request.waitForCompletion,
        send: (message) => {
          writeQueueMessage(socket, message);
        },
        close: () => {
          if (!socket.destroyed) {
            socket.end();
          }
        }
      };
      writeQueueMessage(socket, {
        type: "accepted",
        requestId: request.requestId
      });
      if (!request.waitForCompletion) {
        task.close();
      }
      this.enqueue(task);
    };
    socket.on("data", (chunk) => {
      buffer += chunk;
      let index = buffer.indexOf("\n");
      while (index >= 0) {
        const line = buffer.slice(0, index).trim();
        buffer = buffer.slice(index + 1);
        if (line.length > 0) {
          processLine(line);
        }
        index = buffer.indexOf("\n");
      }
    });
    socket.on("error", () => {
    });
  }
};
async function submitToQueueOwner(owner, options) {
  const socket = await connectToQueueOwner(owner);
  if (!socket) {
    return void 0;
  }
  socket.setEncoding("utf8");
  const requestId = randomUUID3();
  const request = {
    type: "submit_prompt",
    requestId,
    message: options.message,
    permissionMode: options.permissionMode,
    nonInteractivePermissions: options.nonInteractivePermissions,
    timeoutMs: options.timeoutMs,
    suppressSdkConsoleErrors: options.suppressSdkConsoleErrors,
    waitForCompletion: options.waitForCompletion
  };
  options.outputFormatter.setContext({
    sessionId: options.sessionId,
    requestId
  });
  return await new Promise((resolve, reject) => {
    let settled = false;
    let acknowledged = false;
    let buffer = "";
    const finishResolve = (result) => {
      if (settled) {
        return;
      }
      settled = true;
      socket.removeAllListeners();
      if (!socket.destroyed) {
        socket.end();
      }
      resolve(result);
    };
    const finishReject = (error) => {
      if (settled) {
        return;
      }
      settled = true;
      socket.removeAllListeners();
      if (!socket.destroyed) {
        socket.destroy();
      }
      reject(error);
    };
    const processLine = (line) => {
      let parsed;
      try {
        parsed = JSON.parse(line);
      } catch {
        finishReject(
          new QueueProtocolError("Queue owner sent invalid JSON payload", {
            detailCode: "QUEUE_PROTOCOL_INVALID_JSON",
            origin: "queue",
            retryable: true
          })
        );
        return;
      }
      const message = parseQueueOwnerMessage(parsed);
      if (!message || message.requestId !== requestId) {
        finishReject(
          new QueueProtocolError("Queue owner sent malformed message", {
            detailCode: "QUEUE_PROTOCOL_MALFORMED_MESSAGE",
            origin: "queue",
            retryable: true
          })
        );
        return;
      }
      if (message.type === "accepted") {
        acknowledged = true;
        options.outputFormatter.setContext({
          sessionId: options.sessionId,
          requestId: message.requestId
        });
        if (!options.waitForCompletion) {
          const queued = {
            queued: true,
            sessionId: options.sessionId,
            requestId
          };
          finishResolve(queued);
        }
        return;
      }
      if (message.type === "error") {
        options.outputFormatter.setContext({
          sessionId: options.sessionId,
          requestId: message.requestId
        });
        const queueErrorAlreadyEmitted = options.errorEmissionPolicy?.queueErrorAlreadyEmitted ?? true;
        const outputAlreadyEmitted = message.outputAlreadyEmitted === true;
        const shouldEmitInFormatter = !outputAlreadyEmitted || !queueErrorAlreadyEmitted;
        if (shouldEmitInFormatter) {
          options.outputFormatter.onError({
            code: message.code ?? "RUNTIME",
            detailCode: message.detailCode,
            origin: message.origin ?? "queue",
            message: message.message,
            retryable: message.retryable,
            acp: message.acp
          });
          options.outputFormatter.flush();
        }
        finishReject(
          new QueueConnectionError(message.message, {
            outputCode: message.code,
            detailCode: message.detailCode,
            origin: message.origin ?? "queue",
            retryable: message.retryable,
            acp: message.acp,
            ...queueErrorAlreadyEmitted ? { outputAlreadyEmitted: true } : {}
          })
        );
        return;
      }
      if (!acknowledged) {
        finishReject(
          new QueueConnectionError("Queue owner did not acknowledge request", {
            detailCode: "QUEUE_ACK_MISSING",
            origin: "queue",
            retryable: true
          })
        );
        return;
      }
      if (message.type === "event") {
        options.outputFormatter.onEvent(message.event);
        return;
      }
      if (message.type === "result") {
        options.outputFormatter.flush();
        finishResolve(message.result);
        return;
      }
      finishReject(
        new QueueProtocolError("Queue owner returned unexpected response", {
          detailCode: "QUEUE_PROTOCOL_UNEXPECTED_RESPONSE",
          origin: "queue",
          retryable: true
        })
      );
    };
    socket.on("data", (chunk) => {
      buffer += chunk;
      let index = buffer.indexOf("\n");
      while (index >= 0) {
        const line = buffer.slice(0, index).trim();
        buffer = buffer.slice(index + 1);
        if (line.length > 0) {
          processLine(line);
        }
        index = buffer.indexOf("\n");
      }
    });
    socket.once("error", (error) => {
      finishReject(error);
    });
    socket.once("close", () => {
      if (settled) {
        return;
      }
      if (!acknowledged) {
        finishReject(
          new QueueConnectionError(
            "Queue owner disconnected before acknowledging request",
            {
              detailCode: "QUEUE_DISCONNECTED_BEFORE_ACK",
              origin: "queue",
              retryable: true
            }
          )
        );
        return;
      }
      if (!options.waitForCompletion) {
        const queued = {
          queued: true,
          sessionId: options.sessionId,
          requestId
        };
        finishResolve(queued);
        return;
      }
      finishReject(
        new QueueConnectionError("Queue owner disconnected before prompt completion", {
          detailCode: "QUEUE_DISCONNECTED_BEFORE_COMPLETION",
          origin: "queue",
          retryable: true
        })
      );
    });
    socket.write(`${JSON.stringify(request)}
`);
  });
}
async function submitControlToQueueOwner(owner, request, isExpectedResponse) {
  const socket = await connectToQueueOwner(owner);
  if (!socket) {
    return void 0;
  }
  socket.setEncoding("utf8");
  return await new Promise((resolve, reject) => {
    let settled = false;
    let acknowledged = false;
    let buffer = "";
    const finishResolve = (result) => {
      if (settled) {
        return;
      }
      settled = true;
      socket.removeAllListeners();
      if (!socket.destroyed) {
        socket.end();
      }
      resolve(result);
    };
    const finishReject = (error) => {
      if (settled) {
        return;
      }
      settled = true;
      socket.removeAllListeners();
      if (!socket.destroyed) {
        socket.destroy();
      }
      reject(error);
    };
    const processLine = (line) => {
      let parsed;
      try {
        parsed = JSON.parse(line);
      } catch {
        finishReject(
          new QueueProtocolError("Queue owner sent invalid JSON payload", {
            detailCode: "QUEUE_PROTOCOL_INVALID_JSON",
            origin: "queue",
            retryable: true
          })
        );
        return;
      }
      const message = parseQueueOwnerMessage(parsed);
      if (!message || message.requestId !== request.requestId) {
        finishReject(
          new QueueProtocolError("Queue owner sent malformed message", {
            detailCode: "QUEUE_PROTOCOL_MALFORMED_MESSAGE",
            origin: "queue",
            retryable: true
          })
        );
        return;
      }
      if (message.type === "accepted") {
        acknowledged = true;
        return;
      }
      if (message.type === "error") {
        finishReject(
          new QueueConnectionError(message.message, {
            outputCode: message.code,
            detailCode: message.detailCode,
            origin: message.origin ?? "queue",
            retryable: message.retryable,
            acp: message.acp
          })
        );
        return;
      }
      if (!acknowledged) {
        finishReject(
          new QueueConnectionError("Queue owner did not acknowledge request", {
            detailCode: "QUEUE_ACK_MISSING",
            origin: "queue",
            retryable: true
          })
        );
        return;
      }
      if (!isExpectedResponse(message)) {
        finishReject(
          new QueueProtocolError("Queue owner returned unexpected response", {
            detailCode: "QUEUE_PROTOCOL_UNEXPECTED_RESPONSE",
            origin: "queue",
            retryable: true
          })
        );
        return;
      }
      finishResolve(message);
    };
    socket.on("data", (chunk) => {
      buffer += chunk;
      let index = buffer.indexOf("\n");
      while (index >= 0) {
        const line = buffer.slice(0, index).trim();
        buffer = buffer.slice(index + 1);
        if (line.length > 0) {
          processLine(line);
        }
        index = buffer.indexOf("\n");
      }
    });
    socket.once("error", (error) => {
      finishReject(error);
    });
    socket.once("close", () => {
      if (settled) {
        return;
      }
      if (!acknowledged) {
        finishReject(
          new QueueConnectionError(
            "Queue owner disconnected before acknowledging request",
            {
              detailCode: "QUEUE_DISCONNECTED_BEFORE_ACK",
              origin: "queue",
              retryable: true
            }
          )
        );
        return;
      }
      finishReject(
        new QueueConnectionError("Queue owner disconnected before responding", {
          detailCode: "QUEUE_DISCONNECTED_BEFORE_COMPLETION",
          origin: "queue",
          retryable: true
        })
      );
    });
    socket.write(`${JSON.stringify(request)}
`);
  });
}
async function submitCancelToQueueOwner(owner) {
  const request = {
    type: "cancel_prompt",
    requestId: randomUUID3()
  };
  const response = await submitControlToQueueOwner(
    owner,
    request,
    (message) => message.type === "cancel_result"
  );
  if (!response) {
    return void 0;
  }
  if (response.requestId !== request.requestId) {
    throw new QueueProtocolError("Queue owner returned mismatched cancel response", {
      detailCode: "QUEUE_PROTOCOL_MALFORMED_MESSAGE",
      origin: "queue",
      retryable: true
    });
  }
  return response.cancelled;
}
async function submitSetModeToQueueOwner(owner, modeId, timeoutMs) {
  const request = {
    type: "set_mode",
    requestId: randomUUID3(),
    modeId,
    timeoutMs
  };
  const response = await submitControlToQueueOwner(
    owner,
    request,
    (message) => message.type === "set_mode_result"
  );
  if (!response) {
    return void 0;
  }
  if (response.requestId !== request.requestId) {
    throw new QueueProtocolError("Queue owner returned mismatched set_mode response", {
      detailCode: "QUEUE_PROTOCOL_MALFORMED_MESSAGE",
      origin: "queue",
      retryable: true
    });
  }
  return true;
}
async function submitSetConfigOptionToQueueOwner(owner, configId, value, timeoutMs) {
  const request = {
    type: "set_config_option",
    requestId: randomUUID3(),
    configId,
    value,
    timeoutMs
  };
  const response = await submitControlToQueueOwner(
    owner,
    request,
    (message) => message.type === "set_config_option_result"
  );
  if (!response) {
    return void 0;
  }
  if (response.requestId !== request.requestId) {
    throw new QueueProtocolError(
      "Queue owner returned mismatched set_config_option response",
      {
        detailCode: "QUEUE_PROTOCOL_MALFORMED_MESSAGE",
        origin: "queue",
        retryable: true
      }
    );
  }
  return response.response;
}
async function trySubmitToRunningOwner(options) {
  const owner = await readQueueOwnerRecord(options.sessionId);
  if (!owner) {
    return void 0;
  }
  let submitted;
  try {
    submitted = await submitToQueueOwner(owner, options);
  } catch (error) {
    const recovered = await maybeRecoverStaleOwnerAfterProtocolMismatch({
      sessionId: options.sessionId,
      owner,
      error,
      verbose: options.verbose
    });
    if (recovered) {
      return void 0;
    }
    throw error;
  }
  if (submitted) {
    if (options.verbose) {
      process.stderr.write(
        `[acpx] queued prompt on active owner pid ${owner.pid} for session ${options.sessionId}
`
      );
    }
    return submitted;
  }
  const health = await probeQueueOwnerHealth(options.sessionId);
  if (!health.hasLease) {
    return void 0;
  }
  throw new QueueConnectionError(
    "Session queue owner is running but not accepting queue requests",
    {
      detailCode: "QUEUE_NOT_ACCEPTING_REQUESTS",
      origin: "queue",
      retryable: true
    }
  );
}
async function tryCancelOnRunningOwner(options) {
  const owner = await readQueueOwnerRecord(options.sessionId);
  if (!owner) {
    return void 0;
  }
  const cancelled = await submitCancelToQueueOwner(owner);
  if (cancelled !== void 0) {
    if (options.verbose) {
      process.stderr.write(
        `[acpx] requested cancel on active owner pid ${owner.pid} for session ${options.sessionId}
`
      );
    }
    return cancelled;
  }
  const health = await probeQueueOwnerHealth(options.sessionId);
  if (!health.hasLease) {
    return void 0;
  }
  throw new QueueConnectionError(
    "Session queue owner is running but not accepting cancel requests",
    {
      detailCode: "QUEUE_NOT_ACCEPTING_REQUESTS",
      origin: "queue",
      retryable: true
    }
  );
}
async function trySetModeOnRunningOwner(sessionId, modeId, timeoutMs, verbose) {
  const owner = await readQueueOwnerRecord(sessionId);
  if (!owner) {
    return void 0;
  }
  const submitted = await submitSetModeToQueueOwner(owner, modeId, timeoutMs);
  if (submitted) {
    if (verbose) {
      process.stderr.write(
        `[acpx] requested session/set_mode on owner pid ${owner.pid} for session ${sessionId}
`
      );
    }
    return true;
  }
  const health = await probeQueueOwnerHealth(sessionId);
  if (!health.hasLease) {
    return void 0;
  }
  throw new QueueConnectionError(
    "Session queue owner is running but not accepting set_mode requests",
    {
      detailCode: "QUEUE_NOT_ACCEPTING_REQUESTS",
      origin: "queue",
      retryable: true
    }
  );
}
async function trySetConfigOptionOnRunningOwner(sessionId, configId, value, timeoutMs, verbose) {
  const owner = await readQueueOwnerRecord(sessionId);
  if (!owner) {
    return void 0;
  }
  const response = await submitSetConfigOptionToQueueOwner(
    owner,
    configId,
    value,
    timeoutMs
  );
  if (response) {
    if (verbose) {
      process.stderr.write(
        `[acpx] requested session/set_config_option on owner pid ${owner.pid} for session ${sessionId}
`
      );
    }
    return response;
  }
  const health = await probeQueueOwnerHealth(sessionId);
  if (!health.hasLease) {
    return void 0;
  }
  throw new QueueConnectionError(
    "Session queue owner is running but not accepting set_config_option requests",
    {
      detailCode: "QUEUE_NOT_ACCEPTING_REQUESTS",
      origin: "queue",
      retryable: true
    }
  );
}
async function terminateQueueOwnerForSession(sessionId) {
  const owner = await readQueueOwnerRecord(sessionId);
  if (!owner) {
    return;
  }
  if (isProcessAlive(owner.pid)) {
    await terminateProcess(owner.pid);
  }
  await cleanupStaleQueueOwner(sessionId, owner);
}

// src/session-runtime/lifecycle.ts
function applyLifecycleSnapshotToRecord(record, snapshot) {
  record.pid = snapshot.pid;
  record.agentStartedAt = snapshot.startedAt;
  if (snapshot.lastExit) {
    record.lastAgentExitCode = snapshot.lastExit.exitCode;
    record.lastAgentExitSignal = snapshot.lastExit.signal;
    record.lastAgentExitAt = snapshot.lastExit.exitedAt;
    record.lastAgentDisconnectReason = snapshot.lastExit.reason;
    return;
  }
  record.lastAgentExitCode = void 0;
  record.lastAgentExitSignal = void 0;
  record.lastAgentExitAt = void 0;
  record.lastAgentDisconnectReason = void 0;
}
function reconcileAgentSessionId(record, agentSessionId) {
  const normalized = normalizeRuntimeSessionId(agentSessionId);
  if (!normalized) {
    return;
  }
  record.agentSessionId = normalized;
}
function sessionHasAgentMessages(record) {
  return record.messages.some(
    (message) => typeof message === "object" && message !== null && "Agent" in message
  );
}
function applyConversation(record, conversation) {
  record.title = conversation.title;
  record.messages = conversation.messages;
  record.updated_at = conversation.updated_at;
  record.cumulative_token_usage = conversation.cumulative_token_usage;
  record.request_token_usage = conversation.request_token_usage;
}

// src/session-runtime/connect-load.ts
function shouldFallbackToNewSession(error, record) {
  if (error instanceof TimeoutError || error instanceof InterruptedError) {
    return false;
  }
  if (isAcpResourceNotFoundError(error)) {
    return true;
  }
  if (!sessionHasAgentMessages(record)) {
    if (isAcpQueryClosedBeforeResponseError(error)) {
      return true;
    }
    const acp = extractAcpError(error);
    if (acp?.code === -32603) {
      return true;
    }
  }
  return false;
}
async function connectAndLoadSession(options) {
  const record = options.record;
  const client = options.client;
  const storedProcessAlive = isProcessAlive(record.pid);
  const shouldReconnect = Boolean(record.pid) && !storedProcessAlive;
  if (options.verbose) {
    if (storedProcessAlive) {
      process.stderr.write(
        `[acpx] saved session pid ${record.pid} is running; reconnecting with loadSession
`
      );
    } else if (shouldReconnect) {
      process.stderr.write(
        `[acpx] saved session pid ${record.pid} is dead; respawning agent and attempting session/load
`
      );
    }
  }
  await withTimeout(client.start(), options.timeoutMs);
  options.onClientAvailable?.(options.activeController);
  applyLifecycleSnapshotToRecord(record, client.getAgentLifecycleSnapshot());
  record.closed = false;
  record.closedAt = void 0;
  options.onConnectedRecord?.(record);
  await writeSessionRecord(record);
  let resumed = false;
  let loadError;
  let sessionId = record.acpSessionId;
  if (client.supportsLoadSession()) {
    try {
      const loadResult = await withTimeout(
        client.loadSessionWithOptions(record.acpSessionId, record.cwd, {
          suppressReplayUpdates: true
        }),
        options.timeoutMs
      );
      reconcileAgentSessionId(record, loadResult.agentSessionId);
      resumed = true;
    } catch (error) {
      loadError = formatErrorMessage(error);
      if (!shouldFallbackToNewSession(error, record)) {
        throw error;
      }
      const createdSession = await withTimeout(
        client.createSession(record.cwd),
        options.timeoutMs
      );
      sessionId = createdSession.sessionId;
      record.acpSessionId = sessionId;
      reconcileAgentSessionId(record, createdSession.agentSessionId);
    }
  } else {
    const createdSession = await withTimeout(
      client.createSession(record.cwd),
      options.timeoutMs
    );
    sessionId = createdSession.sessionId;
    record.acpSessionId = sessionId;
    reconcileAgentSessionId(record, createdSession.agentSessionId);
  }
  options.onSessionIdResolved?.(sessionId);
  return {
    sessionId,
    agentSessionId: record.agentSessionId,
    resumed,
    loadError
  };
}

// src/session-runtime/prompt-runner.ts
async function withConnectedSession(options) {
  const record = await resolveSessionRecord(options.sessionRecordId);
  const client = new AcpClient({
    agentCommand: record.agentCommand,
    cwd: absolutePath(record.cwd),
    permissionMode: options.permissionMode ?? "approve-reads",
    nonInteractivePermissions: options.nonInteractivePermissions,
    authCredentials: options.authCredentials,
    authPolicy: options.authPolicy,
    verbose: options.verbose
  });
  let activeSessionIdForControl = record.acpSessionId;
  let notifiedClientAvailable = false;
  const activeController = {
    hasActivePrompt: () => client.hasActivePrompt(),
    requestCancelActivePrompt: async () => await client.requestCancelActivePrompt(),
    setSessionMode: async (modeId) => {
      await client.setSessionMode(activeSessionIdForControl, modeId);
    },
    setSessionConfigOption: async (configId, value) => {
      return await client.setSessionConfigOption(
        activeSessionIdForControl,
        configId,
        value
      );
    }
  };
  try {
    return await withInterrupt(
      async () => {
        const {
          sessionId: activeSessionId,
          resumed,
          loadError
        } = await connectAndLoadSession({
          client,
          record,
          timeoutMs: options.timeoutMs,
          verbose: options.verbose,
          activeController,
          onClientAvailable: (controller) => {
            options.onClientAvailable?.(controller);
            notifiedClientAvailable = true;
          },
          onSessionIdResolved: (sessionId) => {
            activeSessionIdForControl = sessionId;
          }
        });
        const value = await options.run(client, activeSessionId, record);
        const now = isoNow3();
        record.lastUsedAt = now;
        record.closed = false;
        record.closedAt = void 0;
        record.protocolVersion = client.initializeResult?.protocolVersion;
        record.agentCapabilities = client.initializeResult?.agentCapabilities;
        applyLifecycleSnapshotToRecord(record, client.getAgentLifecycleSnapshot());
        await writeSessionRecord(record);
        return {
          value,
          record,
          resumed,
          loadError
        };
      },
      async () => {
        await client.cancelActivePrompt(2500);
        applyLifecycleSnapshotToRecord(record, client.getAgentLifecycleSnapshot());
        record.lastUsedAt = isoNow3();
        await writeSessionRecord(record).catch(() => {
        });
        await client.close();
      }
    );
  } finally {
    if (notifiedClientAvailable) {
      options.onClientClosed?.();
    }
    await client.close();
    applyLifecycleSnapshotToRecord(record, client.getAgentLifecycleSnapshot());
    await writeSessionRecord(record).catch(() => {
    });
  }
}
async function runSessionSetModeDirect(options) {
  const result = await withConnectedSession({
    sessionRecordId: options.sessionRecordId,
    nonInteractivePermissions: options.nonInteractivePermissions,
    authCredentials: options.authCredentials,
    authPolicy: options.authPolicy,
    timeoutMs: options.timeoutMs,
    verbose: options.verbose,
    onClientAvailable: options.onClientAvailable,
    onClientClosed: options.onClientClosed,
    run: async (client, sessionId) => {
      await withTimeout(
        client.setSessionMode(sessionId, options.modeId),
        options.timeoutMs
      );
    }
  });
  return {
    record: result.record,
    resumed: result.resumed,
    loadError: result.loadError
  };
}
async function runSessionSetConfigOptionDirect(options) {
  const result = await withConnectedSession({
    sessionRecordId: options.sessionRecordId,
    nonInteractivePermissions: options.nonInteractivePermissions,
    authCredentials: options.authCredentials,
    authPolicy: options.authPolicy,
    timeoutMs: options.timeoutMs,
    verbose: options.verbose,
    onClientAvailable: options.onClientAvailable,
    onClientClosed: options.onClientClosed,
    run: async (client, sessionId) => {
      return await withTimeout(
        client.setSessionConfigOption(sessionId, options.configId, options.value),
        options.timeoutMs
      );
    }
  });
  return {
    record: result.record,
    response: result.value,
    resumed: result.resumed,
    loadError: result.loadError
  };
}

// src/session-runtime.ts
var DEFAULT_QUEUE_OWNER_TTL_MS = 3e5;
var INTERRUPT_CANCEL_WAIT_MS = 2500;
var QUEUE_OWNER_STARTUP_MAX_ATTEMPTS = 120;
var QUEUE_OWNER_MAIN_PATH = fileURLToPath(
  new URL("./queue-owner-main.js", import.meta.url)
);
function toPromptResult(stopReason, sessionId, client) {
  return {
    stopReason,
    sessionId,
    permissionStats: client.getPermissionStats()
  };
}
var QueueTaskOutputFormatter = class {
  requestId;
  send;
  constructor(task) {
    this.requestId = task.requestId;
    this.send = task.send;
  }
  setContext() {
  }
  onEvent(event) {
    this.send({
      type: "event",
      requestId: this.requestId,
      event
    });
  }
  onSessionUpdate(_notification) {
  }
  onClientOperation(_operation) {
  }
  onDone(_stopReason) {
  }
  onError(params) {
    this.send({
      type: "error",
      requestId: this.requestId,
      code: params.code,
      detailCode: params.detailCode,
      origin: params.origin,
      message: params.message,
      retryable: params.retryable,
      acp: params.acp
    });
  }
  flush() {
  }
};
var DISCARD_OUTPUT_FORMATTER = {
  setContext() {
  },
  onEvent() {
  },
  onSessionUpdate() {
  },
  onClientOperation() {
  },
  onDone() {
  },
  onError() {
  },
  flush() {
  }
};
function normalizeQueueOwnerTtlMs(ttlMs) {
  if (ttlMs == null) {
    return DEFAULT_QUEUE_OWNER_TTL_MS;
  }
  if (!Number.isFinite(ttlMs) || ttlMs < 0) {
    return DEFAULT_QUEUE_OWNER_TTL_MS;
  }
  return Math.round(ttlMs);
}
async function runQueuedTask(sessionRecordId, task, options) {
  const outputFormatter = task.waitForCompletion ? new QueueTaskOutputFormatter(task) : DISCARD_OUTPUT_FORMATTER;
  try {
    const result = await runSessionPrompt({
      sessionRecordId,
      message: task.message,
      permissionMode: task.permissionMode,
      nonInteractivePermissions: task.nonInteractivePermissions ?? options.nonInteractivePermissions,
      authCredentials: options.authCredentials,
      authPolicy: options.authPolicy,
      outputFormatter,
      timeoutMs: task.timeoutMs,
      suppressSdkConsoleErrors: task.suppressSdkConsoleErrors ?? options.suppressSdkConsoleErrors,
      verbose: options.verbose,
      onClientAvailable: options.onClientAvailable,
      onClientClosed: options.onClientClosed,
      onPromptActive: options.onPromptActive
    });
    if (task.waitForCompletion) {
      task.send({
        type: "result",
        requestId: task.requestId,
        result
      });
    }
  } catch (error) {
    const normalizedError = normalizeOutputError(error, {
      origin: "runtime",
      detailCode: "QUEUE_RUNTIME_PROMPT_FAILED"
    });
    const alreadyEmitted = error.outputAlreadyEmitted === true;
    if (task.waitForCompletion) {
      task.send({
        type: "error",
        requestId: task.requestId,
        code: normalizedError.code,
        detailCode: normalizedError.detailCode,
        origin: normalizedError.origin,
        message: normalizedError.message,
        retryable: normalizedError.retryable,
        acp: normalizedError.acp,
        outputAlreadyEmitted: alreadyEmitted
      });
    }
    if (error instanceof InterruptedError) {
      throw error;
    }
  } finally {
    task.close();
  }
}
async function runSessionPrompt(options) {
  const output = options.outputFormatter;
  const record = await resolveSessionRecord(options.sessionRecordId);
  const conversation = cloneSessionConversation(record);
  let acpxState = cloneSessionAcpxState(record.acpx);
  recordPromptSubmission(conversation, options.message, isoNow3());
  output.setContext({
    sessionId: record.acpxRecordId,
    acpSessionId: record.acpSessionId,
    agentSessionId: record.agentSessionId,
    nextSeq: record.lastSeq + 1
  });
  const eventWriter = await SessionEventWriter.open(record);
  const pendingEvents = [];
  let eventWriterClosed = false;
  const closeEventWriter = async (checkpoint) => {
    if (eventWriterClosed) {
      return;
    }
    eventWriterClosed = true;
    await eventWriter.close({ checkpoint });
  };
  const flushPendingEvents = async (checkpoint = false) => {
    if (pendingEvents.length === 0) {
      return;
    }
    const batch = pendingEvents.splice(0, pendingEvents.length);
    await eventWriter.appendEvents(batch, { checkpoint });
  };
  const emitEvent = (draft) => {
    const event = eventWriter.createEvent(draft);
    if (!isAcpxEvent(event)) {
      throw new Error("Attempted to emit invalid acpx.event.v1 payload");
    }
    pendingEvents.push(event);
    output.onEvent(event);
    return event;
  };
  const client = new AcpClient({
    agentCommand: record.agentCommand,
    cwd: absolutePath(record.cwd),
    permissionMode: options.permissionMode,
    nonInteractivePermissions: options.nonInteractivePermissions,
    authCredentials: options.authCredentials,
    authPolicy: options.authPolicy,
    suppressSdkConsoleErrors: options.suppressSdkConsoleErrors,
    verbose: options.verbose,
    onSessionUpdate: (notification) => {
      acpxState = recordSessionUpdate(
        conversation,
        acpxState,
        notification
      );
      const drafts = sessionUpdateToEventDrafts(notification);
      for (const draft of drafts) {
        emitEvent(draft);
      }
    },
    onClientOperation: (operation) => {
      acpxState = recordClientOperation(conversation, acpxState, operation);
      emitEvent(clientOperationToEventDraft(operation));
    }
  });
  let activeSessionIdForControl = record.acpSessionId;
  let notifiedClientAvailable = false;
  const activeController = {
    hasActivePrompt: () => client.hasActivePrompt(),
    requestCancelActivePrompt: async () => await client.requestCancelActivePrompt(),
    setSessionMode: async (modeId) => {
      await client.setSessionMode(activeSessionIdForControl, modeId);
    },
    setSessionConfigOption: async (configId, value) => {
      return await client.setSessionConfigOption(
        activeSessionIdForControl,
        configId,
        value
      );
    }
  };
  try {
    return await withInterrupt(
      async () => {
        const {
          sessionId: activeSessionId,
          resumed,
          loadError
        } = await connectAndLoadSession({
          client,
          record,
          timeoutMs: options.timeoutMs,
          verbose: options.verbose,
          activeController,
          onClientAvailable: (controller) => {
            options.onClientAvailable?.(controller);
            notifiedClientAvailable = true;
          },
          onConnectedRecord: (connectedRecord) => {
            connectedRecord.lastPromptAt = isoNow3();
          },
          onSessionIdResolved: (sessionId) => {
            activeSessionIdForControl = sessionId;
          }
        });
        output.setContext({
          sessionId: record.acpxRecordId,
          acpSessionId: record.acpSessionId,
          agentSessionId: record.agentSessionId,
          nextSeq: record.lastSeq + 1
        });
        emitEvent({
          type: ACPX_EVENT_TYPES.TURN_STARTED,
          data: {
            mode: "prompt",
            resumed,
            input_preview: truncateInputPreview(options.message)
          }
        });
        await flushPendingEvents(false);
        let response;
        try {
          const promptPromise = client.prompt(activeSessionId, options.message);
          if (options.onPromptActive) {
            try {
              await options.onPromptActive();
            } catch (error) {
              if (options.verbose) {
                process.stderr.write(
                  "[acpx] onPromptActive hook failed: " + formatErrorMessage(error) + "\n"
                );
              }
            }
          }
          response = await withTimeout(promptPromise, options.timeoutMs);
        } catch (error) {
          const snapshot = client.getAgentLifecycleSnapshot();
          applyLifecycleSnapshotToRecord(record, snapshot);
          if (snapshot.lastExit?.unexpectedDuringPrompt && options.verbose) {
            process.stderr.write(
              "[acpx] agent disconnected during prompt (" + snapshot.lastExit.reason + ", exit=" + snapshot.lastExit.exitCode + ", signal=" + (snapshot.lastExit.signal ?? "none") + ")\n"
            );
          }
          const normalizedError = normalizeOutputError(error, {
            origin: "runtime"
          });
          emitEvent(
            errorToEventDraft({
              code: normalizedError.code,
              detailCode: normalizedError.detailCode,
              origin: normalizedError.origin,
              message: normalizedError.message,
              retryable: normalizedError.retryable,
              acp: normalizedError.acp
            })
          );
          await flushPendingEvents(true).catch(() => {
          });
          output.flush();
          record.lastUsedAt = isoNow3();
          applyConversation(record, conversation);
          record.acpx = acpxState;
          await writeSessionRecord(record).catch(() => {
          });
          const propagated = error instanceof Error ? error : new Error(formatErrorMessage(error));
          propagated.outputAlreadyEmitted = true;
          throw propagated;
        }
        emitEvent({
          type: ACPX_EVENT_TYPES.TURN_DONE,
          data: {
            stop_reason: response.stopReason,
            permission_stats: client.getPermissionStats()
          }
        });
        await flushPendingEvents(true);
        output.flush();
        const now = isoNow3();
        record.lastUsedAt = now;
        record.closed = false;
        record.closedAt = void 0;
        record.protocolVersion = client.initializeResult?.protocolVersion;
        record.agentCapabilities = client.initializeResult?.agentCapabilities;
        applyConversation(record, conversation);
        record.acpx = acpxState;
        applyLifecycleSnapshotToRecord(record, client.getAgentLifecycleSnapshot());
        await writeSessionRecord(record);
        return {
          ...toPromptResult(response.stopReason, record.acpxRecordId, client),
          record,
          resumed,
          loadError
        };
      },
      async () => {
        await client.cancelActivePrompt(INTERRUPT_CANCEL_WAIT_MS);
        applyLifecycleSnapshotToRecord(record, client.getAgentLifecycleSnapshot());
        record.lastUsedAt = isoNow3();
        applyConversation(record, conversation);
        record.acpx = acpxState;
        await flushPendingEvents(true).catch(() => {
        });
        await writeSessionRecord(record).catch(() => {
        });
        await closeEventWriter(false).catch(() => {
        });
        await client.close();
      }
    );
  } finally {
    if (notifiedClientAvailable) {
      options.onClientClosed?.();
    }
    await client.close();
    applyLifecycleSnapshotToRecord(record, client.getAgentLifecycleSnapshot());
    applyConversation(record, conversation);
    record.acpx = acpxState;
    await flushPendingEvents(false).catch(() => {
    });
    await writeSessionRecord(record).catch(() => {
    });
    await closeEventWriter(false).catch(() => {
    });
  }
}
async function runOnce(options) {
  const output = options.outputFormatter;
  const client = new AcpClient({
    agentCommand: options.agentCommand,
    cwd: absolutePath(options.cwd),
    permissionMode: options.permissionMode,
    nonInteractivePermissions: options.nonInteractivePermissions,
    authCredentials: options.authCredentials,
    authPolicy: options.authPolicy,
    suppressSdkConsoleErrors: options.suppressSdkConsoleErrors,
    verbose: options.verbose,
    onSessionUpdate: (notification) => output.onSessionUpdate(notification),
    onClientOperation: (operation) => output.onClientOperation(operation)
  });
  try {
    return await withInterrupt(
      async () => {
        await withTimeout(client.start(), options.timeoutMs);
        const createdSession = await withTimeout(
          client.createSession(absolutePath(options.cwd)),
          options.timeoutMs
        );
        const sessionId = createdSession.sessionId;
        const agentSessionId = normalizeRuntimeSessionId(createdSession.agentSessionId);
        output.setContext({
          sessionId,
          acpSessionId: sessionId,
          agentSessionId,
          nextSeq: 0
        });
        output.onEvent(
          createAcpxEvent(
            {
              sessionId,
              acpSessionId: sessionId,
              agentSessionId,
              seq: 0
            },
            {
              type: ACPX_EVENT_TYPES.TURN_STARTED,
              data: {
                mode: "prompt",
                resumed: false,
                input_preview: truncateInputPreview(options.message)
              }
            }
          )
        );
        const response = await withTimeout(
          client.prompt(sessionId, options.message),
          options.timeoutMs
        );
        output.onDone(response.stopReason);
        output.flush();
        return toPromptResult(response.stopReason, sessionId, client);
      },
      async () => {
        await client.cancelActivePrompt(INTERRUPT_CANCEL_WAIT_MS);
        await client.close();
      }
    );
  } finally {
    await client.close();
  }
}
async function createSession(options) {
  const client = new AcpClient({
    agentCommand: options.agentCommand,
    cwd: absolutePath(options.cwd),
    permissionMode: options.permissionMode,
    nonInteractivePermissions: options.nonInteractivePermissions,
    authCredentials: options.authCredentials,
    authPolicy: options.authPolicy,
    verbose: options.verbose
  });
  try {
    return await withInterrupt(
      async () => {
        await withTimeout(client.start(), options.timeoutMs);
        const createdSession = await withTimeout(
          client.createSession(absolutePath(options.cwd)),
          options.timeoutMs
        );
        const sessionId = createdSession.sessionId;
        const lifecycle = client.getAgentLifecycleSnapshot();
        const now = isoNow3();
        const record = {
          schema: SESSION_RECORD_SCHEMA,
          acpxRecordId: sessionId,
          acpSessionId: sessionId,
          agentSessionId: normalizeRuntimeSessionId(createdSession.agentSessionId),
          agentCommand: options.agentCommand,
          cwd: absolutePath(options.cwd),
          name: normalizeName(options.name),
          createdAt: now,
          lastUsedAt: now,
          lastSeq: 0,
          lastRequestId: void 0,
          eventLog: defaultSessionEventLog(sessionId),
          closed: false,
          closedAt: void 0,
          pid: lifecycle.pid,
          agentStartedAt: lifecycle.startedAt,
          protocolVersion: client.initializeResult?.protocolVersion,
          agentCapabilities: client.initializeResult?.agentCapabilities,
          ...createSessionConversation(now),
          acpx: {}
        };
        await writeSessionRecord(record);
        return record;
      },
      async () => {
        await client.close();
      }
    );
  } finally {
    await client.close();
  }
}
async function ensureSession(options) {
  const cwd = absolutePath(options.cwd);
  const gitRoot = findGitRepositoryRoot(cwd);
  const walkBoundary = options.walkBoundary ?? gitRoot ?? cwd;
  const existing = await findSessionByDirectoryWalk({
    agentCommand: options.agentCommand,
    cwd,
    name: options.name,
    boundary: walkBoundary
  });
  if (existing) {
    return {
      record: existing,
      created: false
    };
  }
  const record = await createSession({
    agentCommand: options.agentCommand,
    cwd,
    name: options.name,
    permissionMode: options.permissionMode,
    nonInteractivePermissions: options.nonInteractivePermissions,
    authCredentials: options.authCredentials,
    authPolicy: options.authPolicy,
    timeoutMs: options.timeoutMs,
    verbose: options.verbose
  });
  return {
    record,
    created: true
  };
}
function queueOwnerRuntimeOptionsFromSend(options) {
  return {
    sessionId: options.sessionId,
    permissionMode: options.permissionMode,
    nonInteractivePermissions: options.nonInteractivePermissions,
    authCredentials: options.authCredentials,
    authPolicy: options.authPolicy,
    suppressSdkConsoleErrors: options.suppressSdkConsoleErrors,
    verbose: options.verbose,
    ttlMs: options.ttlMs
  };
}
function spawnQueueOwnerProcess(options) {
  const payload = JSON.stringify(options);
  const child = spawn(process.execPath, [QUEUE_OWNER_MAIN_PATH], {
    detached: true,
    stdio: "ignore",
    env: {
      ...process.env,
      ACPX_QUEUE_OWNER_PAYLOAD: payload
    }
  });
  child.unref();
}
async function submitToRunningOwner(options, waitForCompletion) {
  return await trySubmitToRunningOwner({
    sessionId: options.sessionId,
    message: options.message,
    permissionMode: options.permissionMode,
    nonInteractivePermissions: options.nonInteractivePermissions,
    outputFormatter: options.outputFormatter,
    errorEmissionPolicy: options.errorEmissionPolicy,
    timeoutMs: options.timeoutMs,
    suppressSdkConsoleErrors: options.suppressSdkConsoleErrors,
    waitForCompletion,
    verbose: options.verbose
  });
}
async function runSessionQueueOwner(options) {
  const lease = await tryAcquireQueueOwnerLease(options.sessionId);
  if (!lease) {
    return;
  }
  let owner;
  const ttlMs = normalizeQueueOwnerTtlMs(options.ttlMs);
  const taskPollTimeoutMs = ttlMs === 0 ? void 0 : ttlMs;
  const initialTaskPollTimeoutMs = taskPollTimeoutMs == null ? void 0 : Math.max(taskPollTimeoutMs, 1e3);
  const turnController = new QueueOwnerTurnController({
    withTimeout: async (run, timeoutMs) => await withTimeout(run(), timeoutMs),
    setSessionModeFallback: async (modeId, timeoutMs) => {
      await runSessionSetModeDirect({
        sessionRecordId: options.sessionId,
        modeId,
        nonInteractivePermissions: options.nonInteractivePermissions,
        authCredentials: options.authCredentials,
        authPolicy: options.authPolicy,
        timeoutMs,
        verbose: options.verbose
      });
    },
    setSessionConfigOptionFallback: async (configId, value, timeoutMs) => {
      const result = await runSessionSetConfigOptionDirect({
        sessionRecordId: options.sessionId,
        configId,
        value,
        nonInteractivePermissions: options.nonInteractivePermissions,
        authCredentials: options.authCredentials,
        authPolicy: options.authPolicy,
        timeoutMs,
        verbose: options.verbose
      });
      return result.response;
    }
  });
  const applyPendingCancel = async () => {
    return await turnController.applyPendingCancel();
  };
  const scheduleApplyPendingCancel = () => {
    void applyPendingCancel().catch((error) => {
      if (options.verbose) {
        process.stderr.write(
          `[acpx] failed to apply deferred cancel: ${formatErrorMessage(error)}
`
        );
      }
    });
  };
  const setActiveController = (controller) => {
    turnController.setActiveController(controller);
    scheduleApplyPendingCancel();
  };
  const clearActiveController = () => {
    turnController.clearActiveController();
  };
  const runPromptTurn = async (run) => {
    turnController.beginTurn();
    try {
      return await run();
    } finally {
      turnController.endTurn();
    }
  };
  try {
    owner = await SessionQueueOwner.start(lease, {
      cancelPrompt: async () => {
        const accepted = await turnController.requestCancel();
        if (!accepted) {
          return false;
        }
        await applyPendingCancel();
        return true;
      },
      setSessionMode: async (modeId, timeoutMs) => {
        await turnController.setSessionMode(modeId, timeoutMs);
      },
      setSessionConfigOption: async (configId, value, timeoutMs) => {
        return await turnController.setSessionConfigOption(configId, value, timeoutMs);
      }
    });
    if (options.verbose) {
      process.stderr.write(
        `[acpx] queue owner ready for session ${options.sessionId} (ttlMs=${ttlMs})
`
      );
    }
    let isFirstTask = true;
    while (true) {
      const pollTimeoutMs = isFirstTask ? initialTaskPollTimeoutMs : taskPollTimeoutMs;
      const task = await owner.nextTask(pollTimeoutMs);
      if (!task) {
        break;
      }
      isFirstTask = false;
      await runPromptTurn(async () => {
        await runQueuedTask(options.sessionId, task, {
          verbose: options.verbose,
          nonInteractivePermissions: options.nonInteractivePermissions,
          authCredentials: options.authCredentials,
          authPolicy: options.authPolicy,
          suppressSdkConsoleErrors: options.suppressSdkConsoleErrors,
          onClientAvailable: setActiveController,
          onClientClosed: clearActiveController,
          onPromptActive: async () => {
            turnController.markPromptActive();
            await applyPendingCancel();
          }
        });
      });
    }
  } finally {
    turnController.beginClosing();
    if (owner) {
      await owner.close();
    }
    await releaseQueueOwnerLease(lease);
    if (options.verbose) {
      process.stderr.write(
        `[acpx] queue owner stopped for session ${options.sessionId}
`
      );
    }
  }
}
async function sendSession(options) {
  const waitForCompletion = options.waitForCompletion !== false;
  const queuedToOwner = await submitToRunningOwner(options, waitForCompletion);
  if (queuedToOwner) {
    return queuedToOwner;
  }
  spawnQueueOwnerProcess(queueOwnerRuntimeOptionsFromSend(options));
  for (let attempt = 0; attempt < QUEUE_OWNER_STARTUP_MAX_ATTEMPTS; attempt += 1) {
    const queued = await submitToRunningOwner(options, waitForCompletion);
    if (queued) {
      return queued;
    }
    await waitMs(QUEUE_CONNECT_RETRY_MS);
  }
  throw new Error(
    `Session queue owner failed to start for session ${options.sessionId}`
  );
}
async function cancelSessionPrompt(options) {
  const cancelled = await tryCancelOnRunningOwner(options);
  return {
    sessionId: options.sessionId,
    cancelled: cancelled === true
  };
}
async function setSessionMode(options) {
  const submittedToOwner = await trySetModeOnRunningOwner(
    options.sessionId,
    options.modeId,
    options.timeoutMs,
    options.verbose
  );
  if (submittedToOwner) {
    return {
      record: await resolveSessionRecord(options.sessionId),
      resumed: false
    };
  }
  return await runSessionSetModeDirect({
    sessionRecordId: options.sessionId,
    modeId: options.modeId,
    nonInteractivePermissions: options.nonInteractivePermissions,
    authCredentials: options.authCredentials,
    authPolicy: options.authPolicy,
    timeoutMs: options.timeoutMs,
    verbose: options.verbose
  });
}
async function setSessionConfigOption(options) {
  const ownerResponse = await trySetConfigOptionOnRunningOwner(
    options.sessionId,
    options.configId,
    options.value,
    options.timeoutMs,
    options.verbose
  );
  if (ownerResponse) {
    return {
      record: await resolveSessionRecord(options.sessionId),
      response: ownerResponse,
      resumed: false
    };
  }
  return await runSessionSetConfigOptionDirect({
    sessionRecordId: options.sessionId,
    configId: options.configId,
    value: options.value,
    nonInteractivePermissions: options.nonInteractivePermissions,
    authCredentials: options.authCredentials,
    authPolicy: options.authPolicy,
    timeoutMs: options.timeoutMs,
    verbose: options.verbose
  });
}
function firstAgentCommandToken(command) {
  const trimmed = command.trim();
  if (!trimmed) {
    return void 0;
  }
  const token = trimmed.split(/\s+/, 1)[0];
  return token.length > 0 ? token : void 0;
}
async function isLikelyMatchingProcess(pid, agentCommand) {
  const expectedToken = firstAgentCommandToken(agentCommand);
  if (!expectedToken) {
    return false;
  }
  const procCmdline = `/proc/${pid}/cmdline`;
  try {
    const payload = await fs4.readFile(procCmdline, "utf8");
    const argv = payload.split("\0").map((entry) => entry.trim()).filter((entry) => entry.length > 0);
    if (argv.length === 0) {
      return false;
    }
    const executableBase = path4.basename(argv[0]);
    const expectedBase = path4.basename(expectedToken);
    return executableBase === expectedBase || argv.some((entry) => path4.basename(entry) === expectedBase);
  } catch {
    return true;
  }
}
async function closeSession2(sessionId) {
  const record = await resolveSessionRecord(sessionId);
  await terminateQueueOwnerForSession(record.acpxRecordId);
  if (record.pid != null && isProcessAlive(record.pid) && await isLikelyMatchingProcess(record.pid, record.agentCommand)) {
    await terminateProcess(record.pid);
  }
  record.pid = void 0;
  record.closed = true;
  record.closedAt = isoNow3();
  await writeSessionRecord(record);
  return record;
}

export {
  EXIT_CODES,
  OUTPUT_FORMATS,
  AUTH_POLICIES,
  NON_INTERACTIVE_PERMISSION_POLICIES,
  ACPX_EVENT_TYPES,
  createAcpxEvent,
  sessionUpdateToEventDrafts,
  clientOperationToEventDraft,
  errorToEventDraft,
  isAcpxEvent,
  normalizeOutputError,
  exitCodeForOutputErrorCode,
  DEFAULT_HISTORY_LIMIT,
  findGitRepositoryRoot,
  listSessionsForAgent,
  findSession,
  findSessionByDirectoryWalk,
  InterruptedError,
  probeQueueOwnerHealth,
  DEFAULT_QUEUE_OWNER_TTL_MS,
  runOnce,
  createSession,
  ensureSession,
  runSessionQueueOwner,
  sendSession,
  cancelSessionPrompt,
  setSessionMode,
  setSessionConfigOption,
  closeSession2 as closeSession
};
