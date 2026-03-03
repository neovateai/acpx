import { AgentCapabilities, SessionConfigOption, SessionNotification, RequestPermissionRequest, RequestPermissionResponse, StopReason } from '@agentclientprotocol/sdk';

declare const PERMISSION_MODES: readonly ["approve-all", "approve-reads", "deny-all"];
type PermissionMode = (typeof PERMISSION_MODES)[number];
declare const AUTH_POLICIES: readonly ["skip", "fail"];
type AuthPolicy = (typeof AUTH_POLICIES)[number];
declare const NON_INTERACTIVE_PERMISSION_POLICIES: readonly ["deny", "fail"];
type NonInteractivePermissionPolicy = (typeof NON_INTERACTIVE_PERMISSION_POLICIES)[number];
declare const ACPX_EVENT_SCHEMA: "acpx.event.v1";
declare const ACPX_EVENT_OUTPUT_STREAMS: readonly ["output", "thought"];
type AcpxEventOutputStream = (typeof ACPX_EVENT_OUTPUT_STREAMS)[number];
declare const ACPX_EVENT_TYPES: {
    readonly TURN_STARTED: "turn_started";
    readonly OUTPUT_DELTA: "output_delta";
    readonly TOOL_CALL: "tool_call";
    readonly PLAN: "plan";
    readonly UPDATE: "update";
    readonly CLIENT_OPERATION: "client_operation";
    readonly TURN_DONE: "turn_done";
    readonly ERROR: "error";
    readonly PROMPT_QUEUED: "prompt_queued";
    readonly SESSION_ENSURED: "session_ensured";
    readonly CANCEL_REQUESTED: "cancel_requested";
    readonly CANCEL_RESULT: "cancel_result";
    readonly MODE_SET: "mode_set";
    readonly CONFIG_SET: "config_set";
    readonly STATUS_SNAPSHOT: "status_snapshot";
    readonly SESSION_CLOSED: "session_closed";
};
declare const ACPX_EVENT_TURN_MODES: readonly ["prompt"];
type AcpxEventTurnMode = (typeof ACPX_EVENT_TURN_MODES)[number];
declare const ACPX_EVENT_STATUS_SNAPSHOT_STATUSES: readonly ["alive", "dead", "no-session"];
type AcpxEventStatusSnapshotStatus = (typeof ACPX_EVENT_STATUS_SNAPSHOT_STATUSES)[number];
declare const ACPX_EVENT_TOOL_CALL_STATUSES: readonly ["pending", "in_progress", "completed", "failed"];
type AcpxEventToolCallStatus = (typeof ACPX_EVENT_TOOL_CALL_STATUSES)[number];
declare const OUTPUT_ERROR_CODES: readonly ["NO_SESSION", "TIMEOUT", "PERMISSION_DENIED", "PERMISSION_PROMPT_UNAVAILABLE", "RUNTIME", "USAGE"];
type OutputErrorCode = (typeof OUTPUT_ERROR_CODES)[number];
declare const OUTPUT_ERROR_ORIGINS: readonly ["cli", "runtime", "queue", "acp"];
type OutputErrorOrigin = (typeof OUTPUT_ERROR_ORIGINS)[number];
type OutputErrorAcpPayload = {
    code: number;
    message: string;
    data?: unknown;
};
type PermissionStats = {
    requested: number;
    approved: number;
    denied: number;
    cancelled: number;
};
type ClientOperationMethod = "fs/read_text_file" | "fs/write_text_file" | "terminal/create" | "terminal/output" | "terminal/wait_for_exit" | "terminal/kill" | "terminal/release";
type ClientOperationStatus = "running" | "completed" | "failed";
type ClientOperation = {
    method: ClientOperationMethod;
    status: ClientOperationStatus;
    summary: string;
    details?: string;
    timestamp: string;
};
type AcpxEventEnvelope = {
    schema: typeof ACPX_EVENT_SCHEMA;
    event_id: string;
    session_id: string;
    acp_session_id?: string;
    agent_session_id?: string;
    request_id?: string;
    seq: number;
    ts: string;
};
type AcpxEvent = (AcpxEventEnvelope & {
    type: typeof ACPX_EVENT_TYPES.TURN_STARTED;
    data: {
        mode: AcpxEventTurnMode;
        resumed: boolean;
        input_preview?: string;
    };
}) | (AcpxEventEnvelope & {
    type: typeof ACPX_EVENT_TYPES.OUTPUT_DELTA;
    data: {
        stream: AcpxEventOutputStream;
        text: string;
    };
}) | (AcpxEventEnvelope & {
    type: typeof ACPX_EVENT_TYPES.TOOL_CALL;
    data: {
        tool_call_id: string;
        title?: string;
        status?: AcpxEventToolCallStatus;
    };
}) | (AcpxEventEnvelope & {
    type: typeof ACPX_EVENT_TYPES.PLAN;
    data: {
        entries: Array<{
            content: string;
            status: string;
            priority: string;
        }>;
    };
}) | (AcpxEventEnvelope & {
    type: typeof ACPX_EVENT_TYPES.UPDATE;
    data: {
        update: string;
    };
}) | (AcpxEventEnvelope & {
    type: typeof ACPX_EVENT_TYPES.CLIENT_OPERATION;
    data: {
        method: ClientOperationMethod;
        status: ClientOperationStatus;
        summary: string;
        details?: string;
    };
}) | (AcpxEventEnvelope & {
    type: typeof ACPX_EVENT_TYPES.TURN_DONE;
    data: {
        stop_reason: StopReason;
        permission_stats?: PermissionStats;
    };
}) | (AcpxEventEnvelope & {
    type: typeof ACPX_EVENT_TYPES.ERROR;
    data: {
        code: OutputErrorCode;
        detail_code?: string;
        origin?: OutputErrorOrigin;
        message: string;
        retryable?: boolean;
        acp_error?: OutputErrorAcpPayload;
    };
}) | (AcpxEventEnvelope & {
    type: typeof ACPX_EVENT_TYPES.PROMPT_QUEUED;
    data: {
        request_id: string;
    };
}) | (AcpxEventEnvelope & {
    type: typeof ACPX_EVENT_TYPES.SESSION_ENSURED;
    data: {
        created: boolean;
        name?: string;
        replaced_session_id?: string;
    };
}) | (AcpxEventEnvelope & {
    type: typeof ACPX_EVENT_TYPES.CANCEL_REQUESTED;
    data: Record<string, never>;
}) | (AcpxEventEnvelope & {
    type: typeof ACPX_EVENT_TYPES.CANCEL_RESULT;
    data: {
        cancelled: boolean;
    };
}) | (AcpxEventEnvelope & {
    type: typeof ACPX_EVENT_TYPES.MODE_SET;
    data: {
        mode_id: string;
        resumed?: boolean;
    };
}) | (AcpxEventEnvelope & {
    type: typeof ACPX_EVENT_TYPES.CONFIG_SET;
    data: {
        config_id: string;
        value: string;
        resumed?: boolean;
        config_options?: unknown[];
    };
}) | (AcpxEventEnvelope & {
    type: typeof ACPX_EVENT_TYPES.STATUS_SNAPSHOT;
    data: {
        status: AcpxEventStatusSnapshotStatus;
        pid?: number;
        summary?: string;
        uptime?: string;
        last_prompt_time?: string;
        exit_code?: number;
        signal?: string;
    };
}) | (AcpxEventEnvelope & {
    type: typeof ACPX_EVENT_TYPES.SESSION_CLOSED;
    data: {
        reason: "close";
    };
});
type AcpxEventDraft = Omit<AcpxEvent, "schema" | "event_id" | "session_id" | "seq" | "ts">;
type SessionEventLog = {
    active_path: string;
    segment_count: number;
    max_segment_bytes: number;
    max_segments: number;
    last_write_at?: string;
    last_write_error?: string | null;
};
type AcpClientOptions = {
    agentCommand: string;
    cwd: string;
    permissionMode: PermissionMode;
    nonInteractivePermissions?: NonInteractivePermissionPolicy;
    authCredentials?: Record<string, string>;
    authPolicy?: AuthPolicy;
    suppressSdkConsoleErrors?: boolean;
    verbose?: boolean;
    onSessionUpdate?: (notification: SessionNotification) => void;
    onClientOperation?: (operation: ClientOperation) => void;
    extraEnv?: Record<string, string>;
    onRequestPermission?: (params: RequestPermissionRequest) => Promise<RequestPermissionResponse>;
    onStderr?: (line: string) => void;
    onTiming?: (label: string, durationMs: number) => void;
};
declare const SESSION_RECORD_SCHEMA: "acpx.session.v1";
type SessionMessageImage = {
    source: string;
    size?: {
        width: number;
        height: number;
    } | null;
};
type SessionUserContent = {
    Text: string;
} | {
    Mention: {
        uri: string;
        content: string;
    };
} | {
    Image: SessionMessageImage;
};
type SessionToolUse = {
    id: string;
    name: string;
    raw_input: string;
    input: unknown;
    is_input_complete: boolean;
    thought_signature?: string | null;
};
type SessionToolResultContent = {
    Text: string;
} | {
    Image: SessionMessageImage;
};
type SessionToolResult = {
    tool_use_id: string;
    tool_name: string;
    is_error: boolean;
    content: SessionToolResultContent;
    output?: unknown;
};
type SessionAgentContent = {
    Text: string;
} | {
    Thinking: {
        text: string;
        signature?: string | null;
    };
} | {
    RedactedThinking: string;
} | {
    ToolUse: SessionToolUse;
};
type SessionUserMessage = {
    id: string;
    content: SessionUserContent[];
};
type SessionAgentMessage = {
    content: SessionAgentContent[];
    tool_results: Record<string, SessionToolResult>;
    reasoning_details?: unknown | null;
};
type SessionMessage = {
    User: SessionUserMessage;
} | {
    Agent: SessionAgentMessage;
} | "Resume";
type SessionTokenUsage = {
    input_tokens?: number;
    output_tokens?: number;
    cache_creation_input_tokens?: number;
    cache_read_input_tokens?: number;
};
type SessionAcpxState = {
    current_mode_id?: string;
    available_commands?: string[];
    config_options?: SessionConfigOption[];
};
type SessionRecord = {
    schema: typeof SESSION_RECORD_SCHEMA;
    acpxRecordId: string;
    acpSessionId: string;
    agentSessionId?: string;
    agentCommand: string;
    cwd: string;
    name?: string;
    createdAt: string;
    lastUsedAt: string;
    lastSeq: number;
    lastRequestId?: string;
    eventLog: SessionEventLog;
    closed?: boolean;
    closedAt?: string;
    pid?: number;
    agentStartedAt?: string;
    lastPromptAt?: string;
    lastAgentExitCode?: number | null;
    lastAgentExitSignal?: NodeJS.Signals | null;
    lastAgentExitAt?: string;
    lastAgentDisconnectReason?: string;
    protocolVersion?: number;
    agentCapabilities?: AgentCapabilities;
    title?: string | null;
    messages: SessionMessage[];
    updated_at: string;
    cumulative_token_usage: SessionTokenUsage;
    request_token_usage: Record<string, SessionTokenUsage>;
    acpx?: SessionAcpxState;
};

export { type AcpClientOptions as A, type ClientOperation as C, type OutputErrorCode as O, type PermissionStats as P, type SessionRecord as S, type OutputErrorOrigin as a, type OutputErrorAcpPayload as b, type AcpxEventDraft as c, type AcpxEvent as d, type PermissionMode as e, SESSION_RECORD_SCHEMA as f };
