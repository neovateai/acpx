import { AgentCapabilities, SessionConfigOption, SessionNotification, RequestPermissionRequest, RequestPermissionResponse } from '@agentclientprotocol/sdk';

declare const PERMISSION_MODES: readonly ["approve-all", "approve-reads", "deny-all"];
type PermissionMode = (typeof PERMISSION_MODES)[number];
declare const AUTH_POLICIES: readonly ["skip", "fail"];
type AuthPolicy = (typeof AUTH_POLICIES)[number];
declare const NON_INTERACTIVE_PERMISSION_POLICIES: readonly ["deny", "fail"];
type NonInteractivePermissionPolicy = (typeof NON_INTERACTIVE_PERMISSION_POLICIES)[number];
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

export type { AcpClientOptions as A, ClientOperation as C, OutputErrorCode as O, PermissionStats as P, SessionRecord as S, OutputErrorOrigin as a, OutputErrorAcpPayload as b, PermissionMode as c };
