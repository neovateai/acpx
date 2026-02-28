import { InitializeResponse, PromptResponse, SetSessionConfigOptionResponse } from '@agentclientprotocol/sdk';
import { A as AcpClientOptions, P as PermissionStats, O as OutputErrorCode, a as OutputErrorOrigin, b as OutputErrorAcpPayload } from './types-DEG8uWyw.js';
export { C as ClientOperation, c as PermissionMode } from './types-DEG8uWyw.js';

type LoadSessionOptions = {
    suppressReplayUpdates?: boolean;
    replayIdleMs?: number;
    replayDrainTimeoutMs?: number;
};
type SessionCreateResult = {
    sessionId: string;
    agentSessionId?: string;
};
type SessionLoadResult = {
    agentSessionId?: string;
};
type AgentDisconnectReason = "process_exit" | "process_close" | "pipe_close" | "connection_close";
type AgentExitInfo = {
    exitCode: number | null;
    signal: NodeJS.Signals | null;
    exitedAt: string;
    reason: AgentDisconnectReason;
    unexpectedDuringPrompt: boolean;
};
type AgentLifecycleSnapshot = {
    pid?: number;
    startedAt?: string;
    running: boolean;
    lastExit?: AgentExitInfo;
};
declare class AcpClient {
    private readonly options;
    private connection?;
    private agent?;
    private initResult?;
    private readonly permissionStats;
    private readonly filesystem;
    private readonly terminalManager;
    private sessionUpdateChain;
    private observedSessionUpdates;
    private processedSessionUpdates;
    private suppressSessionUpdates;
    private activePrompt?;
    private readonly cancellingSessionIds;
    private closing;
    private agentStartedAt?;
    private lastAgentExit?;
    private lastKnownPid?;
    private readonly promptPermissionFailures;
    constructor(options: AcpClientOptions);
    get initializeResult(): InitializeResponse | undefined;
    getAgentPid(): number | undefined;
    getPermissionStats(): PermissionStats;
    getAgentLifecycleSnapshot(): AgentLifecycleSnapshot;
    supportsLoadSession(): boolean;
    hasActivePrompt(sessionId?: string): boolean;
    start(): Promise<void>;
    createSession(cwd?: string): Promise<SessionCreateResult>;
    loadSession(sessionId: string, cwd?: string): Promise<SessionLoadResult>;
    loadSessionWithOptions(sessionId: string, cwd?: string, options?: LoadSessionOptions): Promise<SessionLoadResult>;
    prompt(sessionId: string, text: string): Promise<PromptResponse>;
    setSessionMode(sessionId: string, modeId: string): Promise<void>;
    setSessionConfigOption(sessionId: string, configId: string, value: string): Promise<SetSessionConfigOptionResponse>;
    cancel(sessionId: string): Promise<void>;
    requestCancelActivePrompt(): Promise<boolean>;
    cancelActivePrompt(waitMs?: number): Promise<PromptResponse | undefined>;
    close(): Promise<void>;
    private detachAgentHandles;
    private getConnection;
    private log;
    private selectAuthMethod;
    private authenticateIfRequired;
    private handlePermissionRequest;
    private attachAgentLifecycleObservers;
    private recordAgentExit;
    private notePromptPermissionFailure;
    private consumePromptPermissionFailure;
    private handleReadTextFile;
    private handleWriteTextFile;
    private handleCreateTerminal;
    private handleTerminalOutput;
    private handleWaitForTerminalExit;
    private handleKillTerminal;
    private handleReleaseTerminal;
    private handleSessionUpdate;
    private waitForSessionUpdateDrain;
}

type AcpxErrorOptions = ErrorOptions & {
    outputCode?: OutputErrorCode;
    detailCode?: string;
    origin?: OutputErrorOrigin;
    retryable?: boolean;
    acp?: OutputErrorAcpPayload;
    outputAlreadyEmitted?: boolean;
};
declare class AcpxOperationalError extends Error {
    readonly outputCode?: OutputErrorCode;
    readonly detailCode?: string;
    readonly origin?: OutputErrorOrigin;
    readonly retryable?: boolean;
    readonly acp?: OutputErrorAcpPayload;
    readonly outputAlreadyEmitted?: boolean;
    constructor(message: string, options?: AcpxErrorOptions);
}
declare class AgentSpawnError extends AcpxOperationalError {
    readonly agentCommand: string;
    constructor(agentCommand: string, cause?: unknown);
}
declare class PermissionPromptUnavailableError extends AcpxOperationalError {
    constructor();
}

declare function mergeAgentRegistry(overrides?: Record<string, string>): Record<string, string>;
declare function resolveAgentCommand(agentName: string, overrides?: Record<string, string>): string;
declare function listBuiltInAgents(overrides?: Record<string, string>): string[];

export { AcpClient, AcpClientOptions, AgentSpawnError, PermissionPromptUnavailableError, PermissionStats, type SessionCreateResult, listBuiltInAgents, mergeAgentRegistry, resolveAgentCommand };
