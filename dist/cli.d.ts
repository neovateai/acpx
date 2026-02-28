#!/usr/bin/env node
import { S as SessionRecord } from './types-DEG8uWyw.js';
import '@agentclientprotocol/sdk';

declare function parseTtlSeconds(value: string): number;

type SessionConnectionStatus = "connected" | "needs reconnect";
declare function formatPromptSessionBannerLine(record: SessionRecord, currentCwd: string, connectionStatus?: SessionConnectionStatus): string;

export { formatPromptSessionBannerLine, parseTtlSeconds };
