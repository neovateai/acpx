#!/usr/bin/env node
import {
  runSessionQueueOwner
} from "./chunk-7Z7ROY26.js";
import "./chunk-BIRIVPPE.js";

// src/queue-owner-main.ts
function asRecord(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return void 0;
  }
  return value;
}
function parseQueueOwnerPayload(raw) {
  const parsed = JSON.parse(raw);
  const record = asRecord(parsed);
  if (!record) {
    throw new Error("queue owner payload must be an object");
  }
  if (typeof record.sessionId !== "string" || record.sessionId.trim().length === 0) {
    throw new Error("queue owner payload missing sessionId");
  }
  if (record.permissionMode !== "approve-all" && record.permissionMode !== "approve-reads" && record.permissionMode !== "deny-all") {
    throw new Error("queue owner payload has invalid permissionMode");
  }
  const options = {
    sessionId: record.sessionId,
    permissionMode: record.permissionMode
  };
  if (typeof record.nonInteractivePermissions === "string") {
    options.nonInteractivePermissions = record.nonInteractivePermissions === "deny" || record.nonInteractivePermissions === "fail" ? record.nonInteractivePermissions : void 0;
  }
  if (record.authCredentials && typeof record.authCredentials === "object") {
    const entries = Object.entries(record.authCredentials).filter(
      ([, value]) => typeof value === "string"
    );
    options.authCredentials = Object.fromEntries(entries);
  }
  if (record.authPolicy === "skip" || record.authPolicy === "fail") {
    options.authPolicy = record.authPolicy;
  }
  if (typeof record.suppressSdkConsoleErrors === "boolean") {
    options.suppressSdkConsoleErrors = record.suppressSdkConsoleErrors;
  }
  if (typeof record.verbose === "boolean") {
    options.verbose = record.verbose;
  }
  if (typeof record.ttlMs === "number" && Number.isFinite(record.ttlMs)) {
    options.ttlMs = record.ttlMs;
  }
  return options;
}
async function main() {
  const payload = process.env.ACPX_QUEUE_OWNER_PAYLOAD;
  if (!payload) {
    throw new Error("missing ACPX_QUEUE_OWNER_PAYLOAD");
  }
  const options = parseQueueOwnerPayload(payload);
  await runSessionQueueOwner(options);
}
void main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  process.stderr.write(`[acpx] queue owner failed: ${message}
`);
  process.exit(1);
});
