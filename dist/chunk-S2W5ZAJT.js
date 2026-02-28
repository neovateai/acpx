// src/agent-registry.ts
var AGENT_REGISTRY = {
  codex: "npx @zed-industries/codex-acp",
  claude: "npx -y @zed-industries/claude-agent-acp",
  gemini: "gemini",
  opencode: "npx -y opencode-ai acp",
  pi: "npx pi-acp"
};
var DEFAULT_AGENT_NAME = "codex";
function normalizeAgentName(value) {
  return value.trim().toLowerCase();
}
function mergeAgentRegistry(overrides) {
  if (!overrides) {
    return { ...AGENT_REGISTRY };
  }
  const merged = { ...AGENT_REGISTRY };
  for (const [name, command] of Object.entries(overrides)) {
    const normalized = normalizeAgentName(name);
    if (!normalized || !command.trim()) {
      continue;
    }
    merged[normalized] = command.trim();
  }
  return merged;
}
function resolveAgentCommand(agentName, overrides) {
  const normalized = normalizeAgentName(agentName);
  const registry = mergeAgentRegistry(overrides);
  return registry[normalized] ?? agentName;
}
function listBuiltInAgents(overrides) {
  return Object.keys(mergeAgentRegistry(overrides));
}

export {
  DEFAULT_AGENT_NAME,
  normalizeAgentName,
  mergeAgentRegistry,
  resolveAgentCommand,
  listBuiltInAgents
};
