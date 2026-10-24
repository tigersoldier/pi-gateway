import assert from "node:assert/strict";
import { resolve } from "node:path";
import { test } from "node:test";
import { parsePiArgs } from "../src/piargs.ts";
import { createPiSdkRuntimeFromModules, type PiSdkModules } from "../src/sdk-runtime.ts";
import type { RpcObject, RpcSession, RpcSessionRuntime } from "../src/rpc-adapter.ts";

class RuntimeTestSession implements RpcSession {
  model: RpcObject | undefined;
  thinkingLevel = "medium";
  isStreaming = false;
  isCompacting = false;
  steeringMode: "all" | "one-at-a-time" = "one-at-a-time";
  followUpMode: "all" | "one-at-a-time" = "one-at-a-time";
  sessionFile: string | undefined;
  sessionId = "test-session";
  sessionName: string | undefined;
  autoCompactionEnabled = true;
  pendingMessageCount = 0;
  messages: RpcObject[] = [];
  promptTemplates: Array<{ name: string; sourceInfo: RpcObject }> = [];
  modelRuntime = { getAvailableSnapshot: () => [] };
  sessionManager = { getCwd: () => "/project", getLeafId: () => null, getEntries: () => [], getTree: () => [] };
  extensionRunner = { emitUserBash: async () => undefined, getRegisteredCommands: () => [] };
  resourceLoader = { getSkills: () => ({ skills: [] }) };
  subscribe(): () => void { return () => {}; }
  async bindExtensions(): Promise<void> {}
  async waitForIdle(): Promise<void> {}
  async navigateTree(): Promise<{ cancelled: boolean }> { return { cancelled: false }; }
  async reload(): Promise<void> {}
  async prompt(): Promise<void> {}
  async steer(): Promise<"queued"> { return "queued"; }
  async followUp(): Promise<"queued"> { return "queued"; }
  async abort(): Promise<void> {}
  clearQueue(): { steering: string[]; followUp: string[] } { return { steering: [], followUp: [] }; }
  async setModel(): Promise<void> {}
  async cycleModel(): Promise<RpcObject | undefined> { return undefined; }
  setThinkingLevel(): void {}
  cycleThinkingLevel(): string | undefined { return undefined; }
  getAvailableThinkingLevels(): string[] { return []; }
  setSteeringMode(): void {}
  setFollowUpMode(): void {}
  async compact(): Promise<RpcObject> { return {}; }
  setAutoCompactionEnabled(): void {}
  setAutoRetryEnabled(): void {}
  abortRetry(): void {}
  abortBash(): void {}
  async executeBash(): Promise<RpcObject> { return {}; }
  recordBashResult(): void {}
  getSessionStats(): RpcObject { return {}; }
  async exportToHtml(): Promise<string> { return ""; }
  getUserMessagesForForking(): Array<{ entryId: string; text: string }> { return []; }
  getLastAssistantText(): string | null { return null; }
  setSessionName(name: string): void { this.sessionName = name; }
}

function fakeModules(overrides: {
  scope?: RpcObject[];
  model?: RpcObject;
  enabledModels?: string[];
  defaultProvider?: string;
  defaultModel?: string;
  savedModel?: RpcObject;
  existingSession?: boolean;
  trustRequired?: boolean;
  trustDecision?: boolean | null;
} = {}): { modules: PiSdkModules; captured: { manager?: string; sessionOptions?: RpcObject; serviceOptions?: RpcObject; apiKey?: string; trust?: RpcObject; offline?: string; skipVersion?: string; trustResolution?: RpcObject; modelPatterns?: string[] } } {
  const captured: { manager?: string; sessionOptions?: RpcObject; serviceOptions?: RpcObject; apiKey?: string; trust?: RpcObject; offline?: string; skipVersion?: string; trustResolution?: RpcObject; modelPatterns?: string[] } = {};
  const session = new RuntimeTestSession();
  const sessionManager = {
    getLeafId: () => null,
    getCwd: () => "/project",
    getEntries: () => [],
    getTree: () => [],
    buildSessionContext: () => ({ messages: overrides.existingSession ? [{ role: "user" }] : [] }),
  };
  const modelRuntime = {
    setRuntimeApiKey: async (_provider: string, apiKey: string) => { captured.apiKey = apiKey; },
    getModel: (provider: string, id: string) => overrides.savedModel?.provider === provider && overrides.savedModel.id === id
      ? overrides.savedModel : undefined,
    getAvailableSnapshot: () => [],
  };
  const services = { modelRuntime, diagnostics: [] };
  const settingsManagerValue = {
    marker: "settings-manager",
    getDefaultProjectTrust: () => "ask",
    getEnabledModels: () => overrides.enabledModels ?? [],
    getDefaultProvider: () => overrides.defaultProvider,
    getDefaultModel: () => overrides.defaultModel,
  };
  let runtimeFactory: ((input: RpcObject) => Promise<RpcObject>) | undefined;
  const runtime: RpcSessionRuntime = {
    session,
    async newSession() {
      if (runtimeFactory) await runtimeFactory({ cwd: "/project", agentDir: "/test/agent", sessionManager });
      return { cancelled: false };
    },
    async switchSession() { return { cancelled: false }; },
    async fork() { return { cancelled: false }; },
    async dispose() {},
  };
  const modules: PiSdkModules = {
    sdk: {
      getAgentDir: () => "/test/agent",
      SessionManager: {
        create: (_cwd: string, sessionDir?: string) => { captured.manager = `create:${sessionDir ?? "default"}`; return sessionManager; },
        open: (path: string) => { captured.manager = `open:${path}`; return sessionManager; },
        inMemory: () => { captured.manager = "inMemory"; return sessionManager; },
      },
      createAgentSessionServices: async (options: RpcObject) => {
        captured.serviceOptions = options;
        captured.offline = process.env.PI_OFFLINE;
        captured.skipVersion = process.env.PI_SKIP_VERSION_CHECK;
        return services;
      },
      createAgentSessionFromServices: async (options: RpcObject) => {
        captured.sessionOptions = options;
        if (overrides.model) session.model = overrides.model;
        return { session };
      },
      createAgentSessionRuntime: async (factory: (input: RpcObject) => Promise<RpcObject>, options: RpcObject) => {
        runtimeFactory = factory;
        const created = await factory({ ...options, sessionManager });
        return { ...runtime, session: created.session as RpcSession };
      },
    },
    modelResolver: {
      resolveCliModel: () => ({ model: overrides.model }),
      resolveModelScope: async (patterns: string[]) => { captured.modelPatterns = patterns; return overrides.scope ?? []; },
    },
    theme: { name: "test-theme" },
    settingsManager: {
      SettingsManager: {
        create: (_cwd: string, _agentDir: string, options: RpcObject) => {
          captured.trust = options;
          return settingsManagerValue;
        },
      },
    },
    trustManager: {
      hasTrustRequiringProjectResources: () => overrides.trustRequired === true,
      ProjectTrustStore: class {
        constructor(_agentDir: string) {}
        get() { return overrides.trustDecision ?? null; }
      },
    },
    projectTrust: {
      resolveProjectTrusted: async (options: RpcObject) => {
        captured.trustResolution = options;
        return false;
      },
    },
  };
  return { modules, captured };
}

test("SDK runtime applies Pi spawn args to services and session options", async () => {
  const { modules, captured } = fakeModules();
  const piArgs = parsePiArgs([
    "--session-dir", "./sessions",
    "--extension", "./extensions/test.ts",
    "--skill", "./skills",
    "--prompt-template", "./prompts",
    "--theme", "./themes",
    "--no-extensions",
    "--no-context-files",
    "--tools", "read,bash",
    "--exclude-tools", "edit,write",
    "--system-prompt", "system text",
    "--append-system-prompt", "append one",
    "--append-system-prompt", "append two",
  ]);
  const runtime = await createPiSdkRuntimeFromModules({ cwd: "/project", piArgs }, modules);
  assert.equal(runtime.uiTheme, modules.theme);
  assert.equal(captured.manager, `create:${resolve("/project", "sessions")}`);
  assert.deepEqual(captured.serviceOptions?.resourceLoaderOptions, {
    additionalExtensionPaths: [resolve("/project", "extensions/test.ts")],
    additionalSkillPaths: [resolve("/project", "skills")],
    additionalPromptTemplatePaths: [resolve("/project", "prompts")],
    additionalThemePaths: [resolve("/project", "themes")],
    noExtensions: true,
    noContextFiles: true,
    systemPrompt: "system text",
    appendSystemPrompt: ["append one", "append two"],
  });
  assert.deepEqual(captured.sessionOptions?.tools, ["read", "bash"]);
  assert.deepEqual(captured.sessionOptions?.excludeTools, ["edit", "write"]);
  assert.equal(Object.hasOwn(captured.sessionOptions ?? {}, "noTools"), false);
});

test("SDK runtime resolves untrusted project resources without prompting in RPC mode", async () => {
  const { modules, captured } = fakeModules({ trustRequired: true, trustDecision: null });
  await createPiSdkRuntimeFromModules({ cwd: "/project" }, modules);
  const resourceOptions = captured.serviceOptions?.resourceLoaderReloadOptions as RpcObject | undefined;
  assert.ok(resourceOptions);
  const resolveTrust = resourceOptions.resolveProjectTrust as (input: RpcObject) => Promise<boolean>;
  assert.equal(await resolveTrust({ extensionsResult: {} }), false);
  assert.equal(captured.trustResolution?.cwd, "/project");
  assert.equal(captured.trustResolution?.defaultProjectTrust, "ask");
  const context = captured.trustResolution?.projectTrustContext as RpcObject;
  assert.equal(context.cwd, "/project");
  assert.equal(context.mode, "rpc");
  assert.equal(context.hasUI, false);
  const ui = context.ui as RpcObject;
  for (const method of ["select", "confirm", "input", "notify"]) assert.equal(typeof ui[method], "function");
});

test("SDK runtime applies explicit project trust overrides", async () => {
  const approved = fakeModules();
  await createPiSdkRuntimeFromModules({ cwd: "/project", piArgs: parsePiArgs(["--approve"]) }, approved.modules);
  assert.deepEqual(approved.captured.trust, { projectTrusted: true });
  assert.equal((approved.captured.serviceOptions?.settingsManager as RpcObject).marker, "settings-manager");

  const denied = fakeModules();
  await createPiSdkRuntimeFromModules({ cwd: "/project", piArgs: parsePiArgs(["--no-approve"]) }, denied.modules);
  assert.deepEqual(denied.captured.trust, { projectTrusted: false });
});

test("SDK runtime applies offline mode only during serialized startup", async () => {
  const oldOffline = process.env.PI_OFFLINE;
  const oldSkipVersion = process.env.PI_SKIP_VERSION_CHECK;
  delete process.env.PI_OFFLINE;
  delete process.env.PI_SKIP_VERSION_CHECK;
  try {
    const { modules, captured } = fakeModules();
    const runtime = await createPiSdkRuntimeFromModules({ cwd: "/project", env: {}, piArgs: parsePiArgs(["--offline"]) }, modules);
    assert.equal(captured.offline, "1");
    assert.equal(captured.skipVersion, "1");
    assert.equal(process.env.PI_OFFLINE, undefined);
    await runtime.newSession();
    assert.equal(captured.offline, "1", "replacement sessions retain Pi offline mode");
    assert.equal(captured.skipVersion, "1", "replacement sessions also skip update checks");
    assert.equal(process.env.PI_OFFLINE, undefined, "replacement setup restores the host environment");
    assert.equal(process.env.PI_SKIP_VERSION_CHECK, undefined);
  } finally {
    if (oldOffline === undefined) delete process.env.PI_OFFLINE;
    else process.env.PI_OFFLINE = oldOffline;
    if (oldSkipVersion === undefined) delete process.env.PI_SKIP_VERSION_CHECK;
    else process.env.PI_SKIP_VERSION_CHECK = oldSkipVersion;
  }
});

test("SDK runtime applies API keys to models selected by --models", async () => {
  const selected = { provider: "test", id: "scoped-model" };
  const { modules, captured } = fakeModules({ scope: [{ model: selected, thinkingLevel: "high" }] });
  await createPiSdkRuntimeFromModules({ cwd: "/project", piArgs: parsePiArgs(["--models", "test/scoped-model:high", "--api-key", "scoped-key"]) }, modules);
  assert.deepEqual(captured.modelPatterns, ["test/scoped-model:high"]);
  assert.equal(captured.sessionOptions?.model, selected);
  assert.equal(captured.apiKey, "scoped-key");
});

test("SDK runtime rejects provider without model like Pi CLI", async () => {
  const { modules } = fakeModules();
  await assert.rejects(
    createPiSdkRuntimeFromModules({ cwd: "/project", piArgs: parsePiArgs(["--provider", "test"]) }, modules),
    /--provider requires --model/,
  );
});

test("SDK runtime scopes enabled models and prefers a saved model in scope", async () => {
  const first = { provider: "test", id: "first" };
  const saved = { provider: "test", id: "saved" };
  const { modules, captured } = fakeModules({
    enabledModels: ["test/first", "test/saved"],
    scope: [{ model: first }, { model: saved, thinkingLevel: "low" }],
    defaultProvider: "test",
    defaultModel: "saved",
    savedModel: saved,
  });
  await createPiSdkRuntimeFromModules({ cwd: "/project", piArgs: parsePiArgs(["--api-key", "default-key"]) }, modules);
  assert.deepEqual(captured.modelPatterns, ["test/first", "test/saved"]);
  assert.equal(captured.sessionOptions?.model, saved);
  assert.equal(captured.sessionOptions?.thinkingLevel, "low");
  assert.equal(captured.apiKey, "default-key");
});

test("SDK runtime does not replace the model in an existing session with its scope default", async () => {
  const selected = { provider: "test", id: "scoped-model" };
  const { modules } = fakeModules({ existingSession: true, scope: [{ model: selected }] });
  await assert.rejects(
    createPiSdkRuntimeFromModules({ cwd: "/project", piArgs: parsePiArgs(["--models", "test/scoped-model", "--api-key", "key"]) }, modules),
    /--api-key requires a model/,
  );
});

test("SDK runtime selects scoped models, API keys, and in-memory sessions", async () => {
  const selected = { provider: "test", id: "model-1" };
  const { modules, captured } = fakeModules({ scope: [{ model: selected, thinkingLevel: "high" }], model: selected });
  const piArgs = parsePiArgs(["--no-session", "--models", "test/model-1:high", "--api-key", "secret", "--provider", "test", "--model", "test/model-1", "--name", "named"]);
  const runtime = await createPiSdkRuntimeFromModules({ cwd: "/project", piArgs }, modules);
  assert.equal(captured.manager, "inMemory");
  assert.equal(captured.apiKey, "secret");
  assert.equal(captured.sessionOptions?.model, selected);
  assert.equal(captured.sessionOptions?.thinkingLevel, "high");
  assert.deepEqual(captured.sessionOptions?.scopedModels, [{ model: selected, thinkingLevel: "high" }]);
  assert.equal(runtime.session.sessionName, "named");
});
