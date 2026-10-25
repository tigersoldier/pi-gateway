import { homedir } from "node:os";
import { isAbsolute, join, resolve } from "node:path";
import { loadPiSdk, loadPiSdkSubmodule } from "./pi-sdk.ts";
import { PiArgsSpec } from "./piargs.ts";
import type { RpcObject, RpcSession, RpcSessionRuntime } from "./rpc-adapter.ts";

let startupEnvironmentTail: Promise<void> = Promise.resolve();

export interface PiSdkRuntimeOptions {
  piBin?: string;
  env?: NodeJS.ProcessEnv;
  cwd: string;
  sessionPath?: string;
  sessionDir?: string;
  agentDir?: string;
  piArgs?: PiArgsSpec;
}

export interface PiSdkModules {
  sdk: Record<string, unknown>;
  modelResolver: Record<string, unknown>;
  theme: unknown;
  settingsManager?: Record<string, unknown>;
  trustManager?: Record<string, unknown>;
  projectTrust?: Record<string, unknown>;
}

export async function createPiSdkRuntime(options: PiSdkRuntimeOptions): Promise<RpcSessionRuntime> {
  const piBin = options.piBin ?? "pi";
  const env = options.env ?? process.env;
  const [sdk, modelResolver, themeModule, settingsManager, trustManager, projectTrust] = await Promise.all([
    loadPiSdk(piBin, env),
    loadPiSdkSubmodule("dist/core/model-resolver.js", piBin, env),
    loadPiSdkSubmodule("dist/modes/interactive/theme/theme.js", piBin, env),
    loadPiSdkSubmodule("dist/core/settings-manager.js", piBin, env),
    loadPiSdkSubmodule("dist/core/trust-manager.js", piBin, env),
    loadPiSdkSubmodule("dist/core/project-trust.js", piBin, env),
  ]);
  return createPiSdkRuntimeFromModules(options, {
    sdk,
    modelResolver,
    theme: themeModule.theme,
    settingsManager,
    trustManager,
    projectTrust,
  });
}

/** Build a runtime from SDK exports; injection keeps lifecycle tests deterministic. */
export function createPiSdkRuntimeFromModules(
  options: PiSdkRuntimeOptions,
  modules: PiSdkModules,
): Promise<RpcSessionRuntime> {
  const piArgs = options.piArgs ?? new PiArgsSpec();
  const env = options.env;
  const offline = lastValue(piArgs, "offline") === "true";
  const offlineValue = env === undefined ? (offline ? "1" : undefined) : offline ? "1" : env.PI_OFFLINE ?? null;
  const skipVersionValue = env === undefined ? (offline ? "1" : undefined) : offline ? "1" : env.PI_SKIP_VERSION_CHECK ?? null;
  const startupEnvironment = { offline: offlineValue, skipVersion: skipVersionValue };
  return withStartupEnvironment(startupEnvironment, () =>
    createPiSdkRuntimeInternal(options, modules, startupEnvironment));
}

async function createPiSdkRuntimeInternal(
  options: PiSdkRuntimeOptions,
  modules: PiSdkModules,
  startupEnvironment: { offline: string | null | undefined; skipVersion: string | null | undefined },
): Promise<RpcSessionRuntime> {
  const piArgs = options.piArgs ?? new PiArgsSpec();
  const cwd = resolve(options.cwd);
  const agentDir = options.agentDir ?? stringResult(callSync(modules.sdk, "getAgentDir", []), "getAgentDir");
  const sessionDirArg = lastValue(piArgs, "session-dir");
  const sessionDir = options.sessionDir ?? (sessionDirArg ? resolveCliPath(sessionDirArg, cwd) : undefined);
  const noSession = options.sessionPath === undefined && lastValue(piArgs, "no-session") === "true";
  const sessionManagerClass = getMember(modules.sdk, "SessionManager");
  const sessionManager = options.sessionPath
    ? callSync(sessionManagerClass, "open", [resolveCliPath(options.sessionPath, cwd), sessionDir, undefined])
    : noSession
      ? callSync(sessionManagerClass, "inMemory", [cwd])
      : callSync(sessionManagerClass, "create", [cwd, sessionDir]);

  let firstSession = true;
  let initialFactoryInvocation = true;
  const createRuntime = async (input: RpcObject): Promise<RpcObject> => {
    if (initialFactoryInvocation) {
      initialFactoryInvocation = false;
      return createRuntimeInternal(input);
    }
    return withStartupEnvironment(startupEnvironment, () => createRuntimeInternal(input));
  };
  const createRuntimeInternal = async (input: RpcObject): Promise<RpcObject> => {
    const runtimeCwd = stringResult(input.cwd, "runtime cwd");
    const runtimeAgentDir = stringResult(input.agentDir, "runtime agentDir");
    const serviceOptions: RpcObject = {
      cwd: runtimeCwd,
      agentDir: runtimeAgentDir,
      resourceLoaderOptions: resourceLoaderOptions(piArgs, cwd),
    };
    let runtimeSettingsManager: unknown;
    if (modules.settingsManager) {
      const settingsManagerClass = getMember(modules.settingsManager, "SettingsManager");
      const trustOverride = lastValue(piArgs, "approve");
      const trustOptions = trustOverride === undefined ? {} : { projectTrusted: trustOverride === "true" };
      runtimeSettingsManager = callSync(settingsManagerClass, "create", [runtimeCwd, runtimeAgentDir, trustOptions]);
      serviceOptions.settingsManager = runtimeSettingsManager;
      if (trustOverride === undefined && modules.trustManager && modules.projectTrust &&
        callSync(modules.trustManager, "hasTrustRequiringProjectResources", [runtimeCwd]) === true) {
        const trustStoreClass = getMember(modules.trustManager, "ProjectTrustStore");
        if (typeof trustStoreClass !== "function") throw new Error("Pi SDK ProjectTrustStore is unavailable");
        const trustStore = Reflect.construct(trustStoreClass, [runtimeAgentDir]) as unknown;
        const defaultProjectTrust = callSync(runtimeSettingsManager, "getDefaultProjectTrust", []);
        serviceOptions.resourceLoaderReloadOptions = {
          resolveProjectTrust: async ({ extensionsResult }: { extensionsResult: unknown }) =>
            await callAsync(modules.projectTrust!, "resolveProjectTrusted", [{
              cwd: runtimeCwd,
              trustStore,
              defaultProjectTrust,
              extensionsResult,
              projectTrustContext: {
                cwd: runtimeCwd,
                mode: "rpc",
                hasUI: false,
                ui: { select: async () => undefined, confirm: async () => false, input: async () => undefined, notify: () => {} },
              },
            }]),
        };
      }
    }
    const services = asObject(await callAsync(modules.sdk, "createAgentSessionServices", [serviceOptions]), "agent session services");
    const modelRuntime = getMember(services, "modelRuntime");
    const enabledModels = runtimeSettingsManager === undefined
      ? []
      : stringArray(callSync(runtimeSettingsManager, "getEnabledModels", []), "enabled models");
    const patterns = splitList(piArgs.values.models ?? enabledModels);
    let scopedModels: RpcObject[] | undefined;
    if (patterns.length > 0) {
      const value = await callAsync(modules.modelResolver, "resolveModelScope", [patterns, modelRuntime]);
      scopedModels = arrayOfObjects(value, "scoped models");
    }

    const requestedModel = resolveCliModel(modules.modelResolver, piArgs, modelRuntime);
    if (requestedModel.error) throw new Error(stringResult(requestedModel.error, "model resolution error"));
    if (lastValue(piArgs, "provider") !== undefined && lastValue(piArgs, "model") === undefined) {
      throw new Error(`--provider requires --model (for example: --provider ${lastValue(piArgs, "provider")} --model <pattern>)`);
    }
    const sessionManagerForRuntime = input.sessionManager;
    const scopedSelection = !hasExistingSession(sessionManagerForRuntime)
      ? chooseScopedModel(scopedModels, runtimeSettingsManager, modelRuntime)
      : undefined;
    const model = requestedModel.model ?? scopedSelection?.model;
    const thinkingLevel = lastValue(piArgs, "thinking")
      ?? requestedModel.thinkingLevel
      ?? (scopedSelection === undefined ? undefined : optionalString(scopedSelection.thinkingLevel));
    const apiKey = lastValue(piArgs, "api-key");
    if (apiKey) {
      if (model === undefined) throw new Error("--api-key requires a model to be specified via --model, --provider/--model, or --models");
      const provider = optionalString(getMember(model, "provider"));
      if (!provider) throw new Error("--api-key requires a model with a provider");
      await callAsync(modelRuntime, "setRuntimeApiKey", [provider, apiKey]);
    }
    if (requestedModel.warning) console.warn(String(requestedModel.warning));
    const optionsForSession: RpcObject = {
      services,
      sessionManager: input.sessionManager,
      sessionStartEvent: input.sessionStartEvent,
    };
    if (piArgs.values.tools !== undefined) optionsForSession.tools = splitList(piArgs.values.tools);
    if (piArgs.values["exclude-tools"] !== undefined) optionsForSession.excludeTools = splitList(piArgs.values["exclude-tools"]);
    if (scopedModels !== undefined) optionsForSession.scopedModels = scopedModels;
    if (model !== undefined) optionsForSession.model = model;
    if (thinkingLevel !== undefined) optionsForSession.thinkingLevel = thinkingLevel;
    const noTools = lastValue(piArgs, "no-tools") === "true"
      ? "all"
      : lastValue(piArgs, "no-builtin-tools") === "true" ? "builtin" : undefined;
    if (noTools !== undefined) optionsForSession.noTools = noTools;

    const created = asObject(await callAsync(modules.sdk, "createAgentSessionFromServices", [optionsForSession]), "created session");
    const session = getMember(created, "session");
    if (firstSession) {
      const name = lastValue(piArgs, "name");
      if (name) callSync(session, "setSessionName", [name]);
    }
    firstSession = false;
    return {
      ...created,
      services,
      diagnostics: getMember(services, "diagnostics"),
    };
  };

  const runtimeValue = await callAsync(modules.sdk, "createAgentSessionRuntime", [createRuntime, {
    cwd,
    agentDir,
    sessionManager,
  }]);
  if (!isRpcRuntime(runtimeValue)) throw new Error("Pi SDK created an incompatible AgentSessionRuntime");
  runtimeValue.uiTheme = modules.theme;
  return runtimeValue;
}

async function withStartupEnvironment<T>(
  values: { offline: string | null | undefined; skipVersion: string | null | undefined },
  run: () => Promise<T>,
): Promise<T> {
  const previous = startupEnvironmentTail;
  let release!: () => void;
  startupEnvironmentTail = new Promise<void>((resolveRelease) => { release = resolveRelease; });
  await previous;
  const oldOffline = process.env.PI_OFFLINE;
  const oldSkipVersion = process.env.PI_SKIP_VERSION_CHECK;
  applyEnvironmentValue("PI_OFFLINE", values.offline);
  applyEnvironmentValue("PI_SKIP_VERSION_CHECK", values.skipVersion);
  try { return await run(); }
  finally {
    restoreEnvironmentValue("PI_OFFLINE", oldOffline);
    restoreEnvironmentValue("PI_SKIP_VERSION_CHECK", oldSkipVersion);
    release();
  }
}

function applyEnvironmentValue(name: "PI_OFFLINE" | "PI_SKIP_VERSION_CHECK", value: string | null | undefined): void {
  if (value === undefined) return;
  if (value === null) delete process.env[name];
  else process.env[name] = value;
}

function restoreEnvironmentValue(name: "PI_OFFLINE" | "PI_SKIP_VERSION_CHECK", value: string | undefined): void {
  if (value === undefined) delete process.env[name];
  else process.env[name] = value;
}

function resourceLoaderOptions(args: PiArgsSpec, initialCwd: string): RpcObject {
  const options: RpcObject = {};
  const extensions = resourcePaths(args, "extension", initialCwd);
  const skills = resourcePaths(args, "skill", initialCwd);
  const prompts = resourcePaths(args, "prompt-template", initialCwd);
  const themes = resourcePaths(args, "theme", initialCwd);
  if (extensions.length) options.additionalExtensionPaths = extensions;
  if (skills.length) options.additionalSkillPaths = skills;
  if (prompts.length) options.additionalPromptTemplatePaths = prompts;
  if (themes.length) options.additionalThemePaths = themes;
  if (lastValue(args, "no-extensions") === "true") options.noExtensions = true;
  if (lastValue(args, "no-skills") === "true") options.noSkills = true;
  if (lastValue(args, "no-prompt-templates") === "true") options.noPromptTemplates = true;
  if (lastValue(args, "no-themes") === "true") options.noThemes = true;
  if (lastValue(args, "no-context-files") === "true") options.noContextFiles = true;
  const systemPrompt = lastValue(args, "system-prompt");
  if (systemPrompt !== undefined) options.systemPrompt = systemPrompt;
  const appendSystemPrompt = args.values["append-system-prompt"];
  if (appendSystemPrompt?.length) options.appendSystemPrompt = [...appendSystemPrompt];
  return options;
}

function resourcePaths(args: PiArgsSpec, key: string, cwd: string): string[] {
  return (args.values[key] ?? []).map((item) => {
    if (item.startsWith("builtin:") || item.startsWith("npm:") || item.startsWith("git:")) return item;
    if (isAbsolute(item) || item.startsWith("~") || item.startsWith(".") || item.includes("\\")) return resolveCliPath(item, cwd);
    return item;
  });
}

function resolveCliModel(module: Record<string, unknown>, args: PiArgsSpec, modelRuntime: unknown): RpcObject {
  const cliModel = lastValue(args, "model");
  if (!cliModel) return {};
  const input: RpcObject = { cliModel, modelRuntime, inlineApiKey: lastValue(args, "api-key") !== undefined };
  const provider = lastValue(args, "provider");
  const thinking = lastValue(args, "thinking");
  if (provider !== undefined) input.cliProvider = provider;
  if (thinking !== undefined) input.cliThinking = thinking;
  return asObject(callSync(module, "resolveCliModel", [input]), "model resolution result");
}

function hasExistingSession(sessionManager: unknown): boolean {
  const buildContext = getMember(sessionManager, "buildSessionContext");
  if (typeof buildContext === "function") {
    const context = asObject(callSync(sessionManager, "buildSessionContext", []), "session context");
    return Array.isArray(context.messages) && context.messages.length > 0;
  }
  return callSync(sessionManager, "getLeafId", []) !== undefined && callSync(sessionManager, "getLeafId", []) !== null;
}

function chooseScopedModel(
  scopedModels: RpcObject[] | undefined,
  settingsManager: unknown,
  modelRuntime: unknown,
): RpcObject | undefined {
  if (!scopedModels?.length) return undefined;
  if (settingsManager !== undefined) {
    const provider = optionalString(callSync(settingsManager, "getDefaultProvider", []));
    const modelId = optionalString(callSync(settingsManager, "getDefaultModel", []));
    if (provider && modelId) {
      const savedModel = callSync(modelRuntime, "getModel", [provider, modelId]);
      if (isRecord(savedModel)) return scopedModels.find((entry) => modelsAreEqual(entry.model, savedModel)) ?? scopedModels[0];
    }
  }
  return scopedModels[0];
}

function modelsAreEqual(left: unknown, right: unknown): boolean {
  if (!isRecord(left) || !isRecord(right)) return false;
  return left.provider === right.provider && left.id === right.id;
}

function stringArray(value: unknown, label: string): string[] {
  if (value === undefined || value === null) return [];
  if (!Array.isArray(value) || !value.every((item) => typeof item === "string")) {
    throw new Error(`Pi SDK returned invalid ${label}`);
  }
  return value;
}

function splitList(values: readonly string[]): string[] {
  return values.flatMap((value) => value.split(",").map((part) => part.trim()).filter(Boolean));
}

function lastValue(args: PiArgsSpec, key: string): string | undefined {
  const values = args.values[key];
  return values?.[values.length - 1];
}

function resolveCliPath(path: string, cwd: string): string {
  const home = homedir();
  if (path === "~") return home;
  if (path.startsWith("~/")) return join(home, path.slice(2));
  return isAbsolute(path) ? path : resolve(cwd, path);
}

function isRpcRuntime(value: unknown): value is RpcSessionRuntime {
  if (!isRecord(value) || !isRpcSession(value.session)) return false;
  return hasMethod(value, "newSession") && hasMethod(value, "switchSession") && hasMethod(value, "fork") && hasMethod(value, "dispose");
}

function isRpcSession(value: unknown): value is RpcSession {
  if (!isRecord(value)) return false;
  const modelRuntime = value.modelRuntime;
  const sessionManager = value.sessionManager;
  const extensionRunner = value.extensionRunner;
  const resourceLoader = value.resourceLoader;
  const requiredMethods = [
    "subscribe", "bindExtensions", "waitForIdle", "navigateTree", "reload", "prompt", "steer", "followUp", "abort", "clearQueue",
    "setModel", "cycleModel", "setThinkingLevel", "cycleThinkingLevel", "getAvailableThinkingLevels", "setSteeringMode",
    "setFollowUpMode", "compact", "setAutoCompactionEnabled", "setAutoRetryEnabled", "abortRetry", "abortBash", "executeBash",
    "recordBashResult", "getSessionStats", "exportToHtml", "getUserMessagesForForking", "getLastAssistantText", "setSessionName",
  ];
  return requiredMethods.every((name) => hasMethod(value, name))
    && isRecord(modelRuntime) && hasMethod(modelRuntime, "getAvailableSnapshot")
    && isRecord(sessionManager) && ["getCwd", "getLeafId", "getEntries", "getTree"].every((name) => hasMethod(sessionManager, name))
    && isRecord(extensionRunner) && ["emitUserBash", "getRegisteredCommands"].every((name) => hasMethod(extensionRunner, name))
    && isRecord(resourceLoader) && hasMethod(resourceLoader, "getSkills")
    && Array.isArray(value.messages) && Array.isArray(value.promptTemplates)
    && typeof value.sessionId === "string";
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function hasMethod(value: Record<string, unknown>, name: string): boolean {
  return typeof getMember(value, name) === "function";
}

function asObject(value: unknown, label: string): Record<string, unknown> {
  if (!isRecord(value)) throw new Error(`Pi SDK returned invalid ${label}`);
  return value;
}

function arrayOfObjects(value: unknown, label: string): RpcObject[] {
  if (!Array.isArray(value) || !value.every(isRecord)) throw new Error(`Pi SDK returned invalid ${label}`);
  return value;
}

function stringResult(value: unknown, label: string): string {
  if (typeof value !== "string") throw new Error(`Pi SDK returned invalid ${label}`);
  return value;
}

function optionalString(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function getMember(value: unknown, key: string): unknown {
  if (value === null || (typeof value !== "object" && typeof value !== "function")) throw new Error(`Pi SDK value has no ${key}`);
  return Reflect.get(value, key) as unknown;
}

function callSync(value: unknown, key: string, args: unknown[]): unknown {
  const method = getMember(value, key);
  if (typeof method !== "function") throw new Error(`Pi SDK method ${key} is unavailable`);
  return Reflect.apply(method, value, args) as unknown;
}

async function callAsync(value: unknown, key: string, args: unknown[]): Promise<unknown> {
  return await callSync(value, key, args);
}
