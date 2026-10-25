import { randomUUID } from "node:crypto";

export type RpcObject = Record<string, unknown>;
export type RpcResponse = RpcObject & { type: "response"; command: string; success: boolean };
export type RpcEventSink = (event: RpcObject) => void;
export type PromptDisposition = "handled" | "queued" | "started";
export type QueuedInputDisposition = "handled" | "queued";

export interface RpcSessionManager {
  getCwd(): string;
  getLeafId(): string | null;
  getEntries(): RpcObject[];
  getTree(): RpcObject[];
}

export interface RpcModelRuntime {
  getAvailableSnapshot(): RpcObject[];
}

export interface RpcExtensionRunner {
  emitUserBash(event: RpcObject): Promise<{ result?: RpcObject; operations?: RpcObject } | undefined>;
  getRegisteredCommands(): Array<{
    invocationName: string;
    description?: string;
    sourceInfo: RpcObject;
  }>;
}

export interface RpcResourceLoader {
  getSkills(): { skills: Array<{ name: string; description?: string; sourceInfo: RpcObject }> };
}

export interface RpcSession {
  model: RpcObject | undefined;
  thinkingLevel: string;
  isStreaming: boolean;
  isCompacting: boolean;
  steeringMode: "all" | "one-at-a-time";
  followUpMode: "all" | "one-at-a-time";
  sessionFile: string | undefined;
  sessionId: string;
  sessionName: string | undefined;
  autoCompactionEnabled: boolean;
  pendingMessageCount: number;
  messages: RpcObject[];
  promptTemplates: Array<{ name: string; description?: string; sourceInfo: RpcObject }>;
  modelRuntime: RpcModelRuntime;
  sessionManager: RpcSessionManager;
  extensionRunner: RpcExtensionRunner;
  resourceLoader: RpcResourceLoader;
  subscribe(listener: (event: RpcObject) => void): () => void;
  bindExtensions(bindings: RpcObject): Promise<void>;
  waitForIdle(): Promise<void>;
  navigateTree(targetId: string, options?: RpcObject): Promise<{ cancelled: boolean }>;
  reload(): Promise<void>;
  prompt(message: string, options?: RpcObject): Promise<void>;
  steer(message: string, images?: unknown[], options?: RpcObject): Promise<QueuedInputDisposition>;
  followUp(message: string, images?: unknown[], options?: RpcObject): Promise<QueuedInputDisposition>;
  abort(): Promise<void>;
  clearQueue(): { steering: string[]; followUp: string[] };
  setModel(model: RpcObject): Promise<void>;
  cycleModel(): Promise<RpcObject | undefined>;
  setThinkingLevel(level: string): void;
  cycleThinkingLevel(): string | undefined;
  getAvailableThinkingLevels(): string[];
  setSteeringMode(mode: "all" | "one-at-a-time"): void;
  setFollowUpMode(mode: "all" | "one-at-a-time"): void;
  compact(customInstructions?: string): Promise<RpcObject>;
  setAutoCompactionEnabled(enabled: boolean): void;
  setAutoRetryEnabled(enabled: boolean): void;
  abortRetry(): void;
  abortBash(): void;
  executeBash(command: string, onChunk?: undefined, options?: RpcObject): Promise<RpcObject>;
  recordBashResult(command: string, result: RpcObject, options?: RpcObject): void;
  getSessionStats(): RpcObject;
  exportToHtml(outputPath?: string): Promise<string>;
  getUserMessagesForForking(): Array<{ entryId: string; text: string }>;
  getLastAssistantText(): string | null;
  setSessionName(name: string): void;
}

export interface RpcSessionRuntime {
  session: RpcSession;
  uiTheme?: unknown;
  newSession(options?: RpcObject): Promise<{ cancelled: boolean }>;
  switchSession(sessionPath: string, options?: RpcObject): Promise<{ cancelled: boolean }>;
  fork(entryId: string, options?: RpcObject): Promise<{ cancelled: boolean; selectedText?: string }>;
  dispose(): Promise<void>;
  setRebindSession?(callback: () => Promise<void>): void;
}

export interface RpcUIResponse {
  type: "extension_ui_response";
  id: string;
  value?: string;
  confirmed?: boolean;
  cancelled?: true;
}

interface PendingDialog<T> {
  resolve(value: T): void;
  timer?: ReturnType<typeof setTimeout>;
  signal?: AbortSignal;
  abortListener?: () => void;
}

/** RPC-mode extension UI broker; dialogs are completed by a gateway client. */
export class RpcUIBroker {
  private readonly pending = new Map<string, PendingDialog<unknown>>();
  private readonly emit: RpcEventSink;

  constructor(emit: RpcEventSink) {
    this.emit = emit;
  }

  select(title: string, options: string[], opts?: { signal?: AbortSignal; timeout?: number }): Promise<string | undefined> {
    return this.dialog(opts, undefined, { method: "select", title, options, timeout: opts?.timeout }, (response) => {
      if (response.cancelled) return undefined;
      return typeof response.value === "string" ? response.value : undefined;
    });
  }

  confirm(title: string, message: string, opts?: { signal?: AbortSignal; timeout?: number }): Promise<boolean> {
    return this.dialog(opts, false, { method: "confirm", title, message, timeout: opts?.timeout }, (response) => {
      if (response.cancelled) return false;
      return typeof response.confirmed === "boolean" ? response.confirmed : false;
    });
  }

  input(title: string, placeholder?: string, opts?: { signal?: AbortSignal; timeout?: number }): Promise<string | undefined> {
    return this.dialog(opts, undefined, { method: "input", title, placeholder, timeout: opts?.timeout }, (response) => {
      if (response.cancelled) return undefined;
      return typeof response.value === "string" ? response.value : undefined;
    });
  }

  editor(title: string, prefill?: string): Promise<string | undefined> {
    return this.dialog(undefined, undefined, { method: "editor", title, prefill }, (response) => {
      if (response.cancelled) return undefined;
      return typeof response.value === "string" ? response.value : undefined;
    });
  }

  notify(message: string, notifyType?: "info" | "warning" | "error"): void {
    this.emit({ type: "extension_ui_request", id: randomUUID(), method: "notify", message, notifyType });
  }

  setStatus(statusKey: string, statusText: string | undefined): void {
    this.emit({ type: "extension_ui_request", id: randomUUID(), method: "setStatus", statusKey, statusText });
  }

  setWidget(widgetKey: string, widgetLines: unknown, options?: { placement?: "aboveEditor" | "belowEditor" }): void {
    if (widgetLines !== undefined && !Array.isArray(widgetLines)) return;
    this.emit({
      type: "extension_ui_request",
      id: randomUUID(),
      method: "setWidget",
      widgetKey,
      widgetLines,
      widgetPlacement: options?.placement,
    });
  }

  setTitle(title: string): void {
    this.emit({ type: "extension_ui_request", id: randomUUID(), method: "setTitle", title });
  }

  setEditorText(text: string): void {
    this.emit({ type: "extension_ui_request", id: randomUUID(), method: "set_editor_text", text });
  }

  respond(response: RpcUIResponse): boolean {
    const pending = this.pending.get(response.id);
    if (!pending) return false;
    this.cleanup(response.id, pending);
    pending.resolve(response);
    return true;
  }

  cancelAll(): void {
    for (const [id, pending] of this.pending) {
      this.cleanup(id, pending);
      pending.resolve({ type: "extension_ui_response", id, cancelled: true });
    }
  }

  private dialog<T>(
    opts: { signal?: AbortSignal; timeout?: number } | undefined,
    fallback: T,
    request: RpcObject,
    parse: (response: RpcUIResponse) => T,
  ): Promise<T> {
    if (opts?.signal?.aborted) return Promise.resolve(fallback);
    const id = randomUUID();
    return new Promise<T>((resolve) => {
      const pending: PendingDialog<T> = {
        resolve: (value) => resolve(parse(value as RpcUIResponse)),
        signal: opts?.signal,
      };
      const finishFallback = () => {
        this.cleanup(id, pending);
        resolve(fallback);
      };
      if (opts?.signal) {
        pending.abortListener = finishFallback;
        opts.signal.addEventListener("abort", finishFallback, { once: true });
      }
      if (opts?.timeout) pending.timer = setTimeout(finishFallback, opts.timeout);
      this.pending.set(id, pending as PendingDialog<unknown>);
      this.emit({ type: "extension_ui_request", id, ...request });
    });
  }

  private cleanup(id: string, pending: PendingDialog<unknown>): void {
    if (pending.timer) clearTimeout(pending.timer);
    if (pending.abortListener) pending.signal?.removeEventListener("abort", pending.abortListener);
    this.pending.delete(id);
  }
}

/** Build the subset of ExtensionUIContext supported by Pi RPC mode. */
export function createRpcUIContext(broker: RpcUIBroker, theme?: unknown): RpcObject {
  return {
    select: broker.select.bind(broker),
    confirm: broker.confirm.bind(broker),
    input: broker.input.bind(broker),
    notify: broker.notify.bind(broker),
    onTerminalInput: () => () => {},
    setStatus: broker.setStatus.bind(broker),
    setWorkingMessage: () => {},
    setWorkingVisible: () => {},
    setWorkingIndicator: () => {},
    setHiddenThinkingLabel: () => {},
    setWidget: broker.setWidget.bind(broker),
    setFooter: () => {},
    setHeader: () => {},
    setTitle: broker.setTitle.bind(broker),
    custom: async () => undefined,
    pasteToEditor: (text: string) => broker.setEditorText(text),
    setEditorText: broker.setEditorText.bind(broker),
    getEditorText: () => "",
    editor: broker.editor.bind(broker),
    addAutocompleteProvider: () => {},
    setEditorComponent: () => {},
    getEditorComponent: () => undefined,
    theme,
    getAllThemes: () => [],
    getTheme: () => undefined,
    setTheme: () => ({ success: false, error: "Theme switching not supported in RPC mode" }),
    getToolsExpanded: () => false,
    setToolsExpanded: () => {},
  };
}

/** Pi's RPC mode removes internal assistant-message fields before JSON output. */
export function toRpcJsonEvent(event: RpcObject): RpcObject {
  if (event.type !== "message_update") return event;
  const message = asRecord(event.message);
  if (message.role !== "assistant") throw new Error("message_update message is not an assistant message");
  const assistantMessageEvent = asRecord(event.assistantMessageEvent);
  if (assistantMessageEvent.type === "toolcall_start") {
    const contentIndex = requireNumber(assistantMessageEvent.contentIndex, "contentIndex");
    const partial = asRecord(assistantMessageEvent.partial);
    const content = partial.content;
    const toolCall = Array.isArray(content) ? content[contentIndex] : undefined;
    const tool = asRecord(toolCall);
    if (tool.type !== "toolCall") throw new Error(`toolcall_start content at index ${contentIndex} is not a tool call`);
    const { partial: _partial, ...deltaEvent } = assistantMessageEvent;
    return {
      type: "message_update",
      usage: message.usage,
      assistantMessageEvent: { ...deltaEvent, id: tool.id, toolName: tool.name },
    };
  }
  const { partial: _partial, ...deltaEvent } = assistantMessageEvent;
  return { type: "message_update", usage: message.usage, assistantMessageEvent: deltaEvent };
}

export class SdkRpcAdapter {
  private unsubscribe?: () => void;
  private shutdownRequested = false;
  private shutdownReported = false;
  readonly ui: RpcUIBroker;
  private readonly runtime: RpcSessionRuntime;
  private readonly events: RpcEventSink;

  constructor(runtime: RpcSessionRuntime, events: RpcEventSink) {
    this.runtime = runtime;
    this.events = events;
    this.ui = new RpcUIBroker(events);
  }

  async bind(): Promise<void> {
    this.runtime.setRebindSession?.(() => this.bind());
    this.unsubscribe?.();
    const session = this.runtime.session;
    await session.bindExtensions({
      uiContext: createRpcUIContext(this.ui, this.runtime.uiTheme),
      mode: "rpc",
      commandContextActions: {
        waitForIdle: () => session.waitForIdle(),
        newSession: (options: RpcObject) => this.runtime.newSession(options),
        fork: async (entryId: string, options?: RpcObject) => {
          const result = await this.runtime.fork(entryId, options);
          return { cancelled: result.cancelled };
        },
        navigateTree: (targetId: string, options?: RpcObject) => session.navigateTree(targetId, options),
        switchSession: (path: string, options?: RpcObject) => this.runtime.switchSession(path, options),
        reload: () => session.reload(),
      },
      shutdownHandler: () => { this.shutdownRequested = true; },
      onError: (err: RpcObject) => this.events({
        type: "extension_error",
        extensionPath: err.extensionPath,
        event: err.event,
        error: err.error,
      }),
    });
    this.unsubscribe = session.subscribe((event) => {
      this.events(toRpcJsonEvent(event));
      if (event.type === "agent_settled") this.reportRequestedShutdown();
    });
  }

  private reportRequestedShutdown(): void {
    if (!this.shutdownRequested || this.shutdownReported) return;
    this.shutdownReported = true;
    this.events({ type: "gw_runtime_failure", error: "Pi shut down by extension" });
  }

  async dispose(): Promise<void> {
    this.unsubscribe?.();
    this.unsubscribe = undefined;
    this.ui.cancelAll();
    await this.runtime.dispose();
  }

  async handle(command: RpcObject): Promise<void> {
    const type = requireString(command.type, "type");
    const id = optionalString(command.id);
    const session = this.runtime.session;
    const send = (value: RpcResponse) => this.events(value);
    try {
      if (type === "extension_ui_response") {
        this.ui.respond({
          type,
          id: requireString(command.id, "id"),
          value: optionalString(command.value),
          confirmed: typeof command.confirmed === "boolean" ? command.confirmed : undefined,
          cancelled: command.cancelled === true ? true : undefined,
        });
        return;
      }
      switch (type) {
        case "prompt": {
          let responseSent = false;
          void session.prompt(requireString(command.message, "message"), {
            images: optionalArray(command.images),
            streamingBehavior: optionalEnum(command.streamingBehavior, ["steer", "followUp"] as const),
            source: "rpc",
            preflightResult: (disposition: PromptDisposition) => {
              responseSent = true;
              send(rpcSuccess(id, type, { disposition }));
            },
          }).catch((error: unknown) => {
            if (!responseSent) send(rpcError(id, type, messageOf(error)));
            else this.events({ type: "gw_runtime_failure", ...(id ? { id } : {}), error: messageOf(error) });
          });
          return;
        }
        case "steer": {
          const disposition = await session.steer(requireString(command.message, "message"), optionalArray(command.images), { source: "rpc" });
          send(rpcSuccess(id, type, { disposition }));
          return;
        }
        case "follow_up": {
          const disposition = await session.followUp(requireString(command.message, "message"), optionalArray(command.images), { source: "rpc" });
          send(rpcSuccess(id, type, { disposition }));
          return;
        }
        case "abort":
          await session.abort();
          send(rpcSuccess(id, type));
          return;
        case "clear_queue":
          send(rpcSuccess(id, type, session.clearQueue()));
          return;
        case "new_session": {
          const parentSession = optionalString(command.parentSession);
          const result = await this.runtime.newSession(parentSession ? { parentSession } : undefined);
          if (!result.cancelled) await this.bind();
          send(rpcSuccess(id, type, result));
          return;
        }
        case "get_state":
          send(rpcSuccess(id, type, {
            model: session.model,
            thinkingLevel: session.thinkingLevel,
            isStreaming: session.isStreaming,
            isCompacting: session.isCompacting,
            steeringMode: session.steeringMode,
            followUpMode: session.followUpMode,
            sessionFile: session.sessionFile ?? "",
            sessionId: session.sessionId,
            sessionName: session.sessionName,
            autoCompactionEnabled: session.autoCompactionEnabled,
            messageCount: session.messages.length,
            pendingMessageCount: session.pendingMessageCount,
          }));
          return;
        case "set_model": {
          const provider = requireString(command.provider, "provider");
          const modelId = requireString(command.modelId, "modelId");
          const model = session.modelRuntime.getAvailableSnapshot().find((candidate) => candidate.provider === provider && candidate.id === modelId);
          if (!model) { send(rpcError(id, type, `Model not found: ${provider}/${modelId}`)); return; }
          await session.setModel(model);
          send(rpcSuccess(id, type, model));
          return;
        }
        case "cycle_model": {
          const model = await session.cycleModel();
          send(rpcSuccess(id, type, model ?? null));
          return;
        }
        case "get_available_models":
          send(rpcSuccess(id, type, { models: session.modelRuntime.getAvailableSnapshot() }));
          return;
        case "set_thinking_level":
          session.setThinkingLevel(requireString(command.level, "level"));
          send(rpcSuccess(id, type));
          return;
        case "cycle_thinking_level": {
          const level = session.cycleThinkingLevel();
          send(rpcSuccess(id, type, level ? { level } : null));
          return;
        }
        case "get_available_thinking_levels":
          send(rpcSuccess(id, type, { levels: session.getAvailableThinkingLevels() }));
          return;
        case "set_steering_mode":
          session.setSteeringMode(requireEnum(command.mode, ["all", "one-at-a-time"] as const, "mode"));
          send(rpcSuccess(id, type));
          return;
        case "set_follow_up_mode":
          session.setFollowUpMode(requireEnum(command.mode, ["all", "one-at-a-time"] as const, "mode"));
          send(rpcSuccess(id, type));
          return;
        case "compact":
          send(rpcSuccess(id, type, await session.compact(optionalString(command.customInstructions))));
          return;
        case "set_auto_compaction":
          session.setAutoCompactionEnabled(requireBoolean(command.enabled, "enabled"));
          send(rpcSuccess(id, type));
          return;
        case "set_auto_retry":
          session.setAutoRetryEnabled(requireBoolean(command.enabled, "enabled"));
          send(rpcSuccess(id, type));
          return;
        case "abort_retry":
          session.abortRetry();
          send(rpcSuccess(id, type));
          return;
        case "bash": {
          const bashCommand = requireString(command.command, "command");
          const excludeFromContext = command.excludeFromContext === true;
          const eventResult = await session.extensionRunner.emitUserBash({
            type: "user_bash",
            command: bashCommand,
            excludeFromContext,
            cwd: session.sessionManager.getCwd(),
          });
          if (eventResult?.result) {
            session.recordBashResult(bashCommand, eventResult.result, { excludeFromContext });
            send(rpcSuccess(id, type, eventResult.result));
            return;
          }
          const result = await session.executeBash(bashCommand, undefined, {
            excludeFromContext,
            id,
            operations: eventResult?.operations,
          });
          send(rpcSuccess(id, type, result));
          return;
        }
        case "abort_bash":
          session.abortBash();
          send(rpcSuccess(id, type));
          return;
        case "get_session_stats":
          send(rpcSuccess(id, type, session.getSessionStats()));
          return;
        case "export_html":
          send(rpcSuccess(id, type, { path: await session.exportToHtml(optionalString(command.outputPath)) }));
          return;
        case "switch_session": {
          const result = await this.runtime.switchSession(requireString(command.sessionPath, "sessionPath"));
          if (!result.cancelled) await this.bind();
          send(rpcSuccess(id, type, result));
          return;
        }
        case "fork": {
          const result = await this.runtime.fork(requireString(command.entryId, "entryId"));
          if (!result.cancelled) await this.bind();
          send(rpcSuccess(id, type, { text: result.selectedText, cancelled: result.cancelled }));
          return;
        }
        case "clone": {
          const leafId = session.sessionManager.getLeafId();
          if (!leafId) { send(rpcError(id, type, "Cannot clone session: no current entry selected")); return; }
          const result = await this.runtime.fork(leafId, { position: "at" });
          if (!result.cancelled) await this.bind();
          send(rpcSuccess(id, type, { cancelled: result.cancelled }));
          return;
        }
        case "get_fork_messages":
          send(rpcSuccess(id, type, { messages: session.getUserMessagesForForking() }));
          return;
        case "get_entries": {
          let entries = session.sessionManager.getEntries();
          if (command.since !== undefined) {
            const since = requireString(command.since, "since");
            const index = entries.findIndex((entry) => entry.id === since);
            if (index < 0) { send(rpcError(id, type, `Entry not found: ${since}`)); return; }
            entries = entries.slice(index + 1);
          }
          send(rpcSuccess(id, type, { entries, leafId: session.sessionManager.getLeafId() }));
          return;
        }
        case "get_tree":
          send(rpcSuccess(id, type, { tree: session.sessionManager.getTree(), leafId: session.sessionManager.getLeafId() }));
          return;
        case "get_last_assistant_text":
          send(rpcSuccess(id, type, { text: session.getLastAssistantText() }));
          return;
        case "set_session_name": {
          const name = requireString(command.name, "name").trim();
          if (!name) { send(rpcError(id, type, "Session name cannot be empty")); return; }
          session.setSessionName(name);
          send(rpcSuccess(id, type));
          return;
        }
        case "get_messages":
          send(rpcSuccess(id, type, { messages: session.messages }));
          return;
        case "get_commands":
          send(rpcSuccess(id, type, { commands: getCommands(session) }));
          return;
        default:
          send(rpcError(id, type, `Unknown command: ${type}`));
      }
    } catch (error) {
      send(rpcError(id, type, messageOf(error)));
    }
  }
}

function getCommands(session: RpcSession): RpcObject[] {
  const commands: RpcObject[] = [];
  for (const command of session.extensionRunner.getRegisteredCommands()) {
    commands.push({ name: command.invocationName, description: command.description, source: "extension", sourceInfo: command.sourceInfo });
  }
  for (const template of session.promptTemplates) {
    commands.push({ name: template.name, description: template.description, source: "prompt", sourceInfo: template.sourceInfo });
  }
  for (const skill of session.resourceLoader.getSkills().skills) {
    commands.push({ name: `skill:${skill.name}`, description: skill.description, source: "skill", sourceInfo: skill.sourceInfo });
  }
  return commands;
}

function rpcSuccess(id: string | undefined, command: string, data?: unknown): RpcResponse {
  const response: RpcResponse = { type: "response", command, success: true };
  if (id !== undefined) response.id = id;
  if (data !== undefined) response.data = data;
  return response;
}

function rpcError(id: string | undefined, command: string, error: string): RpcResponse {
  const response: RpcResponse = { type: "response", command, success: false, error };
  if (id !== undefined) response.id = id;
  return response;
}

function asRecord(value: unknown): RpcObject {
  if (value !== null && typeof value === "object" && !Array.isArray(value)) return value as RpcObject;
  return {};
}

function requireString(value: unknown, name: string): string {
  if (typeof value !== "string") throw new Error(`${name} must be a string`);
  return value;
}

function optionalString(value: unknown): string | undefined {
  if (value === undefined || value === null) return undefined;
  if (typeof value !== "string") throw new Error("expected a string");
  return value;
}

function requireBoolean(value: unknown, name: string): boolean {
  if (typeof value !== "boolean") throw new Error(`${name} must be a boolean`);
  return value;
}

function requireNumber(value: unknown, name: string): number {
  if (typeof value !== "number" || !Number.isInteger(value)) throw new Error(`${name} must be an integer`);
  return value;
}

function requireEnum<const T extends readonly string[]>(value: unknown, choices: T, name: string): T[number] {
  if (typeof value !== "string" || !choices.includes(value)) throw new Error(`${name} must be one of ${choices.join(", ")}`);
  return value as T[number];
}

function optionalEnum<const T extends readonly string[]>(value: unknown, choices: T): T[number] | undefined {
  return value === undefined || value === null ? undefined : requireEnum(value, choices, "streamingBehavior");
}

function optionalArray(value: unknown): unknown[] | undefined {
  if (value === undefined || value === null) return undefined;
  if (!Array.isArray(value)) throw new Error("images must be an array");
  return value;
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
