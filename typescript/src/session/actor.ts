import { randomBytes } from "node:crypto";
import {
  boolField,
  dataField,
  decodeRecord,
  field,
  isGatewayType,
  isInternalId,
  namespaceId,
  parsePiState,
  response,
  rewriteCommand,
  rewriteId,
  type ClientRef,
  type ClientSummary,
  type PiState,
  type ProtocolRecord,
} from "../protocol.ts";
import { PromptQueue, type QueuedItem } from "../queue.ts";
import { parsePiArgs, PiArgsSpec } from "../piargs.ts";
import { SdkRpcAdapter, type RpcObject, type RpcSessionRuntime } from "../rpc-adapter.ts";
import { createPiSdkRuntime, type PiSdkRuntimeOptions } from "../sdk-runtime.ts";
import { Hub, type Subscriber, type SubOptions } from "./hub.ts";
import { nopLogger, type GatewayLogger } from "../logging.ts";

export const SESSION_STARTING = "starting";
export const SESSION_READY = "ready";
export const SESSION_RESTARTING = "restarting";
export const SESSION_STOPPING = "stopping";
export const SESSION_HIBERNATED = "hibernated";
export const SESSION_STOPPED = "stopped";
export const SESSION_CRASHED = "crashed";
export const SESSION_DELETED = "deleted";
export const TURN_IDLE = "idle";
export const TURN_RUNNING = "running";
export const INTERNAL_ID_PREFIX = "gwint:";

const globalMutations = new Set([
  "set_model", "cycle_model", "set_thinking_level", "set_steering_mode", "set_follow_up_mode", "compact",
  "set_auto_compaction", "set_auto_retry", "set_session_name", "gw_reload_session",
]);
const dialogMethods = new Set(["select", "confirm", "input", "editor"]);

export interface ActorClient {
  id: string;
  kind: string;
  name: string;
  has(capability: string): boolean;
  ref(): ClientRef;
  summary(): ClientSummary;
}

export interface ActorClientCommand {
  client: ActorClient;
  localId: string;
  type: string;
  raw: Buffer;
}

export interface RuntimeCommand {
  command: string;
  raw: Buffer;
}

export interface ActorOptions extends Omit<PiSdkRuntimeOptions, "cwd" | "sessionPath"> {
  cwd: string;
  sessionPath?: string;
  sessionId?: string;
  hubCapacity?: number;
  idleTimeoutMs?: number;
  shortGraceMs?: number;
  stopGraceMs?: number;
  metric?: (name: string) => void;
  logger?: GatewayLogger;
  createRuntime?: (options: PiSdkRuntimeOptions) => Promise<RpcSessionRuntime>;
  onPath?: (actor: SessionActor, path: string) => void | Promise<void>;
  onStopped?: (actor: SessionActor, reason: string) => void | Promise<void>;
  retire?: (actor: SessionActor, retire: () => Promise<void>) => boolean | Promise<boolean>;
}

export interface ActorInfo {
  path: string;
  state: string;
  headSeq: number;
  oldestSeq: number;
  subscribers: number;
  turn: { state: string; turnId?: string; author?: ClientRef; queued: number };
  clients: ClientSummary[];
}

export interface StopSessionRequest { force: boolean; deleteFile: boolean; requester: string }
export interface StopSessionResult { path: string; name: string; sessionId: string; piStopped: boolean; detachedClients: number }
export interface RestartSessionRequest {
  force: boolean; requester: string; by?: ClientRef; replacement?: PiArgsSpec; additiveSpawnArgs?: string[]; timeoutMs?: number;
}
export class ActorRestartError extends Error {
  readonly code: string;
  constructor(code: string, message: string) { super(message); this.code = code; }
}
export class ActorStopError extends Error {
  readonly code: string;
  constructor(code: string, message: string) { super(message); this.code = code; }
}

interface PendingUI { winner: string; raw: Buffer }
interface PendingCall { resolve(record: ProtocolRecord | undefined): void; timer: ReturnType<typeof setTimeout> }
interface RuntimeCall { command: string; by?: ClientRef }

/** Owns one SDK session, its ordered event log, and the daemon FIFO. */
export class SessionActor {
  readonly hub: Hub;
  readonly queue = new PromptQueue();
  readonly clients = new Map<string, ActorClient>();
  readonly owners = new Map<string, string>();
  readonly pendingUI = new Map<string, PendingUI>();
  readonly queueRuns = new Map<string, ClientRef>();
  readonly runtimeCalls = new Map<string, RuntimeCall>();
  readonly clearPending = new Map<string, QueuedItem[]>();
  readonly forkPending = new Set<string>();
  private readonly internalCallbacks = new Map<string, (record: ProtocolRecord) => void>();
  private readonly pendingCalls = new Map<string, PendingCall>();
  private readonly lastActive = new Map<string, number>();
  private readonly options: ActorOptions;
  private readonly recordMetric: (name: string) => void;
  private readonly log: GatewayLogger;
  private runtime?: RpcSessionRuntime;
  private rpc?: SdkRpcAdapter;
  private serial: Promise<void> = Promise.resolve();
  private idleTimer?: ReturnType<typeof setTimeout>;
  private idleDeadline?: number;
  private runtimeDispatchTail: Promise<void> = Promise.resolve();
  private runtimeDisposal?: Promise<void>;
  private stopPromise?: Promise<void>;
  private lifecycleGeneration = 0;
  private nextInternalId = 0;
  private turnSequence = 0;
  private turnState = TURN_IDLE;
  private turnId = "";
  private turnAuthor?: ClientRef;
  private stopReason = "";
  private stateValue = SESSION_STARTING;
  private pathValue: string;
  private sessionNameValue = "";
  private sessionIdValue: string;
  private runtimeState?: PiState;
  private hasMessages = false;
  private stopped = false;
  private readonly settleWaiters = new Set<() => void>();

  constructor(options: ActorOptions) {
    this.options = options;
    this.pathValue = options.sessionPath ?? "";
    this.sessionIdValue = options.sessionId ?? options.sessionPath ?? `s_${Date.now()}_${randomBytes(4).toString("hex")}`;
    this.recordMetric = options.metric ?? (() => {});
    this.log = options.logger ?? nopLogger();
    this.hub = new Hub(options.hubCapacity ?? 4096, this.sessionIdValue, this.recordMetric);
  }

  async start(): Promise<void> {
    if (this.runtime) throw new Error("session actor already started");
    const generation = this.lifecycleGeneration;
    try {
      await this.startRuntime(generation);
      if (this.stopped || generation !== this.lifecycleGeneration) throw new Error("session stopped during startup");
      this.stateValue = SESSION_READY;
      this.publishState(SESSION_READY);
      this.reevaluateIdle();
    } catch (error) {
      if (this.stopped || generation !== this.lifecycleGeneration) throw error;
      this.stateValue = SESSION_CRASHED;
      this.stopReason = "startup failed";
      this.publishState(SESSION_CRASHED, messageOf(error));
      await this.options.onStopped?.(this, this.stopReason);
      throw error;
    }
  }

  get path(): string { return this.pathValue; }
  get cwd(): string { return this.options.cwd; }
  get state(): string { return this.stateValue; }
  get sessionName(): string { return this.sessionNameValue; }
  get sessionId(): string { return this.sessionIdValue; }
  get attached(): number { return this.clients.size; }
  get live(): boolean { return [SESSION_READY, SESSION_STARTING, SESSION_RESTARTING].includes(this.stateValue); }
  get turn(): { state: string; turnId?: string; author?: ClientRef; queued: number } {
    const value: { state: string; turnId?: string; author?: ClientRef; queued: number } = { state: this.turnState, queued: this.queue.length };
    if (this.turnId) value.turnId = this.turnId;
    if (this.turnAuthor) value.author = this.turnAuthor;
    return value;
  }

  subscribe(clientId: string, options: SubOptions): Subscriber { return this.hub.subscribe(clientId, options); }
  unsubscribe(clientId: string): void { this.hub.unsubscribe(clientId); }
  replay(afterSeq: number): { records: ProtocolRecord[]; ok: boolean } { return this.hub.replay(afterSeq); }

  attach(client: ActorClient): boolean {
    if (this.stopped) return false;
    void this.attachSync(client).catch(() => {});
    return true;
  }

  async attachSync(client: ActorClient): Promise<boolean> {
    if (this.stopped) return false;
    await this.enqueue(() => {
      if (this.clients.has(client.id)) return;
      this.clients.set(client.id, client);
      this.lastActive.set(client.id, Date.now());
      this.publishPresence("join", client);
      this.reevaluateIdle();
    });
    return !this.stopped;
  }

  detach(clientId: string): boolean {
    if (this.stopped) return false;
    void this.enqueue(async () => {
      const client = this.clients.get(clientId);
      if (!client) return;
      this.clients.delete(clientId);
      this.lastActive.delete(clientId);
      this.publishPresence("leave", client);
      this.reassignUI(clientId);
      if (this.clients.size === 0 && [SESSION_CRASHED, SESSION_STOPPED].includes(this.stateValue)) {
        await this.retireDetachedTerminal();
        return;
      }
      this.reevaluateIdle();
    }).catch(() => {});
    return true;
  }

  submit(command: ActorClientCommand): Promise<void> {
    return this.enqueue(async () => {
      if (!this.rpc || this.stateValue !== SESSION_READY) {
        if (command.type === "prompt" || command.type === "follow_up") this.metric("prompts_rejected");
        this.errorResponse(command, "session_crashed", "session is not running");
        return;
      }
      this.lastActive.set(command.client.id, Date.now());
      switch (command.type) {
        case "prompt":
        case "follow_up":
          await this.handlePrompt(command);
          return;
        case "steer":
        case "abort":
          await this.forward(command);
          return;
        case "clear_queue":
          await this.handleClearQueue(command);
          return;
        case "switch_session":
        case "new_session":
          this.errorResponse(command, "not_supported", `${command.type} is handled by the daemon`);
          return;
        case "fork":
        case "clone":
          await this.handleFork(command);
          return;
        case "extension_ui_response":
          await this.handleUIResponse(command);
          return;
        default:
          if (isGatewayType(command.type)) {
            this.errorResponse(command, "bad_frame", `unknown gateway message ${JSON.stringify(command.type)}`);
            return;
          }
          await this.forward(command);
      }
    });
  }

  async applyRuntime(commands: RuntimeCommand[], by?: ClientRef): Promise<void> {
    if (commands.length === 0) return;
    await this.enqueue(() => {
      const rpc = this.rpc;
      if (!rpc || this.stopped) return;
      for (const item of commands) {
        const id = this.internalId("r");
        const raw = rewriteId(item.raw, id);
        this.runtimeCalls.set(id, { command: item.command, by });
        this.runtimeDispatchTail = this.runtimeDispatchTail.then(async () => {
          if (this.stopped || this.rpc !== rpc) return;
          await rpc.handle(parseCommand(raw));
        }).catch((error: unknown) => {
          this.runtimeCalls.delete(id);
          this.log.warn("runtime parameter apply failed", "session", this.pathValue, "command", item.command, "err", messageOf(error));
        });
      }
    });
  }

  async call(command: RpcObject, timeoutMs = 5000): Promise<ProtocolRecord> {
    if (!this.rpc) throw new Error("session: pi is not running");
    const id = this.internalId("c");
    const raw = rewriteId(Buffer.from(JSON.stringify(command)), id);
    const recordPromise = new Promise<ProtocolRecord | undefined>((resolve) => {
      const timer = setTimeout(() => {
        this.pendingCalls.delete(id);
        resolve(undefined);
      }, timeoutMs);
      this.pendingCalls.set(id, { resolve, timer });
    });
    await this.rpc.handle(parseCommand(raw));
    const record = await recordPromise;
    if (!record) throw new Error("session: pi call timed out");
    return record;
  }

  async info(): Promise<ActorInfo> {
    return {
      path: this.pathValue,
      state: this.stateValue,
      headSeq: this.hub.headSeq,
      oldestSeq: this.hub.oldestSeq,
      subscribers: this.hub.subscriberCount,
      turn: this.turn,
      clients: [...this.clients.values()].map((client) => client.summary()),
    };
  }

  async snapshot(): Promise<RpcObject> {
    const state = await this.call({ type: "get_state" }, 10_000);
    const entries = await this.call({ type: "get_entries" }, 10_000);
    const stateData = dataField(state.raw);
    const entriesData = asObject(dataField(entries.raw));
    const snapshot: RpcObject = {
      type: "gw_snapshot",
      piState: stateData ?? null,
      entries: Array.isArray(entriesData.entries) ? entriesData.entries : [],
      headSeq: this.hub.headSeq,
    };
    if (typeof entriesData.leafId === "string" && entriesData.leafId) snapshot.leafId = entriesData.leafId;
    return snapshot;
  }

  async restart(request: RestartSessionRequest): Promise<PiArgsSpec | undefined> {
    const timeoutMs = request.timeoutMs ?? 30_000;
    const deadline = Date.now() + timeoutMs;
    const operation = this.enqueue(async () => {
      const generation = this.lifecycleGeneration;
      const ensureActive = () => {
        if (this.stopped || generation !== this.lifecycleGeneration) {
          throw new ActorRestartError("session_crashed", "session is stopping");
        }
      };
      ensureActive();
      const remaining = (): number => {
        const value = deadline - Date.now();
        if (value <= 0) throw new ActorRestartError("reload_timeout", "session reload timed out and was canceled");
        return value;
      };
      if (this.stateValue !== SESSION_READY && this.stateValue !== SESSION_CRASHED) {
        throw new ActorRestartError("reload_busy", "session is not ready to reload");
      }
      remaining();
      await this.runtimeDispatchTail;
      ensureActive();
      let otherClients = 0;
      for (const clientId of this.clients.keys()) if (clientId !== request.requester) otherClients++;
      const additive = request.additiveSpawnArgs !== undefined;
      if ((!request.force || additive) && (otherClients > 0 || this.turnState === TURN_RUNNING)) {
        this.metric("reloads_refused");
        throw new ActorRestartError("reload_busy", "a turn is running or another client is attached");
      }
      this.metric("reloads");
      this.stateValue = SESSION_RESTARTING;
      this.clearIdleTimer();
      this.publishState(SESSION_RESTARTING, "reload");
      let state = this.runtimeState ?? parsePiState(Buffer.from("{}"));
      if (additive && this.rpc) {
        let stateRecord: ProtocolRecord;
        try { stateRecord = await this.call({ type: "get_state" }, remaining()); }
        catch (error) {
          ensureActive();
          const timedOut = error instanceof ActorRestartError || messageOf(error).includes("timed out");
          this.stateValue = SESSION_READY;
          this.publishState(SESSION_READY, timedOut ? "reload timed out before restart" : "reload preflight failed");
          this.reevaluateIdle();
          throw new ActorRestartError(timedOut ? "reload_timeout" : "session_crashed",
            timedOut ? "session reload timed out and was canceled" : messageOf(error));
        }
        if (!boolField(stateRecord.raw, "success", false)) {
          this.stateValue = SESSION_READY;
          this.publishState(SESSION_READY, "reload preflight failed");
          this.reevaluateIdle();
          throw new ActorRestartError("session_crashed", "could not read runtime state before reload");
        }
        state = parsePiState(Buffer.from(JSON.stringify(dataField(stateRecord.raw) ?? {})));
      }
      const nextSpec = additive
        ? parsePiArgs([...runtimeArgsFromState(state), ...(request.additiveSpawnArgs ?? [])])
        : request.replacement ?? this.options.piArgs ?? new PiArgsSpec();
      for (const [id, author] of this.queueRuns) {
        this.queueRuns.delete(id);
        this.publishError(author, "session_crashed", "session reloaded");
      }
      if (this.turnState === TURN_RUNNING && this.rpc) this.startInternal("abort", () => {});
      this.failPendingCalls();
      this.failInternalCallbacks();
      this.failTurn();
      this.pendingUI.clear();
      this.forkPending.clear();
      this.runtimeCalls.clear();
      this.clearPending.clear();
      this.owners.clear();
      await this.disposeRuntime();
      ensureActive();
      let acceptRuntimeEvents = true;
      const bindingPromise = this.createRuntimeBinding(nextSpec, (event) => {
        if (acceptRuntimeEvents) this.onRpcEvent(event);
      });
      let bindingAssigned = false;
      let bindingDisposed = false;
      try {
        const binding = await withTimeout(bindingPromise, remaining());
        if (this.stopped || generation !== this.lifecycleGeneration) {
          bindingDisposed = true;
          await binding.rpc.dispose().catch(() => {});
          throw new ActorRestartError("session_crashed", "session is stopping");
        }
        this.runtime = binding.runtime;
        this.rpc = binding.rpc;
        bindingAssigned = true;
        const stateRecord = await this.call({ type: "get_state" }, remaining());
        ensureActive();
        if (!boolField(stateRecord.raw, "success", false)) throw new Error("replacement Pi did not report ready");
        this.adoptState(stateRecord.raw);
        this.options.piArgs = nextSpec;
        this.stateValue = SESSION_READY;
        this.publishState(SESSION_READY, "reload");
        const changed: RpcObject = { type: "gw_state_changed", command: "gw_reload_session", data: dataField(stateRecord.raw) ?? null };
        if (request.by) changed.by = request.by;
        this.hub.publish(asProtocolRecord(changed));
        if (this.queue.length > 0) void this.drainQueue();
        else this.reevaluateIdle();
        return additive || request.replacement !== undefined ? nextSpec : undefined;
      } catch (error) {
        acceptRuntimeEvents = false;
        if (!bindingAssigned && !bindingDisposed) void bindingPromise.then((binding) => binding.rpc.dispose()).catch(() => {});
        if (this.stopped || generation !== this.lifecycleGeneration) {
          throw new ActorRestartError("session_crashed", "session is stopping");
        }
        await this.disposeRuntime();
        ensureActive();
        this.stateValue = SESSION_CRASHED;
        this.publishState(SESSION_CRASHED, messageOf(error));
        throw new ActorRestartError(messageOf(error).includes("timed out") ? "reload_timeout" : "session_crashed", messageOf(error));
      }
    });
    // The queued operation owns the shared deadline and performs state cleanup
    // in its timeout path. Let that inner timer run before the outer guard fires.
    return withTimeout(operation, timeoutMs + 1).catch((error: unknown) => {
      if (error instanceof ActorRestartError) throw error;
      if (messageOf(error).includes("timed out")) {
        throw new ActorRestartError("reload_timeout", "session reload timed out and was canceled");
      }
      throw error;
    });
  }

  async stopSession(request: StopSessionRequest, graceMs = this.options.stopGraceMs ?? 5_000): Promise<StopSessionResult> {
    return this.enqueue(async () => {
      if (![SESSION_READY, SESSION_CRASHED, SESSION_RESTARTING].includes(this.stateValue)) {
        throw new ActorStopError("session_crashed", "session is not running");
      }
      let otherClients = 0;
      for (const clientId of this.clients.keys()) if (clientId !== request.requester) otherClients++;
      if (this.turnState === TURN_RUNNING && !request.force) {
        throw new ActorStopError("session_busy", "a turn is running");
      }
      if (!request.deleteFile && !request.force && otherClients > 0) {
        throw new ActorStopError("session_attached", "other clients are attached");
      }
      const result: StopSessionResult = {
        path: this.pathValue, name: this.sessionNameValue, sessionId: this.sessionIdValue,
        piStopped: this.runtime !== undefined, detachedClients: otherClients,
      };
      const reason = request.force ? "forced" : "requested";
      const terminalState = request.deleteFile ? SESSION_DELETED : SESSION_STOPPED;
      this.discardQueue();
      for (const client of this.clients.values()) this.publishPresence("leave", client);
      this.clients.clear();
      this.lastActive.clear();
      if (this.turnState === TURN_RUNNING && request.force && this.rpc) {
        this.startInternal("abort", () => {});
        await this.waitForTurnSettle(graceMs);
      }
      await this.stop(reason, terminalState, true);
      return result;
    });
  }

  async stop(reason = SESSION_STOPPED, terminalState = SESSION_STOPPED, queuedAlreadyFailed = false): Promise<void> {
    if (this.stopPromise) return this.stopPromise;
    if (this.stopped) return;
    this.stopped = true;
    this.lifecycleGeneration++;
    this.clearIdleTimer();
    this.stopReason = reason;
    this.stateValue = SESSION_STOPPING;
    this.stopPromise = (async () => {
      await this.disposeRuntime();
      if (!queuedAlreadyFailed) this.failQueued("session_crashed", `session ${reason}`);
      this.failPendingCalls();
      this.failInternalCallbacks();
      this.stateValue = terminalState;
      this.publishState(terminalState, reason);
      this.hub.closeAll();
      await this.options.onStopped?.(this, reason);
    })();
    return this.stopPromise;
  }

  private enqueue<T = void>(operation: () => T | Promise<T>): Promise<T> {
    const result = this.serial.then(operation);
    this.serial = result.then(() => {}, () => {});
    return result;
  }

  private async disposeRuntime(): Promise<void> {
    if (this.runtimeDisposal) return this.runtimeDisposal;
    const rpc = this.rpc;
    const runtime = this.runtime;
    this.rpc = undefined;
    this.runtime = undefined;
    const disposal = (async () => {
      if (rpc) await rpc.dispose().catch(() => {});
      else await runtime?.dispose().catch(() => {});
    })();
    this.runtimeDisposal = disposal;
    try { await disposal; }
    finally { if (this.runtimeDisposal === disposal) this.runtimeDisposal = undefined; }
  }

  private async createRuntimeBinding(
    piArgs = this.options.piArgs,
    onEvent: (event: RpcObject) => void = (event) => this.onRpcEvent(event),
  ): Promise<{ runtime: RpcSessionRuntime; rpc: SdkRpcAdapter }> {
    const runtime = await (this.options.createRuntime ?? createPiSdkRuntime)({
      piBin: this.options.piBin,
      env: this.options.env,
      cwd: this.options.cwd,
      sessionPath: this.pathValue || undefined,
      sessionDir: this.options.sessionDir,
      agentDir: this.options.agentDir,
      piArgs,
    });
    const rpc = new SdkRpcAdapter(runtime, onEvent);
    try { await rpc.bind(); }
    catch (error) { await rpc.dispose().catch(() => {}); throw error; }
    return { runtime, rpc };
  }

  private async startRuntime(generation: number, piArgs = this.options.piArgs): Promise<void> {
    const binding = await this.createRuntimeBinding(piArgs);
    if (this.stopped || generation !== this.lifecycleGeneration) {
      await binding.rpc.dispose().catch(() => {});
      throw new Error("session stopped during startup");
    }
    this.runtime = binding.runtime;
    this.rpc = binding.rpc;
  }

  private async handlePrompt(command: ActorClientCommand): Promise<void> {
    const raw = rewriteCommand(command.raw, command.localId, "prompt");
    const item: QueuedItem = {
      clientId: command.client.id,
      kind: command.client.kind,
      name: command.client.name,
      localId: command.localId,
      type: "prompt",
      raw,
      message: field(command.raw, "message"),
    };
    if (this.stateValue !== SESSION_READY) {
      this.errorResponse(command, "session_crashed", "session is not running");
      return;
    }
    if (this.turnState === TURN_IDLE && this.queue.length === 0) {
      await this.forwardItem(item, command.client, command.localId);
      return;
    }
    if (!this.queue.tryPush(item, command.raw.length)) {
      this.errorResponse(command, "queue_full", "pending prompt queue limit reached");
      return;
    }
    this.metric("prompts_queued");
    this.respond(command, true, { queued: true });
    this.publishQueue();
  }

  private async forwardItem(item: QueuedItem, client: ActorClient, localId: string): Promise<void> {
    const id = namespaceId(client.id, localId);
    const raw = rewriteId(item.raw, id);
    this.owners.set(id, client.id);
    this.startTurn(refFromItem(item));
    const rpc = this.rpc!;
    void rpc.handle(parseCommand(raw)).catch((error: unknown) => {
      if (this.rpc === rpc) this.errorResponse({ client, localId, type: "prompt", raw: item.raw }, "session_crashed", messageOf(error));
    });
  }

  private async handleClearQueue(command: ActorClientCommand): Promise<void> {
    const cleared = this.queue.clear();
    if (cleared.length > 0) this.publishQueue();
    if (this.stateValue !== SESSION_READY || !this.rpc) {
      this.errorResponse(command, "session_crashed", "session is not running");
      return;
    }
    const id = namespaceId(command.client.id, command.localId);
    if (cleared.length > 0) this.clearPending.set(id, cleared);
    this.owners.set(id, command.client.id);
    await this.rpc.handle(parseCommand(rewriteId(command.raw, id)));
  }

  private async forward(command: ActorClientCommand): Promise<void> {
    const rpc = this.rpc;
    if (!rpc || this.stateValue !== SESSION_READY) {
      this.errorResponse(command, "session_crashed", "session is not running");
      return;
    }
    const id = namespaceId(command.client.id, command.localId);
    this.owners.set(id, command.client.id);
    void rpc.handle(parseCommand(rewriteId(command.raw, id))).catch((error: unknown) => {
      this.owners.delete(id);
      if (this.rpc === rpc) this.errorResponse(command, "session_crashed", messageOf(error));
    });
  }

  private async handleFork(command: ActorClientCommand): Promise<void> {
    if (this.clients.size > 1) {
      this.errorResponse(command, "shared_session", `${command.type} is unavailable while other clients are attached`);
      return;
    }
    if (!this.rpc || this.stateValue !== SESSION_READY) {
      this.errorResponse(command, "session_crashed", "session is not running");
      return;
    }
    const id = namespaceId(command.client.id, command.localId);
    this.owners.set(id, command.client.id);
    this.forkPending.add(id);
    const rpc = this.rpc;
    void rpc.handle(parseCommand(rewriteId(command.raw, id))).catch((error: unknown) => {
      this.owners.delete(id);
      this.forkPending.delete(id);
      if (this.rpc === rpc) this.errorResponse(command, "session_crashed", messageOf(error));
    });
  }

  private async handleUIResponse(command: ActorClientCommand): Promise<void> {
    if (!this.rpc || this.stateValue !== SESSION_READY) {
      this.errorResponse(command, "session_crashed", "session is not running");
      return;
    }
    const pending = this.pendingUI.get(command.localId);
    if (!pending) {
      this.metric("ui_stale");
      this.errorResponse(command, "ui_stale", "extension UI request is no longer pending");
      return;
    }
    if (pending.winner !== command.client.id) {
      this.metric("ui_stale");
      this.errorResponse(command, "ui_stale", "extension UI request was routed to another client");
      return;
    }
    this.metric("ui_answered");
    this.pendingUI.delete(command.localId);
    this.owners.set(command.localId, command.client.id);
    await this.rpc.handle(parseCommand(rewriteId(command.raw, command.localId)));
  }

  private startTurn(author?: ClientRef): void {
    this.metric("turns_started");
    this.turnState = TURN_RUNNING;
    this.turnSequence++;
    this.turnId = `t_${this.turnSequence}`;
    this.turnAuthor = author;
    this.hasMessages = true;
    this.publishTurn(TURN_RUNNING);
    this.reevaluateIdle();
  }

  private onSettled(): void {
    if (this.turnState === TURN_RUNNING) {
      this.metric("turns_settled");
      this.turnState = TURN_IDLE;
      this.publishTurn("settled");
    }
    for (const resolve of this.settleWaiters) resolve();
    this.settleWaiters.clear();
    if (this.queue.length > 0 && this.stateValue === SESSION_READY) {
      void this.drainQueue();
      return;
    }
    this.turnAuthor = undefined;
    this.reevaluateIdle();
  }

  private async drainQueue(): Promise<void> {
    const item = this.queue.pop();
    if (!item) return;
    const id = this.internalId("q");
    const raw = rewriteCommand(item.raw, id, "prompt");
    const author = refFromItem(item);
    this.queueRuns.set(id, author);
    this.publishQueue();
    this.startTurn(author);
    try {
      await this.rpc!.handle(parseCommand(raw));
    } catch (error) {
      this.queueRuns.delete(id);
      this.log.warn("queued prompt failed", "session", this.pathValue, "err", error);
      this.publishError(author, "session_crashed", messageOf(error));
      this.publishQueue();
      this.failTurn();
    }
  }

  private onRpcEvent(event: RpcObject): void {
    if (event.type === "gw_runtime_failure") {
      const reason = typeof event.error === "string" && event.error ? event.error : "Pi runtime failed";
      void this.enqueue(() => this.handleRuntimeFailure(reason));
      return;
    }
    const serialized = JSON.stringify(event);
    if (serialized === undefined) return;
    let record = decodeRecord(Buffer.from(serialized, "utf8"));
    if (record.type === "response" && field(record.raw, "command") === "get_state") this.adoptState(record.raw);
    if (record.type === "response" && isInternalId(record.id)) {
      this.handleInternalResponse(record);
      return;
    }
    if (record.id) {
      const owner = this.owners.get(record.id);
      if (owner !== undefined) {
        record = { ...record, owner };
        if (record.type === "response") this.owners.delete(record.id);
      }
    }
    if (record.type === "response") {
      const cleared = this.clearPending.get(record.id);
      if (cleared) {
        this.clearPending.delete(record.id);
        record = mergeClearedQueue(record, cleared) ?? record;
      }
    }
    if (record.type === "extension_ui_request" && !this.routeUI(record)) return;
    const published = this.hub.publish(record);
    this.postProcess(published);
  }

  private handleInternalResponse(record: ProtocolRecord): void {
    const callback = this.internalCallbacks.get(record.id);
    if (callback) {
      this.internalCallbacks.delete(record.id);
      callback(record);
      return;
    }
    const pending = this.pendingCalls.get(record.id);
    if (pending) {
      clearTimeout(pending.timer);
      this.pendingCalls.delete(record.id);
      pending.resolve(record);
      return;
    }
    const author = this.queueRuns.get(record.id);
    if (author) {
      this.queueRuns.delete(record.id);
      if (!boolField(record.raw, "success", true)) {
        this.publishError(author, "session_crashed", field(record.raw, "error") || "queued prompt failed");
        this.failTurn();
      }
      return;
    }
    const runtimeCall = this.runtimeCalls.get(record.id);
    if (runtimeCall) {
      this.runtimeCalls.delete(record.id);
      if (!boolField(record.raw, "success", false)) {
        this.log.warn("runtime parameter apply failed", "session", this.pathValue, "command", runtimeCall.command,
          "err", field(record.raw, "error") || "runtime command failed");
        return;
      }
      this.updateRuntimeState(runtimeCall.command, record.raw);
      const event: RpcObject = { type: "gw_state_changed", command: runtimeCall.command, data: dataField(record.raw) ?? null };
      if (runtimeCall.by) event.by = runtimeCall.by;
      this.hub.publish(asProtocolRecord(event));
    }
  }

  private async handleRuntimeFailure(reason: string): Promise<void> {
    if (this.stopped || this.stateValue !== SESSION_READY) return;
    this.metric("pi_exits");
    this.stateValue = SESSION_CRASHED;
    this.publishState(SESSION_CRASHED, reason);
    this.failQueued("session_crashed", "pi exited");
    this.failPendingCalls();
    this.failInternalCallbacks();
    this.pendingUI.clear();
    this.forkPending.clear();
    this.runtimeCalls.clear();
    this.clearPending.clear();
    this.owners.clear();
    this.failTurn();
    await this.disposeRuntime();
    if (this.clients.size === 0) await this.retireDetachedTerminal();
  }

  private postProcess(record: ProtocolRecord): void {
    switch (record.type) {
      case "agent_start":
        if (this.turnState !== TURN_RUNNING) this.startTurn();
        return;
      case "agent_settled":
        this.onSettled();
        return;
      case "message_start":
      case "message_update":
      case "message_end":
      case "tool_execution_start":
      case "tool_execution_update":
      case "tool_execution_end":
        this.hasMessages = true;
        return;
      case "response": {
        const command = field(record.raw, "command");
        const success = boolField(record.raw, "success", false);
        if (!success && ["prompt", "follow_up"].includes(command)) this.failTurn();
        if (["fork", "clone"].includes(command) && this.forkPending.delete(record.id) && success) {
          this.startInternal("get_state", (responseRecord) => this.adoptState(responseRecord.raw));
        }
        if (success) {
          this.updateRuntimeState(command, record.raw);
          if (globalMutations.has(command)) this.publishStateChanged(command, record.raw, record.owner);
        }
      }
    }
  }

  private failTurn(): void {
    if (this.turnState !== TURN_RUNNING) return;
    this.turnState = TURN_IDLE;
    this.turnAuthor = undefined;
    this.publishTurn("settled");
    if (this.queue.length > 0 && this.stateValue === SESSION_READY) {
      void this.drainQueue();
      return;
    }
    this.reevaluateIdle();
  }

  private adoptState(raw: Uint8Array): void {
    const data = dataField(raw);
    if (data === undefined) return;
    const state = parsePiState(Buffer.from(JSON.stringify(data)));
    this.runtimeState = state;
    if (state.sessionName) this.sessionNameValue = state.sessionName;
    if (state.sessionId) this.sessionIdValue = state.sessionId;
    if (state.sessionFile && state.sessionFile !== this.pathValue) {
      this.pathValue = state.sessionFile;
      this.hub.setSession(this.pathValue);
      void this.options.onPath?.(this, state.sessionFile);
    }
  }

  private updateRuntimeState(command: string, raw: Uint8Array): void {
    const data = asObject(dataField(raw));
    if (!this.runtimeState) this.runtimeState = parsePiState(Buffer.from("{}"));
    if (command === "set_model" || command === "cycle_model") {
      const model = asObject(data.model);
      const runtimeModel = this.runtime?.session.model ?? {};
      const modelId = stringProp(model, "id") ?? stringProp(data, "modelId") ?? stringProp(data, "id") ?? stringProp(runtimeModel, "id");
      const provider = stringProp(model, "provider") ?? stringProp(data, "provider") ?? stringProp(runtimeModel, "provider");
      if (modelId) this.runtimeState.model.id = modelId;
      if (provider) this.runtimeState.model.provider = provider;
      const thinkingLevel = stringProp(data, "thinkingLevel");
      if (thinkingLevel) this.runtimeState.thinkingLevel = thinkingLevel;
    } else if (command === "set_thinking_level" || command === "cycle_thinking_level") {
      const level = stringProp(data, "thinkingLevel") ?? stringProp(data, "level") ?? this.runtime?.session.thinkingLevel;
      if (level) this.runtimeState.thinkingLevel = level;
    } else if (command === "set_session_name") {
      const name = stringProp(data, "name") ?? stringProp(data, "sessionName") ?? this.runtime?.session.sessionName ?? undefined;
      if (name !== undefined) {
        this.runtimeState.sessionName = name;
        this.sessionNameValue = name;
      }
    }
  }

  private routeUI(record: ProtocolRecord): boolean {
    if (!dialogMethods.has(field(record.raw, "method"))) return true;
    this.metric("ui_requests");
    const winner = this.pickUIWinner();
    if (!winner) {
      this.metric("ui_unroutable");
      return false;
    }
    record.owner = winner;
    if (record.id) this.pendingUI.set(record.id, { winner, raw: record.raw });
    return true;
  }

  private pickUIWinner(): string {
    if (this.turnAuthor) {
      const author = this.clients.get(this.turnAuthor.clientId);
      if (author?.has("ui")) return author.id;
    }
    let best = "";
    let bestActive = -1;
    for (const [id, client] of this.clients) {
      if (!client.has("ui")) continue;
      const active = this.lastActive.get(id) ?? 0;
      if (!best || active > bestActive) { best = id; bestActive = active; }
    }
    return best;
  }

  private reassignUI(clientId: string): void {
    for (const [id, pending] of this.pendingUI) {
      if (pending.winner !== clientId) continue;
      const next = this.pickUIWinner();
      if (next) {
        pending.winner = next;
        const record = decodeRecord(pending.raw);
        this.hub.publish({ ...record, owner: next });
      } else {
        this.pendingUI.delete(id);
      }
    }
  }

  private startInternal(command: string, callback: (record: ProtocolRecord) => void): string {
    if (!this.rpc) return "";
    const id = this.internalId("c");
    this.internalCallbacks.set(id, callback);
    void this.rpc.handle({ type: command, id });
    return id;
  }

  private async waitForTurnSettle(timeoutMs: number): Promise<void> {
    if (this.turnState !== TURN_RUNNING || timeoutMs <= 0) return;
    await new Promise<void>((resolveWait) => {
      let finished = false;
      const finish = () => {
        if (finished) return;
        finished = true;
        clearTimeout(timer);
        this.settleWaiters.delete(finish);
        resolveWait();
      };
      const timer = setTimeout(finish, timeoutMs);
      this.settleWaiters.add(finish);
      if (this.turnState !== TURN_RUNNING) finish();
    });
  }

  private failQueued(code: string, message: string): void {
    for (const item of this.queue.clear()) this.publishError(refFromItem(item), code, message);
    for (const [id, author] of this.queueRuns) {
      this.queueRuns.delete(id);
      this.publishError(author, code, message);
    }
    this.publishQueue();
  }

  private discardQueue(): void {
    this.queue.clear();
    this.queueRuns.clear();
    this.publishQueue();
  }

  private failPendingCalls(): void {
    for (const [id, pending] of this.pendingCalls) {
      clearTimeout(pending.timer);
      pending.resolve(undefined);
      this.pendingCalls.delete(id);
    }
  }

  private failInternalCallbacks(): void { this.internalCallbacks.clear(); }

  private reevaluateIdle(): void {
    if (this.stateValue !== SESSION_READY || this.stopped) return;
    const idle = this.clients.size === 0 && this.turnState === TURN_IDLE && this.queue.length === 0;
    if (!idle) {
      this.clearIdleTimer();
      return;
    }
    if (this.idleTimer) return;
    const requestedDelay = this.hasMessages ? this.options.idleTimeoutMs : this.options.shortGraceMs;
    const delay = requestedDelay !== undefined && requestedDelay > 0 ? requestedDelay : 60_000;
    this.armIdleTimer(delay);
  }

  private async handleIdle(): Promise<void> {
    if (this.stateValue !== SESSION_READY || this.turnState === TURN_RUNNING || this.clients.size > 0) {
      this.reevaluateIdle();
      return;
    }
    const retire = async () => {
      if (this.stopped) return;
      this.stopReason = SESSION_HIBERNATED;
      this.publishState(SESSION_HIBERNATED, "idle timeout");
      await this.stop(SESSION_HIBERNATED, SESSION_HIBERNATED);
    };
    if (this.options.retire) {
      if (!(await this.options.retire(this, retire))) this.reevaluateIdle();
      return;
    }
    await retire();
  }

  private armIdleTimer(delay: number): void {
    const deadline = Date.now() + delay;
    this.idleDeadline = deadline;
    const tick = () => {
      if (this.stopped || this.idleDeadline !== deadline) return;
      const remaining = deadline - Date.now();
      if (remaining > 0) {
        this.idleTimer = setTimeout(tick, Math.min(remaining, 2_147_483_647));
        return;
      }
      this.idleTimer = undefined;
      this.idleDeadline = undefined;
      void this.handleIdle();
    };
    this.idleTimer = setTimeout(tick, Math.min(delay, 2_147_483_647));
  }

  private clearIdleTimer(): void {
    if (this.idleTimer) clearTimeout(this.idleTimer);
    this.idleTimer = undefined;
    this.idleDeadline = undefined;
  }

  private async retireDetachedTerminal(): Promise<void> {
    if (this.stopped || this.clients.size > 0 || ![SESSION_CRASHED, SESSION_STOPPED].includes(this.stateValue)) return;
    const retire = async () => {
      if (this.stopped || this.clients.size > 0) return;
      this.stopped = true;
      this.lifecycleGeneration++;
      this.clearIdleTimer();
      await this.disposeRuntime();
      this.failPendingCalls();
      this.failInternalCallbacks();
      this.hub.closeAll();
      await this.options.onStopped?.(this, this.stopReason || this.stateValue);
    };
    if (this.options.retire) await this.options.retire(this, retire);
    else await retire();
  }

  private respond(command: ActorClientCommand, success: boolean, data?: unknown): void {
    this.publishOwnedResponse(command.client.id, response(command.localId, command.type, success, "", "", data));
  }

  private errorResponse(command: ActorClientCommand, code: string, message: string): void {
    this.publishOwnedResponse(command.client.id, response(command.localId, command.type, false, code, message));
  }

  private publishOwnedResponse(owner: string, responseObject: RpcObject): void {
    this.hub.publish({ ...asProtocolRecord(responseObject), owner });
  }

  private publishError(author: ClientRef | undefined, code: string, message: string): void {
    if (!author) return;
    this.hub.publish(asProtocolRecord({ type: "gw_error", code, message }, author.clientId));
  }

  private publishTurn(state: string): void {
    const event: RpcObject = { type: "gw_turn", state, turnId: this.turnId };
    if (this.turnAuthor) event.author = this.turnAuthor;
    this.hub.publish(asProtocolRecord(event));
  }

  private publishQueue(): void { this.hub.publish(asProtocolRecord({ type: "gw_queue", pending: this.queue.pending() })); }

  private publishPresence(event: string, client: ActorClient): void {
    this.hub.publish(asProtocolRecord({ type: "gw_presence", event, client: client.ref() }));
  }

  private publishState(state: string, reason?: string): void {
    const event: RpcObject = { type: "gw_session_state", state };
    if (reason) event.reason = reason;
    this.hub.publish(asProtocolRecord(event));
  }

  private publishStateChanged(command: string, raw: Uint8Array, owner: string): void {
    const event: RpcObject = { type: "gw_state_changed", command, data: dataField(raw) ?? null };
    const client = this.clients.get(owner);
    if (client) event.by = client.ref();
    this.hub.publish(asProtocolRecord(event));
  }

  private internalId(kind: string): string { this.nextInternalId++; return `${INTERNAL_ID_PREFIX}${kind}${this.nextInternalId}`; }
  private metric(name: string): void { this.recordMetric(name); }
}

function mergeClearedQueue(record: ProtocolRecord, cleared: QueuedItem[]): ProtocolRecord | undefined {
  if (!boolField(record.raw, "success", false)) return undefined;
  const data = asObject(dataField(record.raw));
  const followUp = Array.isArray(data.followUp) ? [...data.followUp] : [];
  for (const item of cleared) followUp.push(item.message);
  data.followUp = followUp;
  if (!Object.hasOwn(data, "steering")) data.steering = [];
  const merged = response(field(record.raw, "id"), "clear_queue", true, "", "", data);
  return { ...asProtocolRecord(merged), owner: record.owner };
}

function withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) return Promise.reject(new Error("session reload timed out"));
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("session reload timed out")), timeoutMs);
    promise.then(
      (value) => { clearTimeout(timer); resolve(value); },
      (error: unknown) => { clearTimeout(timer); reject(error); },
    );
  });
}

function runtimeArgsFromState(state: PiState): string[] {
  const args: string[] = [];
  if (state.model.id) args.push("--model", state.model.id);
  if (state.model.provider) args.push("--provider", state.model.provider);
  if (state.thinkingLevel) args.push("--thinking", state.thinkingLevel);
  if (state.sessionName) args.push("--name", state.sessionName);
  return args;
}

function refFromItem(item: QueuedItem): ClientRef {
  const ref: ClientRef = { clientId: item.clientId };
  if (item.kind) ref.kind = item.kind;
  if (item.name) ref.name = item.name;
  return ref;
}

function asProtocolRecord(event: RpcObject, owner = ""): ProtocolRecord {
  const raw = Buffer.from(JSON.stringify(event) ?? "{}", "utf8");
  const record = decodeRecord(raw);
  return { ...record, owner: owner || record.owner };
}

function parseCommand(raw: Uint8Array): RpcObject {
  try {
    const value: unknown = JSON.parse(Buffer.from(raw).toString("utf8"));
    return asObject(value);
  } catch { return {}; }
}

function asObject(value: unknown): RpcObject { return value !== null && typeof value === "object" && !Array.isArray(value) ? value as RpcObject : {}; }
function stringProp(value: RpcObject, key: string): string | undefined { return typeof value[key] === "string" ? value[key] as string : undefined; }
function messageOf(error: unknown): string { return error instanceof Error ? error.message : String(error); }
