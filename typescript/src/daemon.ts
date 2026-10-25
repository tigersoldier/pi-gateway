import { timingSafeEqual } from "node:crypto";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { Buffer } from "node:buffer";
import { createServer, type Server, type Socket } from "node:net";
import { rmdir, stat, unlink } from "node:fs/promises";
import { realpathSync } from "node:fs";
import { basename, dirname, isAbsolute, relative, resolve, sep } from "node:path";
import { defaultSessionRoots, headerCwd, leafId as readSessionLeafId, parseSessionFile, resolveSessionName, SessionScanner, type SessionInfo } from "./catalog.ts";
import {
  DEFAULT_ADDR,
  removePortIfMatches,
  requireLoopback,
  writePort,
  type TokenGrant,
} from "./config.ts";
import {
  ALL_CAPABILITIES,
  GATEWAY_FEATURES,
  JsonlDecoder,
  VERSION,
  UnsupportedProtocolVersionError,
  encodeFrame,
  parseHello,
  commandCapability,
  dataField,
  field,
  isGatewayType,
  isSupportedCommand,
  normalizeCapabilities,
  restoreId,
  response,
  type ClientRef,
  type ClientSummary,
  type Hello,
  type JsonValue,
  type ProtocolRecord,
  type SessionRow,
} from "./protocol.ts";
import { mergeSpawn, mergeSpawnAdditions, parsePiArgs, PiArgsSpec, redactedSpawnValues, spawnConflict, SpawnConflictError } from "./piargs.ts";
import { SpawnStore, type SpawnConfig } from "./spawn-store.ts";
import { ActorRestartError, ActorStopError, SessionActor, SESSION_CRASHED, SESSION_HIBERNATED, SESSION_READY, SESSION_STOPPED, type ActorClient, type ActorClientCommand, type RuntimeCommand } from "./session/actor.ts";
import { Subscriber } from "./session/hub.ts";
import { DeltaCoalescer } from "./session/coalescer.ts";
import type { RpcObject, RpcSessionRuntime } from "./rpc-adapter.ts";
import type { PiSdkRuntimeOptions } from "./sdk-runtime.ts";
import { nopLogger, type GatewayLogger } from "./logging.ts";

const execFileAsync = promisify(execFile);
const DEFAULT_IDLE_TIMEOUT_MS = 15 * 60_000;
const DEFAULT_SHORT_GRACE_MS = 10_000;
const DEFAULT_SUBSCRIBER_BUFFER = 1024;
const OUT_QUEUE_DEPTH = 1024;
const CLOSE_FLUSH_MS = 250;
const METRIC_NAMES: Record<string, string> = {
  sessions_started: "pi_gateway_sessions_started_total", sessions_ended: "pi_gateway_sessions_ended_total",
  sessions_reaped: "pi_gateway_sessions_reaped_total", sessions_deleted: "pi_gateway_sessions_deleted_total",
  attaches: "pi_gateway_attaches_total", attach_failures: "pi_gateway_attach_failures_total",
  unauthorized: "pi_gateway_unauthorized_total", turns_started: "pi_gateway_turns_started_total",
  turns_settled: "pi_gateway_turns_settled_total", prompts_queued: "pi_gateway_prompts_queued_total",
  prompts_rejected: "pi_gateway_prompts_rejected_total", reloads: "pi_gateway_reloads_total",
  reloads_refused: "pi_gateway_reloads_refused_total", pi_exits: "pi_gateway_pi_exits_total",
  ui_requests: "pi_gateway_ui_requests_total", ui_answered: "pi_gateway_ui_answered_total",
  ui_stale: "pi_gateway_ui_stale_total", ui_unroutable: "pi_gateway_ui_unroutable_total",
  subscriber_drops: "pi_gateway_subscriber_drops_total", client_lags: "pi_gateway_client_lags_total",
  frames_in: "pi_gateway_frames_in_total", frames_out: "pi_gateway_frames_out_total",
  bytes_in: "pi_gateway_bytes_in_total", bytes_out: "pi_gateway_bytes_out_total",
  frames_dropped: "pi_gateway_frames_dropped_total",
};

const CODE_UNAUTHORIZED = "unauthorized";
const CODE_FORBIDDEN = "forbidden";
const CODE_BAD_FRAME = "bad_frame";
const CODE_UNKNOWN_SESSION = "unknown_session";
const CODE_AMBIGUOUS_SESSION = "ambiguous_session";
const CODE_SPAWN_PARAM_CONFLICT = "spawn_param_conflict";
const CODE_SESSION_BUSY = "session_busy";
const CODE_SESSION_ATTACHED = "session_attached";
const CODE_RELOAD_BUSY = "reload_busy";
const CODE_RELOAD_TIMEOUT = "reload_timeout";
const CODE_SPAWN_CONFIG_PERSIST_FAILED = "spawn_config_persist_failed";
const CODE_SLOW_CONSUMER = "slow_consumer";
const CODE_SESSION_CRASHED = "session_crashed";
const CODE_RESYNC_REQUIRED = "resync_required";
const CODE_NOT_SUPPORTED = "not_supported";

export interface DaemonOptions {
  addr?: string;
  token: string;
  tokens?: TokenGrant[];
  portFile?: string;
  piBin?: string;
  idleTimeoutMs?: number;
  shortGraceMs?: number;
  stopGraceMs?: number;
  reloadTimeoutMs?: number;
  subscriberBuffer?: number;
  hubCapacity?: number;
  deltaFlushMs?: number;
  catalogRoots?: string[];
  spawnDir?: string;
  createRuntime?: (options: PiSdkRuntimeOptions) => Promise<RpcSessionRuntime>;
  logger?: GatewayLogger;
}

export interface DaemonAddress { address: string; port: number; family: string }
export interface StopTargetResult {
  path: string; name: string; sessionId: string; piStopped: boolean; detachedClients: number;
  unbound: boolean; fileDeleted?: boolean;
}

/** Loopback TCP daemon that owns SDK-backed Pi sessions. */
export class PiGatewayDaemon {
  private readonly options: DaemonOptions;
  private readonly address: string;
  private readonly scanner: SessionScanner;
  private readonly spawnStore: SpawnStore;
  private readonly log: GatewayLogger;
  private readonly spawnConfigs = new WeakMap<SessionActor, SpawnConfig>();
  private readonly tombstones = new Set<string>();
  private readonly pathLocks = new Map<string, Promise<void>>();
  private readonly pendingAttaches = new Map<string, number>();
  private tokenGrants: TokenGrant[];
  private readonly primaryToken: string;
  private readonly actors = new Map<string, SessionActor>();
  private readonly pending = new Set<SessionActor>();
  private readonly connections = new Set<GatewayConnection>();
  private readonly pendingSockets: Socket[] = [];
  private acceptReady = false;
  private server?: Server;
  private port = 0;
  private piVersion = "";
  private nextClient = 0;
  private stopped = false;
  private readonly startedAt = new Date();
  private readonly metrics = new Map<string, number>();

  constructor(options: DaemonOptions) {
    this.options = options;
    this.address = options.addr || DEFAULT_ADDR;
    this.scanner = new SessionScanner(defaultSessionRoots(options.catalogRoots));
    this.spawnStore = new SpawnStore(options.spawnDir ?? "");
    this.log = options.logger ?? nopLogger();
    this.primaryToken = options.token;
    this.tokenGrants = [];
    this.replaceTokens(options.tokens ?? []);
  }

  get piVersionValue(): string { return this.piVersion; }
  get subscriberBuffer(): number { return this.options.subscriberBuffer ?? DEFAULT_SUBSCRIBER_BUFFER; }
  get deltaFlushMs(): number { return this.options.deltaFlushMs ?? 50; }
  get addr(): DaemonAddress | undefined {
    const bound = this.server?.address();
    if (!bound || typeof bound === "string") return undefined;
    return { address: bound.address, port: bound.port, family: bound.family };
  }

  async listen(): Promise<void> {
    if (this.server) throw new Error("daemon is already listening");
    await requireLoopback(this.address);
    const [host, portText] = splitHostPort(this.address);
    const server = createServer({ allowHalfOpen: false });
    this.server = server;
    server.on("connection", (socket) => {
      if (!this.acceptReady) { this.pendingSockets.push(socket); return; }
      this.acceptSocket(socket);
    });
    await new Promise<void>((resolveListen, reject) => {
      const onError = (error: Error) => { server.off("listening", onListen); reject(error); };
      const onListen = () => { server.off("error", onError); resolveListen(); };
      server.once("error", onError);
      server.once("listening", onListen);
      server.listen({ host, port: Number(portText), exclusive: true });
    });
    const addr = this.addr;
    if (!addr) throw new Error("daemon: listener did not report a TCP address");
    this.port = addr.port;
    if (this.options.portFile) {
      try { await writePort(this.options.portFile, this.port); }
      catch (error) { this.log.warn("cannot write port file", "err", error); }
    }
    this.piVersion = await discoverPiVersion(this.options.piBin ?? "pi");
    this.acceptReady = true;
    for (const socket of this.pendingSockets.splice(0)) this.acceptSocket(socket);
  }

  async serve(signal?: AbortSignal): Promise<void> {
    if (!this.server) throw new Error("daemon must listen before Serve");
    if (this.stopped) return;
    if (signal?.aborted) { await this.shutdown(); return; }
    await new Promise<void>((resolveServe) => {
      const finish = () => { signal?.removeEventListener("abort", onAbort); resolveServe(); };
      const onAbort = () => { void this.shutdown().finally(finish); };
      signal?.addEventListener("abort", onAbort, { once: true });
      this.server?.once("close", finish);
    });
  }

  async shutdown(): Promise<void> {
    if (this.stopped) return;
    this.stopped = true;
    const server = this.server;
    const closeServer = server && server.listening
      ? new Promise<void>((resolveClose) => server.close(() => resolveClose()))
      : Promise.resolve();
    const connections = [...this.connections];
    for (const socket of this.pendingSockets.splice(0)) socket.destroy();
    const actors = [...new Set([...this.actors.values(), ...this.pending])];
    await Promise.all([
      ...connections.map((connection) => connection.close()),
      ...actors.map((actor) => actor.stop("daemon shutdown", SESSION_STOPPED)),
      closeServer,
    ]);
    if (this.options.portFile) await removePortIfMatches(this.options.portFile, this.port);
  }

  allocateClientId(): string { this.nextClient++; return `c_${this.nextClient}`; }
  private acceptSocket(socket: Socket): void {
    const connection = new GatewayConnection(this, socket, this.allocateClientId());
    this.connections.add(connection);
    void connection.run().catch(async (error) => {
      this.log.debug("client connection failed", "err", error);
      await connection.close();
    }).finally(() => this.connections.delete(connection));
  }
  countMetric(name: string, delta = 1): void {
    const key = METRIC_NAMES[name] ?? name;
    this.metrics.set(key, (this.metrics.get(key) ?? 0) + delta);
  }
  get metricSnapshot(): ReadonlyMap<string, number> { return this.metrics; }
  get sessionActors(): readonly SessionActor[] { return [...new Set([...this.actors.values(), ...this.pending])]; }
  get tokenNames(): string[] { return this.tokenGrants.map((grant) => grant.name); }
  get catalogRoots(): string[] { return [...this.scanner.roots]; }
  get started(): Date { return new Date(this.startedAt); }

  async status(): Promise<Record<string, unknown>> {
    const actors = this.sessionActors;
    const liveSessions: Array<Record<string, unknown>> = [];
    let live = 0;
    let streaming = 0;
    let clients = 0;
    let queueDepth = 0;
    let subscribers = 0;
    for (const actor of actors) {
      const info = await actor.info();
      const isStreaming = actor.turn.state === "running";
      if (actor.live) live++;
      if (isStreaming) streaming++;
      clients += info.clients.length;
      queueDepth += actor.turn.queued;
      subscribers += info.subscribers;
      liveSessions.push({ path: actor.path, name: actor.sessionName || undefined, state: actor.state,
        clients: info.clients.length, queueDepth: actor.turn.queued, streaming: isStreaming });
    }
    liveSessions.sort((left, right) => String(left.path).localeCompare(String(right.path)));
    return {
      piVersion: this.piVersion || undefined,
      registered: actors.length,
      live,
      streaming,
      clients,
      connections: this.connections.size,
      queueDepth,
      subscribers,
      tokens: this.tokenGrants.length,
      liveSessions: liveSessions.length ? liveSessions : undefined,
    };
  }

  async metricsText(): Promise<string> {
    const help: Record<string, string> = {
      pi_gateway_sessions_started_total: "Pi SDK sessions initialized",
      pi_gateway_sessions_ended_total: "Pi SDK sessions disposed after stop, hibernation, or failure",
      pi_gateway_sessions_reaped_total: "sessions retired by the idle reaper",
      pi_gateway_sessions_deleted_total: "sessions deleted by gw_delete_session (file and registration)",
      pi_gateway_attaches_total: "successful client attachments to a session",
      pi_gateway_attach_failures_total: "client attachments refused or timed out",
      pi_gateway_unauthorized_total: "connections rejected for a missing or wrong token",
      pi_gateway_turns_started_total: "agent turns started",
      pi_gateway_turns_settled_total: "agent turns that settled",
      pi_gateway_prompts_queued_total: "prompts queued behind a running turn",
      pi_gateway_prompts_rejected_total: "prompts rejected because the session was not running",
      pi_gateway_reloads_total: "gw_reload_session restarts performed",
      pi_gateway_reloads_refused_total: "gw_reload_session requests refused as busy",
      pi_gateway_pi_exits_total: "managed Pi SDK sessions that failed unexpectedly",
      pi_gateway_ui_requests_total: "extension UI dialogs routed to a client",
      pi_gateway_ui_answered_total: "extension UI dialogs answered by their owner",
      pi_gateway_ui_stale_total: "extension UI answers from a client that no longer owns the dialog",
      pi_gateway_ui_unroutable_total: "extension UI dialogs dropped because no ui-capable client was attached",
      pi_gateway_subscriber_drops_total: "subscribers dropped for not keeping up",
      pi_gateway_client_lags_total: "gw_lag markers telling a lossy client which records it missed",
      pi_gateway_frames_in_total: "protocol frames read from clients",
      pi_gateway_frames_out_total: "protocol frames written to clients",
      pi_gateway_bytes_in_total: "protocol bytes read from clients",
      pi_gateway_bytes_out_total: "protocol bytes written to clients",
      pi_gateway_frames_dropped_total: "non-terminal frames dropped for lossy clients",
    };
    const status = await this.status();
    const gauges: Array<[string, string, number]> = [
      ["pi_gateway_sessions", "registered sessions", Number(status.registered)],
      ["pi_gateway_sessions_live", "sessions with an active Pi SDK runtime", Number(status.live)],
      ["pi_gateway_sessions_streaming", "sessions currently running a turn", Number(status.streaming)],
      ["pi_gateway_clients", "clients attached to a session", Number(status.clients)],
      ["pi_gateway_connections", "open client connections", Number(status.connections)],
      ["pi_gateway_queue_depth", "prompts waiting in daemon queues", Number(status.queueDepth)],
      ["pi_gateway_subscribers", "event subscribers", Number(status.subscribers)],
      ["pi_gateway_tokens", "configured tokens", Number(status.tokens)],
      ["pi_gateway_uptime_seconds", "daemon uptime in seconds", Math.floor((Date.now() - this.startedAt.getTime()) / 1000)],
    ];
    const samples = [
      ...Object.entries(METRIC_NAMES).map(([key, name]) => ({
        name, type: "counter", help: help[name] ?? "", value: this.metrics.get(name) ?? this.metrics.get(key) ?? 0,
      })),
      ...gauges.map(([name, description, value]) => ({ name, type: "gauge", help: description, value })),
    ].sort((left, right) => left.name.localeCompare(right.name));
    const lines: string[] = [];
    for (const sample of samples) {
      lines.push(`# HELP ${sample.name} ${sample.help}`, `# TYPE ${sample.name} ${sample.type}`, `${sample.name} ${sample.value}`);
    }
    return `${lines.join("\n")}\n`;
  }

  replaceTokens(extra: readonly TokenGrant[]): void {
    if (!this.primaryToken) { this.tokenGrants = []; return; }
    const grants: TokenGrant[] = [{ name: "default", token: this.primaryToken, capabilities: [...ALL_CAPABILITIES] }];
    const seen = new Set([this.primaryToken]);
    for (const grant of extra) {
      if (!grant.token) throw new Error(`daemon: token ${JSON.stringify(grant.name)} has an empty value`);
      if (seen.has(grant.token)) throw new Error(`daemon: token ${JSON.stringify(grant.name)} duplicates another token value`);
      seen.add(grant.token);
      grants.push({ ...grant, capabilities: [...grant.capabilities] });
    }
    this.tokenGrants = grants;
  }

  authenticate(token: string): TokenGrant | undefined {
    let match: TokenGrant | undefined;
    for (const grant of this.tokenGrants) if (constantTimeEqual(token, grant.token)) match = grant;
    return match;
  }

  grantCapabilities(requested: readonly string[], allowed: readonly string[]): { granted: string[]; set: Set<string> } {
    const canonical = normalizeCapabilities(allowed).known;
    const requestedSet = new Set(requested);
    const granted = requested.length === 0 ? canonical : canonical.filter((capability) => requestedSet.has(capability));
    return { granted, set: new Set(granted) };
  }

  async resolveTarget(target: string): Promise<string> {
    if (!target) throw new GatewayError(CODE_UNKNOWN_SESSION, "no session path given");
    if (looksLikePath(target)) return this.resolveCanonical(canonicalPath(target));
    const files = await this.scanner.findName(target);
    const live = [...new Set(this.actors.values())].filter((actor) => actor.live && actor.path).map((actor) => actorCatalogInfo(actor));
    const resolved = resolveSessionName(target, files.sessions, live);
    if (resolved.error) throw new GatewayError(CODE_AMBIGUOUS_SESSION, resolved.error);
    if (!resolved.info) throw new GatewayError(CODE_UNKNOWN_SESSION, `no session named ${JSON.stringify(target)}`);
    const canon = this.resolveCanonical(canonicalPath(resolved.info.path));
    if (files.truncated && !live.some((item) => canonicalPath(item.path) === canon)) {
      throw new GatewayError(CODE_AMBIGUOUS_SESSION, `session name ${JSON.stringify(target)} could not be resolved uniquely`);
    }
    return canon;
  }

  private resolveCanonical(path: string): string {
    if (this.tombstones.has(path)) throw new GatewayError(CODE_UNKNOWN_SESSION, "session was deleted");
    return path;
  }

  isTombstoned(path: string): boolean { return this.tombstones.has(canonicalPath(path)); }

  async attach(target: string, spec: PiArgsSpec, cwd: string, by?: ClientRef): Promise<SessionActor> {
    const path = await this.resolveTarget(target);
    this.pendingAttaches.set(path, (this.pendingAttaches.get(path) ?? 0) + 1);
    let handedOff = false;
    try {
      const actor = await this.withPathLock(path, async () => {
        if (this.tombstones.has(path)) throw new GatewayError(CODE_UNKNOWN_SESSION, "session was deleted");
        let current = this.actors.get(path);
        if (current && !current.live) {
          this.actors.delete(path);
          current = undefined;
        }
        if (!current) {
          let recorded: SpawnConfig | undefined;
          try { recorded = await this.spawnStore.load(path); }
          catch (error) { this.log.warn("cannot read spawn configuration", "session", path, "err", error); }
          if (this.tombstones.has(path)) throw new GatewayError(CODE_UNKNOWN_SESSION, "session was deleted");
          let effectiveSpec = spec;
          if (recorded) {
            try { effectiveSpec = parsePiArgs(mergeSpawn(recorded.piArgs, spec)); }
            catch { effectiveSpec = spec; }
          }
          return this.createActor(path, effectiveSpec, cwd, recorded?.createdBy, recorded);
        }
        const recorded = this.spawnConfigs.get(current);
        if (recorded) {
          const [key, conflict] = spawnConflict(parsePiArgs(recorded.piArgs).spawnValues(), spec.spawnValues());
          if (conflict) throw new GatewayError(CODE_SPAWN_PARAM_CONFLICT, `session is live with a different value for ${key}`);
        }
        await this.applyRuntime(current, spec, by);
        return current;
      });
      handedOff = true;
      return actor;
    } finally {
      if (!handedOff) this.completeAttachPath(path);
    }
  }

  completeAttach(actor: SessionActor): void {
    if (actor.path) this.completeAttachPath(canonicalPath(actor.path));
  }

  private completeAttachPath(path: string): void {
    const count = this.pendingAttaches.get(path) ?? 0;
    if (count <= 1) this.pendingAttaches.delete(path);
    else this.pendingAttaches.set(path, count - 1);
  }

  async stopTarget(target: string, bound: SessionActor | undefined, requester: string, force: boolean, deleteFile: boolean): Promise<StopTargetResult> {
    let actor = target ? undefined : bound;
    if (!target && !actor) throw new GatewayError(CODE_UNKNOWN_SESSION, "not attached to a session");
    let canon = actor?.path ? canonicalPath(actor.path) : "";
    let info = actor ? actorCatalogInfo(actor) : undefined;
    if (target) canon = await this.resolveTarget(target);
    const lockKey = canon || `actor:${actor?.sessionId ?? requester}`;
    return this.withPathLock(lockKey, async () => {
      if (canon && this.tombstones.has(canon)) throw new GatewayError(CODE_UNKNOWN_SESSION, "session was deleted");
      if (target) {
        actor = this.actors.get(canon);
        info = actor ? actorCatalogInfo(actor) : await this.sessionFileInfo(canon);
        if (!actor && !info) throw new GatewayError(CODE_UNKNOWN_SESSION, `no session at ${canon}`);
      }
      if (deleteFile && !actor && (!info || !this.underRoot(canon))) {
        throw new GatewayError(CODE_UNKNOWN_SESSION, `not a session file under a session root: ${canon}`);
      }
      const unbound = actor !== undefined && actor === bound;
      if (deleteFile && canon) this.tombstones.add(canon);
      let stopped = { path: info?.path ?? canon, name: info?.name ?? "", sessionId: info?.id ?? "", piStopped: false, detachedClients: 0 };
      try {
        if (actor) stopped = await actor.stopSession({ force, deleteFile, requester });
      } catch (error) {
        if (deleteFile && canon) this.tombstones.delete(canon);
        if (error instanceof ActorStopError) {
          const code = error.code === "session_busy" ? CODE_SESSION_BUSY
            : error.code === "session_attached" ? CODE_SESSION_ATTACHED : error.code;
          throw new GatewayError(code, error.message);
        }
        throw new GatewayError(CODE_SESSION_CRASHED, messageOf(error));
      }
      if (!stopped.path) stopped.path = canon;
      const result: StopTargetResult = { ...stopped, unbound };
      if (deleteFile) {
        try { result.fileDeleted = await deleteSessionFile(stopped.path); }
        catch (error) {
          if (canon) this.tombstones.delete(canon);
          throw new GatewayError(CODE_SESSION_CRASHED, messageOf(error));
        }
        this.countMetric("sessions_deleted");
        if (canon) {
          await this.spawnStore.delete(canon).catch((error: unknown) => {
            this.log.warn("cannot remove spawn configuration", "session", canon, "err", error);
          });
        }
        this.log.info("session deleted", "session", result.path, "name", result.name, "client", requester,
          "force", force, "detachedClients", result.detachedClients, "fileDeleted", result.fileDeleted);
      } else {
        this.log.info("session stopped", "session", result.path, "name", result.name, "client", requester,
          "force", force, "detachedClients", result.detachedClients);
      }
      return result;
    });
  }

  private async sessionFileInfo(path: string): Promise<SessionInfo | undefined> {
    if (!path || this.tombstones.has(path) || !this.underRoot(path)) return undefined;
    try { return { ...await parseSessionFile(path), path }; } catch { return undefined; }
  }

  private underRoot(path: string): boolean {
    for (const root of this.scanner.roots) {
      const canonicalRoot = canonicalPath(root);
      const rel = relative(canonicalRoot, path);
      if (rel !== ".." && !rel.startsWith(`..${sep}`) && !isAbsolute(rel)) return true;
    }
    return false;
  }

  private async withPathLock<T>(key: string, operation: () => Promise<T>): Promise<T> {
    const previous = this.pathLocks.get(key) ?? Promise.resolve();
    let release!: () => void;
    const current = new Promise<void>((resolveLock) => { release = resolveLock; });
    this.pathLocks.set(key, current);
    await previous;
    try { return await operation(); }
    finally {
      release();
      if (this.pathLocks.get(key) === current) this.pathLocks.delete(key);
    }
  }

  async reloadTarget(
    target: string, bound: SessionActor | undefined, requester: string, by: ClientRef, force: boolean,
    replacement?: PiArgsSpec, additions?: PiArgsSpec,
  ): Promise<void> {
    let canon: string;
    if (target) canon = await this.resolveTarget(target);
    else if (bound?.path) canon = canonicalPath(bound.path);
    else if (bound) canon = `actor:${bound.sessionId}`;
    else throw new GatewayError(CODE_UNKNOWN_SESSION, "not attached to a session");
    return this.withPathLock(canon, async () => {
      if (this.tombstones.has(canon)) throw new GatewayError(CODE_UNKNOWN_SESSION, "session was deleted");
      const actor = target ? this.actors.get(canon) : bound;
      const reloadable = actor && (actor.state === SESSION_READY || (!target && actor.state === SESSION_CRASHED));
      if (!reloadable || !actor) throw new GatewayError(CODE_UNKNOWN_SESSION, "session is not live");
      if (this.pendingAttaches.get(canon) && !canon.startsWith("actor:")) {
        this.countMetric("reloads_refused");
        throw new GatewayError(CODE_RELOAD_BUSY, "another client is attaching to the session");
      }
      const recorded = this.spawnConfigs.get(actor);
      if (!recorded) throw new GatewayError(CODE_UNKNOWN_SESSION, "session has no recorded spawn configuration");
      if (replacement && additions) throw new GatewayError(CODE_BAD_FRAME, "piArgs and addPiArgs are mutually exclusive");
      let resultSpec = replacement;
      let additiveSpawnArgs: string[] | undefined;
      if (additions) {
        if (!actor.path) throw new GatewayError(CODE_UNKNOWN_SESSION, "session path is not ready");
        try { additiveSpawnArgs = mergeSpawnAdditions(recorded.piArgs, additions).canonicalSpawnArgs(); }
        catch (error) {
          if (error instanceof SpawnConflictError) throw new GatewayError(CODE_SPAWN_PARAM_CONFLICT, error.message);
          throw new GatewayError(CODE_BAD_FRAME, messageOf(error));
        }
      }
      try {
        const committed = await actor.restart({ force, requester, by, replacement: resultSpec, additiveSpawnArgs,
          timeoutMs: this.options.reloadTimeoutMs ?? 30_000 });
        if (!committed) return;
        const config: SpawnConfig = {
          ...recorded,
          path: canonicalPath(actor.path),
          piArgs: committed.canonicalSpawnArgs(),
          spawn: redactedSpawnValues(committed.spawnValues()),
          cwd: actor.cwd,
          sessionId: actor.sessionId,
        };
        this.spawnConfigs.set(actor, config);
        try { await this.spawnStore.save(config); }
        catch {
          throw new GatewayError(CODE_SPAWN_CONFIG_PERSIST_FAILED,
            "pi restarted with the new configuration, but it could not be saved durably; retry after fixing storage");
        }
      } catch (error) {
        if (error instanceof GatewayError) throw error;
        if (error instanceof ActorRestartError) {
          const code = error.code === "reload_busy" ? CODE_RELOAD_BUSY
            : error.code === "reload_timeout" ? CODE_RELOAD_TIMEOUT : error.code;
          throw new GatewayError(code, error.message);
        }
        throw new GatewayError(CODE_SESSION_CRASHED, messageOf(error));
      }
    });
  }

  async createActor(path: string, spec: PiArgsSpec, cwd: string, createdBy?: ClientRef, recorded?: SpawnConfig): Promise<SessionActor> {
    if (this.stopped) throw new Error("daemon is shutting down");
    let spawnCwd = recorded?.cwd || cwd || process.cwd();
    if (path) {
      const headerDirectory = await headerCwd(path);
      if (headerDirectory) {
        try { if ((await stat(headerDirectory)).isDirectory()) spawnCwd = headerDirectory; } catch { /* retain sidecar or requester cwd */ }
      }
    }
    if (this.stopped) throw new Error("daemon is shutting down");
    const actor = new SessionActor({
      piBin: this.options.piBin ?? "pi",
      cwd: spawnCwd,
      sessionPath: path || undefined,
      piArgs: spec,
      hubCapacity: this.options.hubCapacity,
      idleTimeoutMs: this.options.idleTimeoutMs ?? DEFAULT_IDLE_TIMEOUT_MS,
      shortGraceMs: this.options.shortGraceMs ?? DEFAULT_SHORT_GRACE_MS,
      stopGraceMs: this.options.stopGraceMs,
      metric: (name) => this.countMetric(name),
      logger: this.log.with("session", path),
      createRuntime: this.options.createRuntime,
      onPath: (current, value) => { this.registerPath(current, value); void this.persistSpawn(current, value); },
      onStopped: (current, reason) => this.removeActor(current, reason),
      retire: (current, retire) => this.retireActor(current, retire),
    });
    const config: SpawnConfig = {
      path: path ? canonicalPath(path) : "",
      piArgs: spec.canonicalSpawnArgs(),
      spawn: redactedSpawnValues(spec.spawnValues()),
      cwd: spawnCwd,
      createdAt: recorded?.createdAt || new Date().toISOString(),
      sessionId: "",
    };
    const creator = recorded?.createdBy ?? createdBy;
    if (creator) config.createdBy = { ...creator, tags: creator.tags ? { ...creator.tags } : undefined };
    this.spawnConfigs.set(actor, config);
    if (path) this.actors.set(canonicalPath(path), actor);
    else this.pending.add(actor);
    try {
      await actor.start();
      if (path) await this.persistSpawn(actor, path);
      if (this.stopped) {
        await actor.stop("daemon shutdown", SESSION_STOPPED);
        throw new Error("daemon is shutting down");
      }
    } catch (error) {
      this.removeActor(actor, "startup failed");
      throw error;
    }
    this.countMetric("sessions_started");
    this.log.info("session started", "session", path);
    return actor;
  }

  async listSessions(filter: { cwd?: string; live?: boolean; limit?: number } = {}): Promise<SessionRow[]> {
    const scanned = await this.scanner.list(filter.cwd ?? "", filter.limit ?? 0);
    const live = new Map<string, SessionActor>();
    for (const actor of new Set(this.actors.values())) {
      if (actor.live && actor.path) live.set(canonicalPath(actor.path), actor);
    }
    const rows: SessionRow[] = [];
    const seen = new Set<string>();
    for (const item of scanned) {
      const path = canonicalPath(item.path);
      if (seen.has(path)) continue;
      const actor = live.get(path);
      const clients = actor ? (await actor.info()).clients : undefined;
      const stored = actor ? this.spawnConfigs.get(actor) : await this.spawnStore.load(path).catch(() => undefined);
      const row: SessionRow = {
        path,
        name: item.name || (actor?.sessionName || undefined),
        title: item.title || undefined,
        id: item.id || (actor?.sessionId || undefined),
        cwd: item.cwd || undefined,
        live: Boolean(actor),
        isStreaming: actor ? actor.turn.state === "running" : false,
        messageCount: item.messageCount,
        lastActivity: formatCatalogTime(item.lastActivity),
        clients,
        createdBy: stored?.createdBy,
        spawn: stored?.spawn,
      };
      if (!filter.live || row.live) rows.push(row);
      seen.add(path);
    }
    if (!filter.cwd) {
      for (const [path, actor] of live) {
        if (seen.has(path)) continue;
        const info = await actor.info();
        rows.push({ path, name: actor.sessionName || undefined, id: actor.sessionId || undefined,
          live: true, isStreaming: actor.turn.state === "running", messageCount: 0, clients: info.clients,
          createdBy: this.spawnConfigs.get(actor)?.createdBy, spawn: this.spawnConfigs.get(actor)?.spawn });
      }
    }
    rows.sort((left, right) => {
      if (left.live !== right.live) return left.live ? -1 : 1;
      return (right.lastActivity ?? "").localeCompare(left.lastActivity ?? "");
    });
    const limit = filter.limit && filter.limit > 0 ? filter.limit : 0;
    return limit ? rows.slice(0, limit) : rows;
  }

  private async applyRuntime(actor: SessionActor, spec: PiArgsSpec, by?: ClientRef): Promise<void> {
    const runtime = spec.runtimeValues();
    if (Object.keys(runtime).length === 0) return;
    let state;
    try { state = parseStateData(await actor.call({ type: "get_state" })); }
    catch (error) { this.log.warn("cannot read state to apply runtime parameters", "session", actor.path, "err", error); return; }
    const commands: RuntimeCommand[] = [];
    const modelId = runtime.model || state.model.id;
    const provider = runtime.provider || state.model.provider;
    if ((runtime.model || runtime.provider) && (modelId !== state.model.id || provider !== state.model.provider)) {
      commands.push(runtimeCommand("set_model", { type: "set_model", modelId, provider }));
    }
    if (runtime.thinking && runtime.thinking !== state.thinkingLevel) {
      commands.push(runtimeCommand("set_thinking_level", { type: "set_thinking_level", level: runtime.thinking }));
    }
    if (runtime.name && runtime.name !== state.sessionName) {
      commands.push(runtimeCommand("set_session_name", { type: "set_session_name", name: runtime.name }));
    }
    await actor.applyRuntime(commands, by);
  }

  private registerPath(actor: SessionActor, path: string): void {
    const canon = canonicalPath(path);
    this.pending.delete(actor);
    const other = this.actors.get(canon);
    if (other && other !== actor) {
      this.log.warn("session path already registered; retiring duplicate actor", "session", canon);
      void actor.stop("duplicate session path", SESSION_STOPPED);
      return;
    }
    for (const [key, current] of [...this.actors]) {
      if (current !== actor || key === canon) continue;
      this.actors.delete(key);
      const reserved = this.pendingAttaches.get(key) ?? 0;
      if (reserved > 0) {
        this.pendingAttaches.delete(key);
        this.pendingAttaches.set(canon, (this.pendingAttaches.get(canon) ?? 0) + reserved);
      }
    }
    this.actors.set(canon, actor);
    this.log.debug("session registered", "session", canon);
  }

  async persistActorSpawn(actor: SessionActor, path = actor.path): Promise<void> {
    if (path) await this.persistSpawn(actor, path);
  }

  private async persistSpawn(actor: SessionActor, path: string): Promise<void> {
    const previous = this.spawnConfigs.get(actor);
    const canonical = canonicalPath(path);
    if (!previous || !canonical) return;
    const config: SpawnConfig = {
      ...previous,
      path: canonical,
      piArgs: [...previous.piArgs],
      cwd: actor.cwd,
      sessionId: actor.sessionId,
    };
    if (previous.spawn) config.spawn = Object.fromEntries(Object.entries(previous.spawn).map(([key, values]) => [key, [...values]]));
    this.spawnConfigs.set(actor, config);
    try { await this.spawnStore.save(config); }
    catch (error) { this.log.warn("cannot persist spawn configuration", "session", canonical, "err", error); }
  }

  private removeActor(actor: SessionActor, reason = "stopped"): void {
    let tracked = this.pending.delete(actor);
    for (const [path, current] of this.actors) {
      if (current !== actor) continue;
      this.actors.delete(path);
      tracked = true;
    }
    if (!tracked) return;
    this.countMetric("sessions_ended");
    this.log.info("session stopped", "session", actor.path, "reason", reason);
  }

  private async retireActor(actor: SessionActor, retire: () => Promise<void>): Promise<boolean> {
    const path = actor.path ? canonicalPath(actor.path) : `actor:${actor.sessionId}`;
    return this.withPathLock(path, async () => {
      if (this.stopped || actor.attached > 0) return false;
      if (actor.state === SESSION_READY && actor.path && (this.pendingAttaches.get(path) ?? 0) > 0) return false;
      if (actor.path && this.actors.get(path) !== actor) return false;
      await retire();
      if (actor.state === SESSION_HIBERNATED) {
        this.countMetric("sessions_reaped");
        this.log.info("session retired", "session", actor.path);
      }
      return true;
    });
  }
}

class GatewayConnection implements ActorClient {
  readonly id: string;
  private readonly daemon: PiGatewayDaemon;
  private readonly socket: Socket;
  private readonly decoder = new JsonlDecoder();
  private readTail: Promise<void> = Promise.resolve();
  private writeTail: Promise<void> = Promise.resolve();
  private pendingWrites = 0;
  private readonly closedPromise: Promise<void>;
  private resolveClosed!: () => void;
  private closed = false;
  private closing = false;
  private helloDone = false;
  private infoValue: { name: string; kind: string; tags?: Record<string, string> } = { name: "client", kind: "integration" };
  private capabilities = new Set<string>();
  private granted: string[] = [];
  private piSpec = new PiArgsSpec();
  private cwd = "";
  private allowLossy = false;
  private deletedUnbound = false;
  private generation = 0;
  private actor?: SessionActor;
  private subscriber?: Subscriber;

  constructor(daemon: PiGatewayDaemon, socket: Socket, id: string) {
    this.daemon = daemon;
    this.socket = socket;
    this.id = id;
    this.closedPromise = new Promise((resolveClosed) => { this.resolveClosed = resolveClosed; });
  }

  get kind(): string { return this.infoValue.kind; }
  get name(): string { return this.infoValue.name; }
  has(capability: string): boolean { return this.capabilities.has(capability); }
  ref(): ClientRef {
    const value: ClientRef = { clientId: this.id, kind: this.kind, name: this.name };
    if (this.infoValue.tags) value.tags = this.infoValue.tags;
    return value;
  }
  summary(): ClientSummary {
    const value: ClientSummary = { clientId: this.id, kind: this.kind, name: this.name, capabilities: [...this.granted] };
    if (this.infoValue.tags) value.tags = this.infoValue.tags;
    return value;
  }

  async run(): Promise<void> {
    this.socket.setNoDelay(true);
    this.socket.on("data", (chunk: Buffer) => {
      this.socket.pause();
      try {
        const frames = this.decoder.push(chunk);
        for (const raw of frames) {
          this.daemon.countMetric("frames_in");
          this.daemon.countMetric("bytes_in", raw.length);
        }
        this.readTail = this.readTail.then(async () => {
          for (const raw of frames) {
            if (this.closed) return;
            await this.handleFrame(raw);
          }
        }).catch((error) => this.fail(error)).finally(() => {
          if (!this.closed) this.socket.resume();
        });
      } catch (error) {
        void this.sendError(CODE_BAD_FRAME, messageOf(error)).then(() => this.close());
      }
    });
    this.socket.once("end", () => {
      try {
        for (const raw of this.decoder.finish()) {
          this.daemon.countMetric("frames_in");
          this.daemon.countMetric("bytes_in", raw.length);
          this.readTail = this.readTail.then(() => this.handleFrame(raw)).catch((error) => this.fail(error));
        }
      } catch (error) { void this.sendError(CODE_BAD_FRAME, messageOf(error)).then(() => this.close()); }
      void this.readTail.finally(() => this.close());
    });
    this.socket.once("error", () => this.close());
    this.socket.once("close", () => this.close());
    await this.closedPromise;
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closing = true;
    this.closed = true;
    this.socket.destroy();
    this.resolveClosed();
    this.unbind();
    await this.writeTail.catch(() => {});
  }

  private async handleFrame(raw: Buffer): Promise<void> {
    if (this.closed) return;
    if (!this.helloDone) {
      const ok = await this.handleHello(raw);
      if (ok) this.helloDone = true;
      return;
    }
    await this.handleCommand(raw);
  }

  private async handleHello(raw: Buffer): Promise<boolean> {
    let hello: Hello;
    try { hello = parseHello(raw); }
    catch (error) {
      const message = messageOf(error);
      const code = error instanceof UnsupportedProtocolVersionError ? CODE_NOT_SUPPORTED : CODE_BAD_FRAME;
      await this.sendError(code, message);
      await this.closeAfterFlush();
      return false;
    }
    const grant = this.daemon.authenticate(hello.token);
    if (!grant) {
      this.daemon.countMetric("unauthorized");
      await this.sendError(CODE_UNAUTHORIZED, "missing or wrong token");
      await this.closeAfterFlush();
      return false;
    }
    this.infoValue = {
      name: hello.client.name || "client",
      kind: hello.client.kind || "integration",
      tags: hello.client.tags,
    };
    const grants = this.daemon.grantCapabilities(hello.client.capabilities ?? [], grant.capabilities);
    this.granted = grants.granted;
    this.capabilities = grants.set;
    try { this.piSpec = parsePiArgs(hello.piArgs ?? []); }
    catch (error) {
      await this.sendError(CODE_BAD_FRAME, messageOf(error));
      await this.closeAfterFlush();
      return false;
    }
    try { this.cwd = await resolveCwd(hello.cwd); }
    catch (error) {
      await this.sendError(CODE_BAD_FRAME, messageOf(error));
      await this.closeAfterFlush();
      return false;
    }
    this.allowLossy = hello.allowLossy === true;

    let actor: SessionActor | undefined;
    let attachError: GatewayError | undefined;
    if (hello.session) {
      try { actor = await this.daemon.attach(hello.session, this.piSpec, this.cwd, this.ref()); }
      catch (error) { attachError = asGatewayError(error); }
    }
    if (actor) {
      const bound = await this.bind(actor, false, true);
      if (!bound) {
        const deleted = actor.path !== "" && this.daemon.isTombstoned(actor.path);
        attachError = deleted
          ? new GatewayError(CODE_UNKNOWN_SESSION, "session was deleted")
          : new GatewayError(CODE_SESSION_CRASHED, "session is shutting down");
      }
    }
    if (attachError) actor = undefined;

    let replayRequested = hello.resume !== undefined && hello.liveOnly !== true && this.has("observe");
    let replay: ProtocolRecord[] = [];
    let resync = false;
    if (actor && replayRequested) {
      const result = actor.replay(hello.resume?.sinceSeq ?? 0);
      replay = result.records;
      resync = !result.ok;
      if (!resync && replay.length > OUT_QUEUE_DEPTH - 16) { resync = true; replay = []; }
    }
    if (resync && actor && this.has("observe") && hello.resume?.leafEntryId) {
      try {
        const leaf = await readSessionLeafId(actor.path);
        if (leaf && leaf === hello.resume.leafEntryId) {
          resync = false;
          replayRequested = false;
          replay = [];
        }
      } catch { /* a missing/unreadable transcript requires a snapshot */ }
    }
    let piState: unknown;
    if (actor) {
      try { piState = dataOf(await actor.call({ type: "get_state" }, 5000)); } catch { /* state is optional in welcome */ }
    }
    const welcome: RpcObject = {
      type: "gw_welcome",
      protocol: VERSION,
      clientId: this.id,
      kind: this.kind,
      granted: this.granted,
      features: [...GATEWAY_FEATURES],
      piVersion: this.daemon.piVersionValue || undefined,
      concurrency: "queue",
      session: null,
      headSeq: 0,
      oldestSeq: 0,
      resyncRequired: resync,
      piState: piState ?? null,
      turn: actor?.turn ?? { state: "idle", queued: 0 },
      clients: null,
    };
    if (actor) {
      const info = await actor.info();
      const sessionId = actor.sessionId || info.path;
      welcome.session = {
        path: info.path,
        ...(actor.sessionName ? { name: actor.sessionName } : {}),
        id: sessionId,
      };
      welcome.headSeq = info.headSeq;
      welcome.oldestSeq = info.oldestSeq;
      welcome.clients = info.clients;
    }
    await this.sendJSON(welcome);
    if (attachError) {
      this.daemon.countMetric("attach_failures");
      await this.sendError(attachError.code, attachError.message);
      return true;
    }
    if (!actor) return true;

    let watermark = 0;
    if (resync) {
      try {
        const snapshot = await actor.snapshot();
        watermark = (await actor.info()).headSeq;
        snapshot.headSeq = watermark;
        await this.sendJSON(snapshot);
      } catch (error) {
        await this.sendError(CODE_RESYNC_REQUIRED, messageOf(error));
        watermark = (await actor.info()).headSeq;
      }
    } else if (replayRequested) {
      watermark = hello.resume?.sinceSeq ?? 0;
      for (const record of replay) {
        if (!await this.sendRecord(record, this.generation)) { await this.close(); return true; }
        watermark = record.seq;
      }
      await this.sendJSON({ type: "gw_replay_done", headSeq: watermark });
    }
    this.startPump(watermark);
    return true;
  }

  private async handleCommand(raw: Buffer): Promise<void> {
    const type = field(raw, "type");
    const id = field(raw, "id");
    if (!type) { await this.sendError(CODE_BAD_FRAME, "frame has no type"); return; }
    if (type !== type.toLowerCase()) { await this.sendError(CODE_BAD_FRAME, `unknown command ${JSON.stringify(type)}`); return; }
    if (!isSupportedCommand(type) && !isGatewayType(type)) {
      await this.sendResponse(id, type, false, CODE_NOT_SUPPORTED, `${type} is not supported by this daemon revision`);
      return;
    }
    const capability = commandCapability(type);
    if (capability && !this.has(capability)) {
      await this.sendResponse(id, type, false, CODE_FORBIDDEN, `${type} requires the ${capability} capability`);
      return;
    }
    switch (type) {
      case "gw_ping": await this.sendJSON({ type: "gw_pong" }); return;
      case "gw_bye": await this.close(); return;
      case "gw_hello": await this.sendError(CODE_BAD_FRAME, "duplicate gw_hello"); return;
      case "gw_list_sessions": await this.handleListSessions(raw); return;
      case "gw_new_session": await this.handleNewGatewaySession(raw); return;
      case "gw_stop_session": await this.handleStopSession(raw, false); return;
      case "gw_delete_session": await this.handleStopSession(raw, true); return;
      case "gw_reload_session": await this.handleReloadSession(raw); return;
    }
    if (isGatewayType(type)) { await this.sendError(CODE_NOT_SUPPORTED, `${type} is not supported by this daemon revision`); return; }
    if (type === "switch_session") { await this.handleSwitch(raw, id); return; }
    if (type === "new_session") { await this.handleNewSession(id); return; }
    if ((type === "fork" || type === "clone") && !this.actor) {
      await this.sendResponse(id, type, false, CODE_UNKNOWN_SESSION, "not attached to a session");
      return;
    }
    let actor = this.actor;
    if (!actor) {
      if (this.deletedUnbound) {
        await this.sendResponse(id, type, false, CODE_UNKNOWN_SESSION, "session was deleted; attach or create a session explicitly");
        return;
      }
      try {
        actor = await this.daemon.createActor("", this.piSpec, this.cwd || process.cwd());
        if (!await this.bind(actor, true)) throw new Error("session is shutting down");
      } catch (error) {
        await this.sendResponse(id, type, false, CODE_SESSION_CRASHED, messageOf(error));
        return;
      }
    }
    const command: ActorClientCommand = { client: this, localId: id, type, raw };
    await actor.submit(command);
  }

  private async handleSwitch(raw: Buffer, id: string): Promise<void> {
    const request = parseObject(raw);
    const target = stringProperty(request, "sessionPath") || stringProperty(request, "session");
    if (!target) { await this.sendResponse(id, "switch_session", false, CODE_UNKNOWN_SESSION, "missing sessionPath"); return; }
    let actor: SessionActor;
    try { actor = await this.daemon.attach(target, this.piSpec, this.cwd || process.cwd(), this.ref()); }
    catch (error) {
      this.daemon.countMetric("attach_failures");
      const gateway = asGatewayError(error);
      await this.sendResponse(id, "switch_session", false, gateway.code, gateway.message);
      return;
    }
    try {
      if (!await this.bind(actor, true, true)) throw new Error("session is shutting down");
      await this.sendResponse(id, "switch_session", true, "", "", { cancelled: false });
    } catch (error) {
      const gateway = asGatewayError(error);
      await this.sendResponse(id, "switch_session", false, gateway.code, gateway.message);
    }
  }

  private async handleNewSession(id: string): Promise<void> {
    try {
      const actor = await this.daemon.createActor("", this.piSpec, this.cwd || process.cwd());
      if (!await this.bind(actor, true)) throw new Error("session is shutting down");
      await this.sendResponse(id, "new_session", true, "", "", { cancelled: false });
    } catch (error) {
      await this.sendResponse(id, "new_session", false, CODE_SESSION_CRASHED, messageOf(error));
    }
  }

  private async handleNewGatewaySession(raw: Buffer): Promise<void> {
    const request = parseObject(raw);
    const id = field(raw, "id");
    const nameValue = request.name;
    if (nameValue !== undefined && nameValue !== null && typeof nameValue !== "string") {
      await this.sendResponse(id, "gw_new_session", false, CODE_BAD_FRAME, "name must be a string");
      return;
    }
    const name = typeof nameValue === "string" ? nameValue : "";
    const piArgsValue = request.piArgs;
    if (piArgsValue !== undefined && piArgsValue !== null &&
      (!Array.isArray(piArgsValue) || !piArgsValue.every((item) => typeof item === "string"))) {
      await this.sendResponse(id, "gw_new_session", false, CODE_BAD_FRAME, "piArgs must be an array of strings");
      return;
    }
    const tagsValue = request.tags;
    if (tagsValue !== undefined && tagsValue !== null &&
      (typeof tagsValue !== "object" || Array.isArray(tagsValue) ||
        Object.values(tagsValue).some((value) => value !== null && typeof value !== "string"))) {
      await this.sendResponse(id, "gw_new_session", false, CODE_BAD_FRAME, "tags must be an object of string values");
      return;
    }
    const piArgs = Array.isArray(piArgsValue) ? piArgsValue as string[] : [];
    const tags = tagsValue && typeof tagsValue === "object"
      ? Object.fromEntries(Object.entries(tagsValue).map(([key, value]) => [key, value === null ? "" : value])) as Record<string, string>
      : undefined;
    let spec: PiArgsSpec;
    try { spec = parsePiArgs(piArgs); }
    catch (error) { await this.sendResponse(id, "gw_new_session", false, CODE_BAD_FRAME, messageOf(error)); return; }
    if (name) spec.setName(name);
    if (request.cwd !== undefined && request.cwd !== null && typeof request.cwd !== "string") {
      await this.sendResponse(id, "gw_new_session", false, CODE_BAD_FRAME, "cwd must be a string");
      return;
    }
    let cwd = this.cwd || process.cwd();
    const requestedCwd = stringProperty(request, "cwd");
    if (requestedCwd) {
      try { cwd = await resolveCwd(requestedCwd); }
      catch (error) { await this.sendResponse(id, "gw_new_session", false, CODE_BAD_FRAME, messageOf(error)); return; }
    }
    try {
      const creator = this.ref();
      creator.tags = tags ? { ...tags } : undefined;
      const actor = await this.daemon.createActor("", spec, cwd, creator);
      if (!await this.bind(actor, true)) throw new Error("session is shutting down");
      const stateRecord = await actor.call({ type: "get_state" }, 10_000);
      const state = parseStateData(stateRecord);
      if (state.sessionFile) await this.daemon.persistActorSpawn(actor, state.sessionFile);
      if (!state.sessionFile) {
        await this.sendResponse(id, "gw_new_session", false, CODE_SESSION_CRASHED, "pi reported no session file (is --no-session set?)");
        return;
      }
      await this.sendResponse(id, "gw_new_session", true, "", "", { path: state.sessionFile, name: state.sessionName, sessionId: state.sessionId });
    } catch (error) {
      await this.sendResponse(id, "gw_new_session", false, CODE_SESSION_CRASHED, messageOf(error));
    }
  }

  private async handleReloadSession(raw: Buffer): Promise<void> {
    const command = "gw_reload_session";
    const id = field(raw, "id");
    const request = parseObject(raw);
    if (request.session !== undefined && request.session !== null && typeof request.session !== "string") {
      await this.sendResponse(id, command, false, CODE_BAD_FRAME, "session must be a string");
      return;
    }
    if (request.force !== undefined && request.force !== null && typeof request.force !== "boolean") {
      await this.sendResponse(id, command, false, CODE_BAD_FRAME, "force must be a boolean");
      return;
    }
    const parseArgs = (key: "piArgs" | "addPiArgs"): string[] | undefined => {
      const value = request[key];
      if (value === undefined || value === null) return undefined;
      if (!Array.isArray(value) || !value.every((item) => typeof item === "string")) throw new Error(`${key} must be an array of strings`);
      return value;
    };
    let replacementArgs: string[] | undefined;
    let additionArgs: string[] | undefined;
    try { replacementArgs = parseArgs("piArgs"); additionArgs = parseArgs("addPiArgs"); }
    catch (error) { await this.sendResponse(id, command, false, CODE_BAD_FRAME, messageOf(error)); return; }
    if (replacementArgs !== undefined && additionArgs !== undefined) {
      await this.sendResponse(id, command, false, CODE_BAD_FRAME, "piArgs and addPiArgs are mutually exclusive");
      return;
    }
    if (additionArgs !== undefined && additionArgs.length === 0) {
      await this.sendResponse(id, command, false, CODE_BAD_FRAME, "addPiArgs must contain at least one spawn argument");
      return;
    }
    let replacement: PiArgsSpec | undefined;
    let additions: PiArgsSpec | undefined;
    try {
      if (replacementArgs !== undefined) replacement = parsePiArgs(replacementArgs);
      if (additionArgs !== undefined) additions = parsePiArgs(additionArgs);
    } catch (error) { await this.sendResponse(id, command, false, CODE_BAD_FRAME, messageOf(error)); return; }
    try {
      await this.daemon.reloadTarget(stringProperty(request, "session") ?? "", this.actor, this.id, this.ref(),
        request.force === true, replacement, additions);
      await this.sendResponse(id, command, true);
    } catch (error) {
      const gateway = asGatewayError(error);
      await this.sendResponse(id, command, false, gateway.code, gateway.message);
    }
  }

  private async handleStopSession(raw: Buffer, deleteFile: boolean): Promise<void> {
    const command = deleteFile ? "gw_delete_session" : "gw_stop_session";
    const id = field(raw, "id");
    const request = parseObject(raw);
    if (request.session !== undefined && request.session !== null && typeof request.session !== "string") {
      await this.sendResponse(id, command, false, CODE_BAD_FRAME, "session must be a string");
      return;
    }
    if (request.force !== undefined && request.force !== null && typeof request.force !== "boolean") {
      await this.sendResponse(id, command, false, CODE_BAD_FRAME, "force must be a boolean");
      return;
    }
    try {
      const result = await this.daemon.stopTarget(stringProperty(request, "session") ?? "", this.actor, this.id, request.force === true, deleteFile);
      const data: RpcObject = {
        path: result.path, name: result.name, sessionId: result.sessionId,
        piStopped: result.piStopped, detachedClients: result.detachedClients, unbound: result.unbound,
      };
      if (deleteFile) data.fileDeleted = result.fileDeleted === true;
      await this.sendResponse(id, command, true, "", "", data);
    } catch (error) {
      const gateway = asGatewayError(error);
      await this.sendResponse(id, command, false, gateway.code, gateway.message);
    }
  }

  private async handleListSessions(raw: Buffer): Promise<void> {
    const id = field(raw, "id");
    const request = parseObject(raw);
    const filterValue = request.filter;
    if (filterValue !== undefined && filterValue !== null && !isObject(filterValue)) {
      await this.sendResponse(id, "gw_list_sessions", false, CODE_BAD_FRAME, "filter must be an object");
      return;
    }
    const filter = isObject(filterValue) ? filterValue : {};
    for (const [key, typeName, valid] of [
      ["cwd", "a string", (value: unknown) => typeof value === "string"],
      ["live", "a boolean", (value: unknown) => typeof value === "boolean"],
      ["limit", "an integer", (value: unknown) => typeof value === "number" && Number.isInteger(value)],
    ] as const) {
      const value = filter[key];
      if (value !== undefined && value !== null && !valid(value)) {
        await this.sendResponse(id, "gw_list_sessions", false, CODE_BAD_FRAME, `filter.${key} must be ${typeName}`);
        return;
      }
    }
    const cwd = typeof filter.cwd === "string" ? filter.cwd : "";
    const live = filter.live === true;
    const limit = typeof filter.limit === "number" ? filter.limit : 0;
    try {
      const sessions = await this.daemon.listSessions({ cwd, live, limit });
      await this.sendResponse(id, "gw_list_sessions", true, "", "", { sessions });
    } catch (error) { await this.sendResponse(id, "gw_list_sessions", false, CODE_BAD_FRAME, messageOf(error)); }
  }

  private async bind(actor: SessionActor, startLive: boolean, attachReservation = false): Promise<boolean> {
    this.unbind();
    try {
      const subscriber = actor.subscribe(this.id, {
        buffer: this.daemon.subscriberBuffer,
        allowLossy: this.allowLossy,
        filterUnowned: !this.has("observe"),
        onOverflow: () => this.closeSlow(),
      });
      this.generation++;
      if (!await actor.attachSync(this)) {
        subscriber.close();
        return false;
      }
      this.actor = actor;
      this.subscriber = subscriber;
      this.deletedUnbound = false;
      this.daemon.countMetric("attaches");
      if (startLive) this.startPump(0);
      return true;
    } finally {
      if (attachReservation) this.daemon.completeAttach(actor);
    }
  }

  private unbind(): void {
    const actor = this.actor;
    const subscriber = this.subscriber;
    this.actor = undefined;
    this.subscriber = undefined;
    if (actor) {
      actor.unsubscribe(this.id);
      actor.detach(this.id);
    } else subscriber?.close();
  }

  private startPump(watermark: number): void {
    const subscriber = this.subscriber;
    const actor = this.actor;
    if (!subscriber || !actor) return;
    const generation = this.generation;
    void this.pump(subscriber, generation, watermark).catch(() => this.close());
  }

  private async pump(subscriber: Subscriber, generation: number, watermark: number): Promise<void> {
    const coalescer = new DeltaCoalescer();
    const flush = async (): Promise<boolean> => {
      for (const record of coalescer.flush()) {
        if (!await this.sendRecord(record, generation)) return false;
      }
      return true;
    };
    const ticker = setInterval(() => { void flush().catch(() => {}); }, Math.max(1, this.daemon.deltaFlushMs));
    try {
      for (;;) {
        const record = await subscriber.next();
        if (record === undefined) {
          if (this.closed || this.subscriber !== subscriber || this.generation !== generation) return;
          if (!await flush()) return;
          if (subscriber.dead) { await this.closeSlow(); return; }
          await this.sendError(CODE_SESSION_CRASHED, "session stopped");
          await this.closeAfterFlush();
          return;
        }
        if (this.subscriber !== subscriber || this.generation !== generation) return;
        if (record.seq !== 0 && record.seq <= watermark) continue;
        for (const ready of coalescer.offer(record)) {
          if (!await this.sendRecord(ready, generation)) {
            if (this.closed) return;
            await this.closeSlow();
            return;
          }
        }
        if (record.type === "gw_session_state") {
          const state = field(record.raw, "state");
          const reason = field(record.raw, "reason");
          if (state === "deleted" || (state === "stopped" && (reason === "requested" || reason === "forced"))) {
            if (state === "deleted") this.deletedUnbound = true;
            this.unbind();
            return;
          }
          if (state === SESSION_CRASHED) continue;
          if (state === SESSION_STOPPED) {
            await flush();
            await this.sendError(CODE_SESSION_CRASHED, "session stopped");
            await this.closeAfterFlush();
            return;
          }
        }
      }
    } finally { clearInterval(ticker); }
  }

  private async sendRecord(record: ProtocolRecord, generation: number): Promise<boolean> {
    if (generation !== this.generation) return true;
    if (this.closed) return false;
    const raw = transformForClient(record, this.id);
    if (!raw) return true;
    await this.sendRaw(raw);
    return !this.closed;
  }

  private async sendResponse(id: string, command: string, success: boolean, code = "", message = "", data?: unknown): Promise<void> {
    await this.sendJSON(response(id, command, success, code, message, data));
  }

  private async sendError(code: string, message: string): Promise<void> {
    await this.sendJSON({ type: "gw_error", code, message });
  }

  private async sendJSON(value: unknown): Promise<void> { await this.sendRaw(Buffer.from(JSON.stringify(value), "utf8")); }

  private async sendRaw(raw: Uint8Array): Promise<void> {
    if (this.closed || this.closing) return;
    if (this.pendingWrites >= OUT_QUEUE_DEPTH) {
      this.closeSlow();
      return;
    }
    await this.enqueueRaw(raw);
  }

  private async enqueueRaw(raw: Uint8Array): Promise<void> {
    if (this.closed) return;
    if (this.pendingWrites >= OUT_QUEUE_DEPTH) {
      void this.close();
      return;
    }
    const frame = encodeFrame(raw);
    this.pendingWrites += 1;
    const write = this.writeTail.then(() => new Promise<void>((resolveWrite, reject) => {
      if (this.closed) { resolveWrite(); return; }
      this.socket.write(frame, (error?: Error | null) => {
        if (error) { reject(error); return; }
        this.daemon.countMetric("frames_out");
        this.daemon.countMetric("bytes_out", raw.length);
        resolveWrite();
      });
    }));
    const tracked = write.finally(() => { this.pendingWrites -= 1; });
    this.writeTail = tracked.catch(() => { void this.close(); });
    await tracked;
  }

  private async closeAfterFlush(): Promise<void> {
    await Promise.race([this.writeTail.catch(() => {}), new Promise<void>((resolveWait) => setTimeout(resolveWait, CLOSE_FLUSH_MS))]);
    await this.close();
  }

  private closeSlow(): void {
    if (this.closed || this.closing) return;
    this.closing = true;
    if (this.pendingWrites >= OUT_QUEUE_DEPTH) {
      void this.close();
      return;
    }
    const notice = Buffer.from(JSON.stringify({
      type: "gw_error", code: CODE_SLOW_CONSUMER, message: "client is not reading; reconnect and resync",
    }), "utf8");
    void this.enqueueRaw(notice).catch(() => {});
    void this.closeAfterFlush().catch(() => { void this.close(); });
  }

  private async fail(error: unknown): Promise<void> {
    if (this.closed) return;
    await this.sendError(CODE_BAD_FRAME, messageOf(error)).catch(() => {});
    await this.closeAfterFlush();
  }
}

class GatewayError extends Error {
  readonly code: string;
  constructor(code: string, message: string) { super(message); this.code = code; }
}

function transformForClient(record: ProtocolRecord, clientId: string): Buffer | undefined {
  if (record.type === "response") {
    if (record.owner && record.owner !== clientId) return undefined;
    return record.owner === clientId ? restoreId(record.raw, clientId) : record.raw;
  }
  if (record.type === "gw_error") return record.owner && record.owner !== clientId ? undefined : record.raw;
  if (record.owner && record.type === "extension_ui_request") return record.owner === clientId ? record.raw : undefined;
  if (record.owner && record.type === "bash_execution_update") {
    if (record.owner === clientId) return restoreId(record.raw, clientId);
    const value = parseObject(record.raw);
    value.gw_owner = record.owner;
    return Buffer.from(JSON.stringify(value), "utf8");
  }
  return record.raw;
}

function parseStateData(record: ProtocolRecord): { sessionFile: string; sessionName: string; sessionId: string; thinkingLevel: string; model: { id: string; provider: string } } {
  const value = parseObject(record.raw);
  const data = isObject(value.data) ? value.data : {};
  const model = isObject(data.model) ? data.model : {};
  return {
    sessionFile: stringProperty(data, "sessionFile") ?? "",
    sessionName: stringProperty(data, "sessionName") ?? "",
    sessionId: stringProperty(data, "sessionId") ?? "",
    thinkingLevel: stringProperty(data, "thinkingLevel") ?? "",
    model: { id: stringProperty(model, "id") ?? "", provider: stringProperty(model, "provider") ?? "" },
  };
}
function dataOf(record: ProtocolRecord): JsonValue | undefined { return dataField(record.raw); }
function runtimeCommand(command: string, value: RpcObject): RuntimeCommand { return { command, raw: Buffer.from(JSON.stringify(value), "utf8") }; }

async function resolveCwd(cwd: string | undefined): Promise<string> {
  if (!cwd) return process.cwd();
  if (!isAbsolute(cwd)) throw new Error(`cwd must be an absolute path: ${JSON.stringify(cwd)}`);
  const info = await stat(cwd);
  if (!info.isDirectory()) throw new Error(`cwd ${cwd} is not a directory`);
  return resolve(cwd);
}

async function discoverPiVersion(bin: string): Promise<string> {
  try {
    const { stdout } = await execFileAsync(bin, ["--version"], { timeout: 5000, encoding: "utf8" });
    return stdout.trim();
  } catch { return ""; }
}

function constantTimeEqual(left: string, right: string): boolean {
  const a = Buffer.from(left);
  const b = Buffer.from(right);
  const width = Math.max(a.length, b.length, 1);
  const paddedA = Buffer.alloc(width);
  const paddedB = Buffer.alloc(width);
  a.copy(paddedA);
  b.copy(paddedB);
  return timingSafeEqual(paddedA, paddedB) && a.length === b.length;
}

function canonicalPath(path: string): string {
  if (!path) return "";
  const home = process.env.HOME ?? process.cwd();
  const expanded = path === "~" ? home : path.startsWith("~/") ? resolve(home, path.slice(2)) : path;
  const absolute = resolve(expanded);
  try { return realpathSync(absolute); }
  catch {
    try { return resolve(realpathSync(dirname(absolute)), basename(absolute)); }
    catch { return absolute; }
  }
}
async function deleteSessionFile(path: string): Promise<boolean> {
  if (!path) return false;
  try { await unlink(path); }
  catch (error) {
    if (error !== null && typeof error === "object" && "code" in error && error.code === "ENOENT") return false;
    throw error;
  }
  const directory = dirname(path);
  const name = basename(directory);
  if (name.length > 4 && name.startsWith("--") && name.endsWith("--")) await rmdir(directory).catch(() => {});
  return true;
}

function actorCatalogInfo(actor: SessionActor): SessionInfo {
  return { path: canonicalPath(actor.path), id: actor.sessionId, cwd: actor.cwd, name: actor.sessionName,
    title: "", messageCount: 0, lastActivity: new Date(0) };
}
function formatCatalogTime(value: Date): string | undefined {
  if (value.getTime() === 0) return undefined;
  return value.toISOString().replace(".000Z", "Z");
}
function looksLikePath(target: string): boolean { return target.includes("/") || target.endsWith(".jsonl") || target.startsWith("~"); }
function splitHostPort(addr: string): [string, string] {
  if (addr.startsWith("[")) {
    const end = addr.indexOf("]");
    if (end < 0 || addr[end + 1] !== ":") throw new Error(`config: invalid address ${JSON.stringify(addr)}`);
    return [addr.slice(1, end), addr.slice(end + 2)];
  }
  const index = addr.lastIndexOf(":");
  if (index < 0) throw new Error(`config: invalid address ${JSON.stringify(addr)}`);
  return [addr.slice(0, index), addr.slice(index + 1)];
}
function parseObject(raw: Uint8Array): Record<string, unknown> {
  try {
    const value: unknown = JSON.parse(Buffer.from(raw).toString("utf8"));
    return isObject(value) ? value : {};
  } catch { return {}; }
}
function isObject(value: unknown): value is Record<string, unknown> { return value !== null && typeof value === "object" && !Array.isArray(value); }
function stringProperty(value: Record<string, unknown>, key: string): string | undefined { return typeof value[key] === "string" ? value[key] as string : undefined; }
function arrayStringProperty(value: Record<string, unknown>, key: string): string[] | undefined {
  const raw = value[key];
  return Array.isArray(raw) && raw.every((item) => typeof item === "string") ? raw : undefined;
}
function messageOf(error: unknown): string { return error instanceof Error ? error.message : String(error); }
function asGatewayError(error: unknown): GatewayError {
  if (error instanceof GatewayError) return error;
  if (error instanceof Error) return new GatewayError(CODE_UNKNOWN_SESSION, error.message);
  return new GatewayError(CODE_UNKNOWN_SESSION, String(error));
}
