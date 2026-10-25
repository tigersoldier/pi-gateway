import { Buffer } from "node:buffer";
import { connect, Socket } from "node:net";
import type { Readable, Writable } from "node:stream";
import {
  addresses,
  configDir,
  readToken,
  tokenPath,
} from "./config.ts";
import {
  ALL_CAPABILITIES,
  JsonlDecoder,
  VERSION,
  encodeFrame,
  field,
  isGatewayType,
  response,
  stripGatewayFields,
  type Hello,
} from "./protocol.ts";
import { parsePiArgs, splitFlag, type PiArgsSpec } from "./piargs.ts";

const BINARY_NAME = "pi-gateway";
const CLIENT_NAME_ENV = "PI_GATEWAY_CLIENT_NAME";
const CLIENT_KIND_ENV = "PI_GATEWAY_CLIENT_KIND";
const DEFAULT_CLIENT_NAME = "pi-gateway";
const DEFAULT_CLIENT_KIND = "pilish";

const usage = `pi-gateway — pi RPC bridge to a pi-gatewayd session daemon

Usage:
  pi-gateway [client options] [pi options]

A UI spawns this binary instead of \`pi\`; it connects to pi-gatewayd,
performs the gateway handshake, and relays pristine pi RPC on stdin/stdout.
The session survives this process: reconnecting re-attaches to it.

Client options:
  --server <host:port>   daemon address (default: port file, then 127.0.0.1:7331)
  --port <n>             daemon port on 127.0.0.1
  --token-file <path>    token file (default: ~/.config/pi-gateway/token)
  --mode rpc             accepted and consumed; any other mode is an error
  --version              print the managed pi version (daemon-answered)
  --help                 print this help

Accepted pi options: trust (--approve/-a, --no-approve/-na), extensions (-e,
--extension, --no-extensions/-ne), resource/tool toggles (--skill,
--no-skills/-ns, --prompt-template, --no-prompt-templates/-np, --theme,
--no-themes, --no-context-files/-nc, --tools/-t, --exclude-tools/-xt,
--no-builtin-tools/-nbt, --no-tools/-nt, --system-prompt,
--append-system-prompt), and --provider, --model, --models, --thinking,
--name/-n, --session-dir, --no-session, --api-key, --offline, --verbose.
Other pi options are rejected.

Environment:
  PI_GATEWAY_CLIENT_NAME  client name advertised to the daemon (default ${DEFAULT_CLIENT_NAME})
  PI_GATEWAY_CLIENT_KIND  client kind advertised to the daemon (default ${DEFAULT_CLIENT_KIND})
  PI_GATEWAY_CONFIG_DIR   state directory override
`;

export interface BridgeCliArgs {
  server: string;
  port: number;
  tokenFile: string;
  piArgs: string[];
  mode: string;
  help: boolean;
  version: boolean;
}

export interface BridgeRunOptions {
  args: string[];
  stdin?: Readable;
  stdout?: Writable;
  stderr?: Writable;
  env?: NodeJS.ProcessEnv;
  cwd?: () => string;
  dialTimeoutMs?: number;
  /** Internal dial seam used to deterministically test transport failures. */
  dial?: (address: string, timeoutMs: number, signal?: AbortSignal) => Promise<Socket>;
  signal?: AbortSignal;
}

export function parseBridgeArgs(args: readonly string[]): BridgeCliArgs {
  const parsed: BridgeCliArgs = { server: "", port: 0, tokenFile: "", piArgs: [], mode: "", help: false, version: false };
  for (let i = 0; i < args.length; i++) {
    const arg = args[i]!;
    const [name, inline, hasInline] = splitFlag(arg);
    const value = (): string => {
      if (hasInline) return inline;
      if (i + 1 >= args.length) throw new Error(`option ${name} requires a value`);
      i++;
      return args[i]!;
    };
    switch (name) {
      case "--help":
      case "-h":
        parsed.help = true;
        break;
      case "--version":
        parsed.version = true;
        break;
      case "--server":
        parsed.server = value();
        break;
      case "--port": {
        const text = value();
        if (!/^[+-]?\d+$/.test(text)) throw new Error(`invalid --port ${JSON.stringify(text)}`);
        const port = Number(text);
        if (!Number.isSafeInteger(port) || port <= 0 || port > 65535) throw new Error(`invalid --port ${JSON.stringify(text)}`);
        parsed.port = port;
        break;
      }
      case "--token-file":
        parsed.tokenFile = value();
        break;
      case "--mode": {
        const mode = value();
        if (mode !== "rpc") throw new Error("only --mode rpc is supported (the pi TUI is not served by the gateway)");
        parsed.mode = mode;
        break;
      }
      default:
        parsed.piArgs.push(arg);
    }
  }
  if (!parsed.help && !parsed.version && parsed.mode !== "rpc") {
    throw new Error("pi-gateway is a bridge for pi-gatewayd: run it as `pi-gateway --mode rpc` (or --help/--version)");
  }
  return parsed;
}

export async function runBridge(options: BridgeRunOptions): Promise<number> {
  const stdin = options.stdin ?? process.stdin;
  const stdout = options.stdout ?? process.stdout;
  const stderr = options.stderr ?? process.stderr;
  const env = options.env ?? process.env;
  const writeErr = (text: string) => writeWritable(stderr, Buffer.from(text, "utf8"));
  let parsed: BridgeCliArgs;
  try { parsed = parseBridgeArgs(options.args); }
  catch (error) { await writeErr(`${BINARY_NAME}: ${messageOf(error)}\n`); return 2; }
  if (parsed.help) { await writeWritable(stdout, Buffer.from(usage, "utf8")); return 0; }
  let piSpec: PiArgsSpec;
  try { piSpec = parsePiArgs(parsed.piArgs); }
  catch (error) { await writeErr(`${BINARY_NAME}: ${messageOf(error)}\n`); return 2; }
  void piSpec;

  const configPath = configDir(env);
  const serverAddresses = await addresses(parsed.server, parsed.port, configPath);
  const tokenFile = parsed.tokenFile || tokenPath(configPath);
  let token: string;
  try { token = await readToken(tokenFile); }
  catch (error) { await writeErr(`${BINARY_NAME}: cannot read token ${tokenFile}: ${messageOf(error)}\n`); return 3; }

  const timeoutMs = options.dialTimeoutMs && options.dialTimeoutMs > 0 ? options.dialTimeoutMs : 10_000;
  const connected = await dialFirst(serverAddresses, timeoutMs, options.signal, options.dial ?? dial);
  if (connected.aborted) return 0;
  if (!connected.socket) {
    await writeErr(`${BINARY_NAME}: cannot reach pi-gatewayd at ${serverAddresses.join(", ")}: ${messageOf(connected.error)}\n(is pi-gatewayd running?)\n`);
    return 3;
  }
  const socket = connected.socket;
  const abortSocket = () => socket.destroy(new BridgeAbortError());
  options.signal?.addEventListener("abort", abortSocket, { once: true });
  const abortWait = abortPromise(options.signal);
  if (options.signal?.aborted) abortSocket();
  socket.setNoDelay(true);
  const server = new JsonlSocket(socket);
  let ui: JsonlInput | undefined;
  try {
    const hello: Hello = {
      type: "gw_hello",
      protocol: VERSION,
      token,
      client: {
        name: nonEmpty(env[CLIENT_NAME_ENV], DEFAULT_CLIENT_NAME),
        kind: nonEmpty(env[CLIENT_KIND_ENV], DEFAULT_CLIENT_KIND),
        capabilities: [...ALL_CAPABILITIES],
      },
      cwd: safeCwd(options.cwd ?? process.cwd),
      liveOnly: true,
      ...(parsed.piArgs.length ? { piArgs: parsed.piArgs } : {}),
    };
    await server.write(JSON.stringify(hello));
    const welcome = await awaitWelcome(server);
    if (options.signal?.aborted) return 0;
    if (welcome.error) {
      await writeErr(`${BINARY_NAME}: ${welcome.error}\n`);
      return 3;
    }
    if (parsed.version) {
      const version = welcome.value?.piVersion;
      if (typeof version !== "string" || !version.trim()) {
        await writeErr(`${BINARY_NAME}: daemon could not determine the managed pi version\n`);
        return 3;
      }
      await writeWritable(stdout, Buffer.from(`${version}\n`, "utf8"));
      return 0;
    }

    const relayState = { deleted: false };
    const uiInput = new JsonlInput(stdin);
    ui = uiInput;
    const uiTask = relayUi(uiInput, server, stdout, relayState).then((error) => ({
      side: error instanceof DaemonWriteError ? "daemon" as const : "ui" as const,
      error,
    }));
    const daemonTask = relayDaemon(server, stdout, relayState).then((error) => ({ side: "daemon" as const, error }));
    const result = await Promise.race([
      uiTask,
      daemonTask,
      abortWait.promise.then(() => ({ side: "signal" as const, error: undefined })),
    ]);
    socket.destroy();
    if (options.signal?.aborted || result.side === "signal") return 0;
    if (!result.error) return 0;
    if (result.side === "daemon") {
      await writeErr(`${BINARY_NAME}: daemon connection lost: ${messageOf(result.error)}\n`);
      return 3;
    }
    if (result.error instanceof SessionDeletedError) {
      await writeErr(`${BINARY_NAME}: ${result.error.message}\n`);
      return 3;
    }
    await writeErr(`${BINARY_NAME}: ${messageOf(result.error)}\n`);
    return 2;
  } catch (error) {
    if (options.signal?.aborted || error instanceof BridgeAbortError) return 0;
    await writeErr(`${BINARY_NAME}: ${messageOf(error)}\n`);
    return 3;
  } finally {
    ui?.close();
    server.close();
    options.signal?.removeEventListener("abort", abortSocket);
    abortWait.dispose();
    socket.destroy();
  }
}

interface WelcomeResult { value?: Record<string, unknown>; error?: string }
async function awaitWelcome(server: JsonlSocket): Promise<WelcomeResult> {
  for (;;) {
    let raw: Buffer;
    try { raw = await server.read(); }
    catch (error) { return { error: `no gw_welcome from daemon: ${messageOf(error)}` }; }
    const type = field(raw, "type");
    if (type === "gw_welcome") {
      try {
        const value: unknown = JSON.parse(raw.toString("utf8"));
        if (!isRecord(value)) return { error: "malformed gw_welcome" };
        const invalidField = validateWelcome(value);
        return invalidField ? { error: `malformed gw_welcome: ${invalidField}` } : { value };
      } catch (error) { return { error: `malformed gw_welcome: ${messageOf(error)}` }; }
    }
    if (type === "gw_error") {
      const code = field(raw, "code");
      const message = field(raw, "message");
      if (code === "unauthorized") return { error: `daemon rejected the token: ${message}` };
      return { error: `daemon rejected the handshake: ${message}` };
    }
  }
}

async function relayUi(ui: JsonlInput, server: JsonlSocket, stdout: Writable, state: { deleted: boolean }): Promise<Error | undefined> {
  try {
    for (;;) {
      const raw = await ui.read();
      if (state.deleted && !isGatewayType(field(raw, "type"))) {
        const type = field(raw, "type");
        await writeWritable(stdout, encodeFrame(Buffer.from(JSON.stringify(response(field(raw, "id"), type, false, "", "session was deleted by another client")), "utf8")));
        return new SessionDeletedError();
      }
      try { await server.write(raw.toString("utf8")); }
      catch (error) { return new DaemonWriteError(errorOf(error)); }
    }
  } catch (error) {
    return isEof(error) ? undefined : errorOf(error);
  }
}

async function relayDaemon(server: JsonlSocket, stdout: Writable, state: { deleted: boolean }): Promise<Error | undefined> {
  try {
    for (;;) {
      const raw = await server.read();
      const type = field(raw, "type");
      if (type === "gw_session_state" && field(raw, "state") === "deleted") state.deleted = true;
      if (isGatewayType(type)) continue;
      if (field(raw, "gw_owner") !== "") continue;
      await writeWritable(stdout, encodeFrame(stripGatewayFields(raw)));
    }
  } catch (error) {
    return errorOf(error);
  }
}

class SessionDeletedError extends Error {
  constructor() { super("session was deleted by another client"); }
}

class DaemonWriteError extends Error {
  constructor(cause: Error) { super(cause.message, { cause }); }
}

class BridgeAbortError extends Error {
  constructor() { super("bridge cancelled"); }
}

function abortPromise(signal?: AbortSignal): { promise: Promise<void>; dispose(): void } {
  if (!signal) return { promise: new Promise(() => {}), dispose: () => {} };
  if (signal.aborted) return { promise: Promise.resolve(), dispose: () => {} };
  let listener: (() => void) | undefined;
  const promise = new Promise<void>((resolve) => {
    listener = resolve;
    signal.addEventListener("abort", listener, { once: true });
  });
  return {
    promise,
    dispose: () => { if (listener) signal.removeEventListener("abort", listener); },
  };
}

function safeCwd(cwd: () => string): string {
  try { return cwd(); } catch { return ""; }
}

class JsonlSocket {
  private readonly input: JsonlInput;
  private readonly socket: Socket;
  constructor(socket: Socket) { this.socket = socket; this.input = new JsonlInput(socket, true); }
  read(): Promise<Buffer> { return this.input.read(); }
  write(raw: string | Uint8Array): Promise<void> { return writeWritable(this.socket, encodeFrame(Buffer.from(raw))); }
  close(): void { this.input.close(); }
}

export class JsonlInput {
  private readonly decoder = new JsonlDecoder();
  private readonly frames: Buffer[] = [];
  private readonly waiters: Array<{ resolve(value: Buffer): void; reject(error: Error): void }> = [];
  private readonly readable: Readable | Socket;
  private readonly onData: (chunk: Buffer | string) => void;
  private readonly onEnd: () => void;
  private readonly onError: (error: Error) => void;
  private readonly onClose: () => void;
  private readonly keepSocketErrorUntilClose: boolean;
  private ended = false;
  private disposed = false;
  private failure?: Error;

  constructor(readable: Readable | Socket, keepSocketErrorUntilClose = false) {
    this.readable = readable;
    this.keepSocketErrorUntilClose = keepSocketErrorUntilClose;
    this.onData = (chunk) => {
      if (this.disposed) return;
      try {
        this.push(this.decoder.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)));
        if (this.frames.length > 0) this.readable.pause();
      } catch (error) { this.fail(errorOf(error)); }
    };
    this.onEnd = () => {
      if (this.disposed) return;
      try { this.push(this.decoder.finish()); this.ended = true; this.flushEnd(); }
      catch (error) { this.fail(errorOf(error)); }
    };
    this.onError = (error) => { if (!this.disposed) this.fail(error); };
    this.onClose = () => {
      this.readable.off("error", this.onError);
      if (!this.disposed && !this.ended && !this.failure) { this.ended = true; this.flushEnd(); }
    };
    readable.on("data", this.onData);
    readable.once("end", this.onEnd);
    readable.on("error", this.onError);
    if (readable instanceof Socket) readable.once("close", this.onClose);
  }

  read(): Promise<Buffer> {
    const frame = this.frames.shift();
    if (frame) {
      if (this.frames.length === 0 && !this.ended && !this.disposed) this.readable.resume();
      return Promise.resolve(frame);
    }
    if (this.failure) return Promise.reject(this.failure);
    if (this.ended) return Promise.reject(new Error("EOF"));
    return new Promise((resolve, reject) => this.waiters.push({ resolve, reject }));
  }

  close(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.readable.off("data", this.onData);
    this.readable.off("end", this.onEnd);
    if (this.readable instanceof Socket && this.keepSocketErrorUntilClose) {
      // Keep consuming a pending destroy(error) until the socket's close event.
    } else {
      this.readable.off("error", this.onError);
      if (this.readable instanceof Socket) this.readable.off("close", this.onClose);
    }
    this.readable.pause();
    this.frames.length = 0;
    this.ended = true;
    this.flushEnd();
  }

  private push(frames: Buffer[]): void {
    for (const frame of frames) {
      const waiter = this.waiters.shift();
      if (waiter) waiter.resolve(frame);
      else this.frames.push(frame);
    }
  }

  private fail(error: Error): void {
    if (this.disposed) return;
    this.failure = error;
    this.ended = true;
    this.flushEnd();
  }

  private flushEnd(): void {
    if (this.frames.length > 0) return;
    for (const waiter of this.waiters.splice(0)) waiter.reject(this.failure ?? new Error("EOF"));
  }
}

async function dialFirst(
  addrs: string[],
  timeoutMs: number,
  signal: AbortSignal | undefined,
  dialer: (address: string, timeoutMs: number, signal?: AbortSignal) => Promise<Socket>,
): Promise<{ socket?: Socket; error?: Error; aborted?: boolean }> {
  let lastError: Error | undefined;
  for (const address of addrs) {
    try { return { socket: await dialer(address, timeoutMs, signal) }; }
    catch (error) {
      if (error instanceof BridgeAbortError) return { aborted: true };
      lastError = errorOf(error);
    }
  }
  return { error: lastError ?? new Error("no daemon addresses configured") };
}

function dial(address: string, timeoutMs: number, signal?: AbortSignal): Promise<Socket> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) { reject(new BridgeAbortError()); return; }
    const [host, portText] = splitAddress(address);
    const port = Number(portText);
    const socket = connect({ host, port });
    let done = false;
    const timer = setTimeout(() => socket.destroy(new Error("connect timeout")), timeoutMs);
    const finish = (error?: Error) => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
      socket.off("connect", onConnect);
      socket.off("error", onError);
      if (error) { socket.destroy(); reject(error); }
      else resolve(socket);
    };
    const onConnect = () => finish();
    const onError = (error: Error) => finish(error);
    const onAbort = () => finish(new BridgeAbortError());
    socket.once("connect", onConnect);
    socket.once("error", onError);
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

function splitAddress(address: string): [string, string] {
  if (address.startsWith("[")) {
    const end = address.indexOf("]");
    if (end < 0 || address[end + 1] !== ":") throw new Error(`invalid address ${JSON.stringify(address)}`);
    return [address.slice(1, end), address.slice(end + 2)];
  }
  const index = address.lastIndexOf(":");
  if (index < 0) throw new Error(`invalid address ${JSON.stringify(address)}`);
  return [address.slice(0, index), address.slice(index + 1)];
}

function writeWritable(writable: Writable, data: Uint8Array): Promise<void> {
  return new Promise((resolve, reject) => {
    writable.write(data, (error?: Error | null) => error ? reject(error) : resolve());
  });
}

function nonEmpty(value: string | undefined, fallback: string): string { return value ? value : fallback; }
function isRecord(value: unknown): value is Record<string, unknown> { return value !== null && typeof value === "object" && !Array.isArray(value); }

function validateWelcome(welcome: Record<string, unknown>): string | undefined {
  for (const key of ["type", "clientId", "kind", "piVersion", "concurrency"]) {
    const error = invalidField(welcome, key, key, (value) => typeof value === "string");
    if (error) return error;
  }
  for (const key of ["protocol"]) {
    const error = invalidField(welcome, key, key, isInteger);
    if (error) return error;
  }
  for (const key of ["headSeq", "oldestSeq"]) {
    const error = invalidField(welcome, key, key, (value) => isInteger(value) && value >= 0);
    if (error) return error;
  }
  for (const key of ["granted", "features"]) {
    const error = invalidField(welcome, key, key, isStringArray);
    if (error) return error;
  }
  for (const key of ["resyncRequired"]) {
    const error = invalidField(welcome, key, key, (value) => typeof value === "boolean");
    if (error) return error;
  }
  const sessionError = validateObjectField(welcome, "session", "session", ["path", "name", "id"]);
  if (sessionError) return sessionError;
  const turnError = validateObjectField(welcome, "turn", "turn", ["state", "turnId"]);
  if (turnError) return turnError;
  const turn = welcome.turn;
  if (turn !== null && isRecord(turn)) {
    const queuedError = invalidField(turn, "queued", "turn.queued", isInteger);
    if (queuedError) return queuedError;
    const authorError = validateObjectField(turn, "author", "turn.author", ["clientId", "kind", "name"], true);
    if (authorError) return authorError;
  }
  const clientsError = invalidField(welcome, "clients", "clients", isClientSummaryArray);
  return clientsError;
}

function invalidField(
  object: Record<string, unknown>,
  key: string,
  path: string,
  valid: (value: unknown) => boolean,
): string | undefined {
  if (!Object.hasOwn(object, key) || object[key] === null || valid(object[key])) return undefined;
  return `field ${path} has an invalid type`;
}

function validateObjectField(
  object: Record<string, unknown>,
  key: string,
  path: string,
  stringFields: readonly string[],
  hasTags = false,
): string | undefined {
  if (!Object.hasOwn(object, key) || object[key] === null) return undefined;
  const nested = object[key];
  if (!isRecord(nested)) return `field ${path} has an invalid type`;
  for (const field of stringFields) {
    const error = invalidField(nested, field, `${path}.${field}`, (value) => typeof value === "string");
    if (error) return error;
  }
  return hasTags ? validateStringMapField(nested, "tags", `${path}.tags`) : undefined;
}

function validateStringMapField(object: Record<string, unknown>, key: string, path: string): string | undefined {
  return invalidField(object, key, path, (value) => isRecord(value) && Object.values(value).every((item) => item === null || typeof item === "string"));
}

function isInteger(value: unknown): value is number { return typeof value === "number" && Number.isFinite(value) && Number.isInteger(value); }
function isStringArray(value: unknown): boolean { return Array.isArray(value) && value.every((item) => item === null || typeof item === "string"); }
function isClientSummaryArray(value: unknown): boolean {
  return Array.isArray(value) && value.every((item) => item === null || (isRecord(item)
    && ["clientId", "name", "kind"].every((key) => !Object.hasOwn(item, key) || item[key] === null || typeof item[key] === "string")
    && (!Object.hasOwn(item, "capabilities") || item.capabilities === null || isStringArray(item.capabilities))
    && (!Object.hasOwn(item, "tags") || item.tags === null || (isRecord(item.tags)
      && Object.values(item.tags).every((tag) => tag === null || typeof tag === "string")))));
}
function isEof(error: unknown): boolean { return error instanceof Error && error.message === "EOF"; }
function errorOf(error: unknown): Error { return error instanceof Error ? error : new Error(String(error)); }
function messageOf(error: unknown): string { return error instanceof Error ? error.message : String(error); }
