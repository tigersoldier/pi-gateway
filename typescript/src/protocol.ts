import { Buffer } from "node:buffer";

export const VERSION = 1;
export const MAX_FRAME_BYTES = 16 << 20;
export const INTERNAL_ID_PREFIX = "gwint:";
export const FEATURE_SPAWN_CONFIG = "spawn_config";
export const FEATURE_SPAWN_CONFIG_ADDITIVE = "spawn_config_additive";
export const GATEWAY_FEATURES = [FEATURE_SPAWN_CONFIG, FEATURE_SPAWN_CONFIG_ADDITIVE] as const;

export class FrameTooLargeError extends Error {
  constructor() {
    super("protocol: frame exceeds max size");
    this.name = "FrameTooLargeError";
  }
}

export class NoWriterError extends Error {
  constructor() {
    super("protocol: codec has no writer");
    this.name = "NoWriterError";
  }
}

/** Incremental LF-only JSONL framing, matching pi's RPC framing rules. */
export class JsonlDecoder {
  private pending = Buffer.alloc(0);
  private readonly maxBytes: number;

  constructor(maxBytes = MAX_FRAME_BYTES) {
    this.maxBytes = maxBytes;
  }

  push(chunk: Uint8Array): Buffer[] {
    const input = Buffer.from(chunk);
    const frames: Buffer[] = [];
    let lineStart = 0;

    for (const [index, byte] of input.entries()) {
      if (byte !== 0x0a) continue;
      const segment = input.subarray(lineStart, index);
      // Go's codec bounds the wire record, including LF and a possible CR.
      if (this.pending.length + segment.length + 1 > this.maxBytes) throw new FrameTooLargeError();
      let frame = this.pending.length === 0 ? Buffer.from(segment) : Buffer.concat([this.pending, segment]);
      this.pending = Buffer.alloc(0);
      if (frame.length > 0 && frame[frame.length - 1] === 0x0d) frame = frame.subarray(0, frame.length - 1);
      frames.push(frame);
      lineStart = index + 1;
    }

    if (lineStart < input.length) {
      this.pending = Buffer.concat([this.pending, input.subarray(lineStart)]);
      if (this.pending.length > this.maxBytes) throw new FrameTooLargeError();
    }
    return frames;
  }

  /** Return a final unterminated record, if any. */
  finish(): Buffer[] {
    if (this.pending.length > this.maxBytes) throw new FrameTooLargeError();
    if (this.pending.length === 0) return [];
    let frame = this.pending;
    this.pending = Buffer.alloc(0);
    if (frame[frame.length - 1] === 0x0d) frame = frame.subarray(0, frame.length - 1);
    return [frame];
  }
}

export function encodeFrame(raw: Uint8Array): Buffer {
  const bytes = Buffer.from(raw);
  if (bytes.length === 0 || bytes[bytes.length - 1] !== 0x0a) {
    return Buffer.concat([bytes, Buffer.from([0x0a])]);
  }
  return bytes;
}

export function encodeJsonFrame(value: unknown): Buffer {
  const json = JSON.stringify(value);
  if (json === undefined) throw new TypeError("value is not JSON serializable");
  return encodeFrame(Buffer.from(json, "utf8"));
}

export interface GatewayClientInfo {
  name?: string;
  kind?: string;
  capabilities?: string[];
  tags?: Record<string, string>;
}

export interface ResumeCursor {
  sinceSeq?: number;
  leafEntryId?: string;
}

export interface Hello {
  type: "gw_hello";
  protocol: number;
  token: string;
  client: GatewayClientInfo;
  cwd?: string;
  session?: string;
  piArgs?: string[];
  resume?: ResumeCursor;
  liveOnly?: boolean;
  allowLossy?: boolean;
}

export interface ClientSummary extends GatewayClientInfo {
  clientId: string;
}

export interface ClientRef {
  clientId: string;
  kind?: string;
  name?: string;
  tags?: Record<string, string>;
}

export interface SessionRef {
  path: string;
  name?: string;
  id?: string;
}

export interface QueuePending {
  id: string;
  mode: string;
  author?: ClientRef;
  preview: string;
}

export interface SessionRow {
  path: string;
  name?: string;
  title?: string;
  id?: string;
  cwd?: string;
  live: boolean;
  isStreaming: boolean;
  messageCount: number;
  lastActivity?: string;
  createdBy?: ClientRef;
  clients?: ClientSummary[];
  spawn?: Record<string, string[]>;
}

export const CAP_OBSERVE = "observe";
export const CAP_INTERJECT = "interject";
export const CAP_PROMPT = "prompt";
export const CAP_UI = "ui";
export const CAP_CONTROL = "control";
export const CAP_ADMIN = "admin";
export const ALL_CAPABILITIES = [
  CAP_OBSERVE,
  CAP_INTERJECT,
  CAP_PROMPT,
  CAP_UI,
  CAP_CONTROL,
  CAP_ADMIN,
] as const;

export const ROLE_ADMIN = "admin";
export const ROLE_OPERATOR = "operator";
export const ROLE_OBSERVER = "observer";
export const ROLE_NAMES = [ROLE_ADMIN, ROLE_OPERATOR, ROLE_OBSERVER] as const;

export class UnsupportedProtocolVersionError extends Error {
  constructor(version: number) {
    super(`protocol: unsupported protocol version: ${version} (daemon speaks ${VERSION})`);
    this.name = "UnsupportedProtocolVersionError";
  }
}

const roleCapabilities: Record<string, readonly string[]> = {
  [ROLE_ADMIN]: ALL_CAPABILITIES,
  [ROLE_OPERATOR]: [CAP_OBSERVE, CAP_INTERJECT, CAP_PROMPT, CAP_UI],
  [ROLE_OBSERVER]: [CAP_OBSERVE],
};

export function getRoleCapabilities(role: string): string[] | undefined {
  if (!Object.hasOwn(roleCapabilities, role)) return undefined;
  return [...roleCapabilities[role]!];
}

const commandCapabilities: Readonly<Record<string, string>> = {
  get_state: CAP_OBSERVE,
  get_messages: CAP_OBSERVE,
  get_available_models: CAP_OBSERVE,
  get_available_thinking_levels: CAP_OBSERVE,
  get_session_stats: CAP_OBSERVE,
  get_entries: CAP_OBSERVE,
  get_tree: CAP_OBSERVE,
  get_fork_messages: CAP_OBSERVE,
  get_last_assistant_text: CAP_OBSERVE,
  get_commands: CAP_OBSERVE,
  export_html: CAP_OBSERVE,
  gw_list_sessions: CAP_OBSERVE,
  prompt: CAP_PROMPT,
  follow_up: CAP_PROMPT,
  new_session: CAP_PROMPT,
  fork: CAP_PROMPT,
  clone: CAP_PROMPT,
  bash: CAP_PROMPT,
  switch_session: CAP_PROMPT,
  steer: CAP_INTERJECT,
  abort: CAP_INTERJECT,
  abort_bash: CAP_INTERJECT,
  abort_retry: CAP_INTERJECT,
  clear_queue: CAP_INTERJECT,
  extension_ui_response: CAP_UI,
  gw_reload_session: CAP_CONTROL,
  set_model: CAP_CONTROL,
  cycle_model: CAP_CONTROL,
  set_thinking_level: CAP_CONTROL,
  cycle_thinking_level: CAP_CONTROL,
  set_steering_mode: CAP_CONTROL,
  set_follow_up_mode: CAP_CONTROL,
  compact: CAP_CONTROL,
  set_auto_compaction: CAP_CONTROL,
  set_auto_retry: CAP_CONTROL,
  set_session_name: CAP_CONTROL,
  gw_new_session: CAP_ADMIN,
  gw_stop_session: CAP_ADMIN,
  gw_delete_session: CAP_ADMIN,
  gw_ping: "",
  gw_bye: "",
};

export function isSupportedCommand(command: string): boolean {
  return Object.hasOwn(commandCapabilities, command);
}

export function commandCapability(command: string): string {
  return commandCapabilities[command] ?? "";
}

export function normalizeCapabilities(caps: readonly string[]): { known: string[]; unknown: string[] } {
  const requested = new Set(caps);
  const known = ALL_CAPABILITIES.filter((capability) => requested.has(capability));
  const unknown: string[] = [];
  for (const capability of caps) {
    if (!(ALL_CAPABILITIES as readonly string[]).includes(capability) && !unknown.includes(capability)) {
      unknown.push(capability);
    }
  }
  return { known: [...known], unknown };
}

export interface ProtocolRecord {
  seq: number;
  raw: Buffer;
  type: string;
  id: string;
  owner: string;
}

export function decodeRecord(raw: Uint8Array): ProtocolRecord {
  const value = parseObject(Buffer.from(raw).toString("utf8"), "decode frame");
  const type = stringOrEmpty(value.type);
  const id = stringOrEmpty(value.id);
  let owner = "";
  const [namespace] = splitNamespaceId(id);
  if (namespace !== "" && namespace !== id) owner = namespace;
  else if (isInternalId(id)) owner = "";
  return { seq: 0, raw: Buffer.from(raw), type, id, owner };
}

export function parseHello(raw: Uint8Array): Hello {
  const value = parseObject(Buffer.from(raw).toString("utf8"), "gw_hello");
  const type = optionalStringField(value, "type") ?? "";
  if (type !== "gw_hello") throw new Error(`protocol: expected gw_hello, got ${JSON.stringify(type)}`);
  const protocolValue = value.protocol;
  const protocol = protocolValue === undefined || protocolValue === null ? 0 : protocolValue;
  if (typeof protocol !== "number" || !Number.isInteger(protocol)) {
    throw new Error("protocol: gw_hello: protocol must be an integer");
  }
  if (protocol !== VERSION) throw new UnsupportedProtocolVersionError(protocol);

  const clientValue = value.client === undefined || value.client === null ? {} : value.client;
  if (!isObject(clientValue)) throw new Error("protocol: gw_hello: client must be an object");
  const client: GatewayClientInfo = {};
  const clientName = optionalStringField(clientValue, "name");
  const clientKind = optionalStringField(clientValue, "kind");
  const capabilities = optionalStringArray(clientValue, "capabilities");
  const tags = optionalStringMap(clientValue, "tags");
  if (clientName !== undefined) client.name = clientName;
  if (clientKind !== undefined) client.kind = clientKind;
  if (capabilities !== undefined) client.capabilities = capabilities;
  if (tags !== undefined) client.tags = tags;

  const hello: Hello = { type: "gw_hello", protocol, token: optionalStringField(value, "token") ?? "", client };
  const cwd = optionalStringField(value, "cwd");
  const session = optionalStringField(value, "session");
  const piArgs = optionalStringArray(value, "piArgs");
  const liveOnly = optionalBooleanField(value, "liveOnly");
  const allowLossy = optionalBooleanField(value, "allowLossy");
  if (cwd !== undefined) hello.cwd = cwd;
  if (session !== undefined) hello.session = session;
  if (piArgs !== undefined) hello.piArgs = piArgs;
  if (liveOnly !== undefined) hello.liveOnly = liveOnly;
  if (allowLossy !== undefined) hello.allowLossy = allowLossy;
  if (value.resume !== undefined && value.resume !== null) {
    if (!isObject(value.resume)) throw new Error("protocol: gw_hello: resume must be an object");
    const resume: ResumeCursor = {};
    const sinceSeq = value.resume.sinceSeq;
    if (sinceSeq !== undefined && sinceSeq !== null) {
      if (typeof sinceSeq !== "number" || !Number.isSafeInteger(sinceSeq) || sinceSeq < 0) {
        throw new Error("protocol: gw_hello: resume.sinceSeq must be an unsigned integer");
      }
      resume.sinceSeq = sinceSeq;
    }
    const leafEntryId = optionalStringField(value.resume, "leafEntryId");
    if (leafEntryId !== undefined) resume.leafEntryId = leafEntryId;
    hello.resume = resume;
  }
  return hello;
}

export function response(
  id: string,
  command: string,
  success: boolean,
  code = "",
  error = "",
  data?: unknown,
): Record<string, unknown> {
  const result: Record<string, unknown> = { type: "response", command, success };
  if (id !== "") result.id = id;
  if (!success) {
    result.error = error;
    if (code !== "") result.code = code;
  }
  if (data !== undefined) result.data = data;
  return result;
}

export function isInternalId(id: string): boolean {
  return id.startsWith(INTERNAL_ID_PREFIX);
}

export function isGatewayType(type: string): boolean {
  return type.startsWith("gw_");
}

export function namespaceId(clientId: string, localId: string): string {
  return localId === "" ? clientId : `${clientId}:${localId}`;
}

export function splitNamespaceId(id: string): [clientId: string, localId: string] {
  const colon = id.indexOf(":");
  if (colon >= 0) return [id.slice(0, colon), id.slice(colon + 1)];
  if (id === "") return ["", ""];
  return ["", id];
}

export function rewriteCommand(raw: Uint8Array, id: string, type: string): Buffer {
  let value = parseObject(Buffer.from(raw).toString("utf8"), "rewrite command");
  if (id === "") value = withoutProperty(value, "id");
  else value.id = id;
  if (type !== "") value.type = type;
  return Buffer.from(JSON.stringify(value), "utf8");
}

export function rewriteId(raw: Uint8Array, id: string): Buffer {
  return rewriteCommand(raw, id, "");
}

export function field(raw: Uint8Array, name: string): string {
  try {
    return stringOrEmpty(parseObject(Buffer.from(raw).toString("utf8"), "field")[name]);
  } catch {
    return "";
  }
}

export function stamp(raw: Uint8Array, fields: Record<string, unknown>): Buffer {
  if (Object.keys(fields).length === 0) return Buffer.from(raw);
  const value = parseObject(Buffer.from(raw).toString("utf8"), "stamp");
  Object.assign(value, fields);
  return Buffer.from(JSON.stringify(value), "utf8");
}

export function restoreId(raw: Uint8Array, clientId: string): Buffer {
  const original = Buffer.from(raw);
  let value: Record<string, unknown>;
  try {
    value = parseObject(original.toString("utf8"), "restore id");
  } catch {
    return original;
  }
  const id = stringOrEmpty(value.id);
  const prefix = `${clientId}:`;
  if (id.startsWith(prefix)) value.id = id.slice(prefix.length);
  else if (id === clientId) value = withoutProperty(value, "id");
  else return original;
  return Buffer.from(JSON.stringify(value), "utf8");
}

export function stripGatewayFields(raw: Uint8Array): Buffer {
  const original = Buffer.from(raw);
  let value: Record<string, unknown>;
  try {
    value = parseObject(original.toString("utf8"), "strip gateway fields");
  } catch {
    return original;
  }
  const entries = Object.entries(value).filter(([key]) => !key.startsWith("gw_"));
  if (entries.length === Object.keys(value).length) return original;
  return Buffer.from(JSON.stringify(Object.fromEntries(entries)), "utf8");
}

export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };

export function dataField(raw: Uint8Array): JsonValue | undefined {
  try {
    const data = parseObject(Buffer.from(raw).toString("utf8"), "data field").data;
    return isJsonValue(data) ? data : undefined;
  } catch {
    return undefined;
  }
}

export function lagFrame(oldestSeq: number, headSeq: number): Record<string, unknown> {
  return { type: "gw_lag", oldestSeq, headSeq };
}

export interface ImageContent {
  type: "image";
  data: string;
  mimeType: string;
}

export function newImage(data: Uint8Array, mimeType: string): ImageContent {
  return { type: "image", data: Buffer.from(data).toString("base64"), mimeType };
}

export interface PiState {
  sessionFile: string;
  sessionName: string;
  sessionId: string;
  thinkingLevel: string;
  isStreaming: boolean;
  model: { id: string; provider: string };
}

export function parsePiState(raw: Uint8Array): PiState {
  const decoded = dataField(raw);
  let source: Record<string, unknown>;
  try {
    if (decoded === undefined) source = parseObject(Buffer.from(raw).toString("utf8"), "pi state");
    else if (isObject(decoded)) source = decoded;
    else source = {};
  } catch {
    source = {};
  }
  const model = isObject(source.model) ? source.model : {};
  return {
    sessionFile: stringOrEmpty(source.sessionFile),
    sessionName: stringOrEmpty(source.sessionName),
    sessionId: stringOrEmpty(source.sessionId),
    thinkingLevel: stringOrEmpty(source.thinkingLevel),
    isStreaming: source.isStreaming === true,
    model: { id: stringOrEmpty(model.id), provider: stringOrEmpty(model.provider) },
  };
}

export function boolField(raw: Uint8Array, name: string, fallback: boolean): boolean {
  try {
    const value = parseObject(Buffer.from(raw).toString("utf8"), "boolean field")[name];
    return typeof value === "boolean" ? value : fallback;
  } catch {
    return fallback;
  }
}

export function numField(raw: Uint8Array, name: string): number {
  try {
    const value = parseObject(Buffer.from(raw).toString("utf8"), "numeric field")[name];
    return typeof value === "number" ? value : 0;
  } catch {
    return 0;
  }
}

function parseObject(raw: string, context: string): Record<string, unknown> {
  let value: unknown;
  try {
    value = JSON.parse(raw);
  } catch (error) {
    throw new Error(`protocol: ${context}: ${error instanceof Error ? error.message : String(error)}`);
  }
  if (!isObject(value)) throw new Error(`protocol: ${context}: expected JSON object`);
  return value;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function isJsonValue(value: unknown): value is JsonValue {
  if (value === null || typeof value === "string" || typeof value === "boolean" || typeof value === "number") return true;
  if (Array.isArray(value)) return value.every(isJsonValue);
  return isObject(value) && Object.values(value).every(isJsonValue);
}

function optionalStringField(value: Record<string, unknown>, key: string): string | undefined {
  const field = value[key];
  if (field === undefined || field === null) return undefined;
  if (typeof field !== "string") throw new Error(`protocol: gw_hello: ${key} must be a string`);
  return field;
}

function optionalStringArray(value: Record<string, unknown>, key: string): string[] | undefined {
  const field = value[key];
  if (field === undefined || field === null) return undefined;
  if (!Array.isArray(field)) throw new Error(`protocol: gw_hello: ${key} must be an array of strings`);
  return field.map((item) => {
    if (item === null) return "";
    if (typeof item !== "string") throw new Error(`protocol: gw_hello: ${key} must be an array of strings`);
    return item;
  });
}

function optionalStringMap(value: Record<string, unknown>, key: string): Record<string, string> | undefined {
  const field = value[key];
  if (field === undefined || field === null) return undefined;
  if (!isObject(field)) throw new Error(`protocol: gw_hello: ${key} must be an object`);
  const result: Record<string, string> = Object.create(null) as Record<string, string>;
  for (const [name, item] of Object.entries(field)) {
    if (item === null) { result[name] = ""; continue; }
    if (typeof item !== "string") throw new Error(`protocol: gw_hello: ${key}.${name} must be a string`);
    result[name] = item;
  }
  return result;
}

function optionalBooleanField(value: Record<string, unknown>, key: string): boolean | undefined {
  const field = value[key];
  if (field === undefined || field === null) return undefined;
  if (typeof field !== "boolean") throw new Error(`protocol: gw_hello: ${key} must be a boolean`);
  return field;
}

function withoutProperty(value: Record<string, unknown>, key: string): Record<string, unknown> {
  return Object.fromEntries(Object.entries(value).filter(([name]) => name !== key));
}

function stringOrEmpty(value: unknown): string {
  return typeof value === "string" ? value : "";
}
