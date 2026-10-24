import { randomBytes } from "node:crypto";
import { chmod, mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { isIP } from "node:net";
import { lookup } from "node:dns/promises";
import { dirname, join } from "node:path";
import { homedir } from "node:os";
import { ROLE_NAMES, getRoleCapabilities, normalizeCapabilities } from "./protocol.ts";

export const DEFAULT_HOST = "127.0.0.1";
export const DEFAULT_PORT = 7331;
export const DEFAULT_ADDR = "127.0.0.1:7331";
export const DIR_ENV = "PI_GATEWAY_CONFIG_DIR";

export function configDir(env: NodeJS.ProcessEnv = process.env): string {
  const override = env[DIR_ENV];
  if (override) return override;
  if (env.XDG_CONFIG_HOME) return join(env.XDG_CONFIG_HOME, "pi-gateway");
  const home = homedir();
  return home ? join(home, ".config", "pi-gateway") : join(".", ".pi-gateway");
}
export const tokenPath = (dir: string) => join(dir, "token");
export const portPath = (dir: string) => join(dir, "port");
export const debugPortPath = (dir: string) => join(dir, "debug-port");
export const spawnDir = (dir: string) => join(dir, "spawn");
export const tokensPath = (dir: string) => join(dir, "tokens.json");

export async function loadOrCreateToken(path: string): Promise<string> {
  try { return await readToken(path); }
  catch (error) { if (!isNotFound(error)) throw error; }
  const token = randomBytes(32).toString("hex");
  await writeFileMode(path, `${token}\n`, 0o600);
  return token;
}
export async function readToken(path: string): Promise<string> {
  const token = (await readFile(path, "utf8")).trim();
  if (!token) throw new Error(`config: token file ${path} is empty`);
  return token;
}
export async function writePort(path: string, port: number): Promise<void> { await writeFileMode(path, `${port}\n`, 0o600); }
export async function removePortFile(path: string): Promise<void> { await rm(path, { force: true }); }
export async function removePortIfMatches(path: string, port: number): Promise<void> {
  try { if (await readPort(path) === port) await rm(path, { force: true }); }
  catch { /* Missing or malformed port files are not ours to remove. */ }
}
export async function readPort(path: string): Promise<number> {
  const value = (await readFile(path, "utf8")).trim();
  if (!/^[+-]?\d+$/.test(value)) throw new Error(`config: invalid port file ${path}`);
  const port = Number(value);
  if (!Number.isSafeInteger(port) || port <= 0 || port > 65535) throw new Error(`config: invalid port file ${path}`);
  return port;
}
export async function addresses(server: string, port: number, dir: string): Promise<string[]> {
  if (server) return [server];
  if (port > 0) return [`${DEFAULT_HOST}:${port}`];
  const result: string[] = [];
  try { result.push(`${DEFAULT_HOST}:${await readPort(portPath(dir))}`); } catch { /* fallback below */ }
  if (result.length === 0 || result[0] !== DEFAULT_ADDR) result.push(DEFAULT_ADDR);
  return result;
}
export function portOf(addr: string): number {
  const index = addr.lastIndexOf(":");
  if (index < 0) throw new Error(`config: address ${JSON.stringify(addr)} has no port`);
  const text = addr.slice(index + 1);
  if (!/^[+-]?\d+$/.test(text)) throw new Error(`config: invalid port in ${JSON.stringify(addr)}`);
  const port = Number(text);
  if (!Number.isSafeInteger(port) || port < 0 || port > 65535) throw new Error(`config: invalid port in ${JSON.stringify(addr)}`);
  return port;
}
export async function requireLoopback(
  addr: string,
  resolveHost: (hostname: string) => Promise<Array<{ address: string }>> = (hostname) => lookup(hostname, { all: true }),
): Promise<void> {
  const [host] = splitHostPort(addr);
  if (!host) throw new Error(`config: ${JSON.stringify(addr)} listens on every interface; use the loopback address ${DEFAULT_ADDR}`);
  if (isIP(host)) {
    if (isLoopbackIp(host)) return;
    throw new Error(`config: ${JSON.stringify(addr)} is not a loopback address`);
  }
  let resolved: Array<{ address: string }>;
  try { resolved = await resolveHost(host); }
  catch (error) { throw new Error(`config: cannot resolve address ${JSON.stringify(host)}: ${errorMessage(error)}`); }
  for (const entry of resolved) if (!isLoopbackIp(entry.address)) throw new Error(`config: ${JSON.stringify(addr)} is not a loopback address`);
}

export interface TokenSpec { name: string; token: string; role?: string; capabilities?: string[]; comment?: string }
export interface TokenGrant { name: string; token: string; capabilities: string[] }
export interface TokenFile { tokens: TokenSpec[] }

export function resolveTokenSpec(spec: TokenSpec): TokenGrant {
  if (!spec.name) throw new Error("token entry has no name");
  if (!spec.token) throw new Error(`token ${JSON.stringify(spec.name)} has no token value`);
  const caps = spec.capabilities ?? [];
  if (spec.role && caps.length) throw new Error(`token ${JSON.stringify(spec.name)} sets both role and capabilities`);
  if (spec.role) {
    const roleCaps = getRoleCapabilities(spec.role);
    if (!roleCaps) throw new Error(`token ${JSON.stringify(spec.name)} uses unknown role ${JSON.stringify(spec.role)} (want ${ROLE_NAMES.join(", ")})`);
    return { name: spec.name, token: spec.token, capabilities: roleCaps };
  }
  if (caps.length) {
    const normalized = normalizeCapabilities(caps);
    if (normalized.unknown.length) throw new Error(`token ${JSON.stringify(spec.name)} requests unknown capabilities ${normalized.unknown.join(", ")}`);
    return { name: spec.name, token: spec.token, capabilities: normalized.known };
  }
  throw new Error(`token ${JSON.stringify(spec.name)} sets neither role nor capabilities`);
}
export function describeTokenSpec(spec: TokenSpec): string {
  return spec.role ? `role ${spec.role}` : `capabilities ${(spec.capabilities ?? []).join(",")}`;
}
export async function loadTokens(path: string): Promise<TokenGrant[] | undefined> {
  const specs = await readTokenSpecs(path);
  if (!specs.tokens.length) return undefined;
  const grants: TokenGrant[] = [];
  const seen = new Map<string, string>();
  for (const spec of specs.tokens) {
    let grant: TokenGrant;
    try { grant = resolveTokenSpec(spec); }
    catch (error) { throw new Error(`config: ${path}: ${errorMessage(error)}`); }
    const previous = seen.get(grant.token);
    if (previous !== undefined) throw new Error(`config: ${path}: tokens ${JSON.stringify(previous)} and ${JSON.stringify(grant.name)} share the same value`);
    seen.set(grant.token, grant.name);
    grants.push(grant);
  }
  return grants;
}
export async function addToken(path: string, name: string, role: string, capabilities: string[]): Promise<TokenGrant> {
  if (!name) throw new Error("config: a token name is required");
  const file = await readTokenSpecs(path);
  if (file.tokens.some((spec) => spec.name === name)) throw new Error(`config: token ${JSON.stringify(name)} already exists in ${path}`);
  const spec: TokenSpec = { name, token: randomBytes(32).toString("hex") };
  if (role) spec.role = role;
  if (capabilities.length) spec.capabilities = [...capabilities];
  const grant = resolveTokenSpec(spec);
  file.tokens.push(spec);
  await writeFileMode(path, `${JSON.stringify(file, null, 2)}\n`, 0o600);
  return grant;
}

async function readTokenSpecs(path: string): Promise<TokenFile> {
  let text: string;
  try { text = await readFile(path, "utf8"); }
  catch (error) { if (isNotFound(error)) return { tokens: [] }; throw new Error(`config: read ${path}: ${errorMessage(error)}`); }
  if (!text.trim()) return { tokens: [] };
  let input: unknown;
  try { input = JSON.parse(text); }
  catch (error) { throw new Error(`config: parse ${path}: ${errorMessage(error)}`); }
  if (input === null) return { tokens: [] };
  if (!isRecord(input)) throw new Error(`config: parse ${path}: expected object`);
  assertOnlyKeys(input, ["tokens"], path);
  if (input.tokens === undefined || input.tokens === null) return { tokens: [] };
  if (!Array.isArray(input.tokens)) throw new Error(`config: parse ${path}: tokens must be an array`);
  const tokens = input.tokens.map((item) => {
    if (!isRecord(item)) throw new Error(`config: parse ${path}: token entry must be an object`);
    assertOnlyKeys(item, ["name", "token", "role", "capabilities", "comment"], path);
    const name = optionalStringField(item, "name", path) ?? "";
    const token = optionalStringField(item, "token", path) ?? "";
    const role = optionalStringField(item, "role", path);
    const comment = optionalStringField(item, "comment", path);
    const capabilityValue = item.capabilities;
    let capabilities: string[] | undefined;
    if (capabilityValue !== undefined && capabilityValue !== null) {
      if (!Array.isArray(capabilityValue) || !capabilityValue.every((cap): cap is string => typeof cap === "string")) {
        throw new Error(`config: parse ${path}: capabilities must be an array of strings`);
      }
      capabilities = [...capabilityValue];
    }
    const spec: TokenSpec = { name, token };
    if (role !== undefined) spec.role = role;
    if (comment !== undefined) spec.comment = comment;
    if (capabilities !== undefined) spec.capabilities = capabilities;
    return spec;
  });
  return { tokens };
}
async function writeFileMode(path: string, content: string, mode: number): Promise<void> {
  const temp = `${path}.tmp`;
  try {
    await mkdir(dirname(path), { recursive: true, mode: 0o700 });
    await writeFile(temp, content, { mode });
    await chmod(temp, mode);
    await rename(temp, path);
  } catch (error) {
    try { await rm(temp, { force: true }); } catch { /* best effort */ }
    throw new Error(`config: write ${path}: ${errorMessage(error)}`);
  }
}
function splitHostPort(addr: string): [string, string] {
  if (addr.startsWith("[")) {
    const end = addr.indexOf("]");
    if (end < 0 || addr[end + 1] !== ":") throw new Error(`config: invalid address ${JSON.stringify(addr)}`);
    return [addr.slice(1, end), addr.slice(end + 2)];
  }
  const i = addr.lastIndexOf(":");
  if (i < 0 || addr.slice(0, i).includes(":")) throw new Error(`config: invalid address ${JSON.stringify(addr)}`);
  return [addr.slice(0, i), addr.slice(i + 1)];
}
function isLoopbackIp(ip: string): boolean {
  if (isIP(ip) === 4) return Number(ip.split(".")[0]) === 127;
  if (isIP(ip) !== 6) return false;
  const halves = ip.toLowerCase().split("::");
  const left = halves[0] ? halves[0].split(":") : [];
  const right = halves.length > 1 && halves[1] ? halves[1].split(":") : [];
  const groups = [...left, ...Array(Math.max(0, 8 - left.length - right.length)).fill("0"), ...right].map((v) => Number.parseInt(v || "0", 16));
  if (groups.length !== 8) return false;
  if (groups.slice(0, 7).every((n) => n === 0) && groups[7] === 1) return true;
  return groups.slice(0, 5).every((n) => n === 0) && groups[5] === 0xffff && (groups[6] >> 8) === 127;
}
function optionalStringField(value: Record<string, unknown>, key: string, path: string): string | undefined {
  const field = value[key];
  if (field === undefined || field === null) return undefined;
  if (typeof field !== "string") throw new Error(`config: parse ${path}: ${key} must be a string`);
  return field;
}
function assertOnlyKeys(value: Record<string, unknown>, allowed: readonly string[], path: string): void {
  const unknown = Object.keys(value).find((key) => !allowed.includes(key));
  if (unknown !== undefined) throw new Error(`config: parse ${path}: json: unknown field ${JSON.stringify(unknown)}`);
}
function isRecord(value: unknown): value is Record<string, unknown> { return value !== null && typeof value === "object" && !Array.isArray(value); }
function isNotFound(error: unknown): boolean { return isRecord(error) && error.code === "ENOENT"; }
function errorMessage(error: unknown): string { return error instanceof Error ? error.message : String(error); }
