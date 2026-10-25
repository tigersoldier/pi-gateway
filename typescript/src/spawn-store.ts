import { createHash, randomBytes } from "node:crypto";
import { chmod, mkdir, open, readFile, rename, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import type { ClientRef } from "./protocol.ts";

export interface SpawnConfig {
  path: string;
  piArgs: string[];
  spawn?: Record<string, string[]>;
  cwd: string;
  createdBy?: ClientRef;
  createdAt: string;
  sessionId: string;
}

/** Durable, one-record-per-session spawn configuration store. */
export class SpawnStore {
  readonly dir: string;
  constructor(dir: string) { this.dir = dir; }

  recordPath(canonicalPath: string): string {
    const digest = createHash("sha256").update(canonicalPath).digest("hex");
    return join(this.dir, `${digest}.json`);
  }

  async load(canonicalPath: string): Promise<SpawnConfig | undefined> {
    if (!this.dir || !canonicalPath) return undefined;
    let raw: string;
    try { raw = await readFile(this.recordPath(canonicalPath), "utf8"); }
    catch (error) { if (isNotFound(error)) return undefined; throw error; }
    let value: unknown;
    try { value = JSON.parse(raw); }
    catch (error) { throw new Error(`spawn: parse ${canonicalPath}: ${messageOf(error)}`); }
    return parseSpawnConfig(value, canonicalPath);
  }

  async save(config: SpawnConfig): Promise<void> {
    if (!this.dir || !config.path) return;
    const value: Record<string, unknown> = { path: config.path, createdAt: config.createdAt || new Date().toISOString() };
    if (config.piArgs.length) value.piArgs = [...config.piArgs];
    if (config.spawn && Object.keys(config.spawn).length) value.spawn = cloneSpawn(config.spawn);
    if (config.cwd) value.cwd = config.cwd;
    if (config.createdBy) value.createdBy = config.createdBy;
    if (config.sessionId) value.sessionId = config.sessionId;
    const body = `${JSON.stringify(value)}\n`;
    await mkdir(this.dir, { recursive: true, mode: 0o700 });
    const temporary = join(this.dir, `.spawn-${randomBytes(8).toString("hex")}`);
    try {
      await writeFile(temporary, body, { mode: 0o600, flag: "wx" });
      await chmod(temporary, 0o600);
      const file = await open(temporary, "r+");
      try { await file.sync(); } finally { await file.close(); }
      await rename(temporary, this.recordPath(config.path));
    } catch (error) {
      await rm(temporary, { force: true }).catch(() => {});
      throw error;
    }
  }

  async delete(canonicalPath: string): Promise<void> {
    if (!this.dir || !canonicalPath) return;
    try { await rm(this.recordPath(canonicalPath)); }
    catch (error) { if (!isNotFound(error)) throw error; }
  }
}

function parseSpawnConfig(value: unknown, requestedPath: string): SpawnConfig {
  const record = asRecord(value);
  if (typeof record.path !== "string" || !record.path) throw new Error(`spawn: invalid record for ${requestedPath}: path is missing`);
  const piArgs = stringArray(record.piArgs, "piArgs", requestedPath);
  const spawn = record.spawn === undefined ? undefined : parseSpawnValues(record.spawn, requestedPath);
  const cwd = optionalString(record.cwd, "cwd", requestedPath) ?? "";
  const createdAt = optionalString(record.createdAt, "createdAt", requestedPath) ?? "";
  if (createdAt && !Number.isFinite(Date.parse(createdAt))) throw new Error(`spawn: invalid record for ${requestedPath}: createdAt is invalid`);
  const sessionId = optionalString(record.sessionId, "sessionId", requestedPath) ?? "";
  let createdBy: ClientRef | undefined;
  if (record.createdBy !== undefined && record.createdBy !== null) {
    const creator = asRecord(record.createdBy);
    if (typeof creator.clientId !== "string" || !creator.clientId) throw new Error(`spawn: invalid record for ${requestedPath}: createdBy.clientId is missing`);
    const ref: ClientRef = { clientId: creator.clientId };
    for (const key of ["kind", "name"] as const) {
      const value = optionalString(creator[key], `createdBy.${key}`, requestedPath);
      if (value !== undefined) ref[key] = value;
    }
    if (creator.tags !== undefined && creator.tags !== null) {
      const tags = asRecord(creator.tags);
      if (!Object.values(tags).every((item) => typeof item === "string")) throw new Error(`spawn: invalid record for ${requestedPath}: createdBy.tags must contain strings`);
      ref.tags = tags as Record<string, string>;
    }
    createdBy = ref;
  }
  const config: SpawnConfig = { path: record.path, piArgs, cwd, createdAt, sessionId };
  if (spawn) config.spawn = spawn;
  if (createdBy) config.createdBy = createdBy;
  return config;
}

function parseSpawnValues(value: unknown, path: string): Record<string, string[]> {
  const record = asRecord(value);
  const out: Record<string, string[]> = {};
  for (const [key, items] of Object.entries(record)) out[key] = stringArray(items, `spawn.${key}`, path);
  return out;
}
function stringArray(value: unknown, name: string, path: string): string[] {
  if (value === undefined || value === null) return [];
  if (!Array.isArray(value) || !value.every((item) => typeof item === "string")) throw new Error(`spawn: invalid record for ${path}: ${name} must be an array of strings`);
  return [...value];
}
function optionalString(value: unknown, name: string, path: string): string | undefined {
  if (value === undefined || value === null) return undefined;
  if (typeof value !== "string") throw new Error(`spawn: invalid record for ${path}: ${name} must be a string`);
  return value;
}
function cloneSpawn(value: Record<string, string[]>): Record<string, string[]> {
  return Object.fromEntries(Object.entries(value).map(([key, items]) => [key, [...items]]));
}
function asRecord(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) throw new Error("expected an object");
  return value as Record<string, unknown>;
}
function isNotFound(error: unknown): boolean { return error !== null && typeof error === "object" && "code" in error && error.code === "ENOENT"; }
function messageOf(error: unknown): string { return error instanceof Error ? error.message : String(error); }
