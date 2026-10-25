import { createReadStream } from "node:fs";
import { open, readdir, realpath, stat } from "node:fs/promises";
import { homedir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { createInterface } from "node:readline";

export interface SessionInfo {
  path: string;
  id: string;
  cwd: string;
  name: string;
  title: string;
  messageCount: number;
  lastActivity: Date;
}

export interface SessionScanResult { sessions: SessionInfo[]; truncated: boolean }
interface Candidate { path: string; mtime: Date }
const DEFAULT_MAX_FILES = 1000;
export const LEAF_TAIL_LIMIT = 4 << 20;

export function defaultSessionRoots(extra: readonly string[] = [], env: NodeJS.ProcessEnv = process.env): string[] {
  const roots: string[] = [];
  const add = (dir: string | undefined) => { if (dir && !roots.includes(dir)) roots.push(dir); };
  const sessionDir = env.PI_CODING_AGENT_SESSION_DIR?.trim();
  const agentDir = env.PI_CODING_AGENT_DIR?.trim();
  if (sessionDir) add(sessionDir);
  else if (agentDir) add(join(agentDir, "sessions"));
  else if (homedir()) add(join(homedir(), ".pi", "agent", "sessions"));
  for (const dir of extra) add(dir);
  return roots;
}

/** On-demand bounded scanner for Pi's grouped session JSONL layout. */
export class SessionScanner {
  readonly roots: string[];
  readonly maxFiles: number;

  constructor(roots: readonly string[] = defaultSessionRoots(), maxFiles = DEFAULT_MAX_FILES) {
    this.roots = [...roots];
    this.maxFiles = maxFiles > 0 ? maxFiles : DEFAULT_MAX_FILES;
  }

  async list(cwd = "", limit = 0): Promise<SessionInfo[]> {
    const result = await this.parseAll((item) => !cwd || item.cwd === cwd, limit);
    return result.sessions.sort((left, right) => right.lastActivity.getTime() - left.lastActivity.getTime());
  }

  async findName(name: string): Promise<SessionScanResult> {
    return this.parseAll((item) => item.name !== "" && item.name === name, 0);
  }

  async parseAll(keep: (item: SessionInfo) => boolean = () => true, limit = 0): Promise<SessionScanResult> {
    let candidates = await this.candidates();
    const truncated = candidates.length > this.maxFiles;
    if (truncated) candidates = candidates.slice(0, this.maxFiles);
    const sessions: SessionInfo[] = [];
    for (const candidate of candidates) {
      if (limit > 0 && sessions.length >= limit) break;
      try {
        const item = await parseSessionFile(candidate.path);
        if (item.lastActivity.getTime() === 0) item.lastActivity = candidate.mtime;
        if (keep(item)) sessions.push(item);
      } catch { /* unreadable and non-session JSONL files are skipped */ }
    }
    return { sessions, truncated };
  }

  private async candidates(): Promise<Candidate[]> {
    const out: Candidate[] = [];
    for (const root of this.roots) {
      let entries;
      try { entries = await readdir(root, { withFileTypes: true }); } catch { continue; }
      for (const entry of entries) {
        if (entry.isDirectory()) {
          const sub = join(root, entry.name);
          let children;
          try { children = await readdir(sub, { withFileTypes: true }); } catch { continue; }
          await appendCandidates(out, sub, children);
        } else await appendCandidate(out, root, entry.name, entry.isDirectory());
      }
    }
    out.sort((left, right) => {
      const mtime = right.mtime.getTime() - left.mtime.getTime();
      return mtime === 0 ? left.path.localeCompare(right.path) : mtime;
    });
    return out;
  }
}

export async function parseSessionFile(path: string): Promise<SessionInfo> {
  const info: SessionInfo = { path, id: "", cwd: "", name: "", title: "", messageCount: 0, lastActivity: new Date(0) };
  let first = true;
  let headerTime = new Date(0);
  const input = createReadStream(path, { encoding: "utf8" });
  const lines = createInterface({ input, crlfDelay: Infinity });
  try {
    for await (const line of lines) {
      if (!line) continue;
      let entry: Record<string, unknown>;
      try { entry = asRecord(JSON.parse(line)); } catch { continue; }
      if (first) {
        if (entry.type !== "session") throw new Error(`catalog: ${path} is not a session file`);
        info.id = string(entry.id);
        info.cwd = string(entry.cwd);
        headerTime = parseTime(string(entry.timestamp));
        first = false;
        continue;
      }
      if (entry.type === "session_info") info.name = string(entry.name).trim();
      else if (entry.type === "message") {
        info.messageCount++;
        const message = asRecord(entry.message);
        const activity = messageTime(message, string(entry.timestamp));
        if (activity.getTime() > info.lastActivity.getTime()) info.lastActivity = activity;
        if (!info.title && message.role === "user") info.title = firstText(message.content);
      }
    }
  } finally { lines.close(); }
  if (first) throw new Error(`catalog: ${path} is empty`);
  if (info.lastActivity.getTime() === 0) info.lastActivity = headerTime;
  return info;
}

export async function headerCwd(path: string): Promise<string> {
  const input = createReadStream(path, { encoding: "utf8", start: 0, end: 1 << 16 });
  const lines = createInterface({ input, crlfDelay: Infinity });
  try {
    for await (const line of lines) {
      const entry = asRecord(JSON.parse(line));
      return entry.type === "session" ? string(entry.cwd) : "";
    }
  } catch { return ""; }
  finally { lines.close(); input.destroy(); }
  return "";
}

export async function leafId(path: string): Promise<string> {
  let file;
  try { file = await open(path, "r"); } catch (error) { throw error; }
  try {
    const { size } = await file.stat();
    if (size === 0) return "";
    const length = Math.min(size, LEAF_TAIL_LIMIT);
    const buffer = Buffer.alloc(length);
    await file.read(buffer, 0, length, size - length);
    let text = buffer.toString("utf8").replace(/[\r\n]+$/u, "");
    while (text) {
      const index = text.lastIndexOf("\n");
      const line = text.slice(index + 1).replace(/\r+$/u, "");
      if (line) {
        try {
          const entry = asRecord(JSON.parse(line));
          if (entry.type === "session") return "";
          const id = string(entry.id);
          if (id) return id;
        } catch { /* ignore a partial or malformed last line */ }
      }
      if (index < 0) break;
      text = text.slice(0, index);
    }
    return "";
  } finally { await file.close(); }
}

export function resolveSessionName(name: string, files: readonly SessionInfo[], live: readonly SessionInfo[]): { info?: SessionInfo; error?: string } {
  const matches = new Map<string, SessionInfo>();
  for (const item of files) if (item.name && item.name === name) matches.set(item.path, item);
  for (const item of live) if (item.name && item.name === name) matches.set(item.path, item);
  if (matches.size === 0) return {};
  if (matches.size > 1) return { error: `session name ${JSON.stringify(name)} matches ${matches.size} sessions` };
  return { info: matches.values().next().value };
}

export async function canonicalSessionPath(path: string): Promise<string> {
  const expanded = path === "~" ? homedir() : path.startsWith("~/") ? join(homedir(), path.slice(2)) : path;
  const absolute = resolve(expanded);
  try { return await realpath(absolute); }
  catch {
    try { return join(await realpath(dirname(absolute)), basename(absolute)); }
    catch { return absolute; }
  }
}

async function appendCandidates(out: Candidate[], dir: string, entries: import("node:fs").Dirent[]): Promise<void> {
  for (const entry of entries) await appendCandidate(out, dir, entry.name, entry.isDirectory());
}
async function appendCandidate(out: Candidate[], dir: string, name: string, isDirectory: boolean): Promise<void> {
  if (isDirectory || !name.endsWith(".jsonl") || name.startsWith(".")) return;
  const path = join(dir, name);
  try { out.push({ path, mtime: (await stat(path)).mtime }); } catch { /* vanished or unreadable candidate */ }
}
function messageTime(message: Record<string, unknown>, entryTimestamp: string): Date {
  const timestamp = message.timestamp;
  if (typeof timestamp === "number" && timestamp > 0) return new Date(Math.trunc(timestamp));
  if (typeof timestamp === "string") {
    const date = parseTime(timestamp);
    if (date.getTime() > 0) return date;
  }
  return parseTime(entryTimestamp);
}
function firstText(content: unknown): string {
  if (typeof content === "string") return content.trim();
  if (!Array.isArray(content)) return "";
  for (const item of content) {
    const block = asRecord(item);
    if (block.type === "text" && typeof block.text === "string" && block.text) return block.text.trim();
  }
  return "";
}
function parseTime(value: string): Date {
  if (!value) return new Date(0);
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? new Date(parsed) : new Date(0);
}
function asRecord(value: unknown): Record<string, unknown> { return value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {}; }
function string(value: unknown): string { return typeof value === "string" ? value : ""; }
