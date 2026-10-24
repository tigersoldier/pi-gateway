import assert from "node:assert/strict";
import { mkdtemp, mkdir, rm, utimes, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { test } from "node:test";
import { defaultSessionRoots, leafId, parseSessionFile, resolveSessionName, SessionScanner } from "../src/catalog.ts";

function header(id: string, cwd: string, timestamp: string): Record<string, unknown> {
  return { type: "session", version: 3, id, cwd, timestamp };
}
async function writeSession(path: string, ...entries: unknown[]): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `${entries.map((entry) => JSON.stringify(entry)).join("\n")}\n`);
}

test("catalog parses header, latest name, first user title, messages, activity, and leaf", async () => {
  const temp = await mkdtemp(join(tmpdir(), "pi-catalog-"));
  const path = join(temp, "--home-u-proj--", "a.jsonl");
  try {
    await writeSession(path,
      header("sess-1", "/home/u/proj", "2026-01-01T00:00:00.000Z"),
      { type: "session_info", id: "n1", name: "old-name" },
      { type: "message", id: "e1", timestamp: "2026-01-01T00:00:01.000Z", message: { role: "user", content: "first question" } },
      { type: "message", id: "e2", timestamp: "2026-01-01T00:10:00.000Z", message: { role: "assistant", content: [{ type: "thinking", thinking: "hmm" }, { type: "text", text: "answer" }] } },
      { type: "session_info", id: "n2", name: "auth-refactor" },
    );
    const info = await parseSessionFile(path);
    assert.equal(info.id, "sess-1");
    assert.equal(info.cwd, "/home/u/proj");
    assert.equal(info.name, "auth-refactor");
    assert.equal(info.title, "first question");
    assert.equal(info.messageCount, 2);
    assert.equal(info.lastActivity.toISOString(), "2026-01-01T00:10:00.000Z");
    assert.equal(await leafId(path), "n2");
  } finally { await rm(temp, { recursive: true, force: true }); }
});

test("catalog rejects JSONL without a session header", async () => {
  const temp = await mkdtemp(join(tmpdir(), "pi-catalog-invalid-"));
  const path = join(temp, "notes.jsonl");
  try {
    await writeSession(path, { type: "message", id: "x" });
    await assert.rejects(parseSessionFile(path), /not a session file/);
  } finally { await rm(temp, { recursive: true, force: true }); }
});

test("catalog filters by cwd and limit after newest-first scanning", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-catalog-list-"));
  const now = Date.now();
  const older = join(root, "old", "old.jsonl");
  const newer = join(root, "new", "new.jsonl");
  const other = join(root, "other", "other.jsonl");
  try {
    await writeSession(older, header("s1", "/home/u/proj", "2026-01-01T00:00:00Z"));
    await writeSession(newer, header("s2", "/home/u/proj", "2026-01-02T00:00:00Z"));
    await writeSession(other, header("s3", "/home/u/elsewhere", "2026-01-03T00:00:00Z"));
    await utimes(older, new Date(now - 7_200_000), new Date(now - 7_200_000));
    await utimes(newer, new Date(now - 3_600_000), new Date(now - 3_600_000));
    await utimes(other, new Date(now), new Date(now));
    const scanner = new SessionScanner([root]);
    const all = await scanner.list();
    assert.equal(all.length, 3);
    assert.equal(all[0]!.id, "s3");
    const project = await scanner.list("/home/u/proj");
    assert.deepEqual(project.map((item) => item.id), ["s2", "s1"]);
    assert.deepEqual((await scanner.list("/home/u/proj", 1)).map((item) => item.id), ["s2"]);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("catalog finds duplicate names and skips a partial trailing entry", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-catalog-find-"));
  const a = join(root, "a", "a.jsonl");
  const b = join(root, "b", "b.jsonl");
  try {
    await writeSession(a, header("s1", "/x", "2026-01-01T00:00:00Z"), { type: "session_info", id: "n1", name: "auth" });
    await writeSession(b, header("s2", "/x", "2026-01-01T00:00:00Z"), { type: "session_info", id: "n1", name: "auth" });
    const files = await new SessionScanner([root]).findName("auth");
    assert.equal(files.sessions.length, 2);
    assert.match(resolveSessionName("auth", files.sessions, []).error ?? "", /matches 2 sessions/);
    assert.equal(resolveSessionName("missing", files.sessions, []).info, undefined);
    const headerOnly = join(root, "leaf", "x.jsonl");
    await writeSession(headerOnly, header("s3", "/x", "2026-01-01T00:00:00Z"));
    await writeFile(headerOnly, `${JSON.stringify(header("s3", "/x", "2026-01-01T00:00:00Z"))}\n{"type":"message","id":"e9"}\n{"type":"message","id":"e1`);
    assert.equal(await leafId(headerOnly), "e9");
    const emptyLeaf = join(root, "leaf", "header-only.jsonl");
    await writeSession(emptyLeaf, header("s4", "/x", "2026-01-01T00:00:00Z"));
    assert.equal(await leafId(emptyLeaf), "");
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("default catalog roots honor Pi environment precedence and de-duplicate", () => {
  assert.deepEqual(defaultSessionRoots(["/extra"], {
    PI_CODING_AGENT_SESSION_DIR: "/tmp/sessions-env",
    PI_CODING_AGENT_DIR: "/tmp/agent-env",
  }), ["/tmp/sessions-env", "/extra"]);
  assert.deepEqual(defaultSessionRoots(["/extra"], { PI_CODING_AGENT_DIR: "/tmp/agent-env" }), ["/tmp/agent-env/sessions", "/extra"]);
  const home = defaultSessionRoots([], {});
  assert.equal(home.length, 1);
  assert.ok(home[0]!.endsWith(join(".pi", "agent", "sessions")));
  assert.equal(defaultSessionRoots([home[0]!], {}).length, 1);
});
