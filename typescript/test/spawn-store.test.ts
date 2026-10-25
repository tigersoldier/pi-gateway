import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { SpawnStore } from "../src/spawn-store.ts";

test("spawn store hashes canonical paths and atomically persists a restricted record", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-spawn-store-"));
  const store = new SpawnStore(join(dir, "spawn"));
  const path = "/tmp/session.jsonl";
  try {
    const expected = join(dir, "spawn", `${createHash("sha256").update(path).digest("hex")}.json`);
    assert.equal(store.recordPath(path), expected);
    await store.save({ path, piArgs: ["--append-system-prompt", "context", "--api-key", "secret"],
      spawn: { "append-system-prompt": ["context"] }, cwd: "/tmp", createdBy: { clientId: "c1", kind: "bot" },
      createdAt: "2026-10-01T00:00:00.000Z", sessionId: "session-id" });
    const loaded = await store.load(path);
    assert.deepEqual(loaded, { path, piArgs: ["--append-system-prompt", "context", "--api-key", "secret"],
      spawn: { "append-system-prompt": ["context"] }, cwd: "/tmp", createdBy: { clientId: "c1", kind: "bot" },
      createdAt: "2026-10-01T00:00:00.000Z", sessionId: "session-id" });
    assert.equal((await stat(expected)).mode & 0o777, 0o600);
    assert.match(await readFile(expected, "utf8"), /"--api-key","secret"/);
  } finally { await rm(dir, { recursive: true, force: true }); }
});

test("spawn store treats missing files as absent and supports idempotent deletion", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-spawn-store-missing-"));
  const store = new SpawnStore(join(dir, "spawn"));
  try {
    assert.equal(await store.load("/missing.jsonl"), undefined);
    await store.delete("/missing.jsonl");
    await store.save({ path: "/session.jsonl", piArgs: [], cwd: "", createdAt: "2026-10-01T00:00:00.000Z", sessionId: "" });
    await store.delete("/session.jsonl");
    await store.delete("/session.jsonl");
    assert.equal(await store.load("/session.jsonl"), undefined);
  } finally { await rm(dir, { recursive: true, force: true }); }
});

test("spawn store surfaces corrupt and malformed records instead of treating them as absent", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-spawn-store-corrupt-"));
  const store = new SpawnStore(join(dir, "spawn"));
  const path = "/session.jsonl";
  try {
    await mkdir(join(dir, "spawn"), { recursive: true });
    await writeFile(store.recordPath(path), "not-json");
    await assert.rejects(store.load(path), /spawn: parse/);
    await writeFile(store.recordPath(path), JSON.stringify({ path, createdAt: "bad-date", piArgs: [] }));
    await assert.rejects(store.load(path), /createdAt is invalid/);
  } finally { await rm(dir, { recursive: true, force: true }); }
});

test("spawn store is a no-op when persistence is not configured", async () => {
  const store = new SpawnStore("");
  await store.save({ path: "/session.jsonl", piArgs: [], cwd: "", createdAt: "", sessionId: "" });
  assert.equal(await store.load("/session.jsonl"), undefined);
  await store.delete("/session.jsonl");
});
