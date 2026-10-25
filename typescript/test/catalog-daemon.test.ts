import assert from "node:assert/strict";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { PiGatewayDaemon } from "../src/daemon.ts";

async function session(path: string, id: string, name: string): Promise<void> {
  await mkdir(join(path, ".."), { recursive: true });
  await writeFile(path, [
    { type: "session", id, cwd: "/tmp", timestamp: "2026-10-01T00:00:00.000Z" },
    { type: "session_info", id: `name-${id}`, name },
    { type: "message", id: `message-${id}`, timestamp: "2026-10-01T00:01:02.000Z", message: { role: "user", content: "first prompt" } },
  ].map((entry) => JSON.stringify(entry)).join("\n") + "\n");
}

test("daemon lists dormant catalog sessions and resolves names from disk", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-gateway-catalog-"));
  const oldRoot = process.env.PI_CODING_AGENT_SESSION_DIR;
  const file = join(root, "encoded-cwd", "session.jsonl");
  process.env.PI_CODING_AGENT_SESSION_DIR = join(root, "default-sessions");
  const daemon = new PiGatewayDaemon({ token: "test-token", catalogRoots: [root] });
  try {
    await session(file, "session-id", "catalog-name");
    const rows = await daemon.listSessions();
    assert.equal(rows.length, 1);
    assert.equal(rows[0]!.path, file);
    assert.equal(rows[0]!.id, "session-id");
    assert.equal(rows[0]!.name, "catalog-name");
    assert.equal(rows[0]!.title, "first prompt");
    assert.equal(rows[0]!.messageCount, 1);
    assert.equal(rows[0]!.cwd, "/tmp");
    assert.equal(rows[0]!.live, false);
    assert.equal(rows[0]!.lastActivity, "2026-10-01T00:01:02Z");
    assert.equal(await daemon.resolveTarget("catalog-name"), file);
    await assert.rejects(daemon.resolveTarget("missing-name"), (error: unknown) =>
      error instanceof Error && "code" in error && error.code === "unknown_session");
  } finally {
    await daemon.shutdown();
    if (oldRoot === undefined) delete process.env.PI_CODING_AGENT_SESSION_DIR;
    else process.env.PI_CODING_AGENT_SESSION_DIR = oldRoot;
    await rm(root, { recursive: true, force: true });
  }
});

test("daemon rejects ambiguous session names found in dormant files", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-gateway-catalog-ambiguous-"));
  const oldRoot = process.env.PI_CODING_AGENT_SESSION_DIR;
  process.env.PI_CODING_AGENT_SESSION_DIR = join(root, "default-sessions");
  const daemon = new PiGatewayDaemon({ token: "test-token", catalogRoots: [root] });
  try {
    await session(join(root, "a", "one.jsonl"), "one", "duplicate");
    await session(join(root, "b", "two.jsonl"), "two", "duplicate");
    await assert.rejects(daemon.resolveTarget("duplicate"), (error: unknown) =>
      error instanceof Error && "code" in error && error.code === "ambiguous_session");
  } finally {
    await daemon.shutdown();
    if (oldRoot === undefined) delete process.env.PI_CODING_AGENT_SESSION_DIR;
    else process.env.PI_CODING_AGENT_SESSION_DIR = oldRoot;
    await rm(root, { recursive: true, force: true });
  }
});
