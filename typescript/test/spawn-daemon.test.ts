import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { PiGatewayDaemon } from "../src/daemon.ts";
import { parsePiArgs, type PiArgsSpec } from "../src/piargs.ts";
import type { RpcObject, RpcSessionRuntime } from "../src/rpc-adapter.ts";
import { SpawnStore } from "../src/spawn-store.ts";
import { writeSessionFile } from "./support/gateway.ts";

function fakeRuntime(piArgsSeen: string[][]) {
  return async (options: { piArgs?: PiArgsSpec; sessionPath?: string }): Promise<RpcSessionRuntime> => {
    piArgsSeen.push([...(options.piArgs?.args ?? [])]);
    const listeners = new Set<(event: RpcObject) => void>();
    const session = {
      model: { provider: "test", id: "test/model" }, thinkingLevel: "medium", isStreaming: false, isCompacting: false,
      steeringMode: "one-at-a-time", followUpMode: "one-at-a-time", sessionFile: options.sessionPath ?? "/tmp/fake-session.jsonl",
      sessionId: "fake-session-id", sessionName: "", autoCompactionEnabled: true, pendingMessageCount: 0, messages: [], promptTemplates: [],
      subscribe(listener: (event: RpcObject) => void) { listeners.add(listener); return () => listeners.delete(listener); },
      async bindExtensions() {},
    };
    return {
      session: session as unknown as RpcSessionRuntime["session"],
      async newSession() { return { cancelled: false }; },
      async switchSession() { return { cancelled: false }; },
      async fork() { return { cancelled: false, selectedText: "" }; },
      async dispose() {},
      setRebindSession() {},
    };
  };
}

test("spawn config survives daemon restart, merges cold additions, and redacts catalog values", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-gateway-spawn-e2e-"));
  const oldRoot = process.env.PI_CODING_AGENT_SESSION_DIR;
  process.env.PI_CODING_AGENT_SESSION_DIR = join(root, "sessions");
  const path = join(root, "sessions", "session.jsonl");
  const storeDir = join(root, "state", "spawn");
  const firstArgs: string[][] = [];
  const creator = { clientId: "client-1", kind: "bot", name: "integration", tags: { thread: "t1" } };
  const first = new PiGatewayDaemon({ token: "token", spawnDir: storeDir, createRuntime: fakeRuntime(firstArgs) });
  const store = new SpawnStore(storeDir);
  try {
    await writeSessionFile(path);
    const spec = parsePiArgs(["--append-system-prompt", "original", "--api-key", "secret"]);
    const actor = await first.createActor(path, spec, "/tmp", creator);
    assert.deepEqual(firstArgs[0], ["--append-system-prompt", "original", "--api-key", "secret"]);
    const canon = actor.path;
    const recordPath = store.recordPath(canon);
    const record = JSON.parse(await readFile(recordPath, "utf8")) as Record<string, unknown>;
    assert.equal(record.path, canon);
    assert.equal(record.createdBy && (record.createdBy as Record<string, unknown>).clientId, "client-1");
    assert.deepEqual((record.spawn as Record<string, unknown>)["append-system-prompt"], ["original"]);
    assert.equal((record.spawn as Record<string, unknown>)["api-key"], undefined);
    assert.equal((await stat(recordPath)).mode & 0o777, 0o600);
    const rows = await first.listSessions();
    assert.deepEqual(rows.find((row) => row.path === canon)?.spawn, { "append-system-prompt": ["original"] });

    await assert.rejects(first.attach(path, parsePiArgs(["--append-system-prompt", "different"]), "/tmp"), (error: unknown) =>
      error instanceof Error && "code" in error && error.code === "spawn_param_conflict");
    assert.equal(await first.attach(path, parsePiArgs(["--extension", "/new.ts"]), "/tmp"), actor);
    await first.shutdown();

    const secondArgs: string[][] = [];
    const second = new PiGatewayDaemon({ token: "token", spawnDir: storeDir, createRuntime: fakeRuntime(secondArgs) });
    try {
      const coldRow = (await second.listSessions()).find((row) => row.path === canon);
      assert.equal(coldRow?.live, false);
      assert.deepEqual(coldRow?.spawn, { "append-system-prompt": ["original"] });
      const respawned = await second.attach(path, parsePiArgs(["--append-system-prompt", "changed", "--extension", "/new.ts"]), "/tmp");
      assert.notEqual(respawned, actor);
      assert.ok(secondArgs[0]?.includes("original"));
      assert.ok(secondArgs[0]?.includes("/new.ts"));
      assert.equal(secondArgs[0]?.includes("changed"), false);
      const respawnRows = await second.listSessions();
      assert.deepEqual(respawnRows.find((row) => row.path === respawned.path)?.spawn, {
        "append-system-prompt": ["original"], extension: ["/new.ts"],
      });
      const loaded = await store.load(respawned.path);
      assert.deepEqual(loaded?.createdBy, creator);
    } finally { await second.shutdown(); }
  } finally {
    await first.shutdown();
    if (oldRoot === undefined) delete process.env.PI_CODING_AGENT_SESSION_DIR;
    else process.env.PI_CODING_AGENT_SESSION_DIR = oldRoot;
    await rm(root, { recursive: true, force: true });
  }
});
