import assert from "node:assert/strict";
import { mkdir, mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { connect, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { test } from "node:test";
import { JsonlDecoder, encodeJsonFrame } from "../src/protocol.ts";
import { PiGatewayDaemon } from "../src/daemon.ts";
import type { PiSdkRuntimeOptions } from "../src/sdk-runtime.ts";
import type { RpcObject, RpcSessionRuntime } from "../src/rpc-adapter.ts";

class ProtocolPeer {
  readonly socket: Socket;
  welcome?: Record<string, unknown>;
  handshakeError?: Record<string, unknown>;
  private readonly decoder = new JsonlDecoder();
  private readonly frames: Array<Record<string, unknown>> = [];
  private readonly waiters: Array<{ test(frame: Record<string, unknown>): boolean; resolve(frame: Record<string, unknown>): void }> = [];
  constructor(socket: Socket) {
    this.socket = socket;
    socket.on("data", (chunk: Buffer) => {
      for (const raw of this.decoder.push(chunk)) {
        const frame = JSON.parse(raw.toString("utf8")) as Record<string, unknown>;
        const index = this.waiters.findIndex((waiter) => waiter.test(frame));
        if (index >= 0) this.waiters.splice(index, 1)[0]!.resolve(frame);
        else this.frames.push(frame);
      }
    });
  }
  send(frame: Record<string, unknown>): void { this.socket.write(encodeJsonFrame(frame)); }
  receive(predicate: (frame: Record<string, unknown>) => boolean, timeoutMs = 5000): Promise<Record<string, unknown>> {
    const index = this.frames.findIndex(predicate);
    if (index >= 0) return Promise.resolve(this.frames.splice(index, 1)[0]!);
    return new Promise((resolveFrame, reject) => {
      let timer: ReturnType<typeof setTimeout>;
      const waiter = { test: predicate, resolve: (frame: Record<string, unknown>) => { clearTimeout(timer); resolveFrame(frame); } };
      timer = setTimeout(() => {
        const index = this.waiters.indexOf(waiter);
        if (index >= 0) this.waiters.splice(index, 1);
        reject(new Error("timed out waiting for gateway frame"));
      }, timeoutMs);
      this.waiters.push(waiter);
    });
  }
  close(): void { this.socket.destroy(); }
}

async function dial(port: number, token: string, session = "", piArgs?: string[], cwd?: string, expectSession = true): Promise<ProtocolPeer> {
  const socket = connect({ host: "127.0.0.1", port });
  await new Promise<void>((resolve, reject) => { socket.once("connect", resolve); socket.once("error", reject); });
  const peer = new ProtocolPeer(socket);
  const hello: Record<string, unknown> = { type: "gw_hello", protocol: 1, token, client: { name: "test", kind: "test" }, session };
  if (piArgs !== undefined) hello.piArgs = piArgs;
  if (cwd !== undefined) hello.cwd = cwd;
  peer.send(hello);
  const first = await peer.receive((frame) => frame.type === "gw_welcome" || (session !== "" && !expectSession && frame.type === "gw_error"));
  if (first.type === "gw_error") peer.handshakeError = first;
  else {
    peer.welcome = first;
    if (session !== "" && !expectSession) {
      peer.handshakeError = await peer.receive((frame) => frame.type === "gw_error");
    }
  }
  if (session && expectSession) assert.equal(first.session && typeof first.session === "object", true);
  return peer;
}

function fakeRuntime(onCreate?: (args: string[]) => void) {
  return async (options: PiSdkRuntimeOptions): Promise<RpcSessionRuntime> => {
    onCreate?.([...(options.piArgs?.args ?? [])]);
    const listeners = new Set<(event: RpcObject) => void>();
    const session = {
      model: { provider: "test", id: "test/model" }, thinkingLevel: "medium", isStreaming: false, isCompacting: false,
      steeringMode: "one-at-a-time", followUpMode: "one-at-a-time", sessionFile: options.sessionPath,
      sessionId: "fake-session-id", sessionName: "", autoCompactionEnabled: true, pendingMessageCount: 0, messages: [], promptTemplates: [],
      subscribe(listener: (event: RpcObject) => void) { listeners.add(listener); return () => listeners.delete(listener); },
      async bindExtensions() {},
      async prompt(_message: string, options?: RpcObject) {
        const preflight = options?.preflightResult;
        if (typeof preflight === "function") preflight("started");
        for (const listener of listeners) listener({ type: "agent_start" });
      },
      async abort() {
        setTimeout(() => { for (const listener of listeners) listener({ type: "agent_settled" }); }, 15);
      },
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

async function writeSession(path: string): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `${JSON.stringify({ type: "session", id: "session-id", cwd: "/tmp", timestamp: "2026-10-01T00:00:00Z" })}\n`);
}

test("reload replaces or additively merges spawn args and persists the effective config", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-gateway-reload-"));
  const oldRoot = process.env.PI_CODING_AGENT_SESSION_DIR;
  process.env.PI_CODING_AGENT_SESSION_DIR = join(root, "default-sessions");
  const token = "lifecycle-token";
  const path = join(root, "--tmp--", "reload.jsonl");
  const starts: string[][] = [];
  const daemon = new PiGatewayDaemon({ token, addr: "127.0.0.1:0", piBin: "/missing/pi-for-version-test",
    catalogRoots: [root], spawnDir: join(root, "state", "spawn"), createRuntime: fakeRuntime((args) => starts.push(args)) });
  let peer: ProtocolPeer | undefined;
  try {
    await writeSession(path);
    await daemon.listen();
    peer = await dial(daemon.addr!.port, token, path, ["--append-system-prompt", "base"]);
    assert.deepEqual(starts[0], ["--append-system-prompt", "base"]);
    peer.send({ type: "gw_reload_session", id: "add", addPiArgs: ["--extension", "/tmp/reload-extension.ts"] });
    const added = await peer.receive((frame) => frame.type === "response" && frame.id === "add");
    assert.equal(added.success, true);
    assert.deepEqual(starts[1], ["--model", "test/model", "--provider", "test", "--thinking", "medium",
      "--append-system-prompt", "base", "--extension", "/tmp/reload-extension.ts"]);
    peer.send({ type: "gw_reload_session", id: "replace", piArgs: ["--append-system-prompt", "replacement"] });
    const replaced = await peer.receive((frame) => frame.type === "response" && frame.id === "replace");
    assert.equal(replaced.success, true);
    assert.deepEqual(starts[2], ["--append-system-prompt", "replacement"]);
    peer.send({ type: "gw_list_sessions", id: "list-after-reload" });
    const list = await peer.receive((frame) => frame.type === "response" && frame.id === "list-after-reload");
    const sessions = (list.data as Record<string, unknown>).sessions as Array<Record<string, unknown>>;
    const row = sessions.find((item) => item.path === path)!;
    assert.deepEqual(row.spawn, { "append-system-prompt": ["replacement"] });
    peer.send({ type: "prompt", id: "prompt-during-reload", message: "hold turn" });
    const prompt = await peer.receive((frame) => frame.type === "response" && frame.id === "prompt-during-reload");
    assert.equal(prompt.success, true);
    peer.send({ type: "gw_reload_session", id: "refuse-running" });
    const refused = await peer.receive((frame) => frame.type === "response" && frame.id === "refuse-running");
    assert.equal(refused.success, false);
    assert.equal(refused.code, "reload_busy");
    peer.send({ type: "gw_reload_session", id: "refuse-add-running", addPiArgs: ["--extension", "/tmp/during-active.ts"] });
    const additiveRefused = await peer.receive((frame) => frame.type === "response" && frame.id === "refuse-add-running");
    assert.equal(additiveRefused.success, false);
    assert.equal(additiveRefused.code, "reload_busy");
    assert.equal(starts.length, 3, "additive refusal must not start a replacement runtime");
    peer.send({ type: "gw_reload_session", id: "force-running", force: true });
    const forced = await peer.receive((frame) => frame.type === "response" && frame.id === "force-running");
    assert.equal(forced.success, true);
    assert.deepEqual(starts[3], ["--append-system-prompt", "replacement"]);
  } finally {
    peer?.close();
    await daemon.shutdown();
    if (oldRoot === undefined) delete process.env.PI_CODING_AGENT_SESSION_DIR;
    else process.env.PI_CODING_AGENT_SESSION_DIR = oldRoot;
    await rm(root, { recursive: true, force: true });
  }
});

test("reload refuses shared sessions unless plain reload is forced", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-gateway-reload-shared-"));
  const oldRoot = process.env.PI_CODING_AGENT_SESSION_DIR;
  process.env.PI_CODING_AGENT_SESSION_DIR = join(root, "default-sessions");
  const token = "lifecycle-token";
  const path = join(root, "--tmp--", "shared-reload.jsonl");
  const starts: string[][] = [];
  const daemon = new PiGatewayDaemon({ token, addr: "127.0.0.1:0", piBin: "/missing/pi-for-version-test",
    catalogRoots: [root], createRuntime: fakeRuntime((args) => starts.push(args)) });
  const peers: ProtocolPeer[] = [];
  try {
    await writeSession(path);
    await daemon.listen();
    const first = await dial(daemon.addr!.port, token, path, ["--system-prompt", "original"]);
    const second = await dial(daemon.addr!.port, token, path);
    peers.push(first, second);
    first.send({ type: "gw_reload_session", id: "shared-refusal" });
    const refused = await first.receive((frame) => frame.type === "response" && frame.id === "shared-refusal");
    assert.equal(refused.success, false);
    assert.equal(refused.code, "reload_busy");
    first.send({ type: "gw_reload_session", id: "add-conflict", addPiArgs: ["--system-prompt", "replacement"] });
    const conflict = await first.receive((frame) => frame.type === "response" && frame.id === "add-conflict");
    assert.equal(conflict.success, false);
    assert.equal(conflict.code, "spawn_param_conflict");
    first.send({ type: "gw_reload_session", id: "add-shared-force", force: true, addPiArgs: ["--extension", "/tmp/extra.ts"] });
    const additiveRefused = await first.receive((frame) => frame.type === "response" && frame.id === "add-shared-force");
    assert.equal(additiveRefused.success, false);
    assert.equal(additiveRefused.code, "reload_busy");
    first.send({ type: "gw_reload_session", id: "forced-shared", force: true });
    const forced = await first.receive((frame) => frame.type === "response" && frame.id === "forced-shared");
    assert.equal(forced.success, true);
    const changed = await second.receive((frame) => frame.type === "gw_state_changed" && frame.command === "gw_reload_session");
    assert.equal((changed.by as Record<string, unknown>).name, "test");
    assert.equal(starts.length, 2);
  } finally {
    for (const peer of peers) peer.close();
    await daemon.shutdown();
    if (oldRoot === undefined) delete process.env.PI_CODING_AGENT_SESSION_DIR;
    else process.env.PI_CODING_AGENT_SESSION_DIR = oldRoot;
    await rm(root, { recursive: true, force: true });
  }
});

test("failed reload startup leaves the previous durable spawn config intact", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-gateway-reload-start-failure-"));
  const oldRoot = process.env.PI_CODING_AGENT_SESSION_DIR;
  process.env.PI_CODING_AGENT_SESSION_DIR = join(root, "default-sessions");
  const token = "lifecycle-token";
  const path = join(root, "--tmp--", "reload-start-fail.jsonl");
  const starts: string[][] = [];
  const daemon = new PiGatewayDaemon({ token, addr: "127.0.0.1:0", piBin: "/missing/pi-for-version-test",
    catalogRoots: [root], spawnDir: join(root, "state", "spawn"), createRuntime: fakeRuntime((args) => {
      starts.push(args);
      if (starts.length === 2) throw new Error("synthetic replacement startup failure");
    }) });
  let peer: ProtocolPeer | undefined;
  try {
    await writeSession(path);
    await daemon.listen();
    peer = await dial(daemon.addr!.port, token, path, ["--extension", "/old.ts"]);
    peer.send({ type: "gw_reload_session", id: "add-failing", addPiArgs: ["--extension", "/new.ts"] });
    const failed = await peer.receive((frame) => frame.type === "response" && frame.id === "add-failing");
    assert.equal(failed.success, false);
    assert.equal(failed.code, "session_crashed");
    peer.send({ type: "gw_list_sessions", id: "list-old-config" });
    const listed = await peer.receive((frame) => frame.type === "response" && frame.id === "list-old-config");
    const sessions = (listed.data as Record<string, unknown>).sessions as Array<Record<string, unknown>>;
    const row = sessions.find((item) => item.path === path)!;
    assert.deepEqual(row.spawn, { extension: ["/old.ts"] });
    assert.equal(starts.length, 2);
  } finally {
    peer?.close();
    await daemon.shutdown();
    if (oldRoot === undefined) delete process.env.PI_CODING_AGENT_SESSION_DIR;
    else process.env.PI_CODING_AGENT_SESSION_DIR = oldRoot;
    await rm(root, { recursive: true, force: true });
  }
});

test("an invalid client working directory is rejected before creating a session", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-gateway-bad-cwd-"));
  const daemon = new PiGatewayDaemon({ token: "lifecycle-token", addr: "127.0.0.1:0", piBin: "/missing/pi-for-version-test",
    catalogRoots: [root], createRuntime: fakeRuntime() });
  let peer: ProtocolPeer | undefined;
  try {
    await daemon.listen();
    const socket = connect({ host: "127.0.0.1", port: daemon.addr!.port });
    await new Promise<void>((resolve, reject) => { socket.once("connect", resolve); socket.once("error", reject); });
    peer = new ProtocolPeer(socket);
    peer.send({ type: "gw_hello", protocol: 1, token: "lifecycle-token", client: { name: "bad-cwd", kind: "test" }, cwd: join(root, "does-not-exist") });
    const error = await peer.receive((frame) => frame.type === "gw_error");
    assert.equal(error.code, "bad_frame");
    assert.equal(daemon.sessionActors.length, 0);
  } finally {
    peer?.close();
    await daemon.shutdown();
    await rm(root, { recursive: true, force: true });
  }
});

test("an attach racing additive reload validates against the committed spawn config", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-gateway-additive-attach-race-"));
  const token = "lifecycle-token";
  const path = join(root, "--tmp--", "additive-race.jsonl");
  await writeSession(path);
  let createCount = 0;
  let signalReplacement!: () => void;
  const replacementStarted = new Promise<void>((resolve) => { signalReplacement = resolve; });
  let releaseReplacement!: () => void;
  const replacementGate = new Promise<void>((resolve) => { releaseReplacement = resolve; });
  const starts: string[][] = [];
  const create = fakeRuntime((args) => starts.push(args));
  const daemon = new PiGatewayDaemon({ token, addr: "127.0.0.1:0", piBin: "/missing/pi-for-version-test", catalogRoots: [root],
    createRuntime: async (options) => {
      createCount++;
      if (createCount === 2) { signalReplacement(); await replacementGate; }
      return create(options);
    } });
  let owner: ProtocolPeer | undefined;
  let attaching: ProtocolPeer | undefined;
  try {
    await daemon.listen();
    owner = await dial(daemon.addr!.port, token, path, ["--extension", "/old.ts"]);
    owner.send({ type: "gw_reload_session", id: "add", addPiArgs: ["--session-dir", "/new-dir"] });
    await replacementStarted;

    const attachingPromise = dial(daemon.addr!.port, token, path, ["--session-dir", "/other-dir"], undefined, false);
    const pendingAttaches = (daemon as unknown as { pendingAttaches: Map<string, number> }).pendingAttaches;
    const deadline = Date.now() + 2000;
    while (pendingAttaches.get(path) !== 1 && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 5));
    assert.equal(pendingAttaches.get(path), 1, "the competing attach must reserve the path while waiting for reload commit");
    releaseReplacement();

    assert.equal((await owner.receive((frame) => frame.type === "response" && frame.id === "add")).success, true);
    attaching = await attachingPromise;
    assert.equal(attaching.welcome?.session, null, "failed hello attach must remain unbound");
    assert.equal(attaching.handshakeError?.code, "spawn_param_conflict");
    assert.equal(createCount, 2, "the conflicting attach must not start another Pi runtime");
    assert.ok(starts[1]?.includes("/new-dir"));
  } finally {
    releaseReplacement();
    owner?.close();
    attaching?.close();
    await daemon.shutdown();
    await rm(root, { recursive: true, force: true });
  }
});

test("an attach arriving during forced reload joins the restarting actor", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-gateway-reload-attach-race-"));
  const token = "lifecycle-token";
  const path = join(root, "--tmp--", "reload-race.jsonl");
  await writeSession(path);
  let createCount = 0;
  let signalSecondStart!: () => void;
  const secondStart = new Promise<void>((resolve) => { signalSecondStart = resolve; });
  let releaseSecondStart!: () => void;
  const secondGate = new Promise<void>((resolve) => { releaseSecondStart = resolve; });
  const create = fakeRuntime();
  const daemon = new PiGatewayDaemon({ token, addr: "127.0.0.1:0", piBin: "/missing/pi-for-version-test", catalogRoots: [root],
    createRuntime: async (options) => {
      createCount++;
      if (createCount === 2) { signalSecondStart(); await secondGate; }
      return create(options);
    } });
  let first: ProtocolPeer | undefined;
  let second: ProtocolPeer | undefined;
  try {
    await daemon.listen();
    first = await dial(daemon.addr!.port, token, path);
    first.send({ type: "gw_reload_session", id: "reload", force: true });
    await secondStart;

    const socket = connect({ host: "127.0.0.1", port: daemon.addr!.port });
    await new Promise<void>((resolve, reject) => { socket.once("connect", resolve); socket.once("error", reject); });
    second = new ProtocolPeer(socket);
    const welcomePromise = second.receive((frame) => frame.type === "gw_welcome");
    second.send({ type: "gw_hello", protocol: 1, token, client: { name: "mid-reload", kind: "test" }, session: path });
    await new Promise((resolve) => setTimeout(resolve, 30));
    assert.equal(createCount, 2, "attach must wait on the path lock rather than spawn a duplicate Pi");
    releaseSecondStart();

    assert.equal((await first.receive((frame) => frame.type === "response" && frame.id === "reload")).success, true);
    const welcome = await welcomePromise;
    assert.equal((welcome.session as Record<string, unknown>).path, path);
    second.send({ type: "get_state", id: "state-after-reload" });
    const state = await second.receive((frame) => frame.type === "response" && frame.id === "state-after-reload");
    assert.equal(state.success, true);
    assert.equal((state.data as Record<string, unknown>).sessionFile, path);
    assert.equal(daemon.sessionActors.length, 1);
    assert.equal(createCount, 2);
  } finally {
    releaseSecondStart();
    first?.close();
    second?.close();
    await daemon.shutdown();
    await rm(root, { recursive: true, force: true });
  }
});

test("reload timeout cancels a late-ready runtime", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-gateway-reload-timeout-"));
  const oldRoot = process.env.PI_CODING_AGENT_SESSION_DIR;
  process.env.PI_CODING_AGENT_SESSION_DIR = join(root, "default-sessions");
  const token = "lifecycle-token";
  const path = join(root, "--tmp--", "reload-timeout.jsonl");
  let starts = 0;
  let resolveLateDispose!: () => void;
  const lateDisposed = new Promise<void>((resolve) => { resolveLateDispose = resolve; });
  const daemon = new PiGatewayDaemon({ token, addr: "127.0.0.1:0", piBin: "/missing/pi-for-version-test",
    catalogRoots: [root], reloadTimeoutMs: 15, createRuntime: async (options) => {
      starts++;
      if (starts === 2) {
        await new Promise<void>((resolve) => setTimeout(resolve, 80));
        const runtime = await fakeRuntime()(options);
        const dispose = runtime.dispose.bind(runtime);
        runtime.dispose = async () => { resolveLateDispose(); await dispose(); };
        return runtime;
      }
      return fakeRuntime()(options);
    } });
  let peer: ProtocolPeer | undefined;
  try {
    await writeSession(path);
    await daemon.listen();
    peer = await dial(daemon.addr!.port, token, path);
    peer.send({ type: "gw_reload_session", id: "reload-timeout" });
    const timedOut = await peer.receive((frame) => frame.type === "response" && frame.id === "reload-timeout");
    assert.equal(timedOut.success, false);
    assert.equal(timedOut.code, "reload_timeout");
    await Promise.race([lateDisposed, new Promise<void>((_resolve, reject) => setTimeout(() => reject(new Error("late runtime was not disposed")), 1000))]);
    assert.equal(starts, 2);
  } finally {
    peer?.close();
    await daemon.shutdown();
    if (oldRoot === undefined) delete process.env.PI_CODING_AGENT_SESSION_DIR;
    else process.env.PI_CODING_AGENT_SESSION_DIR = oldRoot;
    await rm(root, { recursive: true, force: true });
  }
});

test("additive preflight timeout keeps the old runtime ready", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-gateway-reload-preflight-timeout-"));
  const oldRoot = process.env.PI_CODING_AGENT_SESSION_DIR;
  process.env.PI_CODING_AGENT_SESSION_DIR = join(root, "default-sessions");
  const token = "lifecycle-token";
  const path = join(root, "--tmp--", "preflight-timeout.jsonl");
  let starts = 0;
  const daemon = new PiGatewayDaemon({ token, addr: "127.0.0.1:0", piBin: "/missing/pi-for-version-test",
    catalogRoots: [root], reloadTimeoutMs: 25, createRuntime: async (options) => { starts++; return fakeRuntime()(options); } });
  let peer: ProtocolPeer | undefined;
  try {
    await writeSession(path);
    await daemon.listen();
    peer = await dial(daemon.addr!.port, token, path);
    const actors = (daemon as unknown as { actors: Map<string, { state: string; rpc?: { handle(command: RpcObject): Promise<void> } }> }).actors;
    const actor = actors.get(path)!;
    const rpc = actor.rpc!;
    const originalHandle = rpc.handle.bind(rpc);
    rpc.handle = async (command) => {
      if (command.type === "get_state") return;
      await originalHandle(command);
    };
    peer.send({ type: "gw_reload_session", id: "preflight-timeout", addPiArgs: ["--extension", "/tmp/add.ts"] });
    const timedOut = await peer.receive((frame) => frame.type === "response" && frame.id === "preflight-timeout");
    assert.equal(timedOut.success, false);
    assert.equal(timedOut.code, "reload_timeout");
    assert.equal(actor.state, "ready");
    assert.equal(starts, 1);
    peer.send({ type: "prompt", id: "old-runtime-still-works", message: "still here" });
    const prompt = await peer.receive((frame) => frame.type === "response" && frame.id === "old-runtime-still-works");
    assert.equal(prompt.success, true);
  } finally {
    peer?.close();
    await daemon.shutdown();
    if (oldRoot === undefined) delete process.env.PI_CODING_AGENT_SESSION_DIR;
    else process.env.PI_CODING_AGENT_SESSION_DIR = oldRoot;
    await rm(root, { recursive: true, force: true });
  }
});

test("reload reports sidecar persistence failure but keeps the live config", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-gateway-reload-persist-failure-"));
  const oldRoot = process.env.PI_CODING_AGENT_SESSION_DIR;
  process.env.PI_CODING_AGENT_SESSION_DIR = join(root, "default-sessions");
  const token = "lifecycle-token";
  const path = join(root, "--tmp--", "reload-persist.jsonl");
  const blockedSpawnDir = join(root, "blocked-spawn-dir");
  const starts: string[][] = [];
  const daemon = new PiGatewayDaemon({ token, addr: "127.0.0.1:0", piBin: "/missing/pi-for-version-test",
    catalogRoots: [root], spawnDir: blockedSpawnDir, createRuntime: fakeRuntime((args) => starts.push(args)) });
  let peer: ProtocolPeer | undefined;
  try {
    await writeFile(blockedSpawnDir, "not a directory");
    await writeSession(path);
    await daemon.listen();
    peer = await dial(daemon.addr!.port, token, path);
    peer.send({ type: "gw_reload_session", id: "replace-persist-fail", piArgs: ["--extension", "/tmp/new.ts"] });
    const failed = await peer.receive((frame) => frame.type === "response" && frame.id === "replace-persist-fail");
    assert.equal(failed.success, false);
    assert.equal(failed.code, "spawn_config_persist_failed");
    assert.deepEqual(starts[1], ["--extension", "/tmp/new.ts"]);
    peer.send({ type: "gw_list_sessions", id: "list-live-config" });
    const listed = await peer.receive((frame) => frame.type === "response" && frame.id === "list-live-config");
    const sessions = (listed.data as Record<string, unknown>).sessions as Array<Record<string, unknown>>;
    const row = sessions.find((item) => item.path === path)!;
    assert.deepEqual(row.spawn, { extension: ["/tmp/new.ts"] });
  } finally {
    peer?.close();
    await daemon.shutdown();
    if (oldRoot === undefined) delete process.env.PI_CODING_AGENT_SESSION_DIR;
    else process.env.PI_CODING_AGENT_SESSION_DIR = oldRoot;
    await rm(root, { recursive: true, force: true });
  }
});

test("gateway stop keeps the file; delete removes it and prevents resurrection", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-gateway-lifecycle-"));
  const oldRoot = process.env.PI_CODING_AGENT_SESSION_DIR;
  process.env.PI_CODING_AGENT_SESSION_DIR = join(root, "default-sessions");
  const token = "lifecycle-token";
  const stopPath = join(root, "--tmp--", "stop.jsonl");
  const deletePath = join(root, "--tmp--", "delete.jsonl");
  const daemon = new PiGatewayDaemon({ token, addr: "127.0.0.1:0", piBin: "/missing/pi-for-version-test",
    catalogRoots: [root], spawnDir: join(root, "state", "spawn"), createRuntime: fakeRuntime() });
  const peers: ProtocolPeer[] = [];
  try {
    await writeSession(stopPath);
    await writeSession(deletePath);
    await daemon.listen();
    const port = daemon.addr!.port;
    const stopper = await dial(port, token, stopPath);
    peers.push(stopper);
    stopper.send({ type: "gw_stop_session", id: "stop-1" });
    const stopResponse = await stopper.receive((frame) => frame.type === "response" && frame.id === "stop-1");
    assert.equal(stopResponse.success, true);
    const stopData = stopResponse.data as Record<string, unknown>;
    assert.equal(stopData.piStopped, true);
    assert.equal(stopData.unbound, true);
    assert.equal("fileDeleted" in stopData, false);
    await stat(stopPath);
    stopper.send({ type: "gw_list_sessions", id: "list-after-stop" });
    const listed = await stopper.receive((frame) => frame.type === "response" && frame.id === "list-after-stop");
    const sessions = (listed.data as Record<string, unknown>).sessions as Array<Record<string, unknown>>;
    assert.equal(sessions.find((row) => row.path === stopPath)?.live, false);

    const deleter = await dial(port, token, deletePath);
    peers.push(deleter);
    deleter.send({ type: "gw_delete_session", id: "delete-1" });
    const deleteResponse = await deleter.receive((frame) => frame.type === "response" && frame.id === "delete-1");
    assert.equal(deleteResponse.success, true);
    const deleteData = deleteResponse.data as Record<string, unknown>;
    assert.equal(deleteData.piStopped, true);
    assert.equal(deleteData.fileDeleted, true);
    assert.equal(deleteData.unbound, true);
    await assert.rejects(stat(deletePath), { code: "ENOENT" });
    deleter.send({ type: "prompt", id: "prompt-after-delete", message: "must not resurrect" });
    const rejected = await deleter.receive((frame) => frame.type === "response" && frame.id === "prompt-after-delete");
    assert.equal(rejected.success, false);
    assert.equal(rejected.code, "unknown_session");
    assert.equal(daemon.sessionActors.length, 0, "implicit commands after delete must not create a new actor");
    deleter.send({ type: "switch_session", id: "switch-deleted", sessionPath: deletePath });
    const switchResponse = await deleter.receive((frame) => frame.type === "response" && frame.id === "switch-deleted");
    assert.equal(switchResponse.success, false);
    assert.equal(switchResponse.code, "unknown_session");
    deleter.send({ type: "switch_session", id: "explicit-reattach", sessionPath: stopPath });
    assert.equal((await deleter.receive((frame) => frame.type === "response" && frame.id === "explicit-reattach")).success, true);
  } finally {
    for (const peer of peers) peer.close();
    await daemon.shutdown();
    if (oldRoot === undefined) delete process.env.PI_CODING_AGENT_SESSION_DIR;
    else process.env.PI_CODING_AGENT_SESSION_DIR = oldRoot;
    await rm(root, { recursive: true, force: true });
  }
});

test("stop refuses an attached peer unless forced, then reports detached clients", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-gateway-stop-attached-"));
  const oldRoot = process.env.PI_CODING_AGENT_SESSION_DIR;
  process.env.PI_CODING_AGENT_SESSION_DIR = join(root, "default-sessions");
  const token = "lifecycle-token";
  const path = join(root, "--tmp--", "shared.jsonl");
  const daemon = new PiGatewayDaemon({ token, addr: "127.0.0.1:0", piBin: "/missing/pi-for-version-test",
    catalogRoots: [root], createRuntime: fakeRuntime() });
  const peers: ProtocolPeer[] = [];
  try {
    await writeSession(path);
    await daemon.listen();
    const first = await dial(daemon.addr!.port, token, path);
    const second = await dial(daemon.addr!.port, token, path);
    peers.push(first, second);
    first.send({ type: "gw_stop_session", id: "refuse-stop" });
    const refused = await first.receive((frame) => frame.type === "response" && frame.id === "refuse-stop");
    assert.equal(refused.success, false);
    assert.equal(refused.code, "session_attached");
    first.send({ type: "gw_stop_session", id: "force-stop", force: true });
    const stopped = await first.receive((frame) => frame.type === "response" && frame.id === "force-stop");
    assert.equal(stopped.success, true);
    assert.equal((stopped.data as Record<string, unknown>).detachedClients, 1);
  } finally {
    for (const peer of peers) peer.close();
    await daemon.shutdown();
    if (oldRoot === undefined) delete process.env.PI_CODING_AGENT_SESSION_DIR;
    else process.env.PI_CODING_AGENT_SESSION_DIR = oldRoot;
    await rm(root, { recursive: true, force: true });
  }
});

test("stop refuses a running turn unless forced and waits for abort settlement", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-gateway-stop-turn-"));
  const oldRoot = process.env.PI_CODING_AGENT_SESSION_DIR;
  process.env.PI_CODING_AGENT_SESSION_DIR = join(root, "default-sessions");
  const token = "lifecycle-token";
  const path = join(root, "--tmp--", "running.jsonl");
  const daemon = new PiGatewayDaemon({ token, addr: "127.0.0.1:0", piBin: "/missing/pi-for-version-test",
    catalogRoots: [root], createRuntime: fakeRuntime() });
  let peer: ProtocolPeer | undefined;
  try {
    await writeSession(path);
    await daemon.listen();
    peer = await dial(daemon.addr!.port, token, path);
    peer.send({ type: "prompt", id: "prompt-1", message: "run" });
    const prompt = await peer.receive((frame) => frame.type === "response" && frame.id === "prompt-1");
    assert.equal(prompt.success, true);
    peer.send({ type: "gw_stop_session", id: "busy-stop" });
    const busy = await peer.receive((frame) => frame.type === "response" && frame.id === "busy-stop");
    assert.equal(busy.success, false);
    assert.equal(busy.code, "session_busy");
    peer.send({ type: "gw_stop_session", id: "force-stop", force: true });
    const stopped = await peer.receive((frame) => frame.type === "response" && frame.id === "force-stop");
    assert.equal(stopped.success, true);
    assert.equal((stopped.data as Record<string, unknown>).piStopped, true);
  } finally {
    peer?.close();
    await daemon.shutdown();
    if (oldRoot === undefined) delete process.env.PI_CODING_AGENT_SESSION_DIR;
    else process.env.PI_CODING_AGENT_SESSION_DIR = oldRoot;
    await rm(root, { recursive: true, force: true });
  }
});

test("forced stop bounds an unresponsive turn and preserves its session file", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-gateway-stop-grace-"));
  const oldRoot = process.env.PI_CODING_AGENT_SESSION_DIR;
  process.env.PI_CODING_AGENT_SESSION_DIR = join(root, "default-sessions");
  const token = "lifecycle-token";
  const path = join(root, "--tmp--", "unresponsive.jsonl");
  const baseRuntime = fakeRuntime();
  const daemon = new PiGatewayDaemon({ token, addr: "127.0.0.1:0", piBin: "/missing/pi-for-version-test",
    catalogRoots: [root], stopGraceMs: 30, createRuntime: async (options) => {
      const runtime = await baseRuntime(options);
      runtime.session.abort = async () => {};
      return runtime;
    } });
  let peer: ProtocolPeer | undefined;
  try {
    await writeSession(path);
    await daemon.listen();
    peer = await dial(daemon.addr!.port, token, path);
    peer.send({ type: "prompt", id: "unresponsive-turn", message: "ignore abort" });
    assert.equal((await peer.receive((frame) => frame.type === "response" && frame.id === "unresponsive-turn")).success, true);
    const started = Date.now();
    peer.send({ type: "gw_stop_session", id: "bounded-stop", force: true });
    const stopped = await peer.receive((frame) => frame.type === "response" && frame.id === "bounded-stop");
    assert.equal(stopped.success, true);
    assert.ok(Date.now() - started < 1000, "forced stop should honor its grace deadline");
    const terminal = await peer.receive((frame) => frame.type === "gw_session_state" && frame.state === "stopped");
    assert.equal(terminal.reason, "forced");
    await stat(path);
    assert.equal((await daemon.status()).live, 0);
  } finally {
    peer?.close();
    await daemon.shutdown();
    if (oldRoot === undefined) delete process.env.PI_CODING_AGENT_SESSION_DIR;
    else process.env.PI_CODING_AGENT_SESSION_DIR = oldRoot;
    await rm(root, { recursive: true, force: true });
  }
});

test("delete waits for Pi runtime disposal before unlinking the session file", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-gateway-delete-reap-"));
  const oldRoot = process.env.PI_CODING_AGENT_SESSION_DIR;
  process.env.PI_CODING_AGENT_SESSION_DIR = join(root, "default-sessions");
  const token = "lifecycle-token";
  const path = join(root, "--tmp--", "delete-after-dispose.jsonl");
  const baseRuntime = fakeRuntime();
  let signalDispose!: () => void;
  const disposeStarted = new Promise<void>((resolve) => { signalDispose = resolve; });
  let releaseDispose!: () => void;
  const disposeGate = new Promise<void>((resolve) => { releaseDispose = resolve; });
  const daemon = new PiGatewayDaemon({ token, addr: "127.0.0.1:0", piBin: "/missing/pi-for-version-test",
    catalogRoots: [root], createRuntime: async (options) => {
      const runtime = await baseRuntime(options);
      const dispose = runtime.dispose.bind(runtime);
      runtime.dispose = async () => { signalDispose(); await disposeGate; await dispose(); };
      return runtime;
    } });
  let peer: ProtocolPeer | undefined;
  try {
    await writeSession(path);
    await daemon.listen();
    peer = await dial(daemon.addr!.port, token, path);
    peer.send({ type: "gw_delete_session", id: "delete-after-reap" });
    const responsePromise = peer.receive((frame) => frame.type === "response" && frame.id === "delete-after-reap");
    await disposeStarted;
    await stat(path);
    let completed = false;
    void responsePromise.then(() => { completed = true; });
    await new Promise((resolve) => setTimeout(resolve, 30));
    assert.equal(completed, false, "delete response must wait for runtime disposal");
    await stat(path);
    releaseDispose();
    const response = await responsePromise;
    assert.equal(response.success, true);
    await assert.rejects(stat(path), { code: "ENOENT" });
  } finally {
    releaseDispose();
    peer?.close();
    await daemon.shutdown();
    if (oldRoot === undefined) delete process.env.PI_CODING_AGENT_SESSION_DIR;
    else process.env.PI_CODING_AGENT_SESSION_DIR = oldRoot;
    await rm(root, { recursive: true, force: true });
  }
});

test("delete removes a valid hibernated session without starting Pi", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-gateway-delete-hibernated-"));
  const oldRoot = process.env.PI_CODING_AGENT_SESSION_DIR;
  process.env.PI_CODING_AGENT_SESSION_DIR = join(root, "default-sessions");
  const token = "lifecycle-token";
  const path = join(root, "--tmp--", "hibernated.jsonl");
  let starts = 0;
  const daemon = new PiGatewayDaemon({ token, addr: "127.0.0.1:0", piBin: "/missing/pi-for-version-test",
    catalogRoots: [root], createRuntime: async (options) => { starts++; return fakeRuntime()(options); } });
  let peer: ProtocolPeer | undefined;
  try {
    await writeSession(path);
    await daemon.listen();
    peer = await dial(daemon.addr!.port, token);
    peer.send({ type: "gw_delete_session", id: "delete-hibernated", session: path });
    const deleted = await peer.receive((frame) => frame.type === "response" && frame.id === "delete-hibernated");
    assert.equal(deleted.success, true);
    const data = deleted.data as Record<string, unknown>;
    assert.equal(data.piStopped, false);
    assert.equal(data.fileDeleted, true);
    assert.equal(starts, 0);
    await assert.rejects(stat(path), { code: "ENOENT" });
    await assert.rejects(stat(dirname(path)), { code: "ENOENT" });
  } finally {
    peer?.close();
    await daemon.shutdown();
    if (oldRoot === undefined) delete process.env.PI_CODING_AGENT_SESSION_DIR;
    else process.env.PI_CODING_AGENT_SESSION_DIR = oldRoot;
    await rm(root, { recursive: true, force: true });
  }
});

test("gateway refuses deleting arbitrary files outside configured session roots", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-gateway-lifecycle-root-"));
  const outside = await mkdtemp(join(tmpdir(), "pi-gateway-lifecycle-outside-"));
  const oldRoot = process.env.PI_CODING_AGENT_SESSION_DIR;
  process.env.PI_CODING_AGENT_SESSION_DIR = join(root, "default-sessions");
  const token = "lifecycle-token";
  const path = join(outside, "not-a-session.jsonl");
  const daemon = new PiGatewayDaemon({ token, addr: "127.0.0.1:0", piBin: "/missing/pi-for-version-test",
    catalogRoots: [root], createRuntime: fakeRuntime() });
  let peer: ProtocolPeer | undefined;
  try {
    await writeFile(path, "do not delete\n");
    await daemon.listen();
    peer = await dial(daemon.addr!.port, token);
    peer.send({ type: "gw_delete_session", id: "delete-outside" });
    const result = await peer.receive((frame) => frame.type === "response" && frame.id === "delete-outside");
    assert.equal(result.success, false);
    assert.equal(result.code, "unknown_session");
    assert.equal(await readFile(path, "utf8"), "do not delete\n");
  } finally {
    peer?.close();
    await daemon.shutdown();
    if (oldRoot === undefined) delete process.env.PI_CODING_AGENT_SESSION_DIR;
    else process.env.PI_CODING_AGENT_SESSION_DIR = oldRoot;
    await rm(root, { recursive: true, force: true });
    await rm(outside, { recursive: true, force: true });
  }
});
