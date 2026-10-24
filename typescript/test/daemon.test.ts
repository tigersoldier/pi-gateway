import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PassThrough } from "node:stream";
import { connect } from "node:net";
import { test } from "node:test";
import { DIR_ENV, loadOrCreateToken, tokenPath } from "../src/config.ts";
import { runBridge } from "../src/bridge.ts";
import { PiGatewayDaemon } from "../src/daemon.ts";
import type { RpcObject, RpcSessionRuntime } from "../src/rpc-adapter.ts";
import { dialGateway, WirePeer, writeSessionFile } from "./support/gateway.ts";

function fakeRuntime(sessionFile = "/tmp/pi-gateway-e2e.jsonl"): RpcSessionRuntime {
  const listeners = new Set<(event: RpcObject) => void>();
  const model = { provider: "test", id: "test/model" };
  const emit = (event: RpcObject) => { for (const listener of listeners) listener(event); };
  const session = {
    model,
    thinkingLevel: "medium",
    isStreaming: false,
    isCompacting: false,
    steeringMode: "one-at-a-time",
    followUpMode: "one-at-a-time",
    sessionFile,
    sessionId: "e2e-session",
    sessionName: "e2e",
    autoCompactionEnabled: true,
    pendingMessageCount: 0,
    messages: [],
    promptTemplates: [],
    subscribe(listener: (event: RpcObject) => void) { listeners.add(listener); return () => listeners.delete(listener); },
    async bindExtensions() {},
    async prompt(message: string, options?: RpcObject) {
      const preflight = options?.preflightResult;
      if (typeof preflight === "function") preflight("started");
      emit({ type: "agent_start" });
      setTimeout(() => {
        emit({ type: "message_start", message: { role: "assistant" } });
        emit({ type: "message_end", message: { role: "assistant", content: [{ type: "text", text: `fake reply: ${message}` }] } });
        emit({ type: "agent_settled" });
      }, 5);
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
}

function waitForRecord(
  records: Array<Record<string, unknown>>,
  predicate: (record: Record<string, unknown>) => boolean,
  timeoutMs = 3000,
): Promise<Record<string, unknown>> {
  const existing = records.find(predicate);
  if (existing) return Promise.resolve(existing);
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => { cleanup(); reject(new Error("timed out waiting for daemon record")); }, timeoutMs);
    const poll = () => {
      const record = records.find(predicate);
      if (record) { cleanup(); resolve(record); }
    };
    const interval = setInterval(poll, 2);
    const cleanup = () => { clearTimeout(timer); clearInterval(interval); };
  });
}

test("bridge, TCP daemon, session actor, and fake Pi complete a prompt round trip", async () => {
  const temp = await mkdtemp(join(tmpdir(), "pi-gateway-daemon-e2e-"));
  const token = await loadOrCreateToken(tokenPath(temp));
  const daemon = new PiGatewayDaemon({
    addr: "127.0.0.1:0",
    token,
    piBin: "/missing/pi-for-version-test",
    shortGraceMs: 60_000,
    createRuntime: async () => fakeRuntime(),
  });
  const stdin = new PassThrough();
  const stdout = new PassThrough();
  const stderr = new PassThrough();
  const records: Array<Record<string, unknown>> = [];
  let pending = "";
  stdout.on("data", (chunk: Buffer) => {
    pending += chunk.toString("utf8");
    for (;;) {
      const index = pending.indexOf("\n");
      if (index < 0) break;
      const line = pending.slice(0, index);
      pending = pending.slice(index + 1);
      if (line) records.push(JSON.parse(line) as Record<string, unknown>);
    }
  });
  try {
    await daemon.listen();
    const address = daemon.addr;
    assert.ok(address);
    const bridge = runBridge({
      args: ["--mode", "rpc", "--server", `127.0.0.1:${address.port}`],
      stdin,
      stdout,
      stderr,
      env: { [DIR_ENV]: temp },
      cwd: () => "/tmp",
    });
    stdin.write('{"type":"prompt","id":"prompt-1","message":"hello from test"}\n');
    const response = await waitForRecord(records, (record) => record.type === "response" && record.id === "prompt-1");
    assert.equal(response.success, true);
    assert.equal((response.data as Record<string, unknown>).disposition, "started");
    const message = await waitForRecord(records, (record) => record.type === "message_end");
    assert.match(JSON.stringify(message), /fake reply: hello from test/);
    assert.equal(records.some((record) => typeof record.type === "string" && record.type.startsWith("gw_")), false);
    stdin.end();
    assert.equal(await bridge, 0);
    assert.equal(stderr.readableLength, 0);
  } finally {
    stdin.destroy();
    await daemon.shutdown();
    await rm(temp, { recursive: true, force: true });
  }
});

test("daemon rejects bad tokens before accepting Pi traffic", async () => {
  const temp = await mkdtemp(join(tmpdir(), "pi-gateway-bad-token-"));
  const token = await loadOrCreateToken(tokenPath(temp));
  const daemon = new PiGatewayDaemon({ addr: "127.0.0.1:0", token, piBin: "/missing/pi-for-version-test" });
  try {
    await daemon.listen();
    const address = daemon.addr;
    assert.ok(address);
    const clientInput = new PassThrough();
    const clientOutput = new PassThrough();
    const clientError = new PassThrough();
    const errors: Buffer[] = [];
    clientError.on("data", (chunk: Buffer) => errors.push(Buffer.from(chunk)));
    await loadOrCreateToken(tokenPath(join(temp, "wrong")));
    const code = await runBridge({
      args: ["--mode", "rpc", "--server", `127.0.0.1:${address.port}`, "--token-file", tokenPath(join(temp, "wrong"))],
      stdin: clientInput,
      stdout: clientOutput,
      stderr: clientError,
      env: { [DIR_ENV]: temp },
    });
    assert.equal(code, 3);
    assert.match(Buffer.concat(errors).toString("utf8"), /daemon rejected the token/);
  } finally {
    await daemon.shutdown();
    await rm(temp, { recursive: true, force: true });
  }
});

test("unsupported and noncanonical commands are rejected before lazy Pi creation", async () => {
  let starts = 0;
  const daemon = new PiGatewayDaemon({ token: "security-token", tokens: [{ name: "observer", token: "observer-token", capabilities: ["observe"] }],
    addr: "127.0.0.1:0", piBin: "/missing/pi-for-version-test", createRuntime: async () => { starts++; return fakeRuntime(); } });
  let peer: Awaited<ReturnType<typeof dialGateway>> | undefined;
  try {
    await daemon.listen();
    peer = await dialGateway(daemon.addr!.port, "observer-token");
    peer.send({ type: "unknown_pi_command", id: "unsupported" });
    const unsupported = await peer.receive((frame) => frame.type === "response" && frame.id === "unsupported");
    assert.equal(unsupported.success, false);
    assert.equal(unsupported.code, "not_supported");
    assert.equal(unsupported.error, "unknown_pi_command is not supported by this daemon revision");
    peer.send({ type: "Get_State", id: "case-variant" });
    assert.equal((await peer.receive((frame) => frame.type === "gw_error")).code, "bad_frame");
    assert.equal(starts, 0);
    assert.equal((await daemon.status()).registered, 0);
  } finally {
    peer?.close();
    await daemon.shutdown();
  }
});

test("attaching to an unknown session increments the failure metric", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-gateway-attach-failure-"));
  const oldRoot = process.env.PI_CODING_AGENT_SESSION_DIR;
  process.env.PI_CODING_AGENT_SESSION_DIR = join(root, "default-sessions");
  const daemon = new PiGatewayDaemon({ token: "security-token", addr: "127.0.0.1:0", catalogRoots: [join(root, "sessions")] });
  let peer: Awaited<ReturnType<typeof dialGateway>> | undefined;
  try {
    await daemon.listen();
    peer = await dialGateway(daemon.addr!.port, "security-token", { session: "definitely-not-a-session", expectHandshakeError: true });
    assert.equal(peer.welcome?.session, null);
    assert.equal(peer.handshakeError?.code, "unknown_session");
    assert.equal(daemon.metricSnapshot.get("pi_gateway_attach_failures_total"), 1);
  } finally {
    peer?.close();
    await daemon.shutdown();
    if (oldRoot === undefined) delete process.env.PI_CODING_AGENT_SESSION_DIR;
    else process.env.PI_CODING_AGENT_SESSION_DIR = oldRoot;
    await rm(root, { recursive: true, force: true });
  }
});

test("the client listener refuses non-loopback addresses", async () => {
  for (const addr of ["0.0.0.0:7331", ":7331", "10.1.2.3:7331"]) {
    const daemon = new PiGatewayDaemon({ token: "security-token", addr });
    await assert.rejects(daemon.listen(), /loopback/);
    await daemon.shutdown();
  }
});

test("an empty daemon token starts with no grants and rejects the empty client token", async () => {
  const daemon = new PiGatewayDaemon({ token: "", addr: "127.0.0.1:0", piBin: "/missing/pi-for-version-test" });
  let peer: WirePeer | undefined;
  try {
    await daemon.listen();
    const socket = connect({ host: "127.0.0.1", port: daemon.addr!.port });
    await new Promise<void>((resolve, reject) => { socket.once("connect", resolve); socket.once("error", reject); });
    peer = new WirePeer(socket);
    peer.send({ type: "gw_hello", protocol: 1, token: "", client: { name: "empty", kind: "test" } });
    const error = await peer.receive((frame) => frame.type === "gw_error");
    assert.equal(error.code, "unauthorized");
    assert.equal((await daemon.status()).tokens, 0);
    assert.equal(daemon.metricSnapshot.get("pi_gateway_unauthorized_total"), 1);
  } finally {
    peer?.close();
    await daemon.shutdown();
  }
});

test("daemon enforces restricted capabilities before lazy session creation", async () => {
  const temp = await mkdtemp(join(tmpdir(), "pi-gateway-observer-"));
  const token = await loadOrCreateToken(tokenPath(temp));
  let runtimeStarts = 0;
  const daemon = new PiGatewayDaemon({
    addr: "127.0.0.1:0",
    token,
    tokens: [{ name: "observer", token: "observer-token", capabilities: ["observe"] }],
    piBin: "/missing/pi-for-version-test",
    createRuntime: async () => { runtimeStarts++; return fakeRuntime(); },
  });
  const stdin = new PassThrough();
  const stdout = new PassThrough();
  const stderr = new PassThrough();
  const records: Array<Record<string, unknown>> = [];
  let pending = "";
  stdout.on("data", (chunk: Buffer) => {
    pending += chunk.toString("utf8");
    for (;;) {
      const index = pending.indexOf("\n");
      if (index < 0) break;
      const line = pending.slice(0, index);
      pending = pending.slice(index + 1);
      if (line) records.push(JSON.parse(line) as Record<string, unknown>);
    }
  });
  try {
    await daemon.listen();
    const address = daemon.addr;
    assert.ok(address);
    const observerTokenPath = join(temp, "observer-token");
    await writeFile(observerTokenPath, "observer-token\n");
    const bridge = runBridge({
      args: ["--mode", "rpc", "--server", `127.0.0.1:${address.port}`, "--token-file", observerTokenPath],
      stdin,
      stdout,
      stderr,
      env: { [DIR_ENV]: temp },
    });
    stdin.write('{"type":"prompt","id":"denied","message":"must not start"}\n');
    const response = await waitForRecord(records, (record) => record.type === "response" && record.id === "denied");
    assert.equal(response.success, false);
    assert.equal(response.code, "forbidden");
    assert.equal(runtimeStarts, 0);
    stdin.end();
    assert.equal(await bridge, 0);
  } finally {
    stdin.destroy();
    await daemon.shutdown();
    await rm(temp, { recursive: true, force: true });
  }
});

test("bridge reconnects to the same live session after stdin disconnect", async () => {
  const temp = await mkdtemp(join(tmpdir(), "pi-gateway-bridge-reconnect-"));
  const token = await loadOrCreateToken(tokenPath(temp));
  const path = join(temp, "sessions", "reattach.jsonl");
  let starts = 0;
  const daemon = new PiGatewayDaemon({ addr: "127.0.0.1:0", token, piBin: "/missing/pi-for-version-test",
    idleTimeoutMs: 60_000, shortGraceMs: 60_000, createRuntime: async () => { starts++; return fakeRuntime(path); } });
  const run = () => {
    const stdin = new PassThrough(); const stdout = new PassThrough(); const stderr = new PassThrough();
    const records: Array<Record<string, unknown>> = [];
    let pending = "";
    stdout.on("data", (chunk: Buffer) => {
      pending += chunk.toString("utf8");
      for (;;) {
        const index = pending.indexOf("\n");
        if (index < 0) break;
        const line = pending.slice(0, index); pending = pending.slice(index + 1);
        if (line) records.push(JSON.parse(line) as Record<string, unknown>);
      }
    });
    const bridge = runBridge({ args: ["--mode", "rpc", "--server", `127.0.0.1:${daemon.addr!.port}`],
      stdin, stdout, stderr, env: { [DIR_ENV]: temp }, cwd: () => "/tmp" });
    return { stdin, stderr, records, bridge };
  };
  try {
    await daemon.listen();
    const first = run();
    first.stdin.write('{"type":"get_state","id":"first-state"}\n');
    const initial = await waitForRecord(first.records, (record) => record.type === "response" && record.id === "first-state");
    assert.equal(initial.success, true);
    assert.equal((initial.data as Record<string, unknown>).sessionFile, path);
    first.stdin.write('{"type":"prompt","id":"first-prompt","message":"before disconnect"}\n');
    await waitForRecord(first.records, (record) => record.type === "agent_settled");
    first.stdin.end();
    assert.equal(await first.bridge, 0);

    const second = run();
    second.stdin.write(`${JSON.stringify({ type: "switch_session", id: "reattach", sessionPath: path })}\n`);
    assert.equal((await waitForRecord(second.records, (record) => record.type === "response" && record.id === "reattach")).success, true);
    second.stdin.write('{"type":"get_state","id":"second-state"}\n');
    const restored = await waitForRecord(second.records, (record) => record.type === "response" && record.id === "second-state");
    assert.equal((restored.data as Record<string, unknown>).sessionFile, path);
    second.stdin.write('{"type":"prompt","id":"second-prompt","message":"after reconnect"}\n');
    assert.ok((await waitForRecord(second.records, (record) => record.type === "message_end")).message);
    assert.equal(starts, 1);
    second.stdin.end();
    assert.equal(await second.bridge, 0);
  } finally {
    await daemon.shutdown();
    await rm(temp, { recursive: true, force: true });
  }
});

test("bridge refuses commands after another client deletes its session", async () => {
  const temp = await mkdtemp(join(tmpdir(), "pi-gateway-bridge-delete-"));
  const token = await loadOrCreateToken(tokenPath(temp));
  const path = join(temp, "sessions", "deleted.jsonl");
  await writeSessionFile(path);
  let starts = 0;
  const daemon = new PiGatewayDaemon({ addr: "127.0.0.1:0", token, piBin: "/missing/pi-for-version-test", catalogRoots: [join(temp, "sessions")],
    createRuntime: async () => { starts++; return fakeRuntime(path); } });
  const stdin = new PassThrough(); const stdout = new PassThrough(); const stderr = new PassThrough();
  const records: Array<Record<string, unknown>> = [];
  let pending = "";
  stdout.on("data", (chunk: Buffer) => {
    pending += chunk.toString("utf8");
    for (;;) {
      const index = pending.indexOf("\n");
      if (index < 0) break;
      const line = pending.slice(0, index); pending = pending.slice(index + 1);
      if (line) records.push(JSON.parse(line) as Record<string, unknown>);
    }
  });
  const peers: Array<Awaited<ReturnType<typeof dialGateway>>> = [];
  try {
    await daemon.listen();
    const bridge = runBridge({ args: ["--mode", "rpc", "--server", `127.0.0.1:${daemon.addr!.port}`],
      stdin, stdout, stderr, env: { [DIR_ENV]: temp }, cwd: () => "/tmp" });
    stdin.write('{"type":"switch_session","id":"attach","sessionPath":"' + path + '"}\n');
    assert.equal((await waitForRecord(records, (record) => record.type === "response" && record.id === "attach")).success, true);
    const deleter = await dialGateway(daemon.addr!.port, token, { session: path });
    peers.push(deleter);
    deleter.send({ type: "gw_delete_session", id: "delete" });
    assert.equal((await deleter.receive((frame) => frame.type === "response" && frame.id === "delete")).success, true);
    stdin.write('{"type":"get_state","id":"after-delete"}\n');
    const rejected = await waitForRecord(records, (record) => record.type === "response" && record.id === "after-delete");
    assert.equal(rejected.success, false);
    assert.match(String(rejected.error), /deleted/);
    assert.equal(await bridge, 3);
    assert.equal(starts, 1, "post-delete RPC must not create a replacement session");
    assert.ok(!(await daemon.listSessions()).some((session) => session.path === path), "deleted session should be absent from the catalog");
  } finally {
    stdin.destroy();
    for (const peer of peers) peer.close();
    await daemon.shutdown();
    await rm(temp, { recursive: true, force: true });
  }
});

test("daemon token replacement is atomic and retains the primary full-authority token", () => {
  const daemon = new PiGatewayDaemon({ token: "primary-token", tokens: [{ name: "old", token: "old-token", capabilities: ["observe"] }] });
  assert.equal(daemon.authenticate("old-token")?.name, "old");
  daemon.replaceTokens([{ name: "new", token: "new-token", capabilities: ["prompt"] }]);
  assert.equal(daemon.authenticate("old-token"), undefined);
  assert.equal(daemon.authenticate("new-token")?.name, "new");
  assert.deepEqual(daemon.authenticate("primary-token")?.capabilities, ["observe", "interject", "prompt", "ui", "control", "admin"]);
  assert.throws(() => daemon.replaceTokens([{ name: "duplicate", token: "primary-token", capabilities: ["observe"] }]), /duplicates another token value/);
  assert.equal(daemon.authenticate("new-token")?.name, "new");
});
