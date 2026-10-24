import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PassThrough, Readable } from "node:stream";
import { getEventListeners } from "node:events";
import { createServer, type Socket } from "node:net";
import { test } from "node:test";
import { DIR_ENV, loadOrCreateToken, tokenPath } from "../src/config.ts";
import { JsonlInput, parseBridgeArgs, runBridge } from "../src/bridge.ts";

function encode(records: unknown[]): string { return `${records.map((record) => JSON.stringify(record)).join("\n")}\n`; }

test("JSONL input pauses upstream when frames await demand and resumes after draining", async () => {
  let produced = 0;
  const source = new Readable({
    highWaterMark: 1,
    read() {
      if (produced < 10_000) this.push(`${produced++}\n`);
      else this.push(null);
    },
  });
  const input = new JsonlInput(source);
  try {
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.equal(source.isPaused(), true);
    assert.ok(produced < 10_000, "the readable must stop producing while relay demand is blocked");
    assert.equal((await input.read()).toString(), "0");
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.equal(source.isPaused(), true, "the next unread frame pauses upstream again");
    assert.ok(produced < 10_000);
  } finally { input.close(); }
});

function frameReader(socket: Socket, onFrame: (record: Record<string, unknown>) => void): void {
  let pending = "";
  socket.on("data", (chunk: Buffer) => {
    pending += chunk.toString("utf8");
    for (;;) {
      const index = pending.indexOf("\n");
      if (index < 0) return;
      const line = pending.slice(0, index).replace(/\r$/, "");
      pending = pending.slice(index + 1);
      onFrame(JSON.parse(line) as Record<string, unknown>);
    }
  });
}

test("bridge parser consumes client options and requires RPC mode", () => {
  const parsed = parseBridgeArgs(["--mode=rpc", "--server", "127.0.0.1:7331", "--port=7332", "--token-file", "token", "--model", "m", "--approve"]);
  assert.equal(parsed.mode, "rpc");
  assert.equal(parsed.server, "127.0.0.1:7331");
  assert.equal(parsed.port, 7332);
  assert.equal(parsed.tokenFile, "token");
  assert.deepEqual(parsed.piArgs, ["--model", "m", "--approve"]);
  assert.throws(() => parseBridgeArgs([]), /run it as `pi-gateway --mode rpc`/);
  assert.throws(() => parseBridgeArgs(["--mode", "interactive"]), /only --mode rpc/);
  assert.throws(() => parseBridgeArgs(["--port", "0", "--mode", "rpc"]), /invalid --port/);
  assert.equal(parseBridgeArgs(["--help"]).help, true);
  assert.equal(parseBridgeArgs(["--version"]).version, true);
});

test("bridge runner returns documented CLI and unknown Pi-option failures", async () => {
  const run = async (args: string[]) => {
    const stdout = new PassThrough();
    const stderr = new PassThrough();
    const out: Buffer[] = [];
    const err: Buffer[] = [];
    stdout.on("data", (chunk: Buffer) => out.push(Buffer.from(chunk)));
    stderr.on("data", (chunk: Buffer) => err.push(Buffer.from(chunk)));
    const code = await runBridge({ args, stdin: new PassThrough(), stdout, stderr, env: {} });
    return { code, stdout: Buffer.concat(out).toString("utf8"), stderr: Buffer.concat(err).toString("utf8") };
  };

  const bare = await run([]);
  assert.equal(bare.code, 2);
  assert.match(bare.stderr, /run it as `pi-gateway --mode rpc`/);
  const tui = await run(["--mode", "interactive"]);
  assert.equal(tui.code, 2);
  assert.match(tui.stderr, /only --mode rpc is supported/);
  const unknown = await run(["--mode", "rpc", "--unknown-pi-option"]);
  assert.equal(unknown.code, 2);
  assert.match(unknown.stderr, /unsupported pi option "--unknown-pi-option"/);
  const help = await run(["--help"]);
  assert.equal(help.code, 0);
  assert.match(help.stdout, /Usage:/);
});

test("bridge handshakes, forwards pristine Pi RPC, and hides gateway traffic", async () => {
  const temp = await mkdtemp(join(tmpdir(), "pi-gateway-bridge-"));
  const server = createServer();
  try {
    const token = await loadOrCreateToken(tokenPath(temp));
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    assert.ok(address && typeof address === "object");
    let resolveHello!: (hello: Record<string, unknown>) => void;
    let resolvePrompt!: (command: Record<string, unknown>) => void;
    const hello = new Promise<Record<string, unknown>>((resolve) => { resolveHello = resolve; });
    const prompt = new Promise<Record<string, unknown>>((resolve) => { resolvePrompt = resolve; });
    server.on("connection", (socket) => {
      frameReader(socket, (record) => {
        if (record.type === "gw_hello") {
          resolveHello(record);
          socket.write(encode([
            { type: "gw_welcome", protocol: 1, clientId: "c_1", piVersion: "1.3.0" },
            { type: "gw_turn", state: "running", gw_seq: 1, gw_session: "s1" },
            { type: "message_update", delta: "answer", gw_seq: 2, gw_session: "s1", gw_ts: "now" },
          ]));
        } else resolvePrompt(record);
      });
    });

    const stdin = new PassThrough();
    const stdout = new PassThrough();
    const abort = new AbortController();
    const stderr = new PassThrough();
    const output: Buffer[] = [];
    stdout.on("data", (chunk: Buffer) => output.push(Buffer.from(chunk)));
    const result = runBridge({
      args: ["--mode", "rpc", "--server", `127.0.0.1:${address.port}`, "--model", "test/model"],
      stdin,
      stdout,
      stderr,
      env: { [DIR_ENV]: temp, PI_GATEWAY_CLIENT_NAME: "test-client" },
      cwd: () => "/project",
      signal: abort.signal,
    });
    const receivedHello = await hello;
    assert.equal(receivedHello.protocol, 1);
    assert.equal(receivedHello.token, token);
    assert.equal((receivedHello.client as Record<string, unknown>).name, "test-client");
    assert.equal(receivedHello.cwd, "/project");
    assert.equal(receivedHello.liveOnly, true);
    assert.deepEqual(receivedHello.piArgs, ["--model", "test/model"]);

    stdin.write('{"type":"prompt","id":"req-1","message":"hello"}\n');
    const forwarded = await prompt;
    assert.equal(forwarded.type, "prompt");
    assert.equal(forwarded.id, "req-1");
    stdin.end();
    assert.equal(await result, 0);
    assert.deepEqual(getEventListeners(abort.signal, "abort"), [], "bridge removes abort listeners after each run");
    await new Promise<void>((resolve) => server.close(() => resolve()));

    const lines = Buffer.concat(output).toString("utf8").trim().split("\n").map((line) => JSON.parse(line) as Record<string, unknown>);
    assert.equal(lines.length, 1);
    assert.deepEqual(lines[0], { type: "message_update", delta: "answer" });
    assert.equal(stderr.readableLength, 0);
  } finally {
    if (server.listening) await new Promise<void>((resolve) => server.close(() => resolve()));
    await rm(temp, { recursive: true, force: true });
  }
});

test("bridge classifies a post-welcome daemon write failure as exit 3", async () => {
  const temp = await mkdtemp(join(tmpdir(), "pi-gateway-write-failure-"));
  try {
    await loadOrCreateToken(tokenPath(temp));
    const socket = new PassThrough() as unknown as Socket;
    socket.setNoDelay = () => socket;
    let resolveHello!: () => void;
    const hello = new Promise<void>((resolve) => { resolveHello = resolve; });
    socket.write = ((chunk: string | Uint8Array, ...args: unknown[]) => {
      const callback = args.find((value) => typeof value === "function") as ((error?: Error | null) => void) | undefined;
      const record = JSON.parse(Buffer.from(chunk).toString("utf8").trim()) as Record<string, unknown>;
      if (record.type === "gw_hello") {
        socket.emit("data", Buffer.from(encode([{ type: "gw_welcome", protocol: 1, clientId: "c_write" }])));
        resolveHello();
        callback?.(null);
      } else {
        callback?.(new Error("forced daemon write failure"));
      }
      return true;
    }) as Socket["write"];

    const stdin = new PassThrough();
    const stderr = new PassThrough();
    const errorChunks: Buffer[] = [];
    stderr.on("data", (chunk: Buffer) => errorChunks.push(Buffer.from(chunk)));
    const result = runBridge({
      args: ["--mode", "rpc", "--server", "127.0.0.1:1"],
      stdin,
      stdout: new PassThrough(),
      stderr,
      env: { [DIR_ENV]: temp },
      dial: async () => socket,
    });
    await hello;
    await new Promise<void>((resolve) => setImmediate(resolve));
    stdin.write('{"type":"prompt","id":"write-failure","message":"hello"}\n');
    assert.equal(await result, 3);
    assert.match(Buffer.concat(errorChunks).toString("utf8"), /daemon connection lost: forced daemon write failure/);
  } finally { await rm(temp, { recursive: true, force: true }); }
});

test("bridge sends an empty cwd when the process working directory is unavailable", async () => {
  const temp = await mkdtemp(join(tmpdir(), "pi-gateway-cwd-fallback-"));
  const server = createServer();
  try {
    await loadOrCreateToken(tokenPath(temp));
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    assert.ok(address && typeof address === "object");
    let resolveHello!: (hello: Record<string, unknown>) => void;
    const hello = new Promise<Record<string, unknown>>((resolve) => { resolveHello = resolve; });
    server.on("connection", (socket) => frameReader(socket, (record) => {
      if (record.type === "gw_hello") {
        resolveHello(record);
        socket.write(encode([{ type: "gw_welcome", protocol: 1, clientId: "c_cwd" }]));
      }
    }));
    const stdin = new PassThrough();
    const result = runBridge({
      args: ["--mode", "rpc", "--server", `127.0.0.1:${address.port}`],
      stdin,
      stdout: new PassThrough(),
      stderr: new PassThrough(),
      env: { [DIR_ENV]: temp },
      cwd: () => { throw new Error("cwd was removed"); },
    });
    assert.equal((await hello).cwd, "");
    stdin.end();
    assert.equal(await result, 0);
  } finally {
    if (server.listening) await new Promise<void>((resolve) => server.close(() => resolve()));
    await rm(temp, { recursive: true, force: true });
  }
});

test("bridge rejects welcome fields Go cannot unmarshal", async () => {
  const temp = await mkdtemp(join(tmpdir(), "pi-gateway-malformed-welcome-"));
  const server = createServer();
  const cases: Array<{ welcome: Record<string, unknown>; field: string }> = [
    { welcome: { type: "gw_welcome", protocol: "one", piVersion: "1.3.0" }, field: "protocol" },
    { welcome: { type: "gw_welcome", protocol: 1, session: { path: 7 }, piVersion: "1.3.0" }, field: "session.path" },
    { welcome: { type: "gw_welcome", protocol: 1, turn: { state: "idle", queued: "many" }, piVersion: "1.3.0" }, field: "turn.queued" },
  ];
  try {
    await loadOrCreateToken(tokenPath(temp));
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    assert.ok(address && typeof address === "object");
    let nextWelcome = 0;
    server.on("connection", (socket) => frameReader(socket, (record) => {
      if (record.type === "gw_hello") socket.write(encode([cases[nextWelcome++]!.welcome]));
    }));

    for (const item of cases) {
      const stderr = new PassThrough();
      const errors: Buffer[] = [];
      stderr.on("data", (chunk: Buffer) => errors.push(Buffer.from(chunk)));
      const code = await runBridge({
        args: ["--version", "--server", `127.0.0.1:${address.port}`],
        stdin: new PassThrough(),
        stdout: new PassThrough(),
        stderr,
        env: { [DIR_ENV]: temp },
      });
      assert.equal(code, 3);
      assert.match(Buffer.concat(errors).toString("utf8"), new RegExp(`malformed gw_welcome: field ${item.field.replace(".", "\\.")}`));
    }
  } finally {
    if (server.listening) await new Promise<void>((resolve) => server.close(() => resolve()));
    await rm(temp, { recursive: true, force: true });
  }
});

test("bridge version uses the managed Pi version from the daemon welcome", async () => {
  const temp = await mkdtemp(join(tmpdir(), "pi-gateway-version-"));
  const server = createServer();
  try {
    await loadOrCreateToken(tokenPath(temp));
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    assert.ok(address && typeof address === "object");
    server.on("connection", (socket) => frameReader(socket, (record) => {
      if (record.type === "gw_hello") socket.write(encode([{
        type: "gw_welcome", protocol: null, clientId: null, kind: null, granted: null, features: null,
        piVersion: "1.2.3", concurrency: null, session: null, headSeq: null, oldestSeq: null,
        resyncRequired: null, piState: null, turn: null, clients: null,
      }]));
    }));
    const output = new PassThrough();
    const stdout: Buffer[] = [];
    output.on("data", (chunk: Buffer) => stdout.push(Buffer.from(chunk)));
    const code = await runBridge({
      args: ["--version", "--server", `127.0.0.1:${address.port}`],
      stdout: output,
      stderr: new PassThrough(),
      stdin: new PassThrough(),
      env: { [DIR_ENV]: temp },
    });
    assert.equal(code, 0);
    assert.equal(Buffer.concat(stdout).toString("utf8"), "1.2.3\n");
  } finally {
    if (server.listening) await new Promise<void>((resolve) => server.close(() => resolve()));
    await rm(temp, { recursive: true, force: true });
  }
});
