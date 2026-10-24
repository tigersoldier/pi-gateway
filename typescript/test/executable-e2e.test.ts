import assert from "node:assert/strict";
import { spawn, spawnSync, type ChildProcess, type ChildProcessWithoutNullStreams } from "node:child_process";
import { once } from "node:events";
import { createServer } from "node:http";
import { createServer as createTcpServer, type AddressInfo } from "node:net";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "node:test";
import { portPath } from "../src/config.ts";
import { resolvePiSdkLocation } from "../src/pi-sdk.ts";

const daemonEntry = fileURLToPath(new URL("../bin/pi-gatewayd.ts", import.meta.url));
const bridgeEntry = fileURLToPath(new URL("../bin/pi-gateway.ts", import.meta.url));
const piBin = "pi";
let unavailableReason: string | undefined;
try { await resolvePiSdkLocation(piBin); }
catch (error) { unavailableReason = error instanceof Error ? error.message : String(error); }

class JsonlChild {
  readonly child: ChildProcessWithoutNullStreams;
  readonly closed: Promise<void>;
  readonly stderr: string[] = [];
  private pending = "";
  private readonly frames: Record<string, unknown>[] = [];
  private readonly waiters: Array<{
    predicate: (frame: Record<string, unknown>) => boolean;
    resolve(frame: Record<string, unknown>): void;
    reject(error: Error): void;
    timer: NodeJS.Timeout;
  }> = [];
  private exitError?: Error;

  constructor(child: ChildProcessWithoutNullStreams) {
    this.child = child;
    this.closed = once(child, "close").then(() => {});
    child.stdout.setEncoding("utf8");
    child.stdout.on("data", (chunk: string) => this.onData(chunk));
    child.stderr.setEncoding("utf8");
    child.stderr.on("data", (chunk: string) => this.stderr.push(chunk));
    child.once("exit", (code, signal) => {
      this.exitError = new Error(`bridge exited (${code ?? signal}): ${this.stderr.join("")}`);
      for (const waiter of this.waiters.splice(0)) {
        clearTimeout(waiter.timer);
        waiter.reject(this.exitError);
      }
    });
  }

  write(frame: Record<string, unknown>): void {
    this.child.stdin.write(`${JSON.stringify(frame)}\n`);
  }

  receive(predicate: (frame: Record<string, unknown>) => boolean, timeoutMs = 15_000): Promise<Record<string, unknown>> {
    const index = this.frames.findIndex(predicate);
    if (index >= 0) return Promise.resolve(this.frames.splice(index, 1)[0]!);
    if (this.exitError) return Promise.reject(this.exitError);
    return new Promise((resolve, reject) => {
      const waiter = {
        predicate,
        resolve,
        reject,
        timer: setTimeout(() => {
          const index = this.waiters.indexOf(waiter);
          if (index >= 0) this.waiters.splice(index, 1);
          reject(new Error(`timed out waiting for bridge output; stderr: ${this.stderr.join("")}`));
        }, timeoutMs),
      };
      this.waiters.push(waiter);
    });
  }

  async collectUntil(predicate: (frame: Record<string, unknown>) => boolean): Promise<Record<string, unknown>[]> {
    const frames: Record<string, unknown>[] = [];
    for (;;) {
      const frame = await this.receive(() => true);
      frames.push(frame);
      if (predicate(frame)) return frames;
    }
  }

  private onData(chunk: string): void {
    this.pending += chunk;
    for (;;) {
      const index = this.pending.indexOf("\n");
      if (index < 0) return;
      const line = this.pending.slice(0, index).replace(/\r$/, "");
      this.pending = this.pending.slice(index + 1);
      let frame: Record<string, unknown>;
      try { frame = JSON.parse(line) as Record<string, unknown>; }
      catch { continue; }
      const waiterIndex = this.waiters.findIndex((waiter) => waiter.predicate(frame));
      if (waiterIndex >= 0) {
        const waiter = this.waiters.splice(waiterIndex, 1)[0]!;
        clearTimeout(waiter.timer);
        waiter.resolve(frame);
      } else this.frames.push(frame);
    }
  }
}

function startBridge(server: string, tokenFile: string, cwd: string, extraArgs: string[] = []): JsonlChild {
  const child = spawn(process.execPath, ["--experimental-strip-types", bridgeEntry,
    "--mode", "rpc", "--server", server, "--token-file", tokenFile, ...extraArgs], {
    cwd,
    env: process.env,
    stdio: ["pipe", "pipe", "pipe"],
  });
  return new JsonlChild(child as ChildProcessWithoutNullStreams);
}

async function waitForExit(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  await once(child, "exit");
}

async function stopChild(child: ChildProcess | undefined): Promise<void> {
  if (!child || child.exitCode !== null || child.signalCode !== null) return;
  const exited = once(child, "exit");
  child.kill("SIGTERM");
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<"timeout">((resolve) => { timer = setTimeout(() => resolve("timeout"), 5000); });
  const result = await Promise.race([exited.then(() => "exit" as const), timeout]);
  if (timer) clearTimeout(timer);
  if (result === "timeout") {
    child.kill("SIGKILL");
    await exited;
  }
}

async function waitForPort(path: string, child: ChildProcess, stderr: string[], timeoutMs = 10_000): Promise<number> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (child.exitCode !== null || child.signalCode !== null) {
      assert.fail(`daemon exited before publishing its port: ${stderr.join("")}`);
    }
    try {
      const port = Number((await readFile(path, "utf8")).trim());
      if (Number.isInteger(port) && port > 0) return port;
    } catch { /* daemon is still starting */ }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  assert.fail(`daemon did not publish its port: ${stderr.join("")}`);
}

function assistantText(frame: Record<string, unknown>): string {
  const message = frame.message;
  if (!message || typeof message !== "object") return "";
  const content = (message as Record<string, unknown>).content;
  if (!Array.isArray(content)) return "";
  return content.map((item) => item && typeof item === "object"
    ? String((item as Record<string, unknown>).text ?? "") : "").join("");
}

async function localCompletion(response: import("node:http").ServerResponse): Promise<void> {
  response.writeHead(200, {
    "content-type": "text/event-stream",
    "cache-control": "no-cache",
    connection: "keep-alive",
  });
  response.write(`data: ${JSON.stringify({
    id: "chatcmpl-executable-e2e",
    object: "chat.completion.chunk",
    created: 1,
    model: "echo",
    choices: [{ index: 0, delta: { role: "assistant", content: "local SDK reply" }, finish_reason: null }],
  })}\n\n`);
  response.write(`data: ${JSON.stringify({
    id: "chatcmpl-executable-e2e",
    object: "chat.completion.chunk",
    created: 1,
    model: "echo",
    choices: [{ index: 0, delta: {}, finish_reason: "stop" }],
  })}\n\n`);
  response.end("data: [DONE]\n\n");
}

test("TypeScript daemon and bridge executables run a local-provider success/error/recovery matrix", {
  skip: unavailableReason,
}, async () => {
  const statuses = [200, 401, 200, 200];
  const requests: Array<{ path: string; authorization: string | undefined; body: Record<string, unknown> }> = [];
  const provider = createServer(async (request, response) => {
    const chunks: Buffer[] = [];
    for await (const chunk of request) chunks.push(Buffer.from(chunk));
    let body: Record<string, unknown>;
    try { body = JSON.parse(Buffer.concat(chunks).toString("utf8")) as Record<string, unknown>; }
    catch { response.writeHead(400).end(); return; }
    requests.push({ path: request.url ?? "", authorization: request.headers.authorization, body });
    const status = statuses.shift() ?? 500;
    if (status !== 200) {
      response.writeHead(status, { "content-type": "application/json" });
      response.end(JSON.stringify({ error: { message: "local provider rejected the request" } }));
      return;
    }
    await localCompletion(response);
  });

  const root = await mkdtemp(join(tmpdir(), "pi-gateway-ts-executable-e2e-"));
  const agentDir = join(root, "agent");
  const sessionDir = join(root, "sessions");
  const stateDir = join(root, "state");
  const workDir = join(root, "work");
  const tokenFile = join(stateDir, "token");
  let daemon: ChildProcess | undefined;
  let bridge: JsonlChild | undefined;
  let secondBridge: JsonlChild | undefined;
  try {
    await Promise.all([mkdir(agentDir, { recursive: true }), mkdir(sessionDir), mkdir(stateDir), mkdir(workDir)]);
    await writeFile(tokenFile, "typescript-e2e-token\n", { mode: 0o600 });
    provider.listen(0, "127.0.0.1");
    await once(provider, "listening");
    const providerAddress = provider.address() as AddressInfo;
    await writeFile(join(agentDir, "models.json"), JSON.stringify({
      providers: {
        "gateway-e2e": {
          baseUrl: `http://127.0.0.1:${providerAddress.port}/v1`,
          api: "openai-completions",
          apiKey: "local-test-key",
          models: [{ id: "echo", name: "Local executable test", contextWindow: 4096, maxTokens: 128 }],
        },
      },
    }));
    await writeFile(join(agentDir, "settings.json"), JSON.stringify({ retry: { enabled: false } }));

    const childEnv = {
      ...process.env,
      PI_CODING_AGENT_DIR: agentDir,
      PI_CODING_AGENT_SESSION_DIR: sessionDir,
    };
    let daemonStderr: string[] = [];
    const launchDaemon = () => {
      daemonStderr = [];
      const child = spawn(process.execPath, ["--experimental-strip-types", daemonEntry,
        "--listen", "127.0.0.1:0", "--state-dir", stateDir, "--token-file", tokenFile, "--no-debug"], {
        cwd: workDir,
        env: childEnv,
        stdio: ["ignore", "ignore", "pipe"],
      });
      child.stderr?.setEncoding("utf8");
      child.stderr?.on("data", (chunk: string) => daemonStderr.push(chunk));
      daemon = child;
      return child;
    };
    daemon = launchDaemon();
    let port = await waitForPort(portPath(stateDir), daemon, daemonStderr);
    let address = `127.0.0.1:${port}`;

    const version = spawnSync(process.execPath, ["--experimental-strip-types", bridgeEntry,
      "--version", "--server", address, "--token-file", tokenFile], {
      cwd: workDir, env: childEnv, encoding: "utf8", timeout: 10_000,
    });
    assert.equal(version.status, 0, version.stderr);
    assert.match(version.stdout.trim(), /^\d+\.\d+\.\d+/);

    const badTokenFile = join(root, "bad-token");
    await writeFile(badTokenFile, "wrong-token\n", { mode: 0o600 });
    const unauthorized = spawnSync(process.execPath, ["--experimental-strip-types", bridgeEntry,
      "--version", "--server", address, "--token-file", badTokenFile], {
      cwd: workDir, env: childEnv, encoding: "utf8", timeout: 10_000,
    });
    assert.equal(unauthorized.status, 3);
    assert.match(unauthorized.stderr, /daemon rejected the token/);

    bridge = startBridge(address, tokenFile, workDir, [
      "--model", "gateway-e2e/echo", "--thinking", "off", "--no-tools", "--no-extensions",
      "--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files",
    ]);
    bridge.write({ type: "get_state", id: "state-before" });
    const initial = await bridge.receive((frame) => frame.type === "response" && frame.id === "state-before");
    assert.equal(initial.success, true);

    for (const [id, prompt, expectedText] of [
      ["turn-ok", "first local turn", "local SDK reply"],
      ["turn-error", "provider error turn", ""],
      ["turn-recovery", "turn after provider error", "local SDK reply"],
    ] as const) {
      bridge.write({ type: "prompt", id, message: prompt });
      const accepted = await bridge.receive((frame) => frame.type === "response" && frame.id === id);
      assert.equal(accepted.success, true, JSON.stringify(accepted));
      const turnFrames = await bridge.collectUntil((frame) => frame.type === "agent_settled");
      if (expectedText) {
        assert.ok(turnFrames.some((frame) => frame.type === "message_end" && assistantText(frame) === expectedText),
          `missing assistant result for ${id}`);
        assert.ok(turnFrames.some((frame) => frame.type === "message_update"), `missing streamed delta for ${id}`);
      }
    }

    bridge.write({ type: "get_state", id: "state-after" });
    const finalState = await bridge.receive((frame) => frame.type === "response" && frame.id === "state-after");
    assert.equal(finalState.success, true);
    const state = finalState.data as Record<string, unknown>;
    assert.equal(state.isStreaming, false);
    const transcriptCount = Number(state.messageCount);
    assert.ok(transcriptCount >= 6, `expected all three prompts in transcript, got ${state.messageCount}`);
    const sessionPath = String(state.sessionFile ?? "");
    assert.ok(sessionPath, "persistent Pi session file must be reported by get_state");

    assert.equal(requests.length, 3);
    for (const request of requests) {
      assert.equal(request.path, "/v1/chat/completions");
      assert.equal(request.authorization, "Bearer local-test-key");
      assert.equal(request.body.model, "echo");
      assert.equal(request.body.stream, true);
    }

    bridge.child.stdin.end();
    await waitForExit(bridge.child);
    bridge = undefined;
    await stopChild(daemon);
    daemon = undefined;
    await rm(portPath(stateDir), { force: true });

    daemon = launchDaemon();
    port = await waitForPort(portPath(stateDir), daemon, daemonStderr);
    address = `127.0.0.1:${port}`;
    bridge = startBridge(address, tokenFile, workDir, [
      "--model", "gateway-e2e/echo", "--thinking", "off", "--no-tools", "--no-extensions",
      "--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files",
    ]);
    bridge.write({ type: "switch_session", id: "reattach", sessionPath });
    const reattached = await bridge.receive((frame) => frame.type === "response" && frame.id === "reattach");
    assert.equal(reattached.success, true, JSON.stringify(reattached));
    bridge.write({ type: "get_state", id: "state-restored" });
    const restored = await bridge.receive((frame) => frame.type === "response" && frame.id === "state-restored");
    assert.equal(restored.success, true);
    assert.ok(Number((restored.data as Record<string, unknown>).messageCount) >= transcriptCount,
      "daemon restart must preserve Pi transcript history");

    secondBridge = startBridge(address, tokenFile, workDir, [
      "--model", "gateway-e2e/echo", "--thinking", "off", "--no-tools", "--no-extensions",
      "--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files",
    ]);
    secondBridge.write({ type: "switch_session", id: "share", sessionPath });
    const shared = await secondBridge.receive((frame) => frame.type === "response" && frame.id === "share");
    assert.equal(shared.success, true, JSON.stringify(shared));
    secondBridge.write({ type: "prompt", id: "shared-turn", message: "turn after restart" });
    const accepted = await secondBridge.receive((frame) => frame.type === "response" && frame.id === "shared-turn");
    assert.equal(accepted.success, true, JSON.stringify(accepted));
    const [firstClientFrames, secondClientFrames] = await Promise.all([
      bridge.collectUntil((frame) => frame.type === "agent_settled"),
      secondBridge.collectUntil((frame) => frame.type === "agent_settled"),
    ]);
    for (const [name, frames] of [["first", firstClientFrames], ["second", secondClientFrames]] as const) {
      assert.ok(frames.some((frame) => frame.type === "message_end" && assistantText(frame) === "local SDK reply"),
        `${name} client did not receive the shared turn`);
    }
    assert.equal(requests.length, 4);

    secondBridge.child.stdin.end();
    await waitForExit(secondBridge.child);
    secondBridge = undefined;
    bridge.child.stdin.end();
    await waitForExit(bridge.child);
    bridge = undefined;
  } finally {
    if (secondBridge) await stopChild(secondBridge.child);
    if (bridge) await stopChild(bridge.child);
    await stopChild(daemon);
    if (provider.listening) await new Promise<void>((resolve, reject) => {
      provider.close((error) => error ? reject(error) : resolve());
      provider.closeAllConnections();
    });
    await rm(root, { recursive: true, force: true });
  }
});

test("bridge executable exits after daemon loss while stdin remains open", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-gateway-ts-bridge-disconnect-"));
  const tokenFile = join(root, "token");
  const server = createTcpServer();
  let resolveHello!: () => void;
  let resolveUiCommand!: () => void;
  const helloSeen = new Promise<void>((resolve) => { resolveHello = resolve; });
  const uiCommandSeen = new Promise<void>((resolve) => { resolveUiCommand = resolve; });
  let bridge: JsonlChild | undefined;
  try {
    await writeFile(tokenFile, "bridge-disconnect-token\n", { mode: 0o600 });
    server.on("connection", (socket) => {
      let pending = "";
      socket.on("data", (chunk: Buffer) => {
        pending += chunk.toString("utf8");
        for (;;) {
          const newline = pending.indexOf("\n");
          if (newline < 0) return;
          const line = pending.slice(0, newline).replace(/\r$/, "");
          pending = pending.slice(newline + 1);
          const frame = JSON.parse(line) as Record<string, unknown>;
          if (frame.type === "gw_hello") {
            socket.write(`${JSON.stringify({ type: "gw_welcome", protocol: 1, clientId: "c_test" })}\n`);
            resolveHello();
          } else if (frame.type === "get_state") {
            resolveUiCommand();
            socket.destroy();
          }
        }
      });
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    assert.ok(address && typeof address === "object");
    bridge = startBridge(`127.0.0.1:${address.port}`, tokenFile, root);

    async function within<T>(promise: Promise<T>, message: string, timeoutMs = 5000): Promise<T> {
      let timer: NodeJS.Timeout | undefined;
      try {
        return await Promise.race([
          promise,
          new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error(message)), timeoutMs); }),
        ]);
      } finally { if (timer) clearTimeout(timer); }
    }

    await within(helloSeen, "bridge did not send hello");
    assert.equal(bridge.child.stdin.writableEnded, false, "leave the bridge UI stdin pipe open");
    bridge.write({ type: "get_state", id: "disconnect-probe" });
    await within(uiCommandSeen, "bridge did not relay the UI command");
    const exit = await within(once(bridge.child, "exit").then(([code, signal]) => ({ code, signal })),
      "bridge remained alive after daemon disconnect with open stdin", 3000);
    assert.equal(exit.code, 3);
    await within(bridge.closed, "bridge stdio did not close after exit", 3000);
    assert.match(bridge.stderr.join(""), /daemon connection lost/);
  } finally {
    if (bridge) await stopChild(bridge.child);
    if (server.listening) await new Promise<void>((resolve, reject) => {
      server.close((error) => error ? reject(error) : resolve());
    });
    await rm(root, { recursive: true, force: true });
  }
});
