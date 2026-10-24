import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { PiGatewayDaemon } from "../src/daemon.ts";
import { resolvePiSdkLocation } from "../src/pi-sdk.ts";
import { createPiSdkRuntime } from "../src/sdk-runtime.ts";
import { dialGateway, type WireFrame } from "./support/gateway.ts";

const piBin = process.env.PI_BIN ?? "pi";
let unavailableReason: string | undefined;
try { await resolvePiSdkLocation(piBin); }
catch (error) { unavailableReason = error instanceof Error ? error.message : String(error); }

test("a new daemon restores Pi session history and accepts another turn", { skip: unavailableReason }, async () => {
  let requests = 0;
  const server = createServer(async (request, response) => {
    for await (const _chunk of request) { /* consume request body */ }
    requests++;
    response.writeHead(200, {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      connection: "keep-alive",
    });
    response.write(`data: ${JSON.stringify({
      id: `chatcmpl-restart-${requests}`,
      object: "chat.completion.chunk",
      created: 1,
      model: "echo",
      choices: [{ index: 0, delta: { role: "assistant", content: "local restart reply" }, finish_reason: null }],
    })}\n\n`);
    response.write(`data: ${JSON.stringify({
      id: `chatcmpl-restart-${requests}`,
      object: "chat.completion.chunk",
      created: 1,
      model: "echo",
      choices: [{ index: 0, delta: {}, finish_reason: "stop" }],
    })}\n\n`);
    response.end("data: [DONE]\n\n");
  });

  const root = await mkdtemp(join(tmpdir(), "pi-gateway-daemon-restart-"));
  const agentDir = join(root, "agent");
  const sessionDir = join(root, "sessions");
  const piArgs = ["--model", "gateway-restart/echo", "--thinking", "off", "--no-tools", "--no-extensions",
    "--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files", "--session-dir", sessionDir];
  let first: PiGatewayDaemon | undefined;
  let second: PiGatewayDaemon | undefined;
  let firstPeer: Awaited<ReturnType<typeof dialGateway>> | undefined;
  let secondPeer: Awaited<ReturnType<typeof dialGateway>> | undefined;
  try {
    await mkdir(agentDir);
    await mkdir(sessionDir);
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const provider = server.address() as AddressInfo;
    await writeFile(join(agentDir, "models.json"), JSON.stringify({
      providers: {
        "gateway-restart": {
          baseUrl: `http://127.0.0.1:${provider.port}/v1`,
          api: "openai-completions",
          apiKey: "local-restart-key",
          models: [{ id: "echo", name: "Local restart test", contextWindow: 4096, maxTokens: 128 }],
        },
      },
    }));
    await writeFile(join(agentDir, "settings.json"), JSON.stringify({ retry: { enabled: false } }));

    const createRuntime = (options: Parameters<typeof createPiSdkRuntime>[0]) =>
      createPiSdkRuntime({ ...options, agentDir });
    const newDaemon = () => new PiGatewayDaemon({ token: "restart-token", addr: "127.0.0.1:0", piBin,
      catalogRoots: [sessionDir], createRuntime });

    first = newDaemon();
    await first.listen();
    firstPeer = await dialGateway(first.addr!.port, "restart-token", { cwd: root, piArgs });
    firstPeer.send({ type: "get_state", id: "create-session" });
    const initialState = await firstPeer.receive((frame) => frame.type === "response" && frame.id === "create-session");
    assert.equal(initialState.success, true);
    const sessionPath = String((initialState.data as WireFrame).sessionFile);
    assert.ok(sessionPath.startsWith(sessionDir));
    firstPeer.send({ type: "prompt", id: "first-turn", message: "persisted-before-restart" });
    assert.equal((await firstPeer.receive((frame) => frame.type === "response" && frame.id === "first-turn")).success, true);
    await firstPeer.receive((frame) => frame.type === "agent_settled");
    await first.shutdown();
    first = undefined;
    firstPeer = undefined;

    second = newDaemon();
    await second.listen();
    secondPeer = await dialGateway(second.addr!.port, "restart-token", { cwd: root, session: sessionPath, piArgs });
    secondPeer.send({ type: "get_messages", id: "restored-history" });
    const history = await secondPeer.receive((frame) => frame.type === "response" && frame.id === "restored-history");
    assert.equal(history.success, true);
    assert.match(JSON.stringify((history.data as WireFrame).messages), /persisted-before-restart/);
    assert.match(JSON.stringify((history.data as WireFrame).messages), /local restart reply/);

    secondPeer.send({ type: "prompt", id: "second-turn", message: "continue-after-restart" });
    assert.equal((await secondPeer.receive((frame) => frame.type === "response" && frame.id === "second-turn")).success, true);
    await secondPeer.receive((frame) => frame.type === "agent_settled");
    assert.equal(requests, 2);
  } finally {
    firstPeer?.close();
    secondPeer?.close();
    await first?.shutdown();
    await second?.shutdown();
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    await rm(root, { recursive: true, force: true });
  }
});
