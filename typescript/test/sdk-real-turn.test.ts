import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { resolvePiSdkLocation } from "../src/pi-sdk.ts";
import { createPiSdkRuntime } from "../src/sdk-runtime.ts";
import { parsePiArgs } from "../src/piargs.ts";
import type { RpcObject, RpcSessionRuntime } from "../src/rpc-adapter.ts";

const piBin = process.env.PI_BIN ?? "pi";
let unavailableReason: string | undefined;
try {
  await resolvePiSdkLocation(piBin);
} catch (error) {
  unavailableReason = error instanceof Error ? error.message : String(error);
}

test("real Pi SDK completes a local model turn, settles provider errors, and recovers", {
  skip: unavailableReason,
}, async () => {
  const statuses = [200, 401, 200];
  const requests: Array<{ path: string; authorization: string | undefined; body: RpcObject }> = [];
  const server = createServer(async (request, response) => {
    const chunks: Buffer[] = [];
    for await (const chunk of request) chunks.push(Buffer.from(chunk));
    const body = JSON.parse(Buffer.concat(chunks).toString("utf8")) as RpcObject;
    requests.push({ path: request.url ?? "", authorization: request.headers.authorization, body });

    const status = statuses.shift() ?? 500;
    if (status !== 200) {
      response.writeHead(status, { "content-type": "application/json" });
      response.end(JSON.stringify({ error: { message: "local provider rejected the request" } }));
      return;
    }

    response.writeHead(200, {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      connection: "keep-alive",
    });
    response.write(`data: ${JSON.stringify({
      id: "chatcmpl-local",
      object: "chat.completion.chunk",
      created: 1,
      model: "echo",
      choices: [{ index: 0, delta: { role: "assistant", content: "local SDK reply" }, finish_reason: null }],
    })}\n\n`);
    response.write(`data: ${JSON.stringify({
      id: "chatcmpl-local",
      object: "chat.completion.chunk",
      created: 1,
      model: "echo",
      choices: [{ index: 0, delta: {}, finish_reason: "stop" }],
    })}\n\n`);
    response.end("data: [DONE]\n\n");
  });

  const packageRoot = (await resolvePiSdkLocation(piBin)).packageRoot;
  const originalPackageRoot = process.env.PI_SUBAGENTS_PI_CODING_AGENT_PACKAGE_ROOT;
  const root = await mkdtemp(join(tmpdir(), "pi-gateway-sdk-turn-"));
  const agentDir = join(root, "agent");
  const extensionPath = join(root, "capture-pi-root.ts");
  const capturedRootPath = join(root, "extension-pi-root.txt");
  let runtime: RpcSessionRuntime | undefined;
  try {
    await mkdir(agentDir);
    await writeFile(extensionPath, `
import { writeFileSync } from "node:fs";
writeFileSync(${JSON.stringify(capturedRootPath)}, process.env.PI_SUBAGENTS_PI_CODING_AGENT_PACKAGE_ROOT ?? "");
export default function () {}
`);
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const address = server.address() as AddressInfo;
    await writeFile(join(agentDir, "models.json"), JSON.stringify({
      providers: {
        "gateway-smoke": {
          baseUrl: `http://127.0.0.1:${address.port}/v1`,
          api: "openai-completions",
          apiKey: "local-test-key",
          models: [{ id: "echo", name: "Local SDK test", contextWindow: 4096, maxTokens: 128 }],
        },
      },
    }));
    await writeFile(join(agentDir, "settings.json"), JSON.stringify({ retry: { enabled: false } }));

    runtime = await createPiSdkRuntime({
      piBin,
      env: { ...process.env, PI_SUBAGENTS_PI_CODING_AGENT_PACKAGE_ROOT: "" },
      cwd: root,
      agentDir,
      piArgs: parsePiArgs([
        "--no-session", "--extension", extensionPath, "--approve",
        "--model", "gateway-smoke/echo", "--thinking", "off", "--no-tools",
        "--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files",
      ]),
    });
    assert.equal(await readFile(capturedRootPath, "utf8"), packageRoot, "extensions see the selected Pi SDK root during startup");
    assert.equal(process.env.PI_SUBAGENTS_PI_CODING_AGENT_PACKAGE_ROOT, originalPackageRoot, "startup restores the process environment");
    const settled: RpcObject[] = [];
    runtime.session.subscribe((event) => {
      if (event.type === "agent_settled") settled.push(event);
    });

    await runtime.session.prompt("Return a short reply.");
    await runtime.session.waitForIdle();
    assert.equal(runtime.session.getLastAssistantText(), "local SDK reply");
    assert.equal(settled.length, 1, "successful SDK turn must settle");

    await runtime.session.prompt("This request should be rejected.");
    await runtime.session.waitForIdle();
    const failedMessage = runtime.session.messages.filter((message) => message.role === "assistant").at(-1);
    assert.equal(failedMessage?.stopReason, "error");
    assert.match(String(failedMessage?.errorMessage), /local provider rejected the request/i);
    assert.equal(settled.length, 2, "provider error must settle instead of hanging the turn");

    await runtime.session.prompt("Try again after the provider recovers.");
    await runtime.session.waitForIdle();
    assert.equal(runtime.session.getLastAssistantText(), "local SDK reply");
    assert.equal(settled.length, 3, "a settled provider error must not poison the next turn");

    assert.equal(requests.length, 3);
    for (const request of requests) {
      assert.equal(request.path, "/v1/chat/completions");
      assert.equal(request.authorization, "Bearer local-test-key");
      assert.equal(request.body.model, "echo");
      assert.equal(request.body.stream, true);
    }
  } finally {
    await runtime?.dispose();
    await new Promise<void>((resolve, reject) => {
      server.close((error) => error ? reject(error) : resolve());
    });
    await rm(root, { recursive: true, force: true });
  }
});
