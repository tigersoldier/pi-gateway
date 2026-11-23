import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { resolvePiSdkLocation } from "../src/pi-sdk.ts";
import { createPiSdkRuntime } from "../src/sdk-runtime.ts";
import { parsePiArgs } from "../src/piargs.ts";
import { SdkRpcAdapter, type RpcObject, type RpcSessionRuntime } from "../src/rpc-adapter.ts";

const piBin = process.env.PI_BIN ?? "pi";
let unavailableReason: string | undefined;
try {
  await resolvePiSdkLocation(piBin);
} catch (error) {
  unavailableReason = error instanceof Error ? error.message : String(error);
}

/** Minimal stdio MCP server: one `echo` tool, newline-delimited JSON-RPC. */
const MCP_PROBE_SERVER = `
import { createInterface } from "node:readline";
const rl = createInterface({ input: process.stdin });
const send = (message) => process.stdout.write(JSON.stringify(message) + "\\n");
rl.on("line", (line) => {
  if (!line.trim()) return;
  let message;
  try { message = JSON.parse(line); } catch { return; }
  if (message.method === "initialize") {
    send({ jsonrpc: "2.0", id: message.id, result: {
      protocolVersion: message.params?.protocolVersion ?? "2024-11-05",
      capabilities: { tools: {} },
      serverInfo: { name: "probe", version: "1.0.0" },
    } });
    return;
  }
  if (message.id === undefined) return;
  if (message.method === "tools/list") {
    send({ jsonrpc: "2.0", id: message.id, result: { tools: [{
      name: "echo",
      description: "Echo text",
      inputSchema: { type: "object", properties: { text: { type: "string" } }, required: ["text"] },
    }] } });
    return;
  }
  send({ jsonrpc: "2.0", id: message.id, error: { code: -32601, message: "method not found: " + message.method } });
});
`;

interface TurnResult {
  requests: RpcObject[];
  toolNames: string[];
}

/**
 * Run one turn through the gateway's SDK adapter with a temporary agent
 * directory. `piArgs` selects the built-in extensions under test. The MCP
 * extension reads its config through Pi's process-wide getAgentDir(), so the
 * runtime exports its explicit agentDir there for the turn; the test restores
 * the previous value afterwards.
 */
async function runTurn(piArgs: string[]): Promise<TurnResult> {
  const root = await mkdtemp(join(tmpdir(), "pi-gateway-builtins-"));
  const agentDir = join(root, "agent");
  const mcpServerPath = join(root, "mcp-probe-server.mjs");
  const requests: RpcObject[] = [];
  let runtime: RpcSessionRuntime | undefined;
  let adapter: SdkRpcAdapter | undefined;
  const oldAgentDir = process.env.PI_CODING_AGENT_DIR;

  const provider = createServer(async (request, response) => {
    const chunks: Buffer[] = [];
    for await (const chunk of request) chunks.push(Buffer.from(chunk));
    requests.push(JSON.parse(Buffer.concat(chunks).toString("utf8")) as RpcObject);
    response.writeHead(200, {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      connection: "keep-alive",
    });
    response.write(`data: ${JSON.stringify({
      id: "chatcmpl-local-builtins",
      object: "chat.completion.chunk",
      created: 1,
      model: "echo",
      choices: [{ index: 0, delta: { role: "assistant", content: "local reply" }, finish_reason: null }],
    })}\n\n`);
    response.write(`data: ${JSON.stringify({
      id: "chatcmpl-local-builtins",
      object: "chat.completion.chunk",
      created: 1,
      model: "echo",
      choices: [{ index: 0, delta: {}, finish_reason: "stop" }],
    })}\n\n`);
    response.end("data: [DONE]\n\n");
  });

  try {
    await mkdir(agentDir);
    await writeFile(mcpServerPath, MCP_PROBE_SERVER);
    await writeFile(join(agentDir, "mcp.json"), JSON.stringify({
      mcpServers: { probe: { command: process.execPath, args: [mcpServerPath], exposure: "direct" } },
    }));
    await writeFile(join(agentDir, "settings.json"), JSON.stringify({ retry: { enabled: false } }));

    provider.listen(0, "127.0.0.1");
    await once(provider, "listening");
    const address = provider.address() as AddressInfo;
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

    runtime = await createPiSdkRuntime({
      piBin,
      cwd: root,
      agentDir,
      env: { ...process.env },
      piArgs: parsePiArgs([
        "--no-session", "--approve", "--model", "gateway-smoke/echo", "--thinking", "off",
        "--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files",
        ...piArgs,
      ]),
    });
    adapter = new SdkRpcAdapter(runtime, () => {});
    await adapter.bind();
    assert.equal(
      process.env.PI_CODING_AGENT_DIR,
      agentDir,
      "an explicit agent directory is visible to extensions through getAgentDir()",
    );
    await runtime.session.prompt("Return a short reply.");
    await runtime.session.waitForIdle();
  } finally {
    if (adapter) await adapter.dispose().catch(() => {});
    else await runtime?.dispose().catch(() => {});
    if (oldAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
    else process.env.PI_CODING_AGENT_DIR = oldAgentDir;
    await new Promise<void>((resolve) => {
      provider.close(() => resolve());
      provider.closeAllConnections();
    });
    await rm(root, { recursive: true, force: true });
  }

  const tools = Array.isArray(requests[0]?.tools) ? requests[0]!.tools as RpcObject[] : [];
  const toolNames = tools.map((tool) => {
    const definition = tool.function;
    return definition !== null && typeof definition === "object"
      ? String((definition as RpcObject).name ?? "") : "";
  });
  return { requests, toolNames };
}

test("gateway SDK sessions load the built-in MCP extension and declare mcp.json tools", {
  skip: unavailableReason,
}, async () => {
  const { requests, toolNames } = await runTurn([]);
  assert.ok(requests.length >= 1, "the turn reached the local provider");
  assert.ok(
    toolNames.includes("mcp__probe__echo"),
    `the model request must declare the MCP tool; got ${JSON.stringify(toolNames)}`,
  );
});

test("--no-mcp keeps the built-in MCP extension from loading in gateway sessions", {
  skip: unavailableReason,
}, async () => {
  const { requests, toolNames } = await runTurn(["--no-mcp"]);
  assert.ok(requests.length >= 1, "the turn reached the local provider");
  assert.deepEqual(toolNames.filter((name) => name.startsWith("mcp__")), []);
});

test("--no-extensions also disables built-in extensions in gateway sessions", {
  skip: unavailableReason,
}, async () => {
  const { requests, toolNames } = await runTurn(["--no-extensions"]);
  assert.ok(requests.length >= 1, "the turn reached the local provider");
  assert.deepEqual(toolNames.filter((name) => name.startsWith("mcp__")), []);
});
