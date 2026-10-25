import { createServer, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { writeFile } from "node:fs/promises";

const portFile = process.argv[2];
if (!portFile) throw new Error("usage: local-openai-provider.ts <port-file>");

interface ChatMessage {
  role?: unknown;
  content?: unknown;
}

function messageText(value: unknown): string {
  if (typeof value === "string") return value;
  if (!Array.isArray(value)) return "";
  return value.map((part) => part && typeof part === "object"
    ? String((part as Record<string, unknown>).text ?? "") : "").join("");
}

function messagesOf(body: Record<string, unknown>): ChatMessage[] {
  return Array.isArray(body.messages)
    ? body.messages.filter((message): message is ChatMessage => message !== null && typeof message === "object")
    : [];
}

function latestUserText(messages: ChatMessage[]): string {
  for (let i = messages.length - 1; i >= 0; i--) {
    if (messages[i]?.role === "user") return messageText(messages[i]?.content);
  }
  return "";
}

function writeChunk(response: ServerResponse, delta: Record<string, unknown>, finishReason: string | null = null): boolean {
  if (response.destroyed || response.writableEnded) return false;
  response.write(`data: ${JSON.stringify({
    id: "chatcmpl-local-e2e",
    object: "chat.completion.chunk",
    created: 1,
    model: "echo",
    choices: [{ index: 0, delta, finish_reason: finishReason }],
  })}\n\n`);
  return true;
}

async function finish(response: ServerResponse): Promise<void> {
  if (writeChunk(response, {}, "stop")) response.end("data: [DONE]\n\n");
}

function startStream(response: ServerResponse): void {
  response.writeHead(200, {
    "content-type": "text/event-stream",
    "cache-control": "no-cache",
    connection: "keep-alive",
  });
}

async function streamText(response: ServerResponse, content: string, delayMs = 40): Promise<void> {
  startStream(response);
  const chunks = content.match(/.{1,8}/gs) ?? [content];
  for (const [index, chunk] of chunks.entries()) {
    if (!writeChunk(response, { ...(index === 0 ? { role: "assistant" } : {}), content: chunk })) return;
    await new Promise((resolve) => setTimeout(resolve, delayMs));
  }
  await finish(response);
}

async function holdStreamUntilCancelled(response: ServerResponse, initialText: string, holdMs = 60_000): Promise<void> {
  startStream(response);
  if (!writeChunk(response, { role: "assistant", content: initialText })) return;
  // The default 60s cap exceeds the ERT's 15s first-delta wait plus its 10s
  // shared abort-and-settle deadline. A bounded loop also cleans up if a
  // failed client never cancels the request.
  for (let index = 0; index < Math.ceil(holdMs / 50); index++) {
    await new Promise((resolve) => setTimeout(resolve, 50));
    if (!writeChunk(response, { content: index % 2 === 0 ? " …" : " working" })) return;
  }
  await finish(response);
}

async function streamToolCall(response: ServerResponse, path: string): Promise<void> {
  startStream(response);
  writeChunk(response, {
    role: "assistant",
    tool_calls: [{
      index: 0,
      id: "call-local-e2e-read",
      type: "function",
      function: { name: "read", arguments: JSON.stringify({ path }) },
    }],
  });
  await new Promise((resolve) => setTimeout(resolve, 20));
  if (writeChunk(response, {}, "tool_calls")) response.end("data: [DONE]\n\n");
}

const server = createServer(async (request, response) => {
  if (request.method !== "POST" || request.url !== "/v1/chat/completions") {
    response.writeHead(404).end();
    return;
  }
  const chunks: Buffer[] = [];
  for await (const chunk of request) chunks.push(Buffer.from(chunk));
  if (request.headers.authorization !== "Bearer local-test-key") {
    response.writeHead(401, { "content-type": "application/json" });
    response.end(JSON.stringify({ error: { message: "invalid local test key" } }));
    return;
  }

  let body: Record<string, unknown>;
  try {
    body = JSON.parse(Buffer.concat(chunks).toString("utf8")) as Record<string, unknown>;
    if (body.model !== "echo" || body.stream !== true) throw new Error("unexpected model request");
  } catch (error) {
    response.writeHead(400, { "content-type": "application/json" });
    response.end(JSON.stringify({ error: { message: error instanceof Error ? error.message : "invalid JSON body" } }));
    return;
  }

  const messages = messagesOf(body);
  const prompt = latestUserText(messages);
  const hasToolResult = messages.some((message) => message.role === "tool");
  if (/read tool to read the file/i.test(prompt) && !hasToolResult) {
    const path = prompt.match(/read the file\s+(.+)\.\s*$/i)?.[1];
    const tools = Array.isArray(body.tools) ? body.tools : [];
    const hasReadTool = tools.some((tool) => tool && typeof tool === "object" &&
      (tool as Record<string, unknown>).function &&
      typeof (tool as Record<string, unknown>).function === "object" &&
      ((tool as Record<string, unknown>).function as Record<string, unknown>).name === "read");
    if (!path || !hasReadTool) {
      response.writeHead(400, { "content-type": "application/json" });
      response.end(JSON.stringify({ error: { message: "read-tool contract requested without the Pi read tool" } }));
      return;
    }
    await streamToolCall(response, path);
    return;
  }

  if (/Count from 1 to 100 slowly|typescript-local-contract-abort/i.test(prompt)) {
    await holdStreamUntilCancelled(response, "1");
    return;
  }
  if (/typescript-local-contract-steer-initial/i.test(prompt)) {
    // Leave a clear window to issue steer while streaming, then finish within
    // the ERT's 15s wait for the queued user message.
    await holdStreamUntilCancelled(response, "working", 10_000);
    return;
  }
  if (/queued-steer-test/i.test(prompt)) {
    await holdStreamUntilCancelled(response, "queued");
    return;
  }
  if (/Say:\s*working/i.test(prompt)) {
    await streamText(response, "working", 100);
    return;
  }
  if (/Say\s+OK/i.test(prompt)) {
    await streamText(response, "OK");
    return;
  }
  if (/Say:\s*test/i.test(prompt)) {
    await streamText(response, "test");
    return;
  }
  if (hasToolResult) {
    await streamText(response, "Read the requested file.");
    return;
  }
  await streamText(response, "local gateway e2e reply");
});

server.listen(0, "127.0.0.1", () => {
  const address = server.address() as AddressInfo;
  void writeFile(portFile, `${address.port}\n`, { mode: 0o600 }).catch((error: unknown) => {
    process.stderr.write(`local provider could not publish its port: ${String(error)}\n`);
    process.exitCode = 1;
    server.close();
  });
});

const stop = () => {
  server.close(() => { process.exitCode = 0; });
  server.closeAllConnections();
};
process.once("SIGINT", stop);
process.once("SIGTERM", stop);
