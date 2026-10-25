import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PassThrough } from "node:stream";
import { createServer, type Socket } from "node:net";
import { test } from "node:test";
import { DIR_ENV, loadOrCreateToken, tokenPath } from "../src/config.ts";
import { runBridge } from "../src/bridge.ts";

function sendFrames(socket: Socket, records: unknown[]): void {
  socket.write(`${records.map((record) => JSON.stringify(record)).join("\n")}\n`);
}

test("bridge version reports an error when the daemon has no managed Pi version", async () => {
  const temp = await mkdtemp(join(tmpdir(), "pi-gateway-version-unknown-"));
  const server = createServer();
  try {
    await loadOrCreateToken(tokenPath(temp));
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    assert.ok(address && typeof address === "object");
    let pending = "";
    server.on("connection", (socket) => socket.on("data", (chunk: Buffer) => {
      pending += chunk.toString("utf8");
      const newline = pending.indexOf("\n");
      if (newline < 0) return;
      const hello = JSON.parse(pending.slice(0, newline)) as Record<string, unknown>;
      if (hello.type === "gw_hello") sendFrames(socket, [{ type: "gw_welcome", protocol: 1, piVersion: "" }]);
    }));
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
    assert.match(Buffer.concat(errors).toString("utf8"), /could not determine the managed pi version/);
  } finally {
    if (server.listening) await new Promise<void>((resolve) => server.close(() => resolve()));
    await rm(temp, { recursive: true, force: true });
  }
});
