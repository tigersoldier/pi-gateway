import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { createServer, Socket } from "node:net";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { test } from "node:test";
import { WirePeer } from "./support/gateway.ts";

const daemonEntry = fileURLToPath(new URL("../bin/pi-gatewayd.ts", import.meta.url));
const packageUnit = fileURLToPath(new URL("../../packaging/pi-gatewayd.service", import.meta.url));

async function freePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  assert.ok(address && typeof address !== "string");
  const port = address.port;
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return port;
}

async function authProbe(port: number, token: string): Promise<string> {
  const socket = new Socket();
  socket.connect(port, "127.0.0.1");
  await new Promise<void>((resolve, reject) => { socket.once("connect", resolve); socket.once("error", reject); });
  const peer = new WirePeer(socket);
  peer.send({ type: "gw_hello", protocol: 1, token, client: { name: "probe", kind: "test" } });
  try {
    const result = await peer.receive((frame) => frame.type === "gw_welcome" || frame.type === "gw_error");
    return String(result.type);
  } finally { peer.close(); }
}

async function waitForAuth(port: number, token: string, want: string, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let last = "";
  while (Date.now() < deadline) {
    try { last = await authProbe(port, token); } catch { last = "connection-failed"; }
    if (last === want) return;
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  assert.fail(`token handshake remained ${last}; wanted ${want}`);
}

test("daemon rejects invalid logger configuration before loading token files", () => {
  const result = spawnSync(process.execPath, ["--experimental-strip-types", daemonEntry, "--log-level", "loud"], { encoding: "utf8" });
  assert.equal(result.status, 1);
  assert.match(result.stderr, /gwlog: unknown level/);
});

test("daemon CLI help and version retain their advertised outputs", () => {
  const version = spawnSync(process.execPath, ["--experimental-strip-types", daemonEntry, "--version"], { encoding: "utf8" });
  assert.equal(version.status, 0);
  assert.equal(version.stdout, "pi-gatewayd 0.2.0 (gateway protocol 1)\n");
  const help = spawnSync(process.execPath, ["--experimental-strip-types", daemonEntry, "--help"], { encoding: "utf8" });
  assert.equal(help.status, 0);
  assert.match(help.stderr, /Usage: pi-gatewayd \[options\]/);
  assert.match(help.stderr, /--provision-token/);
  assert.doesNotMatch(help.stderr, /--pi/);
  assert.match(help.stderr, /SIGHUP reloads tokens/);
});

test("daemon CLI no longer accepts the Go-only --pi executable override", () => {
  const result = spawnSync(process.execPath, ["--experimental-strip-types", daemonEntry, "--pi", "/tmp/fake-pi", "--version"], { encoding: "utf8" });
  assert.equal(result.status, 2);
  assert.match(result.stderr, /flag provided but not defined: --pi/);
});

test("token provisioning matches Go validation, exit codes, and description format", async () => {
  const state = await mkdtemp(join(tmpdir(), "pi-gateway-cli-provision-"));
  try {
    const missing = spawnSync(process.execPath, ["--experimental-strip-types", daemonEntry,
      "--state-dir", state, "--provision-token", "--token-name", "missing-role"], { encoding: "utf8" });
    assert.equal(missing.status, 1);
    assert.match(missing.stderr, /--provision-token requires --token-role or --token-caps/);

    const provisioned = spawnSync(process.execPath, ["--experimental-strip-types", daemonEntry,
      "--state-dir", state, "--provision-token", "--token-name", "observer", "--token-role", "observer"], { encoding: "utf8" });
    assert.equal(provisioned.status, 0, provisioned.stderr);
    assert.match(provisioned.stdout, /^[0-9a-f]{64}\n$/);
    assert.match(provisioned.stderr, /provisioned token observer \(role observer\) in/);

    const badRole = spawnSync(process.execPath, ["--experimental-strip-types", daemonEntry,
      "--state-dir", state, "--provision-token", "--token-name", "bad-role", "--token-role", "root"], { encoding: "utf8" });
    assert.equal(badRole.status, 1);
    assert.match(badRole.stderr, /unknown role/);
  } finally { await rm(state, { recursive: true, force: true }); }
});

test("daemon CLI accepts compound Go durations and rejects malformed values", () => {
  const valid = spawnSync(process.execPath, ["--experimental-strip-types", daemonEntry, "--version",
    "--idle-timeout", "1h30m", "--short-grace", "250ms", "--delta-flush", "1.5s"], { encoding: "utf8" });
  assert.equal(valid.status, 0, valid.stderr);
  assert.equal(valid.stdout, "pi-gatewayd 0.2.0 (gateway protocol 1)\n");
  const invalid = spawnSync(process.execPath, ["--experimental-strip-types", daemonEntry,
    "--idle-timeout", "1sfoo", "--version"], { encoding: "utf8" });
  assert.equal(invalid.status, 2);
  assert.match(invalid.stderr, /time: invalid duration/);
});

test("SIGHUP atomically reloads provisioned tokens and preserves the live table on bad input", async () => {
  const state = await mkdtemp(join(tmpdir(), "pi-gateway-cli-sighup-"));
  const port = await freePort();
  const tokenFile = join(state, "token");
  const tokensFile = join(state, "tokens.json");
  await writeFile(tokenFile, "primary-token\n", { mode: 0o600 });
  const child = spawn(process.execPath, ["--experimental-strip-types", daemonEntry,
    "--state-dir", state, "--token-file", tokenFile, "--tokens-file", tokensFile,
    "--listen", `127.0.0.1:${port}`, "--no-debug", "--log-format=json"], {
    stdio: ["ignore", "pipe", "pipe"],
    env: process.env,
  });
  let stderr = "";
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", (chunk: string) => { stderr += chunk; });
  let exited = false;
  child.once("exit", () => { exited = true; });
  const stop = async () => {
    if (!exited && child.pid) child.kill("SIGTERM");
    if (!exited) await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => { child.off("exit", onExit); reject(new Error("daemon child did not stop")); }, 5000);
      const onExit = () => { clearTimeout(timer); resolve(); };
      child.once("exit", onExit);
    });
  };
  try {
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => { cleanup(); reject(new Error(`daemon did not start: ${stderr}`)); }, 8000);
      const onExit = () => { cleanup(); reject(new Error(`daemon exited during startup: ${stderr}`)); };
      const onData = () => { if (stderr.includes("daemon started")) { cleanup(); resolve(); } };
      const cleanup = () => { clearTimeout(timer); child.stderr.off("data", onData); child.off("exit", onExit); };
      child.stderr.on("data", onData);
      child.once("exit", onExit);
    });
    const records = stderr.split("\n").flatMap((line) => { try { return [JSON.parse(line) as Record<string, unknown>]; } catch { return []; } });
    const started = records.find((record) => record.msg === "daemon started");
    assert.equal(started?.level, "INFO");
    assert.equal(started?.protocol, 1);
    await waitForAuth(port, "provisioned-token", "gw_error", 1000);
    const provision = spawnSync(process.execPath, ["--experimental-strip-types", daemonEntry,
      "--state-dir", state, "--provision-token", "--token-name", "bot", "--token-role", "observer"], { encoding: "utf8" });
    assert.equal(provision.status, 0, provision.stderr);
    const provisioned = provision.stdout.trim();
    assert.ok(provisioned.length > 20, "provision command prints only a usable token");
    await waitForAuth(port, provisioned, "gw_error", 1000);

    assert.equal(child.kill("SIGHUP"), true);
    await waitForAuth(port, provisioned, "gw_welcome");
    assert.equal(await authProbe(port, "primary-token"), "gw_welcome");

    await writeFile(tokensFile, "{ this is invalid json", { mode: 0o600 });
    child.kill("SIGHUP");
    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.equal(await authProbe(port, provisioned), "gw_welcome");
    assert.equal(await authProbe(port, "primary-token"), "gw_welcome");
  } finally {
    await stop();
    await rm(state, { recursive: true, force: true });
  }
});

test("packaged systemd unit retains daemon-compatible lifecycle settings", async () => {
  const unit = await readFile(packageUnit, "utf8");
  for (const required of [
    "ExecStart=%h/.local/bin/pi-gatewayd",
    "Type=simple",
    "Restart=on-failure",
    "ExecReload=/bin/kill -HUP $MAINPID",
    "KillSignal=SIGTERM",
    "TimeoutStopSec=30",
  ]) assert.ok(unit.includes(required), `service unit is missing ${required}`);
  for (const line of unit.split("\n")) {
    if (line.trim().startsWith("Environment=")) assert.fail(`active Environment line: ${line.trim()}`);
  }
});
