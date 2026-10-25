import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { PiGatewayDaemon } from "../src/daemon.ts";
import { DebugHttpServer } from "../src/debug-http.ts";
import { dialGateway, fakePi, writeSessionFile } from "./support/gateway.ts";

test("debug listener exposes read-only status, catalog, and Prometheus metrics", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-gateway-debug-catalog-"));
  const oldRoot = process.env.PI_CODING_AGENT_SESSION_DIR;
  process.env.PI_CODING_AGENT_SESSION_DIR = root;
  const pi = fakePi({ turnDelayMs: 5 });
  const daemon = new PiGatewayDaemon({ addr: "127.0.0.1:0", token: "test-token", piBin: "/missing/pi-for-version-test",
    catalogRoots: [root], createRuntime: pi.createRuntime });
  const debug = new DebugHttpServer({ daemon, addr: "127.0.0.1:0" });
  let peer: Awaited<ReturnType<typeof dialGateway>> | undefined;
  try {
    await daemon.listen();
    await debug.listen();
    const statusResponse = await fetch(`http://${debug.address}/status`);
    assert.equal(statusResponse.status, 200);
    assert.match(statusResponse.headers.get("content-type") ?? "", /application\/json/);
    const status = await statusResponse.json() as Record<string, unknown>;
    assert.equal(status.version, "0.2.0");
    assert.equal(status.protocol, 1);
    assert.equal(status.debugAddr, debug.address);
    assert.equal((status.sessions as Record<string, unknown>).registered, 0);

    const catalogResponse = await fetch(`http://${debug.address}/catalog`);
    assert.equal(catalogResponse.status, 200);
    assert.deepEqual(await catalogResponse.json(), { sessions: [] });

    const metricsResponse = await fetch(`http://${debug.address}/metrics`);
    const metrics = await metricsResponse.text();
    assert.equal(metricsResponse.status, 200);
    assert.match(metricsResponse.headers.get("content-type") ?? "", /text\/plain; version=0\.0\.4/);
    assert.match(metrics, /# TYPE pi_gateway_frames_in_total counter\npi_gateway_frames_in_total 0/);
    assert.match(metrics, /# TYPE pi_gateway_sessions gauge\npi_gateway_sessions 0/);
    const samples = metrics.split("\n").filter((line) => line && !line.startsWith("#"));
    const names = samples.map((line) => line.split(" ", 1)[0]!);
    assert.deepEqual(names, [...names].sort(), "Prometheus samples are sorted by metric name");
    assert.equal(samples.length, 34, "all counters and gauges are declared before the first increment");
    for (const name of names) assert.ok(metrics.includes(`# HELP ${name} `), `missing HELP for ${name}`);

    const post = await fetch(`http://${debug.address}/status`, { method: "POST" });
    assert.equal(post.status, 405);
    assert.equal(post.headers.get("allow"), "GET, HEAD");
    const head = await fetch(`http://${debug.address}/status`, { method: "HEAD" });
    assert.equal(head.status, 200);
    assert.equal(await head.text(), "");
    const unknown = await fetch(`http://${debug.address}/missing`);
    assert.equal(unknown.status, 404);
    assert.deepEqual(await unknown.json(), { error: "unknown debug endpoint", paths: ["/status", "/catalog", "/metrics"] });

    const path = join(root, "--tmp--", "debug-live.jsonl");
    await writeSessionFile(path, "/home/u/project");
    peer = await dialGateway(daemon.addr!.port, "test-token", { session: path,
      piArgs: ["--api-key", "sk-debug-secret", "--append-system-prompt", "DEBUG_PROMPT_SECRET"] });
    peer.send({ type: "prompt", id: "debug-turn", message: "debug me" });
    assert.equal((await peer.receive((frame) => frame.type === "response" && frame.id === "debug-turn")).success, true);
    await peer.receive((frame) => frame.type === "agent_settled");

    const liveStatus = await (await fetch(`http://${debug.address}/status`)).json() as Record<string, unknown>;
    const liveSessions = liveStatus.sessions as Record<string, unknown>;
    assert.equal(liveSessions.registered, 1);
    assert.equal(liveSessions.live, 1);
    assert.equal(liveSessions.clients, 1);
    assert.equal(liveSessions.tokens, 1);
    assert.equal((liveSessions.liveSessions as Array<Record<string, unknown>>)[0]?.state, "ready");

    const liveCatalog = await (await fetch(`http://${debug.address}/catalog`)).text();
    assert.match(liveCatalog, /"live":true/);
    assert.doesNotMatch(liveCatalog, /sk-debug-secret|DEBUG_PROMPT_SECRET|"spawn"/);
    const scopedCatalog = await (await fetch(`http://${debug.address}/catalog?cwd=%2Fhome%2Fu%2Fproject&limit=1`)).json() as { sessions: Array<Record<string, unknown>> };
    assert.equal(scopedCatalog.sessions.length, 1);
    assert.equal(scopedCatalog.sessions[0]?.path, path);
    const excludedCatalog = await (await fetch(`http://${debug.address}/catalog?cwd=%2Ftmp`)).json() as { sessions: unknown[] };
    assert.deepEqual(excludedCatalog.sessions, []);
    const invalidLimitCatalog = await (await fetch(`http://${debug.address}/catalog?cwd=%2Fhome%2Fu%2Fproject&limit=invalid`)).json() as { sessions: unknown[] };
    assert.equal(invalidLimitCatalog.sessions.length, 1, "an invalid limit is ignored as zero/unlimited");
    const liveMetrics = await (await fetch(`http://${debug.address}/metrics`)).text();
    assert.match(liveMetrics, /pi_gateway_sessions_started_total 1/);
    assert.match(liveMetrics, /pi_gateway_turns_started_total 1/);
    const statusAgain = await daemon.status();
    assert.equal(statusAgain.live, 1);
    assert.equal(statusAgain.clients, 1);
  } finally {
    peer?.close();
    await debug.close();
    await daemon.shutdown();
    if (oldRoot === undefined) delete process.env.PI_CODING_AGENT_SESSION_DIR;
    else process.env.PI_CODING_AGENT_SESSION_DIR = oldRoot;
    await rm(root, { recursive: true, force: true });
  }
});
