import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { appendFile, mkdir, mkdtemp, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { connect, type Socket } from "node:net";
import { test } from "node:test";
import { PiGatewayDaemon } from "../src/daemon.ts";
import { parsePiArgs } from "../src/piargs.ts";
import { assertResponse, dialGateway, fakePi, WirePeer, type WireFrame, writeSessionFile } from "./support/gateway.ts";

function responseFor(id: string) { return (frame: WireFrame) => frame.type === "response" && frame.id === id; }
function messageText(frame: WireFrame): string {
  const message = frame.message as Record<string, unknown> | undefined;
  const content = message?.content;
  if (!Array.isArray(content)) return "";
  return content.map((item) => item && typeof item === "object" ? String((item as Record<string, unknown>).text ?? "") : "").join("");
}
function positiveSequence(frame: WireFrame, recipient: string): number {
  const sequence = frame.gw_seq;
  assert.ok(
    typeof sequence === "number" && Number.isSafeInteger(sequence) && sequence > 0,
    `${recipient} ${String(frame.type)} event must include a positive integer gw_seq`,
  );
  return sequence as number;
}
function assertSessionEventSequences(frames: WireFrame[], path: string, recipient: string): void {
  const turnStart = frames.findIndex((frame) => frame.type === "gw_turn" && frame.state === "running");
  assert.ok(turnStart >= 0, `${recipient} received no running turn event`);
  // Connection-level controls bypass Hub.publish and intentionally lack the session envelope.
  const unsequencedControls = new Set(["gw_snapshot", "gw_replay_done", "gw_pong"]);
  const events = frames.slice(turnStart).filter((frame) => !unsequencedControls.has(String(frame.type)));
  assert.ok(events.length > 0, `${recipient} received no sequenced session events`);
  let previous = 0;
  for (const frame of events) {
    assert.equal(frame.gw_session, path, `${recipient} ${String(frame.type)} event belongs to another session`);
    const sequence = positiveSequence(frame, recipient);
    assert.ok(sequence > previous, `${recipient} event sequence did not increase: ${previous} then ${sequence}`);
    previous = sequence;
  }
}

async function waitUntil(predicate: () => boolean | Promise<boolean>, timeoutMs = 3000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  assert.fail("timed out waiting for gateway state");
}

async function withSessionRoot<T>(prefix: string, run: (root: string) => Promise<T>): Promise<T> {
  const root = await mkdtemp(join(tmpdir(), prefix));
  const old = process.env.PI_CODING_AGENT_SESSION_DIR;
  process.env.PI_CODING_AGENT_SESSION_DIR = join(root, "default-sessions");
  try { return await run(root); }
  finally {
    if (old === undefined) delete process.env.PI_CODING_AGENT_SESSION_DIR;
    else process.env.PI_CODING_AGENT_SESSION_DIR = old;
    await rm(root, { recursive: true, force: true });
  }
}

test("unbound gw_welcome includes Go-compatible zero-value fields", async () => withSessionRoot("pi-gw-parity-welcome-", async (root) => {
  const daemon = new PiGatewayDaemon({ token: "welcome-token", addr: "127.0.0.1:0", piBin: "/missing/pi", catalogRoots: [root] });
  let peer: Awaited<ReturnType<typeof dialGateway>> | undefined;
  try {
    await daemon.listen();
    peer = await dialGateway(daemon.addr!.port, "welcome-token");
    const welcome = peer.welcome!;
    assert.equal(welcome.session, null);
    assert.equal(welcome.headSeq, 0);
    assert.equal(welcome.oldestSeq, 0);
    assert.equal(welcome.piState, null);
    assert.equal(welcome.clients, null);
    assert.deepEqual(welcome.turn, { state: "idle", queued: 0 });
    assert.equal(Object.hasOwn(welcome, "piVersion"), false, "Go omits an empty piVersion");
  } finally { peer?.close(); await daemon.shutdown(); }
}));

test("shared clients receive the same FIFO turn stream with queue attribution", async () => withSessionRoot("pi-gw-parity-queue-", async (root) => {
  const path = join(root, "--tmp--", "shared.jsonl");
  await writeSessionFile(path);
  const pi = fakePi({ turnDelayMs: 35 });
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi", catalogRoots: [root],
    idleTimeoutMs: 60_000, shortGraceMs: 60_000, createRuntime: pi.createRuntime });
  const peers: Array<Awaited<ReturnType<typeof dialGateway>>> = [];
  try {
    await daemon.listen();
    const a = await dialGateway(daemon.addr!.port, "parity-token", { session: path, client: { name: "first", kind: "test" } });
    const b = await dialGateway(daemon.addr!.port, "parity-token", { session: path, client: { name: "second", kind: "test" } });
    peers.push(a, b);
    const aId = String(a.welcome?.clientId);
    const bId = String(b.welcome?.clientId);
    const features = a.welcome?.features as string[];
    assert.equal(Object.hasOwn(a.welcome?.session as WireFrame, "name"), false, "empty session names are omitted like Go's omitempty");
    assert.ok(features.includes("spawn_config"));
    assert.ok(features.includes("spawn_config_additive"));
    assert.equal(features.includes("inject"), false);
    a.send({ type: "prompt", id: "p1", message: "first" });
    assertResponse(await a.receive(responseFor("p1")), true);
    b.send({ type: "prompt", id: "p2", message: "second" });
    const queued = await b.receive(responseFor("p2"));
    assertResponse(queued, true);
    assert.equal((queued.data as WireFrame).queued, true);

    const framesB = await b.collectUntil((frame) => frame.type === "message_end" && messageText(frame) === "echo: second");
    const framesA = await a.collectUntil((frame) => frame.type === "message_end" && messageText(frame) === "echo: second");
    assertSessionEventSequences(framesB, path, "second client");
    assertSessionEventSequences(framesA, path, "first client");
    for (const [who, frames] of [["second", framesB], ["first", framesA]] as const) {
      const messages = frames.filter((frame) => frame.type === "message_end").map(messageText);
      assert.deepEqual(messages, ["echo: first", "echo: second"], `${who} saw non-FIFO output`);
      const queue = frames.find((frame) => frame.type === "gw_queue" && Array.isArray(frame.pending) && frame.pending.length > 0);
      assert.ok(queue, `${who} did not see pending queue state`);
      const pending = (queue!.pending as WireFrame[])[0]!;
      assert.equal((pending.author as WireFrame).clientId, bId);
      assert.ok(frames.some((frame) => frame.type === "gw_turn" && (frame.author as WireFrame | undefined)?.clientId === aId));
      assert.ok(frames.some((frame) => frame.type === "gw_turn" && (frame.author as WireFrame | undefined)?.clientId === bId));
    }
    const settledEventB = framesB.find((frame) => frame.type === "message_end" && messageText(frame) === "echo: second");
    const settledEventA = framesA.find((frame) => frame.type === "message_end" && messageText(frame) === "echo: second");
    assert.ok(settledEventA, "first client did not receive the settling message_end");
    assert.ok(settledEventB, "second client did not receive the settling message_end");
    const settledSequenceB = positiveSequence(settledEventB!, "second client settling message_end");
    const settledSequenceA = positiveSequence(settledEventA!, "first client settling message_end");
    assert.equal(settledSequenceA, settledSequenceB, "subscribers disagree on the settling event sequence");
    assert.equal(pi.starts, 1);
  } finally {
    for (const peer of peers) peer.close();
    await daemon.shutdown();
  }
}));

test("slow reliable TCP subscribers are closed after bounded-buffer overflow", async () => withSessionRoot("pi-gw-parity-slow-", async (root) => {
  const path = join(root, "--tmp--", "slow.jsonl");
  await writeSessionFile(path);
  const pi = fakePi({ replayEvents: 300, turnDelayMs: 50 });
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi", catalogRoots: [root],
    subscriberBuffer: 8, idleTimeoutMs: 60_000, shortGraceMs: 60_000, createRuntime: pi.createRuntime });
  let peer: Awaited<ReturnType<typeof dialGateway>> | undefined;
  try {
    await daemon.listen();
    peer = await dialGateway(daemon.addr!.port, "parity-token", { session: path });
    const connection = [...(daemon as unknown as { connections: Set<{ socket: Socket }> }).connections][0]!;
    const socket = connection.socket;
    const write = socket.write.bind(socket);
    let blocked = false;
    socket.write = ((chunk: Uint8Array, callback?: (error?: Error | null) => void) => {
      if (!blocked) {
        blocked = true;
        socket.once("close", () => callback?.(new Error("socket closed")));
        return false;
      }
      return write(chunk, callback);
    }) as unknown as typeof socket.write;
    peer.send({ type: "prompt", id: "overflow", message: "flood a reliable subscriber" });
    await new Promise<void>((resolve, reject) => {
      const onClose = () => { clearTimeout(timer); resolve(); };
      const timer = setTimeout(() => { peer!.socket.off("close", onClose); reject(new Error("slow subscriber was not closed")); }, 3000);
      peer!.socket.once("close", onClose);
    });
    assert.ok((daemon.metricSnapshot.get("pi_gateway_subscriber_drops_total") ?? 0) > 0);
  } finally {
    peer?.close();
    await daemon.shutdown();
  }
}));

test("lossy TCP subscribers receive a lag marker and remain connected", async () => withSessionRoot("pi-gw-parity-lossy-", async (root) => {
  const path = join(root, "--tmp--", "lossy.jsonl");
  await writeSessionFile(path);
  const pi = fakePi({ replayEvents: 300, turnDelayMs: 100 });
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi", catalogRoots: [root],
    subscriberBuffer: 8, idleTimeoutMs: 60_000, shortGraceMs: 60_000, createRuntime: pi.createRuntime });
  let peer: Awaited<ReturnType<typeof dialGateway>> | undefined;
  try {
    await daemon.listen();
    peer = await dialGateway(daemon.addr!.port, "parity-token", { session: path, allowLossy: true });
    peer.send({ type: "prompt", id: "lossy", message: "flood a lossy subscriber" });
    assertResponse(await peer.receive(responseFor("lossy")), true);
    const frames = await peer.collectUntil((frame) => frame.type === "agent_settled");
    const lag = frames.find((frame) => frame.type === "gw_lag");
    assert.ok(lag, "a lossy gap must be reported");
    assert.ok(Number(lag.oldestSeq) > 0);
    assert.ok(Number(lag.headSeq) >= Number(lag.oldestSeq));
    assert.equal(peer.socket.destroyed, false);
    assert.ok((daemon.metricSnapshot.get("pi_gateway_client_lags_total") ?? 0) > 0);
  } finally {
    peer?.close();
    await daemon.shutdown();
  }
}));

test("TCP delta coalescing preserves the full streaming text", async () => withSessionRoot("pi-gw-parity-coalesce-", async (root) => {
  const path = join(root, "--tmp--", "coalesce.jsonl");
  await writeSessionFile(path);
  const pi = fakePi({ deltaEvents: 40, turnDelayMs: 5 });
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi", catalogRoots: [root],
    deltaFlushMs: 1000, subscriberBuffer: 1024, idleTimeoutMs: 60_000, shortGraceMs: 60_000, createRuntime: pi.createRuntime });
  let peer: Awaited<ReturnType<typeof dialGateway>> | undefined;
  try {
    await daemon.listen();
    peer = await dialGateway(daemon.addr!.port, "parity-token", { session: path });
    peer.send({ type: "prompt", id: "coalesce", message: "stream" });
    assertResponse(await peer.receive(responseFor("coalesce")), true);
    const frames = await peer.collectUntil((frame) => frame.type === "agent_settled");
    const deltas = frames.filter((frame) => frame.type === "message_update").map((frame) =>
      String(((frame.assistantMessageEvent as WireFrame | undefined)?.delta) ?? ""));
    assert.ok(deltas.length > 0 && deltas.length < 40, `expected coalesced updates, got ${deltas.length}`);
    assert.equal(deltas.join(""), Array.from({ length: 40 }, (_, index) => `stream[${index}]`).join(""));
  } finally {
    peer?.close();
    await daemon.shutdown();
  }
}));

test("replay across a running turn preserves every streaming delta", async () => withSessionRoot("pi-gw-parity-delta-replay-", async (root) => {
  const path = join(root, "--tmp--", "delta-replay.jsonl");
  await writeSessionFile(path);
  const pi = fakePi({ deltaEvents: 200, turnDelayMs: 100 });
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi", catalogRoots: [root],
    idleTimeoutMs: 60_000, shortGraceMs: 60_000, subscriberBuffer: 1024, createRuntime: pi.createRuntime });
  const peers: Array<Awaited<ReturnType<typeof dialGateway>>> = [];
  try {
    await daemon.listen();
    const first = await dialGateway(daemon.addr!.port, "parity-token", { session: path });
    peers.push(first);
    first.send({ type: "prompt", id: "stream", message: "stream" });
    assertResponse(await first.receive(responseFor("stream")), true);
    const resumed = await dialGateway(daemon.addr!.port, "parity-token", { session: path, liveOnly: false, resume: { sinceSeq: 0 } });
    peers.push(resumed);
    const frames = await resumed.collectUntil((frame) => frame.type === "agent_settled");
    assert.ok(frames.some((frame) => frame.type === "gw_replay_done"), "reconnect must deliver its replay boundary");
    const deltas = frames.filter((frame) => frame.type === "message_update").map((frame) =>
      String(((frame.assistantMessageEvent as WireFrame | undefined)?.delta) ?? ""));
    assert.equal(deltas.join(""), Array.from({ length: 200 }, (_, index) => `stream[${index}]`).join(""));
  } finally {
    for (const peer of peers) peer.close();
    await daemon.shutdown();
  }
}));

test("a dropped client does not stop the turn, and reconnect replay is ordered", async () => withSessionRoot("pi-gw-parity-replay-", async (root) => {
  const path = join(root, "--tmp--", "replay.jsonl");
  await writeSessionFile(path);
  const pi = fakePi({ turnDelayMs: 90 });
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi", catalogRoots: [root],
    idleTimeoutMs: 60_000, shortGraceMs: 60_000, createRuntime: pi.createRuntime });
  const peers: Array<Awaited<ReturnType<typeof dialGateway>>> = [];
  try {
    await daemon.listen();
    const a = await dialGateway(daemon.addr!.port, "parity-token", { session: path });
    peers.push(a);
    a.send({ type: "prompt", id: "turn", message: "survive" });
    assertResponse(await a.receive(responseFor("turn")), true);
    a.close();
    const b = await dialGateway(daemon.addr!.port, "parity-token", { session: path });
    peers.push(b);
    assert.equal(pi.starts, 1, "attaching must not start a second Pi session");
    const live = await b.collectUntil((frame) => frame.type === "agent_settled");
    assert.ok(live.some((frame) => frame.type === "message_end" && messageText(frame) === "echo: survive"));
    b.close();
    const replay = await dialGateway(daemon.addr!.port, "parity-token", { session: path, liveOnly: false, resume: { sinceSeq: 0 } });
    peers.push(replay);
    assert.notEqual(replay.welcome?.resyncRequired, true);
    const window = await replay.collectUntil((frame) => frame.type === "gw_replay_done");
    const sequences = window.filter((frame) => Number(frame.gw_seq) > 0).map((frame) => Number(frame.gw_seq));
    assert.ok(sequences.length > 0);
    assert.equal(new Set(sequences).size, sequences.length, "replay/live boundary duplicated a sequence");
    for (let i = 1; i < sequences.length; i++) assert.ok(sequences[i]! > sequences[i - 1]!);
    assert.ok(window.some((frame) => frame.type === "message_end" && messageText(frame) === "echo: survive"));
    assert.equal(pi.starts, 1);
  } finally {
    for (const peer of peers) peer.close();
    await daemon.shutdown();
  }
}));

test("new sessions use client cwd while hibernated sessions respawn in their stored cwd", async () => withSessionRoot("pi-gw-parity-cwd-", async (root) => {
  const clientCwd = join(root, "client-work");
  const sessionCwd = join(root, "session-work");
  await mkdir(clientCwd);
  await mkdir(sessionCwd);
  const freshPath = join(root, "--tmp--", "fresh-cwd.jsonl");
  const storedPath = join(root, "--tmp--", "stored-cwd.jsonl");
  await writeSessionFile(freshPath, clientCwd);
  await writeSessionFile(storedPath, sessionCwd);
  const pi = fakePi({ sessionFile: freshPath, turnDelayMs: 5 });
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi", catalogRoots: [root],
    idleTimeoutMs: 30, shortGraceMs: 20, createRuntime: pi.createRuntime });
  let fresh: Awaited<ReturnType<typeof dialGateway>> | undefined;
  let stored: Awaited<ReturnType<typeof dialGateway>> | undefined;
  let resumed: Awaited<ReturnType<typeof dialGateway>> | undefined;
  try {
    await daemon.listen();
    fresh = await dialGateway(daemon.addr!.port, "parity-token", { cwd: clientCwd });
    fresh.send({ type: "get_state", id: "fresh-state" });
    const freshState = await fresh.receive(responseFor("fresh-state"));
    assertResponse(freshState, true);
    assert.equal((freshState.data as WireFrame).sessionFile, freshPath);
    assert.equal(pi.cwds[0], clientCwd);
    fresh.send({ type: "prompt", id: "fresh-turn", message: "new cwd" });
    assertResponse(await fresh.receive(responseFor("fresh-turn")), true);
    await fresh.receive((frame) => frame.type === "agent_settled");
    fresh.close();
    await waitUntil(async () => (await daemon.status()).live === 0);

    stored = await dialGateway(daemon.addr!.port, "parity-token", { session: storedPath, cwd: clientCwd });
    stored.send({ type: "prompt", id: "stored-turn", message: "stored cwd" });
    assertResponse(await stored.receive(responseFor("stored-turn")), true);
    await stored.receive((frame) => frame.type === "agent_settled");
    assert.equal(pi.cwds[1], sessionCwd, "session header cwd overrides the attaching client's cwd");
    stored.close();
    await waitUntil(async () => (await daemon.status()).live === 0);
    await waitUntil(() => pi.disposals === 2);

    resumed = await dialGateway(daemon.addr!.port, "parity-token", { session: storedPath, cwd: clientCwd });
    assert.equal(pi.cwds[2], sessionCwd, "cold respawn must retain the session's own working directory");
    assert.equal(pi.starts, 3);
  } finally {
    fresh?.close();
    stored?.close();
    resumed?.close();
    await daemon.shutdown();
  }
}));

test("attach waits for idle retirement to dispose the old runtime", async () => withSessionRoot("pi-gw-parity-retire-attach-", async (root) => {
  const path = join(root, "sessions", "retire-race.jsonl");
  await writeSessionFile(path);
  const pi = fakePi();
  let releaseDispose!: () => void;
  let signalDisposeStarted!: () => void;
  const disposeGate = new Promise<void>((resolve) => { releaseDispose = resolve; });
  const disposeStarted = new Promise<void>((resolve) => { signalDisposeStarted = resolve; });
  let firstDisposed = false;
  let secondStartedTooSoon = false;
  const createRuntime = async (options: Parameters<typeof pi.createRuntime>[0]) => {
    const runtime = await pi.createRuntime(options);
    if (pi.starts === 1) return {
      ...runtime,
      async dispose() { signalDisposeStarted(); await disposeGate; await runtime.dispose(); firstDisposed = true; },
    };
    if (!firstDisposed) secondStartedTooSoon = true;
    return runtime;
  };
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi",
    catalogRoots: [root], createRuntime, idleTimeoutMs: 60_000, shortGraceMs: 60_000 });
  let peer: Awaited<ReturnType<typeof dialGateway>> | undefined;
  try {
    await daemon.listen();
    peer = await dialGateway(daemon.addr!.port, "parity-token", { session: path });
    const oldActor = daemon.sessionActors[0]!;
    peer.close();
    await waitUntil(() => oldActor.attached === 0);

    const retirement = (oldActor as unknown as { handleIdle(): Promise<void> }).handleIdle();
    await disposeStarted;
    const pendingAttach = daemon.attach(path, parsePiArgs([]), "/tmp");
    await waitUntil(() => [...(daemon as unknown as { pendingAttaches: Map<string, number> }).pendingAttaches.values()].some((count) => count > 0));
    releaseDispose();
    await retirement;
    const replacement = await pendingAttach;
    assert.notEqual(replacement, oldActor);
    assert.equal(firstDisposed, true);
    assert.equal(secondStartedTooSoon, false, "a replacement runtime must not overlap retirement");
    assert.equal(pi.starts, 2);
  } finally {
    releaseDispose();
    peer?.close();
    await daemon.shutdown();
  }
}));

test("idle reaper never stops a detached session mid-turn", async () => withSessionRoot("pi-gw-parity-mid-turn-", async (root) => {
  const path = join(root, "--tmp--", "mid-turn.jsonl");
  await writeSessionFile(path);
  const pi = fakePi({ turnDelayMs: 120 });
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi", catalogRoots: [root],
    idleTimeoutMs: 10, shortGraceMs: 10, createRuntime: pi.createRuntime });
  let peer: Awaited<ReturnType<typeof dialGateway>> | undefined;
  try {
    await daemon.listen();
    peer = await dialGateway(daemon.addr!.port, "parity-token", { session: path });
    peer.send({ type: "prompt", id: "long-turn", message: "finish before hibernate" });
    assertResponse(await peer.receive(responseFor("long-turn")), true);
    peer.close();
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.equal((await daemon.status()).live, 1, "active turn must outlive the idle grace");
    assert.deepEqual(pi.prompts, ["finish before hibernate"]);
    await waitUntil(async () => (await daemon.status()).live === 0);
    assert.equal(pi.disposals, 1);
  } finally {
    peer?.close();
    await daemon.shutdown();
  }
}));

test("idle sessions hibernate after detach and re-adopt from the session file", async () => withSessionRoot("pi-gw-parity-hibernate-", async (root) => {
  const path = join(root, "--tmp--", "hibernate.jsonl");
  await writeSessionFile(path);
  const pi = fakePi({ turnDelayMs: 5 });
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi", catalogRoots: [root],
    idleTimeoutMs: 30, shortGraceMs: 20, createRuntime: pi.createRuntime });
  let first: Awaited<ReturnType<typeof dialGateway>> | undefined;
  let second: Awaited<ReturnType<typeof dialGateway>> | undefined;
  try {
    await daemon.listen();
    first = await dialGateway(daemon.addr!.port, "parity-token", { session: path });
    first.send({ type: "prompt", id: "turn", message: "persist" });
    assertResponse(await first.receive(responseFor("turn")), true);
    await first.receive((frame) => frame.type === "agent_settled");
    first.close();
    await waitUntil(async () => (await daemon.status()).live === 0);
    await waitUntil(() => pi.disposals === 1);
    assert.equal(daemon.metricSnapshot.get("pi_gateway_sessions_ended_total"), 1);
    assert.equal(daemon.metricSnapshot.get("pi_gateway_sessions_reaped_total"), 1);
    second = await dialGateway(daemon.addr!.port, "parity-token", { session: path });
    second.send({ type: "get_state", id: "after-hibernate" });
    const state = await second.receive(responseFor("after-hibernate"));
    assertResponse(state, true);
    assert.equal((state.data as WireFrame).sessionFile, path);
    assert.equal(pi.starts, 2);
  } finally {
    first?.close();
    second?.close();
    await daemon.shutdown();
  }
}));

test("live attach applies runtime-only changes, ignores new spawn keys, and rejects conflicts", async () => withSessionRoot("pi-gw-parity-spawn-", async (root) => {
  const path = join(root, "--tmp--", "spawn.jsonl");
  await writeSessionFile(path);
  const pi = fakePi();
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi", catalogRoots: [root],
    idleTimeoutMs: 60_000, shortGraceMs: 60_000, createRuntime: pi.createRuntime });
  const peers: Array<Awaited<ReturnType<typeof dialGateway>>> = [];
  try {
    await daemon.listen();
    const a = await dialGateway(daemon.addr!.port, "parity-token", { session: path,
      piArgs: ["--system-prompt", "original", "--model", "test/model", "--provider", "test"] });
    const b = await dialGateway(daemon.addr!.port, "parity-token", { session: path,
      piArgs: ["--system-prompt", "original", "--extension", "/tmp/unrecorded.ts", "--model", "alt/model", "--provider", "alt"] });
    peers.push(a, b);
    assert.equal(pi.starts, 1, "live attach with only additional spawn keys must reuse the actor");
    const changed = await a.receive((frame) => frame.type === "gw_state_changed" && frame.command === "set_model");
    assert.equal((changed.data as WireFrame).id, "alt/model");
    assert.equal((changed.by as WireFrame).clientId, b.welcome?.clientId);
    b.send({ type: "get_state", id: "runtime-state" });
    const state = await b.receive(responseFor("runtime-state"));
    assert.equal(((state.data as WireFrame).model as WireFrame).id, "alt/model");

    const conflict = await dialGateway(daemon.addr!.port, "parity-token", { session: path,
      piArgs: ["--system-prompt", "conflict"], expectHandshakeError: true });
    peers.push(conflict);
    assert.equal(conflict.welcome?.session, null);
    assert.equal(conflict.handshakeError?.code, "spawn_param_conflict");
  } finally {
    for (const peer of peers) peer.close();
    await daemon.shutdown();
  }
}));

test("client runtime mutations broadcast with author attribution to all clients", async () => withSessionRoot("pi-gw-parity-state-change-", async (root) => {
  const path = join(root, "--tmp--", "state-change.jsonl");
  await writeSessionFile(path);
  const pi = fakePi();
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi", catalogRoots: [root],
    idleTimeoutMs: 60_000, shortGraceMs: 60_000, createRuntime: pi.createRuntime });
  const peers: Array<Awaited<ReturnType<typeof dialGateway>>> = [];
  try {
    await daemon.listen();
    const author = await dialGateway(daemon.addr!.port, "parity-token", { session: path });
    const bystander = await dialGateway(daemon.addr!.port, "parity-token", { session: path });
    peers.push(author, bystander);
    author.send({ type: "set_model", id: "set-model", provider: "alt", modelId: "alt/model" });
    assertResponse(await author.receive(responseFor("set-model")), true);
    const changes = await Promise.all(peers.map((peer) => peer.receive((frame) => frame.type === "gw_state_changed" && frame.command === "set_model")));
    for (const changed of changes) {
      assert.equal((changed.data as WireFrame).id, "alt/model");
      assert.equal((changed.by as WireFrame).clientId, author.welcome?.clientId);
    }
  } finally {
    for (const peer of peers) peer.close();
    await daemon.shutdown();
  }
}));

test("rejected prompt releases the turn so the next prompt starts immediately", async () => withSessionRoot("pi-gw-parity-reject-", async (root) => {
  const path = join(root, "--tmp--", "reject.jsonl");
  await writeSessionFile(path);
  const pi = fakePi({ rejectFirstPrompt: true, turnDelayMs: 20 });
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi", catalogRoots: [root],
    idleTimeoutMs: 60_000, shortGraceMs: 60_000, createRuntime: pi.createRuntime });
  let peer: Awaited<ReturnType<typeof dialGateway>> | undefined;
  try {
    await daemon.listen();
    peer = await dialGateway(daemon.addr!.port, "parity-token", { session: path });
    peer.send({ type: "prompt", id: "rejected", message: "reject this one" });
    const rejected = await peer.receive(responseFor("rejected"));
    assert.equal(rejected.success, false);
    assert.match(String(rejected.error), /fake prompt rejected/);
    assert.equal((await peer.receive((frame) => frame.type === "gw_turn" && frame.state === "settled")).state, "settled");

    peer.send({ type: "prompt", id: "next", message: "run immediately" });
    const next = await peer.receive(responseFor("next"));
    assertResponse(next, true);
    assert.notEqual((next.data as WireFrame | undefined)?.queued, true);
    await peer.receive((frame) => frame.type === "agent_settled");
    assert.deepEqual(pi.prompts, ["reject this one", "run immediately"]);
  } finally {
    peer?.close();
    await daemon.shutdown();
  }
}));

test("post-acceptance SDK failure is reported and the session respawns only on explicit attach", async () => withSessionRoot("pi-gw-parity-crash-", async (root) => {
  const path = join(root, "--tmp--", "crash.jsonl");
  await writeSessionFile(path);
  const pi = fakePi({ crashAfterPrompt: true });
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi", catalogRoots: [root],
    idleTimeoutMs: 60_000, shortGraceMs: 60_000, createRuntime: pi.createRuntime });
  let peer: Awaited<ReturnType<typeof dialGateway>> | undefined;
  try {
    await daemon.listen();
    peer = await dialGateway(daemon.addr!.port, "parity-token", { session: path });
    peer.send({ type: "prompt", id: "crash", message: "fail after acceptance" });
    assertResponse(await peer.receive(responseFor("crash")), true);
    const crashed = await peer.receive((frame) => frame.type === "gw_session_state" && frame.state === "crashed");
    assert.match(String(crashed.reason), /fake runtime crashed after prompt acceptance/);
    await waitUntil(async () => Number((await daemon.status()).live) === 0);
    assert.equal(pi.starts, 1, "crashed runtime must not restart automatically");
    assert.equal(pi.disposals, 1);
    peer.send({ type: "gw_reload_session", id: "target-reload-crashed", session: path, force: true });
    assertResponse(await peer.receive(responseFor("target-reload-crashed")), false, "unknown_session");
    peer.send({ type: "prompt", id: "reject-crashed", message: "not sent to a crashed runtime" });
    assertResponse(await peer.receive(responseFor("reject-crashed")), false, "session_crashed");
    assert.equal(daemon.metricSnapshot.get("pi_gateway_prompts_rejected_total"), 1);

    peer.close();
    await waitUntil(() => daemon.sessionActors.length === 0);
    peer = await dialGateway(daemon.addr!.port, "parity-token", { session: path });
    assert.equal(pi.starts, 2, "explicit attach after last-client retirement starts a replacement runtime");
    peer.send({ type: "get_state", id: "re-adopt-state" });
    const state = await peer.receive(responseFor("re-adopt-state"));
    assertResponse(state, true);
    assert.equal((state.data as WireFrame).sessionFile, path);
  } finally {
    peer?.close();
    await daemon.shutdown();
  }
}));

test("shutdown cancels a replacement runtime that is still starting", async () => withSessionRoot("pi-gw-parity-shutdown-reload-", async (root) => {
  const path = join(root, "sessions", "shutdown-reload.jsonl");
  await writeSessionFile(path);
  const pi = fakePi();
  let calls = 0;
  let lateRuntimeDisposals = 0;
  let releaseReplacement!: () => void;
  let signalReplacement!: () => void;
  const replacementGate = new Promise<void>((resolve) => { releaseReplacement = resolve; });
  const replacementStarted = new Promise<void>((resolve) => { signalReplacement = resolve; });
  const createRuntime = async (options: Parameters<typeof pi.createRuntime>[0]) => {
    calls++;
    const call = calls;
    if (call === 2) { signalReplacement(); await replacementGate; }
    const runtime = await pi.createRuntime(options);
    return { ...runtime, async dispose() {
      await runtime.dispose();
      if (call === 2) lateRuntimeDisposals++;
    } };
  };
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi",
    catalogRoots: [root], createRuntime, idleTimeoutMs: 60_000, shortGraceMs: 60_000 });
  let peer: Awaited<ReturnType<typeof dialGateway>> | undefined;
  try {
    await daemon.listen();
    peer = await dialGateway(daemon.addr!.port, "parity-token", { session: path });
    const actor = daemon.sessionActors[0]!;
    peer.send({ type: "gw_reload_session", id: "reload-during-shutdown", session: path, force: true });
    await replacementStarted;
    const connection = [...(daemon as unknown as { connections: Set<{ socket: Socket }> }).connections][0]!;
    assert.equal(connection.socket.isPaused(), true, "incoming frames stay in the bounded socket buffer while reload is pending");
    for (let index = 0; index < 100; index++) peer.send({ type: "gw_ping", id: `pending-${index}` });
    await daemon.shutdown();
    assert.equal(actor.state, "stopped");
    releaseReplacement();
    await waitUntil(() => lateRuntimeDisposals > 0);
    assert.equal(lateRuntimeDisposals, 1, "replacement runtime is disposed exactly once after shutdown");
    assert.equal(actor.state, "stopped", "late startup must not restore readiness");
    assert.equal(actor.live, false);
    assert.equal(daemon.sessionActors.length, 0);
  } finally {
    releaseReplacement();
    peer?.close();
    await daemon.shutdown();
  }
}));

test("additive reload after crash preserves acknowledged runtime settings", async () => withSessionRoot("pi-gw-parity-crash-reload-", async (root) => {
  const path = join(root, "--tmp--", "crash-reload.jsonl");
  await writeSessionFile(path);
  const pi = fakePi({ crashAfterPrompt: true });
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi", catalogRoots: [root],
    spawnDir: join(root, "state", "spawn"), idleTimeoutMs: 60_000, shortGraceMs: 60_000, createRuntime: pi.createRuntime });
  const peers: Array<Awaited<ReturnType<typeof dialGateway>>> = [];
  try {
    await daemon.listen();
    const owner = await dialGateway(daemon.addr!.port, "parity-token", { session: path,
      piArgs: ["--model", "test/model", "--provider", "test", "--thinking", "low"] });
    const runtimeClient = await dialGateway(daemon.addr!.port, "parity-token", { session: path,
      piArgs: ["--model", "alt/model", "--provider", "alt", "--thinking", "high", "--name", "live-name"] });
    peers.push(owner, runtimeClient);
    for (const command of ["set_model", "set_thinking_level", "set_session_name"]) {
      await owner.receive((frame) => frame.type === "gw_state_changed" && frame.command === command);
    }
    runtimeClient.close();
    await waitUntil(async () => Number((await daemon.status()).clients) === 1);
    owner.send({ type: "prompt", id: "crash-after-runtime-change", message: "crash" });
    assertResponse(await owner.receive(responseFor("crash-after-runtime-change")), true);
    await owner.receive((frame) => frame.type === "gw_session_state" && frame.state === "crashed");
    owner.send({ type: "gw_reload_session", id: "add-after-crash", addPiArgs: ["--extension", "/after-crash.ts"] });
    assertResponse(await owner.receive(responseFor("add-after-crash")), true);
    await owner.receive((frame) => frame.type === "gw_session_state" && frame.state === "ready" && frame.reason === "reload");
    assert.equal(pi.starts, 2);
    const args = pi.argsSeen[1]!;
    for (const value of ["--model", "alt/model", "--provider", "alt", "--thinking", "high", "--name", "live-name", "--extension", "/after-crash.ts"]) {
      assert.ok(args.includes(value), `restarted runtime lost ${value}: ${JSON.stringify(args)}`);
    }
  } finally {
    for (const peer of peers) peer.close();
    await daemon.shutdown();
  }
}));

test("stale UI answers after a runtime crash return session_crashed", async () => withSessionRoot("pi-gw-parity-crash-ui-", async (root) => {
  const path = join(root, "--tmp--", "crash-ui.jsonl");
  await writeSessionFile(path);
  const pi = fakePi({ uiOnPrompt: true, crashAfterPrompt: true });
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi", catalogRoots: [root],
    idleTimeoutMs: 60_000, shortGraceMs: 60_000, createRuntime: pi.createRuntime });
  let peer: Awaited<ReturnType<typeof dialGateway>> | undefined;
  try {
    await daemon.listen();
    peer = await dialGateway(daemon.addr!.port, "parity-token", { session: path });
    peer.send({ type: "prompt", id: "crash-dialog", message: "ask then crash" });
    assertResponse(await peer.receive(responseFor("crash-dialog")), true);
    const dialog = await peer.receive((frame) => frame.type === "extension_ui_request" && frame.method === "select");
    peer.send({ type: "extension_ui_response", id: dialog.id, value: "one" });
    const crashed = await peer.receive((frame) => frame.type === "gw_session_state" && frame.state === "crashed");
    assert.match(String(crashed.reason), /fake runtime crashed after prompt acceptance/);
    peer.send({ type: "extension_ui_response", id: dialog.id, value: "stale" });
    const stale = await peer.receive(responseFor(String(dialog.id)));
    assert.equal(stale.success, false);
    assert.equal(stale.code, "session_crashed");
  } finally {
    peer?.close();
    await daemon.shutdown();
  }
}));

test("steer is immediate, follow_up is queued, and clear_queue returns daemon prompts", async () => withSessionRoot("pi-gw-parity-clear-", async (root) => {
  const path = join(root, "--tmp--", "clear.jsonl");
  await writeSessionFile(path);
  const pi = fakePi({ turnDelayMs: 10_000 });
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi", catalogRoots: [root],
    idleTimeoutMs: 60_000, shortGraceMs: 60_000, createRuntime: pi.createRuntime });
  let peer: Awaited<ReturnType<typeof dialGateway>> | undefined;
  try {
    await daemon.listen();
    peer = await dialGateway(daemon.addr!.port, "parity-token", { session: path });
    peer.send({ type: "prompt", id: "active", message: "hold" });
    assertResponse(await peer.receive(responseFor("active")), true);
    peer.send({ type: "steer", id: "steer", message: "urgent" });
    assertResponse(await peer.receive(responseFor("steer")), true);
    peer.send({ type: "follow_up", id: "follow", message: "later" });
    const follow = await peer.receive(responseFor("follow"));
    assertResponse(follow, true);
    assert.equal((follow.data as WireFrame).queued, true);
    assert.deepEqual(pi.steers, ["urgent"]);
    assert.deepEqual(pi.prompts, ["hold"]);
    peer.send({ type: "clear_queue", id: "clear" });
    const cleared = await peer.receive(responseFor("clear"));
    assertResponse(cleared, true);
    assert.match(JSON.stringify(cleared.data), /later/);
    peer.send({ type: "abort", id: "abort" });
    assertResponse(await peer.receive(responseFor("abort")), true);
  } finally {
    peer?.close();
    await daemon.shutdown();
  }
}));

test("abort settles the active turn but preserves and drains queued follow-ups", async () => withSessionRoot("pi-gw-parity-abort-", async (root) => {
  const path = join(root, "--tmp--", "abort.jsonl");
  await writeSessionFile(path);
  const pi = fakePi({ turnDelayMs: 500 });
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi", catalogRoots: [root],
    idleTimeoutMs: 60_000, shortGraceMs: 60_000, createRuntime: pi.createRuntime });
  let peer: Awaited<ReturnType<typeof dialGateway>> | undefined;
  try {
    await daemon.listen();
    peer = await dialGateway(daemon.addr!.port, "parity-token", { session: path });
    peer.send({ type: "prompt", id: "active", message: "long" });
    assertResponse(await peer.receive(responseFor("active")), true);
    peer.send({ type: "follow_up", id: "queued", message: "must still run" });
    const queued = await peer.receive(responseFor("queued"));
    assert.equal((queued.data as WireFrame).queued, true);
    peer.send({ type: "abort", id: "abort" });
    assertResponse(await peer.receive(responseFor("abort")), true);
    const completion = await peer.collectUntil((frame) => frame.type === "message_end" && messageText(frame) === "echo: must still run");
    assert.ok(completion.some((frame) => frame.type === "gw_turn" && frame.state === "settled"));
    assert.deepEqual(pi.prompts, ["long", "must still run"]);
  } finally {
    peer?.close();
    await daemon.shutdown();
  }
}));

test("oversized replay sends a snapshot and leaves the connection usable", async () => withSessionRoot("pi-gw-parity-large-replay-", async (root) => {
  const path = join(root, "--tmp--", "large-replay.jsonl");
  await writeSessionFile(path);
  const pi = fakePi({ sessionFile: path, turnDelayMs: 10, replayEvents: 1200 });
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi", catalogRoots: [root],
    idleTimeoutMs: 60_000, shortGraceMs: 60_000, subscriberBuffer: 2048, createRuntime: pi.createRuntime });
  const peers: Array<Awaited<ReturnType<typeof dialGateway>>> = [];
  try {
    await daemon.listen();
    const first = await dialGateway(daemon.addr!.port, "parity-token", { session: path });
    peers.push(first);
    first.send({ type: "prompt", id: "large", message: "emit a large replay" });
    assertResponse(await first.receive(responseFor("large")), true);
    await first.receive((frame) => frame.type === "agent_settled");
    const resumed = await dialGateway(daemon.addr!.port, "parity-token", { session: path, liveOnly: false, resume: { sinceSeq: 0 } });
    peers.push(resumed);
    assert.equal(resumed.welcome?.resyncRequired, true);
    assert.equal((await resumed.receive((frame) => frame.type === "gw_snapshot")).type, "gw_snapshot");
    resumed.send({ type: "get_state", id: "usable-after-resync" });
    assertResponse(await resumed.receive(responseFor("usable-after-resync")), true);
    assert.equal(pi.starts, 1);
  } finally {
    for (const peer of peers) peer.close();
    await daemon.shutdown();
  }
}));

test("durable matching leaf skips a stale replay snapshot while a stale leaf resyncs", async () => withSessionRoot("pi-gw-parity-leaf-", async (root) => {
  const path = join(root, "--tmp--", "durable.jsonl");
  await writeSessionFile(path);
  const pi = fakePi({ sessionFile: path, turnDelayMs: 10 });
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi", catalogRoots: [root],
    idleTimeoutMs: 60_000, shortGraceMs: 60_000, createRuntime: pi.createRuntime });
  const peers: Array<Awaited<ReturnType<typeof dialGateway>>> = [];
  try {
    await daemon.listen();
    const first = await dialGateway(daemon.addr!.port, "parity-token", { session: path });
    peers.push(first);
    first.send({ type: "prompt", id: "persist", message: "durable" });
    assertResponse(await first.receive(responseFor("persist")), true);
    await first.receive((frame) => frame.type === "agent_settled");
    await appendFile(path, `${JSON.stringify({ type: "message", id: "durable-leaf" })}\n`);

    const matching = await dialGateway(daemon.addr!.port, "parity-token", { session: path, liveOnly: false,
      resume: { sinceSeq: 1_000_000, leafEntryId: "durable-leaf" } });
    peers.push(matching);
    assert.equal(matching.welcome?.resyncRequired, false);
    await assert.rejects(matching.receive((frame) => frame.type === "gw_snapshot" || frame.type === "gw_replay_done", 80), /timed out/);

    const stale = await dialGateway(daemon.addr!.port, "parity-token", { session: path, liveOnly: false,
      resume: { sinceSeq: 1_000_000, leafEntryId: "stale-leaf" } });
    peers.push(stale);
    assert.equal(stale.welcome?.resyncRequired, true);
    assert.equal((await stale.receive((frame) => frame.type === "gw_snapshot")).type, "gw_snapshot");
    stale.send({ type: "get_state", id: "after-snapshot" });
    assertResponse(await stale.receive(responseFor("after-snapshot")), true);
    assert.equal(pi.starts, 1);
  } finally {
    for (const peer of peers) peer.close();
    await daemon.shutdown();
  }
}));

test("forced delete discards queued prompts before the terminal event and unbinds every client", async () => withSessionRoot("pi-gw-parity-delete-queue-", async (root) => {
  const path = join(root, "--tmp--", "delete-queue.jsonl");
  await writeSessionFile(path);
  const pi = fakePi({ turnDelayMs: 10_000 });
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi", catalogRoots: [root],
    idleTimeoutMs: 60_000, shortGraceMs: 60_000, createRuntime: pi.createRuntime });
  const peers: Array<Awaited<ReturnType<typeof dialGateway>>> = [];
  try {
    await daemon.listen();
    const first = await dialGateway(daemon.addr!.port, "parity-token", { session: path });
    const second = await dialGateway(daemon.addr!.port, "parity-token", { session: path });
    peers.push(first, second);
    first.send({ type: "prompt", id: "active", message: "hold" });
    assertResponse(await first.receive(responseFor("active")), true);
    second.send({ type: "follow_up", id: "queued", message: "must be discarded" });
    const queued = await second.receive(responseFor("queued"));
    assertResponse(queued, true);
    assert.equal((queued.data as WireFrame).queued, true);
    const beforeSeq = (await daemon.sessionActors[0]!.info()).headSeq;

    first.send({ type: "gw_delete_session", id: "delete", force: true });
    const frames = await first.collectUntil(responseFor("delete"));
    assertResponse(frames.at(-1)!, true);
    const emptyQueue = frames.find((frame) => frame.type === "gw_queue" && Number(frame.gw_seq) > beforeSeq &&
      Array.isArray(frame.pending) && frame.pending.length === 0);
    const deleted = frames.find((frame) => frame.type === "gw_session_state" && frame.state === "deleted");
    assert.ok(emptyQueue && deleted, "delete must publish queue clearing and a terminal event");
    assert.ok(Number(emptyQueue.gw_seq) < Number(deleted.gw_seq), "empty queue must precede the deleted state");
    const secondFrames = await second.collectUntil((frame) => frame.type === "gw_session_state" && frame.state === "deleted");
    assert.equal(secondFrames.some((frame) => frame.type === "gw_error"), false,
      "discarded queued prompts are not reported as per-request failures");

    for (const peer of peers) {
      peer.send({ type: "gw_list_sessions", id: "still-usable" });
      assertResponse(await peer.receive(responseFor("still-usable")), true);
      await assert.rejects(peer.receive((frame) => frame.type === "gw_session_state" && frame.state === "deleted", 60), /timed out/);
    }
    await new Promise((resolve) => setTimeout(resolve, 60));
    assert.deepEqual(pi.prompts, ["hold"], "discarded follow-up must never start");
    assert.equal((await daemon.status()).registered, 0);
    assert.equal((await daemon.status()).live, 0);
    assert.equal(daemon.metricSnapshot.get("pi_gateway_sessions_deleted_total"), 1);
  } finally {
    for (const peer of peers) peer.close();
    await daemon.shutdown();
  }
}));

test("forced reload drains queued follow-ups after the replacement runtime is ready", async () => withSessionRoot("pi-gw-parity-reload-queue-", async (root) => {
  const path = join(root, "--tmp--", "reload-queue.jsonl");
  await writeSessionFile(path);
  const pi = fakePi({ turnDelayMs: 100 });
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi", catalogRoots: [root],
    idleTimeoutMs: 60_000, shortGraceMs: 60_000, createRuntime: pi.createRuntime });
  let peer: Awaited<ReturnType<typeof dialGateway>> | undefined;
  try {
    await daemon.listen();
    peer = await dialGateway(daemon.addr!.port, "parity-token", { session: path });
    peer.send({ type: "prompt", id: "active", message: "abort on reload" });
    assertResponse(await peer.receive(responseFor("active")), true);
    peer.send({ type: "follow_up", id: "queued", message: "run after reload" });
    const queued = await peer.receive(responseFor("queued"));
    assertResponse(queued, true);
    assert.equal((queued.data as WireFrame).queued, true);
    peer.send({ type: "gw_reload_session", id: "forced-reload", force: true });
    assertResponse(await peer.receive(responseFor("forced-reload")), true);
    const completion = await peer.collectUntil((frame) => frame.type === "message_end" && messageText(frame) === "echo: run after reload");
    assert.ok(completion.some((frame) => frame.type === "gw_turn" && frame.state === "settled"));
    assert.deepEqual(pi.prompts, ["abort on reload", "run after reload"]);
    assert.equal(pi.starts, 2);
  } finally {
    peer?.close();
    await daemon.shutdown();
  }
}));

test("cross-session reload replies to its requester and preserves target busy refusals", async () => withSessionRoot("pi-gw-parity-cross-reload-", async (root) => {
  const target = join(root, "--tmp--", "target.jsonl");
  const other = join(root, "--tmp--", "other.jsonl");
  await writeSessionFile(target);
  await writeSessionFile(other);
  const pi = fakePi();
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi", catalogRoots: [root],
    idleTimeoutMs: 60_000, shortGraceMs: 60_000, createRuntime: pi.createRuntime });
  const peers: Array<Awaited<ReturnType<typeof dialGateway>>> = [];
  try {
    await daemon.listen();
    const requester = await dialGateway(daemon.addr!.port, "parity-token", { session: other });
    const targetClient = await dialGateway(daemon.addr!.port, "parity-token", { session: target });
    peers.push(requester, targetClient);
    requester.send({ type: "gw_reload_session", id: "cross-force", session: target, force: true });
    assertResponse(await requester.receive(responseFor("cross-force")), true);
    const ready = await targetClient.receive((frame) => frame.type === "gw_session_state" && frame.state === "ready" && frame.reason === "reload");
    assert.equal(ready.state, "ready");

    const secondTargetClient = await dialGateway(daemon.addr!.port, "parity-token", { session: target });
    peers.push(secondTargetClient);
    requester.send({ type: "gw_reload_session", id: "cross-busy", session: target });
    const busy = await requester.receive(responseFor("cross-busy"));
    assert.equal(busy.success, false);
    assert.equal(busy.code, "reload_busy");
  } finally {
    for (const peer of peers) peer.close();
    await daemon.shutdown();
  }
}));

test("operator tokens cannot stop or delete sessions", async () => withSessionRoot("pi-gw-parity-admin-denial-", async (root) => {
  const path = join(root, "sessions", "admin-denial.jsonl");
  await writeSessionFile(path);
  const pi = fakePi();
  const daemon = new PiGatewayDaemon({ token: "admin-token", tokens: [
    { name: "operator", token: "operator-token", capabilities: ["observe", "interject", "prompt", "ui", "control"] },
  ], addr: "127.0.0.1:0", piBin: "/missing/pi", catalogRoots: [root], createRuntime: pi.createRuntime,
    idleTimeoutMs: 60_000, shortGraceMs: 60_000 });
  const peers: Array<Awaited<ReturnType<typeof dialGateway>>> = [];
  try {
    await daemon.listen();
    const owner = await dialGateway(daemon.addr!.port, "admin-token", { session: path });
    const operator = await dialGateway(daemon.addr!.port, "operator-token", { session: path });
    peers.push(owner, operator);
    operator.send({ type: "gw_stop_session", id: "denied-stop", session: path, force: true });
    assertResponse(await operator.receive(responseFor("denied-stop")), false, "forbidden");
    operator.send({ type: "gw_delete_session", id: "denied-delete", session: path, force: true });
    assertResponse(await operator.receive(responseFor("denied-delete")), false, "forbidden");
    assert.equal(daemon.sessionActors[0]?.state, "ready");
    assert.equal(pi.disposals, 0);

    owner.send({ type: "prompt", id: "still-live", message: "session survived denied controls" });
    assertResponse(await owner.receive(responseFor("still-live")), true);
    await owner.receive((frame) => frame.type === "agent_settled");
    assert.deepEqual(pi.prompts, ["session survived denied controls"]);
    assert.equal(pi.starts, 1);
    assert.equal(pi.disposals, 0);
  } finally {
    for (const peer of peers) peer.close();
    await daemon.shutdown();
  }
}));

test("gw_new_session validates piArgs and persists request tags", async () => withSessionRoot("pi-gw-parity-new-session-tags-", async (root) => {
  const pi = fakePi({ sessionName: "tagged-session" });
  const fakePiBin = join(root, "fake-pi");
  await writeFile(fakePiBin, "#!/bin/sh\nprintf 'fakepi 0.1.0\\n'\n", { mode: 0o700 });
  const spawnDir = join(root, "state", "spawn");
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: fakePiBin,
    catalogRoots: [root], spawnDir, createRuntime: pi.createRuntime, idleTimeoutMs: 60_000, shortGraceMs: 60_000 });
  let peer: Awaited<ReturnType<typeof dialGateway>> | undefined;
  try {
    await daemon.listen();
    peer = await dialGateway(daemon.addr!.port, "parity-token");
    const welcome = peer.welcome!;
    assert.equal(welcome.concurrency, "queue");
    assert.equal(welcome.piVersion, "fakepi 0.1.0");
    assert.ok(Array.isArray(welcome.granted) && welcome.granted.length > 0);
    assert.deepEqual(welcome.turn, { state: "idle", queued: 0 });
    peer.send({ type: "gw_new_session", id: "bad-args", piArgs: "--model test/model" });
    assertResponse(await peer.receive(responseFor("bad-args")), false, "bad_frame");
    peer.send({ type: "gw_new_session", id: "bad-tags", tags: { team: 7 } });
    assertResponse(await peer.receive(responseFor("bad-tags")), false, "bad_frame");
    peer.send({ type: "gw_new_session", id: "bad-cwd", cwd: 7 });
    assertResponse(await peer.receive(responseFor("bad-cwd")), false, "bad_frame");
    assert.equal(pi.starts, 0, "invalid requests must not create a runtime");

    peer.send({ type: "gw_new_session", id: "tagged", name: "tagged-session", tags: { team: "ops", source: "test", empty: null } });
    const created = await peer.receive(responseFor("tagged"));
    assertResponse(created, true);
    const data = created.data as WireFrame;
    const path = String(data.path);
    assert.equal(data.name, "tagged-session");
    assert.equal(data.sessionId, "fake-session-1");
    assert.equal(pi.cwds[0], process.cwd(), "omitted cwd defaults to the connection working directory");
    peer.send({ type: "get_state", id: "created-session-state" });
    const boundState = await peer.receive(responseFor("created-session-state"));
    assert.equal((boundState.data as WireFrame).sessionFile, path, "gw_new_session binds its requester to the created actor");
    const row = (await daemon.listSessions()).find((session) => session.path === path);
    assert.deepEqual(row?.createdBy?.tags, { team: "ops", source: "test", empty: "" });
    assert.equal(row?.createdBy?.clientId, peer.welcome?.clientId);

    assert.deepEqual(await readdir(spawnDir), [`${createHash("sha256").update(path).digest("hex")}.json`],
      "the request creates exactly one spawn sidecar keyed by its reported session path");
    const saved = JSON.parse(await readFile(join(spawnDir, `${createHash("sha256").update(path).digest("hex")}.json`), "utf8")) as WireFrame;
    assert.equal(saved.path, path);
    assert.deepEqual((saved.createdBy as WireFrame).tags, { team: "ops", source: "test", empty: "" });
    peer.send({ type: "prompt", id: "fresh-turn", message: "fresh session envelope" });
    assertResponse(await peer.receive(responseFor("fresh-turn")), true);
    const freshEvents = await peer.collectUntil((frame) => frame.type === "agent_settled");
    assertSessionEventSequences(freshEvents, path, "fresh session");
    assert.ok(freshEvents.some((frame) => frame.type === "message_end" && messageText(frame) === "echo: fresh session envelope"));
    assert.equal(pi.starts, 1);
    assert.deepEqual(pi.argsSeen[0]?.slice(-2), ["--name", "tagged-session"]);
  } finally {
    peer?.close();
    await daemon.shutdown();
  }
}));

test("gw_new_session passes explicit cwd to the new Pi runtime", async () => withSessionRoot("pi-gw-parity-cwd-", async (root) => {
  const pi = fakePi();
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi",
    createRuntime: pi.createRuntime, idleTimeoutMs: 60_000, shortGraceMs: 60_000 });
  let peer: Awaited<ReturnType<typeof dialGateway>> | undefined;
  try {
    await daemon.listen();
    peer = await dialGateway(daemon.addr!.port, "parity-token");
    peer.send({ type: "gw_new_session", id: "cwd-override", cwd: root });
    assertResponse(await peer.receive(responseFor("cwd-override")), true);
    assert.equal(pi.cwds[0], root, "explicit cwd controls the new runtime working directory");
  } finally {
    peer?.close();
    await daemon.shutdown();
  }
}));

test("restricted prompt and interject scopes remain separate and hide non-owned transcripts", async () => withSessionRoot("pi-gw-parity-scopes-", async (root) => {
  const path = join(root, "sessions", "scoped.jsonl");
  await writeSessionFile(path);
  const pi = fakePi({ turnDelayMs: 100 });
  const daemon = new PiGatewayDaemon({ token: "primary", tokens: [
    { name: "prompt", token: "prompt-only", capabilities: ["prompt"] },
    { name: "interject", token: "interject-only", capabilities: ["interject"] },
    { name: "observer", token: "observe-only", capabilities: ["observe"] },
  ], addr: "127.0.0.1:0", piBin: "/missing/pi", createRuntime: pi.createRuntime });
  const peers: Array<Awaited<ReturnType<typeof dialGateway>>> = [];
  try {
    await daemon.listen();
    const owner = await dialGateway(daemon.addr!.port, "primary", { session: path });
    peers.push(owner);
    const prompt = await dialGateway(daemon.addr!.port, "prompt-only", { session: path });
    const otherPrompt = await dialGateway(daemon.addr!.port, "prompt-only", { session: path });
    const interject = await dialGateway(daemon.addr!.port, "interject-only", { session: path });
    const observer = await dialGateway(daemon.addr!.port, "observe-only", { session: path });
    peers.push(prompt, otherPrompt, interject, observer);

    prompt.send({ type: "get_state", id: "prompt-cannot-observe" });
    assertResponse(await prompt.receive(responseFor("prompt-cannot-observe")), false, "forbidden");
    observer.send({ type: "prompt", id: "observer-cannot-prompt", message: "denied" });
    assertResponse(await observer.receive(responseFor("observer-cannot-prompt")), false, "forbidden");
    interject.send({ type: "prompt", id: "interject-cannot-prompt", message: "denied" });
    assertResponse(await interject.receive(responseFor("interject-cannot-prompt")), false, "forbidden");

    for (const restricted of [prompt, otherPrompt, interject]) {
      restricted.send({ type: "gw_ping" });
      await restricted.collectUntil((frame) => frame.type === "gw_pong");
    }
    const privateTranscriptTypes = new Set(["message_start", "message_update", "message_end", "turn_start", "turn_end",
      "agent_start", "agent_end", "agent_settled", "gw_turn", "gw_queue", "gw_presence"]);
    owner.send({ type: "prompt", id: "owner-secret", message: "secret transcript" });
    assertResponse(await owner.receive(responseFor("owner-secret")), true);
    interject.send({ type: "steer", id: "steer-secret", message: "steer the owner's turn" });
    assertResponse(await interject.receive(responseFor("steer-secret")), true);
    await owner.receive((frame) => frame.type === "agent_settled");
    await observer.receive((frame) => frame.type === "agent_settled");
    for (const restricted of [prompt, otherPrompt, interject]) {
      restricted.send({ type: "gw_ping" });
      const frames = await restricted.collectUntil((frame) => frame.type === "gw_pong");
      assert.deepEqual(frames.filter((frame) => privateTranscriptTypes.has(String(frame.type))), [],
        "non-observe clients must not receive another client's transcript, turn, queue, or presence events");
    }
    prompt.send({ type: "prompt", id: "own-prompt", message: "private answer" });
    assertResponse(await prompt.receive(responseFor("own-prompt")), true);
    await waitUntil(() => pi.prompts.includes("secret transcript") && pi.prompts.includes("private answer"));
    assert.equal(pi.prompts.length, 2);
    assert.deepEqual(pi.steers, ["steer the owner's turn"]);
  } finally {
    for (const peer of peers) peer.close();
    await daemon.shutdown();
  }
}));

test("extension dialogs route to a UI turn author and preserve the Pi request ID", async () => withSessionRoot("pi-gw-parity-ui-owner-", async (root) => {
  const path = join(root, "--tmp--", "ui-owner.jsonl");
  await writeSessionFile(path);
  const pi = fakePi({ uiOnPrompt: true, turnDelayMs: 5 });
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi", catalogRoots: [root],
    idleTimeoutMs: 60_000, shortGraceMs: 60_000, createRuntime: pi.createRuntime });
  const peers: Array<Awaited<ReturnType<typeof dialGateway>>> = [];
  try {
    await daemon.listen();
    const author = await dialGateway(daemon.addr!.port, "parity-token", { session: path,
      client: { name: "turn-author", kind: "test", capabilities: ["observe", "prompt", "ui"] } });
    const observer = await dialGateway(daemon.addr!.port, "parity-token", { session: path,
      client: { name: "observer", kind: "test", capabilities: ["observe", "prompt"] } });
    peers.push(author, observer);
    author.send({ type: "prompt", id: "ui-turn", message: "ask for a choice" });
    assertResponse(await author.receive(responseFor("ui-turn")), true);
    const dialog = await author.receive((frame) => frame.type === "extension_ui_request" && frame.method === "select");
    assert.ok(typeof dialog.id === "string" && dialog.id.length > 0);
    await assert.rejects(observer.receive((frame) => frame.type === "extension_ui_request", 80), /timed out/);
    author.send({ type: "extension_ui_response", id: dialog.id, value: "two" });
    await waitUntil(() => pi.uiSelections.includes("two"));
    await author.receive((frame) => frame.type === "agent_settled");
    assert.deepEqual(pi.uiSelections, ["two"]);
  } finally {
    for (const peer of peers) peer.close();
    await daemon.shutdown();
  }
}));

test("extension dialogs route to an available UI client when the turn author lacks UI", async () => withSessionRoot("pi-gw-parity-ui-fallback-", async (root) => {
  const path = join(root, "--tmp--", "ui-fallback.jsonl");
  await writeSessionFile(path);
  const pi = fakePi({ uiOnPrompt: true, turnDelayMs: 5 });
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi", catalogRoots: [root],
    idleTimeoutMs: 60_000, shortGraceMs: 60_000, createRuntime: pi.createRuntime });
  const peers: Array<Awaited<ReturnType<typeof dialGateway>>> = [];
  try {
    await daemon.listen();
    const author = await dialGateway(daemon.addr!.port, "parity-token", { session: path,
      client: { name: "non-ui-author", kind: "test", capabilities: ["observe", "prompt"] } });
    const ui = await dialGateway(daemon.addr!.port, "parity-token", { session: path,
      client: { name: "ui-client", kind: "test", capabilities: ["observe", "ui"] } });
    peers.push(author, ui);
    author.send({ type: "prompt", id: "fallback-turn", message: "ask another client" });
    assertResponse(await author.receive(responseFor("fallback-turn")), true);
    const dialog = await ui.receive((frame) => frame.type === "extension_ui_request" && frame.method === "select");
    await assert.rejects(author.receive((frame) => frame.type === "extension_ui_request", 80), /timed out/);
    ui.send({ type: "extension_ui_response", id: dialog.id, value: "one" });
    await waitUntil(() => pi.uiSelections.includes("one"));
    assert.deepEqual(pi.uiSelections, ["one"]);
  } finally {
    for (const peer of peers) peer.close();
    await daemon.shutdown();
  }
}));

test("session catalog live and cwd filters match the on-disk catalog", async () => withSessionRoot("pi-gw-parity-catalog-", async (root) => {
  const fileOnly = join(root, "--home-u-proj--", "dormant.jsonl");
  await writeSessionFile(fileOnly, "/home/u/proj");
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi", catalogRoots: [root],
    createRuntime: fakePi().createRuntime });
  let peer: Awaited<ReturnType<typeof dialGateway>> | undefined;
  try {
    await daemon.listen();
    peer = await dialGateway(daemon.addr!.port, "parity-token");
    peer.send({ type: "get_state", id: "create-live" });
    const state = await peer.receive(responseFor("create-live"));
    assertResponse(state, true);
    const livePath = String((state.data as WireFrame).sessionFile);
    peer.send({ type: "gw_list_sessions", id: "all" });
    const all = await peer.receive(responseFor("all"));
    const rows = (all.data as WireFrame).sessions as WireFrame[];
    assert.ok(rows.some((row) => row.path === fileOnly && row.live === false));
    assert.ok(rows.some((row) => row.path === livePath && row.live === true));
    peer.send({ type: "gw_list_sessions", id: "live-only", filter: { live: true } });
    const liveOnly = await peer.receive(responseFor("live-only"));
    const liveRows = (liveOnly.data as WireFrame).sessions as WireFrame[];
    assert.ok(liveRows.some((row) => row.path === livePath));
    assert.ok(!liveRows.some((row) => row.path === fileOnly));
    peer.send({ type: "gw_list_sessions", id: "by-cwd", filter: { cwd: "/home/u/proj" } });
    const byCwd = await peer.receive(responseFor("by-cwd"));
    assert.ok(((byCwd.data as WireFrame).sessions as WireFrame[]).some((row) => row.path === fileOnly));
    const malformed = [
      { filter: [], message: "filter must be an object" },
      { filter: { live: "true" }, message: "filter.live must be a boolean" },
      { filter: { cwd: 7 }, message: "filter.cwd must be a string" },
      { filter: { limit: 1.5 }, message: "filter.limit must be an integer" },
    ];
    for (const [index, item] of malformed.entries()) {
      const id = `bad-filter-${index}`;
      peer.send({ type: "gw_list_sessions", id, filter: item.filter });
      const response = await peer.receive(responseFor(id));
      assertResponse(response, false, "bad_frame");
      assert.equal(response.error, item.message);
    }
  } finally {
    peer?.close();
    await daemon.shutdown();
  }
}));

test("catalog listing remains race-free while sessions are being created", async () => withSessionRoot("pi-gw-parity-catalog-race-", async (root) => {
  const paths = Array.from({ length: 8 }, (_, index) => join(root, "--tmp--", `catalog-${index}.jsonl`));
  const pi = fakePi();
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi", catalogRoots: [root],
    idleTimeoutMs: 60_000, shortGraceMs: 60_000, createRuntime: pi.createRuntime });
  try {
    await daemon.listen();
    const creating = paths.map(async (path) => {
      await writeSessionFile(path);
      return daemon.createActor(path, parsePiArgs([]), "/tmp");
    });
    const scanning = Array.from({ length: 8 }, async () => {
      for (let i = 0; i < 4; i++) {
        const rows = await daemon.listSessions();
        assert.ok(rows.length <= paths.length);
      }
    });
    await Promise.all([...creating, ...scanning]);
    const rows = await daemon.listSessions();
    assert.deepEqual(new Set(rows.map((row) => row.path)), new Set(paths));
    assert.equal(rows.length, paths.length);
  } finally { await daemon.shutdown(); }
}));

test("concurrent attach and delete never resurrect a deleted session", async () => withSessionRoot("pi-gw-parity-attach-delete-", async (root) => {
  const path = join(root, "--tmp--", "attach-delete.jsonl");
  await writeSessionFile(path);
  const pi = fakePi({ turnDelayMs: 5 });
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi", catalogRoots: [root],
    idleTimeoutMs: 60_000, shortGraceMs: 60_000, createRuntime: pi.createRuntime });
  const peers: Array<Awaited<ReturnType<typeof dialGateway>> | WirePeer> = [];
  try {
    await daemon.listen();
    const owner = await dialGateway(daemon.addr!.port, "parity-token", { session: path });
    peers.push(owner);
    const racers = await Promise.all(Array.from({ length: 4 }, async (_, index) => {
      const socket = connect({ host: "127.0.0.1", port: daemon.addr!.port });
      await new Promise<void>((resolve, reject) => { socket.once("connect", resolve); socket.once("error", reject); });
      const peer = new WirePeer(socket);
      peers.push(peer);
      peer.send({ type: "gw_hello", protocol: 1, token: "parity-token", session: path,
        client: { name: `racer-${index}`, kind: "test", capabilities: ["observe", "prompt", "interject", "ui", "control", "admin"] } });
      return peer;
    }));
    owner.send({ type: "gw_delete_session", id: "delete-race" });
    assertResponse(await owner.receive(responseFor("delete-race")), true);
    for (const racer of racers) {
      const first = await racer.receive((frame) => frame.type === "gw_welcome" || frame.type === "gw_error");
      if (first.type === "gw_welcome" && (first.session as WireFrame | undefined)?.path === path) {
        assert.equal((await racer.receive((frame) => frame.type === "gw_session_state" && frame.state === "deleted")).state, "deleted");
      } else {
        const error = first.type === "gw_error" ? first : await racer.receive((frame) => frame.type === "gw_error");
        assert.equal(error.code, "unknown_session");
      }
    }
    assert.equal(daemon.sessionActors.length, 0);
    assert.equal(pi.starts, 1);
  } finally {
    for (const peer of peers) peer.close();
    await daemon.shutdown();
  }
}));

test("cold hello attach losing a delete race sends an unbound welcome then unknown_session", async () => withSessionRoot("pi-gw-parity-cold-attach-delete-", async (root) => {
  const path = join(root, "--tmp--", "cold-attach-delete.jsonl");
  await writeSessionFile(path);
  const pi = fakePi();
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi", catalogRoots: [root],
    idleTimeoutMs: 60_000, shortGraceMs: 60_000, createRuntime: pi.createRuntime });
  let signalAttach!: () => void;
  const attachReady = new Promise<void>((resolve) => { signalAttach = resolve; });
  let releaseAttach!: () => void;
  const attachGate = new Promise<void>((resolve) => { releaseAttach = resolve; });
  let racer: WirePeer | undefined;
  let admin: Awaited<ReturnType<typeof dialGateway>> | undefined;
  const attach = daemon.attach.bind(daemon);
  daemon.attach = async (target, spec, cwd, by) => {
    const actor = await attach(target, spec, cwd, by);
    if (target === path) { signalAttach(); await attachGate; }
    return actor;
  };
  try {
    await daemon.listen();
    const socket = connect({ host: "127.0.0.1", port: daemon.addr!.port });
    await new Promise<void>((resolve, reject) => { socket.once("connect", resolve); socket.once("error", reject); });
    racer = new WirePeer(socket);
    racer.send({ type: "gw_hello", protocol: 1, token: "parity-token", session: path,
      client: { name: "cold-racer", kind: "test", capabilities: ["observe", "prompt", "interject", "ui", "control", "admin"] } });
    await attachReady;

    admin = await dialGateway(daemon.addr!.port, "parity-token");
    admin.send({ type: "gw_delete_session", id: "delete-cold-race", session: path });
    assertResponse(await admin.receive(responseFor("delete-cold-race")), true);
    releaseAttach();

    const welcome = await racer.receive((frame) => frame.type === "gw_welcome");
    assert.equal(welcome.session, null, "a losing attach must not bind the connection");
    const error = await racer.receive((frame) => frame.type === "gw_error");
    assert.equal(error.code, "unknown_session");
    assert.equal(pi.starts, 1);
    assert.equal(daemon.sessionActors.length, 0);
  } finally {
    releaseAttach();
    racer?.close();
    admin?.close();
    await daemon.shutdown();
  }
}));

test("gw_new_session clears deleted-unbound state before a later stop and lazy create", async () => withSessionRoot("pi-gw-parity-deleted-unbound-", async (root) => {
  const pi = fakePi();
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi", catalogRoots: [root],
    idleTimeoutMs: 60_000, shortGraceMs: 60_000, createRuntime: pi.createRuntime });
  let peer: Awaited<ReturnType<typeof dialGateway>> | undefined;
  try {
    await daemon.listen();
    peer = await dialGateway(daemon.addr!.port, "parity-token");
    peer.send({ type: "gw_new_session", id: "first-new" });
    const first = await peer.receive(responseFor("first-new"));
    assertResponse(first, true);
    const deletedPath = String((first.data as WireFrame).path);
    peer.send({ type: "gw_delete_session", id: "delete-first", session: deletedPath });
    assertResponse(await peer.receive(responseFor("delete-first")), true);

    peer.send({ type: "gw_new_session", id: "second-new" });
    assertResponse(await peer.receive(responseFor("second-new")), true);
    peer.send({ type: "gw_stop_session", id: "stop-second" });
    assertResponse(await peer.receive(responseFor("stop-second")), true);
    peer.send({ type: "get_state", id: "lazy-after-stop" });
    const created = await peer.receive(responseFor("lazy-after-stop"));
    assertResponse(created, true);
    assert.notEqual((created.data as WireFrame).sessionFile, deletedPath);
    assert.equal(pi.starts, 3);
  } finally { peer?.close(); await daemon.shutdown(); }
}));

test("no-session actors delete cleanly and report an empty sessionFile", async () => withSessionRoot("pi-gw-parity-no-session-", async (root) => {
  const pi = fakePi({ sessionFile: "" });
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi", catalogRoots: [root],
    idleTimeoutMs: 60_000, shortGraceMs: 60_000, createRuntime: pi.createRuntime });
  let peer: Awaited<ReturnType<typeof dialGateway>> | undefined;
  try {
    await daemon.listen();
    peer = await dialGateway(daemon.addr!.port, "parity-token", { piArgs: ["--no-session"] });
    peer.send({ type: "get_state", id: "no-session-state" });
    const state = await peer.receive(responseFor("no-session-state"));
    assertResponse(state, true);
    assert.equal((state.data as WireFrame).sessionFile, "");
    peer.send({ type: "gw_delete_session", id: "delete-no-session" });
    const deleted = await peer.receive(responseFor("delete-no-session"));
    assertResponse(deleted, true);
    assert.equal((deleted.data as WireFrame).piStopped, true);
    assert.equal((deleted.data as WireFrame).fileDeleted, false);
    assert.equal(daemon.sessionActors.length, 0);
  } finally {
    peer?.close();
    await daemon.shutdown();
  }
}));

test("gw_ping and gw_bye leave an existing shared session available", async () => withSessionRoot("pi-gw-parity-bye-", async (root) => {
  const path = join(root, "sessions", "bye.jsonl");
  await writeSessionFile(path);
  const pi = fakePi();
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi",
    catalogRoots: [root], createRuntime: pi.createRuntime, idleTimeoutMs: 60_000, shortGraceMs: 60_000 });
  const peers: Array<Awaited<ReturnType<typeof dialGateway>>> = [];
  try {
    await daemon.listen();
    const keeper = await dialGateway(daemon.addr!.port, "parity-token", { session: path });
    const bye = await dialGateway(daemon.addr!.port, "parity-token", { session: path });
    peers.push(keeper, bye);
    bye.send({ type: "gw_ping", id: "ping" });
    assert.equal((await bye.receive((frame) => frame.type === "gw_pong")).type, "gw_pong");
    bye.send({ type: "gw_bye", id: "bye" });
    await new Promise<void>((resolve) => bye.socket.once("close", () => resolve()));
    await waitUntil(async () => (await daemon.status()).connections === 1);
    assert.equal((await daemon.status()).registered, 1);
    keeper.send({ type: "prompt", id: "still-running", message: "after bye" });
    assertResponse(await keeper.receive(responseFor("still-running")), true);
    await keeper.receive((frame) => frame.type === "agent_settled");
    assert.deepEqual(pi.prompts, ["after bye"]);
    assert.equal(pi.starts, 1);
  } finally {
    for (const peer of peers) peer.close();
    await daemon.shutdown();
  }
}));

test("clone is refused for shared sessions and rekeys the live actor when sole client", async () => withSessionRoot("pi-gw-parity-clone-", async (root) => {
  const path = join(root, "--tmp--", "clone.jsonl");
  await writeSessionFile(path);
  const pi = fakePi();
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi", catalogRoots: [root],
    idleTimeoutMs: 60_000, shortGraceMs: 60_000, createRuntime: pi.createRuntime });
  const peers: Array<Awaited<ReturnType<typeof dialGateway>>> = [];
  try {
    await daemon.listen();
    const owner = await dialGateway(daemon.addr!.port, "parity-token", { session: path });
    const other = await dialGateway(daemon.addr!.port, "parity-token", { session: path });
    peers.push(owner, other);
    owner.send({ type: "clone", id: "clone-shared" });
    const refused = await owner.receive(responseFor("clone-shared"));
    assert.equal(refused.success, false);
    assert.equal(refused.code, "shared_session");

    other.close();
    await owner.receive((frame) => frame.type === "gw_presence" && frame.event === "leave");
    owner.send({ type: "clone", id: "clone-alone" });
    assertResponse(await owner.receive(responseFor("clone-alone")), true);
    await waitUntil(() => daemon.sessionActors.some((actor) => actor.path !== path));
    const newPath = daemon.sessionActors.find((actor) => actor.path !== path)!.path;
    assert.ok(newPath && newPath !== path);
    const rows = await daemon.listSessions();
    assert.equal(rows.find((row) => row.path === path)?.live, false);
    assert.equal(rows.find((row) => row.path === newPath)?.live, true);

    const reattached = await dialGateway(daemon.addr!.port, "parity-token", { session: newPath });
    peers.push(reattached);
    assert.equal((reattached.welcome?.session as WireFrame).path, newPath);
    assert.notEqual((reattached.welcome?.session as WireFrame).id, newPath);
    assert.equal(pi.starts, 1, "the new path must resolve to the existing actor");
  } finally {
    for (const peer of peers) peer.close();
    await daemon.shutdown();
  }
}));

test("new_session rebinds only its requester and unknown switches fail without creating a session", async () => withSessionRoot("pi-gw-parity-control-", async (root) => {
  const path = join(root, "--tmp--", "original.jsonl");
  await writeSessionFile(path);
  const pi = fakePi();
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi", catalogRoots: [root],
    idleTimeoutMs: 60_000, shortGraceMs: 60_000, createRuntime: pi.createRuntime });
  const peers: Array<Awaited<ReturnType<typeof dialGateway>>> = [];
  try {
    await daemon.listen();
    const a = await dialGateway(daemon.addr!.port, "parity-token", { session: path });
    const b = await dialGateway(daemon.addr!.port, "parity-token", { session: path });
    peers.push(a, b);
    a.send({ type: "new_session", id: "new" });
    const created = await a.receive(responseFor("new"));
    assertResponse(created, true);
    assert.equal((created.data as WireFrame).cancelled, false);
    a.send({ type: "get_state", id: "a-state" });
    b.send({ type: "get_state", id: "b-state" });
    const stateA = await a.receive(responseFor("a-state"));
    const stateB = await b.receive(responseFor("b-state"));
    assert.notEqual((stateA.data as WireFrame).sessionFile, path);
    assert.equal((stateB.data as WireFrame).sessionFile, path);
    b.send({ type: "switch_session", id: "switch-path", sessionPath: path });
    const switched = await b.receive(responseFor("switch-path"));
    assertResponse(switched, true);
    assert.equal((switched.data as WireFrame).cancelled, false);
    a.send({ type: "switch_session", id: "switch-alias", session: path });
    const alias = await a.receive(responseFor("switch-alias"));
    assertResponse(alias, true);
    assert.equal((alias.data as WireFrame).cancelled, false);
    b.send({ type: "switch_session", id: "unknown", sessionPath: "nonexistent-session-name" });
    const unknown = await b.receive(responseFor("unknown"));
    assertResponse(unknown, false, "unknown_session");
    assert.equal(daemon.metricSnapshot.get("pi_gateway_attach_failures_total"), 1);
    assert.equal(pi.starts, 2);
  } finally {
    for (const peer of peers) peer.close();
    await daemon.shutdown();
  }
}));

test("outbound writes close slow clients at the bounded queue limit", async () => withSessionRoot("pi-gw-parity-outbound-limit-", async (root) => {
  const path = join(root, "--tmp--", "slow-client.jsonl");
  await writeSessionFile(path);
  const daemon = new PiGatewayDaemon({ token: "parity-token", addr: "127.0.0.1:0", piBin: "/missing/pi", catalogRoots: [root],
    idleTimeoutMs: 60_000, shortGraceMs: 60_000, createRuntime: fakePi().createRuntime });
  let peer: Awaited<ReturnType<typeof dialGateway>> | undefined;
  try {
    await daemon.listen();
    peer = await dialGateway(daemon.addr!.port, "parity-token", { session: path });
    type TestConnection = { socket: Socket; sendJSON(value: unknown): Promise<void>; pendingWrites: number; closed: boolean };
    const connections = (daemon as unknown as { connections: Set<TestConnection> }).connections;
    const connection = [...connections][0];
    assert.ok(connection);
    const socket = connection.socket as unknown as {
      write(chunk: Uint8Array, callback: (error?: Error | null) => void): boolean;
      once(event: "close", listener: () => void): unknown;
      destroyed: boolean;
    };
    const originalWrite = socket.write.bind(socket);
    let socketWriteCalls = 0;
    let releaseStalledWrite: ((error?: Error | null) => void) | undefined;
    socket.once("close", () => releaseStalledWrite?.(new Error("test socket closed")));
    socket.write = (chunk, callback) => {
      socketWriteCalls += 1;
      if (socketWriteCalls === 1) {
        releaseStalledWrite = callback;
        return false;
      }
      return originalWrite(chunk, callback);
    };

    const firstWrite = connection.sendJSON({ type: "test", index: 0 }).catch(() => {});
    await Promise.resolve();
    assert.equal(socketWriteCalls, 1, "the first frame should be stalled in the socket");
    const writes = Array.from({ length: 1024 }, (_, index) =>
      connection.sendJSON({ type: "test", index: index + 1 }).catch(() => {}));
    assert.equal(connection.pendingWrites, 1024);
    assert.equal(connection.closed, true);
    assert.equal(socketWriteCalls, 1, "the slow-consumer diagnostic must not bypass the bounded write queue");
    await Promise.all([firstWrite, ...writes]);
    assert.equal(connection.pendingWrites, 0);
    assert.equal(socket.destroyed, true);
  } finally {
    peer?.close();
    await daemon.shutdown();
  }
}));
