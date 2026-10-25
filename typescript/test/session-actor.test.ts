import assert from "node:assert/strict";
import { test } from "node:test";
import type { ClientRef, ClientSummary, ProtocolRecord } from "../src/protocol.ts";
import { SessionActor, type ActorClient } from "../src/session/actor.ts";
import type { RpcObject, RpcSession, RpcSessionRuntime } from "../src/rpc-adapter.ts";

function fakeRuntime(turnDelayMs = 10): RpcSessionRuntime {
  const listeners = new Set<(event: RpcObject) => void>();
  let leaf = "leaf";
  const model = { provider: "test", id: "model" };
  const emit = (event: RpcObject) => { for (const listener of listeners) listener(event); };
  const session: RpcSession & { emitForTest(event: RpcObject): void } = {
    model,
    thinkingLevel: "medium",
    isStreaming: false,
    isCompacting: false,
    steeringMode: "one-at-a-time",
    followUpMode: "one-at-a-time",
    sessionFile: "/tmp/session.jsonl",
    sessionId: "test-session",
    sessionName: undefined,
    autoCompactionEnabled: true,
    pendingMessageCount: 0,
    messages: [],
    promptTemplates: [],
    modelRuntime: { getAvailableSnapshot: () => [model] },
    sessionManager: {
      getCwd: () => "/tmp",
      getLeafId: () => leaf,
      getEntries: () => [{ id: leaf }],
      getTree: () => [{ id: leaf }],
    },
    extensionRunner: { emitUserBash: async () => undefined, getRegisteredCommands: () => [] },
    resourceLoader: { getSkills: () => ({ skills: [] }) },
    subscribe(listener) { listeners.add(listener); return () => listeners.delete(listener); },
    emitForTest(event) { emit(event); },
    async bindExtensions() {},
    async waitForIdle() {},
    async navigateTree() { return { cancelled: false }; },
    async reload() {},
    async prompt(message, options) {
      const preflight = options?.preflightResult;
      if (typeof preflight === "function") preflight("started");
      emit({ type: "agent_start" });
      setTimeout(() => {
        emit({ type: "message_start", message: { role: "assistant" } });
        emit({ type: "message_end", message: { role: "assistant", content: [{ type: "text", text: `echo: ${message}` }] } });
        emit({ type: "agent_settled" });
      }, turnDelayMs);
    },
    async steer() { return "queued"; },
    async followUp() { return "queued"; },
    async abort() { emit({ type: "agent_settled" }); },
    clearQueue() { return { steering: [], followUp: [] }; },
    async setModel() {},
    async cycleModel() { return model; },
    setThinkingLevel() {},
    cycleThinkingLevel() { return "high"; },
    getAvailableThinkingLevels() { return ["off", "low", "medium", "high"]; },
    setSteeringMode() {},
    setFollowUpMode() {},
    async compact() { return {}; },
    setAutoCompactionEnabled() {},
    setAutoRetryEnabled() {},
    abortRetry() {},
    abortBash() {},
    async executeBash() { return {}; },
    recordBashResult() {},
    getSessionStats() { return {}; },
    async exportToHtml() { return "/tmp/session.html"; },
    getUserMessagesForForking() { return []; },
    getLastAssistantText() { return ""; },
    setSessionName() {},
  };
  return {
    session,
    async newSession() { return { cancelled: false }; },
    async switchSession() { return { cancelled: false }; },
    async fork(entryId) { leaf = entryId; return { cancelled: false, selectedText: "" }; },
    async dispose() {},
    setRebindSession() {},
  };
}

function client(id: string): ActorClient {
  return {
    id,
    kind: "test",
    name: id,
    has: (capability) => capability === "observe" || capability === "prompt" || capability === "ui",
    ref: (): ClientRef => ({ clientId: id, kind: "test", name: id }),
    summary: (): ClientSummary => ({ clientId: id, kind: "test", name: id, capabilities: ["observe", "prompt", "ui"] }),
  };
}

async function until(subscriber: { next(): Promise<ProtocolRecord | undefined> }, predicate: (record: ProtocolRecord) => boolean): Promise<ProtocolRecord[]> {
  const result: ProtocolRecord[] = [];
  const deadline = Date.now() + 3000;
  while (Date.now() < deadline) {
    const remaining = deadline - Date.now();
    const record = await new Promise<ProtocolRecord | undefined>((resolve, reject) => {
      const timer = setTimeout(() => resolve(undefined), remaining);
      subscriber.next().then((value) => { clearTimeout(timer); resolve(value); }, reject);
    });
    if (record === undefined) throw new Error("timed out waiting for actor record");
    result.push(record);
    if (predicate(record)) return result;
  }
  throw new Error("timed out waiting for actor record");
}

test("extension shutdown requests crash the actor after Pi settlement", async () => {
  const runtime = fakeRuntime();
  let shutdownHandler: (() => void) | undefined;
  runtime.session.bindExtensions = async (bindings) => {
    shutdownHandler = bindings.shutdownHandler as (() => void) | undefined;
  };
  const actor = new SessionActor({ cwd: "/tmp", createRuntime: async () => runtime, idleTimeoutMs: 60_000 });
  const author = client("shutdown-owner");
  await actor.start();
  await actor.attachSync(author);
  const subscriber = actor.subscribe(author.id, { buffer: 64 });
  try {
    assert.equal(typeof shutdownHandler, "function");
    shutdownHandler!();
    (runtime.session as RpcSession & { emitForTest(event: RpcObject): void }).emitForTest({ type: "agent_settled" });
    await until(subscriber, (record) => record.type === "gw_session_state" && record.raw.toString().includes('"state":"crashed"'));
    assert.equal(actor.state, "crashed");
  } finally {
    subscriber.close();
    await actor.stop();
  }
});

test("actor queue limits reject an excess follow-up with queue_full", async () => {
  const actor = new SessionActor({ cwd: "/tmp", createRuntime: async () => fakeRuntime(500), idleTimeoutMs: 60_000 });
  const author = client("queue-owner");
  await actor.start();
  await actor.attachSync(author);
  const subscriber = actor.subscribe(author.id, { buffer: 256 });
  try {
    await actor.submit({ client: author, localId: "active", type: "prompt", raw: Buffer.from('{"type":"prompt","id":"active","message":"active"}') });
    for (let index = 0; index < 33; index++) {
      const id = `queued-${index}`;
      await actor.submit({ client: author, localId: id, type: "follow_up", raw: Buffer.from(JSON.stringify({ type: "follow_up", id, message: id })) });
    }
    const rejected = await until(subscriber, (record) => record.type === "response" && record.raw.toString().includes('"code":"queue_full"'));
    assert.match(rejected.at(-1)!.raw.toString(), /queue_full/);
    assert.equal(actor.queue.length, 32);
  } finally {
    subscriber.close();
    await actor.stop();
  }
});

test("a long-running bash handler does not block a later abort_bash", async () => {
  const runtime = fakeRuntime();
  let signalStarted!: () => void;
  let releaseExecute!: (value: RpcObject) => void;
  const started = new Promise<void>((resolve) => { signalStarted = resolve; });
  let aborted = false;
  runtime.session.executeBash = async () => {
    signalStarted();
    return await new Promise<RpcObject>((resolve) => { releaseExecute = resolve; });
  };
  runtime.session.abortBash = () => { aborted = true; releaseExecute?.({ exitCode: 130 }); };
  const actor = new SessionActor({ cwd: "/tmp", createRuntime: async () => runtime, idleTimeoutMs: 60_000 });
  const author = client("bash-owner");
  await actor.start();
  await actor.attachSync(author);
  try {
    await actor.submit({ client: author, localId: "long-bash", type: "bash", raw: Buffer.from('{"type":"bash","id":"long-bash","command":"sleep"}') });
    await started;
    await actor.submit({ client: author, localId: "abort-bash", type: "abort_bash", raw: Buffer.from('{"type":"abort_bash","id":"abort-bash"}') });
    assert.equal(aborted, true);
  } finally {
    releaseExecute?.({ exitCode: 130 });
    await actor.stop();
  }
});

test("non-positive idle durations normalize to Go's one-minute fallback", async () => {
  const actor = new SessionActor({ cwd: "/tmp", createRuntime: async () => fakeRuntime(), idleTimeoutMs: 0, shortGraceMs: 0 });
  await actor.start();
  try {
    await new Promise((resolve) => setTimeout(resolve, 25));
    assert.equal(actor.state, "ready");
  } finally { await actor.stop(); }
});

test("session actor serializes prompts, preserves FIFO attribution, and settles queued turns", async () => {
  const actor = new SessionActor({
    cwd: "/tmp",
    sessionPath: "/tmp/session.jsonl",
    createRuntime: async () => fakeRuntime(15),
    idleTimeoutMs: 60_000,
  });
  await actor.start();
  const author = client("c_1");
  assert.equal(await actor.attachSync(author), true);
  const subscriber = actor.subscribe(author.id, { buffer: 64 });
  try {
    await actor.submit({ client: author, localId: "r1", type: "prompt", raw: Buffer.from('{"type":"prompt","id":"r1","message":"first"}') });
    await actor.submit({ client: author, localId: "r2", type: "prompt", raw: Buffer.from('{"type":"prompt","id":"r2","message":"second"}') });
    const records = await until(subscriber, (record) => record.type === "message_end" && record.raw.toString("utf8").includes("echo: second"));
    const messages = records.filter((record) => record.type === "message_end").map((record) => record.raw.toString("utf8"));
    assert.equal(messages.length, 2);
    assert.match(messages[0]!, /echo: first/);
    assert.match(messages[1]!, /echo: second/);
    assert.ok(records.some((record) => record.type === "response" && record.raw.toString("utf8").includes('"id":"c_1:r1"')));
    assert.ok(records.some((record) => record.type === "response" && record.raw.toString("utf8").includes('"id":"r2"') && record.raw.toString("utf8").includes('"queued":true')));
    assert.equal(actor.turn.state, "idle");
    assert.equal(actor.turn.queued, 0);
  } finally {
    subscriber.close();
    await actor.stop();
  }
});
