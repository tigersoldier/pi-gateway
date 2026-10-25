import assert from "node:assert/strict";
import { test } from "node:test";
import {
  RpcUIBroker,
  SdkRpcAdapter,
  toRpcJsonEvent,
  type RpcEventSink,
  type RpcObject,
  type RpcResponse,
  type RpcSession,
  type RpcSessionRuntime,
} from "../src/rpc-adapter.ts";

const model = { provider: "test", id: "model-1" };

class FakeSession implements RpcSession {
  model = model;
  thinkingLevel = "medium";
  isStreaming = false;
  isCompacting = false;
  steeringMode: "all" | "one-at-a-time" = "one-at-a-time";
  followUpMode: "all" | "one-at-a-time" = "one-at-a-time";
  sessionFile = "/tmp/session.jsonl";
  sessionId = "session-id";
  sessionName = "session name";
  autoCompactionEnabled = true;
  pendingMessageCount = 0;
  messages: RpcObject[] = [];
  promptTemplates = [{ name: "template", sourceInfo: { path: "template.md" } }];
  modelRuntime = { getAvailableSnapshot: () => [model] };
  sessionManager = {
    getCwd: () => "/tmp",
    getLeafId: () => "leaf-1",
    getEntries: () => [{ id: "entry-1" }, { id: "leaf-1" }],
    getTree: () => [{ id: "leaf-1" }],
  };
  extensionRunner = {
    emitUserBash: async () => undefined,
    getRegisteredCommands: () => [{ invocationName: "hello", sourceInfo: { path: "ext.js" } }],
  };
  resourceLoader = { getSkills: () => ({ skills: [{ name: "skill", sourceInfo: { path: "SKILL.md" } }] }) };
  calls: string[] = [];
  bindings?: RpcObject;
  private readonly listeners = new Set<(event: RpcObject) => void>();

  subscribe(listener: (event: RpcObject) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }
  emit(event: RpcObject): void { for (const listener of this.listeners) listener(event); }
  async bindExtensions(bindings: RpcObject): Promise<void> { this.calls.push("bindExtensions"); this.bindings = bindings; }
  async waitForIdle(): Promise<void> {}
  async navigateTree(): Promise<{ cancelled: boolean }> { return { cancelled: false }; }
  async reload(): Promise<void> {}
  async prompt(_message: string, options?: RpcObject): Promise<void> {
    this.calls.push("prompt");
    const preflight = options?.preflightResult;
    if (typeof preflight === "function") preflight("started");
  }
  async steer(): Promise<"queued"> { this.calls.push("steer"); return "queued"; }
  async followUp(): Promise<"queued"> { this.calls.push("follow_up"); return "queued"; }
  async abort(): Promise<void> { this.calls.push("abort"); }
  clearQueue(): { steering: string[]; followUp: string[] } { this.calls.push("clear_queue"); return { steering: ["s"], followUp: ["f"] }; }
  async setModel(): Promise<void> { this.calls.push("set_model"); }
  async cycleModel(): Promise<RpcObject | undefined> { this.calls.push("cycle_model"); return model; }
  setThinkingLevel(): void { this.calls.push("set_thinking_level"); }
  cycleThinkingLevel(): string { this.calls.push("cycle_thinking_level"); return "high"; }
  getAvailableThinkingLevels(): string[] { return ["off", "low", "medium", "high"]; }
  setSteeringMode(): void { this.calls.push("set_steering_mode"); }
  setFollowUpMode(): void { this.calls.push("set_follow_up_mode"); }
  async compact(): Promise<RpcObject> { this.calls.push("compact"); return { compacted: true }; }
  setAutoCompactionEnabled(): void { this.calls.push("set_auto_compaction"); }
  setAutoRetryEnabled(): void { this.calls.push("set_auto_retry"); }
  abortRetry(): void { this.calls.push("abort_retry"); }
  abortBash(): void { this.calls.push("abort_bash"); }
  async executeBash(): Promise<RpcObject> { this.calls.push("executeBash"); return { stdout: "ok" }; }
  recordBashResult(): void { this.calls.push("recordBashResult"); }
  getSessionStats(): RpcObject { return { messageCount: 0 }; }
  async exportToHtml(): Promise<string> { return "/tmp/session.html"; }
  getUserMessagesForForking(): Array<{ entryId: string; text: string }> { return [{ entryId: "entry-1", text: "hello" }]; }
  getLastAssistantText(): string { return "answer"; }
  setSessionName(): void { this.calls.push("set_session_name"); }
  getActiveToolNames(): string[] { return ["read", "bash"]; }
  getCallableToolNames(): string[] { return ["read", "bash"]; }
}

class FakeRuntime implements RpcSessionRuntime {
  session = new FakeSession();
  async newSession(): Promise<{ cancelled: boolean }> { return { cancelled: false }; }
  async switchSession(): Promise<{ cancelled: boolean }> { return { cancelled: false }; }
  async fork(): Promise<{ cancelled: boolean; selectedText?: string }> { return { cancelled: false, selectedText: "selected" }; }
  async dispose(): Promise<void> {}
}

function setup(): { adapter: SdkRpcAdapter; runtime: FakeRuntime; output: RpcObject[] } {
  const output: RpcObject[] = [];
  const runtime = new FakeRuntime();
  const adapter = new SdkRpcAdapter(runtime, (event) => output.push(event));
  return { adapter, runtime, output };
}

function response(output: RpcObject[]): RpcResponse {
  assert.equal(output.length, 1);
  assert.equal(output[0]?.type, "response");
  return output[0] as RpcResponse;
}

test("RPC adapter implements the Pi RPC command surface", async (t) => {
  const cases: Array<{ name: string; command: RpcObject; method?: string }> = [
    { name: "prompt", command: { type: "prompt", id: "1", message: "hello" }, method: "prompt" },
    { name: "steer", command: { type: "steer", id: "1", message: "hello" }, method: "steer" },
    { name: "follow_up", command: { type: "follow_up", id: "1", message: "hello" }, method: "follow_up" },
    { name: "abort", command: { type: "abort", id: "1" }, method: "abort" },
    { name: "clear_queue", command: { type: "clear_queue", id: "1" }, method: "clear_queue" },
    { name: "new_session", command: { type: "new_session", id: "1" } },
    { name: "get_state", command: { type: "get_state", id: "1" } },
    { name: "set_model", command: { type: "set_model", id: "1", provider: "test", modelId: "model-1" }, method: "set_model" },
    { name: "cycle_model", command: { type: "cycle_model", id: "1" }, method: "cycle_model" },
    { name: "get_available_models", command: { type: "get_available_models", id: "1" } },
    { name: "set_thinking_level", command: { type: "set_thinking_level", id: "1", level: "high" }, method: "set_thinking_level" },
    { name: "cycle_thinking_level", command: { type: "cycle_thinking_level", id: "1" }, method: "cycle_thinking_level" },
    { name: "get_available_thinking_levels", command: { type: "get_available_thinking_levels", id: "1" } },
    { name: "set_steering_mode", command: { type: "set_steering_mode", id: "1", mode: "all" }, method: "set_steering_mode" },
    { name: "set_follow_up_mode", command: { type: "set_follow_up_mode", id: "1", mode: "all" }, method: "set_follow_up_mode" },
    { name: "compact", command: { type: "compact", id: "1" }, method: "compact" },
    { name: "set_auto_compaction", command: { type: "set_auto_compaction", id: "1", enabled: false }, method: "set_auto_compaction" },
    { name: "set_auto_retry", command: { type: "set_auto_retry", id: "1", enabled: false }, method: "set_auto_retry" },
    { name: "abort_retry", command: { type: "abort_retry", id: "1" }, method: "abort_retry" },
    { name: "bash", command: { type: "bash", id: "1", command: "echo ok" }, method: "executeBash" },
    { name: "abort_bash", command: { type: "abort_bash", id: "1" }, method: "abort_bash" },
    { name: "get_session_stats", command: { type: "get_session_stats", id: "1" } },
    { name: "export_html", command: { type: "export_html", id: "1" } },
    { name: "switch_session", command: { type: "switch_session", id: "1", sessionPath: "/tmp/other.jsonl" } },
    { name: "fork", command: { type: "fork", id: "1", entryId: "entry-1" } },
    { name: "clone", command: { type: "clone", id: "1" } },
    { name: "get_fork_messages", command: { type: "get_fork_messages", id: "1" } },
    { name: "get_entries", command: { type: "get_entries", id: "1", since: "entry-1" } },
    { name: "get_tree", command: { type: "get_tree", id: "1" } },
    { name: "get_last_assistant_text", command: { type: "get_last_assistant_text", id: "1" } },
    { name: "set_session_name", command: { type: "set_session_name", id: "1", name: "  new name  " }, method: "set_session_name" },
    { name: "get_messages", command: { type: "get_messages", id: "1" } },
    { name: "get_commands", command: { type: "get_commands", id: "1" } },
  ];

  for (const item of cases) {
    await t.test(item.name, async () => {
      const { adapter, runtime, output } = setup();
      await adapter.handle(item.command);
      const result = response(output);
      assert.equal(result.success, true);
      assert.equal(result.command, item.name);
      if (item.method) assert.ok(runtime.session.calls.includes(item.method));
      if (item.name === "get_state") assert.equal((result.data as RpcObject).sessionId, "session-id");
      if (item.name === "clear_queue") assert.deepEqual(result.data, { steering: ["s"], followUp: ["f"] });
      if (item.name === "get_entries") assert.deepEqual((result.data as RpcObject).entries, [{ id: "leaf-1" }]);
      if (item.name === "get_commands") {
        const commands = (result.data as RpcObject).commands as RpcObject[];
        assert.deepEqual(commands.map(({ source, sourceInfo }) => ({ source, sourceInfo })), [
          { source: "extension", sourceInfo: { path: "ext.js" } },
          { source: "prompt", sourceInfo: { path: "template.md" } },
          { source: "skill", sourceInfo: { path: "SKILL.md" } },
        ]);
      }
    });
  }
});

test("RPC adapter turns extension shutdown into runtime failure after agent settlement", async () => {
  const { adapter, runtime, output } = setup();
  await adapter.bind();
  const shutdownHandler = runtime.session.bindings?.shutdownHandler;
  assert.equal(typeof shutdownHandler, "function");
  (shutdownHandler as () => void)();
  assert.equal(output.length, 0, "Pi RPC waits until agent_settled before shutting down");
  runtime.session.emit({ type: "agent_settled" });
  assert.deepEqual(output.map((event) => event.type), ["agent_settled", "gw_runtime_failure"]);
  assert.equal(output[1]?.error, "Pi shut down by extension");
  runtime.session.emit({ type: "agent_settled" });
  assert.equal(output.filter((event) => event.type === "gw_runtime_failure").length, 1, "shutdown is reported once");
  await adapter.dispose();
});

test("RPC prompt errors are returned only before successful preflight", async () => {
  const { adapter, runtime, output } = setup();
  runtime.session.prompt = async () => { throw new Error("preflight failed"); };
  await adapter.handle({ type: "prompt", id: "req", message: "hello" });
  await Promise.resolve();
  assert.equal(response(output).success, false);
  assert.equal(output[0]?.error, "preflight failed");

  const accepted = setup();
  accepted.runtime.session.prompt = async (_message, options) => {
    if (typeof options?.preflightResult === "function") (options.preflightResult as (value: string) => void)("started");
    throw new Error("runtime failed after acceptance");
  };
  await accepted.adapter.handle({ type: "prompt", id: "accepted", message: "hello" });
  await Promise.resolve();
  await Promise.resolve();
  assert.equal(accepted.output[0]?.success, true, "accepted prompt response remains successful");
  assert.equal(accepted.output[1]?.type, "gw_runtime_failure");
  assert.equal(accepted.output[1]?.error, "runtime failed after acceptance");
});

test("RPC adapter reports unknown commands and command errors", async () => {
  const { adapter, output } = setup();
  await adapter.handle({ type: "not_a_command", id: "req" });
  assert.equal(response(output).error, "Unknown command: not_a_command");

  const second = setup();
  await second.adapter.handle({ type: "set_model", id: "req", provider: "missing", modelId: "model" });
  assert.equal(response(second.output).error, "Model not found: missing/model");
});

test("RPC UI dialogs correlate responses, cancellation, timeout, and notifications", async () => {
  const output: RpcObject[] = [];
  const broker = new RpcUIBroker((event) => output.push(event));
  const selection = broker.select("Pick", ["one", "two"]);
  const request = output[0]!;
  assert.equal(request.type, "extension_ui_request");
  assert.equal(request.method, "select");
  assert.equal(broker.respond({ type: "extension_ui_response", id: String(request.id), value: "two" }), true);
  assert.equal(await selection, "two");
  assert.equal(broker.respond({ type: "extension_ui_response", id: String(request.id), value: "one" }), false);

  const confirm = broker.confirm("Continue?", "Really?", { timeout: 5 });
  assert.equal(await confirm, false);
  broker.notify("notice", "warning");
  assert.equal(output.at(-1)?.method, "notify");
});

test("RPC JSON events remove internal message payloads and enrich toolcall_start", () => {
  const plain = toRpcJsonEvent({
    type: "message_update",
    message: { role: "assistant", usage: { totalTokens: 3 }, internal: true },
    assistantMessageEvent: { type: "text_delta", delta: "x" },
  });
  assert.deepEqual(plain, {
    type: "message_update",
    usage: { totalTokens: 3 },
    assistantMessageEvent: { type: "text_delta", delta: "x" },
  });

  const tool = toRpcJsonEvent({
    type: "message_update",
    message: { role: "assistant", usage: {} },
    assistantMessageEvent: {
      type: "toolcall_start",
      contentIndex: 0,
      partial: { content: [{ type: "toolCall", id: "tool-id", name: "bash" }] },
    },
  });
  assert.deepEqual(tool, {
    type: "message_update",
    usage: {},
    assistantMessageEvent: { type: "toolcall_start", contentIndex: 0, id: "tool-id", toolName: "bash" },
  });
});
