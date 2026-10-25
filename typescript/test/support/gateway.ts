import assert from "node:assert/strict";
import { connect, type Socket } from "node:net";
import { encodeJsonFrame, JsonlDecoder } from "../../src/protocol.ts";
import type { PiSdkRuntimeOptions } from "../../src/sdk-runtime.ts";
import type { RpcObject, RpcSessionRuntime } from "../../src/rpc-adapter.ts";

export type WireFrame = Record<string, unknown>;

export class WirePeer {
  readonly socket: Socket;
  welcome?: WireFrame;
  handshakeError?: WireFrame;
  private readonly decoder = new JsonlDecoder();
  private readonly frames: WireFrame[] = [];
  private readonly waiters: Array<{ test(frame: WireFrame): boolean; resolve(frame: WireFrame): void }> = [];

  constructor(socket: Socket) {
    this.socket = socket;
    socket.on("error", () => {});
    socket.on("data", (chunk: Buffer) => {
      for (const raw of this.decoder.push(chunk)) {
        let frame: WireFrame;
        try { frame = JSON.parse(raw.toString("utf8")) as WireFrame; }
        catch { continue; }
        const index = this.waiters.findIndex((waiter) => waiter.test(frame));
        if (index >= 0) this.waiters.splice(index, 1)[0]!.resolve(frame);
        else this.frames.push(frame);
      }
    });
  }

  send(frame: WireFrame): void { this.socket.write(encodeJsonFrame(frame)); }

  receive(predicate: (frame: WireFrame) => boolean, timeoutMs = 5000): Promise<WireFrame> {
    const index = this.frames.findIndex(predicate);
    if (index >= 0) return Promise.resolve(this.frames.splice(index, 1)[0]!);
    return new Promise((resolveFrame, reject) => {
      let timer: ReturnType<typeof setTimeout>;
      const waiter = { test: predicate, resolve: (frame: WireFrame) => { clearTimeout(timer); resolveFrame(frame); } };
      timer = setTimeout(() => {
        const index = this.waiters.indexOf(waiter);
        if (index >= 0) this.waiters.splice(index, 1);
        reject(new Error("timed out waiting for gateway frame"));
      }, timeoutMs);
      this.waiters.push(waiter);
    });
  }

  async collectUntil(predicate: (frame: WireFrame) => boolean, timeoutMs = 5000): Promise<WireFrame[]> {
    const out: WireFrame[] = [];
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const frame = await this.receive(() => true, deadline - Date.now());
      out.push(frame);
      if (predicate(frame)) return out;
    }
    throw new Error("timed out collecting gateway frames");
  }

  close(): void { this.socket.destroy(); }
}

export async function dialGateway(
  port: number,
  token: string,
  options: {
    session?: string;
    cwd?: string;
    client?: { name?: string; kind?: string; capabilities?: string[] };
    piArgs?: string[];
    resume?: { sinceSeq: number; leafEntryId?: string };
    liveOnly?: boolean;
    allowLossy?: boolean;
    expectHandshakeError?: boolean;
  } = {},
): Promise<WirePeer> {
  const socket = connect({ host: "127.0.0.1", port });
  await new Promise<void>((resolve, reject) => { socket.once("connect", resolve); socket.once("error", reject); });
  const peer = new WirePeer(socket);
  const hello: WireFrame = {
    type: "gw_hello", protocol: 1, token,
    client: { name: options.client?.name ?? "test-client", kind: options.client?.kind ?? "test" },
  };
  if (options.client?.capabilities) (hello.client as WireFrame).capabilities = options.client.capabilities;
  if (options.session !== undefined) hello.session = options.session;
  if (options.cwd !== undefined) hello.cwd = options.cwd;
  if (options.piArgs !== undefined) hello.piArgs = options.piArgs;
  if (options.resume !== undefined) hello.resume = options.resume;
  if (options.liveOnly !== undefined) hello.liveOnly = options.liveOnly;
  if (options.allowLossy !== undefined) hello.allowLossy = options.allowLossy;
  peer.send(hello);
  const first = await peer.receive((frame) => frame.type === "gw_welcome" || (options.expectHandshakeError === true && frame.type === "gw_error"));
  if (first.type === "gw_error") peer.handshakeError = first;
  else {
    peer.welcome = first;
    if (options.expectHandshakeError === true) {
      peer.handshakeError = await peer.receive((frame) => frame.type === "gw_error");
    }
  }
  return peer;
}

export interface FakePiOptions {
  turnDelayMs?: number;
  sessionFile?: string;
  sessionName?: string;
  abortSettles?: boolean;
  replayEvents?: number;
  deltaEvents?: number;
  uiOnPrompt?: boolean;
  crashAfterPrompt?: boolean;
  rejectFirstPrompt?: boolean;
}

export interface FakePiHarness {
  createRuntime(options: PiSdkRuntimeOptions): Promise<RpcSessionRuntime>;
  readonly starts: number;
  readonly prompts: string[];
  readonly uiSelections: string[];
  readonly cwds: string[];
  readonly steers: string[];
  readonly followUps: string[];
  readonly argsSeen: string[][];
  readonly disposals: number;
}

export function fakePi(options: FakePiOptions = {}): FakePiHarness {
  let starts = 0;
  let disposals = 0;
  let nextSession = 0;
  let rejectedFirstPrompt = false;
  const prompts: string[] = [];
  const uiSelections: string[] = [];
  const cwds: string[] = [];
  const steers: string[] = [];
  const followUps: string[] = [];
  const argsSeen: string[][] = [];
  return {
    get starts() { return starts; },
    get prompts() { return prompts; },
    get uiSelections() { return uiSelections; },
    get cwds() { return cwds; },
    get steers() { return steers; },
    get followUps() { return followUps; },
    get argsSeen() { return argsSeen; },
    get disposals() { return disposals; },
    async createRuntime(runtimeOptions: PiSdkRuntimeOptions): Promise<RpcSessionRuntime> {
      starts++;
      cwds.push(runtimeOptions.cwd);
      argsSeen.push([...(runtimeOptions.piArgs?.args ?? [])]);
      nextSession++;
      const listeners = new Set<(event: RpcObject) => void>();
      const runtimeValues = runtimeOptions.piArgs?.runtimeValues() ?? {};
      const model = { provider: runtimeValues.provider ?? "test", id: runtimeValues.model ?? "test/model" };
      const models = [model, { provider: "test", id: "test/model" }, { provider: "alt", id: "alt/model" }];
      const messages: unknown[] = [];
      let turnTimer: ReturnType<typeof setTimeout> | undefined;
      let uiContext: RpcObject | undefined;
      const session = {
        model,
        thinkingLevel: "medium",
        isStreaming: false,
        isCompacting: false,
        steeringMode: "one-at-a-time",
        followUpMode: "one-at-a-time",
        sessionFile: runtimeOptions.sessionPath ?? options.sessionFile ?? `/tmp/fake-session-${nextSession}.jsonl`,
        sessionId: `fake-session-${nextSession}`,
        sessionName: options.sessionName ?? "",
        autoCompactionEnabled: true,
        pendingMessageCount: 0,
        messages,
        promptTemplates: [],
        modelRuntime: { getAvailableSnapshot: () => models },
        sessionManager: {
          getCwd: () => runtimeOptions.cwd,
          getLeafId: () => "leaf-1",
          getEntries: () => [{ id: "leaf-1" }],
          getTree: () => [{ id: "leaf-1" }],
        },
        extensionRunner: { emitUserBash: async () => undefined, getRegisteredCommands: () => [] },
        resourceLoader: { getSkills: () => ({ skills: [] }) },
        subscribe(listener: (event: RpcObject) => void) { listeners.add(listener); return () => listeners.delete(listener); },
        async bindExtensions(bindings?: RpcObject) {
          const context = bindings?.uiContext;
          uiContext = context && typeof context === "object" ? context as RpcObject : undefined;
        },
        async waitForIdle() {},
        async prompt(message: string, commandOptions?: RpcObject) {
          prompts.push(message);
          if (options.rejectFirstPrompt && !rejectedFirstPrompt) {
            rejectedFirstPrompt = true;
            throw new Error("fake prompt rejected before acceptance");
          }
          const preflight = commandOptions?.preflightResult;
          if (typeof preflight === "function") preflight("started");
          messages.push({ role: "user", content: message });
          session.isStreaming = true;
          for (const listener of listeners) listener({ type: "agent_start" });
          for (let i = 0; i < (options.replayEvents ?? 0); i++) {
            const message = { role: "assistant", content: [{ type: "text", text: `replay-${i}` }] };
            for (const listener of listeners) listener({ type: "message_update", message });
          }
          for (let i = 0; i < (options.deltaEvents ?? 0); i++) {
            for (const listener of listeners) listener({ type: "message_update", message: { role: "assistant" },
              assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: `stream[${i}]` } });
          }
          if (options.uiOnPrompt && typeof uiContext?.select === "function") {
            const selected = await (uiContext.select as (title: string, choices: string[]) => Promise<unknown>)("Choose", ["one", "two"]);
            uiSelections.push(typeof selected === "string" ? selected : "");
          }
          if (options.crashAfterPrompt) throw new Error("fake runtime crashed after prompt acceptance");
          turnTimer = setTimeout(() => {
            turnTimer = undefined;
            const text = `echo: ${message}`;
            const assistant = { role: "assistant", content: [{ type: "text", text }] };
            messages.push(assistant);
            for (const listener of listeners) listener({ type: "message_start", message: { role: "assistant" } });
            for (const listener of listeners) listener({ type: "message_end", message: assistant });
            session.isStreaming = false;
            for (const listener of listeners) listener({ type: "agent_settled" });
          }, options.turnDelayMs ?? 15);
        },
        async steer(message: string) { steers.push(message); return "queued"; },
        async followUp(message: string) { followUps.push(message); return "queued"; },
        async abort() {
          if (options.abortSettles !== false) {
            if (turnTimer) clearTimeout(turnTimer);
            turnTimer = undefined;
            session.isStreaming = false;
            for (const listener of listeners) listener({ type: "agent_settled" });
          }
        },
        clearQueue() { return { steering: [], followUp: [] }; },
        async setModel(next: typeof model) { session.model = next; },
        async cycleModel() { return model; },
        setThinkingLevel(level: string) { session.thinkingLevel = level; },
        cycleThinkingLevel() { return "high"; },
        getAvailableThinkingLevels() { return ["off", "low", "medium", "high"]; },
        setSteeringMode(mode: string) { session.steeringMode = mode; },
        setFollowUpMode(mode: string) { session.followUpMode = mode; },
        async compact() { return {}; },
        setAutoCompactionEnabled(enabled: boolean) { session.autoCompactionEnabled = enabled; },
        setAutoRetryEnabled() {},
        abortRetry() {},
        abortBash() {},
        async executeBash(command: string) { return { command, exitCode: 0, output: "" }; },
        recordBashResult() {},
        getSessionStats() { return { messageCount: messages.length }; },
        async exportToHtml() { return "/tmp/fake-session.html"; },
        getUserMessagesForForking() { return []; },
        getLastAssistantText() { return ""; },
        setSessionName(name: string) { session.sessionName = name; },
      };
      return {
        session: session as unknown as RpcSessionRuntime["session"],
        async newSession() { return { cancelled: false }; },
        async switchSession() { return { cancelled: false }; },
        async fork() {
          const currentPath = session.sessionFile;
          session.sessionFile = currentPath ? `${currentPath}.forked-${nextSession}.jsonl` : `/tmp/fake-fork-${nextSession}.jsonl`;
          session.sessionId = `${session.sessionId}-forked`;
          return { cancelled: false, selectedText: "" };
        },
        async dispose() { if (turnTimer) clearTimeout(turnTimer); turnTimer = undefined; disposals++; },
        setRebindSession() {},
      };
    },
  };
}

export async function writeSessionFile(path: string, cwd = "/tmp", name?: string): Promise<void> {
  const { mkdir, writeFile } = await import("node:fs/promises");
  const { dirname } = await import("node:path");
  await mkdir(dirname(path), { recursive: true });
  const header: WireFrame = { type: "session", id: "session-id", cwd, timestamp: "2026-10-01T00:00:00Z" };
  await writeFile(path, `${JSON.stringify(header)}\n${name ? `${JSON.stringify({ type: "session_info", name })}\n` : ""}`);
}

export function assertResponse(frame: WireFrame, success: boolean, code?: string): void {
  assert.equal(frame.type, "response");
  assert.equal(frame.success, success);
  if (code !== undefined) assert.equal(frame.code, code);
}
