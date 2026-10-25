import assert from "node:assert/strict";
import { test } from "node:test";
import {
  ALL_CAPABILITIES,
  CAP_ADMIN,
  CAP_CONTROL,
  CAP_INTERJECT,
  CAP_OBSERVE,
  CAP_PROMPT,
  CAP_UI,
  FrameTooLargeError,
  INTERNAL_ID_PREFIX,
  JsonlDecoder,
  MAX_FRAME_BYTES,
  ROLE_ADMIN,
  ROLE_OBSERVER,
  ROLE_OPERATOR,
  VERSION,
  commandCapability,
  encodeFrame,
  encodeJsonFrame,
  getRoleCapabilities,
  isGatewayType,
  isInternalId,
  isSupportedCommand,
  namespaceId,
  normalizeCapabilities,
  parseHello,
  response,
  restoreId,
  rewriteCommand,
  splitNamespaceId,
  stamp,
  stripGatewayFields,
} from "../src/protocol.ts";

const decode = (decoder: JsonlDecoder, text: string) => decoder.push(Buffer.from(text, "utf8"));

test("TestCodecFraming", () => {
  const decoder = new JsonlDecoder();
  const frames = [
    ...decode(decoder, "{\"a\":1}\r\n{\"b\":\"x\u2028y\"}\n{\"c\":3}"),
    ...decoder.finish(),
  ];
  assert.equal(frames[0].toString("utf8"), '{"a":1}');
  assert.ok(frames[1].toString("utf8").includes("\u2028"));
  assert.equal(frames[2].toString("utf8"), '{"c":3}');
  assert.deepEqual(decoder.finish(), []);
});

test("TestCodecRejectsOversizedFrame", () => {
  const decoder = new JsonlDecoder();
  assert.throws(() => decode(decoder, `${"x".repeat(MAX_FRAME_BYTES + 1)}\n`), FrameTooLargeError);
});

test("TestCodecWriteRawAppendsLF", () => {
  const raw = encodeFrame(Buffer.from('{"a":1}', "utf8"));
  const json = encodeJsonFrame({ b: 2 });
  assert.equal(Buffer.concat([raw, json]).toString("utf8"), '{"a":1}\n{"b":2}\n');
});

test("TestStampAndStripGatewayFields", () => {
  const raw = Buffer.from('{"type":"message_update","delta":"x"}');
  const stamped = stamp(raw, { gw_seq: 7, gw_session: "p" });
  const value = JSON.parse(stamped.toString("utf8")) as Record<string, unknown>;
  assert.equal(value.gw_seq, 7);
  assert.equal(value.type, "message_update");
  const pristine = stripGatewayFields(stamped).toString("utf8");
  assert.ok(!pristine.includes("gw_"));
  assert.ok(pristine.includes('"type":"message_update"'));
});

test("TestRestoreID", () => {
  const raw = Buffer.from('{"type":"response","id":"c_3:req-1","success":true}');
  assert.ok(restoreId(raw, "c_3").toString("utf8").includes('"id":"req-1"'));
  assert.deepEqual(restoreId(raw, "c_9"), raw);
  const bare = Buffer.from('{"type":"response","id":"c_3"}');
  assert.ok(!restoreId(bare, "c_3").toString("utf8").includes('"id"'));
});

test("TestNamespaceRoundTrip", () => {
  assert.equal(namespaceId("c_1", "req-1"), "c_1:req-1");
  assert.deepEqual(splitNamespaceId("c_1:req-1"), ["c_1", "req-1"]);
  assert.deepEqual(splitNamespaceId("plain"), ["", "plain"]);
});

test("TestRewriteCommand", () => {
  const raw = Buffer.from('{"type":"follow_up","id":"local","message":"hi"}');
  const rewritten = rewriteCommand(raw, "c_1:local", "prompt");
  const value = JSON.parse(rewritten.toString("utf8")) as Record<string, unknown>;
  assert.equal(value.id, "c_1:local");
  assert.equal(value.type, "prompt");
});

test("TestIsInternalIDAndGatewayType", () => {
  assert.ok(isInternalId(`${INTERNAL_ID_PREFIX}1`));
  assert.ok(!isInternalId("c_1:req"));
  assert.ok(isGatewayType("gw_turn"));
  assert.ok(!isGatewayType("prompt"));
});

test("TestResponseShape", () => {
  const ok = response("req-1", "prompt", true, "", "", { queued: true });
  assert.equal(ok.success, true);
  assert.equal(ok.command, "prompt");
  assert.equal(ok.id, "req-1");
  assert.ok(!Object.hasOwn(ok, "error"));

  const bad = response("req-2", "switch_session", false, "unknown_session", "nope");
  assert.equal(bad.success, false);
  assert.equal(bad.code, "unknown_session");
  assert.equal(bad.error, "nope");
});

test("TestParseHello", () => {
  const raw = Buffer.from('{"type":"gw_hello","protocol":1,"token":"t","client":{"name":"n","kind":"pilish"},"piArgs":["--approve"],"liveOnly":true}');
  const hello = parseHello(raw);
  assert.equal(hello.client.kind, "pilish");
  assert.equal(hello.liveOnly, true);
  assert.equal(hello.piArgs?.length, 1);
  assert.throws(() => parseHello(Buffer.from('{"type":"gw_hello","protocol":99}')), /unsupported protocol version/);
  assert.throws(() => parseHello(Buffer.from('{"type":"gw_hello"}')), /unsupported protocol version: 0/);
  assert.throws(() => parseHello(Buffer.from('{"type":"gw_hello","protocol":null}')), /unsupported protocol version: 0/);
  assert.throws(() => parseHello(Buffer.from('{"type":"gw_hello","protocol":"1"}')), /protocol must be an integer/);
  assert.throws(() => parseHello(Buffer.from('{"type":"prompt"}')));
  assert.equal(VERSION, 1);
});

test("TestParseHelloNormalizesNullStringMembersLikeGo", () => {
  const hello = parseHello(Buffer.from('{"type":"gw_hello","protocol":1,"token":"t","client":{"capabilities":["observe",null],"tags":{"empty":null}},"piArgs":[null,"--approve"]}'));
  assert.deepEqual(hello.client.capabilities, ["observe", ""]);
  assert.deepEqual(Object.fromEntries(Object.entries(hello.client.tags ?? {})), { empty: "" });
  assert.deepEqual(hello.piArgs, ["", "--approve"]);
});

test("TestParseHelloPreservesPrototypeNamedTags", () => {
  const hello = parseHello(Buffer.from('{"type":"gw_hello","protocol":1,"client":{"tags":{"__proto__":"tag-value"}}}'));
  assert.equal(Object.hasOwn(hello.client.tags ?? {}, "__proto__"), true);
  assert.equal(hello.client.tags?.["__proto__"], "tag-value");
});

test("TestRoleCapabilities", () => {
  assert.deepEqual(getRoleCapabilities(ROLE_ADMIN), [...ALL_CAPABILITIES]);
  assert.deepEqual(getRoleCapabilities(ROLE_OPERATOR), [CAP_OBSERVE, CAP_INTERJECT, CAP_PROMPT, CAP_UI]);
  assert.deepEqual(getRoleCapabilities(ROLE_OBSERVER), [CAP_OBSERVE]);
  assert.equal(getRoleCapabilities("root"), undefined);
  for (const role of ["constructor", "toString", "__proto__"]) assert.equal(getRoleCapabilities(role), undefined);
  const admin = getRoleCapabilities(ROLE_ADMIN)!;
  admin[0] = "mutated";
  assert.notEqual(ALL_CAPABILITIES[0], "mutated");
});

test("TestNormalizeCapabilities", () => {
  assert.deepEqual(normalizeCapabilities([CAP_UI, CAP_OBSERVE, "sudo", "sudo"]), {
    known: [CAP_OBSERVE, CAP_UI],
    unknown: ["sudo"],
  });
  assert.deepEqual(normalizeCapabilities([]), { known: [], unknown: [] });
});

test("TestCommandCapability", () => {
  const expected: Record<string, string> = {
    get_state: CAP_OBSERVE,
    get_messages: CAP_OBSERVE,
    get_available_models: CAP_OBSERVE,
    get_available_thinking_levels: CAP_OBSERVE,
    get_session_stats: CAP_OBSERVE,
    get_entries: CAP_OBSERVE,
    get_tree: CAP_OBSERVE,
    get_fork_messages: CAP_OBSERVE,
    get_last_assistant_text: CAP_OBSERVE,
    get_commands: CAP_OBSERVE,
    export_html: CAP_OBSERVE,
    gw_list_sessions: CAP_OBSERVE,
    prompt: CAP_PROMPT,
    follow_up: CAP_PROMPT,
    new_session: CAP_PROMPT,
    fork: CAP_PROMPT,
    clone: CAP_PROMPT,
    bash: CAP_PROMPT,
    switch_session: CAP_PROMPT,
    steer: CAP_INTERJECT,
    abort: CAP_INTERJECT,
    abort_bash: CAP_INTERJECT,
    abort_retry: CAP_INTERJECT,
    clear_queue: CAP_INTERJECT,
    extension_ui_response: CAP_UI,
    gw_reload_session: CAP_CONTROL,
    set_model: CAP_CONTROL,
    cycle_model: CAP_CONTROL,
    set_thinking_level: CAP_CONTROL,
    cycle_thinking_level: CAP_CONTROL,
    set_steering_mode: CAP_CONTROL,
    set_follow_up_mode: CAP_CONTROL,
    compact: CAP_CONTROL,
    set_auto_compaction: CAP_CONTROL,
    set_auto_retry: CAP_CONTROL,
    set_session_name: CAP_CONTROL,
    gw_new_session: CAP_ADMIN,
    gw_stop_session: CAP_ADMIN,
    gw_delete_session: CAP_ADMIN,
    gw_ping: "",
    gw_bye: "",
  };
  for (const [command, capability] of Object.entries(expected)) {
    assert.equal(commandCapability(command), capability, command);
    assert.ok(isSupportedCommand(command), command);
  }
  for (const command of [
    "notify", "setStatus", "setWidget", "setTitle", "set_editor_text",
    "get_future_feature", "unknown_command",
  ]) {
    assert.equal(commandCapability(command), "", command);
    assert.ok(!isSupportedCommand(command), command);
  }
});

test("TestEveryPiCommandIsGated", () => {
  const piCommands = [
    "prompt", "steer", "follow_up", "abort", "clear_queue", "new_session",
    "get_state", "set_model", "cycle_model", "get_available_models",
    "set_thinking_level", "cycle_thinking_level", "get_available_thinking_levels",
    "set_steering_mode", "set_follow_up_mode", "compact", "set_auto_compaction",
    "set_auto_retry", "abort_retry", "bash", "abort_bash", "get_session_stats",
    "export_html", "switch_session", "fork", "clone", "get_fork_messages",
    "get_entries", "get_tree", "get_last_assistant_text", "set_session_name",
    "get_messages", "get_commands", "extension_ui_response",
  ];
  for (const command of piCommands) {
    assert.notEqual(commandCapability(command), "", command);
    assert.ok(isSupportedCommand(command), command);
  }
});
