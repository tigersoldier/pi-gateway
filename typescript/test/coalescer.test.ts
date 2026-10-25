import assert from "node:assert/strict";
import { Buffer } from "node:buffer";
import { test } from "node:test";
import type { ProtocolRecord } from "../src/protocol.ts";
import { DeltaCoalescer, DELTA_FLUSH_BYTES } from "../src/session/coalescer.ts";

function deltaRecord(seq: number, kind: string, index: string, delta: string): ProtocolRecord {
  const raw = Buffer.from(JSON.stringify({
    type: "message_update",
    gw_seq: seq,
    usage: { output: seq },
    assistantMessageEvent: { type: kind, contentIndex: index, delta },
  }));
  return { raw, type: "message_update", seq, id: "", owner: "" };
}
function deltaOf(record: ProtocolRecord): string {
  return (JSON.parse(record.raw.toString("utf8")) as { assistantMessageEvent: { delta: string } }).assistantMessageEvent.delta;
}

test("coalescer merges adjacent deltas while retaining the newest record metadata", () => {
  const coalescer = new DeltaCoalescer();
  assert.deepEqual(coalescer.offer(deltaRecord(1, "text_delta", "0", "Hello")), []);
  assert.deepEqual(coalescer.offer(deltaRecord(2, "text_delta", "0", " world")), []);
  assert.equal(coalescer.pending, true);
  const flushed = coalescer.flush();
  assert.equal(flushed.length, 1);
  assert.equal(flushed[0]!.seq, 2);
  assert.equal(deltaOf(flushed[0]!), "Hello world");
  assert.match(flushed[0]!.raw.toString("utf8"), /"gw_seq":2/);
  assert.equal(coalescer.pending, false);
});

test("coalescer flushes on non-deltas, content changes, and delta-kind changes", () => {
  const coalescer = new DeltaCoalescer();
  coalescer.offer(deltaRecord(1, "text_delta", "0", "a"));
  const changedIndex = coalescer.offer(deltaRecord(2, "text_delta", "1", "b"));
  assert.equal(deltaOf(changedIndex[0]!), "a");
  assert.equal(deltaOf(coalescer.flush()[0]!), "b");

  coalescer.offer(deltaRecord(3, "text_delta", "0", "x"));
  const nonDelta = coalescer.offer({ raw: Buffer.from('{"type":"tool_execution_start"}'), type: "tool_execution_start", seq: 4, id: "", owner: "" });
  assert.equal(nonDelta.length, 2);
  assert.equal(deltaOf(nonDelta[0]!), "x");
  assert.equal(nonDelta[1]!.type, "tool_execution_start");

  coalescer.offer(deltaRecord(5, "thinking_delta", "0", "think"));
  const kindChange = coalescer.offer(deltaRecord(6, "text_delta", "0", "text"));
  assert.equal(deltaOf(kindChange[0]!), "think");
  assert.equal(deltaOf(coalescer.flush()[0]!), "text");
});

test("coalescer flushes at the UTF-8 byte budget", () => {
  const coalescer = new DeltaCoalescer();
  const chunk = "é".repeat(1024);
  const output: ProtocolRecord[] = [];
  for (let i = 0; i < 8; i++) output.push(...coalescer.offer(deltaRecord(i + 1, "text_delta", "0", chunk)));
  output.push(...coalescer.flush());
  assert.equal(output.length, 2);
  assert.equal(coalescer.pending, false);
  const text = output.map(deltaOf).join("");
  assert.equal(Buffer.byteLength(text, "utf8"), 8 * Buffer.byteLength(chunk, "utf8"));
  assert.equal(Buffer.byteLength(deltaOf(output[0]!), "utf8"), DELTA_FLUSH_BYTES);
  assert.equal(DELTA_FLUSH_BYTES, 8192);
});

test("coalescer passes through nonstreaming and malformed frames", () => {
  const coalescer = new DeltaCoalescer();
  const terminal: ProtocolRecord = { raw: Buffer.from('{"type":"agent_settled"}'), type: "agent_settled", seq: 1, id: "", owner: "" };
  assert.deepEqual(coalescer.offer(terminal), [terminal]);
  const malformed: ProtocolRecord = { raw: Buffer.from('{"type":"message_update"'), type: "message_update", seq: 2, id: "", owner: "" };
  assert.deepEqual(coalescer.offer(malformed), [malformed]);
});
