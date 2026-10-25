import assert from "node:assert/strict";
import { test } from "node:test";
import {
  MAX_PENDING_COMMAND_BYTES,
  MAX_PENDING_ITEMS,
  MAX_PENDING_ITEMS_PER_CLIENT,
  PromptQueue,
  type QueuedItem,
} from "../src/queue.ts";

function item(clientId: string, id: string, type: "prompt" | "follow_up" = "prompt", message = id): QueuedItem {
  return { clientId, kind: "test", name: "client", localId: id, type, raw: Buffer.from(`{\"id\":\"${id}\"}`), message };
}

test("queue limits match the daemon contract and rejected pushes do not mutate state", () => {
  assert.deepEqual([MAX_PENDING_ITEMS, MAX_PENDING_ITEMS_PER_CLIENT, MAX_PENDING_COMMAND_BYTES], [64, 32, 16 << 20]);
  const queue = new PromptQueue();
  for (let i = 0; i < MAX_PENDING_ITEMS; i++) {
    const clientId = i < MAX_PENDING_ITEMS_PER_CLIENT ? "c_1" : "c_2";
    assert.equal(queue.tryPush(item(clientId, `r${i}`), 1), true);
  }
  assert.equal(queue.length, MAX_PENDING_ITEMS);
  assert.equal(queue.serializedBytes, MAX_PENDING_ITEMS);
  const before = queue.pending();
  assert.equal(queue.tryPush(item("c_3", "overflow"), 1), false);
  assert.equal(queue.length, MAX_PENDING_ITEMS);
  assert.equal(queue.serializedBytes, MAX_PENDING_ITEMS);
  assert.deepEqual(queue.pending(), before);
});

test("queue enforces per-client and aggregate-byte limits without charging rejects", () => {
  const perClient = new PromptQueue();
  for (let i = 0; i < MAX_PENDING_ITEMS_PER_CLIENT; i++) assert.equal(perClient.tryPush(item("c_1", `r${i}`), 1), true);
  assert.equal(perClient.tryPush(item("c_1", "overflow"), 1), false);
  assert.equal(perClient.length, MAX_PENDING_ITEMS_PER_CLIENT);
  assert.equal(perClient.clientLength("c_1"), MAX_PENDING_ITEMS_PER_CLIENT);
  assert.equal(perClient.tryPush(item("c_2", "other"), 1), true);

  const bytes = new PromptQueue();
  assert.equal(bytes.tryPush(item("c_1", "large"), MAX_PENDING_COMMAND_BYTES - 3), true);
  assert.equal(bytes.tryPush(item("c_2", "boundary"), 3), true);
  const before = bytes.pending();
  assert.equal(bytes.serializedBytes, MAX_PENDING_COMMAND_BYTES);
  assert.equal(bytes.tryPush(item("c_3", "overflow"), 1), false);
  assert.equal(bytes.length, 2);
  assert.equal(bytes.serializedBytes, MAX_PENDING_COMMAND_BYTES);
  assert.deepEqual(bytes.pending(), before);
});

test("queue accounting is released on pop and clear", () => {
  const queue = new PromptQueue();
  for (const [clientId, id, size] of [["c_1", "first", 5], ["c_1", "second", 10], ["c_2", "third", 7]] as const) {
    assert.equal(queue.tryPush(item(clientId, id), size), true);
  }
  assert.equal(queue.pop()?.localId, "first");
  assert.equal(queue.length, 2);
  assert.equal(queue.serializedBytes, 17);
  assert.equal(queue.clientLength("c_1"), 1);
  assert.equal(queue.clientLength("c_2"), 1);
  assert.deepEqual(queue.clear().map((entry) => entry.localId), ["second", "third"]);
  assert.equal(queue.length, 0);
  assert.equal(queue.serializedBytes, 0);
  assert.equal(queue.clientLength("c_1"), 0);
  assert.equal(queue.tryPush(item("c_1", "reused"), MAX_PENDING_COMMAND_BYTES), true);
});

test("queue preserves FIFO tags and follows the daemon preview format", () => {
  const queue = new PromptQueue();
  assert.equal(queue.tryPush(item("c_1", "req-1", "prompt", "  hello \n world  "), 1), true);
  assert.equal(queue.tryPush(item("c_2", "req-2", "follow_up", "x".repeat(200)), 1), true);
  const pending = queue.pending();
  assert.equal(pending.length, 2);
  assert.equal(pending[0]?.mode, "followUp");
  assert.equal(pending[0]?.author?.clientId, "c_1");
  assert.equal(pending[0]?.author?.kind, "test");
  assert.equal(pending[0]?.preview, "hello world");
  assert.ok(pending[1]?.preview.endsWith("…"));
  assert.ok((pending[1]?.preview.length ?? 0) < 200);
  assert.equal(queue.pop()?.clientId, "c_1");
  assert.equal(queue.clear()[0]?.clientId, "c_2");
  assert.equal(queue.length, 0);
});

test("queue previews truncate at the same UTF-8 byte boundary as the Go implementation", () => {
  const queue = new PromptQueue();
  const message = `${"a".repeat(119)}🙂rest`;
  assert.equal(queue.tryPush(item("c_1", "unicode", "prompt", message), 1), true);
  const preview = queue.pending()[0]?.preview ?? "";
  assert.equal(Buffer.byteLength(preview.slice(0, -1), "utf8"), 122);
  assert.ok(preview.endsWith("…"));
});
