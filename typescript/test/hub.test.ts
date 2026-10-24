import assert from "node:assert/strict";
import { test } from "node:test";
import type { ProtocolRecord } from "../src/protocol.ts";
import { Hub, isTerminal, Subscriber } from "../src/session/hub.ts";

function record(type: string): ProtocolRecord {
  return { seq: 0, raw: Buffer.from(JSON.stringify({ type })), type, id: "", owner: "" };
}

test("hub assigns ordered sequences, stamps records, fans out, and replays retained history", async () => {
  const hub = new Hub(3, "s_test");
  const subscriber = hub.subscribe("c_1", { buffer: 8 });
  for (let i = 0; i < 3; i++) hub.publish(record("message_update"));
  for (let i = 1; i <= 3; i++) {
    const received = await subscriber.next();
    assert.equal(received?.seq, i);
    const decoded = JSON.parse(received!.raw.toString("utf8")) as Record<string, unknown>;
    assert.equal(decoded.gw_seq, i);
    assert.equal(decoded.gw_session, "s_test");
    assert.equal(typeof decoded.gw_ts, "string");
  }
  const replay = hub.replay(1);
  assert.equal(replay.ok, true);
  assert.equal(replay.records.length, 2);
  assert.equal(replay.records[0]?.seq, 2);
  for (let i = 0; i < 4; i++) hub.publish(record("message_update"));
  assert.equal(hub.replay(0).ok, false);
  const tail = hub.replay(5);
  assert.equal(tail.ok, true);
  assert.equal(hub.replay(999).ok, false, "future cursors require a resync");
  assert.equal(tail.records.length, 2);
  assert.equal(tail.records[0]?.seq, 6);
  assert.equal(hub.oldestSeq, 5);
  subscriber.close();
});

test("hub drops slow reliable subscribers and reports dead state", async () => {
  const hub = new Hub(8, "s");
  const subscriber = hub.subscribe("slow", { buffer: 1 });
  hub.publish(record("message_update"));
  hub.publish(record("message_end"));
  assert.equal(subscriber.dead, true);
  assert.equal((await subscriber.next())?.type, "message_update");
  assert.equal(await subscriber.next(), undefined);
});

test("lossy subscribers drop nonterminal records then receive a gap marker", async () => {
  const hub = new Hub(8, "s");
  const subscriber = hub.subscribe("lossy", { buffer: 2, allowLossy: true });
  hub.publish(record("message_update"));
  hub.publish(record("message_update"));
  hub.publish(record("message_update"));
  assert.equal(subscriber.dead, false);
  assert.equal((await subscriber.next())?.seq, 1);
  assert.equal((await subscriber.next())?.seq, 2);
  hub.publish(record("message_update"));
  const lag = await subscriber.next();
  assert.equal(lag?.type, "gw_lag");
  assert.equal(JSON.parse(lag!.raw.toString("utf8")).oldestSeq, 3);
  assert.equal((await subscriber.next())?.seq, 4);
});

test("hub unsubscribe removes and closes its subscriber", () => {
  const hub = new Hub(8, "s");
  const subscriber = hub.subscribe("c_1", { buffer: 4 });
  assert.equal(hub.subscriberCount, 1);
  hub.unsubscribe("c_1");
  assert.equal(hub.subscriberCount, 0);
  assert.equal(subscriber.isClosed, true);
});

test("lossy subscribers still close when a terminal event cannot fit after a gap", () => {
  const hub = new Hub(8, "s");
  const subscriber = hub.subscribe("lossy-terminal", { buffer: 1, allowLossy: true });
  hub.publish(record("message_update"));
  hub.publish(record("message_update"));
  hub.publish(record("agent_settled"));
  assert.equal(subscriber.dead, true);
  assert.equal(subscriber.isClosed, true);
});

test("filter-unowned subscribers see only records routed to their client", async () => {
  const subscriber = new Subscriber("c_1", { buffer: 4, filterUnowned: true });
  subscriber.deliver({ ...record("message_update"), seq: 1, owner: "c_2" });
  subscriber.deliver({ ...record("response"), seq: 2, owner: "c_1" });
  assert.equal((await subscriber.next())?.owner, "c_1");
  assert.equal(subscriber.isClosed, false);
});

test("terminal type classification matches delivery reliability rules", () => {
  assert.equal(isTerminal("agent_settled"), true);
  assert.equal(isTerminal("gw_queue"), true);
  assert.equal(isTerminal("message_update"), false);
});
