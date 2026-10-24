import assert from "node:assert/strict";
import { Writable } from "node:stream";
import { test } from "node:test";
import { fromLogf, newLogger, nopLogger } from "../src/logging.ts";

class BufferWriter extends Writable {
  readonly chunks: Buffer[] = [];
  override _write(chunk: Buffer | string, _encoding: BufferEncoding, callback: (error?: Error | null) => void): void {
    this.chunks.push(Buffer.from(chunk));
    callback();
  }
  text(): string { return Buffer.concat(this.chunks).toString("utf8"); }
}

test("structured logger validates level and format", () => {
  const output = new BufferWriter();
  assert.throws(() => newLogger(output, "loud", "text"), /unknown level/);
  assert.throws(() => newLogger(output, "debug", "xml"), /unknown format/);
  for (const level of ["", "debug", "info", "warn", "warning", "error", "DEBUG"]) assert.doesNotThrow(() => newLogger(output, level, "text"));
});

test("text logger filters levels and renders structured attributes", () => {
  const output = new BufferWriter();
  const logger = newLogger(output, "info", "text");
  logger.debug("hidden detail");
  logger.info("session started", "session", "/tmp/a.jsonl", "pid", 42);
  logger.warn("careful");
  const text = output.text();
  assert.doesNotMatch(text, /hidden detail/);
  assert.match(text, /msg="session started"/);
  assert.match(text, /session=\/tmp\/a\.jsonl/);
  assert.match(text, /pid=42/);
  assert.match(text, /level=WARN/);
});

test("JSON logger and With retain structured context", () => {
  const output = new BufferWriter();
  const logger = newLogger(output, "debug", "json");
  logger.with("session", "/tmp/a.jsonl").error("boom", "err", "broken");
  const record = JSON.parse(output.text()) as Record<string, unknown>;
  assert.equal(record.msg, "boom");
  assert.equal(record.session, "/tmp/a.jsonl");
  assert.equal(record.err, "broken");
  assert.equal(record.level, "ERROR");
});

test("no-op and printf adapters preserve the logger surface", () => {
  nopLogger().info("discarded");
  const lines: string[] = [];
  const logger = fromLogf((format, ...args) => { lines.push(format.replace("%s", String(args[0]))); });
  logger.warn("hello", "k", 1);
  logger.with("component", "actor").info("nested");
  assert.equal(lines.length, 2);
  assert.match(lines[0]!, /hello k=1/);
  assert.match(lines[1]!, /component=actor nested/);
});
