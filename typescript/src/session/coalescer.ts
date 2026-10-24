import { Buffer } from "node:buffer";
import type { ProtocolRecord } from "../protocol.ts";

export const DELTA_FLUSH_BYTES = 8 << 10;

/** Combines consecutive Pi streaming deltas while preserving the newest frame metadata. */
export class DeltaCoalescer {
  private last?: ProtocolRecord;
  private key = "";
  private merged = "";
  private size = 0;
  private count = 0;

  get pending(): boolean { return this.last !== undefined; }

  offer(record: ProtocolRecord): ProtocolRecord[] {
    const delta = deltaKind(record.raw);
    if (!delta) return [...this.flush(), record];
    if (!this.last) {
      this.start(record, delta.key, delta.text);
      return [];
    }
    if (this.key === delta.key) {
      this.merged += delta.text;
      this.size += Buffer.byteLength(delta.text, "utf8");
      this.count++;
      this.last = record;
      if (this.size >= DELTA_FLUSH_BYTES) return this.flush();
      return [];
    }
    const out = this.flush();
    this.start(record, delta.key, delta.text);
    return out;
  }

  flush(): ProtocolRecord[] {
    if (!this.last) return [];
    const record = this.last;
    const text = this.merged;
    const count = this.count;
    this.last = undefined;
    this.key = "";
    this.merged = "";
    this.size = 0;
    this.count = 0;
    if (count <= 1) return [record];
    const raw = patchDelta(record.raw, text);
    return raw ? [{ ...record, raw }] : [record];
  }

  private start(record: ProtocolRecord, key: string, text: string): void {
    this.last = record;
    this.key = key;
    this.merged = text;
    this.size = Buffer.byteLength(text, "utf8");
    this.count = 1;
  }
}

function deltaKind(raw: Uint8Array): { key: string; text: string } | undefined {
  try {
    const value: unknown = JSON.parse(Buffer.from(raw).toString("utf8"));
    if (!isObject(value) || value.type !== "message_update" || !isObject(value.assistantMessageEvent)) return undefined;
    const event = value.assistantMessageEvent;
    if (typeof event.delta !== "string" || event.delta === "") return undefined;
    if (event.type !== "text_delta" && event.type !== "thinking_delta" && event.type !== "toolcall_delta") return undefined;
    const index = event.contentIndex === undefined ? "" : JSON.stringify(event.contentIndex);
    return { key: `${event.type}:${index}`, text: event.delta };
  } catch { return undefined; }
}

function patchDelta(raw: Uint8Array, text: string): Buffer | undefined {
  try {
    const value: unknown = JSON.parse(Buffer.from(raw).toString("utf8"));
    if (!isObject(value) || !isObject(value.assistantMessageEvent)) return undefined;
    value.assistantMessageEvent.delta = text;
    return Buffer.from(JSON.stringify(value), "utf8");
  } catch { return undefined; }
}

function isObject(value: unknown): value is Record<string, unknown> { return value !== null && typeof value === "object" && !Array.isArray(value); }
