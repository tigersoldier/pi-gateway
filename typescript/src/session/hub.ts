import { lagFrame, stamp, type ProtocolRecord } from "../protocol.ts";

export const TERMINAL_TYPES = new Set([
  "message_end", "tool_execution_end", "turn_end", "agent_end", "agent_settled", "response", "extension_ui_request",
  "gw_session_state", "gw_error", "gw_state_changed", "gw_turn", "gw_queue", "gw_presence", "compaction_start",
  "compaction_end", "auto_retry_start", "auto_retry_end", "queue_update", "bash_execution_update", "extension_error",
]);

export function isTerminal(type: string): boolean { return TERMINAL_TYPES.has(type); }

export interface SubOptions {
  buffer: number;
  allowLossy?: boolean;
  filterUnowned?: boolean;
  onOverflow?: () => void;
}

export type MetricCounter = (name: string) => void;

export class Subscriber {
  private queue: ProtocolRecord[] = [];
  private waiters: Array<(record: ProtocolRecord | undefined) => void> = [];
  private closed = false;
  private deadValue = false;
  private lagFrom = 0;
  private lagTo = 0;
  private readonly capacity: number;
  private readonly options: SubOptions;
  private readonly countMetric: MetricCounter;
  readonly id: string;

  constructor(id: string, options: SubOptions, countMetric: MetricCounter = () => {}) {
    this.id = id;
    this.options = { ...options };
    this.capacity = Math.max(1, options.buffer);
    this.countMetric = countMetric;
  }

  get dead(): boolean { return this.deadValue; }
  get isClosed(): boolean { return this.closed; }

  next(): Promise<ProtocolRecord | undefined> {
    const record = this.queue.shift();
    if (record !== undefined) return Promise.resolve(record);
    if (this.closed) return Promise.resolve(undefined);
    return new Promise((resolve) => this.waiters.push(resolve));
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    for (const resolve of this.waiters.splice(0)) resolve(undefined);
  }

  deliver(record: ProtocolRecord): void {
    if (this.closed) return;
    if (this.options.filterUnowned && record.owner !== this.id) return;
    if (this.lagFrom !== 0) {
      if (this.enqueue(this.lagRecord())) {
        this.countMetric("client_lags");
        this.lagFrom = 0;
        this.lagTo = 0;
      } else {
        this.overflow(record);
        return;
      }
    }
    if (!this.enqueue(record)) this.overflow(record);
  }

  private enqueue(record: ProtocolRecord): boolean {
    if (this.waiters.length > 0) {
      this.waiters.shift()!(record);
      return true;
    }
    if (this.queue.length >= this.capacity) return false;
    this.queue.push(record);
    return true;
  }

  private overflow(record: ProtocolRecord): void {
    if (this.options.allowLossy && !TERMINAL_TYPES.has(record.type)) {
      this.countMetric("frames_dropped");
      if (record.seq !== 0) {
        if (this.lagFrom === 0 || record.seq < this.lagFrom) this.lagFrom = record.seq;
        if (record.seq > this.lagTo) this.lagTo = record.seq;
      }
      return;
    }
    this.countMetric("subscriber_drops");
    this.deadValue = true;
    this.close();
    this.options.onOverflow?.();
  }

  private lagRecord(): ProtocolRecord {
    return { seq: 0, raw: Buffer.from(JSON.stringify(lagFrame(this.lagFrom, this.lagTo))), type: "gw_lag", id: "", owner: "" };
  }
}

/** Per-session ordered event log, replay ring, and bounded fan-out. */
export class Hub {
  private sequence = 0;
  private readonly ring: Array<ProtocolRecord | undefined>;
  private head = 0;
  private count = 0;
  private readonly subscribers = new Map<string, Subscriber>();
  private sessionPath: string;
  private readonly countMetric?: MetricCounter;

  constructor(capacity: number, sessionPath: string, countMetric?: MetricCounter) {
    this.ring = new Array(Math.max(1, capacity));
    this.sessionPath = sessionPath;
    this.countMetric = countMetric;
  }

  get subscriberCount(): number { return this.subscribers.size; }
  get headSeq(): number { return this.sequence; }
  get oldestSeq(): number { return this.oldestLocked(); }

  publish(record: ProtocolRecord): ProtocolRecord {
    this.sequence++;
    const next: ProtocolRecord = { ...record, seq: this.sequence, raw: Buffer.from(record.raw) };
    try {
      next.raw = stamp(next.raw, {
        gw_seq: this.sequence,
        gw_session: this.sessionPath,
        gw_ts: new Date().toISOString(),
      });
    } catch {
      // Go leaves an invalid record unchanged if gateway fields cannot be stamped.
    }
    this.ring[this.head] = next;
    this.head = (this.head + 1) % this.ring.length;
    if (this.count < this.ring.length) this.count++;
    for (const subscriber of this.subscribers.values()) subscriber.deliver(next);
    return next;
  }

  subscribe(id: string, options: SubOptions): Subscriber {
    const subscriber = new Subscriber(id, options, this.countMetric);
    this.subscribers.set(id, subscriber);
    return subscriber;
  }

  unsubscribe(id: string): void {
    const subscriber = this.subscribers.get(id);
    this.subscribers.delete(id);
    subscriber?.close();
  }

  replay(afterSeq: number): { records: ProtocolRecord[]; ok: boolean } {
    if (afterSeq > this.sequence || afterSeq + 1 < this.oldestLocked()) return { records: [], ok: false };
    const records: ProtocolRecord[] = [];
    for (let i = 0; i < this.count; i++) {
      const index = (this.head - this.count + i + this.ring.length) % this.ring.length;
      const record = this.ring[index];
      if (record !== undefined && record.seq > afterSeq) records.push(record);
    }
    return { records, ok: true };
  }

  setSession(path: string): void { this.sessionPath = path; }

  closeAll(): void {
    const subscribers = [...this.subscribers.values()];
    this.subscribers.clear();
    for (const subscriber of subscribers) subscriber.close();
  }

  private oldestLocked(): number {
    if (this.count === 0) return this.sequence + 1;
    const index = (this.head - this.count + this.ring.length) % this.ring.length;
    return this.ring[index]?.seq ?? this.sequence + 1;
  }
}
