import type { ClientRef, QueuePending } from "./protocol.ts";

export const MAX_PENDING_ITEMS = 64;
export const MAX_PENDING_ITEMS_PER_CLIENT = 32;
export const MAX_PENDING_COMMAND_BYTES = 16 << 20;

export interface QueuedItem {
  clientId: string;
  kind: string;
  name: string;
  localId: string;
  type: "prompt" | "follow_up";
  raw: Buffer;
  message: string;
  serializedBytes?: number;
}

/** Daemon-owned tagged FIFO; one session actor is its sole writer. */
export class PromptQueue {
  private items: QueuedItem[] = [];
  private perClient = new Map<string, number>();
  private commandByte = 0;

  tryPush(item: QueuedItem, serializedBytes: number): boolean {
    const clientCount = this.perClient.get(item.clientId) ?? 0;
    if (serializedBytes < 0
      || this.items.length >= MAX_PENDING_ITEMS
      || clientCount >= MAX_PENDING_ITEMS_PER_CLIENT
      || serializedBytes > MAX_PENDING_COMMAND_BYTES - this.commandByte) return false;
    const accepted = { ...item, serializedBytes };
    this.items.push(accepted);
    this.perClient.set(item.clientId, clientCount + 1);
    this.commandByte += serializedBytes;
    return true;
  }

  pop(): QueuedItem | undefined {
    const item = this.items.shift();
    if (item === undefined) return undefined;
    this.release(item);
    return item;
  }

  get length(): number { return this.items.length; }
  get serializedBytes(): number { return this.commandByte; }
  clientLength(clientId: string): number { return this.perClient.get(clientId) ?? 0; }

  clear(): QueuedItem[] {
    const items = this.items;
    this.items = [];
    this.perClient = new Map();
    this.commandByte = 0;
    return items;
  }

  pending(): QueuePending[] {
    return this.items.map((item) => {
      const author: ClientRef = { clientId: item.clientId };
      if (item.kind) author.kind = item.kind;
      if (item.name) author.name = item.name;
      return {
        id: item.localId,
        mode: "followUp",
        author,
        preview: previewOf(item.message),
      };
    });
  }

  private release(item: QueuedItem): void {
    this.commandByte -= item.serializedBytes ?? 0;
    const count = this.perClient.get(item.clientId) ?? 0;
    if (count <= 1) this.perClient.delete(item.clientId);
    else this.perClient.set(item.clientId, count - 1);
  }
}

function previewOf(message: string): string {
  const normalized = message.trim().replace(/\p{White_Space}+/gu, " ");
  const encoded = Buffer.from(normalized, "utf8");
  if (encoded.length <= 120) return normalized;
  return `${encoded.subarray(0, 120).toString("utf8")}…`;
}
