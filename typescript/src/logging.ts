import type { Writable } from "node:stream";

export type LogLevel = "debug" | "info" | "warn" | "error";
export type LogFormat = "text" | "json";

export interface GatewayLogger {
  debug(message: string, ...attributes: unknown[]): void;
  info(message: string, ...attributes: unknown[]): void;
  warn(message: string, ...attributes: unknown[]): void;
  error(message: string, ...attributes: unknown[]): void;
  with(...attributes: unknown[]): GatewayLogger;
}

const PRIORITY: Record<LogLevel, number> = { debug: 10, info: 20, warn: 30, error: 40 };

export function newLogger(output: Writable, level = "info", format = "text"): GatewayLogger {
  const normalizedLevel = level.trim().toLowerCase();
  const minimum: LogLevel = normalizedLevel === "" ? "info"
    : normalizedLevel === "warning" ? "warn"
      : normalizedLevel as LogLevel;
  if (!Object.hasOwn(PRIORITY, minimum)) throw new Error(`gwlog: unknown level ${JSON.stringify(level)} (want debug, info, warn, or error)`);
  const normalizedFormat = format.trim().toLowerCase();
  if (normalizedFormat !== "" && normalizedFormat !== "text" && normalizedFormat !== "json") {
    throw new Error(`gwlog: unknown format ${JSON.stringify(format)} (want text or json)`);
  }
  return new StructuredLogger(output, minimum, normalizedFormat === "json" ? "json" : "text", []);
}

export function nopLogger(): GatewayLogger { return new NopLogger(); }

export function fromLogf(write: (format: string, ...args: unknown[]) => void): GatewayLogger {
  return new LogfLogger(write);
}

class StructuredLogger implements GatewayLogger {
  private readonly output: Writable;
  private readonly minimum: LogLevel;
  private readonly format: LogFormat;
  private readonly attributes: unknown[];

  constructor(output: Writable, minimum: LogLevel, format: LogFormat, attributes: unknown[]) {
    this.output = output;
    this.minimum = minimum;
    this.format = format;
    this.attributes = attributes;
  }

  debug(message: string, ...attributes: unknown[]): void { this.write("debug", message, attributes); }
  info(message: string, ...attributes: unknown[]): void { this.write("info", message, attributes); }
  warn(message: string, ...attributes: unknown[]): void { this.write("warn", message, attributes); }
  error(message: string, ...attributes: unknown[]): void { this.write("error", message, attributes); }

  with(...attributes: unknown[]): GatewayLogger {
    return new StructuredLogger(this.output, this.minimum, this.format, [...this.attributes, ...attributes]);
  }

  private write(level: LogLevel, message: string, attributes: unknown[]): void {
    if (PRIORITY[level] < PRIORITY[this.minimum]) return;
    const values = attributePairs([...this.attributes, ...attributes]);
    const time = new Date().toISOString();
    if (this.format === "json") {
      const record: Record<string, unknown> = { time, level: level.toUpperCase(), msg: message };
      for (const [key, value] of values) record[key] = value;
      this.output.write(`${JSON.stringify(record)}\n`);
      return;
    }
    const fields = [`time=${time}`, `level=${level.toUpperCase()}`, `msg=${textValue(message)}`];
    for (const [key, value] of values) fields.push(`${key}=${textValue(value)}`);
    this.output.write(`${fields.join(" ")}\n`);
  }
}

class NopLogger implements GatewayLogger {
  debug(_message: string, ..._attributes: unknown[]): void {}
  info(_message: string, ..._attributes: unknown[]): void {}
  warn(_message: string, ..._attributes: unknown[]): void {}
  error(_message: string, ..._attributes: unknown[]): void {}
  with(..._attributes: unknown[]): GatewayLogger { return this; }
}

class LogfLogger implements GatewayLogger {
  private readonly writeLine: (format: string, ...args: unknown[]) => void;
  private readonly prefix: string;

  constructor(writeLine: (format: string, ...args: unknown[]) => void, prefix = "") {
    this.writeLine = writeLine;
    this.prefix = prefix;
  }
  debug(message: string, ...attributes: unknown[]): void { this.log(message, attributes); }
  info(message: string, ...attributes: unknown[]): void { this.log(message, attributes); }
  warn(message: string, ...attributes: unknown[]): void { this.log(message, attributes); }
  error(message: string, ...attributes: unknown[]): void { this.log(message, attributes); }
  with(...attributes: unknown[]): GatewayLogger {
    const prefix = [this.prefix, formatAttributes(attributes)].filter(Boolean).join(" ");
    return new LogfLogger(this.writeLine, prefix);
  }
  private log(message: string, attributes: unknown[]): void {
    const fields = formatAttributes(attributes);
    const line = [this.prefix, message, fields].filter(Boolean).join(" ");
    this.writeLine("%s", line);
  }
}

function attributePairs(attributes: unknown[]): Array<[string, unknown]> {
  const result: Array<[string, unknown]> = [];
  for (let index = 0; index < attributes.length; index += 2) {
    const rawKey = attributes[index];
    const key = typeof rawKey === "string" ? rawKey : String(rawKey);
    result.push([key, index + 1 < attributes.length ? attributes[index + 1] : "(MISSING)"]);
  }
  return result;
}

function formatAttributes(attributes: unknown[]): string {
  return attributePairs(attributes).map(([key, value]) => `${key}=${formatValue(value)}`).join(" ");
}

function textValue(value: unknown): string {
  if (typeof value !== "string") return formatValue(value);
  return /[\s="\\]/.test(value) ? JSON.stringify(value) : value;
}

function formatValue(value: unknown): string {
  if (value === null) return "<nil>";
  if (value === undefined) return "<nil>";
  if (value instanceof Error) return value.message;
  if (typeof value === "string") return value;
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}
