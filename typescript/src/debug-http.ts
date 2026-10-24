import type { AddressInfo } from "node:net";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { DEFAULT_ADDR, removePortIfMatches, requireLoopback, writePort } from "./config.ts";
import { PiGatewayDaemon } from "./daemon.ts";
import type { GatewayLogger } from "./logging.ts";

export const DAEMON_VERSION = "0.2.0";
export const DEFAULT_DEBUG_ADDR = "127.0.0.1:7332";

export interface DebugHttpOptions {
  daemon: PiGatewayDaemon;
  addr?: string;
  portFile?: string;
  version?: string;
  logger?: GatewayLogger;
}

/** Separate, unauthenticated, read-only loopback operational listener. */
export class DebugHttpServer {
  private readonly options: DebugHttpOptions;
  private server?: Server;
  private boundPort = 0;
  private boundAddr = "";

  constructor(options: DebugHttpOptions) { this.options = options; }
  get address(): string { return this.boundAddr; }

  async listen(): Promise<void> {
    if (this.server) throw new Error("debug listener is already running");
    const addr = this.options.addr || DEFAULT_DEBUG_ADDR;
    await requireLoopback(addr);
    const [host, portText] = splitHostPort(addr);
    const server = createServer((request, response) => { void this.handle(request, response); });
    this.server = server;
    await new Promise<void>((resolveListen, reject) => {
      const onError = (error: Error) => { server.off("listening", onListen); reject(error); };
      const onListen = () => { server.off("error", onError); resolveListen(); };
      server.once("error", onError);
      server.once("listening", onListen);
      server.listen({ host, port: Number(portText), exclusive: true });
    });
    const bound = server.address();
    if (!bound || typeof bound === "string") throw new Error("debug listener did not report a TCP address");
    this.boundPort = (bound as AddressInfo).port;
    this.boundAddr = `${formatHost(bound.address)}:${this.boundPort}`;
    if (this.options.portFile) {
      try { await writePort(this.options.portFile, this.boundPort); }
      catch (error) { this.options.logger?.warn("cannot write debug port file", "err", error, "path", this.options.portFile); }
    }
    this.options.logger?.info("debug listener started", "addr", this.boundAddr, "endpoints", "/status,/catalog,/metrics");
  }

  async close(): Promise<void> {
    const server = this.server;
    this.server = undefined;
    if (server?.listening) await new Promise<void>((resolveClose) => server.close(() => resolveClose()));
    if (this.options.portFile) await removePortIfMatches(this.options.portFile, this.boundPort);
  }

  private async handle(request: IncomingMessage, response: ServerResponse): Promise<void> {
    const method = request.method ?? "GET";
    if (method !== "GET" && method !== "HEAD") {
      response.setHeader("Allow", "GET, HEAD");
      this.writeJSON(response, 405, { error: "read-only endpoint" }, method === "HEAD");
      return;
    }
    let url: URL;
    try { url = new URL(request.url ?? "/", `http://${this.boundAddr || DEFAULT_ADDR}`); }
    catch { this.writeJSON(response, 400, { error: "invalid URL" }, method === "HEAD"); return; }
    if (url.pathname === "/status") {
      const status = await this.options.daemon.status();
      const started = this.options.daemon.started;
      this.writeJSON(response, 200, {
        version: this.options.version ?? DAEMON_VERSION,
        protocol: 1,
        piVersion: status.piVersion,
        addr: daemonAddress(this.options.daemon),
        debugAddr: this.boundAddr,
        startedAt: started.toISOString().replace(/\.\d{3}Z$/, "Z"),
        uptimeSeconds: (Date.now() - started.getTime()) / 1000,
        sessions: status,
      }, method === "HEAD");
      return;
    }
    if (url.pathname === "/catalog") {
      const limitText = url.searchParams.get("limit") ?? "";
      const parsedLimit = /^[+-]?\d+$/.test(limitText) ? Number(limitText) : 0;
      try {
        const sessions = await this.options.daemon.listSessions({ cwd: url.searchParams.get("cwd") ?? undefined, limit: parsedLimit });
        for (const row of sessions) delete row.spawn;
        this.writeJSON(response, 200, { sessions }, method === "HEAD");
      } catch (error) { this.writeJSON(response, 500, { error: errorMessage(error) }, method === "HEAD"); }
      return;
    }
    if (url.pathname === "/metrics") {
      response.statusCode = 200;
      response.setHeader("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
      response.end(method === "HEAD" ? undefined : await this.options.daemon.metricsText());
      return;
    }
    response.setHeader("Allow", "GET, HEAD");
    this.writeJSON(response, 404, { error: "unknown debug endpoint", paths: ["/status", "/catalog", "/metrics"] }, method === "HEAD");
  }

  private writeJSON(response: ServerResponse, status: number, value: unknown, head: boolean): void {
    response.statusCode = status;
    response.setHeader("Content-Type", "application/json");
    response.end(head ? undefined : `${JSON.stringify(value)}\n`);
  }
}

function daemonAddress(daemon: PiGatewayDaemon): string | undefined {
  const address = daemon.addr;
  if (!address) return undefined;
  return `${formatHost(address.address)}:${address.port}`;
}
function formatHost(host: string): string { return host.includes(":") ? `[${host}]` : host; }
function splitHostPort(addr: string): [string, string] {
  if (addr.startsWith("[")) {
    const end = addr.indexOf("]");
    if (end < 0 || addr[end + 1] !== ":") throw new Error(`config: invalid address ${JSON.stringify(addr)}`);
    return [addr.slice(1, end), addr.slice(end + 2)];
  }
  const index = addr.lastIndexOf(":");
  if (index < 0) throw new Error(`config: invalid address ${JSON.stringify(addr)}`);
  return [addr.slice(0, index), addr.slice(index + 1)];
}
function errorMessage(error: unknown): string { return error instanceof Error ? error.message : String(error); }
