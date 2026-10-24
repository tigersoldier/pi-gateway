#!/usr/bin/env -S node --experimental-strip-types
import { rm } from "node:fs/promises";
import {
  addToken,
  configDir,
  debugPortPath,
  describeTokenSpec,
  loadOrCreateToken,
  loadTokens,
  portPath,
  spawnDir,
  tokenPath,
  tokensPath,
} from "../src/config.ts";
import { PiGatewayDaemon } from "../src/daemon.ts";
import { parseDuration } from "../src/duration.ts";
import { DAEMON_VERSION, DEFAULT_DEBUG_ADDR, DebugHttpServer } from "../src/debug-http.ts";
import { VERSION } from "../src/protocol.ts";
import { newLogger, type GatewayLogger } from "../src/logging.ts";

const HELP = `Usage: pi-gatewayd [options]

Options:
  --listen <addr>          loopback address to listen on (default: 127.0.0.1:7331)
  --port <n>               loopback port (overrides --listen)
  --token-file <path>      token file (default: <state-dir>/token)
  --tokens-file <path>     restricted token grants JSON file
  --state-dir <path>       daemon state directory
  --idle-timeout <duration> session idle lifetime (default: 15m)
  --short-grace <duration> unused-session grace (default: 10s)
  --session-dir <path>     extra session catalog directory (repeatable)
  --delta-flush <duration> streaming delta coalescing interval (default: 50ms)
  --log-level <level>      debug, info, warn, or error (default: info)
  --log-format <format>    text or json (default: text)
  --debug-addr <addr>      loopback debug listener (default: ${DEFAULT_DEBUG_ADDR})
  --debug-port <n>         debug listener port (overrides --debug-addr)
  --no-debug               disable the read-only debug listener
  --provision-token        create a restricted token and print it
  --token-name <name>      name for --provision-token
  --token-role <role>      admin, operator, or observer
  --token-caps <list>      comma-separated capabilities (alternative to --token-role)
  --version                print daemon version and exit
  --help                   print this help

Exit: SIGINT/SIGTERM stop all sessions; SIGHUP reloads tokens.
Debug listener (read-only, unauthenticated, loopback): /status /catalog /metrics
`;

interface CliOptions {
  listen: string;
  port: number;
  tokenFile: string;
  tokensFile: string;
  stateDir: string;
  idleTimeoutMs: number;
  shortGraceMs: number;
  deltaFlushMs: number;
  debugAddr: string;
  debugPort: number;
  noDebug: boolean;
  provisionToken: boolean;
  tokenName: string;
  tokenRole: string;
  tokenCaps: string[];
  version: boolean;
  help: boolean;
  sessionDirs: string[];
  logLevel: string;
  logFormat: string;
}

export function parseDaemonArgs(args: readonly string[]): CliOptions {
  const options: CliOptions = {
    listen: "127.0.0.1:7331", port: 0, tokenFile: "", tokensFile: "", stateDir: "",
    idleTimeoutMs: 15 * 60_000, shortGraceMs: 10_000, deltaFlushMs: 50,
    debugAddr: DEFAULT_DEBUG_ADDR, debugPort: 0, noDebug: false, provisionToken: false,
    tokenName: "", tokenRole: "", tokenCaps: [], version: false, help: false,
    sessionDirs: [], logLevel: "info", logFormat: "text",
  };
  for (let i = 0; i < args.length; i++) {
    const arg = args[i]!;
    if (arg === "--" || !arg.startsWith("-")) break;
    const eq = arg.indexOf("=");
    const rawName = eq < 0 ? arg : arg.slice(0, eq);
    const name = rawName === "-h" ? "-h" : rawName.startsWith("-") && !rawName.startsWith("--") ? `-${rawName}` : rawName;
    const inline = eq < 0 ? undefined : arg.slice(eq + 1);
    const value = () => {
      if (inline !== undefined) return inline;
      if (i + 1 >= args.length) throw new Error(`flag needs an argument: ${name}`);
      return args[++i]!;
    };
    const bool = () => {
      if (inline === undefined) return true;
      if (inline === "true") return true;
      if (inline === "false") return false;
      throw new Error(`invalid boolean value ${JSON.stringify(inline)} for ${name}`);
    };
    switch (name) {
      case "--help": case "-h": options.help = bool(); break;
      case "--version": options.version = bool(); break;
      case "--listen": options.listen = value(); break;
      case "--port": options.port = parseIntFlag(name, value()); break;
      case "--token-file": options.tokenFile = value(); break;
      case "--tokens-file": options.tokensFile = value(); break;
      case "--state-dir": options.stateDir = value(); break;
      case "--idle-timeout": options.idleTimeoutMs = parseDuration(value()); break;
      case "--short-grace": options.shortGraceMs = parseDuration(value()); break;
      case "--delta-flush": options.deltaFlushMs = parseDuration(value()); break;
      case "--session-dir": options.sessionDirs.push(value()); break;
      case "--log-level": options.logLevel = value(); break;
      case "--log-format": options.logFormat = value(); break;
      case "--debug-addr": options.debugAddr = value(); break;
      case "--debug-port": options.debugPort = parseIntFlag(name, value()); break;
      case "--no-debug": options.noDebug = bool(); break;
      case "--provision-token": options.provisionToken = bool(); break;
      case "--token-name": options.tokenName = value(); break;
      case "--token-role": options.tokenRole = value(); break;
      case "--token-caps": options.tokenCaps = value().split(",").map((item) => item.trim()).filter(Boolean); break;
      default:
        throw new Error(`flag provided but not defined: ${name}`);
    }
  }
  return options;
}

export async function runDaemon(args: string[]): Promise<number> {
  let options: CliOptions;
  try { options = parseDaemonArgs(args); }
  catch (error) { process.stderr.write(`pi-gatewayd: ${messageOf(error)}\n`); return 2; }
  if (options.help) { process.stderr.write(HELP); return 0; }
  if (options.version) { process.stdout.write(`pi-gatewayd ${DAEMON_VERSION} (gateway protocol ${VERSION})\n`); return 0; }

  const stateDir = options.stateDir || configDir();
  const defaultTokenFile = options.tokenFile || tokenPath(stateDir);
  const restrictedTokensFile = options.tokensFile || tokensPath(stateDir);
  if (options.provisionToken) {
    if (!options.tokenName) { process.stderr.write("pi-gatewayd: --provision-token requires --token-name\n"); return 1; }
    if (options.tokenRole && options.tokenCaps.length > 0) {
      process.stderr.write("pi-gatewayd: --token-role and --token-caps are mutually exclusive\n"); return 1;
    }
    if (!options.tokenRole && options.tokenCaps.length === 0) {
      process.stderr.write("pi-gatewayd: --provision-token requires --token-role or --token-caps\n"); return 1;
    }
    try {
      const grant = await addToken(restrictedTokensFile, options.tokenName, options.tokenRole, options.tokenCaps);
      const description = describeTokenSpec({ name: grant.name, token: grant.token,
        role: options.tokenRole || undefined, capabilities: options.tokenCaps });
      process.stderr.write(`pi-gatewayd: provisioned token ${grant.name} (${description}) in ${restrictedTokensFile}\n`);
      process.stderr.write("pi-gatewayd: run `systemctl --user reload pi-gatewayd` (SIGHUP) to accept it\n");
      process.stdout.write(`${grant.token}\n`);
      return 0;
    } catch (error) { process.stderr.write(`pi-gatewayd: ${messageOf(error)}\n`); return 1; }
  }

  let logger: GatewayLogger;
  try { logger = newLogger(process.stderr, options.logLevel, options.logFormat); }
  catch (error) { process.stderr.write(`pi-gatewayd: ${messageOf(error)}\n`); return 1; }

  let token: string;
  let grants;
  try {
    token = await loadOrCreateToken(defaultTokenFile);
    grants = await loadTokens(restrictedTokensFile);
  } catch (error) { process.stderr.write(`pi-gatewayd: ${messageOf(error)}\n`); return 1; }
  const listen = options.port > 0 ? `127.0.0.1:${options.port}` : options.listen;
  const daemon = new PiGatewayDaemon({
    addr: listen,
    token,
    tokens: grants,
    portFile: portPath(stateDir),
    spawnDir: spawnDir(stateDir),
    idleTimeoutMs: options.idleTimeoutMs,
    shortGraceMs: options.shortGraceMs,
    deltaFlushMs: options.deltaFlushMs,
    catalogRoots: options.sessionDirs,
    logger,
  });
  try { await daemon.listen(); }
  catch (error) { process.stderr.write(`pi-gatewayd: ${messageOf(error)}\n`); await daemon.shutdown(); return 1; }

  let debug: DebugHttpServer | undefined;
  if (options.noDebug) await removeDebugPort(stateDir);
  else {
    const debugAddr = options.debugPort > 0 ? `127.0.0.1:${options.debugPort}` : options.debugAddr;
    debug = new DebugHttpServer({ daemon, addr: debugAddr, portFile: debugPortPath(stateDir), logger });
    try { await debug.listen(); }
    catch (error) {
      logger.error("debug listener disabled", "err", error);
      await removeDebugPort(stateDir);
      debug = undefined;
    }
  }
  const addr = daemon.addr;
  logger.info("daemon started", "version", DAEMON_VERSION, "protocol", VERSION,
    "addr", addr ? `${addr.address}:${addr.port}` : "unknown", "tokenFile", defaultTokenFile,
    "tokensFile", restrictedTokensFile, "tokens", daemon.tokenNames.join(","), "pi", daemon.piVersionValue || "unknown",
    "catalogRoots", daemon.catalogRoots.join(","));
  const controller = new AbortController();
  const abort = () => controller.abort();
  process.once("SIGINT", abort);
  process.once("SIGTERM", abort);
  let reloadTail = Promise.resolve();
  const reloadTokens = () => {
    reloadTail = reloadTail.then(async () => {
      try {
        const next = await loadTokens(restrictedTokensFile);
        daemon.replaceTokens(next ?? []);
        logger.info("tokens reloaded", "tokens", daemon.tokenNames.join(","));
      } catch (error) { logger.error("token reload failed", "err", error, "tokensFile", restrictedTokensFile); }
    });
  };
  process.on("SIGHUP", reloadTokens);
  try { await daemon.serve(controller.signal); }
  catch (error) { process.stderr.write(`pi-gatewayd: ${messageOf(error)}\n`); return 1; }
  finally {
    process.removeListener("SIGINT", abort);
    process.removeListener("SIGTERM", abort);
    process.removeListener("SIGHUP", reloadTokens);
    await reloadTail;
    await debug?.close();
  }
  logger.info("daemon stopped");
  return 0;
}

async function removeDebugPort(dir: string): Promise<void> { await rm(debugPortPath(dir), { force: true }); }
function parseIntFlag(name: string, value: string): number {
  if (!/^[+-]?\d+$/.test(value)) throw new Error(`invalid value ${JSON.stringify(value)} for ${name}`);
  const number = Number(value);
  if (!Number.isSafeInteger(number)) throw new Error(`invalid value ${JSON.stringify(value)} for ${name}`);
  return number;
}
function messageOf(error: unknown): string { return error instanceof Error ? error.message : String(error); }

try {
  process.exitCode = await runDaemon(process.argv.slice(2));
} catch (error) {
  process.stderr.write(`pi-gatewayd: ${messageOf(error)}\n`);
  process.exitCode = 1;
}
