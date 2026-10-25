#!/usr/bin/env -S node --experimental-strip-types
import { runBridge } from "../src/bridge.ts";

const controller = new AbortController();
const abort = () => controller.abort();
process.once("SIGINT", abort);
process.once("SIGTERM", abort);
try {
  process.exitCode = await runBridge({ args: process.argv.slice(2), signal: controller.signal });
} catch (error) {
  process.stderr.write(`pi-gateway: ${error instanceof Error ? error.message : String(error)}\n`);
  process.exitCode = 3;
} finally {
  process.removeListener("SIGINT", abort);
  process.removeListener("SIGTERM", abort);
}
