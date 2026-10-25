import assert from "node:assert/strict";
import { chmod, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { loadPiSdk, resolvePiSdkLocation } from "../src/pi-sdk.ts";

async function fakePackage(root: string, version = "1.3.0"): Promise<string> {
  const packageRoot = join(root, "node_modules", "@earendil-works", "pi-coding-agent");
  await mkdir(join(packageRoot, "dist", "bundle"), { recursive: true });
  await writeFile(join(packageRoot, "package.json"), JSON.stringify({
    name: "@earendil-works/pi-coding-agent",
    version,
    type: "module",
    main: "./dist/index.mjs",
  }));
  await writeFile(join(packageRoot, "dist", "index.mjs"), "export const marker = 'sdk-loaded';\n");
  const cli = join(packageRoot, "dist", "bundle", "cli.js");
  await writeFile(cli, "#!/usr/bin/env node\n");
  await chmod(cli, 0o700);
  return packageRoot;
}

async function withTempDir<T>(run: (dir: string) => Promise<T>): Promise<T> {
  const dir = await mkdtemp(join(tmpdir(), "pi-gateway-sdk-"));
  try { return await run(dir); }
  finally { await rm(dir, { recursive: true, force: true }); }
}

test("ResolvePiSdkFromExecutablePackage", async () => withTempDir(async (dir) => {
  const packageRoot = await fakePackage(dir);
  const cli = join(packageRoot, "dist", "bundle", "cli.js");
  const location = await resolvePiSdkLocation(cli);
  assert.equal(location.packageRoot, packageRoot);
  assert.equal(location.version, "1.3.0");
  assert.equal(location.entrypoint, join(packageRoot, "dist", "index.mjs"));
}));

test("ResolvePiSdkFromManagedLauncher", async () => withTempDir(async (dir) => {
  const agent = join(dir, ".pi", "agent");
  const launcherDir = join(agent, "bin");
  const install = join(agent, "install");
  const packageRoot = join(install, "releases", "1.3.0", "node_modules", "@earendil-works", "pi-coding-agent");
  await mkdir(launcherDir, { recursive: true });
  await writeFile(join(launcherDir, "pi"), "#!/bin/sh\n");
  await chmod(join(launcherDir, "pi"), 0o700);
  await mkdir(install, { recursive: true });
  await writeFile(join(install, "current-version"), "1.3.0\n");
  await mkdir(packageRoot, { recursive: true });
  await writeFile(join(packageRoot, "package.json"), JSON.stringify({
    name: "@earendil-works/pi-coding-agent", version: "1.3.0", main: "dist/index.js",
  }));
  await mkdir(join(packageRoot, "dist"), { recursive: true });
  await writeFile(join(packageRoot, "dist", "index.js"), "export {};\n");
  const location = await resolvePiSdkLocation(join(launcherDir, "pi"));
  assert.equal(location.packageRoot, packageRoot);
}));

test("LoadPiSdkFromPiExecutable", async () => withTempDir(async (dir) => {
  const packageRoot = await fakePackage(dir);
  const sdk = await loadPiSdk(join(packageRoot, "dist", "bundle", "cli.js"));
  assert.equal(sdk.marker, "sdk-loaded");
}));

test("ResolvePiSdkRejectsMissingExecutable", async () => withTempDir(async (dir) => {
  await assert.rejects(resolvePiSdkLocation(join(dir, "missing")), /cannot find Pi executable/);
}));
