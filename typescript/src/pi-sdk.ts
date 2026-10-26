import { constants } from "node:fs";
import { access, readFile, realpath } from "node:fs/promises";
import { dirname, isAbsolute, join, resolve, sep } from "node:path";
import { pathToFileURL } from "node:url";

const PACKAGE_NAME = "@earendil-works/pi-coding-agent";

export interface PiSdkLocation {
  packageRoot: string;
  entrypoint: string;
  version: string;
}

export async function resolvePiSdkLocation(piBin = "pi", env: NodeJS.ProcessEnv = process.env): Promise<PiSdkLocation> {
  const executable = await resolveExecutable(piBin, env.PATH ?? "");
  const candidates: string[] = [];
  candidates.push(...await packageRootsFromExecutable(executable));

  const managedRoot = env.PI_MANAGED_INSTALL_ROOT;
  if (managedRoot) {
    const managed = await managedPackageRoot(managedRoot);
    if (managed) candidates.push(managed);
  }

  for (const packageRoot of [...new Set(candidates)]) {
    const location = await inspectPackageRoot(packageRoot);
    if (location) return location;
  }
  throw new Error(`cannot locate ${PACKAGE_NAME} for Pi executable ${JSON.stringify(piBin)}`);
}

export async function loadPiSdk(piBin = "pi", env: NodeJS.ProcessEnv = process.env): Promise<Record<string, unknown>> {
  return loadPiSdkAtLocation(await resolvePiSdkLocation(piBin, env));
}

export async function loadPiSdkAtLocation(location: PiSdkLocation): Promise<Record<string, unknown>> {
  const module: unknown = await import(pathToFileURL(location.entrypoint).href);
  if (!isRecord(module)) throw new Error(`invalid Pi SDK module at ${location.entrypoint}`);
  return module;
}

export async function loadPiSdkSubmodule(
  relativePath: string,
  piBin = "pi",
  env: NodeJS.ProcessEnv = process.env,
): Promise<Record<string, unknown>> {
  return loadPiSdkSubmoduleAtLocation(relativePath, await resolvePiSdkLocation(piBin, env));
}

export async function loadPiSdkSubmoduleAtLocation(
  relativePath: string,
  location: PiSdkLocation,
): Promise<Record<string, unknown>> {
  const modulePath = resolve(location.packageRoot, relativePath);
  if (!modulePath.startsWith(`${location.packageRoot}${sep}`)) {
    throw new Error(`Pi SDK module path is outside the package: ${JSON.stringify(relativePath)}`);
  }
  try {
    await access(modulePath, constants.R_OK);
  } catch {
    throw new Error(`Pi SDK module not found: ${relativePath}`);
  }
  const module: unknown = await import(pathToFileURL(modulePath).href);
  if (!isRecord(module)) throw new Error(`invalid Pi SDK module at ${modulePath}`);
  return module;
}

async function resolveExecutable(command: string, pathValue: string): Promise<string> {
  let candidates: string[];
  if (command.includes(sep) || isAbsolute(command)) {
    candidates = [resolve(command)];
  } else {
    candidates = [];
    const separator = sep === "\\" ? ";" : ":";
    for (const directory of pathValue.split(separator)) {
      if (directory !== "") candidates.push(join(directory, command));
    }
  }
  for (const candidate of candidates) {
    try {
      await access(candidate, constants.X_OK);
      return await realpath(candidate);
    } catch {
      // Try the next PATH directory.
    }
  }
  throw new Error(`cannot find Pi executable ${JSON.stringify(command)}`);
}

async function packageRootsFromExecutable(executable: string): Promise<string[]> {
  const roots: string[] = [];
  let directory = dirname(executable);
  for (;;) {
    if (await looksLikePackageRoot(directory)) roots.push(directory);
    const parent = dirname(directory);
    if (parent === directory) break;
    directory = parent;
  }

  // The managed Pi launcher is a small shell wrapper at <agent>/bin/pi; its
  // current-version file selects the matching SDK package under install/.
  const launcherDir = dirname(executable);
  if (launcherDir.endsWith(`${sep}bin`)) {
    const managed = await managedPackageRoot(join(dirname(launcherDir), "install"));
    if (managed) roots.push(managed);
  }
  return roots;
}

async function managedPackageRoot(installRoot: string): Promise<string | undefined> {
  let version: string;
  try {
    version = (await readFile(join(installRoot, "current-version"), "utf8")).trim();
  } catch {
    return undefined;
  }
  if (!/^[A-Za-z0-9._+-]+$/.test(version)) return undefined;
  return join(installRoot, "releases", version, "node_modules", "@earendil-works", "pi-coding-agent");
}

async function looksLikePackageRoot(path: string): Promise<boolean> {
  try {
    const metadata = JSON.parse(await readFile(join(path, "package.json"), "utf8")) as { name?: unknown };
    return metadata.name === PACKAGE_NAME;
  } catch {
    return false;
  }
}

async function inspectPackageRoot(packageRoot: string): Promise<PiSdkLocation | undefined> {
  let metadata: { name?: unknown; version?: unknown; main?: unknown };
  try {
    metadata = JSON.parse(await readFile(join(packageRoot, "package.json"), "utf8")) as typeof metadata;
  } catch {
    return undefined;
  }
  if (metadata.name !== PACKAGE_NAME || typeof metadata.version !== "string") return undefined;
  const entrypoint = resolve(packageRoot, typeof metadata.main === "string" ? metadata.main : "dist/index.js");
  try {
    await access(entrypoint, constants.R_OK);
  } catch {
    return undefined;
  }
  return { packageRoot, entrypoint, version: metadata.version };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
