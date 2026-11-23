/**
 * Built-in extensions for SDK-hosted Pi sessions.
 *
 * A `pi` CLI session always loads the built-ins declared by the CLI's
 * `dist/extensions/index.js`. An in-process SDK session only gets them when the
 * host passes named `builtin: true` entries through
 * `DefaultResourceLoader.extensionFactories`; see Pi's `docs/sdk.md`
 * ("Codemode and MCP"). The registry itself is not a package export, so it is
 * optional here: the runtime validates it before use and falls back to the
 * public factories the SDK does export.
 *
 * A registry entry is copied as-is. The loader keys its `builtin:<name>`
 * resources on the entry name, so keeping the CLI's names and flags is what
 * makes `-builtin:<name>`, `--no-extensions`, `--no-mcp`, replacement
 * semantics, and hidden startup listings behave like the CLI.
 */

export type BuiltinExtensionFactory = (...args: unknown[]) => unknown;

/** Named `InlineExtension` entry with `builtin: true` (see Pi's SDK types). */
export interface BuiltinExtensionEntry {
  name: string;
  factory: BuiltinExtensionFactory;
  builtin: true;
  replaceable?: boolean;
  hidden?: boolean;
}

/** Raw `dist/extensions/index.js` snapshot; the runtime validates `extensions`. */
export interface BuiltinRegistrySnapshot {
  extensions?: unknown;
  error?: string;
}

export interface BuiltinExtensionResolution {
  entries: BuiltinExtensionEntry[];
  source: "registry" | "public-factories" | "none";
  /** Why the registry was not used; absent when `source` is "registry". */
  problem?: string;
}

interface PublicBuiltinDefinition {
  name: string;
  exportName: string;
  replaceable: boolean;
}

/**
 * The built-ins with a public factory in the SDK, in the CLI registry's order.
 * `llama.cpp` has no public factory; only the registry can provide it.
 */
const PUBLIC_BUILTIN_EXTENSIONS: readonly PublicBuiltinDefinition[] = [
  { name: "codemode", exportName: "createCodemodeExtension", replaceable: true },
  { name: "tool-search", exportName: "createToolSearchExtension", replaceable: true },
  { name: "mcp", exportName: "createMcpExtension", replaceable: true },
];

/**
 * Validate a `builtInExtensions` value before passing it to the resource
 * loader. An absent export or a changed entry shape returns `undefined` so the
 * caller can fall back instead of failing session creation. An empty array is
 * valid: it is the CLI's registry, and parity means loading no built-ins.
 */
export function builtinExtensionsFromRegistry(value: unknown): BuiltinExtensionEntry[] | undefined {
  if (!Array.isArray(value)) return undefined;
  const entries: BuiltinExtensionEntry[] = [];
  const names = new Set<string>();
  for (const candidate of value) {
    if (!isRecord(candidate)) return undefined;
    if (candidate.builtin !== true) return undefined;
    if (typeof candidate.name !== "string" || candidate.name.length === 0) return undefined;
    if (typeof candidate.factory !== "function") return undefined;
    if (candidate.hidden !== undefined && typeof candidate.hidden !== "boolean") return undefined;
    if (candidate.replaceable !== undefined && typeof candidate.replaceable !== "boolean") return undefined;
    if (names.has(candidate.name)) return undefined;
    names.add(candidate.name);
    entries.push(candidate as unknown as BuiltinExtensionEntry);
  }
  return entries;
}

/** Build the public built-in entries; names and flags mirror the CLI registry. */
export function publicBuiltinExtensions(
  sdk: Record<string, unknown>,
): { entries: BuiltinExtensionEntry[]; missing: string[] } {
  const entries: BuiltinExtensionEntry[] = [];
  const missing: string[] = [];
  for (const definition of PUBLIC_BUILTIN_EXTENSIONS) {
    const create = sdk[definition.exportName];
    const factory = typeof create === "function"
      ? createFactory(create as (...args: unknown[]) => unknown, sdk)
      : undefined;
    if (factory === undefined) {
      missing.push(definition.name);
      continue;
    }
    entries.push({
      name: definition.name,
      factory,
      builtin: true,
      replaceable: definition.replaceable,
    });
  }
  return { entries, missing };
}

/**
 * Prefer the CLI's registry (full parity, including llama.cpp and built-ins a
 * later Pi adds). Fall back to the documented public factories when the
 * internal module is absent or its shape changed. Never throws.
 */
export function resolveBuiltinExtensions(
  registry: BuiltinRegistrySnapshot | undefined,
  sdk: Record<string, unknown>,
): BuiltinExtensionResolution {
  if (registry !== undefined) {
    const entries = builtinExtensionsFromRegistry(registry.extensions);
    if (entries !== undefined) return { entries, source: "registry" };
  }

  const problem = registryProblem(registry);
  const fallback = publicBuiltinExtensions(sdk);
  if (fallback.entries.length === 0) return { entries: [], source: "none", problem };
  const missing = fallback.missing.length > 0 ? `; the SDK also lacks factories for ${fallback.missing.join(", ")}` : "";
  return { entries: fallback.entries, source: "public-factories", problem: `${problem}${missing}` };
}

function registryProblem(registry: BuiltinRegistrySnapshot | undefined): string {
  if (registry === undefined) return "the built-in extension registry was not loaded";
  if (registry.error) return registry.error;
  if (registry.extensions === undefined) return "dist/extensions/index.js has no builtInExtensions export";
  return "dist/extensions/index.js builtInExtensions has an unexpected shape";
}

function createFactory(create: (...args: unknown[]) => unknown, sdk: Record<string, unknown>): BuiltinExtensionFactory | undefined {
  try {
    const factory = Reflect.apply(create, sdk, []);
    return typeof factory === "function" ? factory as BuiltinExtensionFactory : undefined;
  } catch {
    return undefined;
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
