export const KEY_MODEL = "model";
export const KEY_PROVIDER = "provider";
export const KEY_THINKING = "thinking";
export const KEY_NAME = "name";

type ArgGroup = {
  key: string;
  tokens: string[];
  value: string;
  runtime: boolean;
};

const valueFlags: Readonly<Record<string, string>> = {
  "--provider": KEY_PROVIDER,
  "--model": KEY_MODEL,
  "--models": "models",
  "--thinking": KEY_THINKING,
  "--name": KEY_NAME,
  "-n": KEY_NAME,
  "-e": "extension",
  "--extension": "extension",
  "--skill": "skill",
  "--prompt-template": "prompt-template",
  "--theme": "theme",
  "--tools": "tools",
  "-t": "tools",
  "--exclude-tools": "exclude-tools",
  "-xt": "exclude-tools",
  "--system-prompt": "system-prompt",
  "--append-system-prompt": "append-system-prompt",
  "--session-dir": "session-dir",
  "--api-key": "api-key",
};

const boolFlags: Readonly<Record<string, string>> = {
  "--approve": "approve",
  "-a": "approve",
  "--no-approve": "no-approve",
  "-na": "no-approve",
  "--no-extensions": "no-extensions",
  "-ne": "no-extensions",
  "--no-skills": "no-skills",
  "-ns": "no-skills",
  "--no-prompt-templates": "no-prompt-templates",
  "-np": "no-prompt-templates",
  "--no-themes": "no-themes",
  "--no-context-files": "no-context-files",
  "-nc": "no-context-files",
  "--no-builtin-tools": "no-builtin-tools",
  "-nbt": "no-builtin-tools",
  "--no-tools": "no-tools",
  "-nt": "no-tools",
  "--no-session": "no-session",
  "--offline": "offline",
  "--verbose": "verbose",
};

const reservedFlags: Readonly<Record<string, string>> = {
  "--mode": "the gateway always runs pi in RPC mode",
  "--session": "the daemon owns session files; use switch_session instead",
  "--help": "use pi-gateway --help",
  "-h": "use pi-gateway --help",
  "--version": "pi-gateway answers --version from the managed pi binary",
};

const canonicalFlags: Record<string, string> = {};
for (const [flag, key] of Object.entries(valueFlags)) {
  if (flag.startsWith("--")) canonicalFlags[key] = flag;
}
for (const [flag, key] of Object.entries(boolFlags)) {
  if (flag.startsWith("--")) canonicalFlags[key] = flag;
}

const negatedBool: Readonly<Record<string, string>> = {
  approve: "no-approve",
  "no-approve": "approve",
};

function isRuntimeKey(key: string): boolean {
  return key === KEY_MODEL || key === KEY_PROVIDER || key === KEY_THINKING || key === KEY_NAME;
}

export function splitFlag(arg: string): [flag: string, value: string, hasValue: boolean] {
  const index = arg.indexOf("=");
  return index >= 0 ? [arg.slice(0, index), arg.slice(index + 1), true] : [arg, "", false];
}

function emptyValues(): Record<string, string[]> {
  return Object.create(null) as Record<string, string[]>;
}

export class PiArgsSpec {
  args: string[];
  values: Record<string, string[]>;
  groups: ArgGroup[];

  constructor(initial: { args?: string[]; values?: Record<string, string[]>; groups?: ArgGroup[] } = {}) {
    this.args = [...(initial.args ?? [])];
    this.values = emptyValues();
    for (const [key, values] of Object.entries(initial.values ?? {})) this.values[key] = [...values];
    this.groups = [...(initial.groups ?? [])];
  }

  setName(name: string): void {
    this.values[KEY_NAME] = [name];
    const out: string[] = [];
    for (let i = 0; i < this.args.length; i++) {
      const [flag, , hasInline] = splitFlag(this.args[i]);
      if (valueFlags[flag] === KEY_NAME) {
        if (!hasInline) i++;
        continue;
      }
      out.push(this.args[i]);
    }
    this.args = [...out, "--name", name];
    this.groups = this.groups.filter((group) => group.key !== KEY_NAME);
    this.groups.push({ key: KEY_NAME, tokens: ["--name", name], value: name, runtime: true });
  }

  runtimeValues(): Record<string, string> {
    const out: Record<string, string> = {};
    for (const key of [KEY_MODEL, KEY_PROVIDER, KEY_THINKING, KEY_NAME]) {
      const values = this.values[key];
      if (values?.length) out[key] = values[values.length - 1];
    }
    return out;
  }

  runtimeArgs(): string[] {
    return this.groups.filter((group) => group.runtime).flatMap((group) => group.tokens);
  }

  canonicalSpawnArgs(): string[] {
    const out: string[] = [];
    for (const group of this.groups) {
      if (group.runtime) continue;
      const flag = canonicalFlags[group.key];
      if (!flag) {
        out.push(...group.tokens);
      } else if (group.tokens.length === 1) {
        if (group.value === "true") out.push(flag);
        else {
          const opposite = negatedBool[group.key];
          if (opposite && canonicalFlags[opposite]) out.push(canonicalFlags[opposite]);
        }
      } else {
        out.push(flag, group.tokens[1]);
      }
    }
    return out;
  }

  spawnValues(): Record<string, string[]> {
    const out = emptyValues();
    for (const [key, values] of Object.entries(this.values)) {
      if (!isRuntimeKey(key)) out[key] = [...values];
    }
    return out;
  }
}

export function parsePiArgs(args: readonly string[]): PiArgsSpec {
  const spec = new PiArgsSpec();
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (!arg.startsWith("-")) {
      throw new Error(`unexpected argument ${JSON.stringify(arg)} (pi-gateway takes pi options only)`);
    }
    const [flag, inline, hasInline] = splitFlag(arg);
    const reserved = reservedFlags[flag];
    if (reserved !== undefined) throw new Error(`pi option ${flag} is not accepted: ${reserved}`);

    const boolKey = boolFlags[flag];
    if (boolKey !== undefined) {
      let key = boolKey;
      let value = "true";
      if (hasInline) {
        if (inline !== "true" && inline !== "false") {
          throw new Error(`pi option ${flag} takes true or false, got ${JSON.stringify(inline)}`);
        }
        value = inline;
      }
      if (key === "no-approve") {
        key = "approve";
        value = value === "true" ? "false" : "true";
      }
      (spec.values[key] ??= []).push(value);
      let tokens = [flag];
      if (hasInline) {
        tokens = [];
        if (canonicalFlags[key] && value === "true") tokens = [canonicalFlags[key]];
        else {
          const opposite = negatedBool[key];
          if (opposite && canonicalFlags[opposite]) tokens = [canonicalFlags[opposite]];
        }
      }
      spec.args.push(...tokens);
      spec.groups.push({ key, tokens, value, runtime: isRuntimeKey(key) });
      continue;
    }

    const key = valueFlags[flag];
    if (key === undefined) throw new Error(`unsupported pi option ${JSON.stringify(flag)}`);
    let value = inline;
    if (!hasInline) {
      if (i + 1 >= args.length) throw new Error(`pi option ${flag} requires a value`);
      i++;
      value = args[i];
    }
    (spec.values[key] ??= []).push(value);
    const tokens = [flag, value];
    spec.args.push(...tokens);
    spec.groups.push({ key, tokens, value, runtime: isRuntimeKey(key) });
  }
  return spec;
}

export function redactedSpawnValues(values: Record<string, string[]> | undefined): Record<string, string[]> | undefined {
  if (!values || Object.keys(values).length === 0) return undefined;
  const out = emptyValues();
  for (const [key, items] of Object.entries(values)) {
    if (key !== "api-key") out[key] = [...items];
  }
  return Object.keys(out).length === 0 ? undefined : out;
}

export function mergeSpawn(recordedSpawnArgs: readonly string[], requested: PiArgsSpec): string[] {
  let recorded: PiArgsSpec;
  try {
    recorded = parsePiArgs(recordedSpawnArgs);
  } catch {
    return [...requested.args];
  }
  const out = [...recordedSpawnArgs];
  for (const group of requested.groups) {
    if (!group.runtime && recorded.values[group.key] !== undefined) continue;
    out.push(...group.tokens);
  }
  return out;
}

export class SpawnConflictError extends Error {
  readonly key: string;

  constructor(key: string) {
    const flag = canonicalFlags[key] || key;
    super(`conflicting value for ${flag}`);
    this.name = "SpawnConflictError";
    this.key = key;
  }
}

export function mergeSpawnAdditions(recordedSpawnArgs: readonly string[], additions: PiArgsSpec | undefined): PiArgsSpec {
  let recorded: PiArgsSpec;
  try {
    recorded = parsePiArgs(recordedSpawnArgs);
  } catch {
    throw new Error("invalid recorded spawn configuration");
  }
  for (const group of recorded.groups) {
    if (group.runtime) throw new Error(`recorded configuration contains runtime option ${canonicalFlags[group.key] ?? ""}`);
  }
  if (additions === undefined) throw new Error("no spawn arguments to add");
  for (const group of additions.groups) {
    if (group.runtime) throw new Error(`runtime option ${canonicalFlags[group.key] ?? ""} cannot be added to spawn configuration`);
    if (group.key === "no-session") throw new Error("--no-session cannot be added to an existing session");
    if (group.tokens.length === 0) throw new Error(`spawn option ${canonicalFlags[group.key] ?? ""} has no canonical argument form`);
  }

  const normalizedAdditions = parsePiArgs(additions.canonicalSpawnArgs());
  const groups = [...recorded.groups];
  const values = emptyValues();
  for (const [key, items] of Object.entries(recorded.values)) values[key] = [...items];

  for (const original of normalizedAdditions.groups) {
    const group = { ...original, tokens: [...original.tokens] };
    if (isCommaSeparatedList(group.key)) {
      const present = new Set(splitList(values[group.key] ?? []));
      const missing: string[] = [];
      for (const item of splitList([group.value])) {
        if (!present.has(item)) {
          present.add(item);
          missing.push(item);
        }
      }
      if (missing.length === 0) continue;
      group.value = missing.join(",");
      group.tokens = [canonicalFlags[group.key], group.value];
      groups.push(group);
      (values[group.key] ??= []).push(group.value);
      continue;
    }
    if (isRepeatedList(group.key)) {
      if ((values[group.key] ?? []).includes(group.value)) continue;
      groups.push(group);
      (values[group.key] ??= []).push(group.value);
      continue;
    }
    const current = values[group.key];
    if (current?.length) {
      if (current.some((value) => value !== group.value)) throw new SpawnConflictError(group.key);
      continue;
    }
    groups.push(group);
    values[group.key] = [group.value];
  }

  const mergedArgs = groups.flatMap((group) => group.tokens);
  return parsePiArgs(mergedArgs);
}

function isRepeatedList(key: string): boolean {
  return ["extension", "skill", "prompt-template", "theme", "append-system-prompt"].includes(key);
}

function isCommaSeparatedList(key: string): boolean {
  return ["models", "tools", "exclude-tools"].includes(key);
}

export function spawnConflict(
  recorded: Record<string, string[]>,
  requested: Record<string, string[]>,
): [key: string, conflict: boolean] {
  for (const [key, want] of Object.entries(requested)) {
    const previous = recorded[key];
    if (previous === undefined) continue;
    if (!sameValuesForKey(key, previous, want)) return [key, true];
  }
  return ["", false];
}

function sameValuesForKey(key: string, left: readonly string[], right: readonly string[]): boolean {
  if (["tools", "exclude-tools", "models"].includes(key)) return sameValues(splitList(left), splitList(right));
  return sameValues(left, right);
}

function splitList(values: readonly string[]): string[] {
  const out: string[] = [];
  for (const value of values) {
    for (const part of value.split(",")) {
      const trimmed = part.trim();
      if (trimmed !== "") out.push(trimmed);
    }
  }
  return out;
}

function sameValues(left: readonly string[], right: readonly string[]): boolean {
  const leftSet = new Set(left);
  const rightSet = new Set(right);
  if (leftSet.size !== rightSet.size) return false;
  for (const value of rightSet) if (!leftSet.has(value)) return false;
  return true;
}
