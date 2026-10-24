import assert from "node:assert/strict";
import { test } from "node:test";
import {
  KEY_MODEL,
  KEY_NAME,
  KEY_PROVIDER,
  KEY_THINKING,
  PiArgsSpec,
  SpawnConflictError,
  mergeSpawn,
  mergeSpawnAdditions,
  parsePiArgs,
  redactedSpawnValues,
  spawnConflict,
} from "../src/piargs.ts";

const mustParse = (...args: string[]) => parsePiArgs(args);

test("TestParseAcceptsPilishArgv", () => {
  const args = ["--approve", "-e", "/tmp/ext.ts", "--model", "gpt-5", "--provider", "openai", "--thinking", "high", "--no-session", "--name", "work"];
  const spec = parsePiArgs(args);
  assert.equal(spec.args.length, args.length);
  const runtime = spec.runtimeValues();
  assert.equal(runtime[KEY_MODEL], "gpt-5");
  assert.equal(runtime[KEY_PROVIDER], "openai");
  assert.equal(runtime[KEY_THINKING], "high");
  assert.equal(runtime[KEY_NAME], "work");
  const spawn = spec.spawnValues();
  assert.deepEqual(spawn.extension, ["/tmp/ext.ts"]);
  assert.ok(!(KEY_MODEL in spawn));
});

test("TestParseEqualsForm", () => {
  const spec = parsePiArgs(["--model=sonnet", "--approve=true"]);
  assert.equal(spec.runtimeValues()[KEY_MODEL], "sonnet");
  assert.deepEqual(spec.values.approve, ["true"]);
  assert.deepEqual(spec.args, ["--model", "sonnet", "--approve"]);
  const no = mustParse("--offline=false", "--no-approve=false");
  assert.deepEqual(no.args, ["--approve"]);
  assert.deepEqual(no.values.approve, ["true"]);
});

test("TestRedactedSpawnValues", () => {
  const out = redactedSpawnValues({ "api-key": ["sk-secret"], "append-system-prompt": ["instruction"] });
  assert.ok(out !== undefined && !("api-key" in out));
  assert.deepEqual(out?.["append-system-prompt"], ["instruction"]);
  assert.equal(redactedSpawnValues({ "api-key": ["x"] }), undefined);
  assert.equal(redactedSpawnValues(undefined), undefined);
});

test("TestParseRejects", () => {
  const cases: Record<string, string[]> = {
    "reserved mode": ["--mode", "rpc"],
    "reserved session": ["--session", "/tmp/x.jsonl"],
    "reserved version": ["--version"],
    "unknown flag": ["--nope"],
    positional: ["hello"],
    "missing value": ["--model"],
    "bad boolean value": ["--approve=maybe"],
  };
  for (const [name, args] of Object.entries(cases)) assert.throws(() => parsePiArgs(args), name);
});

test("TestSpawnConflict", () => {
  const recorded = { approve: ["true"], extension: ["/a.ts", "/b.ts"] };
  const same = { extension: ["/b.ts", "/a.ts"], approve: ["true"] };
  assert.deepEqual(spawnConflict(recorded, same), ["", false]);
  assert.deepEqual(spawnConflict(recorded, { extension: ["/c.ts"] }), ["extension", true]);
  assert.deepEqual(spawnConflict(recorded, { offline: ["true"] }), ["", false]);
  assert.deepEqual(spawnConflict(recorded, { approve: ["true", "true"] }), ["", false]);
  assert.deepEqual(spawnConflict(recorded, {}), ["", false]);
});

test("TestRuntimeValuesLastWins", () => {
  const spec = parsePiArgs(["--model", "a", "--model", "b", "--thinking", "low"]);
  assert.equal(spec.runtimeValues()[KEY_MODEL], "b");
  assert.ok(spec.args.join(" ").includes("--model a --model b"));
});

test("TestShortAliasesMatchLongForms", () => {
  const cases = [
    ["-a", "--approve", "", "approve"],
    ["-na", "--no-approve", "", "approve"],
    ["-ne", "--no-extensions", "", "no-extensions"],
    ["-ns", "--no-skills", "", "no-skills"],
    ["-np", "--no-prompt-templates", "", "no-prompt-templates"],
    ["-nc", "--no-context-files", "", "no-context-files"],
    ["-nbt", "--no-builtin-tools", "", "no-builtin-tools"],
    ["-nt", "--no-tools", "", "no-tools"],
    ["-e", "--extension", "/tmp/ext.ts", "extension"],
    ["-t", "--tools", "read,bash", "tools"],
    ["-xt", "--exclude-tools", "write", "exclude-tools"],
    ["-n", "--name", "work", KEY_NAME],
    ["", "--models", "sonnet,haiku", "models"],
    ["", "--no-themes", "", "no-themes"],
    ["", "--offline", "", "offline"],
    ["", "--verbose", "", "verbose"],
  ] as const;
  for (const [alias, long, value, key] of cases) {
    const argv = (flag: string) => value === "" ? [flag] : [flag, value];
    const longSpec = parsePiArgs(argv(long));
    assert.ok((longSpec.values[key] ?? []).length > 0, `${long} records ${key}`);
    if (alias === "") continue;
    const aliasSpec = parsePiArgs(argv(alias));
    assert.deepEqual(aliasSpec.values, longSpec.values, `${alias} equals ${long}`);
  }
});

test("TestCanonicalSpawnArgs", () => {
  const spec = parsePiArgs([
    "-a", "-na", "--extension", "/a.ts", "-e", "/b.ts",
    "--append-system-prompt", "one", "--model", "m1", "--thinking", "low",
  ]);
  assert.deepEqual(spec.canonicalSpawnArgs(), [
    "--approve", "--no-approve", "--extension", "/a.ts", "--extension", "/b.ts",
    "--append-system-prompt", "one",
  ]);
  assert.ok(!("no-approve" in spec.values));
  assert.deepEqual(spec.values.approve, ["true", "false"]);
});

test("TestMergeSpawn", () => {
  const recorded = ["--append-system-prompt", "one", "--extension", "/a.ts"];
  const requested = parsePiArgs([
    "--append-system-prompt", "two", "--skill", "/s.ts", "--no-approve", "--model", "m2",
  ]);
  const merged = mergeSpawn(recorded, requested).join(" ");
  assert.ok(!merged.includes("two"));
  for (const expected of ["--append-system-prompt one", "--extension /a.ts", "--skill /s.ts", "--no-approve", "--model m2"]) {
    assert.ok(merged.includes(expected), `missing ${expected}`);
  }
});

test("TestMergeSpawnFallsBackOnCorruptRecord", () => {
  const requested = parsePiArgs(["--approve"]);
  assert.deepEqual(mergeSpawn(["--not-a-flag"], requested), requested.args);
});

test("TestMergeSpawnAdditionsIsIdempotent", () => {
  const recorded = ["--extension", "/old.ts", "--tools", "read,bash", "--approve"];
  const additions = mustParse(
    "-e", "/old.ts", "--extension", "/new.ts", "--tools", "bash,write", "--skill", "/skill.ts",
    "--append-system-prompt", "later instruction",
  );
  const expected = [
    "--extension", "/old.ts", "--tools", "read,bash", "--approve", "--extension", "/new.ts",
    "--tools", "write", "--skill", "/skill.ts", "--append-system-prompt", "later instruction",
  ];
  const merged = mergeSpawnAdditions(recorded, additions);
  assert.deepEqual(merged.canonicalSpawnArgs(), expected);
  assert.deepEqual(mergeSpawnAdditions(merged.canonicalSpawnArgs(), additions).canonicalSpawnArgs(), expected);
});

test("TestMergeSpawnAdditionsRejectsConflictAndRuntimeArgs", () => {
  const recorded = ["--system-prompt", "old", "--api-key", "secret-value"];
  assert.throws(() => mergeSpawnAdditions(recorded, mustParse("--system-prompt", "new")), (error: unknown) => {
    assert.ok(error instanceof SpawnConflictError);
    assert.equal(error.key, "system-prompt");
    return true;
  });
  assert.throws(() => mergeSpawnAdditions(recorded, mustParse("--api-key", "different-secret")), (error: unknown) => {
    assert.ok(error instanceof SpawnConflictError);
    assert.equal(error.key, "api-key");
    assert.ok(!String(error).includes("secret-value"));
    assert.ok(!String(error).includes("different-secret"));
    return true;
  });
  assert.throws(() => mergeSpawnAdditions(recorded, mustParse("--model", "new-model")), /runtime option/);
  assert.throws(() => mergeSpawnAdditions(recorded, mustParse("--no-session")), /--no-session/);
  assert.throws(() => mergeSpawnAdditions(["private-record-value"], mustParse("--extension", "/new.ts")), (error: unknown) => {
    assert.ok(!String(error).includes("private-record-value"));
    return true;
  });
});

test("TestSpawnConflictListValues", () => {
  const recorded = { tools: ["read,bash"] };
  assert.deepEqual(spawnConflict(recorded, { tools: ["bash, read"] }), ["", false]);
  assert.deepEqual(spawnConflict(recorded, { tools: ["read", "bash"] }), ["", false]);
  assert.deepEqual(spawnConflict(recorded, { tools: ["read"] }), ["tools", true]);
});
