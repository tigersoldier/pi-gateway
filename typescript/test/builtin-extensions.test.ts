import assert from "node:assert/strict";
import { test } from "node:test";
import {
  builtinExtensionsFromRegistry,
  publicBuiltinExtensions,
  resolveBuiltinExtensions,
  type BuiltinExtensionEntry,
} from "../src/builtin-extensions.ts";

function factory(): () => void {
  return () => {};
}

function publicSdk(): Record<string, unknown> {
  return {
    createMcpExtension: () => factory(),
    createCodemodeExtension: () => factory(),
    createToolSearchExtension: () => factory(),
  };
}

test("built-in registry entries are preserved with the CLI's names and flags", () => {
  const codemode = { name: "codemode", factory: factory(), builtin: true as const, replaceable: true };
  const mcp = { name: "mcp", factory: factory(), builtin: true as const };
  const entries = builtinExtensionsFromRegistry([codemode, mcp]);
  assert.deepEqual(entries, [codemode, mcp]);
  assert.equal(entries?.[0]?.factory, codemode.factory, "the registry factory is passed through unchanged");
});

test("an empty built-in registry is valid parity", () => {
  assert.deepEqual(builtinExtensionsFromRegistry([]), []);
  const resolution = resolveBuiltinExtensions({ extensions: [] }, {});
  assert.equal(resolution.source, "registry");
  assert.deepEqual(resolution.entries, []);
  assert.equal(resolution.problem, undefined);
});

test("changed built-in registry shapes are rejected instead of loaded", () => {
  const valid = { name: "mcp", factory: factory(), builtin: true as const };
  const cases: unknown[] = [
    undefined,
    null,
    "mcp",
    { name: "mcp" },
    [null],
    [{ ...valid, builtin: false }],
    [{ ...valid, builtin: undefined }],
    [{ ...valid, name: "" }],
    [{ ...valid, factory: "not-a-function" }],
    [{ ...valid, hidden: "yes" }],
    [{ ...valid, replaceable: "yes" }],
    [valid, valid],
  ];
  for (const value of cases) {
    assert.equal(builtinExtensionsFromRegistry(value), undefined, JSON.stringify(value));
  }
});

test("the public factories become named built-in entries in CLI order", () => {
  const sdk = publicSdk();
  const created: unknown[] = [];
  sdk.createMcpExtension = () => { created.push("mcp"); return factory(); };
  const { entries, missing } = publicBuiltinExtensions(sdk);
  assert.deepEqual(missing, []);
  assert.deepEqual(entries.map((entry) => [entry.name, entry.builtin, entry.replaceable]), [
    ["codemode", true, true],
    ["tool-search", true, true],
    ["mcp", true, true],
  ]);
  assert.deepEqual(created, ["mcp"], "only the selected factory is created");
});

test("a changed registry falls back to the public built-in factories", () => {
  const resolution = resolveBuiltinExtensions(
    { extensions: [{ name: "mcp", factory: "changed", builtin: true }] },
    publicSdk(),
  );
  assert.equal(resolution.source, "public-factories");
  assert.deepEqual(resolution.entries.map((entry) => entry.name), ["codemode", "tool-search", "mcp"]);
  assert.match(String(resolution.problem), /unexpected shape/);
});

test("a partially exported public factory set is still loaded", () => {
  const resolution = resolveBuiltinExtensions(
    { error: "dist/extensions/index.js is missing" },
    { createMcpExtension: () => factory() },
  );
  assert.equal(resolution.source, "public-factories");
  assert.deepEqual(resolution.entries.map((entry) => entry.name), ["mcp"]);
  assert.match(String(resolution.problem), /lacks factories for codemode, tool-search/);
});

test("a throwing or missing public factory is skipped", () => {
  const resolution = resolveBuiltinExtensions(undefined, {
    createMcpExtension: () => { throw new Error("bundle changed"); },
    createCodemodeExtension: () => "not-a-factory",
  });
  assert.equal(resolution.source, "none");
  assert.deepEqual(resolution.entries, []);
  assert.match(String(resolution.problem), /not loaded/);
});

test("a registry without usable factories reports no built-ins", () => {
  const resolution = resolveBuiltinExtensions({ extensions: undefined, error: "no builtInExtensions export" }, {});
  assert.equal(resolution.source, "none");
  assert.deepEqual(resolution.entries as BuiltinExtensionEntry[], []);
  assert.match(String(resolution.problem), /no builtInExtensions export/);
});
