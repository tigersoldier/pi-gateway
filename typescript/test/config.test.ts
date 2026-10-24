import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import {
  DEFAULT_ADDR,
  DEFAULT_PORT,
  DIR_ENV,
  addToken,
  configDir,
  describeTokenSpec,
  addresses,
  loadOrCreateToken,
  loadTokens,
  portOf,
  portPath,
  readPort,
  readToken,
  removePortFile,
  removePortIfMatches,
  requireLoopback,
  resolveTokenSpec,
  tokenPath,
  tokensPath,
  writePort,
} from "../src/config.ts";

async function withTempDir<T>(run: (dir: string) => Promise<T>): Promise<T> {
  const dir = await mkdtemp(join(tmpdir(), "pi-gateway-ts-"));
  try {
    return await run(dir);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

test("TestDirHonorsEnv", () => {
  const previousGateway = process.env[DIR_ENV];
  const previousXdg = process.env.XDG_CONFIG_HOME;
  try {
    process.env[DIR_ENV] = "/tmp/custom-pi-gateway";
    assert.equal(configDir(), "/tmp/custom-pi-gateway");
    delete process.env[DIR_ENV];
    process.env.XDG_CONFIG_HOME = "/tmp/xdg";
    assert.equal(configDir(), join("/tmp/xdg", "pi-gateway"));
  } finally {
    if (previousGateway === undefined) delete process.env[DIR_ENV];
    else process.env[DIR_ENV] = previousGateway;
    if (previousXdg === undefined) delete process.env.XDG_CONFIG_HOME;
    else process.env.XDG_CONFIG_HOME = previousXdg;
  }
});

test("TestLoadOrCreateTokenIsStableAndPrivate", async () => withTempDir(async (dir) => {
  const path = tokenPath(join(dir, "dir"));
  const first = await loadOrCreateToken(path);
  assert.equal(first.length, 64);
  assert.equal((await stat(path)).mode & 0o777, 0o600);
  assert.equal(await loadOrCreateToken(path), first);
}));

test("TestReadTokenRejectsEmpty", async () => withTempDir(async (dir) => {
  const path = join(dir, "token");
  await writeFile(path, "  \n", { mode: 0o600 });
  await assert.rejects(readToken(path), /empty/);
}));

test("TestPortRoundTrip", async () => withTempDir(async (dir) => {
  const path = portPath(dir);
  await writePort(path, 7442);
  assert.equal(await readPort(path), 7442);
  await writeFile(path, "nope", { mode: 0o600 });
  await assert.rejects(readPort(path), /invalid port file/);
}));

test("TestPortOf", () => {
  assert.equal(portOf("127.0.0.1:7331"), 7331);
  assert.throws(() => portOf("127.0.0.1"));
});

test("TestAddresses", async () => withTempDir(async (dir) => {
  assert.deepEqual(await addresses("10.0.0.5:9999", 7331, dir), ["10.0.0.5:9999"]);
  assert.deepEqual(await addresses("", 7442, dir), ["127.0.0.1:7442"]);
  assert.deepEqual(await addresses("", 0, dir), [DEFAULT_ADDR]);
  await writePort(portPath(dir), 7442);
  assert.deepEqual(await addresses("", 0, dir), ["127.0.0.1:7442", DEFAULT_ADDR]);
  await writePort(portPath(dir), DEFAULT_PORT);
  assert.deepEqual(await addresses("", 0, dir), [DEFAULT_ADDR]);
}));

test("TestRequireLoopback", async () => {
  const resolver = async (host: string) => host === "localhost"
    ? [{ address: "127.0.0.1" }, { address: "::1" }]
    : [{ address: "93.184.216.34" }];
  for (const addr of ["127.0.0.1:7331", "127.0.0.1:0", "[::1]:7331", "localhost:7331"]) {
    await requireLoopback(addr, resolver);
  }
  for (const addr of ["0.0.0.0:7331", ":7331", "10.1.2.3:7331", "example.com:80", "not-an-address"]) {
    await assert.rejects(requireLoopback(addr, resolver), (error: unknown) => {
      assert.ok(String(error).includes("loopback") || String(error).includes("address"), String(error));
      return true;
    });
  }
});

test("TestPortFileHelpers", async () => withTempDir(async (dir) => {
  const path = portPath(dir);
  await writePort(path, 7331);
  await removePortIfMatches(path, 99);
  assert.equal(await readPort(path), 7331);
  await removePortIfMatches(path, 7331);
  await assert.rejects(readPort(path), { code: "ENOENT" });
  await removePortFile(path);
  await writePort(path, 7331);
  await removePortFile(path);
  await assert.rejects(readPort(path), { code: "ENOENT" });
}));

test("TestLoadTokensMissingFile", async () => withTempDir(async (dir) => {
  assert.equal(await loadTokens(join(dir, "nope.json")), undefined);
  const path = join(dir, "empty.json");
  await writeFile(path, "\n");
  assert.equal(await loadTokens(path), undefined);
}));

test("TestLoadTokensResolvesRolesAndCapabilities", async () => withTempDir(async (dir) => {
  const path = join(dir, "tokens.json");
  await writeFile(path, JSON.stringify({ tokens: [
    { name: "slack", token: "tok-a", role: "operator", comment: "bot" },
    { name: "dashboard", token: "tok-b", capabilities: ["ui", "observe"] },
  ] }));
  const grants = await loadTokens(path);
  assert.deepEqual(grants?.map(({ name, capabilities }) => [name, capabilities]), [
    ["slack", ["observe", "interject", "prompt", "ui"]],
    ["dashboard", ["observe", "ui"]],
  ]);
}));

test("TestLoadTokensErrors", async () => withTempDir(async (dir) => {
  const cases: Record<string, { body: string; want: string }> = {
    "unknown role": { body: '{"tokens":[{"name":"a","token":"t","role":"root"}]}', want: "unknown role" },
    "prototype role constructor": { body: '{"tokens":[{"name":"a","token":"t","role":"constructor"}]}', want: "unknown role" },
    "prototype role proto": { body: '{"tokens":[{"name":"a","token":"t","role":"__proto__"}]}', want: "unknown role" },
    "unknown capability": { body: '{"tokens":[{"name":"a","token":"t","capabilities":["sudo"]}]}', want: "unknown capabilities" },
    both: { body: '{"tokens":[{"name":"a","token":"t","role":"admin","capabilities":["observe"]}]}', want: "both role and capabilities" },
    neither: { body: '{"tokens":[{"name":"a","token":"t"}]}', want: "neither role nor capabilities" },
    "no name": { body: '{"tokens":[{"token":"t","role":"admin"}]}', want: "has no name" },
    "no value": { body: '{"tokens":[{"name":"a","role":"admin"}]}', want: "no token value" },
    "duplicate value": { body: '{"tokens":[{"name":"a","token":"t","role":"observer"},{"name":"b","token":"t","role":"observer"}]}', want: "share the same value" },
    "unknown field": { body: '{"tokens":[{"name":"a","token":"t","role":"observer","extra":1}]}', want: "unknown field" },
    "unknown root field": { body: '{"unexpected":1}', want: "unknown field" },
    "malformed json": { body: '{"tokens":', want: "parse" },
  };
  for (const [name, item] of Object.entries(cases)) {
    const path = join(dir, `${name.replaceAll(" ", "-")}.json`);
    await writeFile(path, item.body, { mode: 0o600 });
    await assert.rejects(loadTokens(path), (error: unknown) => {
      assert.ok(String(error).includes(item.want), `${name}: ${String(error)}`);
      if (item.want === "unknown field") assert.ok(String(error).includes(path), `${name}: missing config path: ${String(error)}`);
      return true;
    });
  }
}));

test("TestAddTokenCreatesAndAppends", async () => withTempDir(async (dir) => {
  const path = tokensPath(dir);
  const grant = await addToken(path, "slack", "observer", []);
  assert.equal(grant.name, "slack");
  assert.equal(grant.token.length, 64);
  assert.equal((await stat(path)).mode & 0o777, 0o600);
  assert.equal((await loadTokens(path))?.[0].token, grant.token);
  const second = await addToken(path, "dashboard", "", ["observe"]);
  assert.equal((await loadTokens(path))?.length, 2);
  assert.equal((await loadTokens(path))?.[1].token, second.token);
  await assert.rejects(addToken(path, "slack", "observer", []), /already exists/);
  await assert.rejects(addToken(path, "broken", "", []));
  await assert.rejects(addToken(path, "", "observer", []));
}));

test("TestAddTokenAppendsToGoAcceptedNullTokenFiles", async () => withTempDir(async (dir) => {
  for (const [name, body] of [["root-null", "null"], ["tokens-null", '{"tokens":null}']] as const) {
    const path = join(dir, `${name}.json`);
    await writeFile(path, body, { mode: 0o600 });
    const grant = await addToken(path, "added", "observer", []);
    assert.equal((await loadTokens(path))?.[0]?.token, grant.token, `${name} should decode as an empty token list`);
  }
}));

test("TestTokenSpecDescribe", () => {
  assert.equal(describeTokenSpec({ name: "", token: "", role: "observer" }), "role observer");
  assert.equal(describeTokenSpec({ name: "", token: "", capabilities: ["observe", "ui"] }), "capabilities observe,ui");
  assert.deepEqual(resolveTokenSpec({ name: "a", token: "t", role: "observer" }).capabilities, ["observe"]);
});
