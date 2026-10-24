import assert from "node:assert/strict";
import { test } from "node:test";
import { parseDuration } from "../src/duration.ts";

test("Go-compatible durations accept compound and fractional components", () => {
  assert.equal(parseDuration("1h30m"), 90 * 60_000);
  assert.equal(parseDuration("1.5s250ms"), 1_750);
  assert.equal(parseDuration("2us500ns"), 0.0025);
  assert.equal(parseDuration("10μs"), 0.01);
  assert.equal(parseDuration("10µs"), 0.01);
  assert.equal(parseDuration("-1m30s"), -90_000);
  assert.equal(parseDuration("0s"), 0);
});

test("invalid and overflowing durations are rejected", () => {
  for (const value of ["", "1", "1sfoo", "1m-2s", "1e3s", "999999999999999999h"]) {
    assert.throws(() => parseDuration(value), /time: invalid duration/);
  }
});
