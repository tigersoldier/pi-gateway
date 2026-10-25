# Go-to-TypeScript test parity map

This map records the source assertions covered by the `rewrite/pi-gateway-typescript`
cutover. The source baseline is `main` at `280d7f2`, with 220 top-level Go test
functions across 32 `_test.go` files. The Go daemon, bridge, and internal
server packages have been removed from the current tree; their runtime
assertions are mapped below to TypeScript tests. Public Go client packages and
examples remain and continue to have Go tests. Test counts do not establish
behavioral parity.

## Replaced runtime and bridge assertions

| Go reference test files | TypeScript assertion locations |
| --- | --- |
| `cmd/pi-gatewayd/main_test.go` | `typescript/test/daemon-cli.test.ts`, `duration.test.ts`, `session-actor.test.ts` — help/version, operator-accepted TypeScript omission of Go's `--pi` override, Go-style compound duration flags, non-positive idle fallback, token provisioning validation/output/exit codes, SIGHUP token replacement and failure preservation, packaged unit, logger configuration. |
| `config/config_test.go`, `config/tokens_test.go` | `typescript/test/config.test.ts` — state directory, addresses/loopback, port files, token creation/permissions, Go-compatible null token-file decoding, path-qualified schema errors, restricted token parsing/provisioning and errors. |
| `piargs/piargs_test.go` | `typescript/test/piargs.test.ts` — Pilish argv forms, validation/conflicts, runtime-vs-spawn values, canonicalization, merges, redaction and corrupt-record fallback. |
| `protocol/protocol_test.go`, `protocol/roles_test.go` | `typescript/test/protocol.test.ts` — framing/limits, gateway fields and IDs, response/hello shapes (including Go-compatible null string members and prototype-named tags), role normalization and per-command capability gates. |
| `internal/catalog/catalog_test.go` | `typescript/test/catalog.test.ts`, `catalog-daemon.test.ts`, and `daemon-parity.test.ts` — JSONL/header parsing, names/title/activity/leaf, partial tails, default roots, filters, live/dormant rows and name resolution. |
| `internal/client/bridge_test.go` | `typescript/test/bridge.test.ts`, `bridge-version-failure.test.ts`, `executable-e2e.test.ts`, and `daemon.test.ts` — pristine Pi-RPC relay, bounded input backpressure, Go-typed `gw_welcome` validation and daemon-write failure classification, gateway traffic hiding, runner-level help/TUI/unknown-option failures and exit codes, version/error handling, auth, reconnect/deleted-session behavior, and bridge process exit after daemon disconnect with stdin left open. |
| `internal/daemon/canonical_test.go` | `typescript/test/daemon-parity.test.ts` — empty-session/no-session lifecycle assertions exercise the empty-path contract; the TypeScript helper also explicitly returns `""` for an empty path. |
| `internal/daemon/coalesce_test.go` | `typescript/test/coalescer.test.ts` and `daemon-parity.test.ts` — delta joining, flush boundaries/byte budget, malformed/non-streaming passthrough and lossless TCP text. |
| `internal/daemon/coverage_test.go`, `lossy_test.go` | `typescript/test/daemon-parity.test.ts`, `daemon-restart.test.ts`, `daemon.test.ts`, `hub.test.ts` — ping/bye with a shared live session, daemon-restart history recovery and another prompt, control response shapes, FIFO/steer/follow-up/abort, replay/resync, bounded 1024-frame outbound writes and slow-consumer closure, lossy gap delivery, and connection survival. |
| `internal/daemon/cwd_test.go` | `typescript/test/daemon-parity.test.ts`, `daemon-lifecycle.test.ts` — requester CWD, stored CWD on hibernated respawn, override and invalid-CWD rejection. |
| `internal/daemon/e2e_test.go`, `e2e_m2_test.go` | `typescript/test/daemon.test.ts`, `daemon-parity.test.ts`, `executable-e2e.test.ts`, and `test/e2e/gateway-cases.el` via `test/e2e/run.sh --suite both` — handshake/auth, lazy session creation, multi-client FIFO/fan-out, client death, idle retirement/hibernation, replay ordering, spawn/runtime args, `gw_new_session` response/binding/tags/sidecar contract, session switching, fork/clone policy and rekeying, UI routing, durable-leaf resume, cross-session reload, creation/list races, queued work after abort/reload, and the TypeScript executable path through Pilish. |
| `internal/daemon/e2e_m3_test.go` | `typescript/test/debug-http.test.ts`, `daemon-cli.test.ts` — live status/catalog/metrics, unauthenticated read-only endpoints, spawn-data redaction, HTTP errors and lifecycle logs. |
| `internal/daemon/e2e_security_test.go` | `typescript/test/daemon.test.ts`, `daemon-parity.test.ts`, `protocol.test.ts` — token gating, restricted roles/scopes, observer queries, transcript privacy, unsupported/noncanonical commands, empty primary token, attach metrics and loopback-only listeners. |
| `internal/daemon/lifecycle_test.go` | `typescript/test/daemon-lifecycle.test.ts`, `daemon-parity.test.ts` — admin gating, stop/delete response shapes, busy/attached refusals, forced abort/grace, deletion ordering/hibernation/tombstones, no resurrection, concurrent attach/delete, delete metrics and absent files. |
| `internal/daemon/reload_attach_test.go` | `typescript/test/daemon-lifecycle.test.ts`, `daemon-parity.test.ts` — attach during restart, replay resync, attaching-client attribution and reload queue drain. |
| `internal/daemon/spawn_test.go` | `typescript/test/spawn-daemon.test.ts`, `spawn-store.test.ts`, `daemon-lifecycle.test.ts`, `daemon-parity.test.ts` — advertised features, durable/cold sidecars, restart/additive merges, attach-vs-additive-commit conflict, active-turn/shared-session busy handling, redaction, runtime-state preservation after crash, timeout/persistence failures, catalog/debug secrecy and stop-unbound behavior. |
| `internal/debughttp/debughttp_test.go` | `typescript/test/debug-http.test.ts` — status/catalog/metrics shapes, live state, `cwd`/`limit` catalog filters, GET/HEAD policy, method/404 errors and loopback binding. |
| `internal/gwlog/gwlog_test.go` | `typescript/test/logging.test.ts`, `daemon-cli.test.ts` — validation, level filtering, text/JSON structured attributes, contextual/no-op/printf adapters and CLI use. |
| `internal/metrics/metrics_test.go` | `typescript/test/debug-http.test.ts` — zero-value counters/gauges, documented metric exposition, sorted samples, live gauges and counters. |
| `internal/session/session_test.go` | `typescript/test/queue.test.ts`, `hub.test.ts`, `session-actor.test.ts`, `daemon-parity.test.ts` — queue accounting/FIFO/limits, actor-level `queue_full` wire rejection without queue mutation, subscriber replay/drop/lag/unsubscribe, actor turn release, stale UI after crash, and future-cursor rejection. |

## Go client/example tests retained

- `gwclient/commands_test.go`, `events_test.go`, `gwclient_test.go`,
  `lifecycle_test.go`, `session_test.go`, and `turn_internal_test.go` continue to
  test the public Go client. Tests that formerly started the Go daemon now use
  `gwclient/testserver_test.go`, a small wire-level fixture that does not
  reimplement gateway runtime behavior.
- `examples/chat/input_test.go` and `render_test.go` remain the Go example
  client's tests; the example is outside the daemon/bridge runtime rewrite.
- The Go daemon, bridge, and internal server tests listed in the source map were
  retired with those implementations after their runtime assertions were
  mapped to TypeScript. They are no longer run by `go test ./...`.

## Adaptations and remaining gates

- Go's direct unexported `canonicalPath("")` assertion is represented by the
  TypeScript empty-path guard and end-to-end no-session/delete assertions rather
  than a test-only export of the helper.
- Go's nil metrics-registry case is architecture-specific: TypeScript metrics
  use an always-initialized `Map`. Zero counters/gauges and full exposition are
  asserted before and after traffic.
- `test/e2e/run.sh` installs an EXIT/signal cleanup trap for started daemons and
  the local provider. An injected setup failure after provider and daemon startup
  verified that both processes are stopped and waited for.
- `typescript/test/executable-e2e.test.ts` launches both TypeScript CLI entry
  points as child processes against an isolated agent directory and loopback
  OpenAI-compatible provider. It verifies bridge version/auth, streamed success,
  provider-error settlement/recovery, session restore after daemon restart, and
  a shared turn delivered to two bridge processes. The Pilish gateway ERT suite
  passes suite A 17/17 (15 shared plus two stream-specific cases) and suite B 9/9 against these entry points with the same local provider.
- The Go fake-process crash contract is adapted to the in-process SDK boundary:
  a rejected prompt after successful preflight marks the actor crashed, avoids
  automatic restart, preserves acknowledged runtime settings for additive
  reload, rejects stale UI answers, and re-adopts only on explicit attach. In
  addition, `sdk-real-turn.test.ts` drives the installed Pi 1.3.0 SDK through a
  loopback OpenAI-compatible endpoint: a streamed success, an HTTP 401 that
  settles as an SDK error, and a successful turn afterward. `daemon-restart.test.ts`
  uses a local provider to verify that a separate daemon restores persisted Pi
  history and accepts another turn. These exercise real SDK session/request
  paths without external credentials or model inference.
- The map documents assertion coverage; it is not itself proof of strict CLI
  parity. Post-cutover validation passed: `cd typescript && npm test` (222/222),
  `cd typescript && npm run typecheck`, `go test -race ./...` for retained Go
  packages, and `test/e2e/run.sh --suite both --no-deps --rm` (suite A 17/17,
  suite B 9/9). The Pilish ERT suite uses the TypeScript daemon/bridge and local
  provider; no Go fake-Pi subprocess, user credentials, or remote model
  inference is used. The operator explicitly scoped replacement of the Go
  runtime, so its mapped tests are retired; retained Go client tests remain
  independently runnable.
