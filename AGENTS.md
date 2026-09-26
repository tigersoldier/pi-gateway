# AGENTS.md

Guidance for coding agents working in this repository.

## What this is

`pi-gateway` is a Go replacement for the `pi` executable for third-party UIs.
`pi-gatewayd` is a background daemon that owns `pi --mode rpc` sessions;
`pi-gateway` is the bridge a UI spawns instead of `pi` (Emacs `pilish` over
ssh/TRAMP, or a non-pi integration speaking the same protocol). Exactly one
`pi` process per session is the sole writer of the session file, many clients
attach concurrently, and a session outlives the clients connected to it.

Read before changing behaviour, in this order:

1. [`docs/development.md`](docs/development.md) — decision log (canonical),
   implementation status, repository layout, conventions.
2. [`docs/design.md`](docs/design.md) — architecture, invariants, failure modes.
3. [`docs/protocol.md`](docs/protocol.md) — the client-facing wire contract.

[`README.md`](README.md) is user-facing (purpose, usage, CLI reference); do not
put development detail or the decision log back into it.

## Commands

| What | Command |
|---|---|
| Build | `go build ./...` |
| Format (must print nothing) | `gofmt -l .` |
| Vet | `go vet ./...` |
| All tests (a few minutes) | `go test -race ./...` |
| One package | `go test -race ./internal/daemon/` |
| One test | `go test -run TestAttach ./internal/daemon/` |
| End-to-end, fake pi (free) | `./test/e2e/run.sh --lane fake --suite both` |
| End-to-end, real model | `./test/e2e/run.sh --lane all --suite both` |

The real-model end-to-end lane **spends model quota** and needs a working pi
model configuration. Run it only when a change touches spawning, resume/replay,
hibernation, daemon restart, or the bridge's argument surface, and prefer
asking the operator first. Case list and coverage: [`test/e2e/README.md`](test/e2e/README.md).

## Rules

- **Docs first.** A behaviour change starts as a decision: record it in the
  [decision log](docs/development.md#decision-log) with its evidence, update
  `docs/design.md` / `docs/protocol.md`, then change code.
- **Every fix gets a regression test**, and it must be checked to fail with the
  fix reverted. Assert the real observable, not a proxy that can pass
  vacuously.
- **Never break these invariants.** One `pi` process per session (sole writer of
  the session file); one capability check per command, driven by the single
  table in `protocol/roles.go`; canonical lowercase command types only;
  terminal event frames are never dropped; both listeners stay loopback-only;
  token files are `0600`; never shadow the real `pi` (UIs are configured to
  point at the bridge instead).
- **Keep tests hermetic.** Use `internal/fakepi` (no real pi needed),
  `t.TempDir()` for state, and never read or write `~/.pi`, never write inside
  the repository, never require the network.
- **Move paired files together.** `packaging/pi-gatewayd.service` is parsed by
  `cmd/pi-gatewayd/main_test.go`; a new pi parameter needs
  `piargs` (every accepted spelling), `docs/protocol.md` §4.3 and the
  README reference table.
- **Commits:** imperative subject; the body says why, what was rejected, and the
  evidence (test counts, end-to-end matrix). Keep the tree clean and push to
  `origin/main`.
- **Flag unrequested additions** as reversible, and report judgment calls
  instead of silently widening scope.

## Where things live

| Path | Owns |
|---|---|
| `cmd/pi-gatewayd` | daemon entry point: flags, signals, listeners, token wiring |
| `cmd/pi-gateway` | bridge entry point: argv, relay, exit codes |
| `protocol` | strict JSONL codec, message types, id namespacing, capability table (exported) |
| `config` | state/token/port paths, discovery, token roles, provisioning (exported) |
| `piargs` | accepted pi parameters and spawn-parameter comparison (exported) |
| `gwclient` | exported client library for integrations and bots (dial, hello, typed commands/events, cursor and reconnect, UI-request classification) |
| `examples/chat` | runnable example CLI built only on `gwclient` (streaming, queue/steer/abort, commands/skills) |
| `internal/client` | bridge implementation (token/port discovery, relay, non-RPC modes) |
| `internal/daemon` | session table, attach/rebinding, spawn-parameter checks, per-connection fan-out, stop/delete lifecycle (tombstoned paths) |
| `internal/session` | `SessionActor`, `Hub` (event log and replay ring), `PromptQueue`, `PiProcess` |
| `internal/catalog` | session-file scanning, name resolution, durable leaf ids |
| `internal/debughttp` | read-only `/status` `/catalog` `/metrics` |
| `internal/gwlog`, `internal/metrics` | structured logging, Prometheus-text metrics |
| `internal/fakepi`, `internal/testutil`, `internal/gwtest` | test fake pi, protocol client, daemon harness |
| `packaging` | systemd user unit |

## Pitfalls

- `testutil.Conn.WaitFor` / `WaitType` / `WaitResponse` **consume and discard**
  frames that do not match. When a test must observe interleaved events, drain
  the connection continuously (the `frameLog` helper in
  `internal/daemon/coverage_test.go` shows the pattern).
- New sessions are spawned in the client's working directory (`gw_hello.cwd`,
  `gw_new_session{cwd}`), but a session whose file already exists is respawned
  in the directory recorded in its header (`catalog.HeaderCwd`): hibernation and
  daemon restarts ignore the attaching client's directory.
- pi writes the session file on the **first message**, not at startup, so a
  freshly created session has no file on disk yet.
- A crashed actor must transition through `setState` so the snapshot is
  published; assigning the state field directly leaves the session looking live
  and un-revivable on re-attach.
- The bridge refuses pi options it does not own, and only `--version`/`--help`
  are answered outside RPC mode.
- `gw_delete_session` tombstones the canonical path for the daemon's lifetime:
  a repeat delete and any attach naming that path answer `unknown_session`, and
  a connection it unbinds refuses session-scoped commands until the client
  attaches or creates a session explicitly (the bridge turns that into a
  pi-shaped error and closes the UI stream). The file is removed only after the
  actor finishes, because pi flushes its session file while shutting down.
