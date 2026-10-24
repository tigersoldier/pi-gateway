# AGENTS.md

Guidance for coding agents working in this repository.

## What this is

`pi-gatewayd` and `pi-gateway` are the TypeScript production daemon and bridge
for Pi sessions. The daemon uses Pi's SDK in-process and owns one SDK
`AgentSession` per gateway session; the bridge is spawned by UIs in place of
`pi`. The exported Go `protocol`, `config`, `piargs`, and `gwclient` packages
and `examples/chat` remain supported for Go integrations. There is no Go
production daemon or bridge.

Read before changing behavior, in this order:

1. [`docs/development.md`](docs/development.md) — implementation status,
   decision log, repository layout, and conventions.
2. [`docs/design.md`](docs/design.md) — architecture, invariants, failure modes.
3. [`docs/protocol.md`](docs/protocol.md) — client-facing wire contract.
4. [`docs/test-parity.md`](docs/test-parity.md) — source assertion mapping.

[`README.md`](README.md) is user-facing (purpose, installation, CLI reference).

## Commands

| What | Command |
|---|---|
| TypeScript tests | `cd typescript && npm test` |
| TypeScript static check | `cd typescript && npm run typecheck` |
| Retained Go packages | `go test -race ./...` |
| Go static check | `go vet ./...` |
| Go formatting | `gofmt -l .` |
| Pilish E2E, local provider only | `./test/e2e/run.sh --suite both --no-deps` |

Node.js >=22.19.0 is required. E2E uses an isolated loopback provider and does
not read user credentials or send remote model inference. The daemon resolves
`pi` from `PATH`; there is no `--pi` CLI override. Do not add a real-model lane
or live service dependency to tests.

## Rules

- **Preserve protocol v1.** The Go protocol package remains the source reference
  for wire shapes and capability behavior. This is an implementation replacement,
  not authorization to add message fields, commands, permissions, or lifecycle
  IDs. The TypeScript omission of the former Go `--pi` flag is an accepted CLI
  exception; do not claim strict CLI parity.
- **Docs first.** For behavior changes, update the decision/status docs and the
  relevant design/protocol sections before changing code.
- **Every fix gets a regression test.** Assert the observable behavior and avoid
  vacuous tests. TypeScript server behavior belongs in `typescript/test/`; the
  Go `gwclient/testserver_test.go` fixture only exercises the Go client API and
  is not a daemon implementation.
- **Keep tests hermetic.** Use temporary directories and local providers. Do not
  read or write `~/.pi`, require network access, contact Slack, or invoke remote
  model inference.
- **Keep the invariants.** One SDK session per gateway session and one writer
  per session file; one capability check per command; canonical lowercase
  command types; terminal events never dropped; both listeners loopback-only;
  token files `0600`; never shadow the real `pi` executable.
- **Keep packaging and CLI tests aligned.** `packaging/pi-gatewayd.service`,
  TypeScript CLI tests, and the user-facing README must describe the same
  runtime and options.
- **Commits/publication require explicit authorization.** Do not commit, push,
  open a PR, or deploy unless the operator explicitly requests it.

## Where things live

| Path | Owns |
|---|---|
| `typescript/bin/` | TypeScript daemon and bridge entry points |
| `typescript/src/` | SDK runtime, actor/hub/queue, Pi-RPC adapter, bridge, catalog, config, debug HTTP, logging, metrics |
| `typescript/test/` | runtime, protocol, lifecycle, CLI, and executable tests |
| `protocol/` | retained Go JSONL codec, messages, and capability table |
| `config/` | retained Go token/port paths, discovery, and provisioning |
| `piargs/` | retained Go Pi argument parser |
| `gwclient/` | retained public Go client and wire test fixture |
| `examples/chat/` | runnable Go integration example |
| `packaging/` | systemd user unit |
| `test/e2e/` | Emacs + pilish suites against the TypeScript executables |
| `docs/` | protocol, design, parity map, and development guide |

## Pitfalls

- The TypeScript daemon embeds Pi's SDK; it does not run `pi --mode rpc` as a
  child process. Runtime arguments configure the SDK session. `pi` on `PATH` is
  used to locate the installed SDK/version, not to select a CLI override.
- New sessions use the requesting client's working directory; restored sessions
  use the directory recorded with the session file.
- A Pi SDK failure marks the actor crashed. Do not automatically recreate it;
  explicit reattach performs lazy recovery.
- `gw_delete_session` tombstones the canonical path for the daemon lifetime;
  stale connections are unbound and cannot silently create a replacement.
- The Pilish E2E runner uses only the local scripted provider. Its fake-named
  shared contract selection still runs against the real SDK and TypeScript
  daemon/bridge, not a fake-Pi subprocess.
