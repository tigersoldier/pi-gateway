# Go-to-TypeScript runtime replacement

The production daemon and bridge are TypeScript in this worktree. The Go
`cmd/` entry points and `internal/` server implementation have been removed.
The exported Go `protocol`, `config`, `piargs`, and `gwclient` packages, plus
`examples/chat`, remain available to Go integrations.

## Scope and compatibility

This is an implementation replacement, not a protocol revision. Keep gateway
protocol v1, the Pi-RPC bridge surface, session lifecycle, permissions,
queueing, diagnostics, metrics, and operational behavior. Do not add lifecycle
IDs, protocol fields, commands, permissions, or other features as part of the
cutover. The Go protocol package remains the source reference for wire shapes
and capability behavior.

The TypeScript daemon uses Pi's SDK in-process, with one SDK `AgentSession` per
gateway session. It requires Node.js >=22.19.0 and resolves the `pi` executable
from `PATH`. The TypeScript CLI intentionally omits the former Go daemon's
`--pi` override. This is an accepted CLI compatibility exception; strict CLI
parity is not claimed.

## Runtime test mapping

[`test-parity.md`](test-parity.md) maps the Go runtime and bridge test assertions
to TypeScript tests. The mapped Go daemon, bridge, and internal server tests
were retired with those implementations. Go client tests remain and use a
wire-level fixture in `gwclient/testserver_test.go`; the fixture is not a
production server or a replacement for TypeScript runtime tests.

Pilish ERT suites run through the real TypeScript daemon and bridge entry points
with the real Pi SDK and a scripted loopback provider. The provider avoids
credentials and external model inference.

## Verification

Latest cutover validation in this worktree:

- `cd typescript && npm test` — 222/222 passed.
- `cd typescript && npm run typecheck` — passed.
- `go test -race ./...` — passed for the retained Go client and example packages.
- `test/e2e/run.sh --suite both --no-deps --rm` — suite A 17/17 and suite B
  9/9 through the TypeScript entry points and local provider.

The accepted `--pi` omission remains an explicit compatibility exception. The
final cutover has not received a fresh independent review; prior review
findings on the TypeScript implementation were fixed and covered by tests.
