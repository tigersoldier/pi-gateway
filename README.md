# pi-gateway — a session daemon and `pi` replacement

## Purpose

`pi-gateway` serves as a **drop-in replacement for the `pi` executable** for
third-party pi UIs, backed by a **background session service** that outlives any
single client connection.

Three goals drive the design:

1. **`pi` replacement over ssh.** A third-party UI — in particular the Emacs
   `pilish` frontend connecting over TRAMP/ssh — invokes this instead of
   `pi --mode rpc` on the remote host. If the ssh/TRAMP connection drops, the
   client process is killed, but the **session keeps running** on the server;
   reconnecting re-attaches to the same session instead of losing it.
2. **One server, many integrations.** The same server also serves other
   integrations — for example a Slack agent bot — that are **not** part of this
   repository. Those integrations talk to the gateway protocol.
3. **Multiple clients per session.** The user can switch between pilish, Slack,
   or other integrations and continue the same work in the same session.

The server is intended to run as a **background service** (e.g. a systemd user
unit) on the machine that owns the sessions, and to accept **local connections**
from clients. Clients are the processes third-party UIs spawn in place of `pi`.

> **Status.** The design is settled and **M1–M3 are implemented**: the
> `pi-gatewayd` daemon and the `pi-gateway` bridge, with token auth and
> capability roles, port discovery, session attach/rebinding by path or name,
> the session catalog, reload and fork/clone policies, turn-author UI routing,
> the daemon-owned queue, replay/durable resume, coalescing, hibernation,
> orphan reaping, a systemd user unit, the read-only debug listener
> (`/status` `/catalog` `/metrics`), and structured logging. Scope, the
> [decision log](docs/development.md#decision-log) and what is deliberately
> deferred live in [`docs/development.md`](docs/development.md).

## Background: why pi alone cannot do this

`pi` cannot be driven by multiple clients on the same session, and a `pi`
process dies with its client. Evidence from the installed
`@earendil-works/pi-coding-agent`:

1. **RPC mode is a single stdin/stdout pipe.** `pi --mode rpc` reads one JSONL
   command stream from stdin and writes one JSONL event/response stream to
   stdout (`dist/modes/rpc/rpc-mode.js`, `dist/modes/rpc/jsonl.js`). There is no
   `listen()` / server / socket anywhere in `dist/`. One process == one client.
2. **The agent loop is single-streaming.** `prompt` while streaming without
   `streamingBehavior` is rejected; `session.isStreaming` is a single flag. A
   second client cannot run a concurrent turn (see `docs/rpc.md`).
3. **The session file has no multi-writer safety.** `SessionManager` keeps
   `fileEntries` / `byId` / `leafId` in memory, appends with `appendFileSync`,
   rewrites the whole file on branch/migration (`_rewriteFile` truncates), and
   uses `openSync(path, "wx")` (O_CREAT|O_EXCL) on first flush
   (`dist/core/session-manager.js`). Two `pi` processes on one session file
   diverge and clobber each other.
4. **No late-join/replay/fan-out.** Events are written once to the one stdout;
   there is no event log, cursor, or subscription mechanism.

`pi` *does* support queued participation within one client
(`steer` / `follow_up` / `streamingBehavior`), but that is queueing, not
multi-client parallelism. So: **true parallel turns on one session are not
possible without changing pi.** What *is* possible — and what this design
provides — is **concurrent participation with a single ordered view**, by
keeping exactly one pi process as the session's sole writer and multiplexing
many clients around it.

Prior art that solves a related part of the problem: `marcfargas/pi-server`
and `tryingET/pi-server` (detach/reconnect, one client at a time). This project
targets the broader goal above: many concurrent clients, a session that
survives client death, and non-pi integrations.

## Documents

| File | Contents |
|------|----------|
| [`docs/design.md`](docs/design.md) | Full architecture, invariants, arbitration, replay, failure modes |
| [`docs/protocol.md`](docs/protocol.md) | Client-facing wire protocol (pi-compatible + thin gateway envelope) |
| [`docs/development.md`](docs/development.md) | Decision log, implementation status, repository layout, building and testing |
| [`test/e2e/README.md`](test/e2e/README.md) | The Emacs + pilish end-to-end suites (fake-pi and real-model lanes) |
| [`cmd/pi-gatewayd`](cmd/pi-gatewayd) | daemon: owns pi sessions, serves the gateway protocol on loopback TCP |
| [`cmd/pi-gateway`](cmd/pi-gateway) | bridge: the binary UIs spawn in place of `pi` |

## TL;DR of the design

```text
 third-party UI (pilish over ssh) ─┐
 Slack bot / other integration ────┼── loopback TCP ──► pi-gatewayd (daemon)
 another pilish tab ───────────────┘     (token auth)        │
                                                             ├─ SessionActor: one per session, sole pi writer
                                                             ├─ Hub: ordered event log (seq) + replay ring
                                                             ├─ PromptQueue: daemon-owned per-client FIFO
                                                             └─ UIBroker: extension_ui_request routing
                                                             ▼
                                               pi --mode rpc --session X
                                               (survives client disconnects)
```

- Clients speak pi's RPC protocol verbatim; a thin `gw_*` envelope adds
  multi-client features for non-pi integrations.
- One pi process per session ⇒ pi's invariants are preserved, and the session
  file has exactly one writer.
- A session outlives its clients: the daemon keeps pi running across ssh
  drops, and clients re-attach.
- All commands are linearized by the actor; all events get a global `seq`.
- Late joiners / reconnects replay from a `seq` cursor, or resync via a
  snapshot.
- Concurrent prompts are queued by the daemon (per-client tagged FIFO, one
  turn at a time); any client may prompt and `steer` interjects immediately.

## Implementation status

**M1 (session core), M2 (multi-client sharing) and M3 (operations) are in**:
`pi-gatewayd` + `pi-gateway`, one `pi` process per session, attach by path or
name, the daemon-owned prompt queue, replay/durable resume, token roles and
capabilities, the read-only debug listener, structured logging, metrics, and a
systemd user unit. M4 (session groups across daemons, WebSocket transport) is
optional and not started. The per-area table and the reasoning belong to
[`docs/development.md`](docs/development.md#implementation-status).

## Usage

### Build

```bash
go build ./cmd/pi-gatewayd ./cmd/pi-gateway
```

### Run the daemon

The daemon owns the pi sessions. On first start it creates its token under
`~/.config/pi-gateway/` (mode `0600`) together with the `port` file clients
discover it through; `--state-dir` moves that directory.

```bash
./pi-gatewayd --pi /usr/local/bin/pi                 # text logs at info level
./pi-gatewayd --log-format json --log-level debug    # structured, verbose
```

### Run it as a systemd user unit

```bash
install -Dm644 packaging/pi-gatewayd.service ~/.config/systemd/user/pi-gatewayd.service
systemctl --user daemon-reload && systemctl --user enable --now pi-gatewayd
journalctl --user -u pi-gatewayd -f
```

The unit is `Type=simple` with `Restart=on-failure` and `ExecReload` sending
`SIGHUP` (which re-reads `tokens.json` without restarting).

### Point a UI at the bridge

A UI spawns the bridge instead of `pi`. `--mode rpc` (which pilish appends) is
accepted and consumed, and the accepted pi parameters are forwarded to the
daemon.

```bash
./pi-gateway --mode rpc --approve   # what a UI runs
./pi-gateway --version              # the managed pi version, answered by the daemon
```

Point `pilish-executable` (local) at the `pi-gateway` binary, or the layer's
`pilish/remote-executables` path (remote). Do **not** shadow the real `pi`:
configure the UI's executable setting instead.

### Operational endpoints

A second loopback listener serves read-only operational data with no token.

```bash
curl -s 127.0.0.1:7332/status    # version, uptime, sessions, clients, queue
curl -s 127.0.0.1:7332/catalog   # same rows as gw_list_sessions
curl -s 127.0.0.1:7332/metrics   # Prometheus text
```

It exposes session paths, names, and titles to any local process, so keep it on
loopback and treat it as read-only. `--no-debug` disables it.

### Restricted tokens

```bash
# Mint a token for one integration, then reload in place.
./pi-gatewayd --provision-token --token-name slack --token-role operator
systemctl --user reload pi-gatewayd     # or: kill -HUP <pid>
```

The daemon-generated token always grants everything. Restricted tokens live in
`~/.config/pi-gateway/tokens.json` (mode `0600`) and name a preset role
(`admin`, `operator`, `observer`) or an explicit capability list. An observer
may read (list sessions, `get_*`, export) and receive events, but cannot
prompt, `steer`/`abort`, `switch_session`, or run `bash`; shared-state
mutations need `control`. [`docs/protocol.md`](docs/protocol.md) §10 maps every
command.

Pick the role for what the integration does, not for what it reads:

- `operator` attaches to existing sessions, prompts, steers, aborts and
  answers dialogs, but cannot create sessions, name them, or change the model.
- `admin` (the daemon-generated token's level) is needed for
  `gw_new_session`, so a bot that creates its own sessions, names them
  (`set_session_name`), or reconfigures them (`set_model`, `compact`, …) needs
  `admin` or an explicit `--token-caps` list containing `admin`/`control`.

### Integrations

The gateway protocol is also the integration API: non-pi clients (a Slack bot,
a dashboard) connect to the daemon directly instead of going through the
bridge. [`docs/protocol.md`](docs/protocol.md) is the reference; the shapes an
integration needs first:

```text
gw_list_sessions -> {"sessions":[{path,name,title,cwd,live,clients,...}]}
gw_new_session   -> {"path","name","sessionId"} (the requester is rebound)
gw_reload_session-> restart pi for a session (control capability)
```

A Go integration can import the client library instead of implementing the
connection loop itself. It is deliberately outside `internal/`, so another
module can depend on it:

```go
import "github.com/tigersoldier/pi-gateway/gwclient"

c, err := gwclient.Dial(ctx, gwclient.Config{Name: "slack", Kind: "bot"})
if err != nil {
	return err
}
defer c.Close()

sess, err := c.NewSession(ctx, gwclient.NewSessionRequest{Cwd: "/work/repo"})
if err != nil {
	return err
}
if _, err := c.Prompt(ctx, "summarise the open issues"); err != nil {
	return err
}
for ev := range c.Events() {
	if ev.Type == "message_end" {
		// render the assistant message into the chat
	}
}
```

`gwclient` covers discovery and the handshake, id-correlated `Do`/`Send`, the
event stream, and session/catalog/prompt/interject/dialog helpers. For a
long-lived bot it also provides:

- **Reconnect and resume** — `Cursor`/`LastSeq`/`LeafID` report what the client
  has consumed; `Reconnect` dials again with that cursor and the daemon answers
  with a replay window or a `gw_snapshot` when the cursor is unreplayable
  (`Welcome.ResyncRequired`). Persist the cursor to survive a process restart.
- **Callback delivery** — `Config.OnEvent` hands every frame to a function on
  the read goroutine, so a bot does not need a drain goroutine and cannot
  overflow the event buffer mid-turn.
- **Typed events and dialogs** — `Event.Turn`/`Queue`/`Snapshot`/`SessionState`
  and friends, plus `Event.UIRequest` with `BlockingUIMethod`, so extension
  dialogs (tool approval, `confirm`/`select`/`input`/`editor`) can be routed
  without hand-rolled JSON.
- **Images** — `Prompt`, `Steer` and `FollowUp` accept `protocol.ImageContent`
  (base64 + MIME type) for screenshot/attachment messages.
- **The rest of pi's surface** — typed wrappers for models, thinking levels,
  steering/follow-up modes, compaction, auto-retry, `bash` (with streamed
  `BashUpdate` callbacks), entries/tree/forks/stats/export, and
  `set_session_name`.

A bot that creates its own sessions needs a token with `admin` (see
[Restricted tokens](#restricted-tokens)); one that only attaches to sessions it
finds in the catalog can use `operator`.

A runnable interactive CLI is [`examples/chat`](examples/chat):
`go run ./examples/chat` gives streaming rendering, `!queue`/`!steer`/`!abort`,
and `/name` for commands, prompt templates and skills.

## CLI reference

### `pi-gatewayd` reference

| Flag | Default | Meaning |
|---|---|---|
| `--listen <host:port>` | `127.0.0.1:7331` | loopback address for the session listener |
| `--port <n>` | — | port shorthand; overrides `--listen` |
| `--state-dir <dir>` | `~/.config/pi-gateway` | where `token`, `tokens.json`, `port` and `debug-port` live |
| `--token-file <path>` | `<state>/token` | token file (mode `0600`) |
| `--tokens-file <path>` | `<state>/tokens.json` | restricted-token file (mode `0600`) |
| `--pi <path>` | `pi` | the pi binary to manage |
| `--idle-timeout <dur>` | `15m` | keep a session warm this long after the last client detaches |
| `--short-grace <dur>` | `10s` | grace for a session that never received a message and has no clients |
| `--session-dir <dir>` | — | extra session directory to scan for the catalog (repeatable) |
| `--delta-flush <dur>` | `50ms` | coalescing window for streaming deltas, per client |
| `--log-level <level>` | `info` | `debug`, `info`, `warn`/`warning`, `error` (case-insensitive) |
| `--log-format <fmt>` | `text` | `text` or `json` |
| `--debug-addr <host:port>` | `127.0.0.1:7332` | read-only, unauthenticated debug listener |
| `--debug-port <n>` | — | port shorthand; overrides `--debug-addr` |
| `--no-debug` | off | disable the debug listener |
| `--provision-token` | — | mint a restricted token and print it (needs `--token-name` and `--token-role` or `--token-caps`) |
| `--token-name <name>` | — | name for `--provision-token` |
| `--token-role <role>` | — | `admin`, `operator` or `observer` |
| `--token-caps <csv>` | — | explicit capability list instead of a role |
| `--version` | — | print the daemon's own version |

`SIGINT`/`SIGTERM` stop all sessions (abort, close stdin, grace, then `SIGKILL`);
`SIGHUP` re-reads `tokens.json` without restarting. Both listeners refuse
non-loopback addresses. The state directory also follows `PI_GATEWAY_CONFIG_DIR`
for the daemon (and `XDG_CONFIG_HOME` when that is unset).

### `pi-gateway` (bridge) reference

| Flag | Meaning |
|---|---|
| `--server <host:port>` | daemon address (default: the `port` file, then `127.0.0.1:7331`) |
| `--port <n>` | daemon port on `127.0.0.1` |
| `--token-file <path>` | token file (default `<state>/token`) |
| `--mode rpc` | accepted and consumed; any other mode is an error |
| `--version` | print the managed pi version (answered by the daemon) |
| `--help`, `-h` | client-local usage |

It also accepts the pi options listed in `docs/protocol.md` §4.3 and forwards
them to the daemon; anything else is refused before connecting. Environment:
`PI_GATEWAY_CLIENT_NAME` (default `pi-gateway`), `PI_GATEWAY_CLIENT_KIND`
(default `pilish`), `PI_GATEWAY_CONFIG_DIR`. Exit codes: `0` success, `2` usage
error (bad flag, unsupported pi option), `3` daemon unreachable or handshake
refused.

## Repository layout

The tree, and what each package owns, is in
[`docs/development.md`](docs/development.md#repository-layout).
