# pi-multi-client — a session daemon and `pi` replacement

## Purpose

Serve as a **drop-in replacement for the `pi` executable** for third-party pi
UIs, backed by a **background session service** that outlives any single client
connection.

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

> **Status.** The design is settled (see [Design decisions](#design-decisions))
> and **M1 is implemented**: the `pi-gatewayd` daemon and the `pi-gateway`
> bridge, with token auth, port discovery, session attach/rebinding, the
> daemon-owned queue, replay, hibernation, and orphan reaping. See
> [Implementation status](#implementation-status) for M1 scope and what is
> deliberately deferred.

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
| [`cmd/pi-gatewayd`](cmd/pi-gatewayd) | M1 daemon: owns pi sessions, serves the gateway protocol on loopback TCP |
| [`cmd/pi-gateway`](cmd/pi-gateway) | M1 bridge: the binary UIs spawn in place of `pi` |

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

## Design decisions

Every decision below was resolved with the operator (or by research) and is
recorded with its evidence. Nothing here should be treated as provisional
unless it is explicitly marked so.

### Resolved

- **Session addressing / reattach key** (research). pilish never passes
  `--session`; it spawns a fresh `pi --mode rpc` and then sends
  `switch_session {sessionPath}` with the process-local session file path
  (`pilish-reload`, `pilish--resume-selected-session`; the layer's revive path
  does the same). The reattach key is the **session file path**, and
  `switch_session` is the attach/route operation. Full contract and citations:
  `docs/design.md` §1.2.
- **Local transport** (operator). **Loopback TCP** (`127.0.0.1:<port>`), chosen
  so an HTTP debug endpoint can be added later. All clients are local. A
  **token** is required because any local process can reach a loopback port.
- **Network surface** (operator). **Local-only** — no network listener, no TLS.
- **Session lifecycle** (operator). **Idle timeout.** After the last client
  detaches and the agent settles, pi keeps running for a configurable grace
  period (default 15 minutes), then hibernates: pi stops and only the session
  file remains. Never mid-turn. The next attach respawns pi and loads the file.
- **Cross-client concurrency** (operator). **Queue.** Any attached client may
  prompt; prompts run FIFO one turn at a time; mid-turn prompts queue as
  follow-ups (or `steer` when asked) and are never rejected with `turn_held`.
- **Session identity / catalog** (operator). **Hybrid, no new store.** The path
  is canonical for pilish. Attach also accepts a session **name**, resolved by
  scanning session files and live actors; missing or ambiguous names fall back
  to the path. No persisted id→file mapping.
- **Packaging / invocation** (operator). **Configure the UI, no shadowing.**
  UIs point their pi-executable setting at the client (`pilish-executable`,
  `pilish/remote-executables`). The client forwards pi parameters to the
  daemon.
- **Non-RPC invocation** (operator). **`--version`/`--help` only.** The daemon
  answers from the real pi binary it manages; the client prints the output and
  exits. Every other non-RPC mode, including TUI, errors.
- **Common pi parameters** (operator). **Split by applicability.** Accepted:
  trust, extensions, resource/tool toggles, `--provider`, `--model`,
  `--thinking`, `--name`, `--session-dir`, `--no-session`, `--api-key`. Other
  pi options error. Not live → applied at spawn. Live → runtime-applicable
  params (`--model`, `--provider`, `--thinking`, `--name`) are applied via RPC
  to shared session state. Spawn-only params are compared against the session's
  recorded spawn config: **identical values are no-ops; only a conflicting
  value fails the attach** with a clear error. (Required for pilish, which
  re-sends `--approve` on every reload.) **Confirmed:** runtime parameter
  changes are session-global, and every client is notified with
  `gw_state_changed`.
- **HTTP debug endpoint** (operator). **Two ports.** The session listener stays
  raw TCP + JSONL; a separate loopback HTTP listener is added later for
  debug/status/metrics, sharing the same token.
- **Daemon restart** (operator). **Lazy re-adoption.** No pi processes at boot;
  the next attach loads the session file. In-flight turns are lost (accepted).
- **Mid-turn attach** (operator, *provisional — revisit after implementation*).
  **Live-only.** A client attaching to a running turn joins the stream where it
  is: it may see a truncated `message_update` fragment until `message_end`
  delivers the complete assistant message, while `tool_execution_update`
  carries accumulated output. No turn replay and no snapshot synthesis for now.
- **`pilish-reload` semantics** (operator). **Attach-only + explicit daemon
  reload.** pilish's reload becomes a re-attach (state/history refresh); it does
  not restart pi. Restarting pi is an explicit daemon operation (a
  `gw_reload_session` control message) that
  refuses while other clients are attached or a turn is running, unless forced.
- **`switch_session` re-routing** (operator). **Server-side rebinding.** The
  server intercepts `switch_session`, resolves the target (file path or session
  name), rebinds that connection to the target session actor (hub subscription
  and command target), synthesizes pi's exact response
  (`success` + `data.cancelled`), and routes the client's following
  `get_state`/`get_messages`/`get_commands` to the new actor. The bridge remains
  a dumb relay and the client keeps one connection.
- **Token provisioning** (operator). **Daemon-generated token at a fixed path.**
  On first start the daemon creates a random token at
  `~/.config/pi-gateway/token` (mode `0600`); the client reads it by default and
  `--token-file` overrides. The token is stable across daemon restarts;
  rotating means deleting/regenerating the file. The future HTTP debug
  endpoints use the same token.
- **Port discovery** (operator). **Hybrid.** The daemon always writes the bound
  port to `~/.config/pi-gateway/port`; the client prefers that file and falls
  back to the fixed default `127.0.0.1:7331`; `--server`/`--port` override.
- **Session creation** (operator). **Explicit create + creator tags.** Non-pi
  clients may create sessions explicitly (a control command returning path and
  name); sessions record the creating client identity/tags, exposed in the
  catalog, so an integration can list and delete its own. pilish's implicit
  creation (fresh client → `get_state` → new session) keeps working. Any client
  may attach to any existing session by path or name — including a pilish
  session. The Slack integration itself is out of scope for this repository.
- **Client kind** (operator). **Tagging only; no gateway policy.** Clients
  declare a kind at `gw_hello` (`pilish`, `integration`, `bot`, `observer`).
  The daemon tags events, queue entries, presence, and catalog entries with it,
  but applies no kind-based policy; authority stays capability-based.
  Integration-specific capability handling and formatting belong to the
  integration's own code, not to the gateway.
- **`new_session` under sharing** (operator). **Create + rebind only the
  requester.** pi's `new_session` is intercepted: the daemon creates a new
  session and rebinds just that connection to it, leaving other clients on the
  previous session (which gets no state-change notification). The response
  stays pi-shaped (`success` + `data.cancelled`) and the following `get_state`
  reports the new file, so pilish's `/new` works unchanged.
- **`fork` / `clone` under sharing** (operator). **Refuse when shared; forward
  when alone.** pi's `fork`/`clone` switch the live pi process to a newly
  created session file, so they are refused with `shared_session` whenever
  other clients are attached (no client is ever moved). When the requester is
  the only client, the daemon forwards the command, adopts the new file as a
  new session, and rebinds the requester. Responses stay pi-shaped.
- **Prompt queue ownership** (operator). **The daemon owns the queue.** It
  accepts prompts, holds them in a per-client tagged FIFO, and feeds pi one at
  a time as a normal `prompt` when the session is idle. `steer` is a real-time
  interjection forwarded to pi immediately; `follow_up` is enqueued like a
  mid-turn prompt. `gw_queue` reports the daemon queue with author/kind tags.
  pi's own queue is not used for cross-client ordering.
- **`abort` / `clear_queue` under sharing** (operator). **pi-native,
  session-wide.** Any client may `abort`; it stops the shared turn, and queued
  prompts remain and run afterwards (pi-native behavior). `clear_queue` clears
  the entire daemon queue plus pi's forwarded steer queue and returns all
  cleared text to the caller — which may include prompts queued by other
  clients. A forwarded `steer` cannot be withdrawn except by this session-wide
  clear.
- **Extension dialog ownership** (operator). **Turn author, with fallback.**
  Blocking dialogs (`select`/`confirm`/`input`/`editor`) are routed to the
  author of the running turn (`gw_turn.author`); if that client lacks `ui` or
  has disconnected, the daemon falls back to the most recently active
  `ui`-capable client, then to pi's own timeout default. With no turn running,
  the same fallback order applies. Fire-and-forget methods broadcast to all.
- **Lazy binding / orphan reaping** (operator). **Confirmed, with aggressive
  reaping.** An unbound connection creates a session on its first session
  command, while `switch_session` binds without creating one (pilish's revive
  path creates then switches). A session with **no messages and no clients** is
  stopped after a short grace (seconds), not the full idle timeout.
- **Packaging: two binaries** (operator). **One server, one client.** The
  **server** binary is the daemon that owns pi sessions and serves the gateway
  protocol (run as a systemd user service). The **client** binary is always the
  bridge: UIs spawn it in place of `pi`, it accepts pi argv (including the
  `--mode rpc` pilish appends, which the client consumes) and forwards the
  accepted pi parameters. Because the client has exactly one job it needs no
  subcommands and no auto-dispatch. **Binary names (operator): `pi-gatewayd`
  (server) and `pi-gateway` (client).**
- **`--help`** (operator). **Client-local.** `pi-gateway --help` prints the
  client's own usage without contacting the daemon; `pi-gatewayd --help` prints
  the daemon's. `--version` stays daemon-answered so the reported pi version
  matches the managed binary.

### Open

None. Every gap found while reviewing `docs/protocol.md` draft 2 has been
resolved and recorded above. The **mid-turn attach** entry is deliberately
provisional (live-only) and is the first item to revisit now that M1 is
implemented.

## Implementation status

**M1 (core daemon and client) is implemented.**

| Area | M1 |
| --- | --- |
| Binaries | `pi-gatewayd` (daemon), `pi-gateway` (bridge) |
| Transport/auth | loopback TCP, token file (0600), `--server`/`--port`/`--token-file`, port-file discovery with fallback to `127.0.0.1:7331` |
| Sessions | attach by path, implicit creation on the first command, server-side `switch_session` rebinding, `new_session` rebinding only the requester |
| Process | one pi per session, crash/exit reporting, idle hibernation, short-grace reaping of never-messaged sessions |
| Commands | passthrough with `id` namespacing, spawn-param conflict detection, runtime-parameter application via RPC plus `gw_state_changed` |
| Queue | daemon-owned per-client tagged FIFO, immediate `steer`, `abort`, session-wide `clear_queue` returning cleared text |
| Events | per-session ordered log with `gw_seq`, `gw_turn`, `gw_queue`, `gw_presence`, `gw_session_state`, `gw_error`; `liveOnly` and `resume.sinceSeq` replay; `gw_snapshot` resync |
| Extension UI | broadcast plus first-response-wins with `ui_stale` (full turn-author routing lands in M2) |

Deliberately deferred to M2+ (roadmap in `docs/design.md` §14): session
catalog and name resolution (`gw_list_sessions`), `gw_new_session`,
`gw_reload_session`, `fork`/`clone` adoption, backpressure coalescing and
`gw_lag`, and the HTTP debug listener. Until then those commands answer
`not_supported`, and `switch_session` accepts file paths only.

### Running M1

```bash
go build ./cmd/pi-gatewayd ./cmd/pi-gateway

# Daemon: owns pi sessions (run as a systemd user unit in production).
# It writes the token and port file under ~/.config/pi-gateway/.
./pi-gatewayd --pi /usr/local/bin/pi

# A UI spawns this instead of `pi`; --mode rpc is accepted and consumed.
./pi-gateway --mode rpc --approve

# The managed pi version, answered by the daemon (pilish's dependency check).
./pi-gateway --version

# Unit + daemon/bridge end-to-end tests (fake pi, no real pi needed).
go test -race ./...
```

Point `pilish-executable` (local) at the `pi-gateway` binary, or the layer's
`pilish/remote-executables` path (remote). Do not shadow the real `pi`.

## Repository layout

```text
cmd/pi-gatewayd/     daemon: listener, auth, session table, connections
cmd/pi-gateway/      bridge: gateway protocol upstream, raw pi RPC on stdio
internal/client/     bridge implementation (argv, token/port, relay)
internal/config/     token and port file paths, defaults
internal/daemon/     session table, attach/rebind, spawn-param checks
internal/piargs/     accepted pi parameter parsing (shared daemon/client)
internal/protocol/   strict JSONL codec, gw_* messages, id namespacing
internal/session/    SessionActor, Hub, PromptQueue, PiProcess
internal/fakepi/     fake pi used by the tests
internal/testutil/   test helpers (build fake pi, raw protocol client)
```
