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

> **Status of this document.** The purpose above is settled. The concrete
> deployment and session-lifecycle decisions that follow from it are **not yet
> decided**; they are listed under [Open design decisions](#open-design-decisions)
> and are being resolved with the operator one at a time.

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
| [`cmd/pi-gateway`](cmd/pi-gateway) | Go reference skeleton (stdlib only): `serve` + `connect` |

## TL;DR of the design

```text
 third-party UI (pilish over ssh) ─┐
 Slack bot / other integration ────┼── local conn ──► pi-gateway serve (daemon)
 another pilish tab ───────────────┘                        │
                                                            ├─ SessionActor: one per session, sole pi writer
                                                            ├─ Hub: ordered event log (seq) + replay ring
                                                            ├─ Arbiter: turn ownership / handoff
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
- Late joiners / reconnects replay from a `seq` cursor, or resync via
  `get_entries since=<entryId>` + snapshot.
- Concurrent prompts are arbitrated by a turn lease; `steer`/`follow_up` are
  always safe and let clients interject without stealing the floor.

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
  not restart pi. Restarting pi is an explicit daemon operation (e.g.
  `pi-gateway reload <session>`, or a `gw_reload_session` control message) that
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
provisional (live-only) and will be revisited after implementation.

## Running the reference skeleton

> **The current Go skeleton predates the settled design.** It is a prototype
> with one `pi-gateway` binary and `serve`/`connect` subcommands. The settled
> packaging is two binaries — `pi-gatewayd` (server) and `pi-gateway` (client)
> — plus token auth, the daemon-owned queue, and the rest of the decision log.
> The skeleton will be reworked; treat `docs/protocol.md` draft 2 as the target.

Requires Go 1.22+ and a `pi` binary on `PATH` (or `-pi <path>`).

### Third-party pi UIs: the `connect` bridge

`pi-gateway` is split into a **server** that owns pi and a **bridge** that is
just another client of that server. The bridge presents raw pi RPC on stdio
(or TCP) and speaks the gateway protocol upstream.

```bash
# server: owns pi, sole writer
pi-gateway serve --listen 127.0.0.1:7331 --session /tmp/shared.jsonl -- --provider openai

# third-party UI: spawn this instead of `pi --mode rpc`
pi-gateway connect --stdio --server 127.0.0.1:7331 --session-id shared
```

The UI sees **only** pristine pi messages (no `gw_*`, no injected fields) with
`id`s restored, so it needs no changes. Additional gateway clients can attach
to the same server session at the same time.

`connect --listen <addr>` accepts raw pi RPC over TCP and bridges each
connection as a **separate server client**, so several raw UIs can share one
session. `--replay` forwards replayed history; by default the bridge attaches
live-only and the UI rebuilds state via `get_state`/`get_messages` as usual.

### Gateway-protocol clients (TCP)

```bash
# terminal 1 — start the server (spawns pi itself)
go run ./cmd/pi-gateway serve --listen 127.0.0.1:7331 --session /tmp/demo.jsonl --mode exclusive

# terminal 2 — drive it with any JSONL client
nc 127.0.0.1 7331
# then paste the handshake line, followed by pi commands, one JSON per line:
# {"type":"gw_hello","protocol":1,"sessionId":"default","client":{"name":"me","capabilities":["observe","prompt","control"]}}
# {"type":"prompt","id":"req-1","message":"hello"}
```

Run the tests (unit + two-client fan-out + bridge end-to-end against a fake pi):

```bash
go test -race ./...
```

### Skeleton scope

Implemented: strict JSONL framing, ordered hub + replay, `gw_hello`/`gw_resume`,
fan-out, targeted response routing with `id` namespacing, turn arbitration
(`exclusive`/`queue`/`owner-only`/`read-only`), extension-UI first-response-wins,
snapshot resync, per-connection backpressure/disconnect, and the two-role
topology: **`serve`** (owns pi) and **`connect`** (a bridge client for raw pi
stdio/TCP compatibility).

Deliberately left out (see roadmap in `docs/design.md`): WebSocket transport,
auth/RBAC beyond capability flags, SQLite session registry and adoption across
gateway restarts, crash auto-restart, and metrics.

