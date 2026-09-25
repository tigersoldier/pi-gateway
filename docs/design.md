# Design: `pi-gateway` — a pi session daemon and `pi` replacement

Status: draft · Target: Go 1.22+ · Depends on: `pi --mode rpc`

> **Reading order.** `README.md` → *Design decisions* is the canonical decision
> log, and `docs/protocol.md` draft 2 is the authoritative protocol. This whole
> document reflects the settled design; the single provisional item (mid-turn
> attach) is marked where it appears.

---

## 1. Problem statement and goals

Goal: provide a **drop-in replacement for the `pi` executable** that third-party
pi UIs — in particular Emacs `pilish` connecting over TRAMP/ssh — can invoke in
pi's place, backed by a **background service** that owns sessions and outlives
any client connection.

- **G1 — survive client death.** A client is spawned per connection (for
  example over ssh). When it dies (ssh drop, editor close), the session and its
  pi process keep running on the server; a later client re-attaches.
- **G2 — pi-compatible.** A UI that spawns `pi --mode rpc` can spawn the client
  instead and see pristine pi RPC on stdio, with no UI changes.
- **G3 — many clients, one session.** Several clients (pilish, a Slack bot,
  other integrations) can be attached at once, and the user can switch between
  them and continue the same work.
- **G4 — non-pi integrations.** Integrations that are not pi UIs (and are not
  part of this repository) can talk to a general gateway protocol.
- **G5 — background service.** The server runs as a service (e.g. a systemd
  user unit) and accepts **local connections** from clients.

The underlying problem is unchanged: `pi --mode rpc` exposes one session over
one stdin/stdout pair, so N clients cannot attach and a client's death kills
the session. We want those N clients to:

- all observe the same conversation, in the same order;
- be able to submit prompts, steering messages, follow-ups, aborts, etc.;
- join late or reconnect without losing or duplicating state;
- never corrupt the session file or pi's in-memory state.

### 1.1 Deployment model

```text
  Emacs/pilish ──ssh/TRAMP──► client process ─┐
  Slack bot (separate repo) ──────────────────┼── loopback TCP ──► pi-gateway serve (daemon)
  other integrations ─────────────────────────┘   (localhost only)         │
                                                                          ▼
                                                          pi --mode rpc --session X
```

The daemon owns every pi process and is the only writer to each session file.
Clients are thin: they translate their transport/protocol to the gateway and
hold no session state. This is what lets a session survive an ssh drop and lets
multiple integrations share it.

Settled deployment facts:

- Every client — the pilish client spawned over ssh, the Slack bot, and other
  integrations — runs on the daemon's machine. The daemon has **no network
  listener**.
- Clients reach the daemon over **loopback TCP** (`127.0.0.1:<port>`), chosen
  so an HTTP debug endpoint can share the same local transport (separate
  loopback port, raw
  JSONL session port unchanged).
- Because any local process can reach a loopback port, clients authenticate
  with a **token**: the daemon generates `~/.config/pi-gateway/token` (mode
  `0600`) on first start, the client reads it by default (`--token-file`
  overrides). The token always grants the full capability set; restricted
  tokens live in `~/.config/pi-gateway/tokens.json` (§10).
- The **HTTP debug listener** is a second loopback port (`127.0.0.1:7332` by
  default) serving read-only `/status`, `/catalog`, and `/metrics`. It is
  deliberately **unauthenticated** (recorded in README → Design decisions), so
  it exposes operational and catalog metadata — paths, names, titles — to any
  local process, and must never gain a mutating endpoint.
- **Port discovery is hybrid**: the daemon writes the bound port to
  `~/.config/pi-gateway/port`; the client prefers that file and falls back to
  `127.0.0.1:7331` (`--server` overrides).
- Sessions **hibernate on an idle timeout**: after the last client detaches and
  the agent settles, pi keeps running for a configurable grace period (default
  15 minutes), then stops; the session file remains and pi is respawned on the
  next attach. Pi is never stopped mid-turn.
- Cross-client concurrency is **queue** semantics: any client may prompt, the
  daemon runs prompts FIFO one turn at a time, and a mid-turn prompt queues as
  a follow-up instead of being rejected.
- The **daemon owns the prompt queue**: a per-client tagged FIFO fed to pi one
  prompt at a time; `steer` is forwarded immediately as a real interjection,
  `follow_up` is enqueued. pi's own queue is not used for cross-client
  ordering.
- Session identity is **path-canonical**: pilish attaches by session file path
  via `switch_session`. Non-pi integrations may also use a session **name**;
  the daemon resolves names by scanning session files and live actors, with no
  persisted mapping, and falls back to the path when a name is missing or
  ambiguous.
- Non-pi clients may **create** sessions explicitly; created sessions record
  creator tags in the catalog. pilish's implicit creation via a fresh
  `get_state` still works, and any client may attach to any session by path or
  name.
- Clients declare a **kind** at `gw_hello` (`pilish`, `integration`, `bot`,
  `observer`). The daemon **tags** events, queue entries, presence, and catalog
  entries with it but applies **no kind-based policy**; authority stays
  capability-based. Integration-specific capability handling and formatting
  belong to the integration's own code, not the gateway.
- The daemon **rebinds** a connection on `switch_session` (server-side); the
  bridge stays a dumb relay.
- Daemon restart is **lazy**: no pi processes are started at boot; the next
  attach loads the session file. In-flight turns are lost on daemon restart.
- **Packaging is configuration, not shadowing.** UIs point their pi-executable
  setting at the client (`pilish-executable`, `pilish/remote-executables`). The
  client forwards pi parameters to the daemon; the daemon accepts common pi
  parameters for the pi it manages. If a session is live, runtime-applicable
  parameters are applied via RPC, and a spawn-only parameter that differs from
  the session's recorded spawn config fails the attach (identical values are
  no-ops). Non-RPC invocations are limited to `--version`/`--help`, which the
  daemon answers from the real pi binary it manages; the client prints the
  output and exits. Every other non-RPC mode (including TUI) errors.

The canonical, up-to-date decision log lives in `README.md` under **Design
decisions**. As of this writing every decision is resolved except the
**mid-turn attach** behavior, which is deliberately provisional (live-only) and
will be revisited after implementation. Do not assume a behavior in code until
it is recorded there.

### 1.2 pilish compatibility contract (researched)

These facts were established by reading the installed pilish source
(`/home/pi/work/performance-fix-2`, the `downstream` branch the Spacemacs layer
tracks) and the layer (`~/.emacs.d/private/pilish`). They are **requirements**,
not choices: the client must satisfy them for pilish to survive a broken ssh
connection.

1. **pilish never passes `--session` at spawn time.** The spawn argv is exactly
   `pilish-executable` + `("--mode" "rpc")` + extra args + trust args
   (`pilish-core.el` `pilish--pi-command`; `pilish-ui.el`
   `pilish-executable` defaults to `'("pi")`). A fresh pi process therefore
   starts a **new** session.
2. **State comes from RPC after spawn.** On a fresh session pilish sends
   `get_state`, then `get_commands` (`pilish.el` `pilish--setup-session`).
   `get_state.sessionFile` is how pilish learns which file the session lives in.
3. **Resume/reattach is `switch_session {sessionPath}` on a fresh process.**
   `pilish-reload` starts a new process and immediately sends
   `switch_session` with the cached process-local path
   (`pilish-menu.el` `pilish-reload`). The layer's revive path does the same:
   spawn + `get_state`, then `pilish--resume-selected-session` →
   `switch_session` (`funcs.el` `pilish//revive-session`,
   `pilish//switch-to-session`).
4. **After a switch, pilish refreshes over the same process:** `get_state` and
   `get_messages` (later `get_commands`)
   (`pilish-menu.el` `pilish--refresh-transition-state-and-history`,
   `pilish--load-session-history`).
5. **No auto-respawn on exit.** `pilish--handle-process-exit` only fails
   pending requests with `:processExit t` (`pilish-core.el`). Recovery is an
   explicit revive/reload by the user or the layer.
6. **Paths are process-local.** Before sending, pilish converts the
   Emacs/TRAMP path to the remote process-local path, preserving remote `~`
   (`pilish-core.el` `pilish--process-local-path`). Remote spawns go through
   `sh -c 'printf <ready-marker>; exec "$0" "$@"'` (`pilish--start-process`,
   `pilish--remote-start-command`), and the layer rebinds
   `pilish-executable`/`pilish-extra-args` for the remote host
   (`funcs.el` `pilish//remote-spawn-start-process`).

**Derived requirement (settled).** The reattach key is the **pi session file
path**, and the attach/route operation is `switch_session`.
A fresh client starts a new session; a client that then sends
`switch_session {sessionPath}` must be routed to the actor that owns that
file — joining it if it is live, creating it otherwise — and the switch
response must be followed, in order, by that client's `get_state`/
`get_messages`/`get_commands`. Nothing about the UI needs to change: it keeps
spawning `pi --mode rpc` and keeps using `switch_session`.

Consequences to design for:

- The replacement binary must **accept pi's argv** (`--mode rpc`, `--approve`,
  `--no-approve`, and `-e <path>` extras) and must not write anything to stdout
  before protocol data.
- The client must preserve `get_state.sessionFile`, the
  `switch_session` response shape (`success` + `data.cancelled`), and the
  `new_session`/`fork`/`clone` response shapes, because pilish branches on
  them.
- A client's binding to a session actor is not fixed at connect: it can be
  re-routed by `switch_session`. **Settled:** re-routing is **server-side
  rebinding** — the server resolves the target and rebinds the connection, and
  the bridge stays a dumb relay (see the README decision log).
- `pilish-reload` is used to pick up extension/skill edits by restarting pi.
  **Settled:** under the daemon a reload is attach-only (re-attach, no
  restart); restarting pi is an explicit daemon operation that refuses while
  other clients are attached or a turn is running, unless forced.

## 2. What is and is not achievable

| Capability | Feasible? | How |
|---|---|---|
| A session survives its client's death | ✅ | the daemon owns pi; a later client re-attaches |
| N clients attached at once | ✅ | one ordered event log per session, fan-out to all |
| All clients see the same ordered stream | ✅ | `gw_seq` from a single Hub per session |
| Any client may prompt | ✅ | daemon-owned per-client FIFO queue |
| Clients interject mid-turn | ✅ | `steer` forwarded immediately to pi |
| Late join / reconnect with replay | ✅ | ring buffer + `resume.sinceSeq`; `gw_snapshot` fallback |
| Attach by path or name, list, create | ✅ | catalog + server-side `switch_session` rebinding |
| **Two LLM turns truly in parallel** | ❌ | pi has one agent loop, one context, one leaf |
| Two pi processes on one session file | ❌ (unsafe) | one writer per file, enforced by the daemon |

**Rule: the daemon never runs two pi processes against one session file.**
Parallelism is expressed as *concurrent prompt submission + daemon queueing*,
not concurrent turns. If parallel agents are needed, run separate sessions
(separate files) and coordinate at a higher layer; that is out of scope.

**Provisional:** a client attaching mid-turn is **live-only** (see §5.3). It may
see a truncated `message_update` fragment until `message_end`; this is
deliberately provisional and revisited after implementation.

---

## 3. Architecture

Two binaries:

- **`pi-gatewayd`** — the daemon. Owns sessions and every pi process, listens
  on loopback TCP, authenticates clients, and speaks the gateway protocol.
  Runs as a systemd user service.
- **`pi-gateway`** — the client bridge. A UI spawns it in place of `pi`; it
  performs `gw_hello` on the UI's behalf, consumes `gw_*` traffic, strips
  gateway fields, and exposes pristine raw pi RPC on stdio.

```text
  Emacs/pilish ──ssh/TRAMP──► pi-gateway (client) ─┐
  Slack bot (separate repo) ───────────────────────┼── loopback TCP ──► pi-gatewayd
  other integrations ──────────────────────────────┘   (token auth)          │
                                                        ┌────────────────────┴──────────────────┐
                                                        │ Per session:                          │
                                                        │   SessionActor  (sole pi writer)      │
                                                        │   Hub           (ordered log + replay)│
                                                        │   PromptQueue   (per-client FIFO)     │
                                                        │   UIBroker      (dialog routing)      │
                                                        └────────────────────┬──────────────────┘
                                                                             ▼
                                                              pi --mode rpc --session X
```

The client never spawns pi and never owns a session. Several clients — bridges
and direct integrations — can be attached to the same session at once.

### 3.1 Processes and goroutines

Per **session**:

| Goroutine | Responsibility |
|---|---|
| `SessionActor` | Single loop. Linearizes commands, drives the prompt queue, writes to pi stdin, handles rebinding and control messages. All state transitions happen here. |
| `piReader` | Reads pi stdout, strict-LF JSONL decode, assigns `seq`, hands records to the Hub. Never blocks on slow clients. |
| `piStderr` | Drains stderr into a bounded log (diagnostics + crash reason). |
| `piWriter` | Serializes writes to pi stdin. |

Per **client connection**:

| Goroutine | Responsibility |
|---|---|
| `connReader` | Decode JSONL, validate size/rate, hand commands to the bound actor. |
| `connWriter` | Drain a bounded queue to the socket with coalescing and drop policy. |

The Hub is a mutex-protected structure plus per-subscriber channels. `Publish`
is called only from `piReader` (pi events) and the actor (gateway events), so
ordering stays trivial.

### 3.2 Ownership invariants

> For a given session file, exactly one `SessionActor` and exactly one
> `PiProcess` exist. All writes to pi stdin and all reads from pi stdout pass
> through that actor/hub pair.
>
> A connection is bound to at most one session at a time, and a binding changes
> only inside the actor (server-side rebinding, §4.2).

This is what makes the design safe. If the daemon is ever horizontally scaled,
enforce it with a lease keyed by session path and sticky routing; otherwise run
a single replica.

### 3.3 Connection binding and rebinding

A connection is **unbound** until it names a session:

- `gw_hello` with `session` (path or name) binds immediately;
- otherwise it stays unbound until its first session-scoped command:
  `switch_session` binds without creating one; any other session command (for
  example `get_state`) creates a new session — pilish's fresh-session flow.

Rebinding happens **on the server** when the actor intercepts `switch_session`
or `new_session`: the connection's hub subscription and command target change,
and the client's following commands are routed to the new actor in order. The
bridge remains a dumb relay.

---

## 4. Session lifecycle

### 4.1 Creation

Two paths, both file-backed:

- **Implicit.** A fresh client sends `get_state` before any `switch_session`;
  the daemon creates a session and `get_state.sessionFile` reports the new
  file. This is pilish's new-chat flow and must keep working.
- **Explicit.** `gw_new_session{name?, cwd?, piArgs?, tags?}` creates a
  session, records the creating client's identity/tags (exposed in the
  catalog), and binds the requester to it.

A created session spawns:

```text
pi --mode rpc --session <file> [accepted piArgs...]
```

The **working directory of a session is the creating client's**, carried in
`gw_hello.cwd` (or `gw_new_session.cwd`) and used as pi's spawn directory. The
UI is the only party that knows which project it is looking at, and a
daemon-side directory would file every session under whatever directory the
service happened to start in. The value must be an absolute existing
directory; an empty `cwd` means "the daemon's own directory" for bare protocol
clients. For a session whose file already exists, the directory recorded in
the session header wins over the attaching client's, so a respawn after
hibernation or a daemon restart always happens in the session's own project
(§4.3, §4.5).

Sessions created with `--no-session` are supported but **ephemeral**: pi
reports no session file, so they have no path to attach to, they cannot be
re-adopted after they stop, and `gw_new_session` refuses them. The `ephemeral`
marker itself is not surfaced in the catalog yet.

### 4.2 Attach and rebinding

`switch_session` resolves its target (path first, then name via the catalog),
then:

1. joins the live actor if the session is warm;
2. loads the session file (spawn + `--session`, using the attaching client's
   accepted parameters) if it is hibernated or absent;
3. rebinds the requesting connection;
4. synthesizes pi's response (`success` + `data.cancelled`) instead of
   forwarding it to the current pi.

A name that matches nothing fails with `unknown_session`; an ambiguous name
fails with `ambiguous_session`.

### 4.3 Hibernation and orphan reaping

- After the **last** client detaches and the agent has settled, a session stays
  warm for a configurable grace period (default 15 minutes), then pi is stopped
  gracefully. The session file remains.
- pi is **never** stopped mid-turn: if the last client detaches during a turn,
  the timer starts at `agent_settled`.
- A session with **no messages and no clients** (typically created by pilish's
  revive path, which sends `get_state` and immediately `switch_session`) is
  stopped after a **short grace (seconds)**, not the full idle timeout.
- The next attach respawns pi and reloads the file; extensions/skills are
  freshly loaded.

### 4.4 Reload

`pilish-reload` is **attach-only** under the daemon: it re-attaches and
refreshes state/history but does not restart pi. Restarting pi is explicit:

```json
{"type": "gw_reload_session", "id": "req-4", "session": "auth-refactor", "force": false}
```

- Refused with `reload_busy` while other clients are attached or a turn is
  running, unless `force: true`.
- On success pi is stopped and respawned with the session's recorded spawn
  config; attached clients get `gw_session_state{state:"restarting"}` then
  `{state:"ready"}` and refresh their own view (the event log and ring survive
  the restart, so no replay or snapshot is forced).

### 4.5 Daemon restart

Re-adoption is **lazy**: the daemon starts with no pi processes and no
persistent registry. The next attach loads the session file. In-flight turns
are lost on daemon restart (accepted).

### 4.6 pi crash

`piReader` sees EOF or a non-zero exit; the actor:

1. publishes `gw_session_state{state:"crashed", reason, exitCode}`;
2. fails pending requests and drains the prompt queue with `session_crashed`;
3. leaves the session file intact for re-attach.

With lazy re-adoption, a crashed session is loaded again on the next attach.
Whether the daemon **also** auto-respawns while clients remain attached is an
implementation policy to settle during implementation; the protocol only
requires the `gw_session_state` signal and a resync after recovery.

### 4.7 Graceful shutdown

`SIGTERM` on the daemon: stop accepting connections, `abort` each session,
close pi stdin, wait with a timeout, then `SIGKILL`.

---

## 5. Event model and ordering

Every record the daemon sends carries gateway metadata:

```json
{ "gw_seq": 1042, "gw_session": "s_abc", "gw_ts": "2026-01-01T00:00:00.000Z", "...": "pi fields unchanged" }
```

- `gw_seq` is monotonic per session and assigned by the Hub.
- Ordering is exactly pi's stdout order; the daemon never reorders pi events.
- Gateway events (`gw_turn`, `gw_queue`, `gw_presence`, …) are sequenced on the
  same log so clients can correlate them with pi events.

### 5.1 Hub

```go
type Hub struct {
    seq  uint64
    ring []Record        // bounded replay buffer
    subs map[string]*Subscriber
    mu   sync.RWMutex
}
```

`Publish` assigns `gw_seq`, stores the record, and fans out non-blockingly to
each subscriber. A subscriber that cannot keep up is handled by §7.

### 5.2 Replay and resync

`gw_hello.resume` controls attach:

- `liveOnly: true` (the bridge default) attaches at the head; no replay.
- `resume.sinceSeq` replays `(sinceSeq, headSeq]` from the ring.
- `resume.leafEntryId` enables durable resync across daemon restarts.

If the cursor is too old or unknown, the daemon sets `resyncRequired` and sends
`gw_snapshot` built from `get_state` + `get_entries`; live records resume from
the `headSeq` captured after the snapshot reads, with the replay watermark
preventing duplicates.

### 5.3 Mid-turn attach (provisional)

A client joining a running turn is **live-only**. It receives subsequent
`message_update` deltas (which may start mid-sentence),
`tool_execution_update` (accumulated output, so tools render fully), and the
final `message_end` with the complete assistant message. No turn replay and no
partial snapshot in this revision; revisit after implementation.

---

## 6. Command path and linearization

All commands for a session flow through that session's actor:

```go
type clientCommand struct {
    client  *Client  // connection
    localID string   // id as sent by the client
    raw     []byte   // original pi command
    typ     string
    recvSeq uint64   // arrival order
}
```

The actor:

1. validates the command (type, capability, connection state);
2. applies session policy (queue, rebinding, interception);
3. rewrites `id` to a globally unique value and forwards to pi when applicable;
4. records the mapping so responses route back to the originator.

### 6.1 `id` namespacing

pi echoes the command `id` on its `response` and on `bash_execution_update`.
Clients reuse `req-1`, so the daemon namespaces inbound ids as
`<clientID>:<localID>` and restores the original on the way back. Responses for
one client are never broadcast to others.

### 6.2 Intercepted commands

| Command | Daemon behavior |
|---|---|
| `switch_session` | resolve target, rebind the requester, synthesize response (§4.2) |
| `new_session` | create a session, rebind **only the requester**, synthesize response |
| `fork`, `clone` | sole client: forward, adopt the new file, rebind; shared: `shared_session` error |
| `abort` | forward to pi; stops the shared turn; daemon queue remains |
| `clear_queue` | clear the daemon queue **and** pi's steer queue; return all cleared text |
| `gw_reload_session` | stop and respawn pi for a session (§4.4) |

Everything else in pi's command surface is forwarded verbatim with `id`
rewritten.

### 6.3 Spawn parameters

Accepted from the client: trust (`--approve`/`--no-approve`), extensions
(`-e`, `--no-extensions`), resource/tool toggles, `--provider`, `--model`,
`--thinking`, `--name`, `--session-dir`, `--no-session`, `--api-key`. Other pi
options are rejected.

- Not-live session: all accepted parameters apply at spawn.
- Live session: runtime-applicable parameters (`--model`, `--provider`,
  `--thinking`, `--name`) are applied via RPC and change shared session state
  (confirmed session-global; all clients get `gw_state_changed`).
- Spawn-only parameters are compared with the session's recorded spawn config:
  identical values are no-ops, a differing value fails the attach with
  `spawn_param_conflict`. (Required because pilish re-sends `--approve`.)

### 6.4 Global mutations

Shared-state commands (`set_model`, `cycle_model`, `set_thinking_level`,
`set_steering_mode`, `set_follow_up_mode`, `compact`, `set_auto_compaction`,
`set_auto_retry`, `set_session_name`, `gw_reload_session`) emit
`gw_state_changed{command, data, by}` to every client of the session.
Per-connection changes (`switch_session`, `new_session`, sole-client
`fork`/`clone`) rebind only the requester and do not notify others.

### 6.5 Response semantics

`success: true` on `prompt` means **accepted or queued**, not complete. Clients
wait for `agent_settled` (or `gw_turn{state:"settled"}`). Responses are
id-addressed and delivered only to the originator.

---

## 7. Backpressure and slow clients

Fan-out to a slow client must never stall pi:

- Each connection has a bounded queue (e.g. 1024 records / 4 MB).
- **Delta coalescing**: per connection, `message_update` text/thinking deltas
  are merged to at most one record per flush interval (default 50 ms) or 8 KB.
- **Terminal events are never dropped**: `message_end`, `tool_execution_end`,
  `turn_end`, `agent_end`, `agent_settled`, `response`, `extension_ui_request`.
  If a queue is saturated with terminal events, the connection is closed with
  `gw_error{code:"slow_consumer"}` so it can reconnect and resync.
- **Lossy clients** may opt into `allowLossy`; non-terminal deltas are dropped
  when full and the daemon sends `gw_lag{oldestSeq, headSeq}` to trigger a
  resync.
- `piReader` never blocks on the Hub; durability lives in the session file.

---

## 8. Queue and turn model

The daemon owns the prompt queue; pi's internal queue is not used for
cross-client ordering.

### 8.1 Admission

- If the session is idle **and** the daemon queue is empty, `prompt` is
  forwarded to pi immediately as a normal `prompt`.
- Otherwise it is enqueued in the per-client tagged FIFO.
- When `agent_settled` arrives, the actor dequeues the next prompt and forwards
  it as a normal `prompt`.

Queue entries carry `{id, mode:"followUp", author:{clientId, kind, name},
preview}`; author/kind tags are informational and never change order.

### 8.2 Interjections

- `steer` is forwarded to pi **immediately** as a real interjection; it is
  never held by the daemon and cannot be withdrawn.
- `follow_up` is enqueued exactly like a mid-turn `prompt`.

### 8.3 `abort` and `clear_queue`

- `abort` stops the shared turn; daemon-queued prompts remain and run
  afterwards (pi-native behavior). Other clients are notified through
  `gw_turn`/`gw_queue`. Like `steer`, it requires `interject` (§10).
- `clear_queue` is **session-wide**: it clears the entire daemon queue and is
  forwarded to pi to clear forwarded steers, returning all cleared text
  (`{steering, followUp}`) to the caller. That text may include prompts queued
  by other clients.

### 8.4 Turn and queue events

- `gw_turn{state:"running"|"settled", turnId, author}` tracks the current turn
  and its author (used by the UI broker, §9).
- `gw_queue{pending:[...]}` republishes the daemon queue whenever it changes.

---

## 9. Extension UI broker

pi emits `extension_ui_request` and blocks until a matching
`extension_ui_response` (or its own timeout resolves a default).

- **Dialogs** (`select`, `confirm`, `input`, `editor`) are routed to the
  **author of the running turn** (`gw_turn.author`) when that client has `ui`.
  If it lacks `ui` or has disconnected, the daemon falls back to the most
  recently active `ui`-capable client; with no client able to answer, pi's
  timeout default applies. With no turn running, the same fallback order is
  used.
- **Fire-and-forget** methods (`notify`, `setStatus`, `setWidget`, `setTitle`,
  `set_editor_text`) are broadcast to all clients.
- The broker keeps `pendingUI map[piRequestID]{winner, request}`; late or
  non-owner responses are dropped with `ui_stale`. If the winner disconnects
  before answering, the broker reassigns to the next candidate; with nobody
  left, pi's own dialog timeout resolves the default. The broker does not keep
  its own deadline.
- `extension_ui_request.id` is preserved; only the response routing is managed.

---

## 10. Security model

- **Local-only, token-authenticated.** The daemon listens on loopback TCP and
  requires a token from `~/.config/pi-gateway/token` (or `tokens.json`) in
  `gw_hello`. There is no TLS and no network listener. The debug listener is a
  separate loopback port with **no authentication**; its routes are read-only
  by construction and it exposes no session content beyond the catalog
  metadata (path, name, title, message count) that `gw_list_sessions` also
  returns.
- **Capabilities are authority.** A client requests capabilities at
  `gw_hello`; the daemon grants the intersection with the token's role. Every
  command is checked once, before dispatch, against that role and a refused
  command is never executed (docs/protocol.md §10 lists the exact mapping);
  command types are canonical, so a case variant cannot dodge the check. The
  default daemon-generated token grants the full set. A token identifies
  itself as `admin`, `operator` (observe + interject + prompt + ui), or
  `observer` (observe), or names an explicit capability list; tokens live
  either in the generated `token` file or in `tokens.json`, which the daemon
  re-reads on SIGHUP. `pi-gatewayd --provision-token` appends one and prints
  only the token so scripts can capture it.

| Capability | Allows |
|---|---|
| `observe` | receiving the event stream, `get_*` queries, `export_html`, `gw_list_sessions` |
| `interject` | `steer`, `abort`, `abort_bash`, `abort_retry`, `clear_queue` |
| `prompt` | `prompt`, `follow_up`, `new_session`, `fork`/`clone`, `bash`, `switch_session` |
| `ui` | answer extension UI dialogs, `notify` |
| `control` | `gw_reload_session`, `set_model`, `cycle_model`, `set_thinking_level`, `cycle_thinking_level`, `set_steering_mode`, `set_follow_up_mode`, `compact`, `set_auto_compaction`, `set_auto_retry`, `set_session_name`, `set_editor_text` |
| `admin` | `gw_new_session` (provision sessions/config) |
| *(none)* | `gw_ping`, `gw_bye` |

Non-lowercase command types are rejected (`bad_frame`) instead of forwarded, so
a case variant cannot dodge the table.

`observe` covers `gw_list_sessions`, pi's `get_*` queries, `export_html`, and
receiving the event stream: a client without it receives only records
addressed to it (its own responses and dialogs), no replay, and no snapshot.
`bash` is gated by `prompt` (a prompt-capable client can already run shell
work through the agent), the shared-state mutations by `control`, and the
cancellation primitives `abort`/`clear_queue` by `interject`, because clearing
can withdraw work another client queued.

- **Kind is informational only** (`pilish`, `integration`, `bot`, `observer`);
  it never changes policy. Integration-specific capability handling and
  formatting live in the integration, not the gateway.
- Per-integration tokens are local files under the operator's control; there
  is no token database, expiry, or revocation beyond editing `tokens.json` and
  sending SIGHUP.
- Limits: max frame size, commands/s, connections per session, sessions per
  daemon. Never accept arbitrary `sessionFile`/`cwd`/argv from an untrusted
  client. Tool output is untrusted data; the daemon does not sanitize it.

---

## 11. Protocol compatibility rules

The daemon is a **relay first, interpreter second**:

1. Accept pi commands verbatim, rewriting only `id` (intercepted commands in
   §6.2 are the exceptions).
2. Forward pi events verbatim, adding only `gw_seq`/`gw_session`/`gw_ts`.
3. Validate `extension_ui_response` against the broker and forward with its
   `id` unchanged.
4. Keep gateway-only messages in the `gw_` namespace; a client may ignore all
   of them and still stream.
5. Strict **LF-only JSONL** framing; strip a trailing `\r`; never split on
   U+2028/U+2029.

Raw pi compatibility is provided by the **client bridge**, not the daemon: it
performs `gw_hello`, consumes `gw_*` frames, strips gateway fields, and
restores ids, so the UI sees pristine pi traffic. The daemon therefore needs no
raw mode.

See `docs/protocol.md` draft 2 for the exact message set.

---

## 12. Go package layout

```text
cmd/pi-gatewayd/       daemon main: flags, signals, systemd unit entry point
cmd/pi-gateway/        client main: the bridge UIs spawn in place of `pi`
internal/protocol/     JSONL codec, gateway messages, compat helpers
internal/config/       token/port file paths, defaults
internal/daemon/       listener, auth, connection table, binding/rebinding
internal/session/      SessionActor, Hub, PromptQueue, UIBroker, PiProcess
internal/catalog/      session catalog: file scan + live index, name resolution
internal/client/       bridge: gateway <-> raw pi, id restore, gw_* filtering
internal/debughttp/    read-only /status, /catalog, /metrics HTTP endpoints
internal/gwlog/        structured logging (text or JSON via log/slog)
internal/metrics/      Prometheus-text counters, gauges, metric names
internal/piargs/       accepted pi parameter parsing (shared daemon <-> client)
internal/fakepi/       fake pi used by the end-to-end tests
internal/gwtest/       shared daemon harness for end-to-end tests
internal/testutil/     test helpers: build fake pi, raw protocol client
```

M1, M2, and M3 implement this layout, plus `packaging/pi-gatewayd.service` for
the systemd user unit. The prototype that predated the settled
design (a single binary with `serve`/`connect` subcommands and floor
arbitration) has been removed.

---

## 13. Testing strategy

- **Protocol conformance**: run pi RPC commands through the daemon and diff
  forwarded events against a direct pi run.
- **Ordering**: property test that every subscriber observes the same `gw_seq`
  order under randomized concurrency. Coalescing deliberately skips
  intermediate sequences (a merged delta carries the newest `gw_seq`), so the
  property is monotonicity plus replayed-window completeness, not contiguity;
  a raw event diff against pi is therefore only valid with coalescing off.
- **Replay / resync**: reconnect at random `seq`; assert reconstructed state
  matches a fresh `get_messages`; test `gw_snapshot` after eviction.
- **Queue**: N clients race to prompt; assert FIFO with author/kind tags, no
  lost or duplicated prompts, and no `turn_held`.
- **Roles**: a provisioned token gets exactly its role in `gw_welcome.granted`;
  every command in the §10 table answers `forbidden` without its capability
  (and without creating a session or starting pi); a non-`observe` token
  receives no transcript, replay, or snapshot; a case-variant command type is
  `bad_frame`; SIGHUP-style `SetTokens` swaps the table for new handshakes
  only; an empty default token authenticates nobody.
- **Listener safety**: `RequireLoopback` refuses `0.0.0.0`, `:port`, and
  routable addresses for both listeners, so the unauthenticated debug
  endpoints cannot leave the host.
- **Operations**: `/status`, `/catalog`, and `/metrics` serve live daemon state
  without a token; counters advance for real turns; the listener rejects
  non-GET methods.
- **Rebinding**: `switch_session` joins a live session without restarting pi;
  assert subsequent `get_state`/`get_messages` land on the new actor and that
  no orphan session outlives the short grace.
- **Hibernation / wake**: last client detaches; assert the idle timer, that pi
  is never stopped mid-turn, and that re-attach reloads the file.
- **Bridge fidelity**: assert the UI side never sees `gw_*` or injected fields
  and that ids/response shapes are preserved; assert `--version` works.
- **Auth/limits**: missing/wrong token rejected; oversized frames rejected.
- **Crash / slow consumer**: `kill -9` pi mid-turn and a client that stops
  reading; assert `gw_session_state`, disconnect/resync, and that pi latency is
  unaffected for healthy clients.
- **Race detector**: `go test -race` on all packages.

---

## 14. Roadmap

1. **M1 — core daemon and client (implemented).** Two binaries; token/port
   discovery; loopback TCP listener with auth; session actor (Hub, PromptQueue,
   PiProcess); `gw_hello`/`gw_welcome`; pi command/event passthrough with id
   namespacing; server-side `switch_session` and `new_session` rebinding;
   daemon-owned queue; spawn-param conflict checks and runtime-parameter
   application; replay and `gw_snapshot` resync; bridge on stdio; hibernation
   and orphan reaping.
2. **M2 — sessions and multi-client (implemented).** Catalog and name
   resolution; `gw_new_session`; `gw_reload_session`; `fork`/`clone` policy;
   extension UI broker; presence and `gw_state_changed`; kind tagging;
   backpressure and coalescing.
3. **M3 — operations (implemented).** systemd user unit; separate loopback
   HTTP debug listener (status/metrics/catalog, unauthenticated and read-only);
   token roles and capability provisioning (`tokens.json`, SIGHUP reload,
   `--provision-token`, per-command `get_*` checks); structured logging
   (`--log-format`/`--log-level`) and Prometheus-text metrics.
4. **M4 (optional).** Session groups across daemons, WebSocket transport for
   non-local clients, and other transport adapters behind the same protocol.
