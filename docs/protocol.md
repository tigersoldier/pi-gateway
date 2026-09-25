# `pi-gateway` client protocol v1 (draft 2)

This document is the client-facing wire protocol for the settled design in
`docs/design.md` and the decision log in `README.md`. It supersedes draft 1.

- Framing: **strict JSONL, LF (`\n`) only**. Strip a trailing `\r`. Do not
  split on U+2028/U+2029. One JSON object per line, in both directions.
- Encoding: UTF-8 JSON.
- A connection is stateful: it starts unbound, then binds to exactly one
  session and can be **rebound** by `switch_session`.
- Every decision for this draft is settled; the one deliberately provisional
  behavior is noted in §6 (mid-turn attach) and will be revisited after
  implementation.

---

## 0. Roles and topology

```text
 third-party UI ── raw pi JSONL (stdio) ──► pi-gateway connect ── gw protocol ──► pi-gateway serve ── pi RPC ──► pi
                                                  (a client)                       (owns pi, sole writer)
 integrations (e.g. Slack bot, separate repo) ── gw protocol ──────────────────────────┘
```

| Role | Command | Owns pi? | Speaks |
|---|---|---|---|
| **daemon** | `pi-gateway serve` | yes | gateway protocol (`gw_hello` + `gw_*`) |
| **bridge** | `pi-gateway connect --stdio` | no | gateway protocol upstream, **raw pi RPC** downstream |

The bridge is an ordinary client of the daemon. It never spawns pi and holds no
session state. Several bridges, and several direct gateway clients, can be
attached to the same session at once.

### Bridge guarantees to a pi UI

- No handshake frame is required on the UI side; the bridge performs
  `gw_hello`/`gw_welcome` on the UI's behalf.
- Every `gw_*` message is consumed by the bridge, and gateway fields
  (`gw_seq`, `gw_session`, `gw_ts`, `gw_owner`, …) are stripped, so the UI sees
  pristine pi messages.
- `id` values are restored exactly on responses, so `id` correlation,
  `bash_execution_update.id`, and `extension_ui_request`/`extension_ui_response`
  behave as with pi.
- The bridge declares kind `pilish` and requests the full capability set.

### Packaging: two binaries

- **`pi-gatewayd` (server):** the daemon. Run as a systemd user service; owns
  pi sessions and speaks the gateway protocol on loopback TCP.
- **`pi-gateway` (client):** always the bridge. UIs spawn it in place of
  `pi`; it accepts pi argv (including the `--mode rpc` that pilish appends,
  which the client consumes) and forwards the accepted pi parameters. It has
  exactly one job, so it has no subcommands and needs no auto-dispatch.

This is what makes the layer's remote mapping work: `pilish/remote-executables`
stores a binary path and execs it with `--mode rpc` appended, so the client
binary at that path is invoked directly. Local pilish points
`pilish-executable` at the client binary path too.

---

## 1. Transport and authentication

- **Loopback TCP only**: `127.0.0.1:<port>`.
- Port discovery is **hybrid**: the daemon writes the bound port to
  `~/.config/pi-gateway/port`; clients prefer that file and fall back to
  `127.0.0.1:7331`. `--server`/`--port` override.
- Authentication: the daemon generates `~/.config/pi-gateway/token` (mode
  `0600`) on first start. Every `gw_hello` must carry it; missing or wrong
  tokens get `gw_error{code:"unauthorized"}` and the connection closes.
  The client reads the file by default; `--token-file` overrides.
- The future HTTP debug listener is a **separate loopback port**, added later,
  sharing the same token. It is not part of this document.
- There is no TLS and no network listener (all clients are local).

---

## 2. Connection lifecycle

```text
client                                        daemon
  |  TCP connect                               |
  |------------------------------------------->|
  |  gw_hello {protocol, token, client, ...}    |
  |------------------------------------------->|
  |  gw_welcome {clientId, granted, seq, ...}   |
  |<-------------------------------------------|
  |  [gw_replay ...], gw_replay_done            |
  |<-------------------------------------------|
  |  pi commands / gw_* controls                |
  |<===========================================>|
  |  pi events + gw_* events (id-targeted       |
  |  responses are sent only to the originator) |
  |<===========================================>|
```

Session binding is **lazy**. A connection with no `session` in `gw_hello` is
unbound until its first session-scoped command:

- `switch_session` (or a non-pi `gw_attach`) binds without creating a session;
- any other session command (e.g. `get_state`) **creates a new session**, which
  is pilish's fresh-session flow.

This ordering is what lets a reconnect that begins with `switch_session` avoid
creating an orphan session first. A connection that creates a session and then
switches away leaves the created session behind; a session with **no messages
and no clients** is stopped after a short grace (seconds) instead of the full
idle timeout, and pi typically writes no file until the first assistant
message.

### `gw_hello` (client → daemon)

```json
{
  "type": "gw_hello",
  "protocol": 1,
  "token": "<contents of ~/.config/pi-gateway/token>",
  "client": {
    "name": "pilish-on-laptop",
    "kind": "pilish",
    "capabilities": ["observe", "interject", "prompt", "ui", "control"],
    "tags": {"host": "laptop"}
  },
  "session": "/home/u/.pi/agent/sessions/--home-u-proj--/2026-...jsonl",
  "piArgs": ["--approve"],
  "resume": {"sinceSeq": 1042, "leafEntryId": "a1b2c3d4"},
  "liveOnly": true,
  "allowLossy": false
}
```

- `session` is optional and accepts a **session file path or a session name**.
  Omit it for the fresh-session/rebind flow (pilish does this).
- `piArgs` carries the pi parameters the UI was invoked with (already filtered
  by the client to the accepted set; see §4.3).
- `resume.sinceSeq` requests replay after that sequence; `resume.leafEntryId`
  enables durable resync after a daemon restart.
- `liveOnly: true` attaches at the current head without replay (the bridge
  default). `resume` is ignored when `liveOnly` is set.
- `kind` is informational and tagged on events, queue entries, presence, and
  catalog entries. It never changes policy. Authority is capability-based.

### `gw_welcome` (daemon → client)

```json
{
  "type": "gw_welcome",
  "protocol": 1,
  "clientId": "c_2",
  "kind": "pilish",
  "granted": ["observe", "interject", "prompt", "ui", "control"],
  "piVersion": "0.81.0",
  "concurrency": "queue",
  "session": null,
  "headSeq": 1100,
  "oldestSeq": 100,
  "resyncRequired": false,
  "piState": { "...": "get_state result when bound, else null" },
  "turn": {"state": "idle", "queued": 0},
  "clients": [
    {"clientId": "c_1", "name": "laptop", "kind": "pilish",
     "capabilities": ["observe", "prompt", "ui"]}
  ]
}
```

- `granted` is the intersection of the requested capabilities and the token's
  role. The default daemon-generated token grants the full set.
- `session` is `{path, name, id}` when the connection is bound at hello, else
  `null`.
- `resyncRequired: true` means the requested cursor is not replayable; the
  daemon follows with `gw_snapshot` and the client discards local transcript
  state.
- `piVersion` is how `--version` is answered: a client may connect, read it,
  print it, and exit without creating a session.

### `gw_replay` / `gw_replay_done`

One frame per replayed record, with the original pi fields plus `gw_seq`:

```json
{"gw_seq": 1043, "gw_session": "s_abc", "type": "message_update", "...": "..."}
```

`gw_replay_done{headSeq}` ends the replay window. Live records continue with
the next `gw_seq`; clients must not double-apply a record whose `gw_seq` they
already processed.

### `gw_snapshot`

```json
{
  "type": "gw_snapshot",
  "piState": { "...": "get_state result" },
  "entries": [ {"type": "message", "id": "a1b2c3d4", "...": "..."} ],
  "leafId": "e5f6g7h8"
}
```

`entries` come from pi's `get_entries` (the full durable tree, pre-compaction
history included), not `get_messages`.

---

## 3. Session model

### 3.1 Identity

- **Canonical identity is the session file path.** pilish attaches by path via
  `switch_session`; the daemon keys sessions by canonicalized path.
- Attach also accepts a session **name**. Names are resolved on demand by
  scanning pi session files and live sessions; there is **no persisted
  id→file mapping**. A name that is missing or ambiguous falls back to path
  semantics and fails with `unknown_session` / `ambiguous_session`.

### 3.2 Catalog

```json
{"type": "gw_list_sessions", "id": "req-1",
 "filter": {"cwd": "/home/u/proj", "live": true, "limit": 50}}
```

Response data:

```json
{"sessions": [
  {"path": "/home/u/.pi/.../x.jsonl",
   "name": "auth-refactor",
   "title": "Refactor auth",
   "id": "uuid",
   "cwd": "/home/u/proj",
   "live": true,
   "isStreaming": true,
   "messageCount": 42,
   "lastActivity": "2026-01-01T00:02:00Z",
   "createdBy": {"clientId": "c_3", "kind": "integration", "tags": {"channel": "#auth"}},
   "clients": [{"clientId": "c_1", "kind": "pilish"}]}
]}
```

`live` means a pi process is attached; non-live sessions exist only as files.
`createdBy` carries the creating client's identity for explicitly created
sessions, so an integration can list/delete its own.

### 3.3 Creating a session

- **Implicit (pilish).** A fresh client sends `get_state` before any
  `switch_session`; the daemon creates a session and `get_state.sessionFile`
  reports its path.
- **Explicit (integrations).**

  ```json
  {"type": "gw_new_session", "id": "req-2",
   "name": "slack-auth-thread",
   "piArgs": ["--provider", "openai"],
   "tags": {"channel": "#auth"}}
  ```

  Response data: `{"path": "...", "name": "...", "sessionId": "uuid"}`. The
  requesting connection is **rebound** to the new session.

### 3.4 Attaching and rebinding

Attach is pi's `switch_session`, intercepted by the daemon:

```json
{"type": "switch_session", "id": "req-3", "sessionPath": "auth-refactor"}
```

- The daemon resolves the target (path first, then name), joins it if live,
  loads it if hibernated, and **rebinds the connection** (hub subscription and
  command target).
- If the session has no live pi process, the daemon spawns one with the
  session's recorded spawn config and the file.
- The response is pi-shaped and synthesized by the daemon, not forwarded to the
  current pi:

  ```json
  {"type": "response", "id": "req-3", "command": "switch_session",
   "success": true, "data": {"cancelled": false}}
  ```

- The client's immediately following `get_state` / `get_messages` /
  `get_commands` are routed to the newly bound session, in order.
- Rebinding does not affect other clients attached to the old or new session.

### 3.5 Hibernation

- After the **last** client detaches and the agent has settled, a session stays
  warm for a configurable grace period (default 15 minutes), then pi is
  stopped. The session file remains.
- pi is **never** stopped mid-turn. If the last client detaches during a turn,
  the timer starts when `agent_settled` arrives.
- A session that was created but never received a message and has no clients is
  stopped after a short grace (seconds), not the idle timeout.
- The next attach respawns pi and loads the file; extensions/skills are freshly
  loaded.

### 3.6 Reload

`pilish-reload` is **attach-only** under the daemon: it re-attaches and
refreshes state/history but does not restart pi. Restarting pi is an explicit
daemon operation:

```json
{"type": "gw_reload_session", "id": "req-4", "session": "auth-refactor", "force": false}
```

- Refused with `reload_busy` while other clients are attached or a turn is
  running, unless `force: true`.
- On success pi is stopped and respawned with the session's recorded spawn
  config; the file reloads. Attached clients receive
  `gw_session_state{state:"restarting"}` then `{state:"ready"}`.

### 3.7 New session (`new_session`)

pi's `new_session` is intercepted. The daemon creates a new session (with the
requester's recorded spawn config) and **rebinds only the requesting
connection** to it. Other clients stay on the previous session and receive no
state change.

```json
{"type": "response", "id": "req-5", "command": "new_session",
 "success": true, "data": {"cancelled": false}}
```

The connection's following `get_state` reports the new session file, which is
exactly what pilish's `/new` expects.

### 3.8 Fork and clone (`fork`, `clone`)

pi's `fork {entryId}` and `clone` create a **new session file** and switch the
live pi process to it (`runtimeHost.fork` then `rebindSession`), so they cannot
be forwarded to a shared session.

- **Sole client:** forwarded to the session's pi. The daemon adopts the newly
  created file as a new session and **rebinds the requester** to it. Responses
  stay pi-shaped (`fork`: `data.text`; `clone`: `data.cancelled`).
- **Shared session (other clients attached):** refused with `shared_session`;
  no other client is ever moved. `pilish-fork` is therefore unavailable while
  another client is attached.

`clone` forks at the pi process's current in-memory leaf, which is not always
persisted to the file; that is why isolation on a scratch process is not used.

### 3.9 Prompt queue (daemon-owned)

The daemon owns the multi-client prompt queue; pi's internal queue is not used
for cross-client ordering.

- A `prompt` received while the session is idle **and** the daemon queue is
  empty is forwarded to pi immediately as a normal `prompt`.
- A `prompt` received while a turn is running (or while the daemon queue is
  non-empty) is **enqueued** in the daemon's per-client tagged FIFO and is
  forwarded to pi as a normal `prompt` when the session settles and the queue
  reaches it.
- `steer` is a **real-time interjection**: forwarded to pi immediately, never
  held by the daemon. `follow_up` is enqueued exactly like a mid-turn prompt.
- `gw_queue` reports the daemon queue with author tags:
  `{pending:[{id, mode:"followUp", author:{clientId,kind,name}, preview}]}`.
- Author/kind tags are informational and never change delivery order.

Because the daemon holds the queue, `clear_queue` and `abort` are defined in
terms of the daemon queue and pi's turn:

- `abort` may be called by any client. It stops the shared current turn;
  daemon-queued prompts remain and run afterwards (pi-native behavior).
- `clear_queue` is **session-wide**: it clears the entire daemon queue and is
  forwarded to pi to clear forwarded steers, returning all cleared text
  (`{steering:[...], followUp:[...]}`) to the caller. That text may include
  items queued by other clients.
- A `steer` already forwarded to pi cannot be withdrawn except by the
  session-wide `clear_queue`.

---

## 4. Commands

### 4.1 Gateway control (client → daemon)

| Type | Payload | Purpose |
|---|---|---|
| `gw_hello` | §2 | handshake, auth, optional bind |
| `gw_list_sessions` | `{filter}` | session catalog |
| `gw_new_session` | `{name?, piArgs?, tags?}` | explicit create + bind |
| `gw_reload_session` | `{session?, force?}` | restart pi for a session |
| `gw_ping` | `{}` | liveness |
| `gw_bye` | `{}` | graceful disconnect |

The previous draft's `gw_take_turn`, `gw_release_turn`, `gw_set_mode`,
`gw_resume`, and floor/lease messages are **removed**: concurrency is queue
semantics with no floor, and resume lives in `gw_hello`.

### 4.2 pi commands (passthrough)

Forwarded verbatim with `id` namespaced per client and restored on responses:
`prompt`, `steer`, `follow_up`, `abort`, `clear_queue`, `get_state`,
`get_messages`, `set_model`, `cycle_model`, `get_available_models`,
`set_thinking_level`, `cycle_thinking_level`, `get_available_thinking_levels`,
`set_steering_mode`, `set_follow_up_mode`, `compact`, `set_auto_compaction`,
`set_auto_retry`, `abort_retry`, `bash`, `abort_bash`, `get_session_stats`,
`export_html`, `get_entries`, `get_tree`, `get_fork_messages`,
`get_last_assistant_text`, `set_session_name`, `get_commands`.

Intercepted (not forwarded to the current pi):

| Command | Daemon behavior |
|---|---|
| `switch_session` | resolve target, rebind connection, synthesize response (§3.4) |
| `new_session` | create a new session, rebind **only this connection**, synthesize response (§3.7) |
| `fork`, `clone` | sole client: forward, adopt the new file, rebind requester; shared: `shared_session` (§3.8) |

`abort` and `clear_queue` are **session-wide** and may be called by any client
(§3.9).

### 4.3 Spawn parameters (`piArgs`)

Accepted set: trust (`--approve`/`--no-approve`), extensions (`-e`,
`--no-extensions`), resource/tool toggles (`--skill`, `--prompt-template`,
`--theme`, `--no-context-files`, `--tools`, `--exclude-tools`,
`--no-builtin-tools`, `--no-tools`, `--system-prompt`,
`--append-system-prompt`), and `--provider`, `--model`, `--thinking`,
`--name`, `--session-dir`, `--no-session`, `--api-key`. Other pi options are
rejected with `bad_frame`.

- If the session is **not live**, all accepted parameters apply at spawn.
- If it **is live**:
  - runtime-applicable parameters (`--model`, `--provider`, `--thinking`,
    `--name`) are applied via RPC and change **shared** session state; this is
    session-global, and every client is notified with `gw_state_changed`;
  - spawn-only parameters are compared to the session's recorded spawn config:
    **identical values are no-ops; a differing value fails the attach** with
    `spawn_param_conflict` (naming the parameter). This is required because
    pilish re-sends `--approve` on every reload.

### 4.4 Global session mutations

Commands that mutate shared session state (`set_model`, `cycle_model`,
`set_thinking_level`, `set_steering_mode`, `set_follow_up_mode`, `compact`,
`set_auto_compaction`, `set_auto_retry`, `set_session_name`,
`gw_reload_session`) cause the daemon to emit `gw_state_changed{command, data,
by}` to every client of the affected session so UIs can reset local state.
Per-connection session changes (`switch_session`, `new_session`, and
`fork`/`clone` for a sole client) rebind only the requesting connection and do
not notify others.

---

## 5. Events

### 5.1 pi events (passthrough)

Forwarded verbatim with `gw_seq`, `gw_session`, `gw_ts` added:
`agent_start`, `agent_end`, `agent_settled`, `turn_start`, `turn_end`,
`message_start`, `message_update`, `message_end`, `bash_execution_update`,
`tool_execution_start`, `tool_execution_update`, `tool_execution_end`,
`queue_update`, `compaction_start`, `compaction_end`, `auto_retry_start`,
`auto_retry_end`, `summarization_retry_scheduled`,
`summarization_retry_attempt_start`, `summarization_retry_finished`,
`extension_error`, `extension_ui_request`.

`bash_execution_update` goes to its originator with the original `id`; other
clients receive a copy tagged with `gw_owner`.

### 5.2 Gateway events (daemon → client)

| Type | Payload | Purpose |
|---|---|---|
| `gw_welcome` | §2 | handshake result |
| `gw_replay` / `gw_replay_done` | record / `{headSeq}` | replay window |
| `gw_snapshot` | §2 | full resync |
| `gw_turn` | `{state:"running"\|"settled", turnId, author:{clientId,kind,name}}` | turn lifecycle; replaces the removed floor |
| `gw_queue` | `{pending:[{id, mode:"followUp", author:{clientId,kind,name}, preview}]}` | daemon-owned pending prompts, tagged by client kind; immediate `steer` interjections are not listed |
| `gw_presence` | `{event:"join"\|"leave"\|"update", client}` | roster |
| `gw_state_changed` | `{command, data, by}` | shared-state mutation |
| `gw_session_state` | `{state:"ready"\|"starting"\|"hibernated"\|"restarting"\|"crashed"\|"stopped", reason?, exitCode?}` | session process lifecycle |
| `gw_lag` | `{oldestSeq, headSeq}` | client fell behind; resync |
| `gw_error` | `{code, message}` | protocol-level error |
| `gw_pong` | `{}` | liveness |

`gw_partial` (in-flight assistant prefix) is **not emitted** in this revision:
mid-turn attach is live-only for now and will be revisited after
implementation.

---

## 6. Attach and replay semantics

- `liveOnly: true` (bridge default) attaches at the head; no history replay.
  The UI reconstructs state with `get_state`/`get_messages`/`get_entries`.
- `resume.sinceSeq` replays from a cursor. If the cursor was evicted, the
  daemon sets `resyncRequired` and sends `gw_snapshot`.
- `resume.leafEntryId` enables resync across daemon restarts.
- **Mid-turn attach (provisional).** A client joining a running turn receives
  live events from that point. It may see a truncated `message_update` fragment
  until `message_end` delivers the complete assistant message;
  `tool_execution_update` carries accumulated output, so tools render fully.

---

## 7. Extension UI

`extension_ui_request` is forwarded to clients; `extension_ui_response` is
validated and forwarded to pi.

- Fire-and-forget methods (`notify`, `setStatus`, `setWidget`, `setTitle`,
  `set_editor_text`) are broadcast.
- Dialog methods (`select`, `confirm`, `input`, `editor`) are routed to the
  **author of the running turn** (`gw_turn.author`). If that client lacks `ui`
  or has disconnected, the daemon falls back to the most recently active
  `ui`-capable client; if none answers, pi's own dialog timeout resolves the
  default. With no turn running, the same fallback order applies.
  Non-owner responses are dropped with `ui_stale`.

---

## 8. Errors

| Code | Meaning |
|---|---|
| `unauthorized` | missing or wrong token |
| `forbidden` | capability missing for the command |
| `bad_frame` | malformed JSONL, oversized frame, or rejected pi option |
| `unknown_session` | path/name did not resolve |
| `ambiguous_session` | name resolved to more than one session |
| `spawn_param_conflict` | spawn-only parameter differs from the live session config |
| `shared_session` | `fork`/`clone` requested while other clients are attached |
| `reload_busy` | reload refused: other clients attached or a turn running |
| `queue_full` | per-session or per-client command queue full |
| `slow_consumer` | client disconnected for not reading |
| `ui_stale` | extension UI response arrived after resolution |
| `session_crashed` | pi exited; command not accepted |
| `resync_required` | cursor too old or unknown |
| `not_supported` | mode/command outside the supported surface (e.g. TUI) |

Errors stay pi-shaped where a `response` is expected:

```json
{"type": "response", "id": "req-7", "command": "switch_session",
 "success": false, "error": "unknown session: auth-x", "code": "unknown_session"}
```

---

## 9. Reference flows

### 9.1 pilish: fresh session

```text
bridge: gw_hello{no session, kind:"pilish", piArgs:["--approve"]}
bridge <- gw_welcome{session:null, turn:{state:"idle"}}
ui ->    get_state            # first session command: creates a new session
bridge <- {response get_state, data.sessionFile:"/home/u/.pi/.../new.jsonl"}
bridge <- message_update...   # UI works normally
```

### 9.2 pilish: reconnect after an ssh drop

```text
bridge: gw_hello{no session}                  # fresh client process
bridge <- gw_welcome{session:null}
ui ->    switch_session{sessionPath:"/~.../auth.jsonl"}
              # daemon resolves, joins the live session or reloads the file,
              # rebinds this connection; pi is NOT restarted
bridge <- {response switch_session, success:true, data:{cancelled:false}}
ui ->    get_state ; get_messages ; get_commands
bridge <- those responses from the rebound session
bridge <- live events (turn keeps running; live-only partial per §6)
```

### 9.3 Integration: list, attach, prompt

```text
bot: gw_hello{kind:"integration", capabilities:["observe","prompt"]}
bot <- gw_welcome{...}
bot -> gw_list_sessions{filter:{cwd:"/home/u/proj"}}
bot <- {sessions:[...]}
bot -> switch_session{sessionPath:"auth-refactor"}
bot <- {response switch_session, success:true}
bot -> {id:"p1", type:"prompt", message:"Summarize the failing test"}
bot <- {response prompt, success:true, queued:true}   # queue semantics
```

### 9.4 Two clients, queue semantics

```text
A -> prompt "Refactor auth"      -> success:true (starts a turn)
all <- gw_turn{state:"running", author:{clientId:"c_A", kind:"pilish"}}
B -> prompt "Also run the tests" -> success:true (queued as follow-up)
all <- gw_queue{pending:[{author:{clientId:"c_B", kind:"integration"}, mode:"followUp"}]}
all <- agent_settled
B's turn runs next
```

### 9.5 Reload

```text
A -> gw_reload_session{session:"auth-refactor", force:false}
A <- {response gw_reload_session, success:false, code:"reload_busy"}
A -> gw_reload_session{session:"auth-refactor", force:true}
all <- gw_session_state{state:"restarting"}
all <- gw_session_state{state:"ready"}
```

---

## 10. Open semantics

None. Every gap found while reviewing this draft has been resolved with the
operator and recorded in `README.md` → Design decisions. The only deliberately
provisional behavior is **mid-turn attach** (§6), which stays live-only and
will be revisited after implementation.

---

## 11. Compatibility notes for client authors

1. **Always send `id`** on commands; the daemon namespaces it per client, so a
   client can freely reuse `req-1`.
2. **`success:true` on `prompt` means accepted or queued**, not complete. Wait
   for `agent_settled` (or `gw_turn{state:"settled"}`).
3. **Persist `gw_seq` and `leafEntryId`** to resume across reconnects.
4. **Ignore unknown `gw_*` messages**; the protocol is forward-compatible.
5. **`kind` is informational only**; authority comes from capabilities. Do not
   branch on `kind` for security.
6. **Never use readline-style splitters**; split on LF only.
7. **Treat `extension_ui_request` as possibly not yours** unless you are the
   turn author or the daemon addressed it to you (§7).
