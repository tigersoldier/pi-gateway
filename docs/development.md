# Development: `pi-gateway`

How `pi-gateway` is built, tested and extended, what state it is in, and why
it is designed the way it is. User-facing setup, the CLI reference and the
integration examples live in the [`README.md`](../README.md).

**Read before changing behaviour:** [`docs/design.md`](design.md) (architecture,
invariants, failure modes) and [`docs/protocol.md`](protocol.md) (the
client-facing wire contract). This document is the canonical record of
decisions.

- [Implementation status](#implementation-status)
- [Decision log](#decision-log)
- [Repository layout](#repository-layout)
- [Building and testing](#building-and-testing)
- [Development conventions](#development-conventions)

## Implementation status

**M1 (session core), M2 (multi-client sharing) and M3 (operations) are implemented.**

| Area | Status |
| --- | --- |
| Binaries | `pi-gatewayd` (daemon), `pi-gateway` (bridge) |
| Transport/auth | loopback TCP, token file (0600), `--server`/`--port`/`--token-file`, port-file discovery with fallback to `127.0.0.1:7331` |
| Sessions | attach by path **or name**, implicit creation on the first command, server-side `switch_session` rebinding, `new_session` rebinding only the requester, explicit `gw_new_session` with creator tags |
| Catalog | `gw_list_sessions` with cwd/live/limit filters over pi's session files plus live actors; name resolution with `unknown_session`/`ambiguous_session`; in-memory `createdBy`/tags |
| Process | one pi per session, crash/exit reporting, idle hibernation, short-grace reaping, `gw_reload_session` restarts (`reload_busy`/`force`), `fork`/`clone` adoption for a sole client (`shared_session` otherwise) |
| Commands | passthrough with `id` namespacing, spawn-param conflict detection, runtime-parameter application via RPC plus `gw_state_changed`, one capability check per command before dispatch (canonical lowercase types only) |
| Queue | daemon-owned per-client tagged FIFO, immediate `steer`, `abort`, session-wide `clear_queue` returning cleared text |
| Events | per-session ordered log with `gw_seq`, `gw_turn`, `gw_queue`, `gw_presence`, `gw_session_state`, `gw_error`; `liveOnly` and `resume.sinceSeq` replay; `gw_snapshot` resync; `resume.leafEntryId` durable resume across daemon restarts |
| Extension UI | dialogs routed to the turn author, then the most recently active `ui` client, reassigned when that client disconnects; non-owner answers are `ui_stale`; fire-and-forget methods broadcast |
| Backpressure | per-connection buffers, streaming-delta coalescing (default 50 ms/8 KB), terminal events never dropped, `allowLossy` clients get `gw_lag` instead of being dropped, `slow_consumer` close |
| Operations | systemd user unit (`packaging/pi-gatewayd.service`), separate loopback debug listener at `127.0.0.1:7332` with `/status` `/catalog` `/metrics` (read-only, no auth), token roles/presets + `tokens.json` + `--provision-token`, SIGHUP token reload, structured `log/slog` logging (`--log-format`/`--log-level`), Prometheus-text metrics |
| Client library | `protocol`/`config`/`piargs` exported at module root, and a `gwclient` package that dials and discovers the daemon, performs `gw_hello`/`gw_welcome`, correlates `Do` responses by id, streams `Events()`, and wraps session/catalog/prompt/interject/dialog helpers, so a bot in another Go module can import the client instead of reimplementing it |
| Roles | `admin`/`operator`/`observer` presets or explicit capability lists; `granted` = request ∩ token role; `get_*`/`export_html`/`gw_list_sessions` and receiving events require `observe`, `bash`/`prompt`/`switch_session` require `prompt`, shared-state mutations require `control`, `steer`/`abort`/`clear_queue` require `interject`, `gw_new_session` requires `admin`. Starting a session (implicit creation, `get_state`, `gw_hello.session`) is not itself privileged: the session a client starts is its own, and everything it may do inside it is governed by this table |

Deferred to M4 (optional, `docs/design.md` §14): session groups across
daemons, WebSocket transport for non-local clients, and other transport
adapters behind the same protocol.

## Decision log

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
  so the HTTP debug endpoint can share the same local transport. All clients
  are local. A
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
  `--models`, `--thinking`, `--name`, `--session-dir`, `--no-session`,
  `--api-key`, `--offline`, `--verbose`, plus pi's short aliases (`-a`, `-na`,
  `-ne`, `-ns`, `-np`, `-nc`, `-nbt`, `-nt`, `-t`, `-xt`, `-n`, `-e`). The
  authoritative list is `docs/protocol.md` §4.3. Other pi options error. Not live → applied at spawn. Live → runtime-applicable
  params (`--model`, `--provider`, `--thinking`, `--name`) are applied via RPC
  to shared session state. Spawn-only params are compared against the session's
  recorded spawn config: **identical values are no-ops; only a conflicting
  value fails the attach** with a clear error. (Required for pilish, which
  re-sends `--approve` on every reload.) **Confirmed:** runtime parameter
  changes are session-global, and every client is notified with
  `gw_state_changed`.
- **HTTP debug endpoint** (operator). **Two ports, no auth on the debug port.**
  The session listener stays raw TCP + JSONL; a separate loopback HTTP listener
  serves read-only `debug/status/metrics/catalog`. It requires **no token**
  (loopback-only, like the session port is reachable only locally, but with no
  authentication at all), so it exposes operational metadata and catalog
  metadata (paths, names, titles) to any local process. It must never gain a
  mutating endpoint; putting it behind the token again is a one-line change if
  that exposure is unwanted.
- **Token roles and provisioning** (operator). **Default token full authority;
  optional `tokens.json` for restricted tokens.** The daemon-generated
  `~/.config/pi-gateway/token` always grants every capability.
  `~/.config/pi-gateway/tokens.json` (mode 0600) may add tokens that name a
  preset role (`admin`, `operator`, `observer`) or an explicit capability list;
  `gw_welcome.granted` is the intersection of the client's request with the
  token's role. `pi-gatewayd --provision-token --token-name X --token-role Y`
  mints one and appends it; SIGHUP reloads the file, so no restart is needed.
  Unknown keys are rejected, and each entry sets **either** `role` **or**
  `capabilities`:
  ```json
  {"tokens": [
    {"name": "slack", "token": "<hex>", "role": "operator"},
    {"name": "dashboard", "token": "<hex>", "capabilities": ["observe"]},
    {"name": "audit", "token": "<hex>", "role": "observer", "comment": "read-only"}
  ]}
  ```
- **Logging and metrics** (operator). **Structured `log/slog`, Prometheus text.**
  `--log-format text|json` and `--log-level` on the daemon (text suits journald);
  counters and gauges are exposed on the debug listener's `/metrics` in
  Prometheus text format with no extra dependencies.
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
  rotating means deleting/regenerating the file. It always grants the full
  capability set; restricted tokens come from `tokens.json` (see **Token roles
  and provisioning** above), and the debug listener takes no token at all.
- **Port discovery** (operator). **Hybrid.** The daemon always writes the bound
  port to `~/.config/pi-gateway/port`; the client prefers that file and falls
  back to the fixed default `127.0.0.1:7331`; `--server`/`--port` override.
- **Session creation** (operator). **Explicit create + creator tags.** Non-pi
  clients may create sessions explicitly (a control command returning path and
  name); sessions record the creating client identity/tags, exposed in the
  catalog, so an integration can list its own. pilish's implicit
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
  session-wide, requiring `interject`.** `abort` stops the shared turn, and
  queued prompts remain and run afterwards (pi-native behavior);
  `clear_queue` clears the entire daemon queue plus pi's forwarded steer queue
  and returns all
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
- **Session working directory** (found by the end-to-end suite). **The
  client's directory, carried in the handshake.** `gw_hello.cwd` (and
  `gw_new_session{cwd}`) is validated as an absolute existing directory and
  used as pi's spawn directory, so a session belongs to the project the UI is
  looking at instead of the directory the daemon was started in. An empty
  `cwd` keeps meaning "the daemon's own directory" for bare protocol clients,
  and a session whose file already exists is always respawned in the directory
  recorded in its header — hibernation and daemon restarts ignore the
  attaching client's directory.
- **pi crash** (implementation policy settled). **No auto-respawn.** A crashed
  pi is reported with `gw_session_state{state:"crashed"}`, its pending work
  fails, and the session file is left intact. The daemon does **not** restart
  pi on its own: the next attach re-adopts the session and respawns it (lazy
  re-adoption), so a crash loop cannot burn model quota unattended.
- **pi option aliases** (found by the end-to-end suite). **Accepted.**
  pi's short forms (`-a`, `-na`, `-ne`, `-ns`, `-np`, `-nc`, `-nbt`, `-nt`,
  `-e`, `-t`, `-xt`, `-n`) map to the same canonical parameters as their long
  forms, and `--models`, `--no-themes`, `--offline`, `--verbose` joined the
  accepted set, so a UI that passes `pi -a` no longer fails the handshake.
  Aliases and long forms are recorded identically, so they can never look like
  a spawn-parameter conflict. Options the daemon owns (session selection,
  one-shot modes) are still rejected with `bad_frame`.
- **Exported client library** (operator). **Promote `protocol`, `config` and
  `piargs` out of `internal/`, then add `gwclient`.** The gateway protocol could
  only be spoken from inside this module, so a bot in another repository could
  not import it; Go's `internal/` rule is the blocker, and the bridge's stdio
  relay is not a reusable API. The wire codec, message types, capability table,
  discovery paths and pi-parameter parsing moved to the module-root packages
  `protocol/`, `config/` and `piargs/`; `config.Addresses` is now shared by the
  bridge and the library so both discover the daemon the same way. The new
  `gwclient` package is the supported programmatic client: `Dial` (explicit
  address or discovery, token from file or value), the handshake,
  id-correlated `Do`/`Send`, an `Events()` stream with an explicit overflow
  error instead of silent loss, and typed `NewSession`/`ListSessions`/
  `GetState`/`Prompt`/`Steer`/`Abort`/`ClearQueue`/`SwitchSession`/
  `ReloadSession`/`RespondUI` helpers. Behaviour is unchanged: the daemon still
  owns the single capability table in `protocol/roles.go`, and the bridge's
  observable output is identical (its own tests and the fake-pi end-to-end lane
  still pass). Evidence: `go test -race ./...` green (139 tests) with six new
  ones — `gwclient` (handshake, create + prompt + catalog, response errors,
  close, UI dialog round trip) and `config.TestAddresses` — plus `go vet ./...`
  and `gofmt -l .` clean.

### Open

None. Every gap found while reviewing `docs/protocol.md` draft 2 has been
resolved and recorded above. The **mid-turn attach** entry is deliberately
provisional (live-only) and is the first item to revisit now that the daemon
is implemented.

## Repository layout

```text
cmd/pi-gatewayd/     daemon: listener, auth, session table, connections
cmd/pi-gateway/      bridge: gateway protocol upstream, raw pi RPC on stdio
gwclient/            exported client library: dial, handshake, commands, events
config/              token/port/tokens-file paths, discovery, roles, provisioning
piargs/              accepted pi parameter parsing (shared daemon/client)
protocol/            strict JSONL codec, gw_* messages, id namespacing, capability table
internal/catalog/   session file scanning, name resolution, durable leaf ids
internal/client/     bridge implementation (argv, token/port, relay)
internal/daemon/     session table, attach/rebind, spawn-param checks
internal/debughttp/  read-only /status /catalog /metrics over loopback HTTP
internal/gwlog/      structured logging (log/slog: text for journald, or JSON)
internal/metrics/    Prometheus-text counters and gauges
internal/session/    SessionActor, Hub, PromptQueue, PiProcess
internal/fakepi/     fake pi used by the tests
internal/gwtest/     shared daemon harness for end-to-end tests
internal/testutil/   test helpers (build fake pi, raw protocol client)
packaging/           systemd user unit
```

## Building and testing

### Building and static checks

```bash
gofmt -l .        # must print nothing
go vet ./...
go build ./...
```

Go 1.22+ is required. The only dependencies are the standard library and
`golang.org/x/term` (bridge terminal detection), so `go build` works offline.

### Unit and integration tests

```bash
go test -race ./...                       # everything (133 tests, a few minutes)
go test -race ./internal/daemon/          # the largest package
go test -run TestAttach ./internal/daemon/  # one test
go test -count=2 ./protocol/          # catch state leaking between runs
```

- **No real `pi` is needed.** `internal/fakepi` is a deterministic stand-in
  (with `FAKEPI_*` knobs for turn length, delay, rejection and UI requests) and
  `internal/testutil` builds it on demand. `internal/gwtest` holds the shared
  daemon harness for tests that go over real sockets.
- Tests never touch `~/.pi` and must not write inside the repository: use
  `t.TempDir()` for state directories and copies.
- Two backpressure tests stream tens of megabytes, which is why the full suite
  takes minutes rather than seconds.
- `cmd/pi-gatewayd/main_test.go` parses `packaging/pi-gatewayd.service` and
  fails if the shipped unit drifts from the daemon's flags and signals, so
  change both together.

### End-to-end suites (Emacs + pilish)

`test/e2e/` drives the real stack: Emacs + pilish → `pi-gateway` →
`pi-gatewayd` → `pi`. [`test/e2e/README.md`](../test/e2e/README.md) lists the
cases and the coverage of each suite.

```bash
./test/e2e/run.sh --lane fake --suite both   # fake pi only: free and fast
./test/e2e/run.sh --lane all  --suite both   # also drives the real model
```

- The **real lane spends model quota** and needs a working pi model
  configuration; it is opt-in. Run it when a change touches spawning, resume
  and replay, hibernation, daemon restart, or the bridge's argument surface.
- Isolation: each run copies the agent directory to a temporary one, mints its
  own tokens, picks free ports, and writes nothing into the repository or into
  the user's `~/.pi`.
- Artifacts: `/tmp/pi-gateway-e2e.*/` — `summary.txt`, `logs/<lane>-<suite>.log`
  and one `daemon.log` per daemon, plus `restart-daemon.sh` for restart cases.
- The suites exist to catch integration bugs the unit tests cannot see. Both
  bugs they found so far (the session working directory and pi's short option
  aliases) are recorded in the [decision log](#decision-log).

## Development conventions

- **Docs first.** A behaviour change starts as a decision: add or amend an
  entry in the [decision log](#decision-log), update `docs/design.md` and
  `docs/protocol.md` as needed, and only then change code. Evidence belongs in
  the entry (a pi source citation, an experiment, an end-to-end run).
- **README is user-facing.** Purpose, usage and CLI reference only; development
  detail, status history and the decision log belong here.
- **Every fix gets a regression test**, and the test is checked to fail with
  the fix reverted. Prefer a test that cannot pass vacuously (assert the real
  observable, not a proxy).
- **Keep the invariants.** One pi process per session (sole writer on the
  session file); one capability check per command, driven by the single table
  in `protocol/roles.go`; canonical lowercase command types only;
  terminal events never dropped; both listeners loopback-only; token files
  `0600`; never shadow the real `pi`.
- **Test helpers are shared, not copied.** `internal/testutil` (raw protocol
  client, fake-pi build) and `internal/gwtest` (daemon harness) exist so tests
  exercise the same paths as production.
- **Commits** use an imperative subject and a body that says why, what was
  rejected, and the evidence (test counts, end-to-end matrix). Keep the tree
  clean and push to `origin/main`.
- **Flag unrequested additions** as reversible, and report judgment calls
  rather than silently widening scope.
