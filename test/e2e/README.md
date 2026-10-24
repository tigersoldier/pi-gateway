# End-to-end tests: Emacs + pilish → TypeScript gateway → Pi SDK

These suites run real Emacs with the real pilish client against the TypeScript
bridge and daemon. The daemon uses Pi's SDK in-process, and a deterministic
loopback provider scripts model responses; the tests do not use model
credentials or send external inference requests.

```text
Emacs (pilish, unmodified) ── stdio ──> pi-gateway ── TCP ──> pi-gatewayd ──> Pi SDK
```

Nothing in the pilish checkout is modified. The runner configures its gateway
executable, daemon registry, and local provider through the existing ERT test
hooks.

## Running

```console
$ test/e2e/run.sh                 # both suites
$ test/e2e/run.sh --suite a       # pilish integration contracts
$ test/e2e/run.sh --suite b       # gateway-specific cases
$ test/e2e/run.sh --only abort    # ERT selector (regexp)
$ test/e2e/run.sh --no-deps       # use already-installed pilish dependencies
$ test/e2e/run.sh --rm            # remove the run directory on success
```

Requirements: Node.js >=22.19.0, a `pi` executable with its SDK available on
`PATH`, Emacs 29+, and the pilish checkout (default:
`/home/pi/work/performance-fix-2`). The TypeScript daemon resolves `pi` from
`PATH`; it has no `--pi` CLI override. The loopback provider is local and
isolated. `make deps` in the pilish checkout may need network access on first
install; use `--no-deps` when those packages are already available.

Environment overrides: `PILISH_DIR`, `EMACS`, `E2E_ROOT` (run directory), and
`ELPA_CACHE` (pilish dependencies).

## Isolation

- The runner creates an isolated Pi agent directory and configures its model
  endpoint to the loopback provider. It does not read or copy `~/.pi` auth or
  model credentials.
- Each daemon gets its own state and session directories, token, random port,
  and ephemeral debug listener. The client discovers these through the ERT
  registry.
- Nothing is written into the repository; artifacts live under `E2E_ROOT` (a
  `/tmp` directory by default, printed at the end).
- An EXIT/INT/TERM trap stops and waits for the daemon processes and provider,
  including when setup fails after startup.

## Coverage

### Suite A — pilish integration contracts through the gateway

`test/e2e/pilish-suites.el` runs the 15 shared contracts using pilish's
fake-named backend selection, but the backend is the real TypeScript gateway
and Pi SDK with scripted local provider responses. Two local-only cases wait
for an actual text delta before aborting or steering. No fake-Pi subprocess is
used.

| Contract | Cases |
|---|---|
| rpc-smoke | process starts, query-on-exit, get_state (+model, +thinking level), get_commands (+structure, +source info), new_session, fork messages |
| prompt | turn lifecycle (`agent_settled`), abort stops streaming |
| session | session name persists across the session file |
| steering | queued steering is delivered in order |
| tool | a read turn emits the shared tool events |
| streaming | abort and steering happen after a real text delta |

The complete suite A run is expected to pass 17/17 cases.

### Suite B — gateway-specific cases

`test/e2e/gateway-cases.el` drives each case through pilish's client code and
the TypeScript executables.

| Case | What it proves |
|---|---|
| client death mid-turn | a client drop does not end the turn or session; re-attach resumes |
| two clients, one session | both attach and observe a later turn on one session |
| daemon restart | a restarted daemon re-adopts the session file and resumes history |
| hibernation | idle timeout disposes the SDK session; the next attach restores it |
| CLI surfaces | `--version`, client-local `--help`, and unsupported mode handling |
| failure modes | daemon down, bad token, and unreadable token fail quickly |
| restricted tokens | observer may read but cannot prompt, mutate, or run bash |
| spawn parameters | conflicting recorded spawn values are refused |
| session directory | new sessions use the requesting client's directory |

The complete suite B run is expected to pass 9/9 cases. The source assertion
mapping and any architecture-specific adaptations are recorded in
[`../test-parity.md`](../../docs/test-parity.md).

## How it works

- `run.sh` writes wrappers for the TypeScript entry points, installs pilish
dependencies if needed, creates the local provider and isolated agent config,
mints an observer token per daemon, starts a normal and short-idle-timeout
daemon, writes `daemons.json`, then runs Emacs for each selected suite.
- `e2e-common.el` contains shared helpers for the daemon registry,
gateway-backed sessions, event waiting, catalog/status access, CLI invocation,
and daemon restart.
- `pilish-suites.el` selects the shared fake-named contracts and routes them
through the TypeScript daemon and local provider without modifying pilish.
- `gateway-cases.el` defines the gateway-specific cases and runs ERT with the
`gateway-e2e-*` selector.
- `restart-daemon.sh` is generated in the run directory and restarts a daemon
from its recorded command line.

## Findings covered by the suites

- **Session working directory:** `gw_hello.cwd` and `gw_new_session{cwd}` carry
the requesting project's directory. The daemon validates it and uses it when
creating a session; restored sessions use the directory recorded with the
session. See `gateway-e2e-session-follows-client-directory` and the TypeScript
lifecycle tests.
- **Pi option surface:** the bridge accepts pi's documented short aliases and
supported long options, while refusing options the gateway owns or does not
model before connecting.
- **Local provider cleanup:** an injected setup failure after daemon and
provider startup verifies the trap stops and waits for both processes.
