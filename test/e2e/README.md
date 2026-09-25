# End-to-end tests: Emacs + pilish → pi-gateway → pi-gatewayd → pi

These suites run **real Emacs with the real pilish client** against a real
`pi-gatewayd`, instead of stubbing either side.  The only thing that can be
faked is the *model side* (pi itself), and only in the `fake` lane, where the
daemon spawns pilish's scenario-driven fake pi so assertions stay deterministic.

```
Emacs (pilish, unmodified) ── stdio ──> pi-gateway ── TCP ──> pi-gatewayd ──> pi
```

Nothing in the pilish checkout is modified: the executor is re-pointed by
advising `pilish-test-backend-spec` and by passing the daemon flags the suite
already forwards to whatever executable it was given.

## Running

```console
$ test/e2e/run.sh                 # everything: 2 model sides, 2 suites, 8 cases
$ test/e2e/run.sh --lane fake     # free and deterministic (no model calls)
$ test/e2e/run.sh --lane real     # real pi + the configured model
$ test/e2e/run.sh --suite a       # only pilish's integration contracts
$ test/e2e/run.sh --suite b       # only the gateway-specific cases
$ test/e2e/run.sh --only abort    # ERT selector (regexp) within the suites
$ test/e2e/run.sh --build --rm    # force rebuild, delete the run dir on success
```

Requirements: Emacs (29+), a working `pi` with a configured provider (for the
real lane), the pilish checkout, `python3`, npm on the first dep install, and
network access once for `make deps`.

Environment overrides: `PILISH_DIR`, `PI_BIN`, `EMACS`, `E2E_ROOT` (run dir),
`ELPA_CACHE` (pilish deps).

### Isolation

* pi runs against a **copied agent dir** (`auth.json` 0600, `models.json`, and a
  settings file with the npm extensions stripped), never `~/.pi`.
* Each daemon gets its own state dir, session dir, token, random port and
  unauthenticated debug listener on an ephemeral port; the client finds them
  through `--server`/`--token-file` computed by the suites.
* Nothing is written inside the repository; artifacts live under `E2E_ROOT`
  (a `/tmp` directory; the path is printed at the end).

## Coverage

### Suite A — pilish's own integration contracts, through the gateway

`test/e2e/pilish-suites.el` re-runs pilish's 15 shared contracts with
`pilish-executable` = `pi-gateway`.  The suite's `fake` backend becomes
"fake model side through the gateway" (its `--scenario` arguments are baked
into the daemon's pi wrapper, because the bridge deliberately rejects pi
options it does not model) and its `real` backend becomes real pi.

| Contract | Cases |
|---|---|
| rpc-smoke | process starts, query-on-exit, get_state (+model, +thinking level), get_commands (+structure, +source info), new_session, fork messages |
| prompt | turn lifecycle (streaming, `agent_settled`), abort stops streaming |
| session | session name persists across the session file |
| steering | queued steering is delivered in order |
| tool | a read turn emits the shared tool events |

### Suite B — gateway-specific cases

`test/e2e/gateway-cases.el`, all driven through pilish's client code:

| Case | What it proves | Lanes |
|---|---|---|
| client death mid-turn | an ssh-style client drop does not end the turn or the session; a re-attach resumes it | fake, real |
| two clients, one session | both attach, both observe a later turn, the session stays single | fake, real |
| daemon restart | a restarted daemon re-adopts the session file and resumes its history | real |
| hibernation | idle timeout stops pi; the next attach respawns it with history intact | real |
| CLI surfaces | `--version` is daemon-answered, `--help` is client-local, other modes are refused | fake, real |
| failure modes | daemon down, bad token, unreadable token: fast, loud, no hang | fake, real |
| restricted tokens | an observer token may read but not prompt, mutate, or run bash | fake, real |
| spawn parameters | a differing spawn-only value is refused; an omitted parameter is not a conflict | fake, real |
| session directory | a new session runs pi in the client's directory (recorded in the session header), not the daemon's | fake, real |

The fake lane skips the two cases that need real pi's `--session` resume
(the scenario harness has no resume support) and the real lane runs all eight.

## How it works

* `run.sh` builds the binaries, installs pilish's deps into `ELPA_CACHE`,
  prepares the agent dir, mints an observer token per daemon, starts the
  daemons (one per fake scenario, plus a real one and a short-idle-timeout
  one), writes `daemons.json`, and runs Emacs once per lane and suite.
* `e2e-common.el` holds the shared helpers: daemon registry, gateway-backed
  client sessions, event waiting, catalog/status access, CLI invocation and
  daemon restart.
* `pilish-suites.el` loads pilish's test support, advises the backend spec, and
  runs ERT with the `pilish-integration-*/(fake|real)` selector.
* `gateway-cases.el` defines the gateway cases and runs ERT with the
  `gateway-e2e-*` selector.
* `restart-daemon.sh` (generated) stops and restarts a daemon from its recorded
  command line and updates `daemons.json` with the new debug port.

## Findings the suites produced

Both findings below were found by this harness, fixed, and are now covered by
cases (and by Go tests beside the code):

* **Session working directory**: new sessions used to inherit the *daemon's*
  directory, because the protocol carried no client cwd.  `gw_hello.cwd` (and
  `gw_new_session{cwd}`) now carry it, the daemon validates it as an absolute
  existing directory and spawns pi there, and a session whose file already
  exists is respawned in the directory recorded in its header — so hibernation
  and daemon restarts ignore whichever client attaches.  See
  `gateway-e2e-session-follows-client-directory` and the cwd tests in
  `internal/daemon/cwd_test.go`.
* **pi option surface**: the bridge used to reject pi's short aliases (`-a`,
  `-nt`, `-t`, …) and a few long options, so a UI that passed `pi -a` could
  not use the gateway.  Aliases now map to the same canonical parameters as
  their long forms, and `--models`, `--no-themes`, `--offline`, `--verbose`
  are accepted; options the daemon owns (session selection, one-shot modes)
  are still rejected with `bad_frame`.
* **Fake harness preconditions**: the daemon probes `<pi> --version` at
  startup, so the generated wrapper answers it; the scenario harness has no
  `--session`, hence no restart coverage in the fake lane.
