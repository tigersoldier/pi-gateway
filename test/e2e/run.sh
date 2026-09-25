#!/usr/bin/env bash
# End-to-end test runner: real Emacs + pilish -> pi-gateway -> pi-gatewayd -> pi.
#
# Usage: test/e2e/run.sh [options]
#   --lane LANE     fake | real | all        (default: all)
#   --suite SUITE   a | b | both             (default: both)
#   --only REGEXP   ERT selector (regexp) applied to the selected suites
#   --build         force a rebuild of the binaries
#   --no-deps       skip pilish dependency installation
#   --rm            remove the run directory afterwards
#   -h, --help
#
# Environment overrides:
#   PILISH_DIR      pilish checkout (default: /home/pi/work/performance-fix-2)
#   PI_BIN          pi executable the daemons manage (default: pi on PATH)
#   EMACS           emacs executable (default: emacs)
#   E2E_ROOT        run directory (default: mktemp -d)
#   ELPA_CACHE      package dir for pilish deps (default: /tmp/pi-gateway-e2e-elpa)
#
# Layout of a run directory:
#   bin/                 freshly built pi-gateway and pi-gatewayd
#   agent/               isolated pi agent dir (auth/models copied, 0600)
#   work/                daemon working directory (test "project")
#   d/<name>/            one daemon: cmd.sh, daemon.log, pid, port, state/
#   daemons.json         registry the Elisp suites read
#   logs/                ERT output per lane and suite
#   summary.txt          per-case matrix

set -euo pipefail

E2E_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(cd "$E2E_DIR/../.." && pwd)
PILISH_DIR=${PILISH_DIR:-/home/pi/work/performance-fix-2}
PI_BIN=${PI_BIN:-$(command -v pi || true)}
EMACS=${EMACS:-emacs}
ELPA_CACHE=${ELPA_CACHE:-/tmp/pi-gateway-e2e-elpa}
LANE=all
SUITE=both
ONLY=${ONLY:-}
FORCE_BUILD=0
INSTALL_DEPS=1
CLEANUP=0

while [ $# -gt 0 ]; do
  case "$1" in
    --lane) LANE=$2; shift 2 ;;
    --suite) SUITE=$2; shift 2 ;;
    --only) ONLY=$2; shift 2 ;;
    --build) FORCE_BUILD=1; shift ;;
    --no-deps) INSTALL_DEPS=0; shift ;;
    --rm) CLEANUP=1; shift ;;
    -h|--help) sed -n '2,30p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done

E2E_ROOT=${E2E_ROOT:-$(mktemp -d /tmp/pi-gateway-e2e.XXXXXX)}
BIN=$E2E_ROOT/bin
AGENT=$E2E_ROOT/agent
WORK=$E2E_ROOT/work
LOGS=$E2E_ROOT/logs
PACKAGE_USER_DIR=$ELPA_CACHE/$( ( "$EMACS" --batch -Q --eval '(princ emacs-major-version)' 2>/dev/null ) )

say()  { printf '\n\033[1m== %s\033[0m\n' "$*"; }
note() { printf '   %s\n' "$*"; }
die()  { printf 'error: %s\n' "$*" >&2; exit 1; }

say "preflight"
[ -d "$PILISH_DIR" ] || die "pilish checkout not found: $PILISH_DIR (set PILISH_DIR)"
[ -x "$PILISH_DIR/test/support/fake_pi.py" ] || die "pilish fake-pi missing under $PILISH_DIR"
[ -n "$PI_BIN" ] && [ -x "$PI_BIN" ] || die "pi executable not found (set PI_BIN)"
command -v "$EMACS" >/dev/null || die "emacs not found"
command -v python3 >/dev/null || die "python3 not found"
command -v npm >/dev/null || note "npm missing (only needed for a fresh pilish dep install)"
note "repo         $REPO"
note "pilish       $PILISH_DIR"
note "pi           $PI_BIN ($("$PI_BIN" --version 2>/dev/null | head -1))"
note "emacs        $($EMACS --version | head -1)"
note "run dir      $E2E_ROOT"

say "build"
mkdir -p "$BIN" "$LOGS" "$WORK"
if [ "$FORCE_BUILD" = 1 ] || [ ! -x "$BIN/pi-gatewayd" ] || [ -n "$(find "$REPO/cmd" "$REPO/internal" -newer "$BIN/pi-gatewayd" -name '*.go' -print -quit 2>/dev/null)" ]; then
  ( cd "$REPO" && go build -o "$BIN/pi-gatewayd" ./cmd/pi-gatewayd && go build -o "$BIN/pi-gateway" ./cmd/pi-gateway )
  note "built pi-gatewayd and pi-gateway"
else
  note "binaries up to date"
fi

say "pilish dependencies"
if [ "$INSTALL_DEPS" = 1 ]; then
  if [ ! -f "$PACKAGE_USER_DIR/.deps-stamp" ]; then
    ( cd "$PILISH_DIR" && make deps PACKAGE_USER_DIR="$PACKAGE_USER_DIR" >"$LOGS/deps.log" 2>&1 ) \
      || { tail -20 "$LOGS/deps.log"; die "pilish deps install failed (see $LOGS/deps.log)"; }
    note "installed into $PACKAGE_USER_DIR"
  else
    note "using $PACKAGE_USER_DIR"
  fi
fi

say "isolated pi agent dir"
mkdir -p "$AGENT"
for f in auth.json models.json; do
  [ -f "$HOME/.pi/agent/$f" ] || die "missing ~/.pi/agent/$f"
  cp "$HOME/.pi/agent/$f" "$AGENT/$f"
done
chmod 600 "$AGENT/auth.json"
python3 - "$HOME/.pi/agent/settings.json" "$AGENT/settings.json" <<'PY'
import json, sys
src, dst = sys.argv[1], sys.argv[2]
data = json.load(open(src))
data.pop("packages", None)          # keep the run free of unrelated extensions
data.pop("lastChangelogVersion", None)
json.dump(data, open(dst, "w"), indent=2)
PY
note "agent dir $AGENT (auth copied 0600, npm extensions disabled)"

free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'
}

# start_daemon NAME PI_COMMAND [extra daemon args...]
declare -a DAEMON_NAMES=()
declare -A DAEMON_ADDR=() DAEMON_DEBUG=() DAEMON_PID=()
start_daemon() {
  local name=$1 pi_cmd=$2; shift 2
  local dir
  local state
  local port
  dir=$E2E_ROOT/d/$name
  state=$dir/state
  port=$(free_port)
  mkdir -p "$dir"
  printf '%s\n' "$port" >"$dir/port"
  # A restricted (observer) token for the capability case, minted before the
  # daemon starts so no reload is needed.  The printed value is the token; the
  # daemon-side file keeps the role.
  "$BIN/pi-gatewayd" --provision-token --token-name e2e-observer --token-role observer \
    --state-dir "$state" --tokens-file "$state/tokens.json" >"$dir/token-observer" 2>/dev/null
  chmod 600 "$dir/token-observer"
  {
    printf '#!/usr/bin/env bash\n# generated by run.sh: restarts %s identically\n' "$name"
    printf 'cd %q\n' "$WORK"
    printf 'exec env PI_CODING_AGENT_DIR=%q %q --pi %q --state-dir %q --port %s --session-dir %q --tokens-file %q --log-level debug --log-format text --debug-addr 127.0.0.1:0' \
      "$AGENT" "$BIN/pi-gatewayd" "$pi_cmd" "$state" "$port" "$AGENT/sessions" "$state/tokens.json"
    printf ' %q' "$@"
    printf '\n'
  } >"$dir/cmd.sh"
  chmod +x "$dir/cmd.sh"
  bash "$dir/cmd.sh" >"$dir/daemon.log" 2>&1 &
  echo $! >"$dir/pid"
  local deadline=$((SECONDS + 20))
  while [ ! -s "$state/port" ] || [ ! -s "$state/debug-port" ]; do
    [ $SECONDS -lt $deadline ] || { tail -20 "$dir/daemon.log"; die "daemon $name did not start"; }
    sleep 0.1
  done
  DAEMON_NAMES+=("$name")
  DAEMON_ADDR[$name]="127.0.0.1:$(cat "$state/port")"
  DAEMON_DEBUG[$name]="127.0.0.1:$(cat "$state/debug-port")"
  DAEMON_PID[$name]=$(cat "$dir/pid")
  note "$name: ${DAEMON_ADDR[$name]} (debug ${DAEMON_DEBUG[$name]}, pid ${DAEMON_PID[$name]})"
}

fake_wrapper() {
  local scenario=$1 wrapper=$E2E_ROOT/pi-fake-$scenario
  {
    printf '#!/bin/sh\n'
    # The daemon probes `<pi> --version` at startup; the fake harness has no
    # such flag, so answer it here (the string is opaque to the daemon).
    printf 'if [ "$1" = "--version" ]; then echo "0.0.0-e2e-%s"; exit 0; fi\n' "$scenario"
    printf 'exec %q %q --scenario %q "$@"\n' \
      "$(command -v python3)" "$PILISH_DIR/test/support/fake_pi.py" "$scenario"
  } >"$wrapper"
  chmod +x "$wrapper"
  printf '%s' "$wrapper"
}

say "daemons"
if [ "$LANE" = all ] || [ "$LANE" = real ]; then
  start_daemon real "$PI_BIN"
  # A second real daemon with aggressive idle reaping for the hibernation case.
  start_daemon hibernate "$PI_BIN" --idle-timeout 2s --short-grace 2s
fi
if [ "$LANE" = all ] || [ "$LANE" = fake ]; then
  for scenario in prompt-lifecycle extension-confirm tool-read-contract; do
    start_daemon "$scenario" "$(fake_wrapper "$scenario")"
  done
fi

python3 - "$E2E_ROOT" "${DAEMON_NAMES[@]}" <<'PY'
import json, os, sys
root, names = sys.argv[1], sys.argv[2:]
out = {}
for name in names:
    state = os.path.join(root, "d", name, "state")
    rec = {
        "name": name,
        "addr": "127.0.0.1:" + open(os.path.join(state, "port")).read().strip(),
        "debug": "127.0.0.1:" + open(os.path.join(state, "debug-port")).read().strip(),
        "token": os.path.join(state, "token"),
        "pid": int(open(os.path.join(root, "d", name, "pid")).read().strip()),
    }
    obs = os.path.join(root, "d", name, "token-observer")
    if os.path.exists(obs):
        rec["observerToken"] = obs
    out[name] = rec
json.dump(out, open(os.path.join(root, "daemons.json"), "w"), indent=2)
PY
note "registry $E2E_ROOT/daemons.json"

cat >"$E2E_ROOT/restart-daemon.sh" <<'SH'
#!/usr/bin/env bash
# Stop and restart a daemon from the run registry (used by the restart case).
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
name=$1
dir=$root/d/$name
[ -f "$dir/pid" ] || { echo "no such daemon: $name" >&2; exit 1; }
pid=$(cat "$dir/pid")
if kill -0 "$pid" 2>/dev/null; then
  kill "$pid"
  for _ in $(seq 1 100); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
fi
rm -f "$dir/state/debug-port"
nohup bash "$dir/cmd.sh" >>"$dir/daemon.log" 2>&1 &
echo $! >"$dir/pid"
port=$(cat "$dir/port")
for _ in $(seq 1 200); do
  [ -s "$dir/state/debug-port" ] || { sleep 0.1; continue; }
  if python3 -c "
import socket,sys
s=socket.socket(); s.settimeout(0.5)
try:
    s.connect(('127.0.0.1',$port)); s.close()
except OSError:
    sys.exit(1)
"; then break; fi
  sleep 0.1
done
# The debug listener picks a new port on every start; keep the registry (and
# therefore every later case) pointed at the live one.
python3 - "$root" "$name" <<'PY'
import json, os, sys
root, name = sys.argv[1], sys.argv[2]
path = os.path.join(root, "daemons.json")
data = json.load(open(path))
state = os.path.join(root, "d", name, "state")
data[name]["debug"] = "127.0.0.1:" + open(os.path.join(state, "debug-port")).read().strip()
data[name]["pid"] = int(open(os.path.join(root, "d", name, "pid")).read().strip())
json.dump(data, open(path, "w"), indent=2)
PY
SH
chmod +x "$E2E_ROOT/restart-daemon.sh"

run_emacs() {
  # run_emacs LOG SUITE_FILES... ; lane comes from $LANE_RUN
  local log=$1; shift
  local backend=$LANE_RUN
  say "suite $* (lane $LANE_RUN)"
  set +e
  env -u DISPLAY \
    PACKAGE_USER_DIR="$PACKAGE_USER_DIR" \
    E2E_PILISH_DIR="$PILISH_DIR" \
    E2E_GATEWAY_BIN="$BIN/pi-gateway" \
    E2E_DAEMONS_FILE="$E2E_ROOT/daemons.json" \
    E2E_RESTART_SCRIPT="$E2E_ROOT/restart-daemon.sh" \
    E2E_WORK_DIR="$WORK" \
    E2E_LANE="$LANE_RUN" \
    E2E_SELECTOR="$ONLY" \
    PI_RUN_INTEGRATION=1 \
    PI_INTEGRATION_BACKENDS="$backend" \
    PI_CODING_AGENT_DIR="$AGENT" \
    "$EMACS" --batch -Q \
      -L "$PILISH_DIR" -L "$PILISH_DIR/test" \
      --eval '(setq load-prefer-newer t)' \
      -l "$@" >"$log" 2>&1
  local status=$?
  set -e
  grep -E '^ +(passed|FAILED|skipped) ' "$log" | sed 's/^ */   /' || true
  if [ $status -ne 0 ]; then
    note "FAILED (see $log)"
    grep -A15 -E '^ +(FAILED|ERROR) ' "$log" | head -60 || true
  fi
  return $status
}

FAILED=0
run_lane() {
  LANE_RUN=$1
  if [ "$SUITE" = a ] || [ "$SUITE" = both ]; then
    run_emacs "$LOGS/$LANE_RUN-a.log" "$E2E_DIR/pilish-suites.el" || FAILED=1
  fi
  if [ "$SUITE" = b ] || [ "$SUITE" = both ]; then
    run_emacs "$LOGS/$LANE_RUN-b.log" "$E2E_DIR/gateway-cases.el" || FAILED=1
  fi
}
[ "$LANE" = all ] || [ "$LANE" = fake ] && run_lane fake
[ "$LANE" = all ] || [ "$LANE" = real ] && run_lane real

say "teardown"
for name in "${DAEMON_NAMES[@]}"; do
  pid=$(cat "$E2E_ROOT/d/$name/pid" 2>/dev/null || true)
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    note "stopped $name (pid $pid)"
  fi
done

{
  printf 'pi-gateway end-to-end run %s\n' "$(date -Is)"
  printf 'pilish=%s pi=%s lanes=%s suite=%s only=%s\n\n' \
    "$PILISH_DIR" "$("$PI_BIN" --version 2>/dev/null | head -1)" "$LANE" "$SUITE" "${ONLY:-<all>}"
  for log in "$LOGS"/*.log; do
    [ -f "$log" ] || continue
    printf '## %s\n' "$(basename "$log" .log)"
    if grep -qE '^ +FAILED ' "$log"; then
      printf 'status: FAILED\n'
    else
      printf 'status: ok\n'
    fi
    grep -E '^ +(passed|FAILED|skipped) +[0-9]+/[0-9]+ +' "$log" \
      | sed -E 's/^ +([a-z]+) +([0-9]+\/[0-9]+) +([^ ]+).*/  \1 \3/' || true
    grep -E '^Ran [0-9]+ tests' "$log" | sed 's/^/  /' || true
    printf '\n'
  done
} >"$E2E_ROOT/summary.txt"

say "summary"
cat "$E2E_ROOT/summary.txt"
if [ "$CLEANUP" = 1 ]; then
  if [ "$FAILED" = 0 ]; then rm -rf "$E2E_ROOT"; else note "keeping $E2E_ROOT (failures)"; fi
else
  note "artifacts in $E2E_ROOT"
fi
exit $FAILED
