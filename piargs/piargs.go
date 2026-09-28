// Package piargs parses the accepted subset of pi's command-line parameters
// (docs/protocol.md §4.3). It is shared by the client bridge, which validates
// what a UI passed, and the daemon, which records spawn configuration and must
// detect conflicts when a live session is re-attached.
package piargs

import (
	"fmt"
	"strings"
)

// Canonical keys.
const (
	KeyModel    = "model"
	KeyProvider = "provider"
	KeyThinking = "thinking"
	KeyName     = "name"
)

// valueFlags maps a flag spelling to a canonical key. Each takes a value.
// pi's short aliases map to the same keys, so `-n x` and `--name x` are the
// same spawn parameter.
var valueFlags = map[string]string{
	"--provider":             KeyProvider,
	"--model":                KeyModel,
	"--models":               "models",
	"--thinking":             KeyThinking,
	"--name":                 KeyName,
	"-n":                     KeyName,
	"-e":                     "extension",
	"--extension":            "extension",
	"--skill":                "skill",
	"--prompt-template":      "prompt-template",
	"--theme":                "theme",
	"--tools":                "tools",
	"-t":                     "tools",
	"--exclude-tools":        "exclude-tools",
	"-xt":                    "exclude-tools",
	"--system-prompt":        "system-prompt",
	"--append-system-prompt": "append-system-prompt",
	"--session-dir":          "session-dir",
	"--api-key":              "api-key",
}

// boolFlags maps a boolean flag spelling to a canonical key.
var boolFlags = map[string]string{
	"--approve":             "approve",
	"-a":                    "approve",
	"--no-approve":          "no-approve",
	"-na":                   "no-approve",
	"--no-extensions":       "no-extensions",
	"-ne":                   "no-extensions",
	"--no-skills":           "no-skills",
	"-ns":                   "no-skills",
	"--no-prompt-templates": "no-prompt-templates",
	"-np":                   "no-prompt-templates",
	"--no-themes":           "no-themes",
	"--no-context-files":    "no-context-files",
	"-nc":                   "no-context-files",
	"--no-builtin-tools":    "no-builtin-tools",
	"-nbt":                  "no-builtin-tools",
	"--no-tools":            "no-tools",
	"-nt":                   "no-tools",
	"--no-session":          "no-session",
	"--offline":             "offline",
	"--verbose":             "verbose",
}

// reservedFlags are pi options the gateway owns and must never forward.
var reservedFlags = map[string]string{
	"--mode":    "the gateway always runs pi in RPC mode",
	"--session": "the daemon owns session files; use switch_session instead",
	"--help":    "use pi-gateway --help",
	"-h":        "use pi-gateway --help",
	"--version": "pi-gateway answers --version from the managed pi binary",
}

// Spec is the result of parsing accepted pi parameters.
type Spec struct {
	// Args is the accepted argument list in its original order and spelling.
	Args []string
	// Values maps canonical keys to all values seen (boolean flags record
	// "true"/"false").
	Values map[string][]string
	// groups is the parsed argument list grouped by canonical key, used to
	// rebuild a canonical spawn configuration (docs/protocol.md §4.3).
	groups []argGroup
}

// argGroup is one accepted flag together with the tokens that carry it
// (either [flag, value] or [flag=value], or a single boolean flag). value is
// the canonical value the flag stands for: for `--no-approve` it is "false"
// under the logical key "approve", so the trust setting has one key and
// `--approve`/`--no-approve` are the same parameter.
type argGroup struct {
	key     string
	tokens  []string
	value   string
	runtime bool
}

// runtimeKeys are the parameters pi can change at runtime; they are applied to
// a live session through RPC instead of being part of its spawn configuration
// (docs/protocol.md §4.3).
func isRuntimeKey(key string) bool {
	switch key {
	case KeyModel, KeyProvider, KeyThinking, KeyName:
		return true
	}
	return false
}

// canonicalFlags maps a canonical key back to its long flag spelling, so a
// recorded spawn configuration never mixes an alias with its long form.
var canonicalFlags = func() map[string]string {
	out := make(map[string]string, len(valueFlags)+len(boolFlags))
	for flag, key := range valueFlags {
		if !strings.HasPrefix(flag, "--") {
			continue
		}
		out[key] = flag
	}
	for flag, key := range boolFlags {
		if !strings.HasPrefix(flag, "--") {
			continue
		}
		out[key] = flag
	}
	return out
}()

// negatedBool maps a boolean key to the flag that expresses the opposite
// state, so a stored "false" can be spelled positively (and a "false" for a
// negative flag can be dropped when no positive counterpart exists).
var negatedBool = map[string]string{
	"approve":    "no-approve",
	"no-approve": "approve",
}

// Parse validates args and returns the accepted spec. Every argument must be
// an accepted pi option with its value; unknown or reserved options and
// positional arguments are rejected.
func Parse(args []string) (*Spec, error) {
	s := &Spec{Values: make(map[string][]string)}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if !strings.HasPrefix(arg, "-") {
			return nil, fmt.Errorf("unexpected argument %q (pi-gateway takes pi options only)", arg)
		}
		flag, inline, hasInline := SplitFlag(arg)
		if why, reserved := reservedFlags[flag]; reserved {
			return nil, fmt.Errorf("pi option %s is not accepted: %s", flag, why)
		}
		if key, ok := boolFlags[flag]; ok {
			val := "true"
			if hasInline {
				if inline != "true" && inline != "false" {
					return nil, fmt.Errorf("pi option %s takes true or false, got %q", flag, inline)
				}
				val = inline
			}
			// `--no-approve` is the same setting as `--approve` with the
			// opposite value, so a recorded trust setting cannot be silently
			// contradicted by the other spelling (docs/protocol.md §4.3).
			if key == "no-approve" {
				key = "approve"
				if val == "true" {
					val = "false"
				} else {
					val = "true"
				}
			}
			s.Values[key] = append(s.Values[key], val)
			tokens := []string{flag}
			if hasInline {
				// pi only accepts the bare boolean spelling; `--approve=false`
				// is an unknown flag to it. Normalize to the flag that expresses
				// the logical value, and drop a false with no opposite flag
				// (omitting reproduces the default).
				tokens = nil
				if flag, ok := canonicalFlags[key]; ok && val == "true" {
					tokens = []string{flag}
				} else if opposite, ok := negatedBool[key]; ok {
					if oflag, ok := canonicalFlags[opposite]; ok {
						tokens = []string{oflag}
					}
				}
			}
			s.Args = append(s.Args, tokens...)
			s.groups = append(s.groups, argGroup{key: key, tokens: tokens, value: val, runtime: isRuntimeKey(key)})
			continue
		}
		key, ok := valueFlags[flag]
		if !ok {
			return nil, fmt.Errorf("unsupported pi option %q", flag)
		}
		val := inline
		if !hasInline {
			if i+1 >= len(args) {
				return nil, fmt.Errorf("pi option %s requires a value", flag)
			}
			i++
			val = args[i]
		}
		s.Values[key] = append(s.Values[key], val)
		s.Args = append(s.Args, flag, val)
		s.groups = append(s.groups, argGroup{key: key, tokens: []string{flag, val}, value: val, runtime: isRuntimeKey(key)})
	}
	return s, nil
}

// SetName sets the session name, replacing any --name the caller passed. It
// is used by gw_new_session, whose explicit name field wins over piArgs.
func (s *Spec) SetName(name string) {
	s.Values[KeyName] = []string{name}
	out := s.Args[:0]
	for i := 0; i < len(s.Args); i++ {
		flag, _, hasInline := SplitFlag(s.Args[i])
		if key, ok := valueFlags[flag]; ok && key == KeyName {
			if !hasInline {
				i++ // skip the value as well
			}
			continue
		}
		out = append(out, s.Args[i])
	}
	s.Args = append(out, "--name", name)
}

// SplitFlag splits "--flag=value" into ("--flag", "value", true). A flag
// without '=' returns ("--flag", "", false).
func SplitFlag(arg string) (flag, value string, hasValue bool) {
	if i := strings.IndexByte(arg, '='); i >= 0 {
		return arg[:i], arg[i+1:], true
	}
	return arg, "", false
}

// RuntimeValues returns the runtime-applicable parameters (last value wins).
func (s *Spec) RuntimeValues() map[string]string {
	out := make(map[string]string)
	for _, key := range []string{KeyModel, KeyProvider, KeyThinking, KeyName} {
		if vals := s.Values[key]; len(vals) > 0 {
			out[key] = vals[len(vals)-1]
		}
	}
	return out
}

// secretSpawnKeys are spawn parameters whose value is a credential and must
// never be exposed through gw_list_sessions or the debug catalog. The value is
// still persisted (it is needed to respawn) and still participates in conflict
// detection; only the exposed view drops it.
var secretSpawnKeys = map[string]bool{
	"api-key": true,
}

// RedactedSpawnValues copies a spawn configuration for exposure, dropping
// credential values. It never returns the input map, so callers cannot leak a
// secret by mutating the result.
func RedactedSpawnValues(values map[string][]string) map[string][]string {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string][]string, len(values))
	for k, v := range values {
		if secretSpawnKeys[k] {
			continue
		}
		out[k] = append([]string(nil), v...)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// CanonicalSpawnArgs returns the spawn-only arguments in canonical long-flag
// form, so a persisted configuration can never hold both `-a` and `--approve`.
// A boolean whose stored value is "false" is written as its opposite flag when
// one exists, and omitted otherwise (omitting reproduces the default).
func (s *Spec) CanonicalSpawnArgs() []string {
	var out []string
	for _, g := range s.groups {
		if g.runtime {
			continue
		}
		flag, ok := canonicalFlags[g.key]
		if !ok {
			out = append(out, g.tokens...)
			continue
		}
		if len(g.tokens) == 1 {
			// Boolean flag: emit only the true state, or the opposite flag for a
			// stored false when one exists (omitting reproduces the default).
			if g.value == "true" {
				out = append(out, flag)
				continue
			}
			if opposite, ok := negatedBool[g.key]; ok {
				if oflag, ok := canonicalFlags[opposite]; ok {
					out = append(out, oflag)
				}
			}
			continue
		}
		out = append(out, flag, g.tokens[1])
	}
	return out
}

// MergeSpawn builds the effective argument list for spawning (or respawning) a
// session that has a recorded spawn configuration. Recorded spawn-only values
// win; the requester's spawn-only arguments are appended only for keys the
// record does not mention; every runtime argument the requester passed is kept
// so it can be applied via RPC on a live session or at spawn on a cold one
// (docs/protocol.md §4.3). recordedSpawnArgs must be canonical spawn-only args
// (CanonicalSpawnArgs), as written by the daemon's spawn sidecar.
func MergeSpawn(recordedSpawnArgs []string, requested *Spec) []string {
	recorded, err := Parse(recordedSpawnArgs)
	if err != nil {
		// A corrupt record must not stop a session from starting; fall back to
		// what the requester asked for.
		return append([]string(nil), requested.Args...)
	}
	out := append([]string(nil), recordedSpawnArgs...)
	for _, g := range requested.groups {
		if _, has := recorded.Values[g.key]; !g.runtime && has {
			continue // recorded wins
		}
		out = append(out, g.tokens...)
	}
	return out
}

// SpawnValues returns every canonical value except the runtime-applicable
// ones. These must match a live session's recorded configuration exactly.
func (s *Spec) SpawnValues() map[string][]string {
	out := make(map[string][]string)
	for k, v := range s.Values {
		switch k {
		case KeyModel, KeyProvider, KeyThinking, KeyName:
			continue
		}
		out[k] = append([]string(nil), v...)
	}
	return out
}

// SpawnConflict reports the first spawn-only key whose requested value differs
// from the recorded session configuration. Values are compared as sets because
// flag order is not meaningful. A parameter the request does not mention is not
// a conflict, and neither is a parameter the record never set: the session
// keeps what it has, and a client that asks for a spawn value the session was
// not created with is not refused attachment (docs/protocol.md §4.3).
func SpawnConflict(recorded, requested map[string][]string) (string, bool) {
	for key, want := range requested {
		rec, ok := recorded[key]
		if !ok {
			continue
		}
		if !sameValues(rec, want) {
			return key, true
		}
	}
	return "", false
}

// sameValues compares values as sets: flag order is not meaningful, and
// repeating an identical flag (`--approve --approve`) is a no-op, so it must
// not look like a conflict.
func sameValues(a, b []string) bool {
	set := make(map[string]bool, len(a))
	for _, v := range a {
		set[v] = true
	}
	seen := make(map[string]bool, len(b))
	for _, v := range b {
		if !set[v] {
			return false
		}
		seen[v] = true
	}
	return len(seen) == len(set)
}
