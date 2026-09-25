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
var valueFlags = map[string]string{
	"--provider":             KeyProvider,
	"--model":                KeyModel,
	"--thinking":             KeyThinking,
	"--name":                 KeyName,
	"-e":                     "extension",
	"--extension":            "extension",
	"--skill":                "skill",
	"--prompt-template":      "prompt-template",
	"--theme":                "theme",
	"--tools":                "tools",
	"--exclude-tools":        "exclude-tools",
	"--system-prompt":        "system-prompt",
	"--append-system-prompt": "append-system-prompt",
	"--session-dir":          "session-dir",
	"--api-key":              "api-key",
}

// boolFlags maps a boolean flag spelling to a canonical key.
var boolFlags = map[string]string{
	"--approve":          "approve",
	"--no-approve":       "no-approve",
	"--no-extensions":    "no-extensions",
	"--no-context-files": "no-context-files",
	"--no-builtin-tools": "no-builtin-tools",
	"--no-tools":         "no-tools",
	"--no-session":       "no-session",
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
			s.Values[key] = append(s.Values[key], val)
			if !hasInline {
				s.Args = append(s.Args, flag)
			} else {
				s.Args = append(s.Args, flag+"="+val)
			}
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
// flag order is not meaningful, and parameters the request does not mention
// are not conflicts: an omitted parameter means "no preference".
func SpawnConflict(recorded, requested map[string][]string) (string, bool) {
	for key, want := range requested {
		if !sameValues(recorded[key], want) {
			return key, true
		}
	}
	return "", false
}

func sameValues(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	counts := make(map[string]int, len(a))
	for _, v := range a {
		counts[v]++
	}
	for _, v := range b {
		counts[v]--
		if counts[v] < 0 {
			return false
		}
	}
	return true
}
