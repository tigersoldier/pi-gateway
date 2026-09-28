package piargs

import (
	"reflect"
	"strings"
	"testing"
)

func TestParseAcceptsPilishArgv(t *testing.T) {
	args := []string{"--approve", "-e", "/tmp/ext.ts", "--model", "gpt-5",
		"--provider", "openai", "--thinking", "high", "--no-session", "--name", "work"}
	spec, err := Parse(args)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(spec.Args) != len(args) {
		t.Fatalf("accepted args changed length: %v", spec.Args)
	}
	rt := spec.RuntimeValues()
	if rt[KeyModel] != "gpt-5" || rt[KeyProvider] != "openai" || rt[KeyThinking] != "high" || rt[KeyName] != "work" {
		t.Fatalf("runtime values: %v", rt)
	}
	spawn := spec.SpawnValues()
	if len(spawn["extension"]) != 1 || spawn["extension"][0] != "/tmp/ext.ts" {
		t.Fatalf("extension not recorded: %v", spawn)
	}
	if _, ok := spawn[KeyModel]; ok {
		t.Fatalf("runtime keys must not be spawn values: %v", spawn)
	}
}

func TestParseEqualsForm(t *testing.T) {
	spec, err := Parse([]string{"--model=sonnet", "--approve=true"})
	if err != nil {
		t.Fatal(err)
	}
	if spec.RuntimeValues()[KeyModel] != "sonnet" {
		t.Fatalf("equals form not parsed: %v", spec.Values)
	}
	if got := spec.Values["approve"]; len(got) != 1 || got[0] != "true" {
		t.Fatalf("boolean equals form: %v", spec.Values)
	}
	// pi only accepts the bare boolean spelling, so the forwarded argv must
	// not carry `--approve=true`.
	if want := []string{"--model", "sonnet", "--approve"}; !reflect.DeepEqual(spec.Args, want) {
		t.Fatalf("Args = %v, want %v", spec.Args, want)
	}

	// A false boolean is dropped when it has no opposite flag; the trust
	// setting has one and is spelled positively.
	no := mustParse(t, "--offline=false", "--no-approve=false")
	if want := []string{"--approve"}; !reflect.DeepEqual(no.Args, want) {
		t.Fatalf("Args = %v, want %v", no.Args, want)
	}
	if got := no.Values["approve"]; !reflect.DeepEqual(got, []string{"true"}) {
		t.Fatalf("approve after --no-approve=false = %v, want [true]", got)
	}
}

func mustParse(t *testing.T, args ...string) *Spec {
	t.Helper()
	spec, err := Parse(args)
	if err != nil {
		t.Fatalf("Parse(%v): %v", args, err)
	}
	return spec
}

func TestRedactedSpawnValues(t *testing.T) {
	out := RedactedSpawnValues(map[string][]string{
		"api-key":              {"sk-secret"},
		"append-system-prompt": {"instruction"},
	})
	if _, ok := out["api-key"]; ok {
		t.Fatalf("api-key must be redacted: %v", out)
	}
	if got := out["append-system-prompt"]; len(got) != 1 || got[0] != "instruction" {
		t.Fatalf("non-secret key lost: %v", out)
	}
	if RedactedSpawnValues(map[string][]string{"api-key": {"x"}}) != nil {
		t.Fatal("a fully redacted view must be nil (omitted)")
	}
	if RedactedSpawnValues(nil) != nil {
		t.Fatal("nil input must stay nil")
	}
}

func TestParseRejects(t *testing.T) {
	cases := map[string][]string{
		"reserved mode":     {"--mode", "rpc"},
		"reserved session":  {"--session", "/tmp/x.jsonl"},
		"reserved version":  {"--version"},
		"unknown flag":      {"--nope"},
		"positional":        {"hello"},
		"missing value":     {"--model"},
		"bad boolean value": {"--approve=maybe"},
	}
	for name, args := range cases {
		if _, err := Parse(args); err == nil {
			t.Errorf("%s: expected error for %v", name, args)
		}
	}
}

func TestSpawnConflict(t *testing.T) {
	recorded := &Spec{Values: map[string][]string{
		"approve":   {"true"},
		"extension": {"/a.ts", "/b.ts"},
	}}
	same := &Spec{Values: map[string][]string{
		"extension": {"/b.ts", "/a.ts"}, // order is not meaningful
		"approve":   {"true"},
	}}
	if key, conflict := SpawnConflict(recorded.Values, same.SpawnValues()); conflict {
		t.Fatalf("order-only difference reported as conflict on %q", key)
	}

	different := &Spec{Values: map[string][]string{"extension": {"/c.ts"}}}
	key, conflict := SpawnConflict(recorded.Values, different.SpawnValues())
	if !conflict {
		t.Fatal("expected conflict")
	}
	if key != "extension" {
		t.Fatalf("unexpected conflict key %q", key)
	}

	// A spawn key the session never set is not a conflict: the record has no
	// preference, and the session keeps what it has (docs/protocol.md §4.3).
	extra := &Spec{Values: map[string][]string{"offline": {"true"}}}
	if key, conflict := SpawnConflict(recorded.Values, extra.SpawnValues()); conflict {
		t.Fatalf("unrecorded parameter must not conflict (got %q)", key)
	}

	// Repeating an identical flag is a no-op, not a conflict: pilish adds its
	// own --approve on top of one the UI already passed.
	doubled := &Spec{Values: map[string][]string{"approve": {"true", "true"}}}
	if key, conflict := SpawnConflict(recorded.Values, doubled.SpawnValues()); conflict {
		t.Fatalf("duplicate identical flag must not conflict (got %q)", key)
	}

	// Omitting a recorded parameter is not a conflict: the client expresses no
	// preference and the session keeps its spawn configuration.
	missing := &Spec{Values: map[string][]string{}}
	if key, conflict := SpawnConflict(recorded.Values, missing.SpawnValues()); conflict {
		t.Fatalf("omitted parameters must not conflict (got %q)", key)
	}
}

func TestRuntimeValuesLastWins(t *testing.T) {
	spec, err := Parse([]string{"--model", "a", "--model", "b", "--thinking", "low"})
	if err != nil {
		t.Fatal(err)
	}
	rt := spec.RuntimeValues()
	if rt[KeyModel] != "b" {
		t.Fatalf("last value should win: %v", rt)
	}
	if !strings.Contains(strings.Join(spec.Args, " "), "--model a --model b") {
		t.Fatalf("spawn args should keep every occurrence: %v", spec.Args)
	}
}

// TestShortAliasesMatchLongForms guards pi's short flags: a UI that passes
// `-a` must record exactly what `--approve` records, or the spawn-parameter
// comparison would treat the same session as a conflict.
func TestShortAliasesMatchLongForms(t *testing.T) {
	cases := []struct {
		alias string
		long  string
		value string
		key   string
	}{
		{"-a", "--approve", "", "approve"},
		{"-na", "--no-approve", "", "approve"}, // recorded as the approve setting, value false
		{"-ne", "--no-extensions", "", "no-extensions"},
		{"-ns", "--no-skills", "", "no-skills"},
		{"-np", "--no-prompt-templates", "", "no-prompt-templates"},
		{"-nc", "--no-context-files", "", "no-context-files"},
		{"-nbt", "--no-builtin-tools", "", "no-builtin-tools"},
		{"-nt", "--no-tools", "", "no-tools"},
		{"-e", "--extension", "/tmp/ext.ts", "extension"},
		{"-t", "--tools", "read,bash", "tools"},
		{"-xt", "--exclude-tools", "write", "exclude-tools"},
		{"-n", "--name", "work", KeyName},
		// Newly accepted long forms (no alias, but recorded canonically).
		{"", "--models", "sonnet,haiku", "models"},
		{"", "--no-themes", "", "no-themes"},
		{"", "--offline", "", "offline"},
		{"", "--verbose", "", "verbose"},
	}
	for _, tc := range cases {
		withValue := func(flag string) []string {
			if tc.value == "" {
				return []string{flag}
			}
			return []string{flag, tc.value}
		}
		long, err := Parse(withValue(tc.long))
		if err != nil {
			t.Fatalf("Parse(%v): %v", withValue(tc.long), err)
		}
		if len(long.Values[tc.key]) == 0 {
			t.Fatalf("%s did not record key %q: %v", tc.long, tc.key, long.Values)
		}
		if tc.alias == "" {
			continue
		}
		alias, err := Parse(withValue(tc.alias))
		if err != nil {
			t.Fatalf("Parse(%v): %v", withValue(tc.alias), err)
		}
		if !reflect.DeepEqual(alias.Values, long.Values) {
			t.Fatalf("%s recorded %v, want %v (same as %s)",
				tc.alias, alias.Values, long.Values, tc.long)
		}
	}
}

func TestCanonicalSpawnArgs(t *testing.T) {
	spec, err := Parse([]string{
		"-a", "-na", "--extension", "/a.ts", "-e", "/b.ts",
		"--append-system-prompt", "one", "--model", "m1", "--thinking", "low",
	})
	if err != nil {
		t.Fatal(err)
	}
	// Runtime keys are not part of the spawn configuration, aliases become
	// their long form, and --approve/--no-approve are one setting (last wins).
	got := spec.CanonicalSpawnArgs()
	want := []string{
		"--approve", "--no-approve",
		"--extension", "/a.ts", "--extension", "/b.ts",
		"--append-system-prompt", "one",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("CanonicalSpawnArgs = %v, want %v", got, want)
	}
	if _, ok := spec.Values["no-approve"]; ok {
		t.Fatalf("no-approve should be recorded as the approve setting: %v", spec.Values)
	}
	if vals := spec.Values["approve"]; !reflect.DeepEqual(vals, []string{"true", "false"}) {
		t.Fatalf("approve values = %v, want [true false]", vals)
	}
}

func TestMergeSpawn(t *testing.T) {
	// A session created with a system prompt and an extension record; the
	// requester asks for a different prompt (recorded wins), a new extension
	// (filled in) and a runtime model (kept for spawn/apply).
	recorded := []string{"--append-system-prompt", "one", "--extension", "/a.ts"}
	requested, err := Parse([]string{
		"--append-system-prompt", "two",
		"--skill", "/s.ts",
		"--no-approve",
		"--model", "m2",
	})
	if err != nil {
		t.Fatal(err)
	}
	got := MergeSpawn(recorded, requested)
	joined := strings.Join(got, " ")
	if strings.Contains(joined, "two") {
		t.Fatalf("recorded value was overridden: %v", got)
	}
	for _, want := range []string{"--append-system-prompt one", "--extension /a.ts", "--skill /s.ts", "--no-approve", "--model m2"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("merged args %q missing %q", joined, want)
		}
	}
}

func TestMergeSpawnFallsBackOnCorruptRecord(t *testing.T) {
	requested, err := Parse([]string{"--approve"})
	if err != nil {
		t.Fatal(err)
	}
	got := MergeSpawn([]string{"--not-a-flag"}, requested)
	if !reflect.DeepEqual(got, requested.Args) {
		t.Fatalf("corrupt record merge = %v, want %v", got, requested.Args)
	}
}
