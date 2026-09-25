package piargs

import (
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

	different := &Spec{Values: map[string][]string{"no-approve": {"true"}}}
	key, conflict := SpawnConflict(recorded.Values, different.SpawnValues())
	if !conflict {
		t.Fatal("expected conflict")
	}
	if key != "no-approve" && key != "approve" {
		t.Fatalf("unexpected conflict key %q", key)
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
