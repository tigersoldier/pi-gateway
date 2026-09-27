package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/tigersoldier/pi-gateway/protocol"
)

func writeTokens(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tokens.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadTokensMissingFile(t *testing.T) {
	grants, err := LoadTokens(filepath.Join(t.TempDir(), "nope.json"))
	if err != nil || grants != nil {
		t.Fatalf("grants=%v err=%v", grants, err)
	}
	grants, err = LoadTokens(writeTokens(t, "\n"))
	if err != nil || grants != nil {
		t.Fatalf("empty file: grants=%v err=%v", grants, err)
	}
}

func TestLoadTokensResolvesRolesAndCapabilities(t *testing.T) {
	path := writeTokens(t, `{
	  "tokens": [
	    {"name": "slack", "token": "tok-a", "role": "operator", "comment": "bot"},
	    {"name": "dashboard", "token": "tok-b", "capabilities": ["ui", "observe"]}
	  ]
	}`)
	grants, err := LoadTokens(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(grants) != 2 {
		t.Fatalf("grants = %v", grants)
	}
	if grants[0].Name != "slack" || !reflect.DeepEqual(grants[0].Capabilities,
		[]string{protocol.CapObserve, protocol.CapInterject, protocol.CapPrompt, protocol.CapContext, protocol.CapUI}) {
		t.Fatalf("slack grant = %+v", grants[0])
	}
	// Explicit capability lists are normalized into canonical order.
	if !reflect.DeepEqual(grants[1].Capabilities, []string{protocol.CapObserve, protocol.CapUI}) {
		t.Fatalf("dashboard grant = %+v", grants[1])
	}
}

func TestLoadTokensErrors(t *testing.T) {
	cases := map[string]struct {
		body string
		want string
	}{
		"unknown role":       {`{"tokens":[{"name":"a","token":"t","role":"root"}]}`, "unknown role"},
		"unknown capability": {`{"tokens":[{"name":"a","token":"t","capabilities":["sudo"]}]}`, "unknown capabilities"},
		"both":               {`{"tokens":[{"name":"a","token":"t","role":"admin","capabilities":["observe"]}]}`, "both role and capabilities"},
		"neither":            {`{"tokens":[{"name":"a","token":"t"}]}`, "neither role nor capabilities"},
		"no name":            {`{"tokens":[{"token":"t","role":"admin"}]}`, "has no name"},
		"no value":           {`{"tokens":[{"name":"a","role":"admin"}]}`, "no token value"},
		"duplicate value":    {`{"tokens":[{"name":"a","token":"t","role":"observer"},{"name":"b","token":"t","role":"observer"}]}`, "share the same value"},
		"unknown field":      {`{"tokens":[{"name":"a","token":"t","role":"observer","extra":1}]}`, "unknown field"},
		"malformed json":     {`{"tokens":`, "parse"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := LoadTokens(writeTokens(t, tc.body))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want mention of %q", err, tc.want)
			}
		})
	}
}

func TestAddTokenCreatesAndAppends(t *testing.T) {
	dir := t.TempDir()
	path := TokensPath(dir)

	grant, err := AddToken(path, "slack", protocol.RoleObserver, nil)
	if err != nil {
		t.Fatal(err)
	}
	if grant.Name != "slack" || len(grant.Token) != 64 {
		t.Fatalf("grant = %+v", grant)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("tokens file mode = %o, want 600", perm)
	}
	grants, err := LoadTokens(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(grants) != 1 || grants[0].Token != grant.Token {
		t.Fatalf("reloaded = %+v", grants)
	}

	second, err := AddToken(path, "dashboard", "", []string{protocol.CapObserve})
	if err != nil {
		t.Fatal(err)
	}
	grants, err = LoadTokens(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(grants) != 2 || grants[1].Token != second.Token {
		t.Fatalf("after append = %+v", grants)
	}
	if _, err := AddToken(path, "slack", protocol.RoleObserver, nil); err == nil ||
		!strings.Contains(err.Error(), "already exists") {
		t.Fatalf("duplicate name err = %v", err)
	}
	if _, err := AddToken(path, "broken", "", nil); err == nil {
		t.Fatal("expected an error when neither role nor capabilities are given")
	}
	if _, err := AddToken(path, "", protocol.RoleObserver, nil); err == nil {
		t.Fatal("expected an error for a missing name")
	}
}

func TestTokenSpecDescribe(t *testing.T) {
	if got := (TokenSpec{Role: "observer"}).Describe(); got != "role observer" {
		t.Fatalf("Describe = %q", got)
	}
	if got := (TokenSpec{Capabilities: []string{"observe", "ui"}}).Describe(); got != "capabilities observe,ui" {
		t.Fatalf("Describe = %q", got)
	}
}
