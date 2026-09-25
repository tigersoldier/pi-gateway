package catalog

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tigersoldier/pi-gateway/internal/testutil"
)

func writeSession(t *testing.T, path string, lines ...any) {
	t.Helper()
	testutil.WriteSession(t, path, lines...)
}

func sessionHeader(id, cwd, ts string) map[string]any {
	return map[string]any{"type": "session", "version": 3, "id": id, "cwd": cwd, "timestamp": ts}
}

func TestParseSessionFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "--home-u-proj--", "a.jsonl")
	writeSession(t, path,
		sessionHeader("sess-1", "/home/u/proj", "2026-01-01T00:00:00.000Z"),
		map[string]any{"type": "session_info", "id": "n1", "name": "old-name"},
		map[string]any{"type": "message", "id": "e1", "timestamp": "2026-01-01T00:00:01.000Z",
			"message": map[string]any{"role": "user", "content": "first question"}},
		map[string]any{"type": "message", "id": "e2", "timestamp": "2026-01-01T00:10:00.000Z",
			"message": map[string]any{"role": "assistant", "content": []any{
				map[string]any{"type": "thinking", "thinking": "hmm"},
				map[string]any{"type": "text", "text": "answer"},
			}}},
		map[string]any{"type": "session_info", "id": "n2", "name": "auth-refactor"},
	)

	info, err := Parse(path)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if info.ID != "sess-1" || info.Cwd != "/home/u/proj" {
		t.Fatalf("header fields = %+v", info)
	}
	if info.Name != "auth-refactor" {
		t.Fatalf("Name = %q, want the latest session_info", info.Name)
	}
	if info.Title != "first question" {
		t.Fatalf("Title = %q, want the first user message", info.Title)
	}
	if info.MessageCount != 2 {
		t.Fatalf("MessageCount = %d, want 2", info.MessageCount)
	}
	want := time.Date(2026, 1, 1, 0, 10, 0, 0, time.UTC)
	if !info.LastActivity.Equal(want) {
		t.Fatalf("LastActivity = %v, want %v", info.LastActivity, want)
	}
	if leaf, err := LeafID(path); err != nil || leaf != "n2" {
		t.Fatalf("LeafID = %q, %v; want n2", leaf, err)
	}
}

func TestParseRejectsNonSessionFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "notes.jsonl")
	writeSession(t, path, map[string]any{"type": "message", "id": "x"})
	if _, err := Parse(path); err == nil {
		t.Fatal("Parse accepted a file without a session header")
	}
}

func TestListFiltersByCwdAndLimit(t *testing.T) {
	root := t.TempDir()
	now := time.Now()
	older := filepath.Join(root, "old", "old.jsonl")
	newer := filepath.Join(root, "new", "new.jsonl")
	other := filepath.Join(root, "other", "other.jsonl")
	writeSession(t, older, sessionHeader("s1", "/home/u/proj", "2026-01-01T00:00:00.000Z"))
	writeSession(t, newer, sessionHeader("s2", "/home/u/proj", "2026-01-02T00:00:00.000Z"))
	writeSession(t, other, sessionHeader("s3", "/home/u/elsewhere", "2026-01-03T00:00:00.000Z"))
	_ = os.Chtimes(older, now.Add(-2*time.Hour), now.Add(-2*time.Hour))
	_ = os.Chtimes(newer, now.Add(-1*time.Hour), now.Add(-1*time.Hour))
	_ = os.Chtimes(other, now, now)

	sc := &Scanner{Roots: []string{root}}
	all := sc.List("", 0)
	if len(all) != 3 {
		t.Fatalf("List() returned %d sessions, want 3", len(all))
	}
	if all[0].ID != "s3" {
		t.Fatalf("List() newest first = %s, want s3", all[0].ID)
	}
	proj := sc.List("/home/u/proj", 0)
	if len(proj) != 2 || proj[0].ID != "s2" {
		t.Fatalf("List(cwd) = %+v, want s2 then s1", proj)
	}
	if limited := sc.List("/home/u/proj", 1); len(limited) != 1 || limited[0].ID != "s2" {
		t.Fatalf("List(cwd, 1) = %+v, want only s2", limited)
	}
}

func TestFindNameAndResolve(t *testing.T) {
	root := t.TempDir()
	a := filepath.Join(root, "a", "a.jsonl")
	b := filepath.Join(root, "b", "b.jsonl")
	writeSession(t, a, sessionHeader("s1", "/x", "2026-01-01T00:00:00.000Z"),
		map[string]any{"type": "session_info", "id": "n1", "name": "auth"})
	writeSession(t, b, sessionHeader("s2", "/x", "2026-01-01T00:00:00.000Z"),
		map[string]any{"type": "session_info", "id": "n1", "name": "auth"})

	sc := &Scanner{Roots: []string{root}}
	files, err := sc.FindName("auth")
	if err != nil {
		t.Fatalf("FindName: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("FindName(auth) = %d matches, want 2", len(files))
	}
	if _, found, err := Resolve("auth", files, nil); found || err == nil {
		t.Fatalf("Resolve(ambiguous) = %v, %v; want an ambiguity error", found, err)
	}
	if _, found, err := Resolve("missing", files, nil); found || err != nil {
		t.Fatalf("Resolve(missing) = %v, %v; want no match", found, err)
	}

	live := []Info{{Path: b, Name: "live-only"}}
	got, found, err := Resolve("live-only", files, live)
	if err != nil || !found || got.Path != b {
		t.Fatalf("Resolve(live) = %+v, %v, %v", got, found, err)
	}
}

func TestLeafIDSkipsPartialLineAndHeaderOnly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.jsonl")
	writeSession(t, path, sessionHeader("s1", "/x", "2026-01-01T00:00:00.000Z"))
	if leaf, err := LeafID(path); err != nil || leaf != "" {
		t.Fatalf("LeafID(header only) = %q, %v; want empty", leaf, err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"type":"message","id":"e9"}` + "\n" + `{"type":"message","id":"e1`); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	leaf, err := LeafID(path)
	if err != nil || leaf != "e9" {
		t.Fatalf("LeafID with partial line = %q, %v; want e9", leaf, err)
	}
}

func TestDefaultRootsPrefersEnvironment(t *testing.T) {
	// pi's precedence: session dir wins, then agent dir, then the home default.
	t.Setenv("PI_CODING_AGENT_SESSION_DIR", "/tmp/sessions-env")
	t.Setenv("PI_CODING_AGENT_DIR", "/tmp/agent-env")
	roots := DefaultRoots("/extra")
	if len(roots) != 2 || roots[0] != "/tmp/sessions-env" || roots[1] != "/extra" {
		t.Fatalf("DefaultRoots = %v", roots)
	}

	t.Setenv("PI_CODING_AGENT_SESSION_DIR", "")
	roots = DefaultRoots("/extra")
	if len(roots) != 2 || roots[0] != "/tmp/agent-env/sessions" || roots[1] != "/extra" {
		t.Fatalf("DefaultRoots with agent dir = %v", roots)
	}

	t.Setenv("PI_CODING_AGENT_DIR", "")
	roots = DefaultRoots()
	if len(roots) != 1 || !strings.HasSuffix(roots[0], filepath.Join(".pi", "agent", "sessions")) {
		t.Fatalf("DefaultRoots home default = %v", roots)
	}

	// Duplicates are removed.
	if again := DefaultRoots(roots[0]); len(again) != 1 {
		t.Fatalf("DefaultRoots dedupe = %v", again)
	}
}
