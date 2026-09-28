package daemon

import "testing"

// TestCanonicalPathOfEmptyIsEmpty is GH-1 at the helper boundary: an unknown
// session path has no canonical form. filepath.Abs("") used to resolve to the
// process working directory, so callers' `canon != ""` guards could not catch
// an empty path, and setCreated persisted a sidecar keyed by the daemon's cwd.
func TestCanonicalPathOfEmptyIsEmpty(t *testing.T) {
	if got := canonicalPath(""); got != "" {
		t.Fatalf("canonicalPath(%q) = %q, want empty", "", got)
	}
}
