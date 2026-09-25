package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDirHonorsEnv(t *testing.T) {
	t.Setenv(DirEnv, "/tmp/custom-pi-gateway")
	if got := Dir(); got != "/tmp/custom-pi-gateway" {
		t.Fatalf("Dir() = %q", got)
	}
	t.Setenv(DirEnv, "")
	t.Setenv("XDG_CONFIG_HOME", "/tmp/xdg")
	if got := Dir(); got != filepath.Join("/tmp/xdg", "pi-gateway") {
		t.Fatalf("Dir() = %q", got)
	}
}

func TestLoadOrCreateTokenIsStableAndPrivate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dir", "token")
	first, err := LoadOrCreateToken(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 64 {
		t.Fatalf("unexpected token length %d", len(first))
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if mode := info.Mode().Perm(); mode != 0o600 {
		t.Fatalf("token mode = %o, want 600", mode)
	}
	second, err := LoadOrCreateToken(path)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("token changed across loads: %q vs %q", first, second)
	}
}

func TestReadTokenRejectsEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("  \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadToken(path); err == nil {
		t.Fatal("empty token file must fail")
	}
}

func TestPortRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "port")
	if err := WritePort(path, 7442); err != nil {
		t.Fatal(err)
	}
	port, err := ReadPort(path)
	if err != nil {
		t.Fatal(err)
	}
	if port != 7442 {
		t.Fatalf("port = %d", port)
	}
	if err := os.WriteFile(path, []byte("nope"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadPort(path); err == nil {
		t.Fatal("invalid port file must fail")
	}
}

func TestPortOf(t *testing.T) {
	if p, err := PortOf("127.0.0.1:7331"); err != nil || p != 7331 {
		t.Fatalf("PortOf = %d, %v", p, err)
	}
	if _, err := PortOf("127.0.0.1"); err == nil {
		t.Fatal("address without port must fail")
	}
}
