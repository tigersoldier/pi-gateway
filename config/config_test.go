package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
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

func TestAddresses(t *testing.T) {
	dir := t.TempDir()
	if got := Addresses("10.0.0.5:9999", 7331, dir); len(got) != 1 || got[0] != "10.0.0.5:9999" {
		t.Fatalf("explicit server = %v", got)
	}
	if got := Addresses("", 7442, dir); len(got) != 1 || got[0] != "127.0.0.1:7442" {
		t.Fatalf("explicit port = %v", got)
	}
	// No port file: only the fixed fallback, so a stale directory still works.
	if got := Addresses("", 0, dir); len(got) != 1 || got[0] != DefaultAddr {
		t.Fatalf("no port file = %v", got)
	}
	// A recorded port comes first, the fallback second.
	if err := WritePort(PortPath(dir), 7442); err != nil {
		t.Fatal(err)
	}
	if got := Addresses("", 0, dir); len(got) != 2 || got[0] != "127.0.0.1:7442" || got[1] != DefaultAddr {
		t.Fatalf("port file = %v", got)
	}
	// A port file equal to the default must not duplicate the fallback.
	if err := WritePort(PortPath(dir), DefaultPort); err != nil {
		t.Fatal(err)
	}
	if got := Addresses("", 0, dir); len(got) != 1 || got[0] != DefaultAddr {
		t.Fatalf("port file equal to the default = %v", got)
	}
}

func TestRequireLoopback(t *testing.T) {
	allowed := []string{"127.0.0.1:7331", "127.0.0.1:0", "[::1]:7331", "localhost:7331"}
	for _, addr := range allowed {
		if err := RequireLoopback(addr); err != nil {
			t.Errorf("RequireLoopback(%q) = %v, want nil", addr, err)
		}
	}
	rejected := []string{"0.0.0.0:7331", ":7331", "10.1.2.3:7331", "example.com:80", "not-an-address"}
	for _, addr := range rejected {
		err := RequireLoopback(addr)
		if err == nil {
			t.Errorf("RequireLoopback(%q) was accepted", addr)
			continue
		}
		if !strings.Contains(err.Error(), "loopback") && !strings.Contains(err.Error(), "address") {
			t.Errorf("RequireLoopback(%q) error = %v", addr, err)
		}
	}
}

func TestPortFileHelpers(t *testing.T) {
	dir := t.TempDir()
	path := PortPath(dir)
	if err := WritePort(path, 7331); err != nil {
		t.Fatal(err)
	}
	// A different port must not be deleted (a newer instance owns the file).
	RemovePortIfMatches(path, 99)
	if got, err := ReadPort(path); err != nil || got != 7331 {
		t.Fatalf("RemovePortIfMatches removed a foreign port file: %d, %v", got, err)
	}
	RemovePortIfMatches(path, 7331)
	if _, err := ReadPort(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("port file survived RemovePortIfMatches: %v", err)
	}
	// RemovePortFile ignores a missing file and removes a stale one.
	RemovePortFile(path)
	if err := WritePort(path, 7331); err != nil {
		t.Fatal(err)
	}
	RemovePortFile(path)
	if _, err := ReadPort(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("port file survived RemovePortFile: %v", err)
	}
}
