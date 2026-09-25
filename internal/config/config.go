// Package config resolves the gateway's on-disk state: the daemon-generated
// token and the port file used for discovery. See docs/protocol.md §1.
package config

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	// DefaultHost and DefaultPort are the fallback listener address when the
	// port file is missing.
	DefaultHost = "127.0.0.1"
	DefaultPort = 7331

	// DefaultAddr is the fixed fallback address (must match DefaultHost and
	// DefaultPort; kept as a literal for const-ness).
	DefaultAddr = "127.0.0.1:7331"

	// DirEnv overrides the config directory (used by tests and unusual setups).
	DirEnv = "PI_GATEWAY_CONFIG_DIR"
)

// Dir returns the gateway config directory. It does not create it.
func Dir() string {
	if d := os.Getenv(DirEnv); d != "" {
		return d
	}
	if d := os.Getenv("XDG_CONFIG_HOME"); d != "" {
		return filepath.Join(d, "pi-gateway")
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return filepath.Join(".", ".pi-gateway")
	}
	return filepath.Join(home, ".config", "pi-gateway")
}

// TokenPath returns the default token path inside dir.
func TokenPath(dir string) string { return filepath.Join(dir, "token") }

// PortPath returns the default port file path inside dir.
func PortPath(dir string) string { return filepath.Join(dir, "port") }

// LoadOrCreateToken reads the token at path, generating a stable random token
// on first use. The file is written mode 0600.
func LoadOrCreateToken(path string) (string, error) {
	tok, err := ReadToken(path)
	if err == nil {
		return tok, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", fmt.Errorf("config: create %s: %w", filepath.Dir(path), err)
	}
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("config: generate token: %w", err)
	}
	tok = hex.EncodeToString(buf)
	if err := os.WriteFile(path, []byte(tok+"\n"), 0o600); err != nil {
		return "", fmt.Errorf("config: write token: %w", err)
	}
	return tok, nil
}

// ReadToken reads a token file, trimming surrounding whitespace.
func ReadToken(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	tok := strings.TrimSpace(string(b))
	if tok == "" {
		return "", fmt.Errorf("config: token file %s is empty", path)
	}
	return tok, nil
}

// WritePort atomically records the bound port.
func WritePort(path string, port int) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("config: create %s: %w", filepath.Dir(path), err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.Itoa(port)+"\n"), 0o600); err != nil {
		return fmt.Errorf("config: write port: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("config: rename port file: %w", err)
	}
	return nil
}

// ReadPort reads a port file.
func ReadPort(path string) (int, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	p, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || p <= 0 || p > 65535 {
		return 0, fmt.Errorf("config: invalid port file %s", path)
	}
	return p, nil
}

// PortOf extracts the port from "host:port" or ":port".
func PortOf(addr string) (int, error) {
	i := strings.LastIndex(addr, ":")
	if i < 0 {
		return 0, fmt.Errorf("config: address %q has no port", addr)
	}
	p, err := strconv.Atoi(addr[i+1:])
	if err != nil || p < 0 || p > 65535 {
		return 0, fmt.Errorf("config: invalid port in %q", addr)
	}
	return p, nil
}
