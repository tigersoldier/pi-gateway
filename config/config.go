// Package config resolves the gateway's on-disk state: the daemon-generated
// token and the port file used for discovery. See docs/protocol.md §1.
package config

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
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

// DebugPortPath returns the port file for the read-only debug listener.
func DebugPortPath(dir string) string { return filepath.Join(dir, "debug-port") }

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
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("config: generate token: %w", err)
	}
	tok = hex.EncodeToString(buf)
	if err := writeFileMode(path, []byte(tok+"\n"), 0o600); err != nil {
		return "", err
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
	return writeFileMode(path, []byte(strconv.Itoa(port)+"\n"), 0o600)
}

// RemovePortFile deletes a port file, ignoring a missing file.
func RemovePortFile(path string) {
	_ = os.Remove(path)
}

// RemovePortIfMatches deletes a port file only while it still holds port, so a
// shutting-down instance never removes a newer instance's file.
func RemovePortIfMatches(path string, port int) {
	if p, err := ReadPort(path); err == nil && p == port {
		_ = os.Remove(path)
	}
}

// RequireLoopback rejects an address that is not loopback. Both listeners are
// documented as local-only (docs/design.md §10), so this is enforced rather
// than trusted to the operator.
func RequireLoopback(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("config: invalid address %q: %w", addr, err)
	}
	if host == "" {
		return fmt.Errorf("config: %q listens on every interface; use the loopback address %s", addr, DefaultAddr)
	}
	if ip := net.ParseIP(host); ip != nil {
		if ip.IsLoopback() {
			return nil
		}
		return fmt.Errorf("config: %q is not a loopback address", addr)
	}
	ips, err := net.LookupHost(host)
	if err != nil {
		return fmt.Errorf("config: cannot resolve %q: %w", host, err)
	}
	for _, resolved := range ips {
		if ip := net.ParseIP(resolved); ip == nil || !ip.IsLoopback() {
			return fmt.Errorf("config: %q is not a loopback address", addr)
		}
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

// Addresses returns the daemon addresses a client should try, in order:
//
//  1. an explicit server ("host:port"), when given;
//  2. an explicit port on the loopback host;
//  3. the port recorded in dir's port file;
//  4. the fixed default address, as a fallback in case the file is stale
//     (for example after an unclean daemon exit).
//
// The bridge and the client library share this so both discover the daemon
// the same way (docs/protocol.md §1).
func Addresses(server string, port int, dir string) []string {
	if server != "" {
		return []string{server}
	}
	if port > 0 {
		return []string{net.JoinHostPort(DefaultHost, strconv.Itoa(port))}
	}
	var addrs []string
	if p, err := ReadPort(PortPath(dir)); err == nil {
		addrs = append(addrs, net.JoinHostPort(DefaultHost, strconv.Itoa(p)))
	}
	if len(addrs) == 0 || addrs[0] != DefaultAddr {
		addrs = append(addrs, DefaultAddr)
	}
	return addrs
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
