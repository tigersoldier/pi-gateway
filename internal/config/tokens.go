package config

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/tigersoldier/pi-gateway/internal/protocol"
)

// TokensPath returns the optional multi-token file inside dir. The
// daemon-generated token file (see TokenPath) always grants the full
// capability set; entries here grant a restricted role.
func TokensPath(dir string) string { return filepath.Join(dir, "tokens.json") }

// TokenFile is the on-disk shape of tokens.json.
type TokenFile struct {
	Tokens []TokenSpec `json:"tokens"`
}

// TokenSpec is one provisioned token: a name for operators, the token itself,
// and either a preset role or an explicit capability list.
type TokenSpec struct {
	Name         string   `json:"name"`
	Value        string   `json:"token"`
	Role         string   `json:"role,omitempty"`
	Capabilities []string `json:"capabilities,omitempty"`
	Comment      string   `json:"comment,omitempty"`
}

// Resolve validates the spec and returns the grant the daemon authenticates
// with.
func (t TokenSpec) Resolve() (protocol.TokenGrant, error) {
	if t.Name == "" {
		return protocol.TokenGrant{}, errors.New("token entry has no name")
	}
	if t.Value == "" {
		return protocol.TokenGrant{}, fmt.Errorf("token %q has no token value", t.Name)
	}
	switch {
	case t.Role != "" && len(t.Capabilities) > 0:
		return protocol.TokenGrant{}, fmt.Errorf("token %q sets both role and capabilities", t.Name)
	case t.Role != "":
		caps, ok := protocol.RoleCapabilities(t.Role)
		if !ok {
			return protocol.TokenGrant{}, fmt.Errorf("token %q uses unknown role %q (want %s)",
				t.Name, t.Role, strings.Join(protocol.RoleNames(), ", "))
		}
		return protocol.TokenGrant{Name: t.Name, Token: t.Value, Capabilities: caps}, nil
	case len(t.Capabilities) > 0:
		caps, unknown := protocol.NormalizeCapabilities(t.Capabilities)
		if len(unknown) > 0 {
			return protocol.TokenGrant{}, fmt.Errorf("token %q requests unknown capabilities %s",
				t.Name, strings.Join(unknown, ", "))
		}
		return protocol.TokenGrant{Name: t.Name, Token: t.Value, Capabilities: caps}, nil
	}
	return protocol.TokenGrant{}, fmt.Errorf("token %q sets neither role nor capabilities", t.Name)
}

// Describe renders the granted authority for operators.
func (t TokenSpec) Describe() string {
	if t.Role != "" {
		return "role " + t.Role
	}
	return "capabilities " + strings.Join(t.Capabilities, ",")
}

// LoadTokens reads the optional tokens file and resolves every entry. A
// missing file yields no grants.
func LoadTokens(path string) ([]protocol.TokenGrant, error) {
	specs, err := readTokenSpecs(path)
	if err != nil {
		return nil, err
	}
	if len(specs.Tokens) == 0 {
		return nil, nil
	}
	out := make([]protocol.TokenGrant, 0, len(specs.Tokens))
	seen := make(map[string]string, len(specs.Tokens))
	for _, spec := range specs.Tokens {
		grant, err := spec.Resolve()
		if err != nil {
			return nil, fmt.Errorf("config: %s: %w", path, err)
		}
		if other, dup := seen[grant.Token]; dup {
			return nil, fmt.Errorf("config: %s: tokens %q and %q share the same value", path, other, grant.Name)
		}
		seen[grant.Token] = grant.Name
		out = append(out, grant)
	}
	return out, nil
}

// AddToken generates a token, appends it to the tokens file (creating it mode
// 0600 when needed), and returns the grant. role and caps are mutually
// exclusive; caps may be empty when role is set.
func AddToken(path, name, role string, caps []string) (protocol.TokenGrant, error) {
	if name == "" {
		return protocol.TokenGrant{}, errors.New("config: a token name is required")
	}
	specs, err := readTokenSpecs(path)
	if err != nil {
		return protocol.TokenGrant{}, err
	}
	for _, spec := range specs.Tokens {
		if spec.Name == name {
			return protocol.TokenGrant{}, fmt.Errorf("config: token %q already exists in %s", name, path)
		}
	}
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return protocol.TokenGrant{}, fmt.Errorf("config: generate token: %w", err)
	}
	spec := TokenSpec{Name: name, Value: hex.EncodeToString(buf), Role: role, Capabilities: caps}
	grant, err := spec.Resolve()
	if err != nil {
		return protocol.TokenGrant{}, fmt.Errorf("config: %w", err)
	}
	specs.Tokens = append(specs.Tokens, spec)
	body, err := json.MarshalIndent(specs, "", "  ")
	if err != nil {
		return protocol.TokenGrant{}, fmt.Errorf("config: encode %s: %w", path, err)
	}
	body = append(body, '\n')
	if err := writeFileMode(path, body, 0o600); err != nil {
		return protocol.TokenGrant{}, err
	}
	return grant, nil
}

// readTokenSpecs reads and decodes the tokens file. A missing file is not an
// error: it means no restricted tokens are provisioned.
func readTokenSpecs(path string) (TokenFile, error) {
	var tf TokenFile
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return tf, nil
	}
	if err != nil {
		return tf, fmt.Errorf("config: read %s: %w", path, err)
	}
	if len(bytes.TrimSpace(b)) == 0 {
		return tf, nil
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&tf); err != nil {
		return tf, fmt.Errorf("config: parse %s: %w", path, err)
	}
	return tf, nil
}

// writeFileMode writes a file atomically with the given permissions.
func writeFileMode(path string, body []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("config: create %s: %w", dir, err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, body, mode); err != nil {
		return fmt.Errorf("config: write %s: %w", tmp, err)
	}
	if err := os.Chmod(tmp, mode); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("config: chmod %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("config: rename %s: %w", path, err)
	}
	return nil
}
