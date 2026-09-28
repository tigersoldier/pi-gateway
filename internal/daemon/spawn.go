package daemon

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/tigersoldier/pi-gateway/piargs"
	"github.com/tigersoldier/pi-gateway/protocol"
)

// spawnConfig is the durable spawn configuration of one session, persisted by
// the daemon next to its own state (never inside pi's session file, whose
// format belongs to pi). It is what makes gw_new_session{piArgs} keep its
// promise across hibernation, a daemon restart, and a respawn triggered by
// another client (docs/protocol.md §4.3).
type spawnConfig struct {
	// Path is the canonical session file path the record is keyed by. It is
	// stored for debuggability; the filename is a hash of it.
	Path string `json:"path"`
	// PiArgs is the canonical spawn-only argument list, in pi's argv spelling,
	// so a run can be reproduced exactly and `-a` can never sit next to
	// `--approve`.
	PiArgs []string `json:"piArgs,omitempty"`
	// Spawn is the same configuration in canonical key→values form, which is
	// what a client inspects through gw_list_sessions.
	Spawn map[string][]string `json:"spawn,omitempty"`
	// Cwd is the directory pi was spawned in; used when the session file has
	// no header cwd to recover it from.
	Cwd string `json:"cwd,omitempty"`
	// CreatedBy is the client that brought the session into existence.
	CreatedBy *protocol.ClientRef `json:"createdBy,omitempty"`
	// CreatedAt is when the record was first written.
	CreatedAt time.Time `json:"createdAt"`
	// SessionID is pi's session id, filled in once known.
	SessionID string `json:"sessionId,omitempty"`
}

// spawnStore persists spawnConfig records as one small JSON file per canonical
// session path under <stateDir>/spawn.
type spawnStore struct {
	dir string
}

func newSpawnStore(dir string) *spawnStore {
	return &spawnStore{dir: dir}
}

// recordPath is the sidecar path for a canonical session path. The hash keeps
// the filename bounded and avoids encoding a path into a name; the path itself
// is stored inside the record.
func (s *spawnStore) recordPath(canon string) string {
	sum := sha256.Sum256([]byte(canon))
	return filepath.Join(s.dir, hex.EncodeToString(sum[:])+".json")
}

// Load reads the record for a canonical session path. A missing record is
// reported as fs.ErrNotExist; any other error means a record exists but could
// not be read, which the caller should surface (a corrupt record otherwise
// silently reintroduces the "lost instruction" defect this store exists to
// fix).
func (s *spawnStore) Load(canon string) (spawnConfig, error) {
	if s == nil || s.dir == "" || canon == "" {
		return spawnConfig{}, fs.ErrNotExist
	}
	b, err := os.ReadFile(s.recordPath(canon))
	if err != nil {
		return spawnConfig{}, err
	}
	var cfg spawnConfig
	if err := json.Unmarshal(b, &cfg); err != nil {
		return spawnConfig{}, fmt.Errorf("spawn: parse %s: %w", canon, err)
	}
	return cfg, nil
}

// Save writes a record atomically (temp file + rename) so a crash mid-write
// cannot leave a half-parsed configuration.
func (s *spawnStore) Save(cfg spawnConfig) error {
	if s == nil || s.dir == "" || cfg.Path == "" {
		return nil
	}
	if cfg.CreatedAt.IsZero() {
		cfg.CreatedAt = time.Now().UTC()
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(s.dir, ".spawn-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer func() { _ = os.Remove(name) }()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(append(b, '\n')); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(name, s.recordPath(cfg.Path))
}

// Delete removes a session's record. A missing file is not an error; any other
// failure is returned so the caller can log it (a surviving record would be
// inherited by a later session created at the same canonical path).
func (s *spawnStore) Delete(canon string) error {
	if s == nil || s.dir == "" || canon == "" {
		return nil
	}
	err := os.Remove(s.recordPath(canon))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

// mergeSpawnSpec builds the effective spec for spawning a session that has a
// recorded configuration. Recorded spawn-only values win, and the requester
// fills the keys the record never set (docs/protocol.md §4.3).
func mergeSpawnSpec(rec spawnConfig, requested *piargs.Spec) *piargs.Spec {
	merged := piargs.MergeSpawn(rec.PiArgs, requested)
	spec, err := piargs.Parse(merged)
	if err != nil {
		// The record is valid enough to have been written by us; a parse
		// failure means it was edited by hand. Fall back to the requester.
		return requested
	}
	return spec
}
