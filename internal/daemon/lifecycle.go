package daemon

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/tigersoldier/pi-gateway/internal/catalog"
	"github.com/tigersoldier/pi-gateway/internal/metrics"
	"github.com/tigersoldier/pi-gateway/internal/session"
	"github.com/tigersoldier/pi-gateway/protocol"
)

// stopRequestTimeout bounds one actor round-trip for gw_stop_session/
// gw_delete_session. A forced stop waits for the aborted turn to settle
// (session.Params.StopGrace), so the daemon allowance is comfortably larger.
const stopRequestTimeout = 30 * time.Second

// errSessionDeleted marks a bind that lost the race with gw_delete_session:
// the actor it was about to join is gone and its path is tombstoned.
var errSessionDeleted = errors.New("daemon: session was deleted")

// bindErrorCode maps a bind failure to its protocol code. A bind that raced a
// delete is unknown_session (the only outcome besides "bound first"), never a
// generic crash: the client must not retry against a session that is gone.
func bindErrorCode(err error) string {
	if errors.Is(err, errSessionDeleted) {
		return protocol.CodeUnknownSession
	}
	return protocol.CodeSessionCrashed
}

// handleStop implements gw_stop_session (deleteFile=false) and
// gw_delete_session (deleteFile=true). It resolves the target, stops pi
// through the actor (which linearizes the command with prompts, turns, and
// attaches), waits for the actor to finish, and removes the file for a
// delete. The response goes straight to the requester's connection, like
// handleReload, because the target may be another session.
func (c *conn) handleStop(raw []byte, deleteFile bool) {
	command := "gw_stop_session"
	if deleteFile {
		command = "gw_delete_session"
	}
	id := protocol.Field(raw, "id")
	var req struct {
		Session string `json:"session"`
		Force   bool   `json:"force"`
	}
	if err := json.Unmarshal(raw, &req); err != nil {
		c.sendResponse(id, command, false, protocol.CodeBadFrame, err.Error(), nil)
		return
	}

	// Resolve the target: the requester's own binding when empty, otherwise a
	// path or a name. A hibernated session resolves by name from the file
	// scan, exactly like switch_session.
	var (
		actor *session.Actor
		canon string
		info  catalog.Info
		found bool
	)
	if req.Session == "" {
		actor = c.bound()
		if actor == nil {
			c.sendResponse(id, command, false, protocol.CodeUnknownSession,
				"not attached to a session", nil)
			return
		}
		info.Path, info.Name, info.ID = actor.Path(), actor.SessionName(), actor.SessionID()
		if info.Path != "" {
			canon = canonicalPath(info.Path)
		}
		found = true
	} else {
		resolved, err := c.d.resolveTarget(req.Session)
		if err != nil {
			c.sendResponse(id, command, false, errorCode(err), err.Error(), nil)
			return
		}
		canon = resolved
		actor, info, found = c.d.lookupSession(canon)
		if !found {
			c.sendResponse(id, command, false, protocol.CodeUnknownSession,
				"no session at "+canon, nil)
			return
		}
	}

	// Capture whether this command targets the requester's own binding before
	// the stop runs: the pump may process the terminal gw_session_state and
	// unbind this connection while we wait for pi, which would make the check
	// after the fact report false for a self stop.
	unbound := actor != nil && actor == c.bound()

	// A delete tombstones the path before anything is stopped, so neither a
	// concurrent attach nor the window before the file is removed can
	// resurrect the session (docs/protocol.md §3.10).
	if deleteFile && actor == nil && !c.d.isSessionFile(canon) {
		// An admin-named path that is not a pi session the daemon could have
		// scanned must not become an arbitrary-file delete.
		c.sendResponse(id, command, false, protocol.CodeUnknownSession,
			"not a session file under a session root: "+canon, nil)
		return
	}
	if deleteFile {
		c.d.markTombstoned(canon)
	}

	res := session.StopResult{Path: info.Path, Name: info.Name, SessionID: info.ID}
	if actor != nil {
		var err error
		res, err = actor.StopSession(session.StopRequest{
			Force:     req.Force,
			Requester: c.id,
			Delete:    deleteFile,
		}, stopRequestTimeout)
		if err != nil {
			if deleteFile {
				c.d.clearTombstoned(canon)
			}
			code := protocol.CodeSessionCrashed
			switch {
			case errors.Is(err, session.ErrSessionBusy):
				code = protocol.CodeSessionBusy
			case errors.Is(err, session.ErrSessionAttached):
				code = protocol.CodeSessionAttached
			}
			c.sendResponse(id, command, false, code, err.Error(), nil)
			return
		}
		if res.Path == "" {
			res.Path = canon
		}
		// pi must be fully reaped before the file is touched: a live pi
		// flushes its session file while shutting down, which would
		// resurrect the session we are deleting.
		<-actor.Finished()
	}

	fileDeleted := false
	if deleteFile {
		removed, err := deleteSessionFile(res.Path)
		if err != nil {
			// The session is stopped but the file is still there: undo the
			// tombstone so the operator can recover it.
			c.d.clearTombstoned(canon)
			c.sendResponse(id, command, false, protocol.CodeSessionCrashed, err.Error(), nil)
			return
		}
		fileDeleted = removed
		c.d.metrics.Inc(metrics.SessionsDeleted)
		// The session is gone: drop its durable spawn configuration so a later
		// session created at the same path does not inherit it.
		if err := c.d.spawn.Delete(canon); err != nil {
			c.d.log.Warn("cannot remove spawn configuration", "session", canon, "err", err)
		}
		c.d.log.Info("session deleted",
			"session", res.Path, "name", res.Name, "client", c.id, "kind", c.Kind(),
			"force", req.Force, "detachedClients", res.DetachedClients, "fileDeleted", removed)
	} else {
		c.d.log.Info("session stopped",
			"session", res.Path, "name", res.Name, "client", c.id, "kind", c.Kind(),
			"force", req.Force, "detachedClients", res.DetachedClients)
	}

	// unbound reports whether this command detached the requester from its own
	// bound session, so a client can clear its binding without waiting for the
	// terminal gw_session_state (which may lag the response).
	body := map[string]any{
		"path":            res.Path,
		"name":            res.Name,
		"sessionId":       res.SessionID,
		"piStopped":       res.PiStopped,
		"detachedClients": res.DetachedClients,
		"unbound":         unbound,
	}
	if deleteFile {
		body["fileDeleted"] = fileDeleted
	}
	payload, err := json.Marshal(body)
	if err != nil {
		c.sendResponse(id, command, false, protocol.CodeBadFrame, err.Error(), nil)
		return
	}
	c.sendResponse(id, command, true, "", "", payload)
}

// lookupSession finds the registered actor for a canonical path (any state,
// including crashed) and reads the catalog row for its file. found is false
// only when neither exists.
func (d *Daemon) lookupSession(canon string) (*session.Actor, catalog.Info, bool) {
	d.mu.Lock()
	e := d.sessions[canon]
	d.mu.Unlock()
	var actor *session.Actor
	if e != nil {
		actor = e.actor
	}
	info, ok := d.isSessionFileInfo(canon)
	return actor, info, actor != nil || ok
}

// deleteSessionFile removes exactly one session file and, best-effort, the
// encoded-cwd directory that held it when it is empty afterwards. It never
// recurses and only ever removes a directory that looks like pi's session
// layout (--<encoded-cwd>--). A file that is already gone is not an error;
// removed reports whether this call removed it.
func deleteSessionFile(path string) (removed bool, err error) {
	if path == "" {
		return false, nil
	}
	if err := os.Remove(path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	dir := filepath.Dir(path)
	if base := filepath.Base(dir); len(base) > 4 &&
		strings.HasPrefix(base, "--") && strings.HasSuffix(base, "--") {
		_ = os.Remove(dir) // succeeds only when empty; otherwise leave it
	}
	return true, nil
}

// detachTerminal reports whether a gw_session_state record means the session
// is gone but the connection should survive it. A deleted session, or one
// stopped by gw_stop_session, leaves its clients connected and unbound
// instead of disconnecting them (docs/protocol.md §3.10); every other
// terminal state keeps the historical close behavior.
func detachTerminal(rec protocol.Record) bool {
	if rec.Type != "gw_session_state" {
		return false
	}
	switch state := protocol.Field(rec.Raw, "state"); state {
	case protocol.SessionStateDeleted:
		return true
	case "stopped":
		reason := protocol.Field(rec.Raw, "reason")
		return reason == protocol.StopReasonRequested || reason == protocol.StopReasonForced
	}
	return false
}
