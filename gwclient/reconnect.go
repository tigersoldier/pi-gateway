package gwclient

import (
	"context"
	"errors"

	"github.com/tigersoldier/pi-gateway/protocol"
)

// LastSeq is the gw_seq of the newest record this client has consumed. It is
// seeded from the attach head (or from Config.Resume) and advances with every
// record, so it can be persisted and reused as Resume.SinceSeq.
func (c *Client) LastSeq() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastSeq
}

// LeafID is the session-file leaf entry the client's state matches, when
// known. It is reported by gw_snapshot and by GetEntries/GetTree, and is sent
// as Resume.LeafEntryID so a daemon whose replay ring was reset can tell an
// already-current client from one that needs a snapshot.
func (c *Client) LeafID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.leafID
}

// Cursor returns this client's replay cursor. Persist it and pass it as
// Config.Resume to resume after a process restart.
func (c *Client) Cursor() protocol.Resume {
	c.mu.Lock()
	defer c.mu.Unlock()
	return protocol.Resume{SinceSeq: c.lastSeq, LeafEntryID: c.leafID}
}

// Reconnect replaces the connection after it ended, resuming from this
// client's cursor. The old connection is closed and its Events() channel
// drains and closes; the returned client is a fresh, live one.
//
// The reconnect attaches to the resolved session path with
// Resume{SinceSeq, LeafEntryID} when the client consumed anything (LastSeq >
// 0); a client that never recorded a cursor attaches live rather than
// replaying history it never saw. If the daemon cannot replay the cursor it
// answers with a snapshot (Welcome.ResyncRequired), which the caller applies
// by decoding the Event whose Type is "gw_snapshot".
func (c *Client) Reconnect(ctx context.Context) (*Client, error) {
	session := c.Session()
	if session == nil || session.Path == "" {
		return nil, errors.New("gwclient: reconnect: the client is not bound to a session")
	}
	cfg := c.cfg
	cfg.Session = session.Path
	cfg.Resume = nil
	if c.LastSeq() > 0 {
		cursor := c.Cursor()
		cfg.Resume = &cursor
		// A resume cursor only takes effect when the daemon is not asked to
		// attach live-only; Reconnect exists to close the gap, so it always
		// replays.
		cfg.LiveOnly = false
	}
	_ = c.Close()
	return Dial(ctx, cfg)
}

// advanceSeq raises the cursor to seq when it is newer.
func (c *Client) advanceSeq(seq uint64) {
	c.mu.Lock()
	if seq > c.lastSeq {
		c.lastSeq = seq
	}
	c.mu.Unlock()
}

// resetSeq replaces the cursor, used when a snapshot supersedes the records a
// previous connection had numbered higher (a restarted daemon restarts its
// hub sequence at zero).
func (c *Client) resetSeq(seq uint64) {
	c.mu.Lock()
	c.lastSeq = seq
	c.mu.Unlock()
}

// noteLeaf records a session leaf learned from a snapshot or an entry page.
func (c *Client) noteLeaf(id string) {
	if id == "" {
		return
	}
	c.mu.Lock()
	c.leafID = id
	c.mu.Unlock()
}
