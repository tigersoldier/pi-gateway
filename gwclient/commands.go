package gwclient

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/tigersoldier/pi-gateway/protocol"
)

// This file holds the typed convenience wrappers a client is expected to use.
// Anything not covered here remains reachable through Do/Send with the pi
// command names from docs/protocol.md §4.

// Ping sends gw_ping and waits for the daemon's gw_pong, so it doubles as a
// liveness check. It gives up after Config.RequestTimeout, or when ctx ends,
// whichever comes first.
func (c *Client) Ping(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	// Drop a pong left over from an earlier ping, so this call cannot be
	// satisfied by an answer to a request that preceded it.
	select {
	case <-c.pong:
	default:
	}
	if err := c.Send("gw_ping", nil); err != nil {
		return err
	}
	timer := time.NewTimer(c.cfg.RequestTimeout)
	defer timer.Stop()
	select {
	case <-c.pong:
		return nil
	case <-c.closed:
		return c.Err()
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("gwclient: no gw_pong after %s", c.cfg.RequestTimeout)
	}
}

// Bye asks the daemon to close the connection. The session survives.
func (c *Client) Bye(ctx context.Context) error {
	return c.Send("gw_bye", nil)
}

// GetState asks pi for its current state. It also refreshes Session() (a
// lazily created session becomes visible here) and the turn state reported by
// TurnRunning.
func (c *Client) GetState(ctx context.Context) (protocol.PiState, error) {
	resp, err := c.Do(ctx, "get_state", nil)
	if err != nil {
		return protocol.PiState{}, err
	}
	state := protocol.ParsePiState(resp.Data)
	if state.SessionFile != "" {
		c.setSession(&protocol.SessionRef{
			Path: state.SessionFile,
			Name: state.SessionName,
			ID:   state.SessionID,
		})
	}
	c.noteTurn(state.IsStreaming)
	return state, nil
}

// GetMessages asks for the session transcript.
func (c *Client) GetMessages(ctx context.Context) (json.RawMessage, error) {
	resp, err := c.Do(ctx, "get_messages", nil)
	if err != nil {
		return nil, err
	}
	return resp.Data, nil
}

// Prompt sends a user message. When the session is idle the turn starts
// immediately; otherwise the daemon queues it and reports progress with
// gw_turn/gw_queue events. Optional images (pi's ImageContent shape) ride
// along with the message.
func (c *Client) Prompt(ctx context.Context, message string, images ...protocol.ImageContent) (*Response, error) {
	return c.Do(ctx, "prompt", messageFields(message, images))
}

// Steer interjects a message into the running turn. It accepts the same
// optional images as Prompt.
func (c *Client) Steer(ctx context.Context, message string, images ...protocol.ImageContent) (*Response, error) {
	return c.Do(ctx, "steer", messageFields(message, images))
}

// FollowUp queues a message to run after the agent has fully settled. Unlike
// Prompt, which starts a turn when the session is idle, it never preempts
// current work. It accepts the same optional images as Prompt.
func (c *Client) FollowUp(ctx context.Context, message string, images ...protocol.ImageContent) (*Response, error) {
	return c.Do(ctx, "follow_up", messageFields(message, images))
}

// messageFields builds a prompt/steer/follow_up payload. Images are copied so
// an empty Type is filled in without mutating the caller's slice.
func messageFields(message string, images []protocol.ImageContent) map[string]any {
	fields := map[string]any{"message": message}
	if len(images) == 0 {
		return fields
	}
	out := make([]protocol.ImageContent, len(images))
	copy(out, images)
	for i := range out {
		if out[i].Type == "" {
			out[i].Type = "image"
		}
	}
	fields["images"] = out
	return fields
}

// Abort cancels the running turn. The daemon queue is left intact, so queued
// prompts still run afterwards.
func (c *Client) Abort(ctx context.Context) (*Response, error) {
	return c.Do(ctx, "abort", nil)
}

// ClearQueue clears the daemon-owned queue and pi's forwarded steer queue,
// returning the cleared text in the response data.
func (c *Client) ClearQueue(ctx context.Context) (*Response, error) {
	return c.Do(ctx, "clear_queue", nil)
}

// SwitchSession rebinds this connection to an existing session, named by file
// path or session name. On success Session() reports the resolved session.
func (c *Client) SwitchSession(ctx context.Context, session string) (*Response, error) {
	resp, err := c.Do(ctx, "switch_session", map[string]any{"sessionPath": session})
	if err != nil {
		return resp, err
	}
	// The response is pi-shaped and carries no reference; GetState learns the
	// resolved path and refreshes Session(). A failure here must not fail the
	// switch.
	_, _ = c.GetState(ctx)
	return resp, nil
}

// NewSessionRequest is the gw_new_session payload (docs/protocol.md §3.3).
type NewSessionRequest struct {
	Name   string            // session name
	Cwd    string            // absolute spawn directory; empty means the daemon's
	PiArgs []string          // accepted pi parameters (docs/protocol.md §4.3)
	Tags   map[string]string // creator tags, reported in the catalog
}

// NewSession explicitly creates a session and rebinds this connection to it.
// It requires the admin capability.
func (c *Client) NewSession(ctx context.Context, req NewSessionRequest) (*protocol.SessionRef, error) {
	fields := map[string]any{}
	if req.Name != "" {
		fields["name"] = req.Name
	}
	if req.Cwd != "" {
		fields["cwd"] = req.Cwd
	}
	if len(req.PiArgs) > 0 {
		fields["piArgs"] = req.PiArgs
	}
	if len(req.Tags) > 0 {
		fields["tags"] = req.Tags
	}
	resp, err := c.Do(ctx, "gw_new_session", fields)
	if err != nil {
		return nil, err
	}
	var out struct {
		Path      string `json:"path"`
		Name      string `json:"name"`
		SessionID string `json:"sessionId"`
	}
	if err := resp.Decode(&out); err != nil {
		return nil, err
	}
	session := &protocol.SessionRef{Path: out.Path, Name: out.Name, ID: out.SessionID}
	c.setSession(session)
	return session, nil
}

// ListSessions returns the session catalog. It requires the observe
// capability.
func (c *Client) ListSessions(ctx context.Context, filter SessionFilter) ([]SessionRow, error) {
	// The daemon cannot filter by creator, so fetch everything and filter
	// here. The other filters stay server-side so the scan stays bounded.
	localFilter := len(filter.Tags) > 0 || filter.CreatedBy != ""
	serverLimit := filter.Limit
	if localFilter {
		serverLimit = 0
	}
	fields := map[string]any{"filter": map[string]any{
		"cwd":   filter.Cwd,
		"live":  filter.Live,
		"limit": serverLimit,
	}}
	resp, err := c.Do(ctx, "gw_list_sessions", fields)
	if err != nil {
		return nil, err
	}
	var out struct {
		Sessions []SessionRow `json:"sessions"`
	}
	if err := resp.Decode(&out); err != nil {
		return nil, err
	}
	if !localFilter {
		return out.Sessions, nil
	}
	rows := make([]SessionRow, 0, len(out.Sessions))
	for _, row := range out.Sessions {
		if !rowMatches(row, filter) {
			continue
		}
		rows = append(rows, row)
		if filter.Limit > 0 && len(rows) == filter.Limit {
			break
		}
	}
	return rows, nil
}

func rowMatches(row SessionRow, filter SessionFilter) bool {
	if filter.CreatedBy != "" {
		if row.CreatedBy == nil || row.CreatedBy.ClientID != filter.CreatedBy {
			return false
		}
	}
	for key, want := range filter.Tags {
		if row.CreatedBy == nil || row.CreatedBy.Tags[key] != want {
			return false
		}
	}
	return true
}

// Command is one entry from get_commands: an extension command, prompt
// template, or skill the session can run (docs/rpc.md, get_commands).
// Sending "/"+Name as a prompt invokes it; skill names already carry the
// "skill:" prefix.
type Command struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	// Source is "extension", "prompt" or "skill".
	Source string `json:"source,omitempty"`
	// Location is "user", "project" or "path" (absent for extensions).
	Location string `json:"location,omitempty"`
	Path     string `json:"path,omitempty"`
}

// GetCommands lists the commands, prompt templates and skills available in
// the bound session.
func (c *Client) GetCommands(ctx context.Context) ([]Command, error) {
	resp, err := c.Do(ctx, "get_commands", nil)
	if err != nil {
		return nil, err
	}
	var out struct {
		Commands []Command `json:"commands"`
	}
	if err := resp.Decode(&out); err != nil {
		return nil, err
	}
	return out.Commands, nil
}

// ReloadSession restarts pi for a session (default: the bound one). It fails
// with the reload_busy code while a turn runs or another client is attached,
// unless force is set.
func (c *Client) ReloadSession(ctx context.Context, session string, force bool) (*Response, error) {
	fields := map[string]any{"force": force}
	if session != "" {
		fields["session"] = session
	}
	return c.Do(ctx, "gw_reload_session", fields)
}

// RespondUI answers an extension_ui_request. requestID is the dialog id from
// the request; response carries pi's answer shape (for example {"value": ...}
// or {"cancelled": true}). It requires the ui capability.
func (c *Client) RespondUI(ctx context.Context, requestID string, response map[string]any) error {
	fields := make(map[string]any, len(response)+1)
	for k, v := range response {
		fields[k] = v
	}
	fields["id"] = requestID
	return c.Send("extension_ui_response", fields)
}
