package gwclient

import (
	"context"
	"encoding/json"
)

// This file wraps the inspection and state-mutation commands from
// docs/protocol.md §4.2 that are not part of the core chat loop. The
// mutating ones need the control capability, Bash needs prompt, and the
// read-only ones need observe. Anything not wrapped here stays reachable
// through Do, so a newer daemon never blocks a client.

// Model is one entry from GetAvailableModels (and the model object inside a
// set_model/cycle_model response). Unknown pi fields are ignored.
type Model struct {
	ID       string `json:"id"`
	Name     string `json:"name,omitempty"`
	Provider string `json:"provider,omitempty"`
}

// SetSessionName sets the session's display name (needs control).
func (c *Client) SetSessionName(ctx context.Context, name string) (*Response, error) {
	return c.Do(ctx, "set_session_name", map[string]any{"name": name})
}

// SetModel switches the session to provider/modelID (needs control).
func (c *Client) SetModel(ctx context.Context, provider, modelID string) (*Response, error) {
	return c.Do(ctx, "set_model", map[string]any{"provider": provider, "modelId": modelID})
}

// CycleModel advances to the next configured model (needs control). Decode
// the response for the model that became active.
func (c *Client) CycleModel(ctx context.Context) (*Response, error) {
	return c.Do(ctx, "cycle_model", nil)
}

// GetAvailableModels lists the configured models.
func (c *Client) GetAvailableModels(ctx context.Context) ([]Model, error) {
	resp, err := c.Do(ctx, "get_available_models", nil)
	if err != nil {
		return nil, err
	}
	var out struct {
		Models []Model `json:"models"`
	}
	if err := resp.Decode(&out); err != nil {
		return nil, err
	}
	return out.Models, nil
}

// SetThinkingLevel sets the reasoning level (needs control): "off", "minimal",
// "low", "medium", "high", "xhigh" or "max".
func (c *Client) SetThinkingLevel(ctx context.Context, level string) (*Response, error) {
	return c.Do(ctx, "set_thinking_level", map[string]any{"level": level})
}

// CycleThinkingLevel advances to the next supported level and returns it.
func (c *Client) CycleThinkingLevel(ctx context.Context) (string, error) {
	resp, err := c.Do(ctx, "cycle_thinking_level", nil)
	if err != nil {
		return "", err
	}
	var out struct {
		Level string `json:"level"`
	}
	if err := resp.Decode(&out); err != nil {
		return "", err
	}
	return out.Level, nil
}

// GetAvailableThinkingLevels lists the levels the current model supports.
func (c *Client) GetAvailableThinkingLevels(ctx context.Context) ([]string, error) {
	resp, err := c.Do(ctx, "get_available_thinking_levels", nil)
	if err != nil {
		return nil, err
	}
	var out struct {
		Levels []string `json:"levels"`
	}
	if err := resp.Decode(&out); err != nil {
		return nil, err
	}
	return out.Levels, nil
}

// SetSteeringMode sets how steer messages are delivered: "all" or
// "one-at-a-time" (needs control).
func (c *Client) SetSteeringMode(ctx context.Context, mode string) (*Response, error) {
	return c.Do(ctx, "set_steering_mode", map[string]any{"mode": mode})
}

// SetFollowUpMode sets how follow_up messages are delivered: "all" or
// "one-at-a-time" (needs control).
func (c *Client) SetFollowUpMode(ctx context.Context, mode string) (*Response, error) {
	return c.Do(ctx, "set_follow_up_mode", map[string]any{"mode": mode})
}

// Compact compacts the conversation context (needs control).
// customInstructions may be empty.
func (c *Client) Compact(ctx context.Context, customInstructions string) (*Response, error) {
	fields := map[string]any{}
	if customInstructions != "" {
		fields["customInstructions"] = customInstructions
	}
	return c.Do(ctx, "compact", fields)
}

// SetAutoCompaction enables or disables automatic compaction (needs control).
func (c *Client) SetAutoCompaction(ctx context.Context, enabled bool) (*Response, error) {
	return c.Do(ctx, "set_auto_compaction", map[string]any{"enabled": enabled})
}

// SetAutoRetry enables or disables retrying transient provider errors (needs
// control).
func (c *Client) SetAutoRetry(ctx context.Context, enabled bool) (*Response, error) {
	return c.Do(ctx, "set_auto_retry", map[string]any{"enabled": enabled})
}

// AbortRetry cancels an in-progress retry wait (needs interject).
func (c *Client) AbortRetry(ctx context.Context) (*Response, error) {
	return c.Do(ctx, "abort_retry", nil)
}

// AbortBash cancels a running direct bash command (needs interject).
func (c *Client) AbortBash(ctx context.Context) (*Response, error) {
	return c.Do(ctx, "abort_bash", nil)
}

// GetSessionStats returns token, cost and context-window statistics. The data
// is pi-shaped; decode it with Response.Decode.
func (c *Client) GetSessionStats(ctx context.Context) (*Response, error) {
	return c.Do(ctx, "get_session_stats", nil)
}

// ExportHTML exports the session transcript to HTML and returns the written
// path. An empty outputPath lets pi choose the file name.
func (c *Client) ExportHTML(ctx context.Context, outputPath string) (string, error) {
	fields := map[string]any{}
	if outputPath != "" {
		fields["outputPath"] = outputPath
	}
	resp, err := c.Do(ctx, "export_html", fields)
	if err != nil {
		return "", err
	}
	var out struct {
		Path string `json:"path"`
	}
	if err := resp.Decode(&out); err != nil {
		return "", err
	}
	return out.Path, nil
}

// EntriesPage is one GetEntries answer: entries in append order plus the
// current leaf id ("" for an empty session).
type EntriesPage struct {
	Entries []json.RawMessage `json:"entries"`
	LeafID  string            `json:"leafId"`
}

// GetEntries returns session entries after the entry id `since` ("" for all).
// Unlike GetMessages it includes pre-compaction history and abandoned
// branches, and it is the durable way to follow a session across restarts.
func (c *Client) GetEntries(ctx context.Context, since string) (EntriesPage, error) {
	fields := map[string]any{}
	if since != "" {
		fields["since"] = since
	}
	resp, err := c.Do(ctx, "get_entries", fields)
	if err != nil {
		return EntriesPage{}, err
	}
	var out EntriesPage
	if err := resp.Decode(&out); err != nil {
		return EntriesPage{}, err
	}
	c.noteLeaf(out.LeafID)
	return out, nil
}

// Tree is the session entry tree; decode the response data with
// Response.Decode (nodes are {entry, children, label?}).
func (c *Client) Tree(ctx context.Context) (*Response, error) {
	resp, err := c.Do(ctx, "get_tree", nil)
	if err != nil {
		return resp, err
	}
	var out struct {
		LeafID string `json:"leafId"`
	}
	_ = json.Unmarshal(resp.Data, &out)
	c.noteLeaf(out.LeafID)
	return resp, nil
}

// ForkMessage is one entry from GetForkMessages.
type ForkMessage struct {
	EntryID string `json:"entryId"`
	Text    string `json:"text"`
}

// GetForkMessages lists the user messages a fork can start from.
func (c *Client) GetForkMessages(ctx context.Context) ([]ForkMessage, error) {
	resp, err := c.Do(ctx, "get_fork_messages", nil)
	if err != nil {
		return nil, err
	}
	var out struct {
		Messages []ForkMessage `json:"messages"`
	}
	if err := resp.Decode(&out); err != nil {
		return nil, err
	}
	return out.Messages, nil
}

// GetLastAssistantText returns the last assistant message's text, or "" when
// the session has none.
func (c *Client) GetLastAssistantText(ctx context.Context) (string, error) {
	resp, err := c.Do(ctx, "get_last_assistant_text", nil)
	if err != nil {
		return "", err
	}
	var out struct {
		Text *string `json:"text"`
	}
	if err := resp.Decode(&out); err != nil {
		return "", err
	}
	if out.Text == nil {
		return "", nil
	}
	return *out.Text, nil
}

// BashUpdate is one streamed chunk of a direct bash command.
type BashUpdate struct {
	ID    string
	Delta string
}

// BashResult is the final answer of a direct bash command.
type BashResult struct {
	ID             string
	Output         string `json:"output"`
	ExitCode       int    `json:"exitCode"`
	Cancelled      bool   `json:"cancelled"`
	Truncated      bool   `json:"truncated"`
	FullOutputPath string `json:"fullOutputPath,omitempty"`
}

// Bash runs a shell command in the session and adds its output to the
// conversation context (needs prompt). While the command runs, every
// bash_execution_update for it is passed to onUpdate (nil to ignore); an
// update consumed by onUpdate is not also delivered to Events().
func (c *Client) Bash(ctx context.Context, command string, onUpdate func(BashUpdate)) (BashResult, error) {
	id := c.newID()
	if onUpdate != nil {
		c.addBashHandler(id, onUpdate)
		defer c.removeBashHandler(id)
	}
	resp, err := c.do(ctx, id, "bash", map[string]any{"command": command})
	if err != nil {
		return BashResult{ID: id}, err
	}
	var out BashResult
	if err := resp.Decode(&out); err != nil {
		return BashResult{ID: id}, err
	}
	out.ID = id
	return out, nil
}

func (c *Client) addBashHandler(id string, h func(BashUpdate)) {
	c.bashMu.Lock()
	c.bashHandlers[id] = h
	c.bashMu.Unlock()
}

func (c *Client) removeBashHandler(id string) {
	c.bashMu.Lock()
	delete(c.bashHandlers, id)
	c.bashMu.Unlock()
}

func (c *Client) bashHandler(id string) func(BashUpdate) {
	if id == "" {
		return nil
	}
	c.bashMu.Lock()
	defer c.bashMu.Unlock()
	return c.bashHandlers[id]
}

// PiVersion is the pi build the daemon manages, from the welcome.
func (c *Client) PiVersion() string { return c.welcome.PiVersion }

// Concurrency is the daemon's concurrency model ("queue").
func (c *Client) Concurrency() string { return c.welcome.Concurrency }
