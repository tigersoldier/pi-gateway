package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync/atomic"

	"github.com/tigersoldier/pi-gateway/gwclient"
	"github.com/tigersoldier/pi-gateway/protocol"
)

// renderer turns the gwclient event stream into terminal output. Assistant
// text goes to out, so `chat | tee transcript.txt` captures a clean
// transcript; prompts, tool summaries, usage and errors go to errOut.
type renderer struct {
	out      io.Writer
	errOut   io.Writer
	thinking bool
	usage    bool
	verbose  bool
	tools    bool

	busy atomic.Bool // a turn is running (fed by gw_turn)

	// waiting is set when this client sent a prompt or follow-up whose turn
	// has not settled yet; queuedCount mirrors the daemon queue. Together with
	// busy they let the CLI wait for outstanding work before it exits.
	waiting   atomic.Bool
	queued    atomic.Int64
	wroteText bool
	use       usageInfo
}

// usageInfo is the cumulative provider usage carried on message_update.
type usageInfo struct {
	Input       int `json:"input"`
	Output      int `json:"output"`
	TotalTokens int `json:"totalTokens"`
}

// contentPart is one pi content block (text, thinking, toolCall, …).
type contentPart struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// Busy reports whether a turn is currently running.
func (r *renderer) Busy() bool { return r.busy.Load() }

// MarkPending records that this client submitted work (prompt/follow-up).
func (r *renderer) MarkPending() { r.waiting.Store(true) }

// Pending reports whether the session still has work this client is waiting
// for: a running turn, a submitted turn, or a daemon-queued prompt.
func (r *renderer) Pending() bool {
	return r.waiting.Load() || r.busy.Load() || r.queued.Load() > 0
}

// Handle renders one event. Unknown event types are ignored, so a newer
// daemon never breaks the CLI.
func (r *renderer) Handle(ev gwclient.Event) {
	if ev.Field("gw_owner") != "" {
		// Output produced by another client's command; a bot would route it
		// back to that client's own conversation instead.
		return
	}
	switch ev.Type {
	case "message_update":
		r.messageUpdate(ev)
	case "message_end":
		r.messageEnd(ev)
	case "tool_execution_start":
		r.toolStart(ev)
	case "tool_execution_end":
		r.toolEnd(ev)
	case "turn_start", "turn_end", "agent_settled", "compaction_start", "compaction_end",
		"auto_retry_start", "auto_retry_end", "summarization_retry_scheduled":
		r.statusLine(ev.Type)
	case "bash_execution_update":
		if r.verbose {
			fmt.Fprint(r.errOut, ev.Field("delta"))
		}
	case "gw_turn":
		var turn protocol.TurnEvent
		if ev.Unmarshal(&turn) == nil {
			r.busy.Store(turn.State == "running")
			if turn.State == "settled" {
				r.waiting.Store(false)
			}
			r.statusLine("turn " + turn.State)
		}
	case "gw_queue":
		var queue protocol.QueueEvent
		if ev.Unmarshal(&queue) == nil {
			r.queued.Store(int64(len(queue.Pending)))
			r.statusLine(fmt.Sprintf("queue %d pending", len(queue.Pending)))
		}
	case "gw_session_state":
		var state protocol.SessionStateEvent
		if ev.Unmarshal(&state) == nil {
			if state.State == "crashed" || state.State == "stopped" || state.State == "hibernated" {
				r.waiting.Store(false)
				r.queued.Store(0)
			}
			line := "session " + state.State
			if state.Reason != "" {
				line += ": " + state.Reason
			}
			fmt.Fprintf(r.errOut, "\n[%s]\n", line)
		}
	case "gw_lag":
		fmt.Fprintf(r.errOut, "\n[lag] dropped records %d..%d; reconnect with a resume cursor to resync\n",
			uint64(protocol.NumField(ev.Raw, "oldestSeq")), uint64(protocol.NumField(ev.Raw, "headSeq")))
	case "gw_error":
		fmt.Fprintf(r.errOut, "\n[error %s] %s\n", ev.Field("code"), ev.Field("message"))
	case "extension_error":
		fmt.Fprintf(r.errOut, "\n[extension error] %s\n", ev.Field("error"))
	}
}

// statusLine prints activity only in verbose mode: the streamed text already
// shows that a turn is running.
func (r *renderer) statusLine(text string) {
	if r.verbose {
		fmt.Fprintf(r.errOut, "\n[%s]\n", text)
	}
}

func (r *renderer) messageUpdate(ev gwclient.Event) {
	var update struct {
		Usage                 usageInfo `json:"usage"`
		AssistantMessageEvent struct {
			Type     string `json:"type"`
			Delta    string `json:"delta"`
			ToolName string `json:"toolName"`
		} `json:"assistantMessageEvent"`
	}
	if err := ev.Unmarshal(&update); err != nil {
		return
	}
	if update.Usage.TotalTokens > 0 {
		r.use = update.Usage
	}
	delta := update.AssistantMessageEvent
	switch delta.Type {
	case "text_start":
		// Nothing to print yet; deltas follow.
	case "text_delta":
		r.wroteText = true
		fmt.Fprint(r.out, delta.Delta)
	case "text_end":
		r.endText()
	case "thinking_start":
		if r.thinking {
			fmt.Fprint(r.errOut, "\n[thinking] ")
		}
	case "thinking_delta":
		if r.thinking {
			fmt.Fprint(r.errOut, delta.Delta)
		}
	case "thinking_end":
		if r.thinking {
			fmt.Fprintln(r.errOut)
		}
	case "toolcall_start":
		if r.verbose {
			fmt.Fprintf(r.errOut, "\n[toolcall %s]\n", delta.ToolName)
		}
	}
}

// endText finishes a streamed text block. message_end calls it again as a
// fallback for a message that never sent text_end.
func (r *renderer) endText() {
	if r.wroteText {
		fmt.Fprintln(r.out)
	}
	r.wroteText = false
}

func (r *renderer) messageEnd(ev gwclient.Event) {
	var msg struct {
		Message struct {
			StopReason   string `json:"stopReason"`
			ErrorMessage string `json:"errorMessage"`
		} `json:"message"`
	}
	_ = ev.Unmarshal(&msg)
	r.endText()
	switch reason := msg.Message.StopReason; reason {
	case "error", "aborted":
		text := msg.Message.ErrorMessage
		if text == "" {
			text = reason
		}
		fmt.Fprintf(r.errOut, "\n[assistant %s: %s]\n", reason, text)
	}
	if r.usage && r.use.TotalTokens > 0 {
		fmt.Fprintf(r.errOut, "[usage] input=%d output=%d total=%d\n",
			r.use.Input, r.use.Output, r.use.TotalTokens)
	}
}

func (r *renderer) toolStart(ev gwclient.Event) {
	if !r.tools {
		return
	}
	var tool struct {
		ToolName string          `json:"toolName"`
		Args     json.RawMessage `json:"args"`
	}
	if err := ev.Unmarshal(&tool); err != nil {
		return
	}
	fmt.Fprintf(r.errOut, "\n· %s %s\n", tool.ToolName, compactJSON(tool.Args))
}

func (r *renderer) toolEnd(ev gwclient.Event) {
	if !r.tools {
		return
	}
	var tool struct {
		ToolName string `json:"toolName"`
		IsError  bool   `json:"isError"`
		Result   struct {
			Content []contentPart `json:"content"`
		} `json:"result"`
	}
	if err := ev.Unmarshal(&tool); err != nil {
		return
	}
	text := contentText(tool.Result.Content)
	switch {
	case tool.IsError:
		fmt.Fprintf(r.errOut, "[tool %s failed] %s\n", tool.ToolName, truncate(text, 2000))
	case strings.TrimSpace(text) != "":
		fmt.Fprintln(r.errOut, truncate(text, 2000))
	}
}

func contentText(parts []contentPart) string {
	var out []string
	for _, part := range parts {
		if part.Type == "text" && part.Text != "" {
			out = append(out, part.Text)
		}
	}
	return strings.Join(out, "\n")
}

func compactJSON(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return truncate(string(raw), 200)
	}
	return truncate(buf.String(), 200)
}

func truncate(s string, max int) string {
	runes := []rune(s)
	if len(runes) <= max {
		return s
	}
	return string(runes[:max]) + fmt.Sprintf("\n… (%d more chars)", len(runes)-max)
}
