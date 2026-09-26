// Command fakepi is a minimal pi RPC stand-in for the gateway's tests. It
// implements just enough of pi's command/event surface: get_state reports a
// session file, prompt runs a short synthetic turn with streaming deltas, and
// everything else gets a success response. It writes a real session file
// (header, message entries, session_info) so the catalog can scan it.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/tigersoldier/pi-gateway/internal/catalog"
	"github.com/tigersoldier/pi-gateway/protocol"
)

func main() {
	args := os.Args[1:]
	if len(args) == 1 && args[0] == "--version" {
		fmt.Println("fakepi 0.1.0")
		return
	}

	var (
		sessionFile string
		sessionDir  string
		sessionName string
		noSession   bool
		model       = "fake-model"
		provider    = "fake-provider"
		thinking    = "medium"
	)
	for i := 0; i < len(args); i++ {
		next := func() string {
			if i+1 < len(args) {
				i++
				return args[i]
			}
			return ""
		}
		switch args[i] {
		case "--mode":
			next()
		case "--session":
			sessionFile = next()
		case "--session-dir":
			sessionDir = next()
		case "--name":
			sessionName = next()
		case "--model":
			model = next()
		case "--provider":
			provider = next()
		case "--thinking":
			thinking = next()
		case "--no-session":
			noSession = true
		}
	}
	s := &state{
		sessionFile:    sessionFile,
		sessionDir:     sessionDir,
		sessionName:    sessionName,
		noSession:      noSession,
		model:          model,
		provider:       provider,
		thinking:       thinking,
		events:         envInt("FAKEPI_TURN_EVENTS", 3),
		delay:          time.Duration(envInt("FAKEPI_TURN_DELAY_MS", 15)) * time.Millisecond,
		rejectPrompt:   os.Getenv("FAKEPI_REJECT_PROMPT") != "",
		uiRequest:      os.Getenv("FAKEPI_UI_REQUEST") != "",
		withCommands:   os.Getenv("FAKEPI_COMMANDS") != "",
		exitAfterFirst: os.Getenv("FAKEPI_EXIT_AFTER_FIRST_TURN") != "",
		abortMarker:    os.Getenv("FAKEPI_ABORT_MARKER"),
		slowSettle:     time.Duration(envInt("FAKEPI_ABORT_SLOW_MS", 0)) * time.Millisecond,
		ignoreAbort:    os.Getenv("FAKEPI_ABORT_IGNORE") != "",
		flushOnExit:    os.Getenv("FAKEPI_FLUSH_ON_EXIT") != "",
	}
	s.open()
	codec := protocol.NewCodec(os.Stdin, os.Stdout)
	for {
		raw, err := codec.Read()
		if err != nil {
			s.flushOnExitFile()
			return
		}
		var msg map[string]any
		if err := json.Unmarshal(raw, &msg); err != nil {
			continue
		}
		s.handle(codec, msg)
	}
}

type state struct {
	mu             sync.Mutex
	sessionFile    string
	sessionDir     string
	sessionID      string
	sessionName    string
	leafID         string
	noSession      bool
	model          string
	provider       string
	thinking       string
	messageCount   int
	streaming      bool
	events         int
	delay          time.Duration
	rejectPrompt   bool
	uiRequest      bool
	withCommands   bool
	exitAfterFirst bool
	lastAssistant  string
	turns          int
	forks          int
	nextEntry      int

	// abortCh is closed by `abort` to unwind the running turn; the gateway's
	// forced stop uses it. abortMarker makes the abort observable to a test,
	// slowSettle delays the settle it causes, and ignoreAbort models a model
	// that does not react at all.
	abortCh     chan struct{}
	abortMarker string
	slowSettle  time.Duration
	ignoreAbort bool
	flushOnExit bool
}

// open adopts an existing session file or creates one with a header.
func (s *state) open() {
	if s.sessionFile == "" {
		if s.noSession {
			return
		}
		dir := s.sessionDir
		if dir == "" {
			dir = os.TempDir()
		}
		s.sessionFile = filepath.Join(dir, fmt.Sprintf("fakepi-%d.jsonl", os.Getpid()))
	}
	if info, err := catalog.Parse(s.sessionFile); err == nil {
		s.sessionID = info.ID
		s.sessionName = firstNonEmpty(s.sessionName, info.Name)
		s.messageCount = info.MessageCount
		if leaf, err := catalog.LeafID(s.sessionFile); err == nil {
			s.leafID = leaf
		}
		return
	}
	if s.noSession {
		return
	}
	if err := os.MkdirAll(filepath.Dir(s.sessionFile), 0o755); err != nil {
		return
	}
	s.sessionID = fmt.Sprintf("fake-%d", os.Getpid())
	s.writeHeader()
	if s.sessionName != "" {
		s.appendEntry(map[string]any{"type": "session_info", "name": s.sessionName})
	}
}

// writeHeader writes the mandatory first line of a session file. Every other
// entry chains off the header, so it must come first.
func (s *state) writeHeader() {
	s.appendEntry(map[string]any{
		"type": "session", "version": 3, "id": s.sessionID,
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		"cwd":       mustGetwd(),
	})
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func mustGetwd() string {
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}
	return dir
}

// appendEntry writes one session line, chaining parentId to the current leaf
// (except for the header, which stays root).
func (s *state) commandList() []any {
	if !s.withCommands {
		return []any{}
	}
	// One of each source type so a client's decoder is exercised against the
	// documented shape (docs/rpc.md, get_commands).
	return []any{
		map[string]any{"name": "session-name", "description": "Set or clear session name",
			"source": "extension", "path": "/tmp/extensions/session.ts"},
		map[string]any{"name": "fix-tests", "description": "Fix failing tests",
			"source": "prompt", "location": "project", "path": "/tmp/prompts/fix-tests.md"},
		map[string]any{"name": "skill:brave-search", "description": "Web search via Brave API",
			"source": "skill", "location": "user", "path": "/tmp/skills/brave-search/SKILL.md"},
	}
}

func (s *state) appendEntry(entry map[string]any) {
	if s.sessionFile == "" {
		return
	}
	s.nextEntry++
	if entry["type"] != "session" {
		if _, ok := entry["id"]; !ok {
			entry["id"] = fmt.Sprintf("e%d", s.nextEntry)
		}
		entry["parentId"] = s.leafID
		if _, ok := entry["timestamp"]; !ok {
			entry["timestamp"] = time.Now().UTC().Format(time.RFC3339Nano)
		}
		s.leafID, _ = entry["id"].(string)
	}
	appendLine(s.sessionFile, entry)
}

func (s *state) handle(c *protocol.Codec, msg map[string]any) {
	typ, _ := msg["type"].(string)
	id, _ := msg["id"].(string)
	respond := func(command string, success bool, data any, errMsg string) {
		var payload json.RawMessage
		if data != nil {
			if raw, err := json.Marshal(data); err == nil {
				payload = raw
			}
		}
		_ = c.WriteRaw(protocol.Response(id, command, success, "", errMsg, payload))
	}

	switch typ {
	case "get_state":
		respond("get_state", true, s.stateData(), "")
	case "get_entries":
		s.mu.Lock()
		leaf := s.leafID
		s.mu.Unlock()
		entries := []any{}
		if leaf != "" {
			entries = append(entries, map[string]any{"type": "message", "id": leaf})
		}
		respond("get_entries", true, map[string]any{"entries": entries, "leafId": leaf}, "")
	case "get_tree":
		s.mu.Lock()
		leaf := s.leafID
		s.mu.Unlock()
		tree := []any{}
		if leaf != "" {
			tree = append(tree, map[string]any{
				"entry":    map[string]any{"type": "message", "id": leaf},
				"children": []any{},
			})
		}
		respond("get_tree", true, map[string]any{"tree": tree, "leafId": leaf}, "")
	case "get_session_stats":
		s.mu.Lock()
		count := s.messageCount
		s.mu.Unlock()
		respond("get_session_stats", true, map[string]any{
			"messageCount": count,
			"tokens":       map[string]any{"total": 7},
		}, "")
	case "export_html":
		path, _ := msg["outputPath"].(string)
		if path == "" {
			path = "/tmp/fakepi-export.html"
		}
		respond("export_html", true, map[string]any{"path": path}, "")
	case "compact":
		data := map[string]any{"summary": "compacted"}
		if v, ok := msg["customInstructions"].(string); ok {
			data["customInstructions"] = v
		}
		respond("compact", true, data, "")
	case "set_auto_compaction", "set_auto_retry":
		respond(typ, true, map[string]any{"enabled": msg["enabled"]}, "")
	case "set_steering_mode", "set_follow_up_mode":
		respond(typ, true, map[string]any{"mode": msg["mode"]}, "")
	case "cycle_model":
		s.mu.Lock()
		model := map[string]any{"id": s.model, "provider": s.provider, "name": "Fake Model"}
		s.mu.Unlock()
		respond("cycle_model", true, map[string]any{"model": model, "thinkingLevel": "medium", "isScoped": false}, "")
	case "get_available_models":
		respond("get_available_models", true, map[string]any{"models": []any{
			map[string]any{"id": "fake-one", "name": "Fake One", "provider": "fake"},
			map[string]any{"id": "fake-two", "name": "Fake Two", "provider": "fake"},
		}}, "")
	case "cycle_thinking_level":
		s.mu.Lock()
		s.thinking = "high"
		level := s.thinking
		s.mu.Unlock()
		respond("cycle_thinking_level", true, map[string]any{"level": level}, "")
	case "get_available_thinking_levels":
		respond("get_available_thinking_levels", true, map[string]any{"levels": []any{"off", "low", "high"}}, "")
	case "get_fork_messages":
		respond("get_fork_messages", true, map[string]any{"messages": []any{
			map[string]any{"entryId": "e1", "text": "first prompt"},
		}}, "")
	case "get_last_assistant_text":
		s.mu.Lock()
		text := s.lastAssistant
		s.mu.Unlock()
		var value any
		if text != "" {
			value = text
		}
		respond("get_last_assistant_text", true, map[string]any{"text": value}, "")
	case "bash":
		command, _ := msg["command"].(string)
		if id != "" {
			_ = c.WriteJSON(map[string]any{
				"type": "bash_execution_update", "id": id, "delta": "fake:" + command + "\n",
			})
		}
		respond("bash", true, map[string]any{
			"output": "fake:" + command, "exitCode": 0, "cancelled": false, "truncated": false,
		}, "")
	case "get_messages":
		respond("get_messages", true, map[string]any{"messages": []any{}}, "")
	case "get_commands":
		respond("get_commands", true, map[string]any{"commands": s.commandList()}, "")
	case "prompt":
		s.handlePrompt(c, msg, respond)
	case "abort":
		s.handleAbort()
		respond("abort", true, map[string]any{}, "")
	case "steer", "follow_up", "abort_retry", "abort_bash":
		respond(typ, true, map[string]any{}, "")
	case "clear_queue":
		respond("clear_queue", true, map[string]any{"steering": []any{}, "followUp": []any{}}, "")
	case "extension_ui_response":
		// pi does not answer dialog responses, but the gateway tests need to
		// see which id pi was handed.
		respond(typ, true, map[string]any{"id": id}, "")
	case "clone", "fork":
		if s.newFile() != nil {
			respond(typ, true, map[string]any{"cancelled": true}, "")
			break
		}
		data := map[string]any{"cancelled": false}
		if typ == "fork" {
			data["text"] = "forked"
		}
		respond(typ, true, data, "")
	case "set_model":
		s.mu.Lock()
		if v, ok := msg["modelId"].(string); ok {
			s.model = v
		}
		if v, ok := msg["provider"].(string); ok {
			s.provider = v
		}
		data := map[string]any{"id": s.model, "provider": s.provider}
		s.mu.Unlock()
		respond("set_model", true, data, "")
	case "set_thinking_level":
		s.mu.Lock()
		if v, ok := msg["level"].(string); ok {
			s.thinking = v
		}
		data := map[string]any{"level": s.thinking}
		s.mu.Unlock()
		respond("set_thinking_level", true, data, "")
	case "set_session_name":
		s.mu.Lock()
		if v, ok := msg["name"].(string); ok {
			s.sessionName = v
			s.appendEntry(map[string]any{"type": "session_info", "name": v})
		}
		data := map[string]any{"name": s.sessionName}
		s.mu.Unlock()
		respond("set_session_name", true, data, "")
	default:
		respond(typ, true, map[string]any{}, "")
	}
}

// stateData mirrors pi's get_state payload for the fields the gateway uses.
func (s *state) stateData() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	data := map[string]any{
		"model":                 map[string]any{"id": s.model, "provider": s.provider, "name": "Fake Model"},
		"thinkingLevel":         s.thinking,
		"isStreaming":           s.streaming,
		"isCompacting":          false,
		"steeringMode":          "all",
		"followUpMode":          "one-at-a-time",
		"sessionId":             s.sessionID,
		"autoCompactionEnabled": true,
		"messageCount":          s.messageCount,
		"pendingMessageCount":   0,
	}
	if s.sessionFile != "" {
		data["sessionFile"] = s.sessionFile
	}
	if s.sessionName != "" {
		data["sessionName"] = s.sessionName
	}
	return data
}

// newFile switches to a fresh session file (pi's fork/clone behavior). It
// returns an error when there is nothing to fork.
func (s *state) newFile() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sessionFile == "" {
		return fmt.Errorf("no session file")
	}
	s.forks++
	dir := s.sessionDir
	if dir == "" {
		dir = filepath.Dir(s.sessionFile)
	}
	s.sessionFile = filepath.Join(dir, fmt.Sprintf("fakepi-fork-%d-%d.jsonl", os.Getpid(), s.forks))
	s.sessionID = fmt.Sprintf("fake-fork-%d", s.forks)
	s.sessionName = ""
	s.leafID = ""
	s.messageCount = 0
	s.writeHeader()
	return nil
}

// flushOnExitFile appends one entry while the process is shutting down. A
// test uses it to prove that gw_delete_session removes the file only after pi
// has been reaped: a file deleted before pi's shutdown flush would reappear.
func (s *state) flushOnExitFile() {
	if !s.flushOnExit {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.appendEntry(map[string]any{"type": "message", "message": map[string]any{
		"role": "assistant", "content": "flushed during shutdown",
	}})
}

// handleAbort unwinds the running turn: the turn goroutine stops streaming
// and settles. A test can watch abortMarker for the abort and use slowSettle
// to make the settle take longer than the gateway's forced-stop grace.
func (s *state) handleAbort() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ignoreAbort {
		return
	}
	if s.abortMarker != "" {
		_ = os.WriteFile(s.abortMarker, []byte("aborted\n"), 0o644)
	}
	if s.abortCh != nil {
		select {
		case <-s.abortCh:
		default:
			close(s.abortCh)
		}
	}
}

func (s *state) handlePrompt(c *protocol.Codec, msg map[string]any,
	respond func(string, bool, any, string)) {
	s.mu.Lock()
	if s.rejectPrompt {
		s.mu.Unlock()
		respond("prompt", false, nil, "rejected by test")
		return
	}
	if s.streaming {
		s.mu.Unlock()
		respond("prompt", false, nil, "already streaming")
		return
	}
	s.streaming = true
	s.appendEntry(map[string]any{"type": "message", "message": map[string]any{
		"role": "user", "content": msg["message"],
	}})
	s.messageCount++
	events, delay := s.events, s.delay
	uiRequest := s.uiRequest
	s.abortCh = make(chan struct{})
	abortCh := s.abortCh
	s.mu.Unlock()

	respond("prompt", true, map[string]any{}, "")
	go s.runTurn(c, msg, events, delay, uiRequest, abortCh)
}

func (s *state) runTurn(c *protocol.Codec, msg map[string]any, events int, delay time.Duration, uiRequest bool, abortCh <-chan struct{}) {
	text, _ := msg["message"].(string)
	echo := "echo: " + text
	if images, ok := msg["images"].([]any); ok && len(images) > 0 {
		echo = fmt.Sprintf("%s [images: %d]", echo, len(images))
	}
	// A turn is a fixed sequence of frames; an abort stops it at the next
	// frame, so the gateway sees a settling turn rather than a dead stream.
	frames := []map[string]any{{"type": "agent_start"}}
	if uiRequest {
		frames = append(frames, map[string]any{
			"type":    "extension_ui_request",
			"id":      "ui-1",
			"method":  "confirm",
			"title":   "proceed?",
			"message": "fake dialog",
			"timeout": 60000,
		})
	}
	frames = append(frames,
		map[string]any{"type": "turn_start"},
		map[string]any{"type": "message_start", "message": map[string]any{"role": "assistant"}},
		map[string]any{"type": "message_update",
			"assistantMessageEvent": map[string]any{"type": "text_start", "contentIndex": 0}},
	)
	for i := 0; i < events; i++ {
		frames = append(frames, map[string]any{
			"type":  "message_update",
			"usage": map[string]any{"output": i + 1, "totalTokens": i + 1},
			"assistantMessageEvent": map[string]any{
				"type": "text_delta", "contentIndex": 0,
				"delta": fmt.Sprintf("%s[%d]", text, i),
			},
		})
	}
	frames = append(frames,
		map[string]any{"type": "message_update",
			"assistantMessageEvent": map[string]any{
				"type": "text_end", "contentIndex": 0, "content": echo,
			}},
		map[string]any{"type": "message_end", "message": map[string]any{
			"role":       "assistant",
			"stopReason": "stop",
			"content":    []any{map[string]any{"type": "text", "text": echo}},
		}},
		map[string]any{"type": "turn_end"},
		map[string]any{"type": "agent_end"},
	)

	aborted := false
	for _, frame := range frames {
		select {
		case <-abortCh:
			aborted = true
		default:
		}
		if aborted {
			break
		}
		_ = c.WriteJSON(frame)
		if delay > 0 {
			time.Sleep(delay)
		}
	}
	if aborted && s.slowSettle > 0 {
		// Model the time a real agent takes to wind down after an abort; the
		// gateway's forced stop stops pi when this outlasts its grace.
		time.Sleep(s.slowSettle)
	}

	s.mu.Lock()
	s.streaming = false
	s.abortCh = nil
	if !aborted {
		s.lastAssistant = echo
		s.appendEntry(map[string]any{"type": "message", "message": map[string]any{
			"role": "assistant", "content": []any{map[string]any{"type": "text", "text": echo}},
		}})
		s.messageCount++
	}
	s.turns++
	exitAfter := s.exitAfterFirst && s.turns >= 1 && !aborted
	s.mu.Unlock()
	// The settle frame is written even after an abort: it is what makes the
	// turn end cleanly for the gateway.
	_ = c.WriteJSON(map[string]any{"type": "agent_settled"})
	if exitAfter {
		// Let the settle event drain before simulating a pi crash.
		time.Sleep(50 * time.Millisecond)
		os.Exit(1)
	}
}

func appendLine(path string, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer func() { _ = f.Close() }()
	_, _ = f.Write(append(b, '\n'))
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
