// Command fakepi is a minimal pi RPC stand-in for the gateway's tests. It
// implements just enough of pi's command/event surface: get_state reports a
// session file, prompt runs a short synthetic turn, and everything else gets a
// success response.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/tigersoldier/pi-gateway/internal/protocol"
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
		}
	}
	if sessionFile == "" {
		dir := sessionDir
		if dir == "" {
			dir = os.TempDir()
		}
		sessionFile = filepath.Join(dir, fmt.Sprintf("fakepi-%d.jsonl", os.Getpid()))
	}
	_ = os.MkdirAll(filepath.Dir(sessionFile), 0o755)
	if f, err := os.OpenFile(sessionFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644); err == nil {
		_ = f.Close()
	}

	s := &state{
		sessionFile:    sessionFile,
		sessionName:    sessionName,
		model:          model,
		provider:       provider,
		thinking:       thinking,
		events:         envInt("FAKEPI_TURN_EVENTS", 3),
		delay:          time.Duration(envInt("FAKEPI_TURN_DELAY_MS", 15)) * time.Millisecond,
		rejectPrompt:   os.Getenv("FAKEPI_REJECT_PROMPT") != "",
		uiRequest:      os.Getenv("FAKEPI_UI_REQUEST") != "",
		exitAfterFirst: os.Getenv("FAKEPI_EXIT_AFTER_FIRST_TURN") != "",
	}
	codec := protocol.NewCodec(os.Stdin, os.Stdout)
	for {
		raw, err := codec.Read()
		if err != nil {
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
	sessionName    string
	model          string
	provider       string
	thinking       string
	messageCount   int
	streaming      bool
	events         int
	delay          time.Duration
	rejectPrompt   bool
	uiRequest      bool
	exitAfterFirst bool
	turns          int
}

func (s *state) handle(c *protocol.Codec, msg map[string]any) {
	typ, _ := msg["type"].(string)
	id, _ := msg["id"].(string)
	respond := func(command string, success bool, data any, errMsg string) {
		obj := map[string]any{"type": "response", "command": command, "success": success}
		if id != "" {
			obj["id"] = id
		}
		if data != nil {
			obj["data"] = data
		}
		if !success {
			obj["error"] = errMsg
		}
		_ = c.WriteJSON(obj)
	}

	switch typ {
	case "get_state":
		s.mu.Lock()
		data := map[string]any{
			"model":                 map[string]any{"id": s.model, "provider": s.provider, "name": "Fake Model"},
			"thinkingLevel":         s.thinking,
			"isStreaming":           s.streaming,
			"isCompacting":          false,
			"steeringMode":          "all",
			"followUpMode":          "one-at-a-time",
			"sessionFile":           s.sessionFile,
			"sessionId":             "fake-session",
			"autoCompactionEnabled": true,
			"messageCount":          s.messageCount,
			"pendingMessageCount":   0,
		}
		if s.sessionName != "" {
			data["sessionName"] = s.sessionName
		}
		s.mu.Unlock()
		respond("get_state", true, data, "")
	case "get_entries":
		respond("get_entries", true, map[string]any{"entries": []any{}, "leafId": nil}, "")
	case "get_messages":
		respond("get_messages", true, map[string]any{"messages": []any{}}, "")
	case "prompt":
		s.handlePrompt(c, id, msg, respond)
	case "steer", "follow_up", "abort", "abort_retry", "abort_bash":
		respond(typ, true, map[string]any{}, "")
	case "clear_queue":
		respond("clear_queue", true, map[string]any{"steering": []any{}, "followUp": []any{}}, "")
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
		}
		data := map[string]any{"name": s.sessionName}
		s.mu.Unlock()
		respond("set_session_name", true, data, "")
	default:
		respond(typ, true, map[string]any{}, "")
	}
}

func (s *state) handlePrompt(c *protocol.Codec, _ string, msg map[string]any,
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
	events, delay := s.events, s.delay
	uiRequest := s.uiRequest
	s.mu.Unlock()

	respond("prompt", true, map[string]any{}, "")
	go s.runTurn(c, msg, events, delay, uiRequest)
}

func (s *state) runTurn(c *protocol.Codec, msg map[string]any, events int, delay time.Duration, uiRequest bool) {
	text, _ := msg["message"].(string)
	emit := func(obj map[string]any) {
		_ = c.WriteJSON(obj)
		if delay > 0 {
			time.Sleep(delay)
		}
	}
	emit(map[string]any{"type": "agent_start"})
	if uiRequest {
		emit(map[string]any{
			"type":    "extension_ui_request",
			"id":      "ui-1",
			"method":  "confirm",
			"title":   "proceed?",
			"message": "fake dialog",
			"timeout": 60000,
		})
	}
	emit(map[string]any{"type": "turn_start"})
	emit(map[string]any{"type": "message_start", "message": map[string]any{"role": "assistant"}})
	for i := 0; i < events; i++ {
		emit(map[string]any{
			"type":  "message_update",
			"delta": fmt.Sprintf("%s[%d]", text, i),
		})
	}
	emit(map[string]any{"type": "message_end", "message": map[string]any{
		"role":    "assistant",
		"content": []any{map[string]any{"type": "text", "text": "echo: " + text}},
	}})
	emit(map[string]any{"type": "turn_end"})
	emit(map[string]any{"type": "agent_end"})

	s.mu.Lock()
	s.streaming = false
	s.messageCount++
	s.turns++
	file := s.sessionFile
	exitAfter := s.exitAfterFirst && s.turns >= 1
	s.mu.Unlock()
	emit(map[string]any{"type": "agent_settled"})
	appendLine(file, map[string]any{"type": "message", "message": map[string]any{"role": "user", "content": text}})
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
