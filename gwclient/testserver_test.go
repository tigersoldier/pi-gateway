package gwclient_test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/tigersoldier/pi-gateway/protocol"
)

// testGateway is a small wire-level fixture for the public Go client tests.
// It deliberately does not start the production gateway or pi: server-runtime
// behavior is covered by the TypeScript suite, while this fixture checks the
// Go client's framing, correlation, and helpers without retaining a second
// gateway implementation as test infrastructure.
type testGateway struct {
	listener net.Listener
	root     string

	mu       sync.Mutex
	nextPeer int
	nextSess int
	sessions map[string]*testSession
	peers    map[*testPeer]struct{}
	conns    map[net.Conn]struct{}

	acceptWG  sync.WaitGroup
	handlerWG sync.WaitGroup
	stopOnce  sync.Once
	uiAnswers chan json.RawMessage
}

type testSession struct {
	path, name, id, cwd string
	piArgs              []string
	createdBy           *protocol.ClientRef
	modelProvider       string
	modelID             string
	thinkingLevel       string
	lastText            string
	seq                 uint64
	history             [][]byte
	peers               map[*testPeer]struct{}
}

type testPeer struct {
	gateway  *testGateway
	conn     net.Conn
	writeMu  sync.Mutex
	clientID string
	tags     map[string]string
	session  *testSession
}

func startTestDaemon(t *testing.T) (string, *testGateway) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for client test gateway: %v", err)
	}
	g := &testGateway{
		listener:  listener,
		root:      t.TempDir(),
		sessions:  make(map[string]*testSession),
		peers:     make(map[*testPeer]struct{}),
		conns:     make(map[net.Conn]struct{}),
		uiAnswers: make(chan json.RawMessage, 16),
	}
	g.acceptWG.Add(1)
	go g.accept()
	t.Cleanup(g.stop)
	return listener.Addr().String(), g
}

func (g *testGateway) accept() {
	defer g.acceptWG.Done()
	for {
		conn, err := g.listener.Accept()
		if err != nil {
			return
		}
		g.mu.Lock()
		g.conns[conn] = struct{}{}
		g.mu.Unlock()
		g.handlerWG.Add(1)
		go func() {
			defer g.handlerWG.Done()
			g.serve(conn)
		}()
	}
}

func (g *testGateway) stop() {
	g.stopOnce.Do(func() {
		_ = g.listener.Close()
		g.acceptWG.Wait()
		g.mu.Lock()
		conns := make([]net.Conn, 0, len(g.conns))
		for conn := range g.conns {
			conns = append(conns, conn)
		}
		g.mu.Unlock()
		for _, conn := range conns {
			_ = conn.Close()
		}
		g.handlerWG.Wait()
	})
}

func (g *testGateway) serve(conn net.Conn) {
	defer conn.Close()
	defer func() {
		g.mu.Lock()
		delete(g.conns, conn)
		for peer := range g.peers {
			if peer.conn == conn {
				if peer.session != nil {
					delete(peer.session.peers, peer)
				}
				delete(g.peers, peer)
				break
			}
		}
		g.mu.Unlock()
	}()

	reader := bufio.NewReader(conn)
	raw, err := reader.ReadBytes('\n')
	if err != nil {
		return
	}
	var hello protocol.Hello
	if err := json.Unmarshal(raw, &hello); err != nil || hello.Type != "gw_hello" {
		return
	}

	peer := &testPeer{gateway: g, conn: conn, tags: hello.Client.Tags}
	var session *testSession
	var welcomeSession *protocol.SessionRef
	var attachError string
	var headSeq uint64
	g.mu.Lock()
	g.nextPeer++
	peer.clientID = fmt.Sprintf("test-client-%d", g.nextPeer)
	if hello.Session != "" {
		session = g.findSessionLocked(hello.Session)
		if session == nil {
			session = g.createSessionLocked(hello.Session, "", filepath.Dir(hello.Session), hello.PiArgs, peer.clientID, peer.tags)
		}
		if len(session.piArgs) > 0 && len(hello.PiArgs) > 0 && !reflect.DeepEqual(session.piArgs, hello.PiArgs) {
			attachError = protocol.CodeSpawnParamConflict
		} else {
			peer.session = session
			session.peers[peer] = struct{}{}
			headSeq = session.seq
			welcomeSession = &protocol.SessionRef{Path: session.path, Name: session.name, ID: session.id}
		}
	}
	g.peers[peer] = struct{}{}
	g.mu.Unlock()

	welcome := map[string]any{
		"type": "gw_welcome", "protocol": protocol.Version,
		"clientId": peer.clientID, "kind": hello.Client.Kind,
		"granted": hello.Client.Capabilities, "features": protocol.GatewayFeatures(),
		"concurrency": "queue", "session": welcomeSession,
		"headSeq": headSeq, "oldestSeq": uint64(1),
		"resyncRequired": false, "piState": map[string]any{},
		"turn": map[string]any{"state": "idle", "queued": 0}, "clients": []any{},
	}
	if hello.Resume != nil && session != nil && attachError == "" {
		if hello.Resume.SinceSeq > headSeq {
			welcome["resyncRequired"] = true
		}
	}
	if err := peer.send(welcome); err != nil {
		return
	}
	if attachError != "" {
		_ = peer.send(map[string]any{
			"type": "gw_error", "code": attachError,
			"message": "requested spawn parameters conflict with the session",
		})
		return
	}
	if hello.Resume != nil && session != nil {
		g.sendResume(peer, session, hello.Resume.SinceSeq, hello.Resume.SinceSeq > headSeq)
	}

	for {
		raw, err = reader.ReadBytes('\n')
		if err != nil {
			return
		}
		var frame map[string]any
		if json.Unmarshal(raw, &frame) != nil {
			continue
		}
		typ := stringField(frame, "type")
		switch typ {
		case "gw_ping":
			_ = peer.send(map[string]any{"type": "gw_pong"})
		case "gw_bye":
			return
		case "extension_ui_response":
			select {
			case g.uiAnswers <- append(json.RawMessage(nil), raw...):
			default:
			}
		default:
			g.command(peer, frame)
		}
	}
}

func (g *testGateway) sendResume(peer *testPeer, session *testSession, since uint64, resync bool) {
	g.mu.Lock()
	head := session.seq
	history := append([][]byte(nil), session.history...)
	leaf := fmt.Sprintf("entry-%d", head)
	g.mu.Unlock()
	if resync {
		_ = peer.send(map[string]any{
			"type": "gw_snapshot", "headSeq": head, "leafId": leaf,
			"piState": map[string]any{}, "entries": []any{map[string]any{"id": "entry-1"}},
		})
		return
	}
	for _, event := range history {
		var fields map[string]any
		if json.Unmarshal(event, &fields) == nil && numberField(fields, "gw_seq") > since {
			_ = peer.sendRaw(event)
		}
	}
	_ = peer.send(map[string]any{"type": "gw_replay_done", "headSeq": head})
}

func (g *testGateway) command(peer *testPeer, frame map[string]any) {
	command, id := stringField(frame, "type"), stringField(frame, "id")
	fields, _ := frame["fields"].(map[string]any)
	if fields == nil {
		fields = frame
	}
	var data any = map[string]any{}
	var failureCode, failureMessage string

	switch command {
	case "gw_new_session":
		if args := stringSlice(fields["piArgs"]); len(args) > 0 && args[0] == "--definitely-not-a-pi-flag" {
			failureCode, failureMessage = protocol.CodeBadFrame, "unsupported pi argument"
			break
		}
		cwd, name := stringField(fields, "cwd"), stringField(fields, "name")
		tags, _ := fields["tags"].(map[string]any)
		creatorTags := make(map[string]string, len(tags))
		for key, value := range tags {
			if text, ok := value.(string); ok {
				creatorTags[key] = text
			}
		}
		g.mu.Lock()
		session := g.createSessionLocked("", name, cwd, stringSlice(fields["piArgs"]), peer.clientID, creatorTags)
		g.bindLocked(peer, session)
		data = map[string]any{"path": session.path, "name": session.name, "sessionId": session.id}
		g.mu.Unlock()
	case "gw_list_sessions":
		g.mu.Lock()
		rows := make([]protocol.SessionRow, 0, len(g.sessions))
		for _, session := range g.sessions {
			rows = append(rows, protocol.SessionRow{
				Path: session.path, Name: session.name, ID: session.id, Cwd: session.cwd,
				Live: true, CreatedBy: session.createdBy,
			})
		}
		g.mu.Unlock()
		data = map[string]any{"sessions": rows}
	case "get_commands":
		data = map[string]any{"commands": []any{
			map[string]any{"name": "hello", "description": "say hello", "source": "extension"},
			map[string]any{"name": "fix-tests", "source": "prompt", "location": "project"},
			map[string]any{"name": "skill:brave-search", "source": "skill", "location": "user"},
		}}
	case "switch_session":
		key := stringField(fields, "sessionPath")
		g.mu.Lock()
		session := g.findSessionLocked(key)
		if session == nil {
			failureCode, failureMessage = protocol.CodeUnknownSession, "unknown session"
		} else {
			g.bindLocked(peer, session)
		}
		g.mu.Unlock()
		data = map[string]any{"cancelled": false}
	case "get_state":
		g.mu.Lock()
		data = g.stateLocked(peer.session)
		g.mu.Unlock()
	case "prompt", "steer", "follow_up":
		message := stringField(fields, "message")
		g.mu.Lock()
		session := peer.session
		if session == nil {
			session = g.createSessionLocked("", "", "", nil, peer.clientID, peer.tags)
			g.bindLocked(peer, session)
		}
		images, _ := fields["images"].([]any)
		text := "echo: " + message
		if len(images) > 0 {
			text += fmt.Sprintf(" [images: %d]", len(images))
		}
		session.lastText = text
		g.mu.Unlock()
		if command == "prompt" {
			if strings.Contains(message, "needs a dialog") {
				_ = peer.send(map[string]any{
					"type": "extension_ui_request", "id": "dialog-1",
					"method": "confirm", "message": "continue?",
				})
			}
			g.emit(session, map[string]any{"type": "gw_turn", "state": "running", "turnId": "turn-1"})
			g.emit(session, map[string]any{
				"type": "message_end", "message": map[string]any{
					"role": "assistant", "content": []any{map[string]any{"type": "text", "text": text}},
				},
			})
			g.emit(session, map[string]any{"type": "gw_turn", "state": "settled", "turnId": "turn-1"})
		}
	case "bash":
		bashID := id
		output := "fake:" + stringField(fields, "command")
		_ = peer.send(map[string]any{"type": "bash_execution_update", "id": bashID, "delta": output + "\n"})
		data = map[string]any{"output": output, "exitCode": 0, "cancelled": false, "truncated": false}
	case "gw_reload_session":
		data = map[string]any{"reloaded": true}
	case "gw_stop_session", "gw_delete_session":
		g.mu.Lock()
		session := peer.session
		if requested := stringField(fields, "session"); requested != "" {
			session = g.findSessionLocked(requested)
		}
		if session == nil {
			failureCode, failureMessage = protocol.CodeUnknownSession, "unknown session"
			g.mu.Unlock()
			break
		}
		peers := make([]*testPeer, 0, len(session.peers))
		for attached := range session.peers {
			peers = append(peers, attached)
		}
		path := session.path
		g.mu.Unlock()

		if command == "gw_delete_session" {
			g.emit(session, map[string]any{"type": "gw_session_state", "state": protocol.SessionStateDeleted})
		} else {
			g.emit(session, map[string]any{"type": "gw_session_state", "state": "stopped", "reason": protocol.StopReasonRequested})
		}
		g.mu.Lock()
		for _, attached := range peers {
			g.bindLocked(attached, nil)
		}
		if command == "gw_delete_session" {
			delete(g.sessions, path)
		}
		g.mu.Unlock()

		fileDeleted := false
		if command == "gw_delete_session" {
			if err := os.Remove(path); err == nil || os.IsNotExist(err) {
				fileDeleted = true
			}
		}
		data = map[string]any{
			"path": path, "piStopped": true, "fileDeleted": fileDeleted,
			"unbound": peer == findPeer(peers, peer),
		}
	case "set_session_name":
		g.mu.Lock()
		if peer.session != nil {
			peer.session.name = stringField(fields, "name")
		}
		g.mu.Unlock()
	case "set_model":
		g.mu.Lock()
		if peer.session != nil {
			peer.session.modelProvider = stringField(fields, "provider")
			peer.session.modelID = stringField(fields, "modelId")
		}
		g.mu.Unlock()
	case "cycle_model":
		data = map[string]any{"model": map[string]any{"id": "fake-two", "name": "Fake Two", "provider": "fake"}}
	case "get_available_models":
		data = map[string]any{"models": []any{
			map[string]any{"id": "fake-one", "name": "Fake One", "provider": "fake"},
			map[string]any{"id": "fake-two", "name": "Fake Two", "provider": "fake"},
		}}
	case "set_thinking_level":
		g.mu.Lock()
		if peer.session != nil {
			peer.session.thinkingLevel = stringField(fields, "level")
		}
		g.mu.Unlock()
	case "cycle_thinking_level":
		data = map[string]any{"level": "high"}
		g.mu.Lock()
		if peer.session != nil {
			peer.session.thinkingLevel = "high"
		}
		g.mu.Unlock()
	case "get_available_thinking_levels":
		data = map[string]any{"levels": []string{"off", "low", "high"}}
	case "set_steering_mode", "set_follow_up_mode":
		data = map[string]any{"mode": stringField(fields, "mode")}
	case "compact":
		data = map[string]any{"customInstructions": stringField(fields, "customInstructions")}
	case "get_session_stats":
		data = map[string]any{"messageCount": 1}
	case "export_html":
		data = map[string]any{"path": stringField(fields, "outputPath")}
	case "get_fork_messages":
		data = map[string]any{"messages": []any{map[string]any{"entryId": "e1", "text": "first prompt"}}}
	case "get_last_assistant_text":
		g.mu.Lock()
		text := ""
		if peer.session != nil {
			text = peer.session.lastText
		}
		g.mu.Unlock()
		data = map[string]any{"text": text}
	case "get_entries", "get_tree":
		g.mu.Lock()
		leaf := "entry-1"
		if peer.session != nil && peer.session.seq > 0 {
			leaf = fmt.Sprintf("entry-%d", peer.session.seq)
		}
		g.mu.Unlock()
		if command == "get_entries" {
			data = map[string]any{"leafId": leaf, "entries": []any{map[string]any{"id": "entry-1"}}}
		} else {
			data = map[string]any{"leafId": leaf, "tree": []any{}}
		}
	case "gw_ping", "gw_bye":
		return
	case "set_auto_compaction", "set_auto_retry", "abort_retry", "abort_bash":
		// The wrapper tests only require a successful pi-shaped response.
	default:
		failureCode, failureMessage = protocol.CodeNotSupported, "unsupported test command"
	}

	response := map[string]any{
		"type": "response", "id": id, "command": command,
		"success": failureCode == "", "data": data,
	}
	if failureCode != "" {
		response["code"] = failureCode
		response["error"] = failureMessage
	}
	_ = peer.send(response)
}

func findPeer(peers []*testPeer, target *testPeer) *testPeer {
	for _, peer := range peers {
		if peer == target {
			return peer
		}
	}
	return nil
}

func (g *testGateway) stateLocked(session *testSession) map[string]any {
	state := map[string]any{"isStreaming": false}
	if session == nil {
		return state
	}
	state["sessionFile"] = session.path
	state["sessionName"] = session.name
	state["sessionId"] = session.id
	state["model"] = map[string]any{"id": session.modelID, "provider": session.modelProvider}
	state["thinkingLevel"] = session.thinkingLevel
	return state
}

func (g *testGateway) createSessionLocked(path, name, cwd string, args []string, clientID string, tags map[string]string) *testSession {
	g.nextSess++
	if path == "" {
		base := name
		if base == "" {
			base = "session"
		}
		if cwd == "" {
			cwd = g.root
		}
		path = filepath.Join(cwd, fmt.Sprintf("%s-%d.jsonl", base, g.nextSess))
	}
	if name == "" {
		name = strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	}
	if cwd == "" {
		cwd = filepath.Dir(path)
	}
	_ = os.MkdirAll(filepath.Dir(path), 0o700)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		_ = os.WriteFile(path, nil, 0o600)
	}
	creator := &protocol.ClientRef{ClientID: clientID, Name: "test-bot", Kind: "bot", Tags: cloneTags(tags)}
	session := &testSession{
		path: path, name: name, id: fmt.Sprintf("s_%d", g.nextSess), cwd: cwd,
		piArgs: append([]string(nil), args...), createdBy: creator,
		modelProvider: "fake", modelID: "fake-one", thinkingLevel: "off",
		peers: make(map[*testPeer]struct{}),
	}
	g.sessions[path] = session
	return session
}

func (g *testGateway) findSessionLocked(key string) *testSession {
	if session := g.sessions[key]; session != nil {
		return session
	}
	for _, session := range g.sessions {
		if session.name == key {
			return session
		}
	}
	return nil
}

func (g *testGateway) bindLocked(peer *testPeer, session *testSession) {
	if peer.session != nil {
		delete(peer.session.peers, peer)
	}
	peer.session = session
	if session != nil {
		session.peers[peer] = struct{}{}
	}
}

func (g *testGateway) emit(session *testSession, event map[string]any) {
	g.mu.Lock()
	session.seq++
	event["gw_seq"] = session.seq
	raw, err := json.Marshal(event)
	if err != nil {
		g.mu.Unlock()
		return
	}
	session.history = append(session.history, append([]byte(nil), raw...))
	peers := make([]*testPeer, 0, len(session.peers))
	for peer := range session.peers {
		peers = append(peers, peer)
	}
	g.mu.Unlock()
	for _, peer := range peers {
		_ = peer.sendRaw(raw)
	}
}

func (peer *testPeer) send(value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return peer.sendRaw(raw)
}

func (peer *testPeer) sendRaw(raw []byte) error {
	peer.writeMu.Lock()
	defer peer.writeMu.Unlock()
	data := append(append([]byte(nil), raw...), '\n')
	for len(data) > 0 {
		n, err := peer.conn.Write(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

func stringField(fields map[string]any, key string) string {
	if value, ok := fields[key].(string); ok {
		return value
	}
	return ""
}

func numberField(fields map[string]any, key string) uint64 {
	if value, ok := fields[key].(float64); ok && value > 0 {
		return uint64(value)
	}
	return 0
}

func stringSlice(value any) []string {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	result := make([]string, 0, len(items))
	for _, item := range items {
		if text, ok := item.(string); ok {
			result = append(result, text)
		}
	}
	return result
}

func cloneTags(tags map[string]string) map[string]string {
	if len(tags) == 0 {
		return nil
	}
	out := make(map[string]string, len(tags))
	for key, value := range tags {
		out[key] = value
	}
	return out
}
