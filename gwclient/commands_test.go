package gwclient_test

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/tigersoldier/pi-gateway/gwclient"
	"github.com/tigersoldier/pi-gateway/protocol"
)

func TestPingRoundTrip(t *testing.T) {
	c := dial(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

// TestPingTimesOutWithoutPong pins that Ping actually waits: the old
// implementation only sent gw_ping and reported success without an answer.
func TestPingTimesOutWithoutPong(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer func() { _ = conn.Close() }()
				br := bufio.NewReader(conn)
				if _, err := br.ReadBytes('\n'); err != nil { // gw_hello
					return
				}
				welcome, _ := json.Marshal(map[string]any{
					"type": "gw_welcome", "protocol": protocol.Version,
					"clientId": "silent", "concurrency": "queue", "granted": []string{},
				})
				if _, err := conn.Write(append(welcome, '\n')); err != nil {
					return
				}
				_, _ = io.Copy(io.Discard, br) // never answer gw_ping
			}(conn)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := gwclient.Dial(ctx, gwclient.Config{
		Addr: ln.Addr().String(), Token: "unused",
		RequestTimeout: 300 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	start := time.Now()
	err = c.Ping(ctx)
	if err == nil {
		t.Fatal("Ping returned nil although the daemon never answered")
	}
	if elapsed := time.Since(start); elapsed < 200*time.Millisecond {
		t.Fatalf("Ping gave up after %v, want it to wait for the pong", elapsed)
	}
}

func TestRuntimeCommandHelpers(t *testing.T) {
	c := dial(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := c.NewSession(ctx, gwclient.NewSessionRequest{Cwd: t.TempDir()}); err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	if _, err := c.SetSessionName(ctx, "renamed"); err != nil {
		t.Fatalf("SetSessionName: %v", err)
	}
	if state, err := c.GetState(ctx); err != nil {
		t.Fatalf("GetState: %v", err)
	} else if state.SessionName != "renamed" {
		t.Fatalf("session name = %q, want renamed", state.SessionName)
	}

	if _, err := c.SetModel(ctx, "fake", "fake-two"); err != nil {
		t.Fatalf("SetModel: %v", err)
	}
	if state, err := c.GetState(ctx); err != nil {
		t.Fatalf("GetState: %v", err)
	} else if state.Model.ID != "fake-two" || state.Model.Provider != "fake" {
		t.Fatalf("model = %+v, want fake/fake-two", state.Model)
	}

	if _, err := c.SetThinkingLevel(ctx, "high"); err != nil {
		t.Fatalf("SetThinkingLevel: %v", err)
	}
	if state, err := c.GetState(ctx); err != nil {
		t.Fatalf("GetState: %v", err)
	} else if state.ThinkingLevel != "high" {
		t.Fatalf("thinking level = %q, want high", state.ThinkingLevel)
	}

	models, err := c.GetAvailableModels(ctx)
	if err != nil {
		t.Fatalf("GetAvailableModels: %v", err)
	}
	if len(models) != 2 || models[0].ID != "fake-one" || models[0].Provider != "fake" || models[1].Name != "Fake Two" {
		t.Fatalf("models = %+v", models)
	}
	if _, err := c.CycleModel(ctx); err != nil {
		t.Fatalf("CycleModel: %v", err)
	}
	level, err := c.CycleThinkingLevel(ctx)
	if err != nil {
		t.Fatalf("CycleThinkingLevel: %v", err)
	}
	if level != "high" {
		t.Fatalf("cycled thinking level = %q, want high", level)
	}
	levels, err := c.GetAvailableThinkingLevels(ctx)
	if err != nil {
		t.Fatalf("GetAvailableThinkingLevels: %v", err)
	}
	if len(levels) != 3 {
		t.Fatalf("levels = %v, want 3", levels)
	}

	checkMode := func(name string, resp *gwclient.Response, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		var out struct {
			Mode string `json:"mode"`
		}
		if err := resp.Decode(&out); err != nil {
			t.Fatalf("%s decode: %v", name, err)
		}
		if out.Mode != "all" {
			t.Fatalf("%s mode = %q, want all", name, out.Mode)
		}
	}
	resp, err := c.SetSteeringMode(ctx, "all")
	checkMode("SetSteeringMode", resp, err)
	resp, err = c.SetFollowUpMode(ctx, "all")
	checkMode("SetFollowUpMode", resp, err)

	resp, err = c.Compact(ctx, "focus on tests")
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	var compact struct {
		CustomInstructions string `json:"customInstructions"`
	}
	if err := resp.Decode(&compact); err != nil {
		t.Fatalf("Compact decode: %v", err)
	}
	if compact.CustomInstructions != "focus on tests" {
		t.Fatalf("compact instructions = %q", compact.CustomInstructions)
	}

	if _, err := c.SetAutoCompaction(ctx, true); err != nil {
		t.Fatalf("SetAutoCompaction: %v", err)
	}
	if _, err := c.SetAutoRetry(ctx, false); err != nil {
		t.Fatalf("SetAutoRetry: %v", err)
	}
	if _, err := c.AbortRetry(ctx); err != nil {
		t.Fatalf("AbortRetry: %v", err)
	}
	if _, err := c.AbortBash(ctx); err != nil {
		t.Fatalf("AbortBash: %v", err)
	}

	stats, err := c.GetSessionStats(ctx)
	if err != nil {
		t.Fatalf("GetSessionStats: %v", err)
	}
	var statsOut struct {
		MessageCount int `json:"messageCount"`
	}
	if err := stats.Decode(&statsOut); err != nil {
		t.Fatalf("GetSessionStats decode: %v", err)
	}

	exportPath := filepath.Join(t.TempDir(), "session.html")
	gotPath, err := c.ExportHTML(ctx, exportPath)
	if err != nil {
		t.Fatalf("ExportHTML: %v", err)
	}
	if gotPath != exportPath {
		t.Fatalf("ExportHTML = %q, want %q", gotPath, exportPath)
	}

	forks, err := c.GetForkMessages(ctx)
	if err != nil {
		t.Fatalf("GetForkMessages: %v", err)
	}
	if len(forks) != 1 || forks[0].EntryID != "e1" || forks[0].Text != "first prompt" {
		t.Fatalf("fork messages = %+v", forks)
	}

	if _, err := c.Prompt(ctx, "hello"); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if _, err := waitForAssistantText(c, 20*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := c.AwaitSettled(ctx); err != nil {
		t.Fatalf("AwaitSettled: %v", err)
	}
	if c.TurnRunning() {
		t.Fatal("TurnRunning = true after AwaitSettled")
	}
	last, err := c.GetLastAssistantText(ctx)
	if err != nil {
		t.Fatalf("GetLastAssistantText: %v", err)
	}
	if last != "echo: hello" {
		t.Fatalf("last assistant text = %q", last)
	}

	page, err := c.GetEntries(ctx, "")
	if err != nil {
		t.Fatalf("GetEntries: %v", err)
	}
	if page.LeafID == "" || c.LeafID() != page.LeafID {
		t.Fatalf("entries leaf = %q, client leaf = %q", page.LeafID, c.LeafID())
	}
	if len(page.Entries) == 0 {
		t.Fatal("GetEntries returned no entries")
	}
	if _, err := c.Tree(ctx); err != nil {
		t.Fatalf("Tree: %v", err)
	}
	if c.LeafID() == "" {
		t.Fatal("Tree did not report a leaf")
	}
}

func TestBashStreamsUpdates(t *testing.T) {
	c := dial(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := c.NewSession(ctx, gwclient.NewSessionRequest{Cwd: t.TempDir()}); err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	var updates []gwclient.BashUpdate
	res, err := c.Bash(ctx, "echo hi", func(u gwclient.BashUpdate) {
		updates = append(updates, u)
	})
	if err != nil {
		t.Fatalf("Bash: %v", err)
	}
	if res.Output != "fake:echo hi" || res.ExitCode != 0 || res.Cancelled || res.Truncated || res.ID == "" {
		t.Fatalf("bash result = %+v", res)
	}
	if len(updates) != 1 {
		t.Fatalf("bash updates = %+v, want exactly the streamed chunk", updates)
	}
	if updates[0].ID != res.ID || updates[0].Delta != "fake:echo hi\n" {
		t.Fatalf("bash update = %+v (result id %s)", updates[0], res.ID)
	}
}

// TestInjectRoundTrip covers the typed Inject helper and the daemon's
// `inject`→`send_message` translation: the response is the client-facing
// command with a queued payload.
func TestInjectRoundTrip(t *testing.T) {
	c := dial(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	display := false
	resp, err := c.Inject(ctx, gwclient.InjectRequest{
		CustomType: "gwclient/test",
		Content:    "the retry logic is off limits",
		Display:    &display,
		DeliverAs:  "nextTurn",
		DedupeKey:  "gwclient:1",
	})
	if err != nil {
		t.Fatalf("Inject: %v", err)
	}
	if resp.Command != "inject" {
		t.Fatalf("response command = %q, want inject", resp.Command)
	}
	var out struct {
		Queued bool `json:"queued"`
	}
	if err := resp.Decode(&out); err != nil || !out.Queued {
		t.Fatalf("inject data = %s (%v), want queued:true", resp.Data, err)
	}
	if !c.HasFeature(protocol.FeatureInject) {
		t.Fatalf("daemon does not advertise %s: %v", protocol.FeatureInject, c.Features())
	}
}
