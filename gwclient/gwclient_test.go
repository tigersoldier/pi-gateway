package gwclient_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/tigersoldier/pi-gateway/gwclient"
	"github.com/tigersoldier/pi-gateway/internal/gwtest"
	"github.com/tigersoldier/pi-gateway/protocol"
)

const testToken = "gwclient-test-token"

// dial starts a fake-pi daemon and connects a client to it. mutate may adjust
// the client configuration before Dial.
func dial(t *testing.T, mutate func(*gwclient.Config)) *gwclient.Client {
	t.Helper()
	addr, _ := gwtest.StartDaemon(t, testToken, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cfg := gwclient.Config{
		Addr:     addr,
		Token:    testToken,
		LiveOnly: true,
		Name:     "test-bot",
		Kind:     "bot",
	}
	if mutate != nil {
		mutate(&cfg)
	}
	c, err := gwclient.Dial(ctx, cfg)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestDialHandshake(t *testing.T) {
	c := dial(t, nil)
	if c.ClientID() == "" {
		t.Fatal("welcome carried no clientId")
	}
	for _, capability := range []string{protocol.CapObserve, protocol.CapPrompt, protocol.CapAdmin} {
		if !c.Can(capability) {
			t.Errorf("granted = %v, missing %s", c.Granted(), capability)
		}
	}
	if c.Session() != nil {
		t.Fatalf("unbound client reports session %+v", c.Session())
	}
	if err := c.Err(); err != nil {
		t.Fatalf("Err() = %v", err)
	}
}

func TestNewSessionPromptAndCatalog(t *testing.T) {
	c := dial(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	dir := t.TempDir()
	sess, err := c.NewSession(ctx, gwclient.NewSessionRequest{
		Name: "bot-session",
		Cwd:  dir,
		Tags: map[string]string{"bot": "test"},
	})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if sess.Path == "" {
		t.Fatal("NewSession returned an empty path")
	}
	if got := c.Session(); got == nil || got.Path != sess.Path {
		t.Fatalf("Session() = %+v, want path %s", got, sess.Path)
	}

	if _, err := c.Prompt(ctx, "hello"); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	text, err := waitForAssistantText(c, 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if text != "echo: hello" {
		t.Fatalf("assistant text = %q", text)
	}

	rows, err := c.ListSessions(ctx, gwclient.SessionFilter{Live: true})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	var row *gwclient.SessionRow
	for i := range rows {
		if rows[i].Path == sess.Path {
			row = &rows[i]
			break
		}
	}
	if row == nil {
		t.Fatalf("session %s missing from catalog %+v", sess.Path, rows)
	}
	if !row.Live {
		t.Errorf("catalog row is not live: %+v", row)
	}
	if row.CreatedBy == nil || row.CreatedBy.Tags["bot"] != "test" {
		t.Errorf("catalog row lost creator tags: %+v", row.CreatedBy)
	}
}

func TestDoReturnsResponseError(t *testing.T) {
	c := dial(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := c.Do(ctx, "gw_new_session", map[string]any{
		"piArgs": []string{"--definitely-not-a-pi-flag"},
	})
	var rerr *gwclient.ResponseError
	if !errors.As(err, &rerr) {
		t.Fatalf("err = %v (%T), want *ResponseError", err, err)
	}
	if rerr.Code != protocol.CodeBadFrame {
		t.Fatalf("code = %q, want %q", rerr.Code, protocol.CodeBadFrame)
	}
	if resp == nil || resp.Success {
		t.Fatalf("response = %+v, want the failing response", resp)
	}
}

func TestCloseClosesEvents(t *testing.T) {
	c := dial(t, nil)
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	timeout := time.After(10 * time.Second)
	for {
		select {
		case _, ok := <-c.Events():
			if !ok {
				if !gwclient.IsClosed(c.Err()) {
					t.Fatalf("Err() = %v after Close", c.Err())
				}
				return
			}
		case <-timeout:
			t.Fatal("Events() was not closed by Close")
		}
	}
}

func TestExtensionDialogRoundTrip(t *testing.T) {
	t.Setenv("FAKEPI_UI_REQUEST", "1")
	c := dial(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if _, err := c.NewSession(ctx, gwclient.NewSessionRequest{Cwd: t.TempDir()}); err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if _, err := c.Prompt(ctx, "needs a dialog"); err != nil {
		t.Fatalf("Prompt: %v", err)
	}

	dialogID := ""
	deadline := time.After(15 * time.Second)
	for dialogID == "" {
		select {
		case ev, ok := <-c.Events():
			if !ok {
				t.Fatalf("events closed: %v", c.Err())
			}
			if ev.Type != "extension_ui_request" {
				continue
			}
			if method := ev.Field("method"); method != "confirm" {
				t.Fatalf("dialog method = %q", method)
			}
			dialogID = ev.Field("id")
		case <-deadline:
			t.Fatal("no extension_ui_request arrived")
		}
	}

	if err := c.RespondUI(ctx, dialogID, map[string]any{"confirmed": true}); err != nil {
		t.Fatalf("RespondUI: %v", err)
	}
	deadline = time.After(15 * time.Second)
	for {
		select {
		case ev, ok := <-c.Events():
			if !ok {
				t.Fatalf("events closed: %v", c.Err())
			}
			if ev.Type == "response" && ev.Field("id") == dialogID {
				return
			}
		case <-deadline:
			t.Fatal("the dialog answer never reached pi")
		}
	}
}

// waitForAssistantText drains Events until a message_end with text arrives.
func waitForAssistantText(c *gwclient.Client, timeout time.Duration) (string, error) {
	deadline := time.After(timeout)
	for {
		select {
		case ev, ok := <-c.Events():
			if !ok {
				return "", fmt.Errorf("events closed: %w", c.Err())
			}
			if ev.Type != "message_end" {
				continue
			}
			var msg struct {
				Message struct {
					Content []struct {
						Type string `json:"type"`
						Text string `json:"text"`
					} `json:"content"`
				} `json:"message"`
			}
			if err := ev.Unmarshal(&msg); err != nil {
				return "", err
			}
			for _, part := range msg.Message.Content {
				if part.Type == "text" {
					return part.Text, nil
				}
			}
		case <-deadline:
			return "", errors.New("timed out waiting for message_end")
		}
	}
}
