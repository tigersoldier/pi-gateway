package main

import (
	"encoding/json"
	"testing"

	"github.com/tigersoldier/pi-gateway/gwclient"
)

func TestParseInput(t *testing.T) {
	cases := []struct {
		in   string
		kind inputKind
		arg  string
	}{
		{"hello there", inputPrompt, "hello there"},
		{"  padded  ", inputPrompt, "padded"},
		{"/compact", inputPrompt, "/compact"},
		{"/skill:brave-search web search", inputPrompt, "/skill:brave-search web search"},
		{"!steer focus on tests", inputSteer, "focus on tests"},
		{"!queue\tlater", inputQueue, "later"},
		{"!queue", inputQueue, ""},
		{"!abort", inputAbort, ""},
		{"!commands", inputCommands, ""},
		{"!cmds", inputCommands, ""},
		{"!session", inputSession, ""},
		{"!help", inputHelp, ""},
		{"!?", inputHelp, ""},
		{"!quit", inputQuit, ""},
		{"!exit", inputQuit, ""},
		{"!nope", inputUnknown, "!nope"},
		{"   ", inputPrompt, ""},
	}
	for _, tc := range cases {
		kind, arg := parseInput(tc.in)
		if kind != tc.kind || arg != tc.arg {
			t.Errorf("parseInput(%q) = (%d, %q), want (%d, %q)", tc.in, kind, arg, tc.kind, tc.arg)
		}
	}
}

func TestQueued(t *testing.T) {
	if !queued(&gwclient.Response{Data: json.RawMessage(`{"queued":true}`)}) {
		t.Fatal("queued:true was not detected")
	}
	if queued(&gwclient.Response{Data: json.RawMessage(`{"queued":false}`)}) {
		t.Fatal("queued:false was detected as queued")
	}
	if queued(&gwclient.Response{Data: json.RawMessage(`{}`)}) {
		t.Fatal("empty data was detected as queued")
	}
	if queued(nil) {
		t.Fatal("nil response was detected as queued")
	}
}
