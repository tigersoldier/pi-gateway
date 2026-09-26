package gwclient_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/tigersoldier/pi-gateway/gwclient"
)

func TestTypedEventDecoders(t *testing.T) {
	turn := gwclient.Event{
		Type: "gw_turn",
		Raw:  json.RawMessage(`{"type":"gw_turn","state":"settled","turnId":"t_2"}`),
	}
	gotTurn, err := turn.Turn()
	if err != nil || gotTurn.State != "settled" || gotTurn.TurnID != "t_2" {
		t.Fatalf("Turn() = %+v, %v", gotTurn, err)
	}

	queue := gwclient.Event{
		Type: "gw_queue",
		Raw:  json.RawMessage(`{"type":"gw_queue","pending":[{"id":"p1","mode":"followUp","preview":"hi"}]}`),
	}
	gotQueue, err := queue.Queue()
	if err != nil || len(gotQueue.Pending) != 1 || gotQueue.Pending[0].Preview != "hi" {
		t.Fatalf("Queue() = %+v, %v", gotQueue, err)
	}

	state := gwclient.Event{
		Type: "gw_session_state",
		Raw:  json.RawMessage(`{"type":"gw_session_state","state":"crashed","reason":"exit"}`),
	}
	gotState, err := state.SessionState()
	if err != nil || gotState.State != "crashed" || gotState.Reason != "exit" {
		t.Fatalf("SessionState() = %+v, %v", gotState, err)
	}

	presence := gwclient.Event{
		Type: "gw_presence",
		Raw:  json.RawMessage(`{"type":"gw_presence","event":"join","client":{"clientId":"c_1","kind":"ui"}}`),
	}
	gotPresence, err := presence.Presence()
	if err != nil || gotPresence.Event != "join" || gotPresence.Client.ClientID != "c_1" {
		t.Fatalf("Presence() = %+v, %v", gotPresence, err)
	}

	changed := gwclient.Event{
		Type: "gw_state_changed",
		Raw:  json.RawMessage(`{"type":"gw_state_changed","command":"set_model","data":{"id":"m"}}`),
	}
	gotChanged, err := changed.StateChanged()
	if err != nil || gotChanged.Command != "set_model" || string(gotChanged.Data) != `{"id":"m"}` {
		t.Fatalf("StateChanged() = %+v, %v", gotChanged, err)
	}

	replay := gwclient.Event{
		Type: "gw_replay_done",
		Raw:  json.RawMessage(`{"type":"gw_replay_done","headSeq":41}`),
	}
	gotReplay, err := replay.ReplayDone()
	if err != nil || gotReplay.HeadSeq != 41 {
		t.Fatalf("ReplayDone() = %+v, %v", gotReplay, err)
	}

	protoErr := gwclient.Event{
		Type: "gw_error",
		Raw:  json.RawMessage(`{"type":"gw_error","code":"forbidden","message":"nope"}`),
	}
	gotErr, err := protoErr.ErrorEvent()
	if err != nil || gotErr.Code != "forbidden" || gotErr.Message != "nope" {
		t.Fatalf("ErrorEvent() = %+v, %v", gotErr, err)
	}

	snapshot := gwclient.Event{
		Type: "gw_snapshot",
		Raw:  json.RawMessage(`{"type":"gw_snapshot","leafId":"e9","headSeq":7,"entries":[{"id":"e9"}]}`),
	}
	gotSnapshot, err := snapshot.Snapshot()
	if err != nil || gotSnapshot.LeafID != "e9" || gotSnapshot.HeadSeq != 7 || len(gotSnapshot.Entries) != 1 {
		t.Fatalf("Snapshot() = %+v, %v", gotSnapshot, err)
	}

	lag := gwclient.Event{
		Type: "gw_lag",
		Raw:  json.RawMessage(`{"type":"gw_lag","oldestSeq":10,"headSeq":99}`),
	}
	if oldSeq, headSeq := lag.Lag(); oldSeq != 10 || headSeq != 99 {
		t.Fatalf("Lag() = %d, %d", oldSeq, headSeq)
	}
}

func TestUIRequestClassification(t *testing.T) {
	selectReq := gwclient.Event{
		Type: "extension_ui_request",
		Raw: json.RawMessage(`{"type":"extension_ui_request","id":"u1","method":"select",
			"title":"Allow?","options":["Allow","Block"],"timeout":10000}`),
	}
	req, ok := selectReq.UIRequest()
	if !ok {
		t.Fatal("UIRequest() did not decode a select request")
	}
	if !req.Blocking || req.ID != "u1" || req.Title != "Allow?" || req.TimeoutMS != 10000 {
		t.Fatalf("select request = %+v", req)
	}
	if !reflect.DeepEqual(req.Options, []string{"Allow", "Block"}) {
		t.Fatalf("options = %v", req.Options)
	}
	if got := req.Value("Allow"); !reflect.DeepEqual(got, map[string]any{"value": "Allow"}) {
		t.Fatalf("Value() = %v", got)
	}

	confirm := gwclient.Event{
		Type: "extension_ui_request",
		Raw:  json.RawMessage(`{"type":"extension_ui_request","id":"u2","method":"confirm","message":"sure?"}`),
	}
	req, ok = confirm.UIRequest()
	if !ok || !req.Blocking || req.Message != "sure?" {
		t.Fatalf("confirm request = %+v, ok=%v", req, ok)
	}
	if got := req.Confirmed(true); !reflect.DeepEqual(got, map[string]any{"confirmed": true}) {
		t.Fatalf("Confirmed() = %v", got)
	}
	if got := req.Cancelled(); !reflect.DeepEqual(got, map[string]any{"cancelled": true}) {
		t.Fatalf("Cancelled() = %v", got)
	}

	notify := gwclient.Event{
		Type: "extension_ui_request",
		Raw:  json.RawMessage(`{"type":"extension_ui_request","id":"u3","method":"notify","message":"done","notifyType":"warning"}`),
	}
	req, ok = notify.UIRequest()
	if !ok || req.Blocking {
		t.Fatalf("notify request = %+v, ok=%v", req, ok)
	}
	if req.NotifyTypeOrInfo() != "warning" {
		t.Fatalf("NotifyTypeOrInfo() = %q", req.NotifyTypeOrInfo())
	}
	if (gwclient.UIRequest{}).NotifyTypeOrInfo() != "info" {
		t.Fatalf("empty NotifyTypeOrInfo() = %q, want info", (gwclient.UIRequest{}).NotifyTypeOrInfo())
	}

	if !gwclient.BlockingUIMethod("editor") || !gwclient.BlockingUIMethod("input") {
		t.Fatal("editor/input must be blocking methods")
	}
	if gwclient.BlockingUIMethod("setStatus") || gwclient.BlockingUIMethod("setWidget") {
		t.Fatal("setStatus/setWidget must not be blocking")
	}

	if _, ok := (gwclient.Event{Type: "message_end"}).UIRequest(); ok {
		t.Fatal("UIRequest() decoded a non-UI event")
	}
}
