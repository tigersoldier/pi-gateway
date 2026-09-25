package daemon

import (
	"testing"

	"github.com/tigersoldier/pi-gateway/internal/protocol"
)

func TestLossyTailTracksDroppedRange(t *testing.T) {
	var l lossyTail
	if _, ok := l.frame(); ok {
		t.Fatal("an empty tail must not produce a marker")
	}
	l.note(5)
	l.note(3) // out of order
	l.note(9)
	l.note(0) // sequence 0 is not a log record
	f, ok := l.frame()
	if !ok {
		t.Fatal("the tail should produce a marker")
	}
	if got := protocol.NumField(f.Raw, "oldestSeq"); got != 3 {
		t.Fatalf("oldestSeq = %v, want 3", got)
	}
	if got := protocol.NumField(f.Raw, "headSeq"); got != 9 {
		t.Fatalf("headSeq = %v, want 9", got)
	}
	if _, ok := l.frame(); !ok {
		t.Fatal("frame must not clear the range; the caller clears after queueing")
	}
	l.reset()
	if _, ok := l.frame(); ok {
		t.Fatal("reset must clear the range")
	}
}

func TestConnDroppableOnlyForLossyNonTerminalSessionRecords(t *testing.T) {
	c := &conn{}
	f := outFrame{gen: 1, rec: protocol.Record{Seq: 3, Type: "message_update"}}
	if c.droppable(f) {
		t.Fatal("a non-lossy client must never have records dropped")
	}
	c.allowLossy = true
	if !c.droppable(f) {
		t.Fatal("a lossy client may have non-terminal session records dropped")
	}
	if c.droppable(outFrame{gen: 1, rec: protocol.Record{Seq: 4, Type: "message_end"}}) {
		t.Fatal("terminal records must not be dropped")
	}
	if c.droppable(outFrame{gen: 0, rec: protocol.Record{Seq: 5, Type: "message_update"}}) {
		t.Fatal("connection-scoped frames must not be dropped")
	}
	if c.droppable(outFrame{gen: 1, rec: protocol.Record{Type: "message_update"}}) {
		t.Fatal("records without a sequence cannot be reported in a lag marker")
	}
}
