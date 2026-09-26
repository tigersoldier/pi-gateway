package gwclient

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestNoteTurnWakesAwaitSettled exercises the turn latch directly, without a
// daemon: a wait must block while a turn runs, wake on settle, return
// immediately when idle, and surface the connection error when the client
// dies mid-turn.
func TestNoteTurnWakesAwaitSettled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c := &Client{turnRunning: true, turnWait: make(chan struct{}), closed: make(chan struct{})}

	done := make(chan error, 1)
	go func() { done <- c.AwaitSettled(ctx) }()
	select {
	case err := <-done:
		t.Fatalf("AwaitSettled returned (%v) while a turn was running", err)
	case <-time.After(50 * time.Millisecond):
	}

	c.noteTurn(false)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("AwaitSettled = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("AwaitSettled did not wake on settle")
	}
	if c.TurnRunning() {
		t.Fatal("TurnRunning = true after settle")
	}

	if err := c.AwaitSettled(ctx); err != nil {
		t.Fatalf("AwaitSettled on an idle client = %v", err)
	}

	boom := errors.New("connection lost")
	c.noteTurn(true)
	c.mu.Lock()
	c.err = boom
	c.mu.Unlock()
	close(c.closed)
	if err := c.AwaitSettled(ctx); !errors.Is(err, boom) {
		t.Fatalf("AwaitSettled on a dead connection = %v, want %v", err, boom)
	}
}
