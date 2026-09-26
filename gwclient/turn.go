package gwclient

import "context"

// TurnRunning reports whether the bound session has a turn in flight, as last
// reported by gw_turn (and the welcome).
func (c *Client) TurnRunning() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.turnRunning
}

// AwaitSettled blocks until the session has no turn running. It returns
// immediately when the session is already idle, the connection error when the
// connection ends first, and ctx.Err() on cancellation.
func (c *Client) AwaitSettled(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		c.mu.Lock()
		if !c.turnRunning {
			err := c.err
			c.mu.Unlock()
			return err
		}
		wait := c.turnWait
		c.mu.Unlock()
		select {
		case <-wait:
		case <-c.closed:
			return c.Err()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// noteTurn records a turn-state change and wakes AwaitSettled waiters.
func (c *Client) noteTurn(running bool) {
	c.mu.Lock()
	if c.turnRunning != running {
		c.turnRunning = running
		close(c.turnWait)
		c.turnWait = make(chan struct{})
	}
	c.mu.Unlock()
}
