package gateway

import (
	"time"
)

// Mode selects how concurrent prompts from multiple clients are arbitrated.
type Mode string

const (
	// ModeExclusive: only the floor holder may start a new prompt.
	ModeExclusive Mode = "exclusive"
	// ModeQueue: any client may prompt; the actor serializes them FIFO.
	ModeQueue Mode = "queue"
	// ModeOwnerOnly: only the designated owner may prompt; others observe.
	ModeOwnerOnly Mode = "owner-only"
	// ModeReadOnly: nobody may prompt or interject.
	ModeReadOnly Mode = "read-only"
)

// Verdict is the arbiter's ruling on a command.
type Verdict int

const (
	Allow Verdict = iota
	Reject
	Queue
)

// Decision is the arbiter's ruling plus an explanation for the client.
type Decision struct {
	Verdict Verdict
	Code    string
	Message string
	Owner   string
}

// Arbiter owns the floor (turn lease) and the prompt queue policy. It is not
// safe for concurrent use; the SessionActor calls it from its single loop.
type Arbiter struct {
	mode     Mode
	owner    string
	expires  time.Time
	leaseTTL time.Duration
	queue    []string // client IDs with queued prompts (queue mode)
}

func NewArbiter(mode Mode, leaseTTL time.Duration) *Arbiter {
	if leaseTTL <= 0 {
		leaseTTL = 120 * time.Second
	}
	if mode == "" {
		mode = ModeExclusive
	}
	return &Arbiter{mode: mode, leaseTTL: leaseTTL}
}

func (a *Arbiter) Mode() Mode      { return a.mode }
func (a *Arbiter) QueueDepth() int { return len(a.queue) }

func (a *Arbiter) SetMode(mode Mode) {
	a.mode = mode
	if mode == ModeReadOnly {
		a.owner = ""
		a.queue = nil
	}
}

// Owner returns the current floor holder ("" if none) and whether the lease
// is still live.
func (a *Arbiter) Owner(now time.Time) (string, bool) {
	if a.owner == "" {
		return "", false
	}
	if !a.expires.IsZero() && now.After(a.expires) {
		a.owner = ""
		a.queue = nil
		return "", false
	}
	return a.owner, true
}

// TakeTurn explicitly acquires the floor. steal requires control capability
// and is validated by the caller.
func (a *Arbiter) TakeTurn(client string, ttl time.Duration, now time.Time) Decision {
	if a.mode == ModeReadOnly {
		return Decision{Verdict: Reject, Code: "read_only", Message: "session is read-only"}
	}
	if owner, live := a.Owner(now); live && owner != client {
		return Decision{Verdict: Reject, Code: "turn_held", Message: "turn held by " + owner, Owner: owner}
	}
	if ttl <= 0 {
		ttl = a.leaseTTL
	}
	a.owner = client
	a.expires = now.Add(ttl)
	return Decision{Verdict: Allow}
}

func (a *Arbiter) ReleaseTurn(client string) Decision {
	if a.owner == "" || a.owner == client {
		a.owner = ""
		a.expires = time.Time{}
		return Decision{Verdict: Allow}
	}
	return Decision{Verdict: Reject, Code: "turn_held", Message: "turn held by " + a.owner, Owner: a.owner}
}

// AdmitInterjection rules on steer/follow_up, which pi queues natively and
// which never require the floor.
func (a *Arbiter) AdmitInterjection() Decision {
	if a.mode == ModeReadOnly {
		return Decision{Verdict: Reject, Code: "read_only", Message: "session is read-only"}
	}
	return Decision{Verdict: Allow}
}

// AdmitPrompt rules on a prompt. idle reports whether pi is currently settled.
func (a *Arbiter) AdmitPrompt(client string, idle bool, now time.Time) Decision {
	switch a.mode {
	case ModeReadOnly:
		return Decision{Verdict: Reject, Code: "read_only", Message: "session is read-only"}
	case ModeOwnerOnly:
		if a.owner != "" && a.owner != client {
			return Decision{Verdict: Reject, Code: "turn_held", Message: "turn held by " + a.owner, Owner: a.owner}
		}
		a.owner = client
		a.expires = now.Add(a.leaseTTL)
		return Decision{Verdict: Allow}
	case ModeQueue:
		if !idle {
			a.queue = append(a.queue, client)
			return Decision{Verdict: Queue}
		}
		return Decision{Verdict: Allow}
	default: // ModeExclusive
		owner, live := a.Owner(now)
		if live && owner != client {
			return Decision{Verdict: Reject, Code: "turn_held", Message: "turn held by " + owner, Owner: owner}
		}
		if !idle {
			// Same client prompting mid-turn: pi requires streamingBehavior.
			// Let the caller convert to steer/follow_up or reject.
			return Decision{Verdict: Reject, Code: "busy", Message: "agent is streaming; use steer or follow_up", Owner: owner}
		}
		a.owner = client
		a.expires = now.Add(a.leaseTTL)
		return Decision{Verdict: Allow}
	}
}

// Settle clears the floor after agent_settled and returns the next queued
// client ("" if none).
func (a *Arbiter) Settle() (next string) {
	a.owner = ""
	a.expires = time.Time{}
	if len(a.queue) == 0 {
		return ""
	}
	next = a.queue[0]
	a.queue = a.queue[1:]
	a.owner = next
	a.expires = time.Now().Add(a.leaseTTL)
	return next
}

// AbortAll clears all pending prompts, e.g. after a crash.
func (a *Arbiter) AbortAll() {
	a.owner = ""
	a.expires = time.Time{}
	a.queue = nil
}
