package session

import (
	"strings"

	"github.com/tigersoldier/pi-gateway/internal/protocol"
)

// QueuedItem is one daemon-queued prompt or follow-up.
type QueuedItem struct {
	ClientID string
	Kind     string
	Name     string
	LocalID  string
	Type     string // prompt | follow_up
	Raw      []byte // original command; the id is namespaced when forwarded
	Message  string
}

// Ref returns the author identity carried on gw_queue entries.
func (it QueuedItem) Ref() *protocol.ClientRef {
	return &protocol.ClientRef{ClientID: it.ClientID, Kind: it.Kind, Name: it.Name}
}

// Queue is the daemon-owned per-client tagged FIFO. It is not safe for
// concurrent use; the SessionActor owns it.
type Queue struct {
	items []QueuedItem
}

// Push appends an item.
func (q *Queue) Push(it QueuedItem) {
	q.items = append(q.items, it)
}

// Pop removes and returns the oldest item.
func (q *Queue) Pop() (QueuedItem, bool) {
	if len(q.items) == 0 {
		return QueuedItem{}, false
	}
	it := q.items[0]
	q.items = q.items[1:]
	return it, true
}

// Len is the number of pending items.
func (q *Queue) Len() int { return len(q.items) }

// Clear removes everything and returns the cleared items in order.
func (q *Queue) Clear() []QueuedItem {
	out := q.items
	q.items = nil
	return out
}

// Pending describes the queue for a gw_queue event. Immediate steer
// interjections are never listed.
func (q *Queue) Pending() []protocol.QueuePending {
	out := make([]protocol.QueuePending, 0, len(q.items))
	for _, it := range q.items {
		mode := "followUp"
		if it.Type == "prompt" {
			mode = "followUp" // queued prompts are executed as follow-ups
		}
		out = append(out, protocol.QueuePending{
			ID:      it.LocalID,
			Mode:    mode,
			Author:  it.Ref(),
			Preview: previewOf(it.Message),
		})
	}
	return out
}

func previewOf(msg string) string {
	msg = strings.Join(strings.Fields(msg), " ")
	const max = 120
	if len(msg) <= max {
		return msg
	}
	return msg[:max] + "…"
}
