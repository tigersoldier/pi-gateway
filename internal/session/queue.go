package session

import (
	"strings"

	"github.com/tigersoldier/pi-gateway/protocol"
)

const (
	maxPendingItems          = 64
	maxPendingItemsPerClient = 32
	maxPendingCommandBytes   = 16 << 20
)

// QueuedItem is one daemon-queued prompt or follow-up.
type QueuedItem struct {
	ClientID string
	Kind     string
	Name     string
	LocalID  string
	Type     string // prompt | follow_up
	Raw      []byte // command to forward; the id is namespaced when forwarded
	Message  string

	serializedBytes int64 // size of the received serialized command
}

// Ref returns the author identity carried on gw_queue entries.
func (it QueuedItem) Ref() *protocol.ClientRef {
	return &protocol.ClientRef{ClientID: it.ClientID, Kind: it.Kind, Name: it.Name}
}

// Queue is the daemon-owned per-client tagged FIFO. It is not safe for
// concurrent use; the SessionActor owns it.
type Queue struct {
	items       []QueuedItem
	perClient   map[string]int
	commandByte int64
}

// TryPush appends an item if it fits within the pending queue limits. A
// rejected item leaves the accepted queue and its accounting unchanged.
func (q *Queue) TryPush(it QueuedItem, serializedBytes int64) bool {
	if serializedBytes < 0 ||
		len(q.items) >= maxPendingItems ||
		q.perClient[it.ClientID] >= maxPendingItemsPerClient ||
		serializedBytes > maxPendingCommandBytes-q.commandByte {
		return false
	}

	it.serializedBytes = serializedBytes
	q.items = append(q.items, it)
	if q.perClient == nil {
		q.perClient = make(map[string]int)
	}
	q.perClient[it.ClientID]++
	q.commandByte += serializedBytes
	return true
}

// Pop removes and returns the oldest item.
func (q *Queue) Pop() (QueuedItem, bool) {
	if len(q.items) == 0 {
		return QueuedItem{}, false
	}
	it := q.items[0]
	q.items[0] = QueuedItem{}
	q.items = q.items[1:]
	if len(q.items) == 0 {
		q.items = nil
	}
	q.release(it)
	return it, true
}

// Len is the number of pending items.
func (q *Queue) Len() int { return len(q.items) }

// Clear removes everything and returns the cleared items in order.
func (q *Queue) Clear() []QueuedItem {
	out := q.items
	q.items = nil
	q.perClient = nil
	q.commandByte = 0
	return out
}

func (q *Queue) release(it QueuedItem) {
	q.commandByte -= it.serializedBytes
	if n := q.perClient[it.ClientID] - 1; n == 0 {
		delete(q.perClient, it.ClientID)
	} else {
		q.perClient[it.ClientID] = n
	}
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
