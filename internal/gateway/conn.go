package gateway

import (
	"encoding/json"
	"io"
	"log"
	"sync"
	"time"

	"github.com/example/pi-gateway/internal/protocol"
)

// stream is the transport-agnostic connection surface. net.Conn satisfies it,
// and stdioStream adapts os.Stdin/os.Stdout for the `connect` bridge.
type stream interface {
	io.Reader
	io.Writer
	io.Closer
}

// deadlineStream is implemented by net.Conn; stdio does not support deadlines.
type deadlineStream interface {
	SetReadDeadline(t time.Time) error
}

func setReadDeadline(st stream, d time.Duration) {
	if ds, ok := st.(deadlineStream); ok {
		_ = ds.SetReadDeadline(time.Now().Add(d))
	}
}

func clearReadDeadline(st stream) {
	if ds, ok := st.(deadlineStream); ok {
		_ = ds.SetReadDeadline(time.Time{})
	}
}

// stdioStream adapts stdin/stdout to the stream interface.
type stdioStream struct {
	r io.ReadCloser
	w io.Writer
}

func (s stdioStream) Read(p []byte) (int, error)  { return s.r.Read(p) }
func (s stdioStream) Write(p []byte) (int, error) { return s.w.Write(p) }
func (s stdioStream) Close() error                { return s.r.Close() }

// Conn is one client connection. It implements Client and owns the transport,
// the per-connection send queue, and the hub subscription.
type Conn struct {
	id      string
	name    string
	caps    map[string]bool
	session SessionID
	actor   *Actor
	server  *Server

	st    stream
	codec *protocol.Codec
	sub   *Subscriber

	sendCh chan []byte
	closed chan struct{}
	once   sync.Once

	// lossy lets non-terminal deltas be dropped instead of disconnecting.
	lossy bool

	writeMu sync.Mutex
}

func (c *Conn) ID() string                    { return c.id }
func (c *Conn) Name() string                  { return c.name }
func (c *Conn) Capabilities() map[string]bool { return c.caps }

// SendRaw enqueues a direct (targeted) message. Direct messages are not
// droppable: if the client cannot keep up, it is disconnected so it can
// reconnect and resync.
func (c *Conn) SendRaw(raw []byte) {
	select {
	case c.sendCh <- raw:
	case <-c.closed:
	default:
		log.Printf("gateway: client %s send queue full; disconnecting", c.id)
		c.Close()
	}
}

func (c *Conn) SendJSON(v any) {
	raw, err := json.Marshal(v)
	if err != nil {
		log.Printf("gateway: marshal to client %s: %v", c.id, err)
		return
	}
	c.SendRaw(raw)
}

// Close is idempotent.
func (c *Conn) Close() {
	c.once.Do(func() {
		close(c.closed)
		_ = c.st.Close()
		if c.actor != nil {
			c.actor.Hub().Unsubscribe(c.id)
		}
		if c.server != nil {
			c.server.removeConn(c)
			c.server.publishPresence(c, "leave")
		}
	})
}

func (c *Conn) writeRaw(raw []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.codec.WriteRaw(raw)
}

// writer multiplexes direct messages and hub broadcasts onto the transport.
// skipUntil suppresses broadcast records already delivered via replay.
func (c *Conn) writer(skipUntil uint64) {
	defer c.Close()
	for {
		select {
		case <-c.closed:
			return
		case raw := <-c.sendCh:
			if err := c.writeRaw(raw); err != nil {
				return
			}
		case rec, ok := <-c.sub.C():
			if !ok {
				return
			}
			if rec.Seq <= skipUntil {
				continue
			}
			skipUntil = rec.Seq
			// The originator already received this via a direct send.
			if protocol.Field(rec.Raw, "gw_owner") == c.id {
				continue
			}
			if err := c.writeRaw(rec.Raw); err != nil {
				return
			}
		}
	}
}

func (c *Conn) readLoop() {
	defer c.Close()
	for {
		raw, err := c.codec.Read()
		if err != nil {
			return
		}
		rec, err := protocol.DecodeRecord(raw)
		if err != nil {
			c.SendJSON(errorResponse("", "parse", "bad_frame", err.Error(), ""))
			continue
		}
		cmd := command{client: c, localID: rec.ID, raw: raw, typ: rec.Type}
		if !c.actor.Submit(cmd) {
			c.SendJSON(errorResponse(rec.ID, rec.Type, "queue_full", "command queue full", ""))
		}
	}
}

// writeHandshake writes gw_welcome and, unless the client asked for live-only,
// the replay window. It returns the live watermark and whether the client must
// resync via snapshot.
func (c *Conn) writeHandshake(hello helloMessage) (head uint64, resync bool) {
	hub := c.actor.Hub()
	c.sub = hub.Subscribe(c.id, 4096, c.lossy)
	head = hub.HeadSeq()

	var replay []protocol.Record
	replayOK := true
	if !hello.LiveOnly {
		replay, replayOK = hub.Replay(hello.Resume.SinceSeq)
	}
	// A live-only client attaches at the head and never needs a snapshot;
	// its UI rebuilds state via get_state/get_messages/get_entries.
	resync = !hello.LiveOnly && !replayOK

	welcome := map[string]any{
		"type":            "gw_welcome",
		"protocol":        protocol.Version,
		"sessionId":       string(c.session),
		"clientId":        c.id,
		"granted":         capabilityList(c.caps),
		"headSeq":         head,
		"oldestSeq":       hub.OldestSeq(),
		"resyncRequired":  resync,
		"concurrencyMode": string(c.actor.arb.Mode()),
	}
	if idle, _ := c.actor.Snapshot(); idle {
		welcome["piState"] = map[string]any{"isStreaming": false}
	} else {
		welcome["piState"] = map[string]any{"isStreaming": true}
	}
	if owner, live := c.actor.arb.Owner(time.Now()); live {
		welcome["floor"] = map[string]any{"owner": owner}
	}
	if err := c.writeRaw(mustJSON(welcome)); err != nil {
		return head, resync
	}
	if !hello.LiveOnly {
		for _, rec := range replay {
			if err := c.writeRaw(rec.Raw); err != nil {
				return head, resync
			}
		}
	}
	_ = c.writeRaw(mustJSON(map[string]any{"type": "gw_replay_done", "headSeq": head}))
	return head, resync
}

func capabilityList(caps map[string]bool) []string {
	out := make([]string, 0, len(caps))
	for k, v := range caps {
		if v {
			out = append(out, k)
		}
	}
	return out
}
