package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/example/pi-gateway/internal/protocol"
)

// ClientOptions configures the `connect` bridge: a separate client process
// that dials a gateway server and presents raw pi RPC to a local peer
// (a third-party UI on stdio, or another process on TCP).
type ClientOptions struct {
	ServerAddr   string
	SessionID    string
	Name         string
	Capabilities []string
	// Replay forwards replayed history to the raw peer. Default false: attach
	// live-only and let the pi client rebuild state via get_state/get_messages/
	// get_entries, exactly as it would against pi itself.
	Replay      bool
	DialTimeout time.Duration
}

// Client is a gateway-protocol client connection used by the bridge.
type BridgeClient struct {
	conn    net.Conn
	codec   *protocol.Codec
	opts    ClientOptions
	welcome json.RawMessage
	writeMu sync.Mutex
}

// Connect dials the server and completes gw_hello/gw_welcome.
func Connect(ctx context.Context, opts ClientOptions) (*BridgeClient, error) {
	if opts.ServerAddr == "" {
		return nil, fmt.Errorf("gateway: server address required")
	}
	if opts.Name == "" {
		opts.Name = "pi-bridge"
	}
	if len(opts.Capabilities) == 0 {
		opts.Capabilities = []string{"observe", "interject", "prompt", "ui", "control"}
	}
	if opts.DialTimeout <= 0 {
		opts.DialTimeout = 10 * time.Second
	}

	d := net.Dialer{Timeout: opts.DialTimeout}
	conn, err := d.DialContext(ctx, "tcp", opts.ServerAddr)
	if err != nil {
		return nil, fmt.Errorf("gateway: dial %s: %w", opts.ServerAddr, err)
	}
	c := &BridgeClient{conn: conn, codec: protocol.NewCodec(conn, conn), opts: opts}
	hello := map[string]any{
		"type":      "gw_hello",
		"protocol":  protocol.Version,
		"sessionId": opts.SessionID,
		"client": map[string]any{
			"name":         opts.Name,
			"capabilities": opts.Capabilities,
		},
		"liveOnly": !opts.Replay,
	}
	if err := c.codec.WriteJSON(hello); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := c.awaitWelcome(); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return c, nil
}

func (c *BridgeClient) awaitWelcome() error {
	for {
		raw, err := c.codec.Read()
		if err != nil {
			return fmt.Errorf("gateway: handshake: %w", err)
		}
		switch protocol.Field(raw, "type") {
		case "gw_welcome":
			c.welcome = append(json.RawMessage(nil), raw...)
			return nil
		case "gw_error":
			return fmt.Errorf("gateway: handshake rejected: %s", protocol.Field(raw, "error"))
		}
	}
}

// Welcome returns the raw gw_welcome frame received during Connect.
func (c *BridgeClient) Welcome() json.RawMessage { return c.welcome }

// Close closes the connection to the server.
func (c *BridgeClient) Close() error { return c.conn.Close() }

// Bridge pumps records between a raw pi peer on st and the gateway server.
//
// Direction server->raw: gw_* messages are consumed by the bridge; pi events
// are stripped of gateway fields and forwarded pristine. Direction raw->server:
// pi commands are forwarded verbatim (the server namespaces and restores ids).
func (c *BridgeClient) Bridge(ctx context.Context, st stream) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer c.Close()
	defer st.Close()

	errc := make(chan error, 2)
	go func() { errc <- c.pumpServerToRaw(st) }()
	go func() { errc <- c.pumpRawToServer(st) }()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errc:
		return err
	}
}

func (c *BridgeClient) pumpServerToRaw(st stream) error {
	out := protocol.NewCodec(st, st)
	for {
		line, err := c.codec.Read()
		if err != nil {
			return err
		}
		typ := protocol.Field(line, "type")
		if protocol.IsGatewayType(typ) {
			// Replay records are pi events; forward them only on request.
			if typ == "gw_replay" && c.opts.Replay {
				if err := out.WriteRaw(stripGatewayFields(line)); err != nil {
					return err
				}
			}
			continue
		}
		// Skip copies broadcast for other clients (e.g. remote bash output).
		if protocol.Field(line, "gw_owner") != "" {
			continue
		}
		if err := out.WriteRaw(stripGatewayFields(line)); err != nil {
			return err
		}
	}
}

func (c *BridgeClient) pumpRawToServer(st stream) error {
	in := protocol.NewCodec(st, st)
	for {
		line, err := in.Read()
		if err != nil {
			return err
		}
		c.writeMu.Lock()
		err = c.codec.WriteRaw(line)
		c.writeMu.Unlock()
		if err != nil {
			return err
		}
	}
}

// stripGatewayFields removes every gw_* key, yielding a pristine pi frame.
func stripGatewayFields(raw []byte) []byte {
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return raw
	}
	changed := false
	for k := range obj {
		if strings.HasPrefix(k, "gw_") {
			delete(obj, k)
			changed = true
		}
	}
	if !changed {
		return raw
	}
	out, err := json.Marshal(obj)
	if err != nil {
		return raw
	}
	return out
}

// StdioStream returns a stream backed by os.Stdin/os.Stdout.
func StdioStream() stream { return stdioStream{r: os.Stdin, w: os.Stdout} }
