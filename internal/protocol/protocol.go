// Package protocol implements strict JSONL framing and the small set of
// gateway control messages that wrap pi's RPC protocol.
//
// Framing rule (inherited from pi): records are delimited by LF only. A
// trailing CR is stripped. U+2028/U+2029 are valid inside JSON strings and
// must NOT be treated as record separators.
package protocol

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
)

// Version is the gateway protocol version negotiated in gw_hello/gw_welcome.
const Version = 1

// MaxFrameBytes bounds a single JSONL record. pi allows large base64 image
// payloads; keep this generous but finite.
const MaxFrameBytes = 16 << 20

var (
	ErrFrameTooLarge = errors.New("protocol: frame exceeds max size")
	ErrNoWriter      = errors.New("protocol: codec has no writer")
)

// Record is one decoded JSONL frame plus gateway routing metadata.
type Record struct {
	Seq  uint64 // gateway-assigned global sequence (0 = not sequenced)
	Raw  []byte // gateway view of the frame (may include gw_* fields)
	Type string // value of "type"
	ID   string // value of "id" ("" if absent)
}

// DecodeRecord extracts the routing fields from a raw frame.
func DecodeRecord(raw []byte) (Record, error) {
	var probe struct {
		Type string `json:"type"`
		ID   string `json:"id"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return Record{}, fmt.Errorf("protocol: decode frame: %w", err)
	}
	return Record{Raw: raw, Type: probe.Type, ID: probe.ID}, nil
}

// Codec reads and writes strict JSONL. Reads use LF-only framing.
type Codec struct {
	r   *bufio.Reader
	w   io.Writer
	mu  sync.Mutex
	max int
}

func NewCodec(r io.Reader, w io.Writer) *Codec {
	return &Codec{
		r:   bufio.NewReaderSize(r, 64<<10),
		w:   w,
		max: MaxFrameBytes,
	}
}

// Read returns the next frame. It handles frames larger than the internal
// buffer and strips a single trailing CR. At EOF, a final unterminated frame
// is still returned.
func (c *Codec) Read() ([]byte, error) {
	var buf []byte
	for {
		chunk, err := c.r.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) {
			buf = append(buf, chunk...)
			if len(buf) > c.max {
				return nil, ErrFrameTooLarge
			}
			continue
		}
		if err != nil {
			if errors.Is(err, io.EOF) && (len(chunk) > 0 || len(buf) > 0) {
				return trimEOL(append(buf, chunk...)), nil
			}
			return nil, err
		}
		buf = append(buf, chunk...)
		if len(buf) > c.max {
			return nil, ErrFrameTooLarge
		}
		return trimEOL(buf), nil
	}
}

func trimEOL(b []byte) []byte {
	b = bytes.TrimSuffix(b, []byte("\n"))
	b = bytes.TrimSuffix(b, []byte("\r"))
	return b
}

// WriteRaw writes one frame followed by LF. Safe for concurrent use.
func (c *Codec) WriteRaw(raw []byte) error {
	if c.w == nil {
		return ErrNoWriter
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(raw) == 0 || raw[len(raw)-1] != '\n' {
		raw = append(append(make([]byte, 0, len(raw)+1), raw...), '\n')
	}
	_, err := c.w.Write(raw)
	return err
}

// WriteJSON marshals v and writes it as one frame.
func (c *Codec) WriteJSON(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.WriteRaw(b)
}

// Stamp returns a copy of raw with the given gateway fields added.
func Stamp(raw []byte, fields map[string]any) ([]byte, error) {
	if len(fields) == 0 {
		return raw, nil
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("protocol: stamp: %w", err)
	}
	for k, v := range fields {
		obj[k] = v
	}
	return json.Marshal(obj)
}

// RewriteID returns a copy of raw with its "id" field replaced.
func RewriteID(raw []byte, id string) ([]byte, error) {
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("protocol: rewrite id: %w", err)
	}
	if id == "" {
		delete(obj, "id")
	} else {
		obj["id"] = id
	}
	return json.Marshal(obj)
}

// Field returns a top-level string field of raw ("" if absent).
func Field(raw []byte, name string) string {
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return ""
	}
	s, _ := obj[name].(string)
	return s
}

// IsGatewayType reports whether typ is a gateway-only message. Raw/compat
// clients never receive these.
func IsGatewayType(typ string) bool {
	return len(typ) > 3 && typ[0] == 'g' && typ[1] == 'w' && typ[2] == '_'
}

// NamespaceID builds the globally unique id forwarded to pi.
func NamespaceID(clientID, localID string) string {
	if localID == "" {
		return clientID
	}
	return clientID + ":" + localID
}

// SplitNamespaceID reverses NamespaceID.
func SplitNamespaceID(id string) (clientID, localID string) {
	for i := 0; i < len(id); i++ {
		if id[i] == ':' {
			return id[:i], id[i+1:]
		}
	}
	return id, ""
}
