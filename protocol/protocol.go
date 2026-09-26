// Package protocol implements strict JSONL framing for the gateway protocol
// and pi's RPC protocol, plus the small set of gateway control messages that
// wrap pi's messages.
//
// Framing rule (inherited from pi): records are delimited by LF only. A
// trailing CR is stripped. U+2028/U+2029 are valid inside JSON strings and
// must NOT be treated as record separators.
package protocol

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
)

// Version is the gateway protocol version negotiated in gw_hello/gw_welcome.
const Version = 1

// MaxFrameBytes bounds a single JSONL record. pi allows large base64 image
// payloads; keep this generous but finite.
const MaxFrameBytes = 16 << 20

// InternalIDPrefix marks command ids the daemon uses for its own pi calls.
// pi only ever sees ids with this prefix or a client namespace prefix.
const InternalIDPrefix = "gwint:"

var (
	ErrFrameTooLarge = errors.New("protocol: frame exceeds max size")
	ErrNoWriter      = errors.New("protocol: codec has no writer")
)

// Record is one decoded JSONL frame plus gateway routing metadata.
type Record struct {
	Seq  uint64 // gateway-assigned per-session sequence (0 = not sequenced)
	Raw  []byte // wire frame (may already carry gw_* fields)
	Type string // value of "type"
	ID   string // value of "id" ("" if absent)

	// Owner is the client ID a frame is addressed to, derived from the
	// namespaced "id" pi echoed. Responses are delivered only to their owner;
	// other owner-tagged frames are broadcast with gw_owner set. Empty means
	// broadcast.
	Owner string
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
	rec := Record{Raw: raw, Type: probe.Type, ID: probe.ID}
	if owner, _ := SplitNamespaceID(probe.ID); owner != "" && owner != probe.ID {
		rec.Owner = owner
	} else if strings.HasPrefix(probe.ID, InternalIDPrefix) {
		rec.Owner = "" // internal ids are consumed by the actor
	}
	return rec, nil
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

// IsInternalID reports whether id belongs to a daemon-internal pi call.
func IsInternalID(id string) bool { return strings.HasPrefix(id, InternalIDPrefix) }

// RewriteCommand returns a copy of raw with its "id" and "type" fields set.
// An empty id removes the field; an empty typ leaves the type unchanged.
func RewriteCommand(raw []byte, id, typ string) ([]byte, error) {
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("protocol: rewrite command: %w", err)
	}
	if id == "" {
		delete(obj, "id")
	} else {
		obj["id"] = id
	}
	if typ != "" {
		obj["type"] = typ
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

// RewriteID returns a copy of raw with its "id" field replaced. An empty id
// removes the field.
func RewriteID(raw []byte, id string) ([]byte, error) {
	return RewriteCommand(raw, id, "")
}

// RestoreID removes a client namespace prefix from raw's id. It is a no-op
// when the id is not namespaced for clientID.
func RestoreID(raw []byte, clientID string) []byte {
	prefix := clientID + ":"
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return raw
	}
	id, _ := obj["id"].(string)
	switch {
	case strings.HasPrefix(id, prefix):
		obj["id"] = strings.TrimPrefix(id, prefix)
	case id == clientID:
		delete(obj, "id")
	default:
		return raw
	}
	out, err := json.Marshal(obj)
	if err != nil {
		return raw
	}
	return out
}

// StripGatewayFields returns a copy of raw with every gw_* key removed,
// yielding a pristine pi frame. It is used by the bridge for raw pi peers.
func StripGatewayFields(raw []byte) []byte {
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

// IsGatewayType reports whether typ is a gateway-only message. Raw/compat
// clients never receive these.
func IsGatewayType(typ string) bool {
	return strings.HasPrefix(typ, "gw_")
}

// NamespaceID builds the globally unique id forwarded to pi.
func NamespaceID(clientID, localID string) string {
	if localID == "" {
		return clientID
	}
	return clientID + ":" + localID
}

// SplitNamespaceID reverses NamespaceID. For an id with no namespace it
// returns ("", id); for "c_1" it returns ("c_1", "").
func SplitNamespaceID(id string) (clientID, localID string) {
	if i := strings.IndexByte(id, ':'); i >= 0 {
		return id[:i], id[i+1:]
	}
	if id == "" {
		return "", ""
	}
	return "", id
}

// DataField returns raw's "data" object as JSON, or nil.
func DataField(raw []byte) json.RawMessage {
	var obj struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil
	}
	return obj.Data
}

// LagFrame builds a gw_lag marker naming the range of records that was
// dropped for a lossy client. It carries no gw_seq: it is a control signal,
// not a log record.
func LagFrame(oldestSeq, headSeq uint64) ([]byte, error) {
	return json.Marshal(map[string]any{
		"type":      "gw_lag",
		"oldestSeq": oldestSeq,
		"headSeq":   headSeq,
	})
}

// ImageContent is pi's image attachment shape (docs/rpc.md): base64 data
// plus its MIME type, sent in the `images` array of prompt, steer and
// follow_up. The gateway forwards it unchanged.
type ImageContent struct {
	Type     string `json:"type"`     // always "image"
	Data     string `json:"data"`     // base64-encoded bytes
	MIMEType string `json:"mimeType"` // for example "image/png"
}

// NewImage builds an ImageContent from raw bytes, base64-encoding them.
func NewImage(data []byte, mimeType string) ImageContent {
	return ImageContent{
		Type:     "image",
		Data:     base64.StdEncoding.EncodeToString(data),
		MIMEType: mimeType,
	}
}

// PiState is the subset of pi's get_state payload the gateway depends on.
type PiState struct {
	SessionFile   string `json:"sessionFile"`
	SessionName   string `json:"sessionName"`
	SessionID     string `json:"sessionId"`
	ThinkingLevel string `json:"thinkingLevel"`
	IsStreaming   bool   `json:"isStreaming"`
	Model         struct {
		ID       string `json:"id"`
		Provider string `json:"provider"`
	} `json:"model"`
}

// ParsePiState decodes a get_state response (or a bare state payload).
func ParsePiState(raw []byte) PiState {
	var st PiState
	if err := json.Unmarshal(DataField(raw), &st); err != nil {
		_ = json.Unmarshal(raw, &st)
	}
	return st
}

// BoolField returns a top-level boolean field, or def when absent.
func BoolField(raw []byte, name string, def bool) bool {
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return def
	}
	v, ok := obj[name].(bool)
	if !ok {
		return def
	}
	return v
}

// NumField returns a top-level numeric field, or 0 when absent or not a
// number.
func NumField(raw []byte, name string) float64 {
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return 0
	}
	n, _ := obj[name].(float64)
	return n
}
