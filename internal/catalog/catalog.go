// Package catalog discovers pi session files and resolves session names.
//
// pi stores sessions under <agent dir>/sessions/<encoded-cwd>/<id>.jsonl
// (docs/session-format.md). The catalog scans those files on demand: there is
// no persisted index (docs/development.md, decision 6), so a name that is
// ambiguous falls back to path semantics and fails with unknown_session or
// ambiguous_session.
package catalog

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Info describes one session file, mirroring the fields pi's own resume
// listing derives (name from the latest session_info entry, title from the
// first user message, activity from message timestamps).
type Info struct {
	Path         string
	ID           string
	Cwd          string
	Name         string
	Title        string
	MessageCount int
	LastActivity time.Time
}

// Scanner finds session files under a set of roots.
type Scanner struct {
	Roots    []string
	MaxFiles int // bound on files parsed per scan; 0 means defaultMaxFiles
}

const (
	defaultMaxFiles = 1000
	// lastLineLimit bounds the tail read used to find a session's durable leaf.
	lastLineLimit = 4 << 20
)

// ErrTooManyCandidates is returned by Resolve when the scan bound was hit
// before every candidate could be inspected.
var ErrTooManyCandidates = errors.New("catalog: too many session files to resolve a name reliably")

// DefaultRoots returns the session directories for the current environment,
// using pi's own precedence: $PI_CODING_AGENT_SESSION_DIR, else
// $PI_CODING_AGENT_DIR/sessions, else ~/.pi/agent/sessions, plus any extra
// roots. Duplicates are removed.
func DefaultRoots(extra ...string) []string {
	var roots []string
	add := func(dir string) {
		if dir == "" {
			return
		}
		for _, r := range roots {
			if r == dir {
				return
			}
		}
		roots = append(roots, dir)
	}
	switch {
	case strings.TrimSpace(os.Getenv("PI_CODING_AGENT_SESSION_DIR")) != "":
		add(strings.TrimSpace(os.Getenv("PI_CODING_AGENT_SESSION_DIR")))
	case strings.TrimSpace(os.Getenv("PI_CODING_AGENT_DIR")) != "":
		add(filepath.Join(strings.TrimSpace(os.Getenv("PI_CODING_AGENT_DIR")), "sessions"))
	default:
		if home, err := os.UserHomeDir(); err == nil {
			add(filepath.Join(home, ".pi", "agent", "sessions"))
		}
	}
	for _, dir := range extra {
		add(dir)
	}
	return roots
}

// candidate is a session file before it has been parsed.
type candidate struct {
	path  string
	mtime time.Time
}

// listCandidates walks the roots and returns *.jsonl files newest first,
// capped at MaxFiles. It does not open any file.
func (s *Scanner) listCandidates() []candidate {
	var out []candidate
	for _, root := range s.Roots {
		entries, err := os.ReadDir(root)
		if err != nil {
			continue
		}
		for _, ent := range entries {
			if ent.IsDir() {
				// pi groups files by encoded cwd: one level of subdirectories.
				sub := filepath.Join(root, ent.Name())
				subEntries, err := os.ReadDir(sub)
				if err != nil {
					continue
				}
				out = append(out, jsonlCandidates(sub, subEntries)...)
				continue
			}
			if c, ok := jsonlCandidate(root, ent); ok {
				out = append(out, c)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].mtime.Equal(out[j].mtime) {
			return out[i].mtime.After(out[j].mtime)
		}
		return out[i].path < out[j].path
	})
	return out
}

func jsonlCandidates(dir string, entries []os.DirEntry) []candidate {
	out := make([]candidate, 0, len(entries))
	for _, ent := range entries {
		if c, ok := jsonlCandidate(dir, ent); ok {
			out = append(out, c)
		}
	}
	return out
}

func jsonlCandidate(dir string, ent os.DirEntry) (candidate, bool) {
	name := ent.Name()
	if ent.IsDir() || !strings.HasSuffix(name, ".jsonl") || strings.HasPrefix(name, ".") {
		return candidate{}, false
	}
	info, err := ent.Info()
	if err != nil {
		return candidate{}, false
	}
	return candidate{path: filepath.Join(dir, name), mtime: info.ModTime()}, true
}

// List returns session info for the newest limit matching files. A limit below
// one lists everything. cwd filters on the header's working directory, live is
// applied by the caller because only the daemon knows what is running.
func (s *Scanner) List(cwd string, limit int) []Info {
	keep := func(Info) bool { return true }
	if cwd != "" {
		keep = func(info Info) bool { return info.Cwd == cwd }
	}
	out, _ := s.parseAll(keep, limit)
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].LastActivity.After(out[j].LastActivity)
	})
	return out
}

// FindName returns the sessions whose recorded name equals name, newest
// first. ErrTooManyCandidates is returned when the scan bound was hit, because
// the result may be incomplete.
func (s *Scanner) FindName(name string) ([]Info, error) {
	out, truncated := s.parseAll(func(info Info) bool {
		return info.Name != "" && info.Name == name
	}, 0)
	if truncated {
		return out, ErrTooManyCandidates
	}
	return out, nil
}

// parseAll parses session files newest first, keeping the entries for which
// keep returns true and stopping after limit kept entries (0 means no limit).
// truncated reports that the scan bound was reached, so the result may be
// incomplete.
func (s *Scanner) parseAll(keep func(Info) bool, limit int) (out []Info, truncated bool) {
	candidates := s.listCandidates()
	if max := s.maxFiles(); len(candidates) > max {
		candidates = candidates[:max]
		truncated = true
	}
	for _, c := range candidates {
		if limit > 0 && len(out) >= limit {
			break
		}
		info, err := Parse(c.path)
		if err != nil {
			continue
		}
		if info.LastActivity.IsZero() {
			info.LastActivity = c.mtime
		}
		if keep(info) {
			out = append(out, info)
		}
	}
	return out, truncated
}

func (s *Scanner) maxFiles() int {
	if s.MaxFiles > 0 {
		return s.MaxFiles
	}
	return defaultMaxFiles
}

// header is the first line of a session file.
type header struct {
	ID        string `json:"id"`
	Cwd       string `json:"cwd"`
	Timestamp string `json:"timestamp"`
}

type entryLine struct {
	Type      string `json:"type"`
	ID        string `json:"id"`
	Name      string `json:"name"`
	Timestamp string `json:"timestamp"`
	Message   *struct {
		Role      string          `json:"role"`
		Timestamp json.RawMessage `json:"timestamp"`
		Content   json.RawMessage `json:"content"`
	} `json:"message"`
}

// Parse reads a session file and returns its catalog info. Files that do not
// start with a session header are not sessions.
func Parse(path string) (Info, error) {
	f, err := os.Open(path)
	if err != nil {
		return Info{}, err
	}
	defer func() { _ = f.Close() }()

	info := Info{Path: path}
	reader := bufio.NewReaderSize(f, 1<<16)
	first := true
	headerTime := time.Time{}
	for {
		line, err := readLine(reader)
		if len(line) > 0 {
			var entry entryLine
			if jsonErr := json.Unmarshal(line, &entry); jsonErr == nil {
				if first {
					var h header
					if jsonErr := json.Unmarshal(line, &h); jsonErr != nil || entry.Type != "session" {
						return Info{}, fmt.Errorf("catalog: %s is not a session file", path)
					}
					info.ID, info.Cwd = h.ID, h.Cwd
					headerTime = parseTime(h.Timestamp)
					first = false
				} else {
					applyEntry(&info, entry)
				}
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return Info{}, err
		}
	}
	if first {
		return Info{}, fmt.Errorf("catalog: %s is empty", path)
	}
	if info.LastActivity.IsZero() {
		info.LastActivity = headerTime
	}
	return info, nil
}

// applyEntry folds one non-header entry into the running info.
func applyEntry(info *Info, entry entryLine) {
	switch entry.Type {
	case "session_info":
		// The latest entry wins, including explicit clears.
		info.Name = strings.TrimSpace(entry.Name)
	case "message":
		info.MessageCount++
		if ts := messageTime(entry); ts.After(info.LastActivity) {
			info.LastActivity = ts
		}
		if info.Title == "" && entry.Message != nil && entry.Message.Role == "user" {
			info.Title = firstText(entry.Message.Content)
		}
	}
}

// messageTime prefers the message timestamp, then the entry timestamp.
func messageTime(entry entryLine) time.Time {
	if entry.Message != nil {
		var n float64
		if err := json.Unmarshal(entry.Message.Timestamp, &n); err == nil && n > 0 {
			return time.UnixMilli(int64(n))
		}
		var s string
		if err := json.Unmarshal(entry.Message.Timestamp, &s); err == nil && s != "" {
			if t := parseTime(s); !t.IsZero() {
				return t
			}
		}
	}
	return parseTime(entry.Timestamp)
}

// firstText extracts the first text block of a message content field, which
// may be a plain string or an array of typed blocks.
func firstText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return strings.TrimSpace(s)
	}
	var blocks []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return ""
	}
	for _, b := range blocks {
		if b.Type == "text" && b.Text != "" {
			return strings.TrimSpace(b.Text)
		}
	}
	return ""
}

func parseTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t
	}
	return time.Time{}
}

// readLine reads one LF-terminated line, tolerating lines longer than the
// reader buffer (session entries can be large).
func readLine(r *bufio.Reader) ([]byte, error) {
	var buf []byte
	for {
		chunk, err := r.ReadSlice('\n')
		buf = append(buf, chunk...)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err == nil || !errors.Is(err, io.EOF) {
			return trimEOL(buf), err
		}
		return trimEOL(buf), io.EOF
	}
}

func trimEOL(b []byte) []byte {
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == '\r') {
		b = b[:len(b)-1]
	}
	return b
}

// HeaderCwd returns the working directory recorded in a session file's
// header, or "" when the file is unreadable or has none. Used to respawn a
// session in its own directory rather than the attaching client's
// (docs/protocol.md §2).
func HeaderCwd(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer func() { _ = f.Close() }()
	line, err := readLine(bufio.NewReaderSize(f, 1<<16))
	if (err != nil && !errors.Is(err, io.EOF)) || len(line) == 0 {
		return ""
	}
	var entry entryLine
	if json.Unmarshal(line, &entry) != nil || entry.Type != "session" {
		return ""
	}
	var h header
	if json.Unmarshal(line, &h) != nil {
		return ""
	}
	return h.Cwd
}

// LeafID returns the id of the last entry in the file: pi's durable leaf.
// It reads from the end of the file, bounded by lastLineLimit.
func LeafID(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	stat, err := f.Stat()
	if err != nil {
		return "", err
	}
	size := stat.Size()
	if size == 0 {
		return "", nil
	}
	read := int64(lastLineLimit)
	if read > size {
		read = size
	}
	buf := make([]byte, read)
	if _, err := f.ReadAt(buf, size-read); err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	text := strings.TrimRight(string(buf), "\r\n")
	// Walk backwards to the last complete line: pi may be mid-append, and the
	// durable leaf is the newest entry that was fully written.
	for text != "" {
		idx := strings.LastIndexByte(text, '\n')
		line := text[idx+1:]
		if line = strings.TrimRight(line, "\r"); line != "" {
			var entry struct {
				ID   string `json:"id"`
				Type string `json:"type"`
			}
			if err := json.Unmarshal([]byte(line), &entry); err == nil {
				if entry.Type == "session" {
					return "", nil
				}
				if entry.ID != "" {
					return entry.ID, nil
				}
			}
		}
		if idx < 0 {
			break
		}
		text = text[:idx]
	}
	return "", nil
}

// Resolve matches a session name against file candidates and live sessions.
// found is false when nothing matches; err is non-nil when the name matched
// more than one session.
func Resolve(name string, files []Info, live []Info) (Info, bool, error) {
	matches := make(map[string]Info)
	for _, info := range files {
		if info.Name != "" && info.Name == name {
			matches[info.Path] = info
		}
	}
	for _, info := range live {
		if info.Name != "" && info.Name == name {
			matches[info.Path] = info
		}
	}
	switch len(matches) {
	case 0:
		return Info{}, false, nil
	case 1:
		for _, info := range matches {
			return info, true, nil
		}
	}
	return Info{}, false, fmt.Errorf("session name %q matches %d sessions", name, len(matches))
}
