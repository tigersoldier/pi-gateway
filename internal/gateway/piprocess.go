package gateway

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"sync"

	"github.com/example/pi-gateway/internal/protocol"
)

// PiConfig describes how to spawn pi in RPC mode for one session.
type PiConfig struct {
	Bin   string
	Args  []string // e.g. ["--mode","rpc","--session","/path/x.jsonl"]
	Dir   string
	Env   []string
	OnLog func(line []byte) // stderr lines (may be nil)
}

// PiProcess owns exactly one `pi --mode rpc` child. All writes go through
// Send; all reads are delivered on Events in stdout order.
type PiProcess struct {
	cfg    PiConfig
	codec  *protocol.Codec
	events chan protocol.Record

	mu    sync.Mutex
	cmd   *exec.Cmd
	stdin io.WriteCloser
	err   error
	done  chan struct{}
}

// StartPi spawns the child and starts the reader and stderr drain goroutines.
func StartPi(ctx context.Context, cfg PiConfig) (*PiProcess, error) {
	if cfg.Bin == "" {
		cfg.Bin = "pi"
	}
	cmd := exec.CommandContext(ctx, cfg.Bin, cfg.Args...)
	cmd.Dir = cfg.Dir
	cmd.Env = cfg.Env

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("pi stdin: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("pi stdout: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("pi stderr: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start pi: %w", err)
	}

	p := &PiProcess{
		cfg:    cfg,
		codec:  protocol.NewCodec(stdout, nil),
		events: make(chan protocol.Record, 256),
		cmd:    cmd,
		stdin:  stdin,
		done:   make(chan struct{}),
	}
	go p.readLoop()
	go p.stderrLoop(stderr)
	return p, nil
}

// Events is closed when pi exits or stdout is exhausted.
func (p *PiProcess) Events() <-chan protocol.Record { return p.events }

// Done is closed when the reader loop terminates.
func (p *PiProcess) Done() <-chan struct{} { return p.done }

func (p *PiProcess) Err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

func (p *PiProcess) setErr(err error) {
	p.mu.Lock()
	if p.err == nil {
		p.err = err
	}
	p.mu.Unlock()
}

// Send writes one already-encoded pi command as a JSONL record.
func (p *PiProcess) Send(raw []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stdin == nil {
		return fmt.Errorf("pi process is not running")
	}
	line := make([]byte, 0, len(raw)+1)
	line = append(line, raw...)
	line = append(line, '\n')
	_, err := p.stdin.Write(line)
	return err
}

func (p *PiProcess) readLoop() {
	defer close(p.events)
	defer close(p.done)
	for {
		raw, err := p.codec.Read()
		if err != nil {
			if err != io.EOF {
				p.setErr(err)
			}
			return
		}
		rec, err := protocol.DecodeRecord(raw)
		if err != nil {
			// pi should never emit malformed JSON; skip rather than stall.
			continue
		}
		p.events <- rec
	}
}

func (p *PiProcess) stderrLoop(stderr io.Reader) {
	if p.cfg.OnLog == nil {
		_, _ = io.Copy(io.Discard, stderr)
		return
	}
	buf := make([]byte, 0, 4096)
	tmp := make([]byte, 4096)
	for {
		n, err := stderr.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
			for {
				i := indexByte(buf, '\n')
				if i < 0 {
					break
				}
				p.cfg.OnLog(buf[:i])
				buf = buf[i+1:]
			}
		}
		if err != nil {
			if len(buf) > 0 {
				p.cfg.OnLog(buf)
			}
			return
		}
	}
}

func indexByte(b []byte, c byte) int {
	for i := range b {
		if b[i] == c {
			return i
		}
	}
	return -1
}

// Close terminates the child. It first closes stdin so pi can shut down
// gracefully, then kills if the context does not cancel it.
func (p *PiProcess) Close() error {
	p.mu.Lock()
	stdin := p.stdin
	cmd := p.cmd
	p.stdin = nil
	p.mu.Unlock()

	if stdin != nil {
		_ = stdin.Close()
	}
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	return cmd.Process.Kill()
}
