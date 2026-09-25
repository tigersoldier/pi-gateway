package session

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/tigersoldier/pi-gateway/internal/protocol"
)

// PiConfig describes how to spawn pi in RPC mode for one session.
type PiConfig struct {
	Bin  string
	Args []string
	Dir  string
	Logf func(format string, args ...any)
}

// PiProcess owns exactly one `pi --mode rpc` child. All writes go through
// Send; stdout records are delivered on Events in order.
//
// Writes are serialized by a dedicated goroutine with a bounded queue so the
// session actor never blocks on pi's stdin; a stalled pipe fails the command
// instead of wedging the daemon.
type PiProcess struct {
	cfg   PiConfig
	codec *protocol.Codec

	events chan protocol.Record
	done   chan struct{}
	writes chan []byte

	mu       sync.Mutex
	proc     *os.Process
	stdin    io.WriteCloser
	waitErr  error
	writeErr error
	exitCode int
}

// StartPi spawns the child and starts reader, writer, and stderr-drain
// goroutines. The executable is operator configuration (the daemon's --pi
// flag), never client input; it is resolved through PATH and started
// directly, without a shell.
func StartPi(cfg PiConfig) (*PiProcess, error) {
	if cfg.Bin == "" {
		cfg.Bin = "pi"
	}
	resolved, err := exec.LookPath(cfg.Bin)
	if err != nil {
		return nil, fmt.Errorf("find pi binary %q: %w", cfg.Bin, err)
	}

	stdinR, stdinW, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("pi stdin pipe: %w", err)
	}
	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		closeAll(stdinR, stdinW)
		return nil, fmt.Errorf("pi stdout pipe: %w", err)
	}
	stderrR, stderrW, err := os.Pipe()
	if err != nil {
		closeAll(stdinR, stdinW, stdoutR, stdoutW)
		return nil, fmt.Errorf("pi stderr pipe: %w", err)
	}

	// A nil Env inherits the daemon's environment.
	proc, err := os.StartProcess(resolved, append([]string{resolved}, cfg.Args...), &os.ProcAttr{
		Dir:   cfg.Dir,
		Files: []*os.File{stdinR, stdoutW, stderrW},
	})
	if err != nil {
		closeAll(stdinR, stdinW, stdoutR, stdoutW, stderrR, stderrW)
		return nil, fmt.Errorf("start pi: %w", err)
	}
	// The parent keeps only the ends it talks to.
	closeAll(stdinR, stdoutW, stderrW)

	p := &PiProcess{
		cfg:    cfg,
		codec:  protocol.NewCodec(stdoutR, nil),
		events: make(chan protocol.Record, 256),
		done:   make(chan struct{}),
		writes: make(chan []byte, 256),
		proc:   proc,
		stdin:  stdinW,
	}
	go p.readLoop()
	go p.writeLoop()
	go p.stderrLoop(stderrR)
	go p.wait()
	return p, nil
}

func closeAll(files ...*os.File) {
	for _, f := range files {
		_ = f.Close()
	}
}

// Events delivers pi stdout records. Closed when stdout is exhausted.
func (p *PiProcess) Events() <-chan protocol.Record { return p.events }

// Done is closed after the child has exited and been reaped.
func (p *PiProcess) Done() <-chan struct{} { return p.done }

// WaitErr returns the process wait error, if any. Valid after Done.
func (p *PiProcess) WaitErr() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.waitErr
}

// ExitCode returns the process exit status. Valid after Done.
func (p *PiProcess) ExitCode() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.exitCode
}

// Send queues one encoded pi command for the writer goroutine.
func (p *PiProcess) Send(raw []byte) error {
	line := make([]byte, 0, len(raw)+1)
	line = append(line, raw...)
	line = append(line, '\n')
	if err := p.writeError(); err != nil {
		return err
	}
	select {
	case p.writes <- line:
		return nil
	case <-p.done:
		return errors.New("pi process is not running")
	default:
	}
	// The writer is behind: pi is not draining stdin. Wait briefly, then fail
	// the command rather than blocking the session actor indefinitely.
	t := time.NewTimer(2 * time.Second)
	defer t.Stop()
	select {
	case p.writes <- line:
		return nil
	case <-p.done:
		return errors.New("pi process is not running")
	case <-t.C:
		return errors.New("pi stdin is not draining")
	}
}

func (p *PiProcess) writeLoop() {
	for {
		select {
		case line := <-p.writes:
			p.mu.Lock()
			stdin := p.stdin
			p.mu.Unlock()
			if stdin == nil {
				return
			}
			if _, err := stdin.Write(line); err != nil {
				p.setWriteError(err)
				return
			}
		case <-p.done:
			return
		}
	}
}

func (p *PiProcess) setWriteError(err error) {
	p.mu.Lock()
	if p.writeErr == nil {
		p.writeErr = err
	}
	p.mu.Unlock()
}

func (p *PiProcess) writeError() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.writeErr
}

func (p *PiProcess) readLoop() {
	defer close(p.events)
	for {
		raw, err := p.codec.Read()
		if err != nil {
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
	if p.cfg.Logf == nil {
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
				i := bytes.IndexByte(buf, '\n')
				if i < 0 {
					break
				}
				p.cfg.Logf("pi: %s", buf[:i])
				buf = buf[i+1:]
			}
			if len(buf) > 64<<10 {
				buf = buf[:0]
			}
		}
		if err != nil {
			if len(buf) > 0 {
				p.cfg.Logf("pi: %s", buf)
			}
			return
		}
	}
}

func (p *PiProcess) wait() {
	state, err := p.proc.Wait()
	p.mu.Lock()
	p.waitErr = err
	if state != nil {
		p.exitCode = state.ExitCode()
	}
	p.mu.Unlock()
	close(p.done)
}

// Close shuts pi down: stdin is closed so pi can exit gracefully, then the
// process is killed if it does not exit within grace.
func (p *PiProcess) Close(grace time.Duration) {
	p.mu.Lock()
	stdin := p.stdin
	p.stdin = nil
	p.mu.Unlock()
	if stdin != nil {
		_ = stdin.Close()
	}
	select {
	case <-p.done:
		return
	case <-time.After(grace):
	}
	if p.proc != nil {
		_ = p.proc.Kill()
	}
	<-p.done
}
