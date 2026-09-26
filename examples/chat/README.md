# `chat` — example CLI for the `gwclient` library

An interactive client for a `pi-gatewayd` session, built only on
[`gwclient`](../../gwclient) and [`protocol`](../../protocol). It exists to show
a real integration (a chat bot, a remote shell, a dashboard) how to drive one
session; it is **not** a shipped binary and is not installed by `packaging/`.

```console
$ go run ./examples/chat                       # new session in the current directory
$ go run ./examples/chat --name scratch        # new session with a name
$ go run ./examples/chat --session auth        # attach by session name or file path
$ go run ./examples/chat --pi-arg=--approve    # pi parameters for a new session
$ go run ./examples/chat --thinking --usage --verbose
```

It reads the daemon's token and port from `~/.config/pi-gateway` (or
`--state-dir`), exactly like the bridge. Assistant text goes to **stdout**, so
a transcript can be piped; prompts, tool summaries and status lines go to
**stderr**.

## Input

| Input | Effect | Library call |
| --- | --- | --- |
| `text` | send a prompt; the daemon queues it while a turn runs | `Prompt` |
| `/name [args]` | run a pi command, prompt template or skill | `Prompt` (pi expands `/…`) |
| `!commands` | list the session's commands, templates and skills | `GetCommands` |
| `!queue <text>` | queue a follow-up for after the agent settles | `FollowUp` |
| `!steer <text>` | interject into the running turn | `Steer` |
| `!abort` | abort the running turn (the queue survives) | `Abort` |
| `!session` | print the attached session file/name/id | `Session` |
| `!help`, `!quit` | local help and exit | — |

`Ctrl-D` exits; `Ctrl-C` aborts the running turn and quits when idle. On exit
(or end of piped input) the CLI waits for a submitted turn and the daemon queue
to drain, so `printf 'summarise the repo\n' | go run ./examples/chat` still
prints the answer. A second `Ctrl-C`, or two minutes, skips the wait.

Blocking extension dialogs (`confirm`, `select`, `input`, `editor`) are
answered from the same input stream; `notify` and the fire-and-forget `set*`
methods are printed or ignored. A real integration replaces this with its own
UI — dialogs are the only part of the CLI that is a stand-in.

## What it demonstrates

- **Discovery and handshake** — `gwclient.Dial` with `--server`/`--port`/
  `--state-dir`/`--token-file` and the port-file fallback.
- **Streaming render** — `message_update` text/thinking deltas, tool
  `start`/`update`/`end`, `message_end` errors and usage (`render.go`).
- **One connection, many commands** — `Do` correlates responses by id while
  `Events()` streams; `gw_turn`/`gw_queue` drive the busy/queue status.
- **Queue, steer, abort** — the daemon owns the queue, so a prompt sent during
  a turn comes back as `{"queued":true}` and `follow_up` waits for settle.
- **Commands and skills** — `get_commands` + `/skill:name` prompts.
- **Sessions** — `NewSession` (with `cwd`, `name`, `piArgs`) or
  `SwitchSession` by path or name.

The renderer and the input parser are covered by `render_test.go` and
`input_test.go` with synthetic frames and lines, so the example is exercised by
`go test ./examples/chat/` without a daemon.
