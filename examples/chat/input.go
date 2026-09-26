package main

import "strings"

// inputKind classifies one REPL line.
type inputKind int

const (
	inputPrompt inputKind = iota // text sent to pi, including "/name" commands
	inputSteer
	inputQueue
	inputAbort
	inputCommands
	inputSession
	inputHelp
	inputQuit
	inputUnknown
)

// parseInput classifies one line. Local commands use "!" so that pi's own
// "/name" commands, prompt templates and skills pass through untouched.
func parseInput(line string) (inputKind, string) {
	text := strings.TrimSpace(line)
	if !strings.HasPrefix(text, "!") {
		return inputPrompt, text
	}
	cmd, arg := text[1:], ""
	if i := strings.IndexAny(text, " \t"); i >= 0 {
		cmd, arg = text[1:i], strings.TrimSpace(text[i+1:])
	}
	switch cmd {
	case "steer":
		return inputSteer, arg
	case "queue":
		return inputQueue, arg
	case "abort":
		return inputAbort, arg
	case "commands", "cmds":
		return inputCommands, arg
	case "session":
		return inputSession, arg
	case "help", "?":
		return inputHelp, arg
	case "quit", "exit":
		return inputQuit, arg
	default:
		return inputUnknown, text
	}
}

const helpText = `
input          send a prompt (the daemon queues it while a turn runs)
/name [args]   run a pi command, prompt template or skill
!commands      list this session's commands and skills
!queue <text>  queue a follow-up (runs after the agent settles)
!steer <text>  interject into the running turn
!abort         abort the running turn
!session       show the attached session
!help          this help
!quit          exit (Ctrl-D too; Ctrl-C aborts a turn, again to quit)
`
