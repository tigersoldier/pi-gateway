// Command pi-gateway is the bridge a third-party UI spawns in place of `pi`.
// It is always a bridge: it accepts pi's argv, connects to pi-gatewayd, and
// exposes pristine pi RPC on stdin/stdout. See docs/protocol.md §0.
package main

import (
	"context"
	"os"

	"github.com/tigersoldier/pi-gateway/internal/client"
)

func main() {
	os.Exit(client.Run(context.Background(), client.Options{
		Args:   os.Args[1:],
		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}))
}
