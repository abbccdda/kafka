package cmd

import (
	"context"
	"os"
	"os/signal"
)

// Starts up a signal handler which will cancel the provided context when an interrupt
// is caught.
func withInterruptHandling(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		select {
		case <-c:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, func() {
		signal.Stop(c)
		cancel()
	}
}
