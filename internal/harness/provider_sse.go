package harness

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
)

type providerSSELine struct {
	line string
	err  error
	done bool
}

func scanProviderSSE(ctx context.Context, body io.Reader, idleTimeout time.Duration, handle func(string) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	lines := make(chan providerSSELine, 1)
	go func() {
		scanner := bufio.NewScanner(body)
		scanner.Buffer(make([]byte, 4096), 4<<20)
		for scanner.Scan() {
			select {
			case lines <- providerSSELine{line: scanner.Text()}:
			case <-ctx.Done():
				return
			}
		}
		select {
		case lines <- providerSSELine{err: scanner.Err(), done: true}:
		case <-ctx.Done():
		}
	}()

	var timer *time.Timer
	var timeout <-chan time.Time
	if idleTimeout > 0 {
		timer = time.NewTimer(idleTimeout)
		timeout = timer.C
		defer timer.Stop()
	}
	reset := func() {
		if timer == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(idleTimeout)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout:
			return &ProviderError{Code: "TIMEOUT", Message: fmt.Sprintf("provider stream idle timeout after %s", idleTimeout)}
		case next := <-lines:
			if next.done {
				if next.err == nil {
					return nil
				}
				var netErr net.Error
				if errors.As(next.err, &netErr) {
					return &ProviderError{Code: "TRANSPORT", Message: next.err.Error(), Err: next.err}
				}
				return next.err
			}
			line := strings.TrimSpace(next.line)
			if line != "" && !strings.HasPrefix(line, ":") {
				reset()
			}
			if err := handle(line); err != nil {
				return err
			}
		}
	}
}
