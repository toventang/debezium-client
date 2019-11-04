package adapter

import (
	"context"
	"time"
)

func Context(timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout == 0 {
		return context.Background(), func() {}
	}
	return context.WithTimeout(context.Background(), timeout)
}
