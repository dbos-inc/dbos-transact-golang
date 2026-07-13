package dbos

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"
)

var backoffWithJitterTestcases = []struct {
	name         string
	retryAttempt int
	wantMin      time.Duration
	wantMax      time.Duration
}{
	{
		name:         "first retry attempt (0)",
		retryAttempt: 0,
		wantMin:      750 * time.Millisecond,
		wantMax:      1250 * time.Millisecond,
	},
	{
		name:         "second retry attempt (1)",
		retryAttempt: 1,
		wantMin:      1500 * time.Millisecond,
		wantMax:      2500 * time.Millisecond,
	},
	{
		name:         "ninth retry attempt (8)",
		retryAttempt: 8,
		wantMin:      90 * time.Second,
		wantMax:      150 * time.Second,
	},
	{
		name:         "exceeds max retries",
		retryAttempt: 10,
		wantMin:      90 * time.Second,
		wantMax:      150 * time.Second,
	},
}

func TestBackoffWithJitter(t *testing.T) {
	for _, testcase := range backoffWithJitterTestcases {
		t.Run(testcase.name, func(t *testing.T) {
			// delayFor is 1-based; the listener loop counts attempts from 0 and
			// passes retryAttempt+1, so mirror that here.
			got := sysdb.ConnectionRetryBackoff.DelayFor(testcase.retryAttempt + 1)

			if got < testcase.wantMin || got > testcase.wantMax {
				t.Errorf("Should be between %v and %v, got=%v, attempt=%v",
					testcase.wantMin, testcase.wantMax, got, testcase.retryAttempt)
			}
		})
	}
}

// TestRetryWithResultContextError guards the contract that retryWithResult
// surfaces the context error (so callers can detect it via errors.Is) when the
// context is cancelled while a retryable operation is in flight.
func TestRetryWithResultContextError(t *testing.T) {
	// A retryable error mirroring what the DB layer wraps an in-flight network
	// failure as (postgresDialect.IsRetryable treats io.EOF as retryable).
	retryableErr := fmt.Errorf("failed to query workflow status: %w", io.EOF)

	t.Run("CanceledDuringBackoff", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := sysdb.RetryWithResult(ctx, func() (int, error) {
			return 0, retryableErr
		})

		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got: %v", err)
		}
	})

	t.Run("DeadlineExceededDuringBackoff", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Hour))
		defer cancel()

		_, err := sysdb.RetryWithResult(ctx, func() (int, error) {
			return 0, retryableErr
		})

		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected context.DeadlineExceeded, got: %v", err)
		}
	})

	t.Run("NonRetryableErrorPreserved", func(t *testing.T) {
		sentinel := errors.New("boom")
		_, err := sysdb.RetryWithResult(context.Background(), func() (int, error) {
			return 0, sentinel
		})

		if !errors.Is(err, sentinel) {
			t.Errorf("expected the original error to be preserved, got: %v", err)
		}
	})
}
