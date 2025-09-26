package dbos

import (
	"testing"
	"time"
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
			got := backoffWithJitter(testcase.retryAttempt)

			if got < testcase.wantMin || got > testcase.wantMax {
				t.Errorf("Should be between %v and %v, got=%v, attempt=%v",
					testcase.wantMin, testcase.wantMax, got, testcase.retryAttempt)
			}
		})
	}
}
