package dbos

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrUnexpectedWorkflowSentinel(t *testing.T) {
	err := models.NewUnexpectedWorkflowError("wf-id", "Workflow already exists with a different name: a, but the provided name is: b")
	assert.True(t, errors.Is(err, ErrUnexpectedWorkflow), "NewUnexpectedWorkflowError should match ErrUnexpectedWorkflow")
	assert.False(t, errors.Is(err, ErrConflictingWorkflowID), "NewUnexpectedWorkflowError should not match ErrConflictingWorkflowID")

	concurrentErr := models.NewWorkflowConflictIDError("wf-id")
	assert.True(t, errors.Is(concurrentErr, ErrConflictingWorkflowID))
	assert.False(t, errors.Is(concurrentErr, ErrUnexpectedWorkflow))
}

func TestInvalidOptionErrors(t *testing.T) {
	ctx, err := NewContext(context.Background(), Config{
		AppName:     "test-invalid-option",
		DatabaseURL: "sqlite:" + filepath.Join(t.TempDir(), "dbos.db"),
	})
	require.NoError(t, err)
	defer Shutdown(ctx, 5*time.Second)

	wf := func(ctx Context, in string) (string, error) { return in, nil }
	RegisterWorkflow(ctx, wf)

	_, err = RunWorkflow(ctx, wf, "in", WithQueue(nil))
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOption)
	var dbosErr *Error
	require.ErrorAs(t, err, &dbosErr)
	assert.Equal(t, ErrorCodeInvalidOption, dbosErr.Code)
	assert.Contains(t, dbosErr.Message, "queue cannot be nil")

	err = SetWorkflowDelay(ctx, "some-id", WithDelayDuration(time.Second), WithDelayUntil(time.Now().Add(time.Hour)))
	require.ErrorIs(t, err, ErrInvalidOption)

	_, err = Enqueue[string](ctx, "", "wf", "in")
	require.ErrorIs(t, err, ErrInvalidOption)
}

func roundTripError(t *testing.T, err error) error {
	t.Helper()
	s := serializeWorkflowError(nil, err, NewGobSerializer().Name())
	require.NotEmpty(t, s)
	return deserializeWorkflowError(&s)
}

func TestWrappedCauseSurvivesDBRoundTrip(t *testing.T) {
	t.Run("Canceled", func(t *testing.T) {
		got := roundTripError(t, models.NewWorkflowCancelledError("wf-id", context.Canceled))
		assert.True(t, errors.Is(got, ErrWorkflowCancelled))
		assert.True(t, errors.Is(got, context.Canceled))
		assert.False(t, errors.Is(got, context.DeadlineExceeded))
	})

	t.Run("DeadlineExceeded", func(t *testing.T) {
		got := roundTripError(t, models.NewTimeoutError("wf-id", "step", "", context.DeadlineExceeded))
		assert.True(t, errors.Is(got, ErrTimeout))
		assert.True(t, errors.Is(got, context.DeadlineExceeded))
		assert.False(t, errors.Is(got, context.Canceled))
	})

	t.Run("ArbitraryCauseNoFalsePositives", func(t *testing.T) {
		got := roundTripError(t, models.NewWorkflowExecutionError("wf-id", errors.New("boom")))
		assert.True(t, errors.Is(got, &Error{Code: ErrorCodeWorkflowExecution}))
		assert.False(t, errors.Is(got, context.Canceled))
		assert.False(t, errors.Is(got, context.DeadlineExceeded))
		assert.Contains(t, got.Error(), "boom")
	})

	t.Run("OldPayloadWithoutCauseKind", func(t *testing.T) {
		// Zero-value CauseKind gob-encodes identically to a pre-CauseKind payload.
		old := &Error{Message: "Workflow wf-id was cancelled", Code: ErrorCodeWorkflowCancelled, WorkflowID: "wf-id"}
		got := roundTripError(t, old)
		require.NotNil(t, got)
		assert.True(t, errors.Is(got, ErrWorkflowCancelled))
		assert.False(t, errors.Is(got, context.Canceled))
		assert.False(t, errors.Is(got, context.DeadlineExceeded))
	})
}
