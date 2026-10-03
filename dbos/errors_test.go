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

func TestWorkflowPanicIsRecovered(t *testing.T) {
	ctx, err := NewContext(context.Background(), Config{
		AppName:     "test-workflow-panic",
		DatabaseURL: "sqlite:" + filepath.Join(t.TempDir(), "dbos.db"),
	})
	require.NoError(t, err)
	defer Shutdown(ctx, 5*time.Second)

	sentinel := errors.New("boom")
	panicWithString := func(ctx Context, in string) (string, error) { panic("bad " + in) }
	panicWithError := func(ctx Context, in string) (string, error) { panic(sentinel) }
	stepPanics := func(ctx Context, in string) (string, error) {
		return RunAsStep(ctx, func(ctx context.Context) (string, error) { panic("in step") })
	}
	var goStepErr error
	goStepPanics := func(ctx Context, in string) (string, error) {
		ch, err := Go(ctx, func(ctx context.Context) (string, error) { panic("in go step") })
		if err != nil {
			return "", err
		}
		out := <-ch
		goStepErr = out.Err
		return out.Result, out.Err
	}
	RegisterWorkflow(ctx, panicWithString)
	RegisterWorkflow(ctx, panicWithError)
	RegisterWorkflow(ctx, stepPanics)
	RegisterWorkflow(ctx, goStepPanics)
	require.NoError(t, Launch(ctx))

	check := func(t *testing.T, wf func(Context, string) (string, error), contains string) *Error {
		t.Helper()
		handle, err := RunWorkflow(ctx, wf, "input")
		require.NoError(t, err)
		_, err = handle.GetResult()
		require.ErrorIs(t, err, ErrWorkflowPanic)
		var dbosErr *Error
		require.ErrorAs(t, err, &dbosErr)
		assert.Equal(t, handle.GetWorkflowID(), dbosErr.WorkflowID)
		assert.Contains(t, dbosErr.Message, contains)

		status, err := handle.GetStatus()
		require.NoError(t, err)
		assert.Equal(t, WorkflowStatusError, status.Status)
		require.ErrorIs(t, status.Error, ErrWorkflowPanic, "recorded error survives the DB round trip")

		// A fresh handle reads the outcome back from the database.
		retrieved, err := RetrieveWorkflow[string](ctx, handle.GetWorkflowID())
		require.NoError(t, err)
		_, err = retrieved.GetResult()
		require.ErrorIs(t, err, ErrWorkflowPanic)
		return dbosErr
	}

	t.Run("PanicWithString", func(t *testing.T) {
		check(t, panicWithString, "bad input")
	})
	t.Run("PanicWithError", func(t *testing.T) {
		dbosErr := check(t, panicWithError, "boom")
		assert.ErrorIs(t, dbosErr, sentinel, "an error panic value is wrapped as the cause")
	})
	t.Run("PanicInStep", func(t *testing.T) {
		check(t, stepPanics, "in step")
	})
	t.Run("PanicInGoStep", func(t *testing.T) {
		check(t, goStepPanics, "in go step")
		assert.ErrorIs(t, goStepErr, ErrWorkflowPanic, "the workflow sees the panic as the step outcome")
	})
}
