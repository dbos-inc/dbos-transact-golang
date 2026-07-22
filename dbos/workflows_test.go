package dbos

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Global counter for idempotency testing
var idempotencyCounter int64

func simpleWorkflow(dbosCtx Context, input string) (string, error) {
	return input, nil
}

func simpleWorkflowError(dbosCtx Context, input string) (int, error) {
	return 0, fmt.Errorf("failure")
}

func simpleWorkflowWithStep(dbosCtx Context, input string) (string, error) {
	return RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
		return simpleStep(ctx)
	})
}

func slowWorkflow(dbosCtx Context, sleepTime time.Duration) (string, error) {
	Sleep(dbosCtx, sleepTime)
	return "done", nil
}

func simpleStep(_ context.Context) (string, error) {
	return "from step", nil
}

func simpleStepError(_ context.Context) (string, error) {
	return "", fmt.Errorf("step failure")
}

func stepWithSleep(_ context.Context, duration time.Duration) (string, error) {
	time.Sleep(duration)
	return fmt.Sprintf("from step that slept for %s", duration), nil
}

func simpleWorkflowWithStepError(dbosCtx Context, input string) (string, error) {
	return RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
		return simpleStepError(ctx)
	})
}

// idempotencyWorkflow increments a global counter and returns the input
func incrementCounter(_ context.Context, value int64) (int64, error) {
	idempotencyCounter += value
	return idempotencyCounter, nil
}

// Unified struct that demonstrates both pointer and value receiver methods
type workflowStruct struct{}

// Pointer receiver method
func (w *workflowStruct) simpleWorkflow(dbosCtx Context, input string) (string, error) {
	return simpleWorkflow(dbosCtx, input)
}

// Value receiver method on the same struct
func (w workflowStruct) simpleWorkflowValue(dbosCtx Context, input string) (string, error) {
	return input + "-value", nil
}

// interface for workflow methods
type TestWorkflowInterface interface {
	Execute(dbosCtx Context, input string) (string, error)
}

type workflowImplementation struct {
	field string
}

func (w *workflowImplementation) Execute(dbosCtx Context, input string) (string, error) {
	return input + "-" + w.field + "-interface", nil
}

// Generic workflow function
func Identity[T any](dbosCtx Context, in T) (T, error) {
	return in, nil
}

func TestWorkflowsRegistration(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	// Setup workflows with executor
	RegisterWorkflow(dbosCtx, simpleWorkflow)
	RegisterWorkflow(dbosCtx, simpleWorkflowError)
	RegisterWorkflow(dbosCtx, simpleWorkflowWithStep)
	RegisterWorkflow(dbosCtx, simpleWorkflowWithStepError)
	// struct methods
	s := workflowStruct{}
	RegisterWorkflow(dbosCtx, s.simpleWorkflow)
	RegisterWorkflow(dbosCtx, s.simpleWorkflowValue)
	// interface method workflow
	workflowIface := TestWorkflowInterface(&workflowImplementation{
		field: "example",
	})
	RegisterWorkflow(dbosCtx, workflowIface.Execute)
	// Generic workflow
	RegisterWorkflow(dbosCtx, Identity[int])
	RegisterWorkflow(dbosCtx, Identity[string])
	// Closure with captured state
	prefix := "hello-"
	closureWorkflow := func(dbosCtx Context, in string) (string, error) {
		return prefix + in, nil
	}
	RegisterWorkflow(dbosCtx, closureWorkflow)
	// Anonymous workflow
	anonymousWorkflow := func(dbosCtx Context, in string) (string, error) {
		return "anonymous-" + in, nil
	}
	RegisterWorkflow(dbosCtx, anonymousWorkflow)

	require.NoError(t, Launch(dbosCtx), "failed to launch DBOS")

	type testCase struct {
		name           string
		workflowFunc   func(Context, string, ...WorkflowOption) (any, error)
		input          string
		expectedResult any
		expectError    bool
		expectedError  string
	}

	tests := []testCase{
		{
			name: "SimpleWorkflow",
			workflowFunc: func(dbosCtx Context, input string, opts ...WorkflowOption) (any, error) {
				handle, err := RunWorkflow(dbosCtx, simpleWorkflow, input, opts...)
				if err != nil {
					return nil, err
				}
				result, err := handle.GetResult()
				_, err2 := handle.GetResult()
				if err2 == nil {
					return nil, fmt.Errorf("Second call to GetResult should return an error")
				}
				expectedErrorMsg := "workflow result channel is already closed. Did you call GetResult() twice on the same workflow handle?"
				if err2.Error() != expectedErrorMsg {
					return nil, fmt.Errorf("Unexpected error message: %v, expected: %s", err2, expectedErrorMsg)
				}
				return result, err
			},
			input:          "echo",
			expectedResult: "echo",
			expectError:    false,
		},
		{
			name: "SimpleWorkflowError",
			workflowFunc: func(dbosCtx Context, input string, opts ...WorkflowOption) (any, error) {
				handle, err := RunWorkflow(dbosCtx, simpleWorkflowError, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:         "echo",
			expectError:   true,
			expectedError: "failure",
		},
		{
			name: "SimpleWorkflowWithStep",
			workflowFunc: func(dbosCtx Context, input string, opts ...WorkflowOption) (any, error) {
				handle, err := RunWorkflow(dbosCtx, simpleWorkflowWithStep, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "echo",
			expectedResult: "from step",
			expectError:    false,
		},
		{
			name: "SimpleWorkflowStruct",
			workflowFunc: func(dbosCtx Context, input string, opts ...WorkflowOption) (any, error) {
				handle, err := RunWorkflow(dbosCtx, s.simpleWorkflow, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "echo",
			expectedResult: "echo",
			expectError:    false,
		},
		{
			name: "ValueReceiverWorkflow",
			workflowFunc: func(dbosCtx Context, input string, opts ...WorkflowOption) (any, error) {
				handle, err := RunWorkflow(dbosCtx, s.simpleWorkflowValue, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "echo",
			expectedResult: "echo-value",
			expectError:    false,
		},
		{
			name: "interfaceMethodWorkflow",
			workflowFunc: func(dbosCtx Context, input string, opts ...WorkflowOption) (any, error) {
				handle, err := RunWorkflow(dbosCtx, workflowIface.Execute, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "echo",
			expectedResult: "echo-example-interface",
			expectError:    false,
		},
		{
			name: "GenericWorkflow",
			workflowFunc: func(dbosCtx Context, input string, opts ...WorkflowOption) (any, error) {
				handle, err := RunWorkflow(dbosCtx, Identity[int], 42, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "42", // input not used in this case
			expectedResult: 42,
			expectError:    false,
		},
		{
			name: "GenericWorkflowWithString",
			workflowFunc: func(dbosCtx Context, input string, opts ...WorkflowOption) (any, error) {
				handle, err := RunWorkflow(dbosCtx, Identity[string], input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "test-generic",
			expectedResult: "test-generic",
			expectError:    false,
		},
		{
			name: "ClosureWithCapturedState",
			workflowFunc: func(dbosCtx Context, input string, opts ...WorkflowOption) (any, error) {
				handle, err := RunWorkflow(dbosCtx, closureWorkflow, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "world",
			expectedResult: "hello-world",
			expectError:    false,
		},
		{
			name: "AnonymousClosure",
			workflowFunc: func(dbosCtx Context, input string, opts ...WorkflowOption) (any, error) {
				handle, err := RunWorkflow(dbosCtx, anonymousWorkflow, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "test",
			expectedResult: "anonymous-test",
			expectError:    false,
		},
		{
			name: "SimpleWorkflowWithStepError",
			workflowFunc: func(dbosCtx Context, input string, opts ...WorkflowOption) (any, error) {
				handle, err := RunWorkflow(dbosCtx, simpleWorkflowWithStepError, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:         "echo",
			expectError:   true,
			expectedError: "step failure",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := tc.workflowFunc(dbosCtx, tc.input, WithWorkflowID(uuid.NewString()))

			if tc.expectError {
				require.Error(t, err, "expected error but got none")
				if tc.expectedError != "" {
					assert.Equal(t, tc.expectedError, err.Error())
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expectedResult, result)
			}
		})
	}

	t.Run("DoubleRegistrationWithoutName", func(t *testing.T) {
		// Create a fresh DBOS context for this test
		// Don't reset DB; leak checking is done by the parent test's context
		// (its launched runtime goroutines would trip a per-subtest check).
		freshCtx := setupDBOS(t, setupDBOSOptions{dropDB: false, checkLeaks: false})

		// First registration should work
		RegisterWorkflow(freshCtx, simpleWorkflow)

		// Second registration of the same workflow should panic with ErrorCodeConflictingRegistration
		defer func() {
			r := recover()
			require.NotNil(t, r, "expected panic from double registration but got none")
			dbosErr, ok := r.(*Error)
			require.True(t, ok, "expected panic to be *Error, got %T", r)
			assert.Equal(t, ErrorCodeConflictingRegistration, dbosErr.Code)
		}()
		RegisterWorkflow(freshCtx, simpleWorkflow)
	})

	t.Run("DoubleRegistrationWithCustomName", func(t *testing.T) {
		// Create a fresh DBOS context for this test
		// Don't reset DB; leak checking is done by the parent test's context
		// (its launched runtime goroutines would trip a per-subtest check).
		freshCtx := setupDBOS(t, setupDBOSOptions{dropDB: false, checkLeaks: false})

		// First registration with custom name should work
		RegisterWorkflow(freshCtx, simpleWorkflow, WithWorkflowName("custom-workflow"))

		// Second registration with same custom name should panic with ErrorCodeConflictingRegistration
		defer func() {
			r := recover()
			require.NotNil(t, r, "expected panic from double registration with custom name but got none")
			dbosErr, ok := r.(*Error)
			require.True(t, ok, "expected panic to be *Error, got %T", r)
			assert.Equal(t, ErrorCodeConflictingRegistration, dbosErr.Code)
		}()
		RegisterWorkflow(freshCtx, simpleWorkflow, WithWorkflowName("custom-workflow"))
	})

	t.Run("DifferentWorkflowsSameCustomName", func(t *testing.T) {
		// Create a fresh DBOS context for this test
		// Don't reset DB; leak checking is done by the parent test's context
		// (its launched runtime goroutines would trip a per-subtest check).
		freshCtx := setupDBOS(t, setupDBOSOptions{dropDB: false, checkLeaks: false})

		// First registration with custom name should work
		RegisterWorkflow(freshCtx, simpleWorkflow, WithWorkflowName("same-name"))

		// Second registration of different workflow with same custom name should panic with ErrorCodeConflictingRegistration
		defer func() {
			r := recover()
			require.NotNil(t, r, "expected panic from registering different workflows with same custom name but got none")
			dbosErr, ok := r.(*Error)
			require.True(t, ok, "expected panic to be *Error, got %T", r)
			assert.Equal(t, ErrorCodeConflictingRegistration, dbosErr.Code)
		}()
		RegisterWorkflow(freshCtx, simpleWorkflowError, WithWorkflowName("same-name"))
	})

	t.Run("SameWorkflowDifferentCustomNames", func(t *testing.T) {
		// Create a fresh DBOS context for this test
		// Don't reset DB; leak checking is done by the parent test's context
		// (its launched runtime goroutines would trip a per-subtest check).
		freshCtx := setupDBOS(t, setupDBOSOptions{dropDB: false, checkLeaks: false})

		// First registration with a custom name should work
		RegisterWorkflow(freshCtx, simpleWorkflow, WithWorkflowName("name-a"))

		// Registering the SAME function under a DIFFERENT custom name should panic with
		// ErrorCodeConflictingRegistration: the registry is keyed on the function's FQN
		// (runtime.FuncForPC), which is identical for the same function value, so the FQN
		// collision is detected before the second custom name is ever stored.
		defer func() {
			r := recover()
			require.NotNil(t, r, "expected panic from registering the same function under a different custom name but got none")
			dbosErr, ok := r.(*Error)
			require.True(t, ok, "expected panic to be *Error, got %T", r)
			assert.Equal(t, ErrorCodeConflictingRegistration, dbosErr.Code)
		}()
		RegisterWorkflow(freshCtx, simpleWorkflow, WithWorkflowName("name-b"))
	})

	t.Run("RegisterAfterLaunchPanics", func(t *testing.T) {
		// Create a fresh DBOS context for this test
		// Don't reset DB; leak checking is done by the parent test's context
		// (its launched runtime goroutines would trip a per-subtest check).
		freshCtx := setupDBOS(t, setupDBOSOptions{dropDB: false, checkLeaks: false})

		// Launch DBOS context
		err := Launch(freshCtx)
		require.NoError(t, err)
		defer Shutdown(freshCtx, 10*time.Second)

		// Attempting to register after launch should panic
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic from registration after launch but got none")
			}
		}()
		RegisterWorkflow(freshCtx, simpleWorkflow)
	})
}

// The types and factories below each produce TWO workflow function values that
// share a fully-qualified name (FQN) under resolveWorkflowFunctionName, even
// though each carries different captured state. The FQN is derived from the
// entry program counter via runtime.FuncForPC; the receiver/closure data is
// lost. These are the five FQN-collision cases from the issue.

// Case 1: value-receiver method values -> "...fqnCollidingValueHolder.Run-fm".
type fqnCollidingValueHolder struct {
	label string
}

func (h fqnCollidingValueHolder) Run(ctx Context, input string) (string, error) {
	return h.label, nil
}

// Case 2: pointer-receiver method values -> "...(*fqnCollidingPtrHolder).Run-fm".
type fqnCollidingPtrHolder struct {
	label string
}

func (h *fqnCollidingPtrHolder) Run(ctx Context, input string) (string, error) {
	return h.label, nil
}

// Case 3: interface method values -> "...fqnCollidingSpeaker.Run-fm" (named for
// the interface method, independent of the concrete type behind it).
type fqnCollidingSpeaker interface {
	Run(ctx Context, input string) (string, error)
}

// Case 4: closures from a single literal evaluated multiple times (loop) ->
// "...fqnCollidingLoopClosures.func1".
func fqnCollidingLoopClosures(labels ...string) []Workflow[string, string] {
	fns := make([]Workflow[string, string], 0, len(labels))
	for _, label := range labels {
		fns = append(fns, func(ctx Context, input string) (string, error) {
			return label, nil
		})
	}
	return fns
}

// Case 5: factory closures. The factory is kept un-inlined so every call shares
// "...fqnCollidingFactory.func1". Without go:noinline the compiler may inline the
// factory per call site, yielding distinct func1/func2 names (no collision).
//
//go:noinline
func fqnCollidingFactory(label string) Workflow[string, string] {
	return func(ctx Context, input string) (string, error) {
		return label, nil
	}
}

// TestRunWorkflowProvidedVsRegisteredDivergence reproduces the bug where
// RunWorkflow directly executes the *provided* function value, while queue and
// recovery execution run the *registered* function looked up by FQN. When two
// distinct function values share an FQN, the same workflow ID produces different
// results depending on the execution path (direct vs recovery), causing
// nondeterminism. Each subtest exercises one FQN-collision case from the issue.
func TestRunWorkflowProvidedVsRegisteredDivergence(t *testing.T) {
	loopClosures := fqnCollidingLoopClosures("registered", "provided")

	cases := []struct {
		name         string
		registeredWf Workflow[string, string]
		providedWf   Workflow[string, string]
	}{
		{
			name:         "value receiver method values",
			registeredWf: fqnCollidingValueHolder{label: "registered"}.Run,
			providedWf:   fqnCollidingValueHolder{label: "provided"}.Run,
		},
		{
			name:         "pointer receiver method values",
			registeredWf: (&fqnCollidingPtrHolder{label: "registered"}).Run,
			providedWf:   (&fqnCollidingPtrHolder{label: "provided"}).Run,
		},
		{
			name:         "interface method values",
			registeredWf: fqnCollidingSpeaker(fqnCollidingValueHolder{label: "registered"}).Run,
			providedWf:   fqnCollidingSpeaker(fqnCollidingValueHolder{label: "provided"}).Run,
		},
		{
			name:         "loop closures from one literal",
			registeredWf: loopClosures[0],
			providedWf:   loopClosures[1],
		},
		{
			name:         "factory closures (not inlined)",
			registeredWf: fqnCollidingFactory("registered"),
			providedWf:   fqnCollidingFactory("provided"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Precondition: the two values must share an FQN, otherwise the
			// registry entry for registeredWf would not be the one resolved for
			// providedWf and the bug would not be exercised.
			require.Equal(t,
				resolveWorkflowFunctionName(tc.registeredWf),
				resolveWorkflowFunctionName(tc.providedWf),
				"the two function values must share an FQN to exercise the bug")

			dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

			RegisterWorkflow(dbosCtx, tc.registeredWf)
			require.NoError(t, Launch(dbosCtx), "failed to launch DBOS")

			workflowID := uuid.NewString()

			// Direct execution path: RunWorkflow runs the provided fn.
			handle, err := RunWorkflow(dbosCtx, tc.providedWf, "input", WithWorkflowID(workflowID))
			require.NoError(t, err, "failed to run workflow")
			result1, err := handle.GetResult()
			require.NoError(t, err, "failed to get result from direct execution")

			// Recovery path: resolves the function from the registry by FQN and
			// runs the registered wrapped function.
			setWorkflowStatusPending(t, dbosCtx, workflowID)
			recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
			require.NoError(t, err, "failed to recover pending workflows")
			require.Len(t, recoveredHandles, 1, "expected 1 recovered handle")
			require.Equal(t, workflowID, recoveredHandles[0].GetWorkflowID())
			result2, err := recoveredHandles[0].GetResult()
			require.NoError(t, err, "failed to get result from recovered execution")

			// Invariant: the same workflow ID must produce the same result
			// regardless of execution path. Under the bug, direct execution runs
			// the provided body ("provided") while recovery runs the registered
			// body ("registered") -> nondeterminism. After the fix, both run the
			// registered body.
			require.Equal(t, result1, result2,
				"recovery returned a different result than direct execution for the same workflow ID (nondeterminism): direct=%q recovered=%q",
				result1, result2)
		})
	}
}

// configuredNotifier is a workflow holder whose method is registered once per instance
// via WithInstance, distinguishing receivers that would otherwise share an FQN.
type configuredNotifier struct {
	channel string
}

func (n *configuredNotifier) ConfigName() string { return n.channel }

func (n *configuredNotifier) Send(ctx Context, msg string) (string, error) {
	return n.channel + ": " + msg, nil
}

// TestConfiguredInstanceWorkflows verifies that two instances of the same workflow method,
// registered with WithInstance and run with WithRunInstance, each execute on their own
// receiver -- on the direct path and on recovery -- and that running without
// WithRunInstance fails loudly instead of silently using another instance.
func TestConfiguredInstanceWorkflows(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	slack := &configuredNotifier{channel: "slack"}
	email := &configuredNotifier{channel: "email"}

	RegisterWorkflow(dbosCtx, slack.Send, WithInstance(slack))
	RegisterWorkflow(dbosCtx, email.Send, WithInstance(email))
	require.NoError(t, Launch(dbosCtx), "failed to launch DBOS")

	for _, inst := range []*configuredNotifier{slack, email} {
		workflowID := uuid.NewString()

		handle, err := RunWorkflow(dbosCtx, inst.Send, "hi", WithWorkflowID(workflowID), WithRunInstance(inst))
		require.NoError(t, err, "failed to run workflow on instance %q", inst.channel)
		direct, err := handle.GetResult()
		require.NoError(t, err, "failed to get direct result for instance %q", inst.channel)
		require.Equal(t, inst.channel+": hi", direct, "direct execution ran the wrong instance")

		// The record stores the unqualified workflow name plus the instance class and config names
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get status for instance %q", inst.channel)
		require.Equal(t, resolveWorkflowFunctionName(inst.Send), status.Name)
		require.Equal(t, "configuredNotifier", status.ClassName)
		require.NotNil(t, status.ConfigName, "config name not recorded")
		require.Equal(t, inst.channel, *status.ConfigName)

		setWorkflowStatusPending(t, dbosCtx, workflowID)
		recovered, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "failed to recover pending workflows")
		require.Len(t, recovered, 1, "expected 1 recovered handle")
		require.Equal(t, workflowID, recovered[0].GetWorkflowID())
		recResult, err := recovered[0].GetResult()
		require.NoError(t, err, "failed to get recovered result for instance %q", inst.channel)
		require.Equal(t, direct, recResult, "recovery ran a different instance than direct execution")
	}

	// Without WithRunInstance the bare (colliding) FQN was never registered: fail loudly
	// rather than silently running some other instance.
	_, err := RunWorkflow(dbosCtx, slack.Send, "hi")
	require.Error(t, err, "running an instance method without WithRunInstance should fail")
}

// notifier is implemented by configuredNotifier and loudNotifier. Method values taken
// through an interface share a single FQN across all implementations and instances.
type notifier interface {
	ConfiguredInstance
	Send(ctx Context, msg string) (string, error)
}

type loudNotifier struct {
	channel string
}

func (n *loudNotifier) ConfigName() string { return n.channel }

func (n *loudNotifier) Send(ctx Context, msg string) (string, error) {
	return strings.ToUpper(n.channel + ": " + msg), nil
}

// TestConfiguredInstanceInterfaceWorkflows verifies WithInstance disambiguates method values
// taken through an interface: two different implementations behind the same interface FQN each
// run on their own concrete receiver, on the direct path and on recovery.
func TestConfiguredInstanceInterfaceWorkflows(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	var quiet notifier = &configuredNotifier{channel: "quiet"}
	var loud notifier = &loudNotifier{channel: "loud"}
	require.Equal(t, resolveWorkflowFunctionName(quiet.Send), resolveWorkflowFunctionName(loud.Send),
		"precondition: interface method values should share an FQN across implementations")

	RegisterWorkflow(dbosCtx, quiet.Send, WithInstance(quiet))
	RegisterWorkflow(dbosCtx, loud.Send, WithInstance(loud))
	require.NoError(t, Launch(dbosCtx), "failed to launch DBOS")

	for _, tc := range []struct {
		inst      notifier
		expected  string
		className string
	}{
		{quiet, "quiet: hi", "configuredNotifier"},
		{loud, "LOUD: HI", "loudNotifier"},
	} {
		workflowID := uuid.NewString()

		handle, err := RunWorkflow(dbosCtx, tc.inst.Send, "hi", WithWorkflowID(workflowID), WithRunInstance(tc.inst))
		require.NoError(t, err, "failed to run workflow on instance %q", tc.inst.ConfigName())
		direct, err := handle.GetResult()
		require.NoError(t, err, "failed to get direct result for instance %q", tc.inst.ConfigName())
		require.Equal(t, tc.expected, direct, "direct execution ran the wrong implementation or instance")

		// The recorded class name is the concrete implementation, not the interface
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get status for instance %q", tc.inst.ConfigName())
		require.Equal(t, tc.className, status.ClassName)
		require.NotNil(t, status.ConfigName, "config name not recorded")
		require.Equal(t, tc.inst.ConfigName(), *status.ConfigName)

		setWorkflowStatusPending(t, dbosCtx, workflowID)
		recovered, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "failed to recover pending workflows")
		require.Len(t, recovered, 1, "expected 1 recovered handle")
		recResult, err := recovered[0].GetResult()
		require.NoError(t, err, "failed to get recovered result for instance %q", tc.inst.ConfigName())
		require.Equal(t, direct, recResult, "recovery ran a different implementation than direct execution")
	}
}

func stepWithinAStep(ctx context.Context) (string, error) {
	return simpleStep(ctx)
}

func stepWithinAStepWorkflow(dbosCtx Context, input string) (string, error) {
	return RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
		return stepWithinAStep(ctx)
	})
}

// Global counter for retry testing
var stepRetryAttemptCount int

// errStepRetrySentinel is wrapped by every failing attempt so the test can prove
// the underlying errors remain reachable through the ErrorCodeMaxStepRetriesExceeded wrapper
// via errors.Is (i.e. the wrappedErr/Unwrap chain, not just the formatted message).
var errStepRetrySentinel = errors.New("step retry sentinel")

func stepRetryAlwaysFailsStep(_ context.Context) (string, error) {
	stepRetryAttemptCount++
	return "", fmt.Errorf("always fails - attempt %d: %w", stepRetryAttemptCount, errStepRetrySentinel)
}

var stepIdempotencyCounter int

// --- retry predicate test helpers ---

var retryPredicateAttemptCount int

// retryPredicateWorkflow: stops on "permanent" errors, retries transient ones.
func retryPredicateWorkflow(ctx Context, _ string) (string, error) {
	return RunAsStep(ctx, func(_ context.Context) (string, error) {
		retryPredicateAttemptCount++
		if retryPredicateAttemptCount == 2 {
			return "", fmt.Errorf("permanent failure")
		}
		return "", fmt.Errorf("transient failure")
	},
		WithStepMaxRetries(5),
		WithStepBaseInterval(1*time.Millisecond),
		WithStepRetryPredicate(func(err error) bool {
			return !strings.Contains(err.Error(), "permanent")
		}),
	)
}

func stepIdempotencyTest(_ context.Context) (string, error) {
	stepIdempotencyCounter++
	return "", nil
}

func stepRetryWorkflow(dbosCtx Context, input string) (string, error) {
	RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
		return stepIdempotencyTest(ctx)
	})

	return RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
		return stepRetryAlwaysFailsStep(ctx)
	}, WithStepMaxRetries(5), WithStepBaseInterval(1*time.Millisecond), WithStepMaxInterval(10*time.Millisecond))
}

func step1(_ context.Context) (string, error) {
	return "", nil
}

func testStepWf1(dbosCtx Context, input string) (string, error) {
	return RunAsStep(dbosCtx, step1)
}

func step2(_ context.Context) (string, error) {
	return "", nil
}

func testStepWf2(dbosCtx Context, input string) (string, error) {
	return RunAsStep(dbosCtx, step2)
}

// genericStep is a generic step function that processes a value of any type
func genericStep[T any](_ context.Context, value T) (T, error) {
	return value, nil
}

// genericStepWorkflow uses a generic step function with both string and int types
func genericStepWorkflow(dbosCtx Context, input string) (string, error) {
	// Use the generic step with a string type
	result1, err := RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
		return genericStep(ctx, input+"-processed")
	})
	if err != nil {
		return "", err
	}

	// Use the generic step with an int type
	result2, err := RunAsStep(dbosCtx, func(ctx context.Context) (int, error) {
		return genericStep(ctx, 21)
	})
	if err != nil {
		return "", err
	}

	// Combine results
	return fmt.Sprintf("%s-%d", result1, result2*2), nil
}

// --- One-shot fault injection used by the step-ID-drift regression test ---
//
// faultPool wraps a Pool and, exactly once, makes the operation_outputs read
// inside checkOperationExecution fail with a retryable ("conn closed") error for
// a single target workflow. That forces retryWithResult to re-run the DB-layer
// closure.

type errRow struct{ err error }

func (r errRow) Scan(...any) error { return r.err }

type faultTx struct {
	Tx
	p *faultPool
}

func (t *faultTx) QueryRow(ctx context.Context, query string, args ...any) Row {
	target, _ := t.p.target.Load().(string)
	if target != "" && strings.Contains(query, "operation_outputs") &&
		len(args) > 0 && args[0] == any(target) &&
		t.p.fired.CompareAndSwap(false, true) {
		return errRow{errors.New("conn closed")} // retryable per postgresDialect.IsRetryable
	}
	return t.Tx.QueryRow(ctx, query, args...)
}

// faultPool is installed over sysDB.Pool() before Launch — while no goroutine
// reads the field — and stays for the whole test, so arming it later doesn't
// race pool readers such as the queue runner. Disarmed (empty target) it is a
// transparent pass-through.
type faultPool struct {
	Pool
	target atomic.Value // string: workflow ID whose operation_outputs read should fault; "" disarms
	fired  atomic.Bool  // ensures the fault triggers at most once per arm
}

func (p *faultPool) arm(workflowID string) {
	p.fired.Store(false)
	p.target.Store(workflowID)
}

func (p *faultPool) BeginTx(ctx context.Context, opts TxOptions) (Tx, error) {
	tx, err := p.Pool.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &faultTx{Tx: tx, p: p}, nil
}

// sleepStepIDDriftWorkflow does a single durable Sleep, which records exactly one
// step (DBOS.sleep) at function_id 0.
func sleepStepIDDriftWorkflow(ctx Context, _ string) (string, error) {
	if _, err := Sleep(ctx, time.Millisecond); err != nil {
		return "", err
	}
	return "ok", nil
}

func TestSteps(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	stepsDatabaseURL := backendDatabaseURL(t)

	// Create workflows with executor
	RegisterWorkflow(dbosCtx, stepWithinAStepWorkflow)
	RegisterWorkflow(dbosCtx, stepRetryWorkflow)
	RegisterWorkflow(dbosCtx, testStepWf1)
	RegisterWorkflow(dbosCtx, testStepWf2)
	RegisterWorkflow(dbosCtx, genericStepWorkflow)
	RegisterWorkflow(dbosCtx, retryPredicateWorkflow)
	// Create a workflow that uses custom step names
	customNameWorkflow := func(dbosCtx Context, input string) (string, error) {
		// Run a step with a custom name
		result1, err := RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
			return "custom-step-1-result", nil
		}, WithStepName("MyCustomStep1"))
		if err != nil {
			return "", err
		}

		// Run another step with a different custom name
		result2, err := RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
			return "custom-step-2-result", nil
		}, WithStepName("MyCustomStep2"))
		if err != nil {
			return "", err
		}

		return result1 + "-" + result2, nil
	}

	RegisterWorkflow(dbosCtx, customNameWorkflow)

	// A workflow that mistakenly runs its step with the outer executor context
	// (captured from the closure) instead of the workflow context it is handed.
	wrongCtxWorkflow := func(_ Context, input string) (string, error) {
		return RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
			return simpleStep(ctx)
		})
	}
	RegisterWorkflow(dbosCtx, wrongCtxWorkflow, WithWorkflowName("wrongCtxWorkflow"))

	// Single-step workflow for the RecordOperationResultIdempotency subtest.
	recordOpResultWf := func(ctx Context, _ string) (string, error) {
		return RunAsStep(ctx, func(_ context.Context) (string, error) {
			return "step-output", nil
		})
	}
	RegisterWorkflow(dbosCtx, recordOpResultWf, WithWorkflowName("recordOpResultWf"))

	// Define user-defined types for testing serialization
	type StepInput struct {
		Name      string            `json:"name"`
		Count     int               `json:"count"`
		Active    bool              `json:"active"`
		Metadata  map[string]string `json:"metadata"`
		CreatedAt time.Time         `json:"created_at"`
	}

	type StepOutput struct {
		ProcessedName string    `json:"processed_name"`
		TotalCount    int       `json:"total_count"`
		Success       bool      `json:"success"`
		ProcessedAt   time.Time `json:"processed_at"`
		Details       []string  `json:"details"`
	}

	// Create a step function that accepts StepInput and returns StepOutput
	processUserObjectStep := func(_ context.Context, input StepInput) (StepOutput, error) {
		// Process the input and create output
		output := StepOutput{
			ProcessedName: fmt.Sprintf("Processed_%s", input.Name),
			TotalCount:    input.Count * 2,
			Success:       input.Active,
			ProcessedAt:   time.Now(),
			Details:       []string{"step1", "step2", "step3"},
		}

		// Verify input was correctly deserialized
		if input.Metadata == nil {
			return StepOutput{}, fmt.Errorf("metadata map was not properly deserialized")
		}

		return output, nil
	}

	// Create a workflow that uses the step with user-defined objects
	userObjectWorkflow := func(dbosCtx Context, workflowInput string) (string, error) {
		// Create input for the step
		stepInput := StepInput{
			Name:   workflowInput,
			Count:  42,
			Active: true,
			Metadata: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			CreatedAt: time.Now(),
		}

		// Run the step with user-defined input and output
		output, err := RunAsStep(dbosCtx, func(ctx context.Context) (StepOutput, error) {
			return processUserObjectStep(ctx, stepInput)
		})
		if err != nil {
			return "", fmt.Errorf("step failed: %w", err)
		}

		// Verify the output was correctly returned
		if output.ProcessedName == "" {
			return "", fmt.Errorf("output ProcessedName is empty")
		}
		if output.TotalCount != 84 {
			return "", fmt.Errorf("expected TotalCount to be 84, got %d", output.TotalCount)
		}
		if len(output.Details) != 3 {
			return "", fmt.Errorf("expected 3 details, got %d", len(output.Details))
		}

		return "", nil
	}
	// Register the workflow
	RegisterWorkflow(dbosCtx, userObjectWorkflow)

	RegisterWorkflow(dbosCtx, sleepStepIDDriftWorkflow)

	var interruptedStepAttempts atomic.Int64
	interruptedStepStarted := NewEvent()
	interruptibleStepWorkflow := func(ctx Context, _ string) (string, error) {
		return RunAsStep(ctx, func(ctx context.Context) (string, error) {
			if interruptedStepAttempts.Add(1) == 1 {
				interruptedStepStarted.Set()
				<-ctx.Done()
				return "", ctx.Err()
			}
			return "completed", nil
		})
	}
	RegisterWorkflow(dbosCtx, interruptibleStepWorkflow)

	// Child that accepts preemption: the first attempt blocks until cancelled,
	// later attempts complete.
	var awaitedChildExecutions atomic.Int64
	awaitedChildStarted := NewEvent()
	awaitedChildWorkflow := func(ctx Context, _ string) (string, error) {
		if awaitedChildExecutions.Add(1) == 1 {
			awaitedChildStarted.Set()
			<-ctx.Done()
			return "", ctx.Err()
		}
		return "child-result", nil
	}
	RegisterWorkflow(dbosCtx, awaitedChildWorkflow)

	awaitingParentWorkflow := func(ctx Context, _ string) (string, error) {
		childHandle, err := RunWorkflow(ctx, awaitedChildWorkflow, "")
		if err != nil {
			return "", err
		}
		return childHandle.GetResult()
	}
	RegisterWorkflow(dbosCtx, awaitingParentWorkflow)

	// Child that ignores cancellation and completes once released.
	var stubbornChildExecutions atomic.Int64
	stubbornChildStarted := NewEvent()
	stubbornChildRelease := make(chan struct{})
	stubbornChildWorkflow := func(ctx Context, _ string) (string, error) {
		stubbornChildExecutions.Add(1)
		stubbornChildStarted.Set()
		<-stubbornChildRelease
		return "child-result", nil
	}
	RegisterWorkflow(dbosCtx, stubbornChildWorkflow)

	stubbornParentWorkflow := func(ctx Context, _ string) (string, error) {
		childHandle, err := RunWorkflow(ctx, stubbornChildWorkflow, "")
		if err != nil {
			return "", err
		}
		res, err := childHandle.GetResult()
		if err != nil {
			return "", err
		}
		return RunAsStep(ctx, func(stepCtx context.Context) (string, error) {
			if stepCtx.Err() != nil {
				return "", stepCtx.Err()
			}
			return res + "-done", nil
		})
	}
	RegisterWorkflow(dbosCtx, stubbornParentWorkflow)

	// Child cancelled via the API while the parent stays healthy: blocks until
	// released, then observes its cancellation at the next step boundary.
	var apiCancelledChildID string
	apiCancelChildStarted := NewEvent()
	apiCancelChildRelease := make(chan struct{})
	apiCancelChildWorkflow := func(ctx Context, _ string) (string, error) {
		id, err := GetWorkflowID(ctx)
		if err != nil {
			return "", err
		}
		apiCancelledChildID = id
		apiCancelChildStarted.Set()
		<-apiCancelChildRelease
		return RunAsStep(ctx, func(context.Context) (string, error) {
			return "child-result", nil
		})
	}
	RegisterWorkflow(dbosCtx, apiCancelChildWorkflow)

	apiCancelParentWorkflow := func(ctx Context, _ string) (string, error) {
		childHandle, err := RunWorkflow(ctx, apiCancelChildWorkflow, "")
		if err != nil {
			return "", err
		}
		return childHandle.GetResult()
	}
	RegisterWorkflow(dbosCtx, apiCancelParentWorkflow)

	// Two live executions of the same workflow race to checkpoint this step:
	// each blocks until released; the one released second loses the checkpoint
	// race. Used by ConflictingRunDisarmsDurableCancel.
	var conflictCancelExecs atomic.Int64
	conflictCancelFirstStarted := NewEvent()
	conflictCancelSecondStarted := NewEvent()
	conflictCancelReleaseFirst := make(chan struct{})
	conflictCancelReleaseSecond := make(chan struct{})
	conflictCancelWorkflow := func(ctx Context, _ string) (string, error) {
		return RunAsStep(ctx, func(context.Context) (string, error) {
			if conflictCancelExecs.Add(1) == 1 {
				conflictCancelFirstStarted.Set()
				<-conflictCancelReleaseFirst
			} else {
				conflictCancelSecondStarted.Set()
				<-conflictCancelReleaseSecond
			}
			return "ok", nil
		})
	}
	RegisterWorkflow(dbosCtx, conflictCancelWorkflow, WithWorkflowName("conflict-cancel-workflow"))

	// Installed before Launch so no goroutine reads sysDB.Pool() concurrently
	// with the swap; armed on demand by StepIDNotReallocatedOnDBRetry.
	sysDB := dbosCtx.(*dbosContext).systemDB.(*sysdb.SysDB)
	stepsFaultPool := &faultPool{Pool: sysDB.Pool()}
	sysDB.SetPool(stepsFaultPool)

	err := Launch(dbosCtx)
	require.NoError(t, err, "failed to launch DBOS")

	t.Run("StepsMustRunInsideWorkflows", func(t *testing.T) {
		// Attempt to run a step outside of a workflow context
		_, err := RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
			return simpleStep(ctx)
		})
		require.Error(t, err, "expected error when running step outside of workflow context, but got none")

		// Check the error type
		dbosErr, ok := err.(*Error)
		require.True(t, ok, "expected error to be of type *Error, got %T", err)

		require.Equal(t, ErrorCodeStepExecution, dbosErr.Code, "expected error code to be ErrorCodeStepExecution, got %v", dbosErr.Code)

		// Test the specific message from the 3rd argument
		expectedMessagePart := "workflow state not found in context: are you running this step within a workflow?"
		require.Contains(t, err.Error(), expectedMessagePart, "expected error message to contain %q, but got %q", expectedMessagePart, err.Error())
	})

	t.Run("WorkflowCallsStepWithWrongContext", func(t *testing.T) {
		// The workflow runs its step with the outer executor context, which carries no
		// workflow state, so the step must fail with a clear ErrorCodeStepExecution.
		handle, err := RunWorkflow(dbosCtx, wrongCtxWorkflow, "echo")
		require.NoError(t, err)
		_, err = handle.GetResult()
		require.Error(t, err)
		var dbosErr *Error
		require.ErrorAs(t, err, &dbosErr)
		require.Equal(t, ErrorCodeStepExecution, dbosErr.Code)
		require.ErrorContains(t, err, "workflow state not found in context")
	})

	t.Run("GuardRejectsNonStepContext", func(t *testing.T) {
		// checkStepContext is the last-line guard ensuring the step body only ever runs
		// with the DBOS-provided step context (isWithinStep == true).

		// A plain workflow context (isWithinStep == false) is not a valid step context.
		wfCtx := WithValue(dbosCtx, workflowStateKey, &workflowState{workflowID: "wf-1"})
		err := checkStepContext(wfCtx, "wf-1", "myStep")
		require.Error(t, err)
		var dbosErr *Error
		require.ErrorAs(t, err, &dbosErr)
		require.Equal(t, ErrorCodeStepExecution, dbosErr.Code)
		require.ErrorContains(t, err, "step must use the context.Context received from its dbos.Func closure")

		// A context with no workflow state at all is also rejected.
		err = checkStepContext(dbosCtx, "wf-1", "myStep")
		require.Error(t, err)
		require.ErrorContains(t, err, "step must use the context.Context received from its dbos.Func closure")

		// A proper step context passes.
		stepCtx := WithValue(dbosCtx, workflowStateKey, &workflowState{workflowID: "wf-1", isWithinStep: true})
		require.NoError(t, checkStepContext(stepCtx, "wf-1", "myStep"))
	})

	t.Run("StepWithinAStepAreJustFunctions", func(t *testing.T) {
		handle, err := RunWorkflow(dbosCtx, stepWithinAStepWorkflow, "test")
		require.NoError(t, err, "failed to run step within a step")
		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result from step within a step")
		assert.Equal(t, "from step", result)

		steps, err := GetWorkflowSteps(dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err, "failed to list steps")
		require.Len(t, steps, 1, "expected 1 step, got %d", len(steps))
	})

	t.Run("StepRetryWithExponentialBackoff", func(t *testing.T) {
		// Reset the global counters before test
		stepRetryAttemptCount = 0
		stepIdempotencyCounter = 0

		// Execute the workflow
		handle, err := RunWorkflow(dbosCtx, stepRetryWorkflow, "test")
		require.NoError(t, err, "failed to start retry workflow")

		_, err = handle.GetResult()
		require.Error(t, err, "expected error from failing workflow but got none")

		// Verify the step was called exactly 6 times (max attempts + 1 initial attempt)
		assert.Equal(t, 6, stepRetryAttemptCount, "expected 6 attempts")

		// Verify the error is a ErrorCodeMaxStepRetriesExceeded error
		dbosErr, ok := err.(*Error)
		require.True(t, ok, "expected error to be of type *Error, got %T", err)

		assert.Equal(t, ErrorCodeMaxStepRetriesExceeded, dbosErr.Code, "expected error code to be ErrorCodeMaxStepRetriesExceeded")

		// Verify the error contains the step name and max retries
		expectedErrorMessage := "has exceeded its maximum of 5 retries"
		assert.Contains(t, dbosErr.Message, expectedErrorMessage, "expected error message to contain expected text")

		// Verify each error message is present in the joined error
		for i := 1; i <= 5; i++ {
			expectedMsg := fmt.Sprintf("always fails - attempt %d", i)
			assert.Contains(t, dbosErr.Error(), expectedMsg, "expected joined error to contain expected message")
		}

		// Verify the wrapping contract itself (not just the formatted message):
		// the error must match the ErrorCodeMaxStepRetriesExceeded code via errors.Is, and
		// the underlying step errors must remain reachable through Unwrap. This
		// last check fails if models.NewMaxStepRetriesExceededError stops setting wrappedErr.
		assert.True(t, errors.Is(err, ErrMaxStepRetriesExceeded), "expected errors.Is to match ErrorCodeMaxStepRetriesExceeded code")
		assert.True(t, errors.Is(err, errStepRetrySentinel), "expected underlying step error to be reachable via errors.Is (Unwrap chain)")

		// Verify that the failed step was still recorded in the database
		steps, err := GetWorkflowSteps(dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps")

		require.Len(t, steps, 2, "expected 2 recorded steps")

		// Verify the second step has the error
		step := steps[1]
		require.NotNil(t, step.Error, "expected error in recorded step, got none")

		assert.Equal(t, dbosErr.Error(), step.Error.Error(), "expected recorded step error to match joined error")

		// Verify the idempotency step was executed only once
		assert.Equal(t, 1, stepIdempotencyCounter, "expected idempotency step to be executed only once")
	})

	t.Run("RetryPredicateStopsOnNonRetryableError", func(t *testing.T) {
		retryPredicateAttemptCount = 0
		handle, err := RunWorkflow(dbosCtx, retryPredicateWorkflow, "")
		require.NoError(t, err)

		_, err = handle.GetResult()
		require.Error(t, err, "expected error from non-retryable failure")
		// attempt 1 = transient (retried), attempt 2 = permanent (predicate returns false - stop)
		assert.Equal(t, 2, retryPredicateAttemptCount, "expected exactly 2 attempts before predicate stopped retrying")
		assert.Contains(t, err.Error(), "permanent failure")
	})

	t.Run("checkStepName", func(t *testing.T) {
		// Run first workflow with custom step name
		handle1, err := RunWorkflow(dbosCtx, testStepWf1, "test-input-1")
		require.NoError(t, err, "failed to run testStepWf1")
		_, err = handle1.GetResult()
		require.NoError(t, err, "failed to get result from testStepWf1")

		// Run second workflow with custom step name
		handle2, err := RunWorkflow(dbosCtx, testStepWf2, "test-input-2")
		require.NoError(t, err, "failed to run testStepWf2")
		_, err = handle2.GetResult()
		require.NoError(t, err, "failed to get result from testStepWf2")

		// Get workflow steps for first workflow and check step name
		steps1, err := GetWorkflowSteps(dbosCtx, handle1.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps for testStepWf1")
		require.Len(t, steps1, 1, "expected 1 step in testStepWf1")
		s1 := steps1[0]
		expectedStepName1 := runtime.FuncForPC(reflect.ValueOf(step1).Pointer()).Name()
		assert.Equal(t, expectedStepName1, s1.StepName, "expected step name to match runtime function name")

		// Get workflow steps for second workflow and check step name
		steps2, err := GetWorkflowSteps(dbosCtx, handle2.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps for testStepWf2")
		require.Len(t, steps2, 1, "expected 1 step in testStepWf2")
		s2 := steps2[0]
		expectedStepName2 := runtime.FuncForPC(reflect.ValueOf(step2).Pointer()).Name()
		assert.Equal(t, expectedStepName2, s2.StepName, "expected step name to match runtime function name")
	})

	t.Run("customStepNames", func(t *testing.T) {

		// Execute the workflow
		handle, err := RunWorkflow(dbosCtx, customNameWorkflow, "test-input")
		require.NoError(t, err, "failed to run workflow with custom step names")

		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result from workflow with custom step names")
		assert.Equal(t, "custom-step-1-result-custom-step-2-result", result)

		// Verify the custom step names were recorded
		steps, err := GetWorkflowSteps(dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps")
		require.Len(t, steps, 2, "expected 2 steps")

		// Check that the first step has the custom name
		assert.Equal(t, "MyCustomStep1", steps[0].StepName, "expected first step to have custom name")
		assert.Equal(t, 0, steps[0].StepID)

		// Check that the second step has the custom name
		assert.Equal(t, "MyCustomStep2", steps[1].StepName, "expected second step to have custom name")
		assert.Equal(t, 1, steps[1].StepID)
	})

	t.Run("stepsOutputEncoding", func(t *testing.T) {
		// Execute the workflow
		handle, err := RunWorkflow(dbosCtx, userObjectWorkflow, "TestObject")
		require.NoError(t, err, "failed to run workflow with user-defined objects")

		// Get the result
		_, err = handle.GetResult()
		require.NoError(t, err, "failed to get result from workflow")

		// Verify the step was recorded
		steps, err := GetWorkflowSteps(dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps")
		require.Len(t, steps, 1, "expected 1 step")

		// Verify step output was properly serialized and stored
		step := steps[0]
		require.NotNil(t, step.Output, "step output should not be nil")
		assert.Nil(t, step.Error)

		// Listing paths return default-JSON rows as raw JSON strings
		var storedOutput StepOutput
		err = json.Unmarshal([]byte(step.Output.(string)), &storedOutput)
		require.NoError(t, err, "failed to decode step output to StepOutput")

		// Verify all fields were correctly serialized and deserialized
		assert.Equal(t, "Processed_TestObject", storedOutput.ProcessedName, "ProcessedName not correctly serialized")
		assert.Equal(t, 84, storedOutput.TotalCount, "TotalCount not correctly serialized")
		assert.True(t, storedOutput.Success, "Success flag not correctly serialized")
		assert.Len(t, storedOutput.Details, 3, "Details array length incorrect")
		assert.Equal(t, []string{"step1", "step2", "step3"}, storedOutput.Details, "Details array not correctly serialized")
		assert.False(t, storedOutput.ProcessedAt.IsZero(), "ProcessedAt timestamp should not be zero")
	})

	t.Run("genericStepFunction", func(t *testing.T) {
		// Execute the workflow that uses generic step with both string and int
		handle, err := RunWorkflow(dbosCtx, genericStepWorkflow, "test-input")
		require.NoError(t, err, "failed to run workflow with generic step function")

		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result from workflow with generic step function")
		assert.Equal(t, "test-input-processed-42", result, "expected combined result from both generic steps")

		// Verify both steps were recorded
		steps, err := GetWorkflowSteps(dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps")
		require.Len(t, steps, 2, "expected 2 steps (one for string, one for int)")
		assert.NotEmpty(t, steps[0].StepName, "first step name should not be empty")
		assert.NotEmpty(t, steps[1].StepName, "second step name should not be empty")
	})

	t.Run("StepIDNotReallocatedOnDBRetry", func(t *testing.T) {
		wfID := "step-id-drift-test"

		stepsFaultPool.arm(wfID)
		defer stepsFaultPool.arm("")

		handle, err := RunWorkflow(dbosCtx, sleepStepIDDriftWorkflow, "", WithWorkflowID(wfID))
		require.NoError(t, err)
		result, err := handle.GetResult()
		require.NoError(t, err)
		require.Equal(t, "ok", result)
		require.True(t, stepsFaultPool.fired.Load(), "fault injection never triggered; the test did not exercise a retry")

		steps, err := GetWorkflowSteps(dbosCtx, wfID)
		require.NoError(t, err)
		require.Len(t, steps, 1, "expected exactly one recorded step (DBOS.sleep)")
		require.Equal(t, "DBOS.sleep", steps[0].StepName)
		require.Equal(t, 0, steps[0].StepID,
			"step ID was reallocated by the DB-layer retry: nextStepID() must be called outside the retried closure")
	})

	t.Run("CancelledStepNotCheckpointed", func(t *testing.T) {
		// A step interrupted by workflow cancellation must not checkpoint its
		// cancellation error, so resume re-executes it instead of replaying the error.
		cancelCtx, cancelFunc := WithTimeout(dbosCtx, 5*time.Hour)
		defer cancelFunc()
		handle, err := RunWorkflow(cancelCtx, interruptibleStepWorkflow, "")
		require.NoError(t, err, "failed to start workflow")

		interruptedStepStarted.Wait()
		cancelFunc()

		_, err = handle.GetResult()
		require.Error(t, err, "expected error from cancelled workflow")
		require.True(t, errors.Is(err, ErrWorkflowCancelled), "expected ErrorCodeWorkflowCancelled error, got: %v", err)
		require.True(t, errors.Is(err, context.Canceled), "expected wrapped context.Canceled, got: %v", err)

		require.Eventually(t, func() bool {
			status, err := handle.GetStatus()
			require.NoError(t, err, "failed to get workflow status")
			return status.Status == WorkflowStatusCancelled
		}, 5*time.Second, 100*time.Millisecond, "workflow did not reach cancelled status in time")

		steps, err := GetWorkflowSteps(dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps")
		require.Len(t, steps, 0, "step interrupted by cancellation must not be recorded")

		resumedHandle, err := ResumeWorkflow[string](dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err, "failed to resume workflow")
		result, err := resumedHandle.GetResult()
		require.NoError(t, err, "resumed workflow should complete successfully")
		require.Equal(t, "completed", result)
		require.EqualValues(t, 2, interruptedStepAttempts.Load(), "expected the step to re-execute on resume")

		steps, err = GetWorkflowSteps(dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps after resume")
		require.Len(t, steps, 1, "expected the re-executed step to be recorded")
	})

	t.Run("CancelledParentCancelsChild", func(t *testing.T) {
		// Cancelling the parent durably cancels the child too: a cancelled run
		// never writes its outcome, even if its function ignores cancellation and
		// returns successfully. The parent checkpoints the child's cancellation
		// via getResult; resuming the parent replays it deterministically.
		cancelCtx, cancelFunc := WithCancel(dbosCtx)
		defer cancelFunc()
		handle, err := RunWorkflow(cancelCtx, stubbornParentWorkflow, "")
		require.NoError(t, err, "failed to start parent workflow")

		stubbornChildStarted.Wait()
		cancelFunc()
		close(stubbornChildRelease)

		_, err = handle.GetResult()
		require.Error(t, err, "expected error from cancelled parent")
		require.True(t, errors.Is(err, ErrAwaitedWorkflowCancelled), "expected ErrorCodeAwaitedWorkflowCancelled, got: %v", err)

		require.Eventually(t, func() bool {
			status, err := handle.GetStatus()
			require.NoError(t, err, "failed to get workflow status")
			return status.Status == WorkflowStatusCancelled
		}, 5*time.Second, 100*time.Millisecond, "parent did not reach cancelled status in time")

		steps, err := GetWorkflowSteps(dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps")
		require.Len(t, steps, 2, "expected child spawn and getResult recorded")
		childID := steps[0].ChildWorkflowID
		require.NotEmpty(t, childID, "expected the first step to be the child spawn")
		require.Equal(t, "DBOS.getResult", steps[1].StepName)
		require.NotNil(t, steps[1].Error, "the child's cancellation must be checkpointed")

		childHandle, err := RetrieveWorkflow[string](dbosCtx, childID)
		require.NoError(t, err, "failed to retrieve child workflow")
		childStatus, err := childHandle.GetStatus()
		require.NoError(t, err, "failed to get child workflow status")
		require.Equal(t, WorkflowStatusCancelled, childStatus.Status, "child cannot outlive the parent's cancellation")

		// The checkpointed child cancellation is a terminal outcome for the
		// parent: resuming replays it.
		resumedHandle, err := ResumeWorkflow[string](dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err, "failed to resume parent workflow")
		_, err = resumedHandle.GetResult()
		require.Error(t, err, "resumed parent must replay the checkpointed child cancellation")
		require.True(t, errors.Is(err, ErrAwaitedWorkflowCancelled), "expected ErrorCodeAwaitedWorkflowCancelled on replay, got: %v", err)
		require.EqualValues(t, 1, stubbornChildExecutions.Load(), "child must not re-execute on parent resume")

		status, err := resumedHandle.GetStatus()
		require.NoError(t, err, "failed to get resumed workflow status")
		require.Equal(t, WorkflowStatusError, status.Status, "replayed child cancellation is a terminal error outcome")
	})

	t.Run("PreemptedChildCancellationNotCheckpointed", func(t *testing.T) {
		// Parent cancelled while awaiting a child that accepts preemption: the
		// child's cancellation error passes through getResult and must not be
		// checkpointed, so the parent resumes at the await. After resuming the
		// child, resuming the parent re-awaits and gets the child's real outcome.
		cancelCtx, cancelFunc := WithCancel(dbosCtx)
		defer cancelFunc()
		handle, err := RunWorkflow(cancelCtx, awaitingParentWorkflow, "")
		require.NoError(t, err, "failed to start parent workflow")

		awaitedChildStarted.Wait()
		cancelFunc()

		_, err = handle.GetResult()
		require.Error(t, err, "expected error from cancelled parent")
		// The durable cancel lands in the DB as soon as the context is cancelled,
		// so the parent is interrupted either by the delivered child cancellation
		// or by observing its own CANCELLED status at the step boundary.
		require.True(t, errors.Is(err, ErrWorkflowCancelled), "expected ErrorCodeWorkflowCancelled error, got: %v", err)

		require.Eventually(t, func() bool {
			status, err := handle.GetStatus()
			require.NoError(t, err, "failed to get workflow status")
			return status.Status == WorkflowStatusCancelled
		}, 5*time.Second, 100*time.Millisecond, "parent did not reach cancelled status in time")

		steps, err := GetWorkflowSteps(dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps")
		require.Len(t, steps, 1, "only the child spawn must be recorded, not the preempted await")
		childID := steps[0].ChildWorkflowID
		require.NotEmpty(t, childID, "expected the recorded step to be the child spawn")

		childHandle, err := RetrieveWorkflow[string](dbosCtx, childID)
		require.NoError(t, err, "failed to retrieve child workflow")
		require.Eventually(t, func() bool {
			status, err := childHandle.GetStatus()
			require.NoError(t, err, "failed to get child workflow status")
			return status.Status == WorkflowStatusCancelled
		}, 5*time.Second, 100*time.Millisecond, "child did not reach cancelled status in time")

		// Resume the child, then the parent: the re-executed await must return
		// the child's real outcome.
		resumedChild, err := ResumeWorkflow[string](dbosCtx, childID)
		require.NoError(t, err, "failed to resume child workflow")
		childResult, err := resumedChild.GetResult()
		require.NoError(t, err, "resumed child should complete")
		require.Equal(t, "child-result", childResult)

		resumedHandle, err := ResumeWorkflow[string](dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err, "failed to resume parent workflow")
		result, err := resumedHandle.GetResult()
		require.NoError(t, err, "resumed parent should complete with the child's outcome")
		require.Equal(t, "child-result", result)
		require.EqualValues(t, 2, awaitedChildExecutions.Load(), "child executes once per attempt, never replays past outcomes")

		steps, err = GetWorkflowSteps(dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps after resume")
		require.Len(t, steps, 2, "expected spawn and the re-executed getResult after resume")
		require.Equal(t, "DBOS.getResult", steps[1].StepName)
		require.Nil(t, steps[1].Error)
	})

	t.Run("CancelledChildOutcomeCheckpointed", func(t *testing.T) {
		// The child is cancelled via the API while the parent stays healthy: the
		// child's cancellation is a terminal outcome for the parent, recorded
		// durably by getResult so replay is deterministic (like the other SDKs).
		handle, err := RunWorkflow(dbosCtx, apiCancelParentWorkflow, "")
		require.NoError(t, err, "failed to start parent workflow")

		apiCancelChildStarted.Wait()
		require.NoError(t, CancelWorkflow(dbosCtx, apiCancelledChildID), "failed to cancel child workflow")
		close(apiCancelChildRelease)

		_, err = handle.GetResult()
		require.Error(t, err, "expected error from parent awaiting a cancelled child")
		require.True(t, errors.Is(err, ErrAwaitedWorkflowCancelled), "expected ErrorCodeAwaitedWorkflowCancelled error, got: %v", err)

		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get parent workflow status")
		require.Equal(t, WorkflowStatusError, status.Status, "healthy parent observing a cancelled child ends in ERROR")

		childHandle, err := RetrieveWorkflow[string](dbosCtx, apiCancelledChildID)
		require.NoError(t, err, "failed to retrieve child workflow")
		childStatus, err := childHandle.GetStatus()
		require.NoError(t, err, "failed to get child workflow status")
		require.Equal(t, WorkflowStatusCancelled, childStatus.Status, "expected child workflow to be cancelled")

		steps, err := GetWorkflowSteps(dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps")
		require.Len(t, steps, 2, "expected the child spawn and the recorded getResult")
		require.Equal(t, "DBOS.getResult", steps[1].StepName)
		require.Error(t, steps[1].Error, "the child's cancellation must be durably recorded")
		require.True(t, errors.Is(steps[1].Error, ErrAwaitedWorkflowCancelled), "expected recorded ErrorCodeAwaitedWorkflowCancelled error, got: %v", steps[1].Error)
	})

	t.Run("ConflictingRunDisarmsDurableCancel", func(t *testing.T) {
		t.Skip("must fix context ownership")
		// When two live executions of the same workflow ID race to checkpoint a
		// step, the loser's function returns ErrorCodeConflictingID and its
		// RunWorkflow goroutine awaits the winner's result. Losing the conflict
		// disproves ownership, so the branch must disarm the durable-cancel
		// AfterFunc right there: a later cancellation of the context the caller
		// used for the losing dispatch (the routine `defer cancelFunc()`
		// pattern) must not durably cancel the owning — or a future resumed —
		// run. The disarm cannot wait for the branch to settle: the loss is
		// observable through polling handles while the loser is still awaiting.
		//
		// The losing execution needs a real second executor: a single
		// in-process guard cannot double-run a workflow (same construction as
		// TestRecvStepConflict). No leak check: its lifetime overlaps dbosCtx's.
		// Pin executor B to the parent's database: sqlite URLs are per-test, so a
		// subtest's setupDBOS would otherwise get a fresh DB with nothing to recover.
		ctxB := setupDBOS(t, setupDBOSOptions{dropDB: false, checkLeaks: false, databaseURL: stepsDatabaseURL})
		RegisterWorkflow(ctxB, conflictCancelWorkflow, WithWorkflowName("conflict-cancel-workflow"))
		// Register the parking queue on executor B but don't listen to it (and
		// never register it on the main executor), so the later resume leaves
		// the workflow durably ENQUEUED — making a spurious cancel observable.
		const parkedQueue = "conflict-cancel-parked-queue"
		_, err := RegisterQueue(ctxB, parkedQueue)
		require.NoError(t, err, "failed to register parking queue")
		ListenQueues(ctxB, "conflict-cancel-unused-queue")
		require.NoError(t, Launch(ctxB), "failed to launch executor B")

		wfID := uuid.NewString()

		// Execution 1 on the main executor: enters the step and blocks.
		handleA, err := RunWorkflow(dbosCtx, conflictCancelWorkflow, "", WithWorkflowID(wfID))
		require.NoError(t, err, "failed to start workflow")
		conflictCancelFirstStarted.Wait()

		// Execution 2 on executor B, dispatched under a user-cancellable
		// context. Recovery dispatch is the sanctioned way to get a genuinely
		// concurrent second execution (a direct RunWorkflow attaches to the
		// owner's run instead).
		cancelCtx, cancelFunc := WithCancel(ctxB)
		defer cancelFunc()
		recovered, err := recoverPendingWorkflows(cancelCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "failed to recover the workflow on executor B")
		require.Len(t, recovered, 1, "expected exactly one pending workflow to recover")
		require.Equal(t, wfID, recovered[0].GetWorkflowID())
		conflictCancelSecondStarted.Wait()

		// Cancel the workflow durably, then let execution 1 finish: its
		// in-flight step checkpoints and the run ends cancelled.
		require.NoError(t, CancelWorkflow(dbosCtx, wfID), "failed to cancel workflow")
		close(conflictCancelReleaseFirst)
		_, _ = handleA.GetResult()

		// Let execution 2 finish: its step-0 checkpoint hits the unique
		// violation and its goroutine takes the conflict-await branch.
		close(conflictCancelReleaseSecond)
		_, err = recovered[0].GetResult()
		require.Error(t, err, "the losing execution must observe the cancelled outcome")
		require.True(t, errors.Is(err, ErrAwaitedWorkflowCancelled) || errors.Is(err, ErrWorkflowCancelled),
			"expected a cancellation error from the losing execution, got: %v", err)
		require.EqualValues(t, 2, conflictCancelExecs.Load(), "both executions must have genuinely run the step body")

		// Resume the workflow onto the unlistened queue: it is durably
		// ENQUEUED, non-terminal again.
		resumedHandle, err := ResumeWorkflow[string](ctxB, wfID, WithResumeQueue(parkedQueue))
		require.NoError(t, err, "failed to resume workflow")
		status, err := resumedHandle.GetStatus()
		require.NoError(t, err, "failed to get resumed workflow status")
		require.Equal(t, WorkflowStatusEnqueued, status.Status, "precondition: resumed workflow is ENQUEUED")

		// Cancel the context used for the conflicting dispatch. That run lost
		// the conflict and never owned this workflow: the resumed row must not
		// be touched. A still-armed AfterFunc would durably cancel it within
		// milliseconds.
		cancelFunc()

		// Poll synchronously rather than with require.Never: testify runs each
		// tick in a goroutine and returns at the timeout without awaiting an
		// in-flight tick, so a straggler GetStatus can race the subtest's
		// ctxB shutdown in t.Cleanup and fail with "context canceled".
		for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); {
			status, err := resumedHandle.GetStatus()
			require.NoError(t, err, "failed to get workflow status")
			require.NotEqual(t, WorkflowStatusCancelled, status.Status,
				"cancelling the stale conflicting-dispatch context must not durably cancel the resumed workflow")
			time.Sleep(100 * time.Millisecond)
		}

		// Drain: start listening to the parking queue so the resumed workflow
		// completes (replaying the checkpointed step), which also lets the
		// loser's conflict-await goroutine finish before executor B shuts down.
		ListenQueues(ctxB, parkedQueue)
		result, err := resumedHandle.GetResult()
		require.NoError(t, err, "resumed workflow should complete")
		require.Equal(t, "ok", result)
	})

	t.Run("RecordOperationResultIdempotency", func(t *testing.T) {
		handle, err := RunWorkflow(dbosCtx, recordOpResultWf, "")
		require.NoError(t, err, "failed to start workflow")
		res, err := handle.GetResult()
		require.NoError(t, err, "failed to get workflow result")
		require.Equal(t, "step-output", res)

		wfID := handle.GetWorkflowID()
		sysDB, ok := dbosCtx.(*dbosContext).systemDB.(*sysdb.SysDB)
		require.True(t, ok, "expected sysDB instance")
		ctx := context.Background()

		// Read the step-0 checkpoint exactly as recorded, so the test can replay a
		// byte-identical write (the lost-commit-ack retry) without depending on
		// name-resolution or serialization details.
		var recordedName string
		var recordedOutput *string
		var recordedSerialization *string
		var recordedStartedAtMs, recordedCompletedAtMs int64
		q := sysDB.RenderSQL(`SELECT function_name, output, serialization, started_at_epoch_ms, completed_at_epoch_ms
			FROM %soperation_outputs WHERE workflow_uuid = $1 AND function_id = 0`, sysDB.Dialect().SchemaPrefix(sysDB.Schema()))
		require.NoError(t, sysDB.Pool().QueryRow(ctx, q, wfID).Scan(
			&recordedName, &recordedOutput, &recordedSerialization, &recordedStartedAtMs, &recordedCompletedAtMs))
		require.NotNil(t, recordedOutput)
		require.NotNil(t, recordedSerialization)

		t.Run("IdenticalRetrySucceeds", func(t *testing.T) {
			// Same content and timestamps as the recorded row: this is our own write
			// re-run after a lost commit ack, and must be a no-op success.
			err := sysDB.RecordOperationResult(ctx, sysdb.RecordOperationResultDBInput{
				WorkflowID:    wfID,
				StepID:        0,
				StepName:      recordedName,
				Output:        recordedOutput,
				StartedAt:     time.UnixMilli(recordedStartedAtMs),
				CompletedAt:   time.UnixMilli(recordedCompletedAtMs),
				Serialization: *recordedSerialization,
			})
			require.NoError(t, err, "replaying our own committed write must succeed")
		})

		t.Run("DifferentWriteIsConflict", func(t *testing.T) {
			// Same step, different content/timestamps: a concurrent execution
			// checkpointed first; the caller must park via ErrorCodeConflictingID.
			differentPayload := "different-payload"
			err := sysDB.RecordOperationResult(ctx, sysdb.RecordOperationResultDBInput{
				WorkflowID:    wfID,
				StepID:        0,
				StepName:      recordedName,
				Output:        &differentPayload,
				StartedAt:     time.Now(),
				CompletedAt:   time.Now(),
				Serialization: *recordedSerialization,
			})
			require.Error(t, err, "a different write at a recorded step must be a conflict")
			var dbosErr *Error
			require.ErrorAs(t, err, &dbosErr)
			require.Equal(t, ErrorCodeConflictingID, dbosErr.Code)
		})

		t.Run("FreshRecordSucceeds", func(t *testing.T) {
			payload := "fresh-payload"
			err := sysDB.RecordOperationResult(ctx, sysdb.RecordOperationResultDBInput{
				WorkflowID:    wfID,
				StepID:        99,
				StepName:      "someStep",
				Output:        &payload,
				StartedAt:     time.Now(),
				CompletedAt:   time.Now(),
				Serialization: "json",
			})
			require.NoError(t, err)
		})

		t.Run("MismatchedNameIsNonDeterminismError", func(t *testing.T) {
			err := sysDB.RecordOperationResult(ctx, sysdb.RecordOperationResultDBInput{
				WorkflowID:    wfID,
				StepID:        0,
				StepName:      recordedName + "-different",
				StartedAt:     time.Now(),
				CompletedAt:   time.Now(),
				Serialization: "json",
			})
			require.Error(t, err, "a different step name at a recorded step must be a non-determinism error")
			var dbosErr *Error
			require.ErrorAs(t, err, &dbosErr)
			require.Equal(t, ErrorCodeUnexpectedStep, dbosErr.Code)
			require.Equal(t, recordedName+"-different", dbosErr.ExpectedName)
			require.Equal(t, recordedName, dbosErr.RecordedName)
		})
	})

	t.Run("StepOptionsRetryIntervalDefaults", func(t *testing.T) {
		tests := []struct {
			name         string
			baseInterval time.Duration
			maxInterval  time.Duration
			wantBase     time.Duration
			wantMax      time.Duration
			wantWarning  bool
		}{
			{
				name:     "both implicit",
				wantBase: _DEFAULT_STEP_BASE_INTERVAL,
				wantMax:  _DEFAULT_STEP_MAX_INTERVAL,
			},
			{
				name:         "explicit base below implicit max",
				baseInterval: time.Second,
				wantBase:     time.Second,
				wantMax:      _DEFAULT_STEP_MAX_INTERVAL,
			},
			{
				name:         "explicit base equals implicit max",
				baseInterval: _DEFAULT_STEP_MAX_INTERVAL,
				wantBase:     _DEFAULT_STEP_MAX_INTERVAL,
				wantMax:      _DEFAULT_STEP_MAX_INTERVAL,
			},
			{
				name:         "explicit base exceeds implicit max",
				baseInterval: 2 * _DEFAULT_STEP_MAX_INTERVAL,
				wantBase:     2 * _DEFAULT_STEP_MAX_INTERVAL,
				wantMax:      2 * _DEFAULT_STEP_MAX_INTERVAL,
				wantWarning:  true,
			},
			{
				name:        "implicit base and explicit max",
				maxInterval: time.Second,
				wantBase:    _DEFAULT_STEP_BASE_INTERVAL,
				wantMax:     time.Second,
			},
			{
				name:         "explicit max below explicit base remains explicit",
				baseInterval: 2 * time.Second,
				maxInterval:  time.Second,
				wantBase:     2 * time.Second,
				wantMax:      time.Second,
			},
			{
				name:         "explicit max above explicit base",
				baseInterval: 2 * _DEFAULT_STEP_MAX_INTERVAL,
				maxInterval:  3 * _DEFAULT_STEP_MAX_INTERVAL,
				wantBase:     2 * _DEFAULT_STEP_MAX_INTERVAL,
				wantMax:      3 * _DEFAULT_STEP_MAX_INTERVAL,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				var logOutput bytes.Buffer
				logger := slog.New(slog.NewTextHandler(&logOutput, nil))
				opts := stepOptions{
					baseInterval: tt.baseInterval,
					maxInterval:  tt.maxInterval,
				}

				opts.setDefaults(logger)

				require.Equal(t, tt.wantBase, opts.baseInterval)
				require.Equal(t, tt.wantMax, opts.maxInterval)
				require.Equal(t, tt.wantWarning, strings.Contains(logOutput.String(), "increasing max interval"))
			})
		}
	})
}

func stepReturningStepID(ctx context.Context) (int, error) {
	stepID, err := GetStepID(ctx.(Context))
	if err != nil {
		return -1, err
	}
	return stepID, nil
}

func TestGoRunningStepsInsideGoRoutines(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	t.Run("Go must run steps inside a workflow", func(t *testing.T) {
		_, err := Go(dbosCtx, func(ctx context.Context) (string, error) {
			return stepWithSleep(ctx, 1*time.Second)
		})
		require.Error(t, err, "expected error when running step outside of workflow context, but got none")

		dbosErr, ok := err.(*Error)
		require.True(t, ok, "expected error to be of type *Error, got %T", err)
		require.Equal(t, ErrorCodeStepExecution, dbosErr.Code)
		expectedMessagePart := "workflow state not found in context: are you running this step within a workflow?"
		require.Contains(t, err.Error(), expectedMessagePart, "expected error message to contain %q, but got %q", expectedMessagePart, err.Error())
	})

	goErrWorkflow := func(dbosCtx Context, input string) (string, error) {
		result, _ := Go(dbosCtx, func(ctx context.Context) (string, error) {
			return "", fmt.Errorf("step error")
		})

		resultChan := <-result
		return resultChan.Result, resultChan.Err
	}
	RegisterWorkflow(dbosCtx, goErrWorkflow)

	const numSteps = 100
	resultChans := make([]<-chan StepOutcome[int], 0)
	goManyStepsWorkflow := func(dbosCtx Context, input string) (string, error) {
		for range numSteps {
			resultChan, err := Go(dbosCtx, func(ctx context.Context) (int, error) {
				return stepReturningStepID(ctx)
			})

			if err != nil {
				return "", err
			}
			resultChans = append(resultChans, resultChan)
		}

		return "", nil
	}
	RegisterWorkflow(dbosCtx, goManyStepsWorkflow)

	goIdempotencyWorkflow := func(dbosCtx Context, input string) (string, error) {
		channels := make([]<-chan StepOutcome[string], 0, 10)
		for i := range 10 {
			ch, err := Go(dbosCtx, func(ctx context.Context) (string, error) {
				return stepWithSleep(ctx, 1*time.Second)
			}, WithStepName(fmt.Sprintf("goStep-%d", i)))
			if err != nil {
				return "", err
			}
			channels = append(channels, ch)
		}
		for _, ch := range channels {
			outcome := <-ch
			if outcome.Err != nil {
				return "", outcome.Err
			}
		}
		return "ok", nil
	}
	RegisterWorkflow(dbosCtx, goIdempotencyWorkflow)

	require.NoError(t, Launch(dbosCtx), "failed to launch DBOS")

	t.Run("Go must return step error correctly", func(t *testing.T) {
		handle, err := RunWorkflow(dbosCtx, goErrWorkflow, "test-input")
		require.NoError(t, err, "failed to run go workflow")
		_, err = handle.GetResult()
		require.Error(t, err, "expected error when running step, but got none")
		require.Equal(t, "step error", err.Error())
	})

	t.Run("Go must execute 100 steps simultaneously then return the stepIDs in the correct sequence", func(t *testing.T) {
		handle, err := RunWorkflow(dbosCtx, goManyStepsWorkflow, "test-input")
		require.NoError(t, err, "failed to run go workflow")
		_, err = handle.GetResult()
		require.NoError(t, err, "failed to get result from go workflow")
		assert.Equal(t, len(resultChans), numSteps, "expected %d results, got %d", numSteps, len(resultChans))
		for i, resultChan := range resultChans {
			res := <-resultChan
			assert.Equal(t, i, res.Result, "expected step ID to be %d, got %d", i, res.Result)
			assert.NoError(t, res.Err, "expected no error, got %v", res.Err)

			res2, ok := <-resultChan
			assert.False(t, ok, "channel should be closed after receiving result")
			assert.Equal(t, StepOutcome[int]{}, res2, "closed channel should return zero value")
		}
	})

	t.Run("Go idempotency", func(t *testing.T) {
		workflowID := uuid.NewString()
		handle1, err := RunWorkflow(dbosCtx, goIdempotencyWorkflow, "test-input", WithWorkflowID(workflowID))
		require.NoError(t, err, "failed to run go workflow")
		result1, err := handle1.GetResult()
		require.NoError(t, err, "failed to get result from first run")

		setWorkflowStatusPending(t, dbosCtx, workflowID)

		// Restart the workflow from scratch with the same ID; expect a normal handle and same result
		handles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "failed to recover pending workflows")
		require.Len(t, handles, 1, "expected 1 recovered handle")
		require.Equal(t, workflowID, handles[0].GetWorkflowID(), "expected recovered handle to have the same ID as the original workflow")
		handle2 := handles[0]
		result2, err := handle2.GetResult()
		require.NoError(t, err, "failed to get result from second run")
		require.Equal(t, result1, result2, "both runs should return the same result")
	})
}

func TestSelect(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	selectWorkflow := func(dbosCtx Context, input string) (string, error) {
		return Select(dbosCtx, []<-chan StepOutcome[string]{})
	}
	RegisterWorkflow(dbosCtx, selectWorkflow)

	selectBlockStartEvent := NewEvent()
	selectBlockEvent := NewEvent()
	selectGoStepStarted := NewEvent()
	selectCancelWorkflow := func(dbosCtx Context, input string) (string, error) {
		ch1, err := Go(dbosCtx, func(ctx context.Context) (string, error) {
			// Signal the step body has started (its checkpoint lookup passed), so
			// the test can cancel without racing the durable cancel against it.
			selectGoStepStarted.Set()
			selectBlockEvent.Wait()
			return "result", nil
		})
		if err != nil {
			return "", err
		}

		selectBlockStartEvent.Set()
		// Select will block waiting for the channel, but context cancellation should interrupt it
		return Select(dbosCtx, []<-chan StepOutcome[string]{ch1})
	}
	RegisterWorkflow(dbosCtx, selectCancelWorkflow)

	selectIdempotencyWorkflow := func(dbosCtx Context, input string) (string, error) {
		ch1, err := Go(dbosCtx, func(ctx context.Context) (string, error) {
			return "result1", nil
		})
		if err != nil {
			return "", err
		}
		ch2, err := Go(dbosCtx, func(ctx context.Context) (string, error) {
			return "result2", nil
		})
		if err != nil {
			return "", err
		}
		selectedResult, err := Select(dbosCtx, []<-chan StepOutcome[string]{ch1, ch2})
		if err != nil {
			return "", err
		}
		return selectedResult, nil
	}
	RegisterWorkflow(dbosCtx, selectIdempotencyWorkflow)

	dbosCtx.Launch()

	t.Run("Select must run inside a workflow", func(t *testing.T) {
		ch1, _ := Go(dbosCtx, func(ctx context.Context) (string, error) {
			return "result1", nil
		})
		channels := []<-chan StepOutcome[string]{ch1}
		_, err := Select(dbosCtx, channels)
		require.Error(t, err, "expected error when running Select outside of workflow context, but got none")

		dbosErr, ok := err.(*Error)
		require.True(t, ok, "expected error to be of type *Error, got %T", err)
		require.Equal(t, ErrorCodeStepExecution, dbosErr.Code)
		expectedMessagePart := "workflow state not found in context: are you running this step within a workflow?"
		require.Contains(t, err.Error(), expectedMessagePart, "expected error message to contain %q, but got %q", expectedMessagePart, err.Error())
	})

	t.Run("Select with empty channels slice", func(t *testing.T) {
		handle, err := RunWorkflow(dbosCtx, selectWorkflow, "test-input")
		require.NoError(t, err, "failed to run select workflow")
		result, err := handle.GetResult()
		require.NoError(t, err, "expected no error for empty channels")
		// Should return zero value string
		assert.Equal(t, "", result)

		// Verify DBOS.select step is present (empty channels don't create a step, so no steps expected)
		steps, err := GetWorkflowSteps(dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps")
		require.Len(t, steps, 0, "expected no steps for empty channels slice")
	})

	t.Run("Select with context cancellation", func(t *testing.T) {
		// Create a cancellable context
		cancelCtx, cancelFunc := WithCancelCause(dbosCtx)
		defer cancelFunc(nil)

		// Run the workflow with the cancellable context
		handle, err := RunWorkflow(cancelCtx, selectCancelWorkflow, "test-input")
		require.NoError(t, err, "failed to run select workflow")

		// Wait for the workflow to reach the Select call (step has started and set the event)
		selectBlockStartEvent.Wait()
		selectBlockStartEvent.Clear()
		// Wait for the Go step body to start: once it runs, its outcome is delivered
		// and checkpointed even though the workflow is cancelled. Cancelling earlier
		// would race the durable cancel against the step's checkpoint lookup, which
		// can refuse to start the step at all (a valid outcome, but not this test's).
		selectGoStepStarted.Wait()
		selectGoStepStarted.Clear()

		// Cancel the context manually
		cancelFunc(nil)

		// Verify that Select returns with a cancellation error
		result, err := handle.GetResult()
		require.Error(t, err, "expected error from cancelled workflow")
		assert.Equal(t, "", result, "expected zero value string when cancelled")

		// Verify the error is a cancellation error. The durable cancel lands in the
		// DB as soon as the context is cancelled, so Select is interrupted either
		// mid-wait (wrapping context.Canceled) or at its step boundary by observing
		// the CANCELLED status; both wrap ErrorCodeWorkflowCancelled.
		assert.True(t, errors.Is(err, ErrWorkflowCancelled), "expected ErrorCodeWorkflowCancelled error, got: %v", err)

		// Set the event to unblock the goroutine (cleanup)
		selectBlockEvent.Set()

		// Verify workflow status is cancelled (the workflow was interrupted by context cancellation)
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		assert.Equal(t, WorkflowStatusCancelled, status.Status, "expected workflow status to be WorkflowStatusCancelled")

		// The cancelled Select step must not be checkpointed (it would replay its
		// cancellation error on resume); only the Go step, unblocked above, records.
		require.Eventually(t, func() bool {
			steps, err := GetWorkflowSteps(dbosCtx, handle.GetWorkflowID())
			if err != nil {
				return false
			}
			return len(steps) == 1 && steps[0].StepID == 0
		}, 5*time.Second, 100*time.Millisecond, "expected only the Go step to be recorded")
	})

	t.Run("Select idempotency", func(t *testing.T) {
		workflowID := uuid.NewString()
		handle1, err := RunWorkflow(dbosCtx, selectIdempotencyWorkflow, "test-input", WithWorkflowID(workflowID))
		require.NoError(t, err, "failed to run select workflow")
		result1, err := handle1.GetResult()
		require.NoError(t, err, "failed to get result from first run")

		// Restart with the same ID ten times; each time recover via recoverPendingWorkflows and expect same result
		for i := range 10 {
			setWorkflowStatusPending(t, dbosCtx, workflowID)
			handles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
			require.NoError(t, err, "failed to recover pending workflows (iteration %d)", i+1)
			require.Len(t, handles, 1, "expected 1 recovered handle (iteration %d)", i+1)
			handle2 := handles[0]
			require.Equal(t, workflowID, handle2.GetWorkflowID(), "expected recovered handle to have the same ID as the original workflow")
			result2, err := handle2.GetResult()
			require.NoError(t, err, "failed to get result from run (iteration %d)", i+1)
			require.Equal(t, result1, result2, "run (iteration %d) should return the same result", i+1)
		}

		// Verify steps after execution: two Go steps and one Select step
		steps, err := GetWorkflowSteps(dbosCtx, workflowID)
		require.NoError(t, err, "failed to get workflow steps")
		require.Len(t, steps, 3, "expected 3 steps (2 Go + Select)")
		assert.Equal(t, 0, steps[0].StepID, "first step should have StepID 0")
		assert.Equal(t, 1, steps[1].StepID, "second step should have StepID 1")
		assert.Equal(t, "DBOS.select", steps[2].StepName, "third step should be DBOS.select")
		assert.Equal(t, 2, steps[2].StepID, "Select step should have StepID 2")
		var output0 string
		err = json.Unmarshal([]byte(steps[0].Output.(string)), &output0)
		require.NoError(t, err, "failed to decode step 0 output")
		assert.Equal(t, "result1", output0, "first Go step should have output 'result1'")
		var output1 string
		err = json.Unmarshal([]byte(steps[1].Output.(string)), &output1)
		require.NoError(t, err, "failed to decode step 1 output")
		assert.Equal(t, "result2", output1, "second Go step should have output 'result2'")
		var output2 string
		err = json.Unmarshal([]byte(steps[2].Output.(string)), &output2)
		require.NoError(t, err, "failed to decode step 2 output")
		assert.Equal(t, result1, output2, "Select step output should match workflow result")
	})
}

func TestChildWorkflow(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	type Inheritance struct {
		ParentID string
		Index    int
	}

	// Create child workflows with executor
	childWf := func(ctx Context, input Inheritance) (string, error) {
		workflowID, err := GetWorkflowID(ctx)
		if err != nil {
			return "", fmt.Errorf("failed to get workflow ID: %w", err)
		}
		expectedCurrentID := fmt.Sprintf("%s-0", input.ParentID)
		if workflowID != expectedCurrentID {
			return "", fmt.Errorf("expected childWf workflow ID to be %s, got %s", expectedCurrentID, workflowID)
		}
		// Steps of a child workflow start with an incremented step ID, because the first step ID is allocated to the child workflow
		return RunAsStep(ctx, func(ctx context.Context) (string, error) {
			return simpleStep(ctx)
		})
	}
	RegisterWorkflow(dbosCtx, childWf)

	parentWf := func(ctx Context, input Inheritance) (string, error) {
		workflowID, err := GetWorkflowID(ctx)
		if err != nil {
			return "", fmt.Errorf("failed to get workflow ID: %w", err)
		}

		childHandle, err := RunWorkflow(ctx, childWf, Inheritance{ParentID: workflowID})
		if err != nil {
			return "", fmt.Errorf("failed to run child workflow: %w", err)
		}

		// Check this wf ID is built correctly
		expectedParentID := fmt.Sprintf("%s-%d", input.ParentID, input.Index)
		if workflowID != expectedParentID {
			return "", fmt.Errorf("expected parentWf workflow ID to be %s, got %s", expectedParentID, workflowID)
		}
		res, err := childHandle.GetResult()
		if err != nil {
			return "", fmt.Errorf("failed to get result from child workflow: %w", err)
		}

		// Check the steps from this workflow
		steps, err := GetWorkflowSteps(ctx, workflowID)
		if err != nil {
			return "", fmt.Errorf("failed to get workflow steps: %w", err)
		}
		if len(steps) != 2 {
			return "", fmt.Errorf("expected 2 recorded steps, got %d", len(steps))
		}
		// Verify the first step is the child workflow
		if steps[0].StepID != 0 {
			return "", fmt.Errorf("expected first step ID to be 0, got %d", steps[0].StepID)
		}
		if steps[0].StepName != runtime.FuncForPC(reflect.ValueOf(childWf).Pointer()).Name() {
			return "", fmt.Errorf("expected first step to be child workflow, got %s", steps[0].StepName)
		}
		if steps[0].Output != nil {
			return "", fmt.Errorf("expected first step output to be nil, got %s", steps[0].Output)
		}
		if steps[1].Error != nil {
			return "", fmt.Errorf("expected second step error to be nil, got %s", steps[1].Error)
		}
		if steps[0].ChildWorkflowID != childHandle.GetWorkflowID() {
			return "", fmt.Errorf("expected first step child workflow ID to be %s, got %s", childHandle.GetWorkflowID(), steps[0].ChildWorkflowID)
		}

		// The second step is the result from the child workflow
		if steps[1].StepID != 1 {
			return "", fmt.Errorf("expected second step ID to be 1, got %d", steps[1].StepID)
		}
		if steps[1].StepName != "DBOS.getResult" {
			return "", fmt.Errorf("expected second step name to be getResult, got %s", steps[1].StepName)
		}
		var stepOutput string
		err = json.Unmarshal([]byte(steps[1].Output.(string)), &stepOutput)
		if err != nil {
			return "", fmt.Errorf("failed to unmarshal step output: %w", err)
		}
		if stepOutput != "from step" {
			return "", fmt.Errorf("expected second step output to be 'from step', got %s", steps[1].Output)
		}
		if steps[1].Error != nil {
			return "", fmt.Errorf("expected second step error to be nil, got %s", steps[1].Error)
		}
		if steps[1].ChildWorkflowID != childHandle.GetWorkflowID() {
			return "", fmt.Errorf("expected second step child workflow ID to be %s, got %s", childHandle.GetWorkflowID(), steps[1].ChildWorkflowID)
		}

		return res, nil
	}
	RegisterWorkflow(dbosCtx, parentWf)

	grandParentWf := func(ctx Context, r int) (string, error) {
		workflowID, err := GetWorkflowID(ctx)
		if err != nil {
			return "", fmt.Errorf("failed to get workflow ID: %w", err)
		}

		// 2 steps per loop: spawn child and get result
		for i := range r {
			expectedStepID := (2 * i)
			parentHandle, err := RunWorkflow(ctx, parentWf, Inheritance{ParentID: workflowID, Index: expectedStepID})
			if err != nil {
				return "", fmt.Errorf("failed to run parent workflow: %w", err)
			}

			// Verify parent (this workflow's child) ID follows the pattern: parentID-functionID
			parentWorkflowID := parentHandle.GetWorkflowID()

			expectedParentID := fmt.Sprintf("%s-%d", workflowID, expectedStepID)
			if parentWorkflowID != expectedParentID {
				return "", fmt.Errorf("expected parent workflow ID to be %s, got %s", expectedParentID, parentWorkflowID)
			}

			result, err := parentHandle.GetResult()
			if err != nil {
				return "", fmt.Errorf("failed to get result from parent workflow: %w", err)
			}
			if result != "from step" {
				return "", fmt.Errorf("expected result from parent workflow to be 'from step', got %s", result)
			}

		}
		// Check the steps from this workflow
		steps, err := GetWorkflowSteps(ctx, workflowID)
		if err != nil {
			return "", fmt.Errorf("failed to get workflow steps: %w", err)
		}
		if len(steps) != r*2 {
			return "", fmt.Errorf("expected 2 recorded steps, got %d", len(steps))
		}

		// We do expect the steps to be returned in the order of execution, which seems to be the case even without an ORDER BY function_id ASC clause in the SQL query
		for i := 0; i < r; i += 2 {
			expectedStepID := i
			expectedChildID := fmt.Sprintf("%s-%d", workflowID, i)
			childWfStep := steps[i]
			getResultStep := steps[i+1]

			if childWfStep.StepID != expectedStepID {
				return "", fmt.Errorf("expected child wf step ID to be %d, got %d", expectedStepID, childWfStep.StepID)
			}
			if getResultStep.StepID != expectedStepID+1 {
				return "", fmt.Errorf("expected get result step ID to be %d, got %d", expectedStepID+1, getResultStep.StepID)
			}
			expectedName := runtime.FuncForPC(reflect.ValueOf(parentWf).Pointer()).Name()
			if childWfStep.StepName != expectedName {
				return "", fmt.Errorf("expected child wf step name to be %s, got %s", expectedName, childWfStep.StepName)
			}
			expectedName = "DBOS.getResult"
			if getResultStep.StepName != expectedName {
				return "", fmt.Errorf("expected get result step name to be %s, got %s", expectedName, getResultStep.StepName)
			}

			if childWfStep.Output != nil {
				return "", fmt.Errorf("expected child wf step output to be nil, got %s", childWfStep.Output)
			}
			var stepOutput string
			err = json.Unmarshal([]byte(getResultStep.Output.(string)), &stepOutput)
			if err != nil {
				return "", fmt.Errorf("failed to unmarshal step output: %w", err)
			}
			if stepOutput != "from step" {
				return "", fmt.Errorf("expected get result step output to be 'from step', got %s", getResultStep.Output)
			}

			if childWfStep.Error != nil {
				return "", fmt.Errorf("expected child wf step error to be nil, got %s", childWfStep.Error)
			}
			if getResultStep.Error != nil {
				return "", fmt.Errorf("expected get result step error to be nil, got %s", getResultStep.Error)
			}
			if childWfStep.ChildWorkflowID != expectedChildID {
				return "", fmt.Errorf("expected step child workflow ID to be %s, got %s", expectedChildID, childWfStep.ChildWorkflowID)
			}
			if getResultStep.ChildWorkflowID != expectedChildID {
				return "", fmt.Errorf("expected step child workflow ID to be %s, got %s", expectedChildID, getResultStep.ChildWorkflowID)
			}
		}

		return "", nil
	}
	RegisterWorkflow(dbosCtx, grandParentWf)

	// Register workflows needed for ChildWorkflowWithCustomID test
	simpleChildWf := func(dbosCtx Context, input string) (string, error) {
		return RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
			return simpleStep(ctx)
		})
	}
	RegisterWorkflow(dbosCtx, simpleChildWf)

	// Register workflows needed for RecoveredChildWorkflowPollingHandle test
	var pollingHandleCompleteEvent *Event
	pollingHandleChildWf := func(dbosCtx Context, input string) (string, error) {
		// Wait if event is set
		if pollingHandleCompleteEvent != nil {
			pollingHandleCompleteEvent.Wait()
		}
		return input + "-result", nil
	}
	RegisterWorkflow(dbosCtx, pollingHandleChildWf)

	var pollingCounter int
	var pollingHandleStartEvent *Event
	pollingHandleParentWf := func(ctx Context, input string) (string, error) {
		pollingCounter++

		// Run child workflow with a known ID
		childHandle, err := RunWorkflow(ctx, pollingHandleChildWf, "child-input", WithWorkflowID("known-child-workflow-id"))
		if err != nil {
			return "", fmt.Errorf("failed to run child workflow: %w", err)
		}

		switch pollingCounter {
		case 1:
			// First handle will be a direct handle
			_, ok := childHandle.(*workflowHandle[string])
			if !ok {
				return "", fmt.Errorf("expected child handle to be of type workflowDirectHandle, got %T", childHandle)
			}
			// Signal the child workflow is started
			if pollingHandleStartEvent != nil {
				pollingHandleStartEvent.Set()
			}

			result, err := childHandle.GetResult()
			if err != nil {
				return "", fmt.Errorf("failed to get result from child workflow: %w", err)
			}
			return result, nil
		case 2:
			// Second handle will be a polling handle
			_, ok := childHandle.(*workflowPollingHandle[string])
			if !ok {
				return "", fmt.Errorf("expected recovered child handle to be of type workflowPollingHandle, got %T", childHandle)
			}
		}
		return "", nil
	}
	RegisterWorkflow(dbosCtx, pollingHandleParentWf)

	// Register workflows needed for ChildWorkflowCannotBeSpawnedFromStep test
	childWfForStepTest := func(dbosCtx Context, input string) (string, error) {
		return "child-result", nil
	}
	RegisterWorkflow(dbosCtx, childWfForStepTest)

	parentWfForStepTest := func(ctx Context, input string) (string, error) {
		return RunAsStep(ctx, func(context context.Context) (string, error) {
			dbosCtx := context.(Context)
			_, err := RunWorkflow(dbosCtx, childWfForStepTest, input)
			if err != nil {
				return "", err
			}
			return "should-not-reach", nil
		})
	}
	RegisterWorkflow(dbosCtx, parentWfForStepTest)
	// Simple parent that starts one child with a custom workflow ID
	simpleParentWf := func(ctx Context, customChildID string) (string, error) {
		childHandle, err := RunWorkflow(ctx, simpleChildWf, "test-child-input", WithWorkflowID(customChildID))
		if err != nil {
			return "", fmt.Errorf("failed to run child workflow: %w", err)
		}

		result, err := childHandle.GetResult()
		if err != nil {
			return "", fmt.Errorf("failed to get result from child workflow: %w", err)
		}

		return result, nil
	}

	RegisterWorkflow(dbosCtx, simpleParentWf)

	// Workflows for deletion tests
	deleteBlockEvent := NewEvent()
	deleteBlockingWf := func(ctx Context, _ string) (string, error) {
		deleteBlockEvent.Wait()
		return "done", nil
	}
	RegisterWorkflow(dbosCtx, deleteBlockingWf)

	// Leaf workflow for delete topology tests
	deleteLeafWf := func(ctx Context, input string) (string, error) {
		return "leaf:" + input, nil
	}
	RegisterWorkflow(dbosCtx, deleteLeafWf)

	// Mid-layer workflow: spawns 2 leaves
	deleteMidWf := func(ctx Context, input string) (string, error) {
		for i := 0; i < 2; i++ {
			childID := fmt.Sprintf("%s-leaf-%d", input, i)
			h, err := RunWorkflow(ctx, deleteLeafWf, input, WithWorkflowID(childID))
			if err != nil {
				return "", err
			}
			if _, err := h.GetResult(); err != nil {
				return "", err
			}
		}
		return "mid:" + input, nil
	}
	RegisterWorkflow(dbosCtx, deleteMidWf)

	// Root workflow: spawns 2 mid-layer children
	deleteRootWf := func(ctx Context, input string) (string, error) {
		for i := 0; i < 2; i++ {
			childID := fmt.Sprintf("%s-mid-%d", input, i)
			h, err := RunWorkflow(ctx, deleteMidWf, childID, WithWorkflowID(childID))
			if err != nil {
				return "", err
			}
			if _, err := h.GetResult(); err != nil {
				return "", err
			}
		}
		return "root:" + input, nil
	}
	RegisterWorkflow(dbosCtx, deleteRootWf)

	// Workflow for cascade data deletion test
	deleteCascadeWf := func(ctx Context, _ string) (string, error) {
		if err := SetEvent(ctx, "cascade-key", "cascade-value"); err != nil {
			return "", err
		}
		if err := WriteStream(ctx, "cascade-stream", "stream-data"); err != nil {
			return "", err
		}
		if err := CloseStream(ctx, "cascade-stream"); err != nil {
			return "", err
		}
		// Recv the notification so it's recorded as a step
		_, err := Recv[string](ctx, "test-topic", 10*time.Second)
		if err != nil {
			return "", err
		}
		return "done", nil
	}
	RegisterWorkflow(dbosCtx, deleteCascadeWf)

	t.Cleanup(func() { deleteBlockEvent.Set() })

	// Launch the context once for all subtests
	err := Launch(dbosCtx)
	require.NoError(t, err, "failed to launch DBOS")

	t.Run("ChildWorkflowIDGeneration", func(t *testing.T) {
		r := 3
		h, err := RunWorkflow(dbosCtx, grandParentWf, r)
		require.NoError(t, err, "failed to execute grand parent workflow")
		_, err = h.GetResult()
		require.NoError(t, err, "failed to get result from grand parent workflow")

		// Verify ParentWorkflowID along the chain: grandparent -> parent -> child
		grandParentID := h.GetWorkflowID()
		grandParentStatus, err := h.GetStatus()
		require.NoError(t, err, "failed to get grandparent workflow status")
		require.Empty(t, grandParentStatus.ParentWorkflowID, "top-level grandparent should have no ParentWorkflowID")

		parentID := fmt.Sprintf("%s-0", grandParentID)
		parentHandle, err := RetrieveWorkflow[string](dbosCtx, parentID)
		require.NoError(t, err, "failed to retrieve parent workflow")
		parentStatus, err := parentHandle.GetStatus()
		require.NoError(t, err, "failed to get parent workflow status")
		require.Equal(t, grandParentID, parentStatus.ParentWorkflowID, "parent workflow ParentWorkflowID should be grandparent's ID")

		childID := fmt.Sprintf("%s-0", parentID)
		childHandle, err := RetrieveWorkflow[string](dbosCtx, childID)
		require.NoError(t, err, "failed to retrieve child workflow")
		childStatus, err := childHandle.GetStatus()
		require.NoError(t, err, "failed to get child workflow status")
		require.Equal(t, parentID, childStatus.ParentWorkflowID, "child workflow ParentWorkflowID should be parent's ID")

		// CompletedAt is populated once these workflows reach a terminal state.
		require.False(t, grandParentStatus.CompletedAt.IsZero(), "completed grandparent should have CompletedAt set")
		require.False(t, childStatus.CompletedAt.IsZero(), "completed child should have CompletedAt set")

		// WithFilterHasParent filters on the presence of a parent workflow.
		withParent, err := ListWorkflows(dbosCtx, WithFilterHasParent(true))
		require.NoError(t, err)
		hasParentIDs := make(map[string]bool)
		for _, wf := range withParent {
			require.NotEmpty(t, wf.ParentWorkflowID, "WithFilterHasParent(true) must only return workflows with a parent")
			hasParentIDs[wf.ID] = true
		}
		assert.True(t, hasParentIDs[parentID], "parent workflow should be returned by WithFilterHasParent(true)")
		assert.True(t, hasParentIDs[childID], "child workflow should be returned by WithFilterHasParent(true)")
		assert.False(t, hasParentIDs[grandParentID], "top-level grandparent must not be returned by WithFilterHasParent(true)")

		withoutParent, err := ListWorkflows(dbosCtx, WithFilterHasParent(false))
		require.NoError(t, err)
		noParentIDs := make(map[string]bool)
		for _, wf := range withoutParent {
			require.Empty(t, wf.ParentWorkflowID, "WithFilterHasParent(false) must only return workflows without a parent")
			noParentIDs[wf.ID] = true
		}
		assert.True(t, noParentIDs[grandParentID], "grandparent should be returned by WithFilterHasParent(false)")
		assert.False(t, noParentIDs[childID], "child workflow must be excluded by WithFilterHasParent(false)")
	})

	t.Run("ChildWorkflowWithCustomID", func(t *testing.T) {
		customChildID := uuid.NewString()

		parentHandle, err := RunWorkflow(dbosCtx, simpleParentWf, customChildID)
		require.NoError(t, err, "failed to start parent workflow")

		result, err := parentHandle.GetResult()
		require.NoError(t, err, "failed to get result from parent workflow")
		require.Equal(t, "from step", result)

		// Verify the child workflow was recorded as step 0
		steps, err := GetWorkflowSteps(dbosCtx, parentHandle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps")
		require.Len(t, steps, 2, "expected 2 recorded steps, got %d", len(steps))

		// Verify first step is the child workflow with stepID=0
		require.Equal(t, 0, steps[0].StepID)
		require.Equal(t, runtime.FuncForPC(reflect.ValueOf(simpleChildWf).Pointer()).Name(), steps[0].StepName)
		require.Equal(t, customChildID, steps[0].ChildWorkflowID)

		// Verify second step is the getResult call with stepID=1
		require.Equal(t, 1, steps[1].StepID)
		require.Equal(t, "DBOS.getResult", steps[1].StepName)
		require.Equal(t, customChildID, steps[1].ChildWorkflowID)

		// Verify ParentWorkflowID: parent has none, child has parent's ID
		parentStatus, err := parentHandle.GetStatus()
		require.NoError(t, err, "failed to get parent workflow status")
		require.Empty(t, parentStatus.ParentWorkflowID, "top-level parent workflow should have no ParentWorkflowID")

		childHandle, err := RetrieveWorkflow[string](dbosCtx, customChildID)
		require.NoError(t, err, "failed to retrieve child workflow")
		childStatus, err := childHandle.GetStatus()
		require.NoError(t, err, "failed to get child workflow status")
		require.Equal(t, parentHandle.GetWorkflowID(), childStatus.ParentWorkflowID, "child workflow ParentWorkflowID should be parent's workflow ID")
	})

	t.Run("RecoveredChildWorkflowPollingHandle", func(t *testing.T) {
		// Reset counter and set up events for this test
		pollingCounter = 0
		pollingHandleStartEvent = NewEvent()
		pollingHandleCompleteEvent = NewEvent()
		knownChildID := "known-child-workflow-id"
		knownParentID := "known-parent-workflow-id"

		// Execute parent workflow - it will block after starting the child
		parentHandle, err := RunWorkflow(dbosCtx, pollingHandleParentWf, "parent-input", WithWorkflowID(knownParentID))
		require.NoError(t, err, "failed to start parent workflow")

		// Wait for the workflows to start
		pollingHandleStartEvent.Wait()

		// Recover pending workflows - this should give us both parent and child handles
		recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "failed to recover pending workflows")

		// Should have recovered both parent and child workflows
		require.Len(t, recoveredHandles, 2, "expected 2 recovered handles (parent and child), got %d", len(recoveredHandles))

		// Find the child handle and verify it's a polling handle with the correct ID
		var childRecoveredHandle WorkflowHandle[any]
		for _, handle := range recoveredHandles {
			if handle.GetWorkflowID() == knownChildID {
				childRecoveredHandle = handle
				break
			}
		}

		require.NotNil(t, childRecoveredHandle, "failed to find recovered child workflow handle with ID %s", knownChildID)

		// Complete both workflows
		pollingHandleCompleteEvent.Set()
		result, err := parentHandle.GetResult()
		require.NoError(t, err, "failed to get result from original parent workflow")
		require.Equal(t, "child-input-result", result)
		childResult, err := childRecoveredHandle.GetResult()
		require.NoError(t, err, "failed to get result from recovered child handle")
		require.Equal(t, result, childResult)
	})

	t.Run("ChildWorkflowCannotBeSpawnedFromStep", func(t *testing.T) {
		// Execute the workflow - should fail when step tries to spawn child workflow
		handle, err := RunWorkflow(dbosCtx, parentWfForStepTest, "test-input")
		require.NoError(t, err, "failed to start parent workflow")

		// Expect the workflow to fail
		_, err = handle.GetResult()
		require.Error(t, err, "expected error when spawning child workflow from step, but got none")

		// Check the error type and message
		dbosErr, ok := err.(*Error)
		require.True(t, ok, "expected error to be of type *Error, got %T", err)
		require.Equal(t, ErrorCodeStepExecution, dbosErr.Code, "expected error code to be ErrorCodeStepExecution, got %v", dbosErr.Code)

		expectedMessagePart := "cannot spawn child workflow from within a step"
		require.Contains(t, err.Error(), expectedMessagePart, "expected error message to contain %q, but got %q", expectedMessagePart, err.Error())
	})

	t.Run("DeleteCompletedWorkflow", func(t *testing.T) {
		handle, err := RunWorkflow(dbosCtx, simpleChildWf, "test-delete")
		require.NoError(t, err)

		result, err := handle.GetResult()
		require.NoError(t, err)
		require.Equal(t, "from step", result)

		err = DeleteWorkflows(dbosCtx, []string{handle.GetWorkflowID()})
		require.NoError(t, err)

		// Verify workflow no longer exists
		_, err = RetrieveWorkflow[string](dbosCtx, handle.GetWorkflowID())
		require.Error(t, err)
		var dbosErr *Error
		require.ErrorAs(t, err, &dbosErr)
		require.Equal(t, ErrorCodeNonExistentWorkflow, dbosErr.Code)
	})

	t.Run("DeletePendingWorkflow", func(t *testing.T) {
		deleteBlockEvent.Clear()
		handle, err := RunWorkflow(dbosCtx, deleteBlockingWf, "pending")
		require.NoError(t, err)

		// Delete succeeds even though workflow is still PENDING
		err = DeleteWorkflows(dbosCtx, []string{handle.GetWorkflowID()})
		require.NoError(t, err)

		// Verify the workflow is gone
		_, err = RetrieveWorkflow[string](dbosCtx, handle.GetWorkflowID())
		require.Error(t, err)
		var dbosErr *Error
		require.ErrorAs(t, err, &dbosErr)
		require.Equal(t, ErrorCodeNonExistentWorkflow, dbosErr.Code)
	})

	t.Run("DeleteNonExistentWorkflowIsNoOp", func(t *testing.T) {
		err := DeleteWorkflows(dbosCtx, []string{"non-existent-delete-wf-id"})
		require.NoError(t, err, "expected no error when deleting non-existent workflow")
	})

	t.Run("DeleteCascadesRelatedData", func(t *testing.T) {
		wfID := "delete-cascade-test-wf"
		handle, err := RunWorkflow(dbosCtx, deleteCascadeWf, "input", WithWorkflowID(wfID))
		require.NoError(t, err)

		// Send a notification while the workflow is running so it can Recv it
		err = Send(dbosCtx, wfID, "test-notification", "test-topic")
		require.NoError(t, err)

		_, err = handle.GetResult()
		require.NoError(t, err)

		// Send another notification to the completed workflow (unconsumed, stays in table)
		err = Send(dbosCtx, wfID, "extra-notification", "test-topic")
		require.NoError(t, err)

		// Verify events, streams, notifications, and steps exist via direct DB query
		sysDB := dbosCtx.(*dbosContext).systemDB.(*sysdb.SysDB)
		schemaPrefix := sysDB.Dialect().SchemaPrefix(sysDB.Schema())

		var eventCount, streamCount, notifCount, stepCount int
		err = sysDB.Pool().QueryRow(dbosCtx,
			sysDB.RenderSQL(`SELECT COUNT(*) FROM %sworkflow_events WHERE workflow_uuid = $1`, schemaPrefix),
			wfID).Scan(&eventCount)
		require.NoError(t, err)
		require.Greater(t, eventCount, 0, "expected events to exist before deletion")

		err = sysDB.Pool().QueryRow(dbosCtx,
			sysDB.RenderSQL(`SELECT COUNT(*) FROM %sstreams WHERE workflow_uuid = $1`, schemaPrefix),
			wfID).Scan(&streamCount)
		require.NoError(t, err)
		require.Greater(t, streamCount, 0, "expected stream entries to exist before deletion")

		err = sysDB.Pool().QueryRow(dbosCtx,
			sysDB.RenderSQL(`SELECT COUNT(*) FROM %snotifications WHERE destination_uuid = $1`, schemaPrefix),
			wfID).Scan(&notifCount)
		require.NoError(t, err)
		require.Greater(t, notifCount, 0, "expected notifications to exist before deletion")

		steps, err := GetWorkflowSteps(dbosCtx, wfID)
		require.NoError(t, err)
		// At least 4 steps (SetEvent, WriteStream, CloseStream, Recv). Recv may
		// also record an inner DBOS.sleep step when it has to wait for the
		// notification (timing-dependent: more likely on the polling backend),
		// so we just check the floor.
		require.GreaterOrEqual(t, len(steps), 4, "expected at least 4 steps: SetEvent, WriteStream, CloseStream, Recv")

		err = sysDB.Pool().QueryRow(dbosCtx,
			sysDB.RenderSQL(`SELECT COUNT(*) FROM %soperation_outputs WHERE workflow_uuid = $1`, schemaPrefix),
			wfID).Scan(&stepCount)
		require.NoError(t, err)
		require.Greater(t, stepCount, 0, "expected operation_outputs to exist before deletion")

		// Delete the workflow
		err = DeleteWorkflows(dbosCtx, []string{wfID})
		require.NoError(t, err)

		// Verify all related data was cascade-deleted
		err = sysDB.Pool().QueryRow(dbosCtx,
			sysDB.RenderSQL(`SELECT COUNT(*) FROM %sworkflow_events WHERE workflow_uuid = $1`, schemaPrefix),
			wfID).Scan(&eventCount)
		require.NoError(t, err)
		require.Equal(t, 0, eventCount, "expected events to be cascade-deleted")

		err = sysDB.Pool().QueryRow(dbosCtx,
			sysDB.RenderSQL(`SELECT COUNT(*) FROM %sstreams WHERE workflow_uuid = $1`, schemaPrefix),
			wfID).Scan(&streamCount)
		require.NoError(t, err)
		require.Equal(t, 0, streamCount, "expected stream entries to be cascade-deleted")

		err = sysDB.Pool().QueryRow(dbosCtx,
			sysDB.RenderSQL(`SELECT COUNT(*) FROM %snotifications WHERE destination_uuid = $1`, schemaPrefix),
			wfID).Scan(&notifCount)
		require.NoError(t, err)
		require.Equal(t, 0, notifCount, "expected notifications to be cascade-deleted")

		err = sysDB.Pool().QueryRow(dbosCtx,
			sysDB.RenderSQL(`SELECT COUNT(*) FROM %soperation_outputs WHERE workflow_uuid = $1`, schemaPrefix),
			wfID).Scan(&stepCount)
		require.NoError(t, err)
		require.Equal(t, 0, stepCount, "expected operation_outputs to be cascade-deleted")
	})

	t.Run("DeleteWithChildrenThreeLayers", func(t *testing.T) {
		// Topology: root → 2 mid nodes → 4 leaf nodes (2 per mid)
		rootID := "delete-tree-root"
		handle, err := RunWorkflow(dbosCtx, deleteRootWf, rootID, WithWorkflowID(rootID))
		require.NoError(t, err)

		result, err := handle.GetResult()
		require.NoError(t, err)
		require.Equal(t, "root:"+rootID, result)

		// Build expected IDs for all 7 workflows
		midIDs := []string{
			rootID + "-mid-0",
			rootID + "-mid-1",
		}
		leafIDs := []string{
			midIDs[0] + "-leaf-0",
			midIDs[0] + "-leaf-1",
			midIDs[1] + "-leaf-0",
			midIDs[1] + "-leaf-1",
		}
		allIDs := append([]string{rootID}, midIDs...)
		allIDs = append(allIDs, leafIDs...)

		// Verify all 7 workflows exist
		for _, id := range allIDs {
			_, err := RetrieveWorkflow[string](dbosCtx, id)
			require.NoError(t, err, "expected workflow %s to exist", id)
		}

		// Delete root with children — should recursively delete all 7
		err = DeleteWorkflows(dbosCtx, []string{rootID}, WithDeleteChildren())
		require.NoError(t, err)

		// Verify all 7 are gone
		for _, id := range allIDs {
			_, err := RetrieveWorkflow[string](dbosCtx, id)
			require.Error(t, err, "expected workflow %s to be deleted", id)
			var dbosErr *Error
			require.ErrorAs(t, err, &dbosErr)
			require.Equal(t, ErrorCodeNonExistentWorkflow, dbosErr.Code)
		}
	})

	t.Run("DeleteMultipleWorkflowsWithChildren", func(t *testing.T) {
		// Create two independent trees, each: root → 2 mid → 4 leaf (7 per tree, 14 total)
		root1 := "delete-multi-root-1"
		root2 := "delete-multi-root-2"

		h1, err := RunWorkflow(dbosCtx, deleteRootWf, root1, WithWorkflowID(root1))
		require.NoError(t, err)
		h2, err := RunWorkflow(dbosCtx, deleteRootWf, root2, WithWorkflowID(root2))
		require.NoError(t, err)
		_, err = h1.GetResult()
		require.NoError(t, err)
		_, err = h2.GetResult()
		require.NoError(t, err)

		// Build all 14 expected IDs
		var allIDs []string
		for _, root := range []string{root1, root2} {
			allIDs = append(allIDs, root)
			for i := range 2 {
				mid := fmt.Sprintf("%s-mid-%d", root, i)
				allIDs = append(allIDs, mid)
				for j := range 2 {
					allIDs = append(allIDs, fmt.Sprintf("%s-leaf-%d", mid, j))
				}
			}
		}
		require.Len(t, allIDs, 14)

		// Verify all 14 exist
		for _, id := range allIDs {
			_, err := RetrieveWorkflow[string](dbosCtx, id)
			require.NoError(t, err, "expected workflow %s to exist", id)
		}

		// Delete both roots with children in a single call
		err = DeleteWorkflows(dbosCtx, []string{root1, root2}, WithDeleteChildren())
		require.NoError(t, err)

		// Verify all 14 are gone
		for _, id := range allIDs {
			_, err := RetrieveWorkflow[string](dbosCtx, id)
			require.Error(t, err, "expected workflow %s to be deleted", id)
			var dbosErr *Error
			require.ErrorAs(t, err, &dbosErr)
			require.Equal(t, ErrorCodeNonExistentWorkflow, dbosErr.Code)
		}
	})
}

// TestChildWorkflowDeterminismCheck verifies that checkChildWorkflow detects a
// non-deterministic child invocation: if a child workflow is already recorded at
// a given step ID, re-invoking that step under a different name is rejected with
// an ErrorCodeUnexpectedStep error rather than silently proceeding.
func TestChildWorkflowDeterminismCheck(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	determinismChildWf := func(_ Context, _ string) (string, error) {
		return "child-result", nil
	}
	RegisterWorkflow(dbosCtx, determinismChildWf)

	determinismParentWf := func(ctx Context, _ string) (string, error) {
		childHandle, err := RunWorkflow(ctx, determinismChildWf, "")
		if err != nil {
			return "", err
		}
		return childHandle.GetResult()
	}
	RegisterWorkflow(dbosCtx, determinismParentWf)

	require.NoError(t, Launch(dbosCtx))

	// Run the parent to completion so the child workflow is durably recorded in
	// operation_outputs at step 0.
	parentHandle, err := RunWorkflow(dbosCtx, determinismParentWf, "")
	require.NoError(t, err, "failed to start parent workflow")
	res, err := parentHandle.GetResult()
	require.NoError(t, err, "failed to get parent result")
	require.Equal(t, "child-result", res)

	parentID := parentHandle.GetWorkflowID()
	expectedChildID := fmt.Sprintf("%s-0", parentID)

	sysDB, ok := dbosCtx.(*dbosContext).systemDB.(*sysdb.SysDB)
	require.True(t, ok, "expected sysDB instance")
	ctx := context.Background()

	// Read the name recorded for the child workflow at step 0 directly from the
	// table, so the test does not depend on the function-name resolution rules.
	var recordedName string
	nameQuery := sysDB.RenderSQL(`SELECT function_name FROM %soperation_outputs WHERE workflow_uuid = $1 AND function_id = 0`, sysDB.Dialect().SchemaPrefix(sysDB.Schema()))
	require.NoError(t, sysDB.Pool().QueryRow(ctx, nameQuery, parentID).Scan(&recordedName))
	require.NotEmpty(t, recordedName, "child workflow should have a recorded function name")

	t.Run("MatchingNameReturnsChildID", func(t *testing.T) {
		childID, err := sysDB.CheckChildWorkflow(ctx, parentID, 0, recordedName)
		require.NoError(t, err, "matching child workflow name must not error")
		require.NotNil(t, childID, "expected a recorded child workflow ID")
		require.Equal(t, expectedChildID, *childID)
	})

	t.Run("MismatchedNameIsNonDeterminismError", func(t *testing.T) {
		childID, err := sysDB.CheckChildWorkflow(ctx, parentID, 0, recordedName+"-different")
		require.Error(t, err, "a different child workflow name must be a non-determinism error")
		require.Nil(t, childID, "no child ID should be returned on a determinism error")

		var dbosErr *Error
		require.ErrorAs(t, err, &dbosErr)
		require.Equal(t, ErrorCodeUnexpectedStep, dbosErr.Code)
		require.Equal(t, parentID, dbosErr.WorkflowID)
		require.Equal(t, 0, dbosErr.StepID)
		require.Equal(t, recordedName+"-different", dbosErr.ExpectedName)
		require.Equal(t, recordedName, dbosErr.RecordedName)
	})

	t.Run("UnrecordedStepReturnsNil", func(t *testing.T) {
		childID, err := sysDB.CheckChildWorkflow(ctx, parentID, 99, recordedName)
		require.NoError(t, err, "an unrecorded step must not error")
		require.Nil(t, childID, "an unrecorded step must return a nil child ID")
	})

	t.Run("RecordSameChildIsIdempotent", func(t *testing.T) {
		err := sysDB.RecordChildWorkflow(ctx, sysdb.RecordChildWorkflowDBInput{
			ParentWorkflowID: parentID,
			ChildWorkflowID:  expectedChildID,
			StepID:           0,
			StepName:         recordedName,
		})
		require.NoError(t, err, "re-recording the same child at the same step must succeed")
	})

	t.Run("RecordSameChildInTxDoesNotAbortTx", func(t *testing.T) {
		tx, err := sysDB.Pool().BeginTx(ctx, sysdb.TxOptions{})
		require.NoError(t, err)
		defer tx.Rollback(ctx)
		err = sysDB.RecordChildWorkflow(ctx, sysdb.RecordChildWorkflowDBInput{
			ParentWorkflowID: parentID,
			ChildWorkflowID:  expectedChildID,
			StepID:           0,
			StepName:         recordedName,
			Tx:               tx,
		})
		require.NoError(t, err, "re-recording the same child within a tx must succeed")
		// The tx must still be usable after the conflict.
		var one int
		require.NoError(t, tx.QueryRow(ctx, "SELECT 1").Scan(&one))
		require.NoError(t, tx.Commit(ctx))
	})

	t.Run("RecordDifferentChildIsNonDeterminismError", func(t *testing.T) {
		err := sysDB.RecordChildWorkflow(ctx, sysdb.RecordChildWorkflowDBInput{
			ParentWorkflowID: parentID,
			ChildWorkflowID:  expectedChildID + "-different",
			StepID:           0,
			StepName:         recordedName,
		})
		require.Error(t, err, "a different child at the same step must be a non-determinism error")
		var dbosErr *Error
		require.ErrorAs(t, err, &dbosErr)
		require.Equal(t, ErrorCodeUnexpectedStep, dbosErr.Code)
		require.Equal(t, parentID, dbosErr.WorkflowID)
		require.Equal(t, 0, dbosErr.StepID)
		require.Equal(t, expectedChildID+"-different", dbosErr.ExpectedName)
		require.Equal(t, expectedChildID, dbosErr.RecordedName)
	})
}

// Idempotency workflows moved to test functions

func idempotencyWorkflow(dbosCtx Context, input string) (string, error) {
	RunAsStep(dbosCtx, func(ctx context.Context) (int64, error) {
		return incrementCounter(ctx, int64(1))
	})
	return input, nil
}

func TestWorkflowIdempotency(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	RegisterWorkflow(dbosCtx, idempotencyWorkflow)
	require.NoError(t, Launch(dbosCtx), "failed to launch DBOS")

	t.Run("WorkflowExecutedOnlyOnce", func(t *testing.T) {
		idempotencyCounter = 0

		workflowID := uuid.NewString()
		input := "idempotency-test"

		// Execute the same workflow twice with the same ID
		// First execution
		handle1, err := RunWorkflow(dbosCtx, idempotencyWorkflow, input, WithWorkflowID(workflowID))
		require.NoError(t, err, "failed to execute workflow first time")
		result1, err := handle1.GetResult()
		require.NoError(t, err, "failed to get result from first execution")

		// Second execution with the same workflow ID
		handle2, err := RunWorkflow(dbosCtx, idempotencyWorkflow, input, WithWorkflowID(workflowID))
		require.NoError(t, err, "failed to execute workflow second time")
		result2, err := handle2.GetResult()
		require.NoError(t, err, "failed to get result from second execution")

		require.Equal(t, handle1.GetWorkflowID(), handle2.GetWorkflowID())

		// Verify the second handle is a polling handle
		_, ok := handle2.(*workflowPollingHandle[string])
		require.True(t, ok, "expected handle2 to be of type workflowPollingHandle, got %T", handle2)

		// Verify both executions return the same result
		require.Equal(t, result1, result2)

		// Verify the counter was only incremented once (idempotency)
		require.Equal(t, int64(1), idempotencyCounter, "expected counter to be 1 (workflow executed only once)")
	})
}

func TestNoConcurrentWorkflowSameID(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	startedEvent := NewEvent()
	unblockEvent := NewEvent()
	var runCount int64

	blockingWorkflow := func(dbosCtx Context, input string) (string, error) {
		_, err := RunAsStep(dbosCtx, func(ctx context.Context) (int64, error) {
			n := atomic.AddInt64(&runCount, 1)
			startedEvent.Set()
			return n, nil
		})
		if err != nil {
			return "", err
		}
		unblockEvent.Wait()
		return "done", nil
	}
	RegisterWorkflow(dbosCtx, blockingWorkflow)
	require.NoError(t, Launch(dbosCtx), "failed to launch DBOS")

	workflowID := uuid.NewString()

	handle1, err := RunWorkflow(dbosCtx, blockingWorkflow, "input", WithWorkflowID(workflowID))
	require.NoError(t, err, "failed to start first workflow")

	startedEvent.Wait()

	handle2, err := RunWorkflow(dbosCtx, blockingWorkflow, "input", WithWorkflowID(workflowID))
	require.NoError(t, err, "failed to run second workflow call")
	_, ok := handle2.(*workflowPollingHandle[string])
	require.True(t, ok, "expected second call to return polling handle, got %T", handle2)
	require.Equal(t, handle1.GetWorkflowID(), handle2.GetWorkflowID(), "both handles should refer to the same workflow ID")

	unblockEvent.Set()

	result1, err := handle1.GetResult()
	require.NoError(t, err, "failed to get result from first handle")
	result2, err := handle2.GetResult()
	require.NoError(t, err, "failed to get result from second handle")
	require.Equal(t, result1, result2, "both handles should observe the same result")
	require.Equal(t, "done", result1)

	require.Equal(t, int64(1), atomic.LoadInt64(&runCount), "workflow body should run only once")

	// Check the number of attempts is 1
	status, err := handle1.GetStatus()
	require.NoError(t, err, "failed to get status from first handle")
	require.Equal(t, 1, status.Attempts, "expected number of attempts to be 1")
}

func TestWorkflowRecovery(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	var recoveryCounters []int64

	// A child that fails while still returning a value: the parent's getResult
	// checkpoint must carry both so replay matches the live execution.
	var recoveryChildExecutions atomic.Int64
	recoveryChildWorkflow := func(dbosCtx Context, index int) (int64, error) {
		recoveryChildExecutions.Add(1)
		return 42, errors.New("child failure")
	}
	RegisterWorkflow(dbosCtx, recoveryChildWorkflow, WithWorkflowName("recovery-child-workflow"))

	recoveryWorkflow := func(dbosCtx Context, index int) (int64, error) {
		// First step - increments the counter
		_, err := RunAsStep(dbosCtx, func(ctx context.Context) (int64, error) {
			recoveryCounters[index]++
			return recoveryCounters[index], nil
		}, WithStepName("step-one"))
		if err != nil {
			return 0, err
		}

		// Second step
		_, err = RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
			return fmt.Sprintf("completed-%d", index), nil
		}, WithStepName("step-two"))
		if err != nil {
			return 0, err
		}

		childHandle, err := RunWorkflow(dbosCtx, recoveryChildWorkflow, index)
		if err != nil {
			return 0, err
		}
		childRes, childErr := childHandle.GetResult()
		if childErr == nil {
			return 0, errors.New("expected the child failure to be returned")
		}
		if childRes != 42 {
			return 0, fmt.Errorf("child value lost alongside its error: got %d", childRes)
		}

		return recoveryCounters[index], nil
	}

	RegisterWorkflow(dbosCtx, recoveryWorkflow)

	blockingStart := NewEvent()
	blockingEvent := NewEvent()
	blockingWorkflow := func(dbosCtx Context, input string) (string, error) {
		return RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
			blockingStart.Set()
			blockingEvent.Wait()
			return input, nil
		})
	}
	RegisterWorkflow(dbosCtx, blockingWorkflow, WithWorkflowName("blocking-recovery-workflow"))

	err := Launch(dbosCtx)
	require.NoError(t, err, "failed to launch DBOS")

	t.Run("WorkflowRecovery", func(t *testing.T) {
		const numWorkflows = 5

		recoveryCounters = make([]int64, numWorkflows)
		recoveryChildExecutions.Store(0)

		// Start all workflows and let them run to completion
		handles := make([]WorkflowHandle[int64], numWorkflows)
		for i := range numWorkflows {
			handle, err := RunWorkflow(dbosCtx, recoveryWorkflow, i, WithWorkflowID(fmt.Sprintf("recovery-test-%d", i)))
			require.NoError(t, err, "failed to start workflow %d", i)
			handles[i] = handle
		}
		for i := range numWorkflows {
			_, err := handles[i].GetResult()
			require.NoError(t, err, "failed to get result from workflow %d", i)
		}
		require.EqualValues(t, numWorkflows, recoveryChildExecutions.Load(), "each workflow runs its child once")

		// Flip all workflow statuses to PENDING, then recover
		for i := range numWorkflows {
			setWorkflowStatusPending(t, dbosCtx, handles[i].GetWorkflowID())
		}
		recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "failed to recover pending workflows")
		require.Len(t, recoveredHandles, numWorkflows, "expected %d recovered handles, got %d", numWorkflows, len(recoveredHandles))

		recoveredMap := make(map[string]WorkflowHandle[any])
		for _, h := range recoveredHandles {
			recoveredMap[h.GetWorkflowID()] = h
		}

		// 1) Result is as expected (counter value 1 from single execution, replayed
		// idempotently). The recovered run replays the child's checkpointed
		// getResult, which must carry both the child's value and its error.
		for i := range numWorkflows {
			recoveredHandle := recoveredMap[handles[i].GetWorkflowID()]
			require.NotNil(t, recoveredHandle, "workflow %d not found in recovered handles", i)
			result, err := recoveredHandle.GetResult()
			require.NoError(t, err, "failed to get result from recovered workflow %d", i)
			require.Equal(t, float64(1), result.(float64), "workflow %d result should be 1", i)
		}
		require.EqualValues(t, numWorkflows, recoveryChildExecutions.Load(), "children must replay from their checkpoint, not re-execute")

		// 2) Steps are as expected from a single execution (4 steps: step-one, step-two, child spawn, getResult)
		for i := range numWorkflows {
			steps, err := GetWorkflowSteps(dbosCtx, handles[i].GetWorkflowID())
			require.NoError(t, err, "failed to get steps for workflow %d", i)
			require.Len(t, steps, 4, "expected 4 steps for workflow %d", i)
			assert.Equal(t, "step-one", steps[0].StepName, "workflow %d first step name", i)
			assert.Equal(t, 0, steps[0].StepID, "workflow %d first step ID", i)
			assert.NotNil(t, steps[0].Output, "workflow %d first step should have output", i)
			assert.Nil(t, steps[0].Error, "workflow %d first step should not have error", i)
			assert.Equal(t, "step-two", steps[1].StepName, "workflow %d second step name", i)
			assert.Equal(t, 1, steps[1].StepID, "workflow %d second step ID", i)
			assert.NotNil(t, steps[1].Output, "workflow %d second step should have output", i)
			assert.Nil(t, steps[1].Error, "workflow %d second step should not have error", i)
			assert.Equal(t, "recovery-child-workflow", steps[2].StepName, "workflow %d third step name", i)
			assert.Equal(t, 2, steps[2].StepID, "workflow %d third step ID", i)
			assert.NotEmpty(t, steps[2].ChildWorkflowID, "workflow %d third step should record the child spawn", i)
			assert.Equal(t, "DBOS.getResult", steps[3].StepName, "workflow %d fourth step name", i)
			assert.Equal(t, 3, steps[3].StepID, "workflow %d fourth step ID", i)
			assert.NotNil(t, steps[3].Output, "workflow %d getResult must checkpoint the child's value alongside its error", i)
			assert.Error(t, steps[3].Error, "workflow %d getResult must checkpoint the child's error", i)
		}

		// 3) Workflow Attempts counter is 2 (initial run + recovery)
		workflowIDs := make([]string, numWorkflows)
		for i := range numWorkflows {
			workflowIDs[i] = handles[i].GetWorkflowID()
		}
		workflows, err := dbosCtx.(*dbosContext).systemDB.ListWorkflows(dbosCtx, sysdb.ListWorkflowsDBInput{
			WorkflowIDs: workflowIDs,
		})
		require.NoError(t, err, "failed to list workflows")
		require.Len(t, workflows, numWorkflows, "expected %d workflow entries", numWorkflows)
		workflowsByID := make(map[string]struct{ Attempts int }, numWorkflows)
		for _, wf := range workflows {
			workflowsByID[wf.ID] = struct{ Attempts int }{Attempts: wf.Attempts}
		}
		for i := range numWorkflows {
			wf, ok := workflowsByID[handles[i].GetWorkflowID()]
			require.True(t, ok, "workflow %d not found in list result", i)
			require.Equal(t, 2, wf.Attempts, "workflow %d should have 2 attempts after recovery", i)
		}
	})

	// Recovering a workflow that is actively running on this executor must not
	// fence out the live run: recovery skips launching (already active locally)
	// and must leave owner_xid untouched so the run can record its outcome.
	t.Run("RecoverWhileRunning", func(t *testing.T) {
		handle, err := RunWorkflow(dbosCtx, blockingWorkflow, "hello", WithWorkflowID("recover-while-running"))
		require.NoError(t, err, "failed to start blocking workflow")
		blockingStart.Wait()

		recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "failed to recover pending workflows")
		var recoveredHandle WorkflowHandle[any]
		for _, h := range recoveredHandles {
			if h.GetWorkflowID() == handle.GetWorkflowID() {
				recoveredHandle = h
			}
		}
		require.NotNil(t, recoveredHandle, "expected a handle for the running workflow")

		blockingEvent.Set()

		result, err := handle.GetResult()
		require.NoError(t, err, "live run should complete despite recovery dispatch")
		require.Equal(t, "hello", result)

		recoveredResult, err := recoveredHandle.GetResult()
		require.NoError(t, err, "recovered handle should observe the run's outcome")
		require.Equal(t, "hello", recoveredResult)

		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		require.Equal(t, WorkflowStatusSuccess, status.Status)
	})
}

var (
	maxRecoveryAttempts = 20
	recoveryCount       int64
)

func deadLetterQueueWorkflow(ctx Context, input string) (int, error) {
	recoveryCount++
	wfid, err := GetWorkflowID(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get workflow ID: %v", err)
	}
	fmt.Printf("Dead letter queue workflow %s started, recovery count: %d\n", wfid, recoveryCount)
	return 0, nil
}

func infiniteDeadLetterQueueWorkflow(ctx Context, input string) (int, error) {
	return 0, nil
}

func poisonedDLQWorkflow(ctx Context, input string) (int, error) {
	return 0, nil
}

func TestWorkflowDeadLetterQueue(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	RegisterWorkflow(dbosCtx, deadLetterQueueWorkflow, WithMaxRecoveryAttempts(maxRecoveryAttempts))
	RegisterWorkflow(dbosCtx, infiniteDeadLetterQueueWorkflow, WithMaxRecoveryAttempts(-1)) // A negative value means infinite retries
	RegisterWorkflow(dbosCtx, poisonedDLQWorkflow, WithMaxRecoveryAttempts(1))
	dbosCtx.Launch()

	t.Run("DatabaseRetryWithSameOwnerDoesNotDeadLetter", func(t *testing.T) {
		sysDB := dbosCtx.(*dbosContext).systemDB.(*sysdb.SysDB)
		workflowID := uuid.NewString()
		status := models.WorkflowStatus{
			ID:            workflowID,
			Status:        models.WorkflowStatusPending,
			Name:          "dead-letter-owner-test",
			ExecutorID:    "local",
			CreatedAt:     time.Now(),
			Serialization: "DBOS_JSON",
		}

		insert := func(ownerXID string, incrementAttempts bool, maxRetries int) (*sysdb.InsertWorkflowResult, error) {
			tx, err := sysDB.Pool().BeginTx(context.Background(), TxOptions{})
			require.NoError(t, err)
			defer tx.Rollback(context.Background())
			result, err := sysDB.InsertWorkflowStatus(context.Background(), sysdb.InsertWorkflowStatusDBInput{
				Status:            status,
				MaxRetries:        maxRetries,
				Tx:                tx,
				OwnerXID:          &ownerXID,
				IncrementAttempts: incrementAttempts,
			})
			if err != nil {
				return nil, err
			}
			require.NoError(t, tx.Commit(context.Background()))
			return result, nil
		}

		_, err := insert("initial-owner", false, 1)
		require.NoError(t, err)
		result, err := insert("recovery-owner-1", true, 100)
		require.NoError(t, err)
		require.Equal(t, 2, result.Attempts)
		result, err = insert("recovery-owner-2", true, 100)
		require.NoError(t, err)
		require.Equal(t, 3, result.Attempts)

		// Replay the original initialization after a lost commit acknowledgement.
		// Concurrent recoveries raised the counter, but this replay is not a new
		// recovery attempt and must not dead-letter the workflow.
		result, err = insert("initial-owner", false, 1)
		require.NoError(t, err)
		require.Equal(t, 3, result.Attempts)
		require.Equal(t, models.WorkflowStatusPending, result.Status)

		// A genuinely new recovery owner is a new attempt and may dead-letter.
		_, err = insert("next-recovery-owner", true, 1)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrDeadLetterQueue)
	})

	t.Run("DeadLetterQueueBehavior", func(t *testing.T) {
		recoveryCount = 0

		wfID := uuid.NewString()
		handle, err := RunWorkflow(dbosCtx, deadLetterQueueWorkflow, "test", WithWorkflowID(wfID))
		require.NoError(t, err, "failed to start dead letter queue workflow")
		result1, err := handle.GetResult()
		require.NoError(t, err, "failed to get result from initial run")
		require.Equal(t, int64(1), recoveryCount, "expected recovery count 1 after initial run")

		// Recover the workflow the maximum number of times; each time let it complete then flip to PENDING
		setWorkflowStatusPending(t, dbosCtx, wfID)
		for i := range maxRecoveryAttempts {
			recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
			require.NoError(t, err, "failed to recover pending workflows on attempt %d", i+1)
			require.Len(t, recoveredHandles, 1, "expected 1 recovered handle on attempt %d", i+1)
			require.Equal(t, wfID, recoveredHandles[0].GetWorkflowID(), "expected recovered handle to have the same ID as the original workflow")
			_, err = recoveredHandles[0].GetResult()
			require.NoError(t, err, "failed to get result from recovered handle on attempt %d", i+1)
			expectedCount := int64(i + 2) // +1 for initial execution, +1 for each recovery
			require.Equal(t, expectedCount, recoveryCount, "expected recovery count to be %d, got %d", expectedCount, recoveryCount)
			status, err := recoveredHandles[0].GetStatus()
			require.NoError(t, err, "failed to get status from recovered handle")
			require.Equal(t, int(expectedCount), status.Attempts, "expected number of attempts to be %d, got %d", expectedCount, status.Attempts)
			setWorkflowStatusPending(t, dbosCtx, wfID)
		}

		// Verify an additional attempt dead-letters the workflow; recovery skips it without error
		dlqHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "recovery should not fail on a dead-lettered workflow")
		require.Empty(t, dlqHandles, "dead-lettered workflow should not be recovered")

		// Verify workflow status is MAX_RECOVERY_ATTEMPTS_EXCEEDED
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		require.Equal(t, WorkflowStatusMaxRecoveryAttemptsExceeded, status.Status)

		// Verify that getResult returns the DLQ error. Need a new handle
		retrievedHandle, err := RetrieveWorkflow[int](dbosCtx, wfID)
		require.NoError(t, err, "failed to retrieve workflow")
		_, err = retrievedHandle.GetResult()
		require.Error(t, err, "expected dead letter queue error but got none")
		expectedDLQMsg := fmt.Sprintf("Workflow %s has been moved to the dead-letter queue after exceeding the maximum of %d retries", wfID, maxRecoveryAttempts)
		require.Contains(t, err.Error(), expectedDLQMsg, "expected error to mention dead-letter queue, got: %v", err)

		// Verify that attempting to start a workflow with the same ID throws a DLQ error
		_, err = RunWorkflow(dbosCtx, deadLetterQueueWorkflow, "test", WithWorkflowID(wfID))
		require.Error(t, err, "expected dead letter queue error when restarting workflow with same ID but got none")

		require.True(t, errors.Is(err, ErrDeadLetterQueue), "expected error to be ErrorCodeDeadLetterQueue, got %T", err)

		// Now resume the workflow -- this clears the DLQ status
		resumedHandle, err := ResumeWorkflow[int](dbosCtx, wfID)
		require.NoError(t, err, "failed to resume workflow")

		result2, err := resumedHandle.GetResult()
		require.NoError(t, err, "failed to get result from resumed handle")
		require.Equal(t, result1, result2)
		setWorkflowStatusPending(t, dbosCtx, wfID)

		// Recover pending workflows again - should work without error
		handles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.Len(t, handles, 1, "expected 1 recovered handle after resume")
		require.Equal(t, resumedHandle.GetWorkflowID(), handles[0].GetWorkflowID(), "expected recovered handle to have the same ID as the resumed handle")
		require.NoError(t, err, "failed to recover pending workflows after resume")

		result3, err := handles[0].GetResult()
		require.NoError(t, err, "failed to get result from resumed handle")

		require.Equal(t, result1, int(result3.(float64)))

		// Verify workflow status is SUCCESS
		status, err = handle.GetStatus()
		require.NoError(t, err, "failed to get final workflow status")
		require.Equal(t, WorkflowStatusSuccess, status.Status)

		// Verify that retries of a completed workflow do not raise the DLQ exception
		for i := 0; i < maxRecoveryAttempts*2; i++ {
			_, err = RunWorkflow(dbosCtx, deadLetterQueueWorkflow, "test", WithWorkflowID(wfID))
			require.NoError(t, err, "unexpected error when retrying completed workflow")
		}
	})

	t.Run("InfiniteRetriesWorkflow", func(t *testing.T) {
		// Verify that a workflow with MaxRetries=-1 (infinite retries) can be recovered many times without hitting DLQ
		wfID := uuid.NewString()
		handle, err := RunWorkflow(dbosCtx, infiniteDeadLetterQueueWorkflow, "test", WithWorkflowID(wfID))
		require.NoError(t, err, "failed to start infinite dead letter queue workflow")
		result1, err := handle.GetResult()
		require.NoError(t, err, "failed to get result from initial run")
		require.Equal(t, 0, result1)

		// Recover the workflow many times; each time let it complete then flip to PENDING (should never hit DLQ)
		const infiniteRetryIterations = 10
		for i := range infiniteRetryIterations {
			setWorkflowStatusPending(t, dbosCtx, wfID)
			recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
			require.NoError(t, err, "failed to recover pending workflows on attempt %d", i+1)
			require.Len(t, recoveredHandles, 1, "expected 1 recovered handle on attempt %d", i+1)
			resultAny, err := recoveredHandles[0].GetResult()
			require.NoError(t, err, "failed to get result from recovered handle on attempt %d", i+1)
			jsonBytes, err := json.Marshal(resultAny)
			require.NoError(t, err, "failed to marshal result to JSON")
			var result int
			err = json.Unmarshal(jsonBytes, &result)
			require.NoError(t, err, "failed to decode result to int")
			require.Equal(t, 0, result, "expected result 0 on attempt %d", i+1)
		}
	})

	t.Run("DeadLetterDoesNotAbortRecovery", func(t *testing.T) {
		// One poisoned (max attempts already reached) and one healthy pending workflow:
		// recovery must skip the poisoned one and still recover the healthy one.
		poisonedID := uuid.NewString()
		poisonedHandle, err := RunWorkflow(dbosCtx, poisonedDLQWorkflow, "test", WithWorkflowID(poisonedID))
		require.NoError(t, err, "failed to start poisoned workflow")
		_, err = poisonedHandle.GetResult()
		require.NoError(t, err, "failed to get result from poisoned workflow initial run")

		// Exhaust the single allowed recovery attempt
		setWorkflowStatusPending(t, dbosCtx, poisonedID)
		primed, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "failed priming recovery of poisoned workflow")
		require.Len(t, primed, 1)
		_, err = primed[0].GetResult()
		require.NoError(t, err, "failed to get result from primed recovery")

		healthyID := uuid.NewString()
		healthyHandle, err := RunWorkflow(dbosCtx, infiniteDeadLetterQueueWorkflow, "test", WithWorkflowID(healthyID))
		require.NoError(t, err, "failed to start healthy workflow")
		_, err = healthyHandle.GetResult()
		require.NoError(t, err, "failed to get result from healthy workflow initial run")

		setWorkflowStatusPending(t, dbosCtx, poisonedID)
		setWorkflowStatusPending(t, dbosCtx, healthyID)

		handles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "dead-lettered workflow must not abort recovery")
		require.Len(t, handles, 1, "expected only the healthy workflow to be recovered")
		require.Equal(t, healthyID, handles[0].GetWorkflowID())
		_, err = handles[0].GetResult()
		require.NoError(t, err, "failed to get result from recovered healthy workflow")

		status, err := poisonedHandle.GetStatus()
		require.NoError(t, err, "failed to get status of poisoned workflow")
		require.Equal(t, WorkflowStatusMaxRecoveryAttemptsExceeded, status.Status)
	})
}

func TestCancelWorkflows(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	// Workflows for the CancelWorkflowsWithChildren subtest, registered before
	// Launch; the subtest itself runs after Launch (see below).
	var (
		parentWorkflowID     = uuid.NewString()
		childWorkflowID      = uuid.NewString()
		grandChildWorkflowID = uuid.NewString()

		IDs = []string{parentWorkflowID, childWorkflowID, grandChildWorkflowID}

		mainEvents     = make(map[string]*Event)
		workflowEvents = make(map[string]*Event)
	)

	for i := range IDs {
		mainEvents[IDs[i]] = NewEvent()
		workflowEvents[IDs[i]] = NewEvent()
	}

	grandChildWorkflow := func(ctx Context, _ string) (string, error) {
		workflowID, err := GetWorkflowID(ctx)
		require.NoError(t, err, "failed to get workflow ID")

		require.Equal(t, grandChildWorkflowID, workflowID)

		mainEvents[workflowID].Set()
		workflowEvents[workflowID].Wait()
		return simpleStep(ctx)
	}
	RegisterWorkflow(dbosCtx, grandChildWorkflow)

	childWorkflow := func(ctx Context, _ string) (string, error) {
		workflowID, err := GetWorkflowID(ctx)
		require.NoError(t, err, "failed to get workflow ID")

		require.Equal(t, childWorkflowID, workflowID)

		RunWorkflow(ctx, grandChildWorkflow, "test-grand-child-in", WithWorkflowID(grandChildWorkflowID))

		mainEvents[workflowID].Set()
		workflowEvents[workflowID].Wait()

		return simpleStep(ctx)
	}
	RegisterWorkflow(dbosCtx, childWorkflow)

	parentWorkflow := func(ctx Context, _ string) (string, error) {
		workflowID, err := GetWorkflowID(ctx)
		require.NoError(t, err, "failed to get workflow ID")

		require.Equal(t, parentWorkflowID, workflowID)

		RunWorkflow(ctx, childWorkflow, "test-child-input", WithWorkflowID(childWorkflowID))

		mainEvents[workflowID].Set()
		workflowEvents[workflowID].Wait()

		return simpleStep(ctx)
	}
	RegisterWorkflow(dbosCtx, parentWorkflow)

	cancelWorkflowsWithChildren := func(t *testing.T) {
		sysDB := dbosCtx.(*dbosContext).systemDB.(*sysdb.SysDB)

		_, err := RunWorkflow(dbosCtx, parentWorkflow, "test-input", WithWorkflowID(parentWorkflowID))
		require.NoError(t, err, "failed to start parent workflow")

		// wait until whole tree is running and blocked
		for id := range mainEvents {
			mainEvents[id].Wait()
		}

		workflowStatuses, err := sysDB.GetWorkflowChildren(dbosCtx, sysdb.GetWorkflowChildrenDBInput{WorkflowID: parentWorkflowID})
		require.NoError(t, err, "failed to get workflow children")
		require.Equal(t, len(workflowStatuses), 2, "expected %d children got %d", 2, len(workflowStatuses))

		require.Equal(t, IDs[1:], []string{workflowStatuses[0].ID, workflowStatuses[1].ID}, "children mismatch")

		// cancel workflow without cancelling the child workflows
		require.NoError(t, CancelWorkflow(dbosCtx, parentWorkflowID), "failed to cancel workflow") // first cancel without children
		allStatuses, err := dbosCtx.ListWorkflows(dbosCtx,
			WithFilterWorkflowIDs(IDs...),
			WithFilterLoadInput(false),
			WithFilterLoadOutput(false),
		)
		require.NoError(t, err, "failed to list workflow statuses")
		require.Len(t, allStatuses, 3, "expected 3 workflow statuses")

		statusByID := make(map[string]WorkflowStatusType, len(allStatuses))
		for _, s := range allStatuses {
			statusByID[s.ID] = s.Status
		}

		assert.Equal(t, WorkflowStatusCancelled, statusByID[parentWorkflowID], "parent should be CANCELLED")
		assert.Equal(t, WorkflowStatusPending, statusByID[childWorkflowID], "child should be PENDING")
		assert.Equal(t, WorkflowStatusPending, statusByID[grandChildWorkflowID], "grandchild should be PENDING")

		// now cancel workflow with children
		require.NoError(t, CancelWorkflow(dbosCtx, parentWorkflowID, WithCancelChildren()), "failed to cancel workflow") // first cancel without children
		allStatuses, err = dbosCtx.ListWorkflows(dbosCtx,
			WithFilterWorkflowIDs(IDs...),
			WithFilterLoadInput(false),
			WithFilterLoadOutput(false),
		)
		require.NoError(t, err, "failed to list workflow statuses")
		require.Len(t, allStatuses, 3, "expected 3 workflow statuses")

		statusByID = make(map[string]WorkflowStatusType, len(allStatuses))
		for _, s := range allStatuses {
			statusByID[s.ID] = s.Status
		}

		assert.Equal(t, WorkflowStatusCancelled, statusByID[parentWorkflowID], "parent should be CANCELLED")
		assert.Equal(t, WorkflowStatusCancelled, statusByID[childWorkflowID], "child should be CANCELLED")
		assert.Equal(t, WorkflowStatusCancelled, statusByID[grandChildWorkflowID], "grandchild should be CANCELLED")

		// release the workflows
		for _, event := range workflowEvents {
			event.Set()
		}
		assert.Equal(t, queueEntriesAreCleanedUp(dbosCtx), true)
	}

	blockEvent := NewEvent()
	blockingWorkflow := func(ctx Context, input string) (string, error) {
		blockEvent.Wait()
		return input, nil
	}
	RegisterWorkflow(dbosCtx, blockingWorkflow)

	var noDeadlineCancelAttempts atomic.Int64
	noDeadlineCancelStarted := NewEvent()
	noDeadlineCancelWorkflow := func(ctx Context, _ string) (string, error) {
		if noDeadlineCancelAttempts.Add(1) == 1 {
			noDeadlineCancelStarted.Set()
			<-ctx.Done()
			return "", ctx.Err()
		}
		return "completed", nil
	}
	RegisterWorkflow(dbosCtx, noDeadlineCancelWorkflow)

	var finalStepCancelAttempts atomic.Int64
	finalStepCancelStarted := NewEvent()
	finalStepCancelRelease := make(chan struct{})
	finalStepCancelWorkflow := func(ctx Context, _ string) (string, error) {
		if finalStepCancelAttempts.Add(1) == 1 {
			finalStepCancelStarted.Set()
			<-finalStepCancelRelease
		}
		return "completed", nil
	}
	RegisterWorkflow(dbosCtx, finalStepCancelWorkflow)

	// Ignores its cancellation entirely and returns a successful result.
	var swallowCancelAttempts atomic.Int64
	swallowCancelStarted := NewEvent()
	swallowCancelRelease := make(chan struct{})
	swallowCancelWorkflow := func(ctx Context, _ string) (string, error) {
		if swallowCancelAttempts.Add(1) == 1 {
			swallowCancelStarted.Set()
			<-swallowCancelRelease
		}
		return "swallowed", nil
	}
	RegisterWorkflow(dbosCtx, swallowCancelWorkflow)

	err := Launch(dbosCtx)
	require.NoError(t, err, "failed to launch DBOS instance")

	t.Run("CancelWorkflowsWithChildren", cancelWorkflowsWithChildren)

	startBlockedWorkflows := func(t *testing.T, n int, prefix string) []string {
		t.Helper()
		ids := make([]string, n)
		for i := range ids {
			h, err := RunWorkflow(dbosCtx, blockingWorkflow, fmt.Sprintf("%s-%d", prefix, i))
			require.NoError(t, err, "failed to start workflow %d", i)
			ids[i] = h.GetWorkflowID()
		}
		return ids
	}

	t.Run("CancelWorkflowsBatch", func(t *testing.T) {
		blockEvent.Clear()
		defer blockEvent.Set()
		ids := startBlockedWorkflows(t, 3, "cancel-batch")

		require.NoError(t, CancelWorkflows(dbosCtx, ids), "failed to cancel workflows batch")

		for _, id := range ids {
			handle, err := RetrieveWorkflow[string](dbosCtx, id)
			require.NoError(t, err, "failed to retrieve workflow %s", id)
			status, err := handle.GetStatus()
			require.NoError(t, err, "failed to get status for workflow %s", id)
			assert.Equal(t, WorkflowStatusCancelled, status.Status, "workflow %s should be CANCELLED", id)
			assert.Empty(t, status.QueueName, "workflow %s queue should be cleared", id)
		}
	})

	t.Run("CancelWorkflowsSkipsMissingIDs", func(t *testing.T) {
		blockEvent.Clear()
		defer blockEvent.Set()
		ids := startBlockedWorkflows(t, 1, "cancel-mixed")

		missingID := "missing-" + uuid.NewString()
		require.NoError(t, CancelWorkflows(dbosCtx, []string{missingID, ids[0]}),
			"CancelWorkflows should not error on missing IDs")

		handle, err := RetrieveWorkflow[string](dbosCtx, ids[0])
		require.NoError(t, err, "failed to retrieve workflow")
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get status")
		assert.Equal(t, WorkflowStatusCancelled, status.Status)
	})

	t.Run("CancelWorkflowsLeavesTerminalUntouched", func(t *testing.T) {
		blockEvent.Set()
		h, err := RunWorkflow(dbosCtx, blockingWorkflow, "cancel-terminal")
		require.NoError(t, err, "failed to start workflow")
		_, err = h.GetResult()
		require.NoError(t, err, "workflow should complete successfully")

		require.NoError(t, CancelWorkflows(dbosCtx, []string{h.GetWorkflowID()}),
			"CancelWorkflows should succeed on already-completed workflow")

		status, err := h.GetStatus()
		require.NoError(t, err, "failed to get status")
		assert.Equal(t, WorkflowStatusSuccess, status.Status, "completed workflow should remain SUCCESS")
	})

	t.Run("CancelWorkflowsEmpty", func(t *testing.T) {
		require.NoError(t, CancelWorkflows(dbosCtx, nil),
			"CancelWorkflows with nil slice should be a no-op")
		require.NoError(t, CancelWorkflows(dbosCtx, []string{}),
			"CancelWorkflows with empty slice should be a no-op")
	})

	t.Run("CancelWorkflowsNilContext", func(t *testing.T) {
		require.Error(t, CancelWorkflows(nil, []string{"id"}),
			"CancelWorkflows should error on nil context")
	})

	t.Run("CancelledDuringFinalStepDoesNotComplete", func(t *testing.T) {
		// A workflow API-cancelled while finishing its last work must end as
		// CANCELLED, not complete: the refused outcome write is surfaced as a
		// cancellation and the workflow stays resumable (same semantics as the
		// Python/TS/Java SDKs).
		handle, err := RunWorkflow(dbosCtx, finalStepCancelWorkflow, "")
		require.NoError(t, err, "failed to start workflow")
		finalStepCancelStarted.Wait()

		require.NoError(t, CancelWorkflow(dbosCtx, handle.GetWorkflowID()), "failed to cancel workflow")
		close(finalStepCancelRelease)

		_, err = handle.GetResult()
		require.Error(t, err, "a cancelled workflow must not complete")
		require.True(t, errors.Is(err, ErrWorkflowCancelled), "expected ErrorCodeWorkflowCancelled error, got: %v", err)

		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		require.Equal(t, WorkflowStatusCancelled, status.Status, "the durable status must remain CANCELLED")
		require.Nil(t, status.Output, "the refused outcome must not record an output")

		resumedHandle, err := ResumeWorkflow[string](dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err, "failed to resume workflow")
		result, err := resumedHandle.GetResult()
		require.NoError(t, err, "resumed workflow should complete successfully")
		require.Equal(t, "completed", result)
		require.EqualValues(t, 2, finalStepCancelAttempts.Load(), "expected the workflow to re-execute on resume")
	})

	t.Run("CancelWithoutDeadlineIsResumable", func(t *testing.T) {
		// A workflow interrupted by plain context cancellation (no deadline, so no
		// AfterFunc cancels it in the DB) must be marked CANCELLED, not ERROR, so
		// it can be resumed.
		cancelCtx, cancelFunc := WithCancel(dbosCtx)
		defer cancelFunc()
		handle, err := RunWorkflow(cancelCtx, noDeadlineCancelWorkflow, "")
		require.NoError(t, err, "failed to start workflow")

		noDeadlineCancelStarted.Wait()
		cancelFunc()

		_, err = handle.GetResult()
		require.Error(t, err, "expected error from cancelled workflow")
		require.True(t, errors.Is(err, context.Canceled), "expected context.Canceled, got: %v", err)

		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		require.Equal(t, WorkflowStatusCancelled, status.Status, "expected workflow status to be WorkflowStatusCancelled")

		resumedHandle, err := ResumeWorkflow[string](dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err, "failed to resume workflow")
		result, err := resumedHandle.GetResult()
		require.NoError(t, err, "resumed workflow should complete successfully")
		require.Equal(t, "completed", result)
		require.EqualValues(t, 2, noDeadlineCancelAttempts.Load(), "expected the workflow to re-execute on resume")
	})

	t.Run("SwallowedCancellationIsNotSuccess", func(t *testing.T) {
		// A workflow that ignores its cancellation and returns (result, nil) must
		// not report success on the in-process handle: the durable row is CANCELLED
		// and no output was recorded, so GetResult surfaces ErrorCodeWorkflowCancelled —
		// consistent with what a polling handle for the same workflow returns.
		cancelCtx, cancelFunc := WithCancel(dbosCtx)
		defer cancelFunc()
		handle, err := RunWorkflow(cancelCtx, swallowCancelWorkflow, "")
		require.NoError(t, err, "failed to start workflow")

		swallowCancelStarted.Wait()
		cancelFunc()

		// Wait for the durable cancel before releasing the workflow, so its
		// normal return deterministically lands after the cancellation.
		require.Eventually(t, func() bool {
			status, err := handle.GetStatus()
			require.NoError(t, err, "failed to get workflow status")
			return status.Status == WorkflowStatusCancelled
		}, 5*time.Second, 10*time.Millisecond, "workflow did not reach cancelled status in time")
		close(swallowCancelRelease)

		result, err := handle.GetResult()
		require.Error(t, err, "a cancelled workflow must not report success")
		require.True(t, errors.Is(err, ErrWorkflowCancelled), "expected ErrorCodeWorkflowCancelled error, got: %v", err)
		require.Equal(t, "", result, "no output may be reported for a cancelled workflow")

		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		require.Equal(t, WorkflowStatusCancelled, status.Status)
		require.Nil(t, status.Output, "the swallowed result must not be recorded")

		// Plain cancel remains resumable; re-execution completes normally.
		resumedHandle, err := ResumeWorkflow[string](dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err, "failed to resume workflow")
		result, err = resumedHandle.GetResult()
		require.NoError(t, err, "resumed workflow should complete successfully")
		require.Equal(t, "swallowed", result)
		require.EqualValues(t, 2, swallowCancelAttempts.Load(), "expected the workflow to re-execute on resume")
	})
}

func TestResumeWorkflows(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	resumeBatchQueue, err := RegisterQueue(dbosCtx, "resume-batch-target-queue",
		WithQueueBasePollingInterval(50*time.Millisecond))
	require.NoError(t, err, "failed to register queue")

	blockEvent := NewEvent()
	blockingWorkflow := func(ctx Context, input string) (string, error) {
		blockEvent.Wait()
		return input, nil
	}
	RegisterWorkflow(dbosCtx, blockingWorkflow)

	err = Launch(dbosCtx)
	require.NoError(t, err, "failed to launch DBOS instance")

	cancelledIDs := func(t *testing.T, n int, prefix string) []string {
		t.Helper()
		ids := make([]string, n)
		for i := range ids {
			h, err := RunWorkflow(dbosCtx, blockingWorkflow, fmt.Sprintf("%s-%d", prefix, i))
			require.NoError(t, err, "failed to start workflow %d", i)
			ids[i] = h.GetWorkflowID()
			require.NoError(t, CancelWorkflow(dbosCtx, ids[i]), "failed to cancel workflow %d", i)
		}
		return ids
	}

	t.Run("ResumeWorkflowsBatchOnInternalQueue", func(t *testing.T) {
		blockEvent.Clear()
		ids := cancelledIDs(t, 3, "resume-batch")
		blockEvent.Set()

		handles, err := ResumeWorkflows[string](dbosCtx, ids)
		require.NoError(t, err, "failed to resume workflows batch")
		require.Len(t, handles, len(ids), "expected one handle per resumed workflow")

		expectedIDs := make(map[string]struct{}, len(ids))
		for _, id := range ids {
			expectedIDs[id] = struct{}{}
		}
		for _, h := range handles {
			_, ok := expectedIDs[h.GetWorkflowID()]
			require.True(t, ok, "unexpected workflow ID %s in resumed handles", h.GetWorkflowID())

			_, err := h.GetResult()
			require.NoError(t, err, "failed to get result for resumed workflow %s", h.GetWorkflowID())

			status, err := h.GetStatus()
			require.NoError(t, err, "failed to get status for resumed workflow %s", h.GetWorkflowID())
			assert.Equal(t, models.InternalQueueName, status.QueueName, "batch resume should default to the internal queue")
		}
	})

	t.Run("ResumeWorkflowsBatchToCustomQueue", func(t *testing.T) {
		blockEvent.Clear()
		ids := cancelledIDs(t, 3, "resume-batch-queue")
		blockEvent.Set()

		handles, err := ResumeWorkflows[string](dbosCtx, ids, WithResumeQueue(resumeBatchQueue.GetName()))
		require.NoError(t, err, "failed to resume workflows batch to custom queue")
		require.Len(t, handles, len(ids), "expected one handle per resumed workflow")

		expectedIDs := make(map[string]struct{}, len(ids))
		for _, id := range ids {
			expectedIDs[id] = struct{}{}
		}
		for _, h := range handles {
			_, ok := expectedIDs[h.GetWorkflowID()]
			require.True(t, ok, "unexpected workflow ID %s in resumed handles", h.GetWorkflowID())

			_, err := h.GetResult()
			require.NoError(t, err, "failed to get result for resumed workflow %s", h.GetWorkflowID())

			status, err := h.GetStatus()
			require.NoError(t, err, "failed to get status for resumed workflow %s", h.GetWorkflowID())
			assert.Equal(t, resumeBatchQueue.GetName(), status.QueueName, "batch-resumed workflow should be attributed to the custom queue")
		}
	})

	t.Run("ResumeWorkflowsSkipsMissingIDs", func(t *testing.T) {
		blockEvent.Clear()
		ids := cancelledIDs(t, 1, "resume-mixed")
		blockEvent.Set()

		missingID := "missing-" + uuid.NewString()
		handles, err := ResumeWorkflows[string](dbosCtx, []string{missingID, ids[0]})
		require.NoError(t, err, "ResumeWorkflows should not error on missing IDs")
		require.Len(t, handles, 1, "only the existing workflow should produce a handle")
		assert.Equal(t, ids[0], handles[0].GetWorkflowID())

		_, err = handles[0].GetResult()
		require.NoError(t, err, "failed to get result from resumed workflow")
	})

	t.Run("ResumeWorkflowSingularPreservesNonExistentError", func(t *testing.T) {
		missingID := "missing-" + uuid.NewString()
		_, err := ResumeWorkflow[string](dbosCtx, missingID)
		require.Error(t, err, "expected error resuming non-existent workflow")
		var dbosErr *Error
		require.ErrorAs(t, err, &dbosErr, "expected *Error, got %T", err)
		assert.Equal(t, ErrorCodeNonExistentWorkflow, dbosErr.Code)
		assert.Equal(t, missingID, dbosErr.WorkflowID)
	})
}

var (
	receiveIdempotencyStartEvent = NewEvent()
	sendRecvSyncEvent            = NewEvent() // Event to synchronize send/recv in tests
	numConcurrentRecvWfs         = 5
	concurrentRecvReadyEvents    = make([]*Event, numConcurrentRecvWfs)
	concurrentRecvStartEvent     = NewEvent()
)

type sendWorkflowInput struct {
	DestinationID string
	Topic         string
}

func sendWorkflow(ctx Context, input sendWorkflowInput) (string, error) {
	err := Send(ctx, input.DestinationID, "message1", input.Topic)
	if err != nil {
		return "", err
	}
	err = Send(ctx, input.DestinationID, "message2", input.Topic)
	if err != nil {
		return "", err
	}
	err = Send(ctx, input.DestinationID, "message3", input.Topic)
	if err != nil {
		return "", err
	}
	return "", nil
}

func receiveWorkflow(ctx Context, input struct {
	Topic   string
	Timeout time.Duration
}) (string, error) {
	logger := ctx.(*dbosContext).logger
	// Wait for the test to signal it's ready
	sendRecvSyncEvent.Wait()

	msg1, err := Recv[string](ctx, input.Topic, input.Timeout)
	if err != nil {
		logger.Error("failed to receive first message", "error", err)
		return "", err
	}
	msg2, err := Recv[string](ctx, input.Topic, input.Timeout)
	if err != nil {
		logger.Error("failed to receive second message", "error", err, "msg1", msg1)
		return "", err
	}
	msg3, err := Recv[string](ctx, input.Topic, input.Timeout)
	if err != nil {
		logger.Error("failed to receive third message", "error", err, "msg1", msg1, "msg2", msg2)
		return "", err
	}
	return msg1 + "-" + msg2 + "-" + msg3, nil
}

func receiveWorkflowCoordinated(ctx Context, input struct {
	Topic string
	i     int
}) (string, error) {
	// Signal that this workflow has started and is ready
	concurrentRecvReadyEvents[input.i].Set()

	// Wait for the coordination event before starting to receive
	concurrentRecvStartEvent.Wait()

	// Do a single Recv call with timeout
	msg, err := Recv[string](ctx, input.Topic, 3*time.Second)
	if err != nil {
		return "", err
	}
	return msg, nil
}

func sendStructWorkflow(ctx Context, input sendWorkflowInput) (string, error) {
	testStruct := sendRecvType{Value: "test-struct-value"}
	err := Send(ctx, input.DestinationID, testStruct, input.Topic)
	return "", err
}

func receiveStructWorkflow(ctx Context, topic string) (sendRecvType, error) {
	// Wait for the test to signal it's ready
	sendRecvSyncEvent.Wait()
	return Recv[sendRecvType](ctx, topic, 3*time.Second)
}

func sendIdempotencyWorkflow(ctx Context, input sendWorkflowInput) (string, error) {
	err := Send(ctx, input.DestinationID, "m1", input.Topic)
	if err != nil {
		return "", err
	}
	return "idempotent-send-completed", nil
}

func receiveIdempotencyWorkflow(ctx Context, topic string) (string, error) {
	// Wait for the test to signal it's ready
	sendRecvSyncEvent.Wait()
	msg, err := Recv[string](ctx, topic, 60*time.Minute) // Should not timeout
	if err != nil {
		// Unlock the test in this case
		receiveIdempotencyStartEvent.Set()
		return "", err
	}
	return msg, nil
}

func stepThatCallsSend(ctx context.Context, input sendWorkflowInput) (string, error) {
	err := Send(ctx.(Context), input.DestinationID, "message-from-step", input.Topic)
	if err != nil {
		return "", err
	}
	return "send-completed", nil
}

func workflowThatCallsSendInStep(ctx Context, input sendWorkflowInput) (string, error) {
	return RunAsStep(ctx, func(context context.Context) (string, error) {
		return stepThatCallsSend(context, input)
	})
}

type sendRecvType struct {
	Value string
}

func recvContextCancelWorkflow(ctx Context, topic string) (string, error) {
	// Try to receive with a 5 second timeout, but context will cancel before that
	msg, err := Recv[string](ctx, topic, 5*time.Second)
	if err != nil {
		return "", err
	}
	return msg, nil
}

func TestSendRecv(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	// Register all send/recv workflows with executor
	RegisterWorkflow(dbosCtx, sendWorkflow)
	RegisterWorkflow(dbosCtx, receiveWorkflow)
	RegisterWorkflow(dbosCtx, receiveWorkflowCoordinated)
	RegisterWorkflow(dbosCtx, sendStructWorkflow)
	RegisterWorkflow(dbosCtx, receiveStructWorkflow)
	RegisterWorkflow(dbosCtx, sendIdempotencyWorkflow)
	RegisterWorkflow(dbosCtx, receiveIdempotencyWorkflow)
	RegisterWorkflow(dbosCtx, workflowThatCallsSendInStep)
	RegisterWorkflow(dbosCtx, recvContextCancelWorkflow)

	Launch(dbosCtx)

	t.Run("SendRecvSuccess", func(t *testing.T) {
		// Clear the sync event before starting
		sendRecvSyncEvent.Clear()

		// Start the receive workflow - it will wait for sendRecvSyncEvent before calling Recv
		receiveHandle, err := RunWorkflow(dbosCtx, receiveWorkflow, struct {
			Topic   string
			Timeout time.Duration
		}{
			Topic:   "test-topic",
			Timeout: 30 * time.Second,
		})
		require.NoError(t, err, "failed to start receive workflow")

		// Send messages to the receive workflow
		sendHandle, err := RunWorkflow(dbosCtx, sendWorkflow, sendWorkflowInput{
			DestinationID: receiveHandle.GetWorkflowID(),
			Topic:         "test-topic",
		})
		require.NoError(t, err, "failed to send message")

		// Wait for send workflow to complete
		_, err = sendHandle.GetResult()
		require.NoError(t, err, "failed to get result from send workflow")

		// Now that the send workflow has completed, signal the receive workflow to proceed
		sendRecvSyncEvent.Set()

		// Wait for receive workflow to complete
		result, err := receiveHandle.GetResult()
		require.NoError(t, err, "failed to get result from receive workflow")
		require.Equal(t, "message1-message2-message3", result)

		// Verify step counting for send workflow (sendWorkflow calls Send 3 times)
		sendSteps, err := GetWorkflowSteps(dbosCtx, sendHandle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps for send workflow")
		require.Len(t, sendSteps, 3, "expected 3 steps in send workflow (3 Send calls), got %d", len(sendSteps))
		for i, step := range sendSteps {
			require.Equal(t, i, step.StepID, "expected step %d to have correct StepID", i)
			require.Equal(t, "DBOS.send", step.StepName, "expected step %d to have StepName 'DBOS.send'", i)
			require.False(t, step.StartedAt.IsZero(), "expected step %d to have StartedAt set", i)
			require.False(t, step.CompletedAt.IsZero(), "expected step %d to have CompletedAt set", i)
			require.True(t, step.CompletedAt.After(step.StartedAt) || step.CompletedAt.Equal(step.StartedAt),
				"expected step %d CompletedAt to be after or equal to StartedAt", i)
		}

		// Verify step counting for receive workflow (receiveWorkflow calls Recv 3 times)
		receiveSteps, err := GetWorkflowSteps(dbosCtx, receiveHandle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps for receive workflow")
		require.Len(t, receiveSteps, 3, "expected 3 steps in receive workflow (3 Recv calls), got %d", len(receiveSteps))
		require.Equal(t, "DBOS.recv", receiveSteps[0].StepName, "expected step 0 to have StepName 'DBOS.recv'")
		require.Equal(t, "DBOS.recv", receiveSteps[1].StepName, "expected step 1 to have StepName 'DBOS.recv'")
		require.Equal(t, "DBOS.recv", receiveSteps[2].StepName, "expected step 2 to have StepName 'DBOS.recv'")
		for i, step := range receiveSteps {
			require.False(t, step.StartedAt.IsZero(), "expected recv step %d to have StartedAt set", i)
			require.False(t, step.CompletedAt.IsZero(), "expected recv step %d to have CompletedAt set", i)
			require.True(t, step.CompletedAt.After(step.StartedAt) || step.CompletedAt.Equal(step.StartedAt),
				"expected recv step %d CompletedAt to be after or equal to StartedAt", i)
		}
	})

	t.Run("SendRecvCustomStruct", func(t *testing.T) {
		// Clear the sync event before starting
		sendRecvSyncEvent.Clear()

		// Start the receive workflow - it will wait for sendRecvSyncEvent before calling Recv
		receiveHandle, err := RunWorkflow(dbosCtx, receiveStructWorkflow, "struct-topic")
		require.NoError(t, err, "failed to start receive workflow")

		// Send the struct to the receive workflow
		sendHandle, err := RunWorkflow(dbosCtx, sendStructWorkflow, sendWorkflowInput{
			DestinationID: receiveHandle.GetWorkflowID(),
			Topic:         "struct-topic",
		})
		require.NoError(t, err, "failed to send struct")

		// Wait for send workflow to complete
		_, err = sendHandle.GetResult()
		require.NoError(t, err, "failed to get result from send workflow")

		// Now that the send workflow has completed, signal the receive workflow to proceed
		sendRecvSyncEvent.Set()

		// Wait for receive workflow to complete
		result, err := receiveHandle.GetResult()
		require.NoError(t, err, "failed to get result from receive workflow")

		// Verify the struct was received correctly
		require.Equal(t, "test-struct-value", result.Value)

		// Verify step counting for sendStructWorkflow (calls Send 1 time)
		sendSteps, err := GetWorkflowSteps(dbosCtx, sendHandle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps for send struct workflow")
		require.Len(t, sendSteps, 1, "expected 1 step in send struct workflow (1 Send call), got %d", len(sendSteps))
		require.Equal(t, 0, sendSteps[0].StepID)
		require.Equal(t, "DBOS.send", sendSteps[0].StepName)
		require.False(t, sendSteps[0].StartedAt.IsZero(), "expected send step to have StartedAt set")
		require.False(t, sendSteps[0].CompletedAt.IsZero(), "expected send step to have CompletedAt set")
		require.True(t, sendSteps[0].CompletedAt.After(sendSteps[0].StartedAt) || sendSteps[0].CompletedAt.Equal(sendSteps[0].StartedAt),
			"expected send step CompletedAt to be after or equal to StartedAt")

		// Verify step counting for receiveStructWorkflow (calls Recv 1 time)
		receiveSteps, err := GetWorkflowSteps(dbosCtx, receiveHandle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps for receive struct workflow")
		require.Len(t, receiveSteps, 1, "expected 1 step in receive struct workflow (1 Recv call), got %d", len(receiveSteps))
		require.Equal(t, 0, receiveSteps[0].StepID)
		require.Equal(t, "DBOS.recv", receiveSteps[0].StepName)
		require.False(t, receiveSteps[0].StartedAt.IsZero(), "expected recv step to have StartedAt set")
		require.False(t, receiveSteps[0].CompletedAt.IsZero(), "expected recv step to have CompletedAt set")
		require.True(t, receiveSteps[0].CompletedAt.After(receiveSteps[0].StartedAt) || receiveSteps[0].CompletedAt.Equal(receiveSteps[0].StartedAt),
			"expected recv step CompletedAt to be after or equal to StartedAt")
	})

	t.Run("SendToNonExistentUUID", func(t *testing.T) {
		// Generate a non-existent UUID
		destUUID := uuid.NewString()

		// Send to non-existent UUID should fail
		handle, err := RunWorkflow(dbosCtx, sendWorkflow, sendWorkflowInput{
			DestinationID: destUUID,
			Topic:         "testtopic",
		})
		require.NoError(t, err, "failed to start send workflow")

		_, err = handle.GetResult()
		require.Error(t, err, "expected error when sending to non-existent UUID but got none")
		require.True(t, errors.Is(err, ErrNonExistentWorkflow), "expected error to be ErrorCodeNonExistentWorkflow, got %T", err)

		expectedErrorMsg := fmt.Sprintf("workflow %s does not exist", destUUID)
		require.Contains(t, err.Error(), expectedErrorMsg)
	})

	t.Run("RecvTimeout", func(t *testing.T) {
		// Set the event so the receive workflow can proceed immediately
		sendRecvSyncEvent.Set()

		// Create a receive workflow that tries to receive a message but no send happens
		receiveHandle, err := RunWorkflow(dbosCtx, receiveWorkflow, struct {
			Topic   string
			Timeout time.Duration
		}{
			Topic:   "timeout-test-topic",
			Timeout: 2 * time.Second,
		})
		require.NoError(t, err, "failed to start receive workflow")
		_, err = receiveHandle.GetResult()
		require.Error(t, err, "expected timeout error")

		// Check that the error is a ErrorCodeTimeout
		dbosErr, ok := err.(*Error)
		require.True(t, ok, "expected error to be of type *Error, got %T", err)
		require.Equal(t, ErrorCodeTimeout, dbosErr.Code, "expected ErrorCodeTimeout code")
		require.Contains(t, err.Error(), "DBOS.recv timed out", "error message should contain 'Operation timed out'")

		// Check that only two steps were recorded (the recv that timed out and the sleep that timed out)
		steps, err := GetWorkflowSteps(dbosCtx, receiveHandle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps")
		require.Len(t, steps, 2, "expected 2 steps in receive workflow (recv that timed out and sleep that timed out), got %d", len(steps))
		// First step should be recv
		require.Equal(t, "DBOS.recv", steps[0].StepName, "expected step 0 to have StepName 'DBOS.recv'")
		require.NotNil(t, steps[0].Error, "expected step 0 to have an error")
		require.Contains(t, steps[0].Error.Error(), "DBOS.recv timed out", "expected step 0 to contain 'DBOS.recv timed out' in error message")
		// Second step should be sleep
		require.Equal(t, "DBOS.sleep", steps[1].StepName, "expected step 1 to have StepName 'DBOS.sleep'")
	})

	t.Run("RecvForkReplay", func(t *testing.T) {
		sendRecvSyncEvent.Clear()

		receiveHandle, err := RunWorkflow(dbosCtx, receiveIdempotencyWorkflow, "fork-replay-topic")
		require.NoError(t, err, "failed to start receive workflow")

		// Send the message before Recv runs so it is already pending (no sleep step recorded)
		err = Send(dbosCtx, receiveHandle.GetWorkflowID(), "fork-me", "fork-replay-topic")
		require.NoError(t, err, "failed to send message")
		sendRecvSyncEvent.Set()

		result, err := receiveHandle.GetResult()
		require.NoError(t, err, "failed to get result from receive workflow")
		require.Equal(t, "fork-me", result)

		originalSteps, err := GetWorkflowSteps(dbosCtx, receiveHandle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps")
		require.Len(t, originalSteps, 1, "expected only the recv step when the message was already pending")

		// Fork past the recv step: its checkpoint is copied and the recv must replay from it,
		// without waiting (the recv timeout is 60 minutes) and without recording new steps.
		start := time.Now()
		forkedHandle, err := ForkWorkflow[string](dbosCtx, ForkWorkflowInput{OriginalWorkflowID: receiveHandle.GetWorkflowID(), StartStep: 2})
		require.NoError(t, err, "failed to fork receive workflow")
		forkedResult, err := forkedHandle.GetResult()
		require.NoError(t, err, "failed to get result from forked receive workflow")
		require.Equal(t, "fork-me", forkedResult, "forked recv should replay the checkpointed message")
		require.Less(t, time.Since(start), 30*time.Second, "forked recv replay should not wait on the recv timeout")

		forkedSteps, err := GetWorkflowSteps(dbosCtx, forkedHandle.GetWorkflowID())
		require.NoError(t, err, "failed to get forked workflow steps")
		require.Len(t, forkedSteps, 1, "recv replay must not record extra steps")
		require.Equal(t, "DBOS.recv", forkedSteps[0].StepName)
	})

	t.Run("RecvTimeoutForkReplay", func(t *testing.T) {
		sendRecvSyncEvent.Set()

		receiveHandle, err := RunWorkflow(dbosCtx, receiveWorkflow, struct {
			Topic   string
			Timeout time.Duration
		}{
			Topic:   "timeout-fork-topic",
			Timeout: 1 * time.Second,
		})
		require.NoError(t, err, "failed to start receive workflow")
		_, err = receiveHandle.GetResult()
		require.Error(t, err, "expected timeout error")

		// Fork past the recv step: the checkpointed timeout error must round-trip through
		// the recorded errStr with its concrete type and code preserved.
		forkedHandle, err := ForkWorkflow[string](dbosCtx, ForkWorkflowInput{OriginalWorkflowID: receiveHandle.GetWorkflowID(), StartStep: 2})
		require.NoError(t, err, "failed to fork receive workflow")
		_, err = forkedHandle.GetResult()
		require.Error(t, err, "expected replayed timeout error")
		dbosErr, ok := err.(*Error)
		require.True(t, ok, "expected error to be of type *Error, got %T", err)
		require.Equal(t, ErrorCodeTimeout, dbosErr.Code, "expected ErrorCodeTimeout code")
		require.Contains(t, err.Error(), "DBOS.recv timed out")
	})

	t.Run("RecvMustRunInsideWorkflows", func(t *testing.T) {
		// Attempt to run Recv outside of a workflow context
		_, err := Recv[string](dbosCtx, "test-topic", 1*time.Second)
		require.Error(t, err, "expected error when running Recv outside of workflow context, but got none")

		// Check the error type
		dbosErr, ok := err.(*Error)
		require.True(t, ok, "expected error to be of type *Error, got %T", err)
		require.Equal(t, ErrorCodeStepExecution, dbosErr.Code)

		// Test the specific message from the error
		expectedMessagePart := "workflow state not found in context: are you running this step within a workflow?"
		require.Contains(t, err.Error(), expectedMessagePart)
	})

	t.Run("SendOutsideWorkflow", func(t *testing.T) {
		// Clear the sync event before starting
		sendRecvSyncEvent.Clear()

		// Start a receive workflow - it will wait for sendRecvSyncEvent before calling Recv
		receiveHandle, err := RunWorkflow(dbosCtx, receiveWorkflow, struct {
			Topic   string
			Timeout time.Duration
		}{
			Topic:   "outside-workflow-topic",
			Timeout: 30 * time.Second, // This should not timeout
		})
		require.NoError(t, err, "failed to start receive workflow")

		// Send messages from outside a workflow context
		for i := range 3 {
			err = Send(dbosCtx, receiveHandle.GetWorkflowID(), fmt.Sprintf("message%d", i+1), "outside-workflow-topic")
			require.NoError(t, err, "failed to send message%d from outside workflow", i+1)
		}

		// Now that all messages have been sent, signal the receive workflow to proceed
		sendRecvSyncEvent.Set()

		// Verify the receive workflow gets all messages
		result, err := receiveHandle.GetResult()
		require.NoError(t, err, "failed to get result from receive workflow")
		assert.Equal(t, "message1-message2-message3", result, "expected correct result from receive workflow")

		// Verify step counting for receive workflow (calls Recv 3 times, no sleep steps)
		receiveSteps, err := GetWorkflowSteps(dbosCtx, receiveHandle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps for receive workflow")
		require.Len(t, receiveSteps, 3, "expected 3 steps in receive workflow (3 Recv calls), got %d", len(receiveSteps))
		for i, step := range receiveSteps {
			// Step IDs are incremented twice (1 for possible sleep, 1 for the recv)
			require.Equal(t, i*2, step.StepID, "expected step %d to have correct StepID", i)
			require.Equal(t, "DBOS.recv", step.StepName, "expected step %d to have StepName 'DBOS.recv'", i)
			require.False(t, step.StartedAt.IsZero(), "expected recv step %d to have StartedAt set", i)
			require.False(t, step.CompletedAt.IsZero(), "expected recv step %d to have CompletedAt set", i)
			require.True(t, step.CompletedAt.After(step.StartedAt) || step.CompletedAt.Equal(step.StartedAt),
				"expected recv step %d CompletedAt to be after or equal to StartedAt", i)
		}
	})

	t.Run("SendRecvIdempotency", func(t *testing.T) {
		// Clear the sync events before starting
		sendRecvSyncEvent.Clear()

		// Start the receive workflow - it will wait for sendRecvSyncEvent before calling Recv
		receiveHandle, err := RunWorkflow(dbosCtx, receiveIdempotencyWorkflow, "idempotency-topic")
		require.NoError(t, err, "failed to start receive idempotency workflow")

		// Send the message to the receive workflow
		sendHandle, err := RunWorkflow(dbosCtx, sendIdempotencyWorkflow, sendWorkflowInput{
			DestinationID: receiveHandle.GetWorkflowID(),
			Topic:         "idempotency-topic",
		})
		require.NoError(t, err, "failed to send idempotency message")

		// Wait for the send step to complete (the workflow itself is still waiting on sendIdempotencyEvent)
		require.Eventually(t, func() bool {
			steps, err := GetWorkflowSteps(dbosCtx, sendHandle.GetWorkflowID())
			return err == nil && len(steps) > 0 && !steps[0].CompletedAt.IsZero()
		}, 5*time.Second, 10*time.Millisecond, "send step should complete")

		// Now that the send step has completed, signal the receive workflow to proceed
		sendRecvSyncEvent.Set()

		// Wait for the receive workflow to complete
		result, err := receiveHandle.GetResult()
		require.NoError(t, err, "failed to get result from receive workflow")
		require.Equal(t, "m1", result, "expected result to be 'm1'")

		// Now get the result from the send workflow
		result2, err := sendHandle.GetResult()
		require.NoError(t, err, "failed to get result from send idempotency workflow")
		assert.Equal(t, "idempotent-send-completed", result2, "expected result to be 'idempotent-send-completed'")

		// Now reset both workflows
		setWorkflowStatusPending(t, dbosCtx, sendHandle.GetWorkflowID())
		setWorkflowStatusPending(t, dbosCtx, receiveHandle.GetWorkflowID())

		// Attempt recovering both workflows. There should be only 1 and 1 steps recorded for send and receive, respectively, after recovery.
		recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "failed to recover pending workflows")
		require.Len(t, recoveredHandles, 2, "expected 2 recovered handles, got %d", len(recoveredHandles))

		// Find the recovered handle for the send workflow (iterate and check IDs)
		sendRecoveredHandle := WorkflowHandle[any](nil)
		receiveRecoveredHandle := WorkflowHandle[any](nil)
		for _, handle := range recoveredHandles {
			if handle.GetWorkflowID() == sendHandle.GetWorkflowID() {
				sendRecoveredHandle = handle
			}
			if handle.GetWorkflowID() == receiveHandle.GetWorkflowID() {
				receiveRecoveredHandle = handle
			}
		}
		require.NotNil(t, sendRecoveredHandle, "failed to find recovered handle for send workflow")
		require.NotNil(t, receiveRecoveredHandle, "failed to find recovered handle for receive workflow")

		steps, err := GetWorkflowSteps(dbosCtx, sendHandle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps")
		require.Len(t, steps, 1, "expected 1 step in send idempotency workflow, got %d", len(steps))
		assert.Equal(t, 0, steps[0].StepID, "expected send idempotency step to have StepID 0")
		assert.Equal(t, "DBOS.send", steps[0].StepName, "expected send idempotency step to have StepName 'DBOS.send'")
		require.False(t, steps[0].StartedAt.IsZero(), "expected send step to have StartedAt set")
		require.False(t, steps[0].CompletedAt.IsZero(), "expected send step to have CompletedAt set")
		require.True(t, steps[0].CompletedAt.After(steps[0].StartedAt) || steps[0].CompletedAt.Equal(steps[0].StartedAt),
			"expected send step CompletedAt to be after or equal to StartedAt")

		steps, err = GetWorkflowSteps(dbosCtx, receiveHandle.GetWorkflowID())
		require.NoError(t, err, "failed to get steps for receive idempotency workflow")
		require.Len(t, steps, 1, "expected 1 step in receive idempotency workflow (1 Recv call), got %d", len(steps))
		assert.Equal(t, 0, steps[0].StepID, "expected receive idempotency step to have StepID 0")
		assert.Equal(t, "DBOS.recv", steps[0].StepName, "expected receive idempotency step to have StepName 'DBOS.recv'")
		require.False(t, steps[0].StartedAt.IsZero(), "expected recv step to have StartedAt set")
		require.False(t, steps[0].CompletedAt.IsZero(), "expected recv step to have CompletedAt set")
		require.True(t, steps[0].CompletedAt.After(steps[0].StartedAt) || steps[0].CompletedAt.Equal(steps[0].StartedAt),
			"expected recv step CompletedAt to be after or equal to StartedAt")

		// Unblock the workflows to complete
		result3, err := receiveRecoveredHandle.GetResult()
		require.NoError(t, err, "failed to get result from receive idempotency workflow")
		assert.Equal(t, "m1", result3, "expected result to be 'm1'")

		result4, err := sendRecoveredHandle.GetResult()
		require.NoError(t, err, "failed to get result from send idempotency workflow")
		assert.Equal(t, "idempotent-send-completed", result4, "expected result to be 'idempotent-send-completed'")
	})

	t.Run("SendCannotBeCalledWithinStep", func(t *testing.T) {
		// Set the event so the receive workflow can proceed immediately
		sendRecvSyncEvent.Set()

		// Start a receive workflow to have a valid destination
		receiveHandle, err := RunWorkflow(dbosCtx, receiveWorkflow, struct {
			Topic   string
			Timeout time.Duration
		}{
			Topic:   "send-within-step-topic",
			Timeout: 500 * time.Millisecond,
		})
		require.NoError(t, err, "failed to start receive workflow")

		// Execute the workflow that tries to call Send within a step
		handle, err := RunWorkflow(dbosCtx, workflowThatCallsSendInStep, sendWorkflowInput{
			DestinationID: receiveHandle.GetWorkflowID(),
			Topic:         "send-within-step-topic",
		})
		require.NoError(t, err, "failed to start workflow")

		// Expect the workflow to fail with the specific error
		_, err = handle.GetResult()
		require.Error(t, err, "expected error when calling Send within a step, but got none")

		// Check the error type
		dbosErr, ok := err.(*Error)
		require.True(t, ok, "expected error to be of type *Error, got %T", err)
		require.Equal(t, ErrorCodeStepExecution, dbosErr.Code)

		// Test the specific message from the error
		expectedMessagePart := "cannot call Send within a step"
		require.Contains(t, err.Error(), expectedMessagePart, "expected error message to contain expected text")

		// Wait for the receive workflow to time out
		_, err = receiveHandle.GetResult()
		require.Error(t, err, "expected timout error when getting result from receive workflow, but got none")
		require.Contains(t, err.Error(), "DBOS.recv timed out", "expected error message to contain 'DBOS.recv timed out'")
	})

	t.Run("RecvContextCancellation", func(t *testing.T) {
		// Create a context with a shorter timeout than the Recv timeout (1s < 5s)
		timeoutCtx, cancel := WithTimeout(dbosCtx, 1*time.Second)
		defer cancel()

		// Start the workflow with the timeout context
		handle, err := RunWorkflow(timeoutCtx, recvContextCancelWorkflow, "context-cancel-topic")
		require.NoError(t, err, "failed to start recv context cancel workflow")

		// Get the result - should fail with context deadline exceeded
		result, err := handle.GetResult()
		require.Error(t, err, "expected error from context cancellation")
		require.True(t, errors.Is(err, context.DeadlineExceeded), "expected context.DeadlineExceeded error, got: %v", err)
		require.Equal(t, "", result, "expected empty result when context cancelled")

		// Verify the workflow status is cancelled
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		require.Equal(t, WorkflowStatusCancelled, status.Status, "expected workflow status to be WorkflowStatusCancelled")
	})

	t.Run("ConcurrentRecvSameTopicConflicts", func(t *testing.T) {
		// A single (destination, topic) may only have one active receiver at a time.
		// A second concurrent registration must be rejected with a ErrorCodeConflictingID
		// rather than silently sharing/stealing the first receiver's slot.
		sysDB := dbosCtx.(*dbosContext).systemDB.(*sysdb.SysDB)
		destID := uuid.NewString()
		topic := "single-receiver-topic"

		waiter1, err := sysDB.StartRecvListener(context.Background(), destID, topic)
		require.NoError(t, err, "first receiver should register")
		defer waiter1.Release()

		_, err = sysDB.StartRecvListener(context.Background(), destID, topic)
		require.Error(t, err, "second concurrent receiver for the same (destination, topic) must be rejected")
		dbosErr, ok := err.(*Error)
		require.True(t, ok, "expected *Error, got %T", err)
		require.Equal(t, ErrorCodeConflictingID, dbosErr.Code, "expected ErrorCodeConflictingID")
	})
}

// TestRecvStepConflict verifies that when two executors concurrently run the same
// workflow and race to checkpoint the recv step, the loser does not fail: it either
// replays the winner's checkpoint or loses the record race with a ErrorCodeConflictingID
// that routes through the workflow-level conflict handler and awaits the winner's
// result. Either way both executions converge on the delivered message.
//
// The two executors share one database (a single in-process guard cannot double-run
// a workflow, so a real second executor is required). PostgreSQL row locking on
// consumeMessage serializes consumption, making the converged outcome deterministic.
func TestRecvStepConflict(t *testing.T) {
	// checkLeaks is off: the two executors' lifetimes overlap, so a per-executor
	// goroutine leak check would observe the other executor's live goroutines.
	ctxA := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: false})
	ctxB := setupDBOS(t, setupDBOSOptions{dropDB: false, checkLeaks: false})

	recvConflictWorkflow := func(ctx Context, topic string) (string, error) {
		return Recv[string](ctx, topic, 60*time.Second)
	}
	RegisterWorkflow(ctxA, recvConflictWorkflow)
	RegisterWorkflow(ctxB, recvConflictWorkflow)
	require.NoError(t, Launch(ctxA))
	require.NoError(t, Launch(ctxB))

	topic := "recv-step-conflict-topic"
	workflowID := uuid.NewString()

	// Executor A starts the workflow; it registers as receiver and blocks in wait.
	handleA, err := RunWorkflow(ctxA, recvConflictWorkflow, topic, WithWorkflowID(workflowID))
	require.NoError(t, err, "failed to start recv workflow on executor A")

	sysA := ctxA.(*dbosContext).systemDB.(*sysdb.SysDB)
	sysB := ctxB.(*dbosContext).systemDB.(*sysdb.SysDB)
	payload := fmt.Sprintf("%s::%s", workflowID, topic)
	require.Eventually(t, func() bool {
		return sysA.RecvNotifier.Has(payload)
	}, 5*time.Second, 10*time.Millisecond, "executor A never registered as receiver")

	// Executor B recovers the same workflow: a genuinely concurrent second
	// execution with its own in-memory receiver map, so it proceeds to wait and
	// later races A to consume+checkpoint the message.
	setWorkflowStatusPending(t, ctxA, workflowID)
	recovered, err := recoverPendingWorkflows(ctxB.(*dbosContext), []string{"local"})
	require.NoError(t, err, "failed to recover workflow on executor B")
	require.Len(t, recovered, 1, "expected one recovered handle")
	require.Equal(t, workflowID, recovered[0].GetWorkflowID())

	// Executor B must actually run the body (register as receiver), not
	// short-circuit; its separate map confirms a real concurrent execution.
	require.Eventually(t, func() bool {
		return sysB.RecvNotifier.Has(payload)
	}, 5*time.Second, 10*time.Millisecond, "executor B (recovery) never ran the body")

	// Deliver the message. Exactly one executor consumes and checkpoints it; the
	// other replays that checkpoint or loses the checkpoint race and awaits. Both
	// must converge on the delivered value with no permanent failure.
	require.NoError(t, Send(ctxA, workflowID, "delivered", topic), "failed to send message")

	gotA, err := handleA.GetResult()
	require.NoError(t, err, "executor A workflow should succeed")
	require.Equal(t, "delivered", gotA)

	gotB, err := recovered[0].GetResult()
	require.NoError(t, err, "the concurrent recovery must converge on the result, not fail")
	require.Equal(t, "delivered", gotB)
}

// receiveTwiceShortWorkflow receives one message (blocking up to 30s), then attempts a
// second Recv with a short timeout. The second slot is "<timeout>" when no further message
// arrives, letting a test observe whether a duplicate Send was deduplicated.
func receiveTwiceShortWorkflow(ctx Context, topic string) (string, error) {
	first, err := Recv[string](ctx, topic, 30*time.Second)
	if err != nil {
		return "", err
	}
	second, err := Recv[string](ctx, topic, 2*time.Second)
	if err != nil {
		if errors.Is(err, ErrTimeout) {
			second = "<timeout>"
		} else {
			return "", err
		}
	}
	return first + "|" + second, nil
}

// receiveOneShortWorkflow receives a single message with a short timeout, returning
// "<timeout>" if none arrives so a test can observe a silently dropped Send.
func receiveOneShortWorkflow(ctx Context, topic string) (string, error) {
	msg, err := Recv[string](ctx, topic, 3*time.Second)
	if err != nil {
		if errors.Is(err, ErrTimeout) {
			return "<timeout>", nil
		}
		return "", err
	}
	return msg, nil
}

func TestSendIdempotencyKey(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	RegisterWorkflow(dbosCtx, receiveTwiceShortWorkflow)
	RegisterWorkflow(dbosCtx, receiveOneShortWorkflow)
	Launch(dbosCtx)

	t.Run("DuplicateKeyDeliversOnce", func(t *testing.T) {
		handle, err := RunWorkflow(dbosCtx, receiveTwiceShortWorkflow, "idem-dup-topic")
		require.NoError(t, err, "failed to start receive workflow")

		// Two sends with the SAME idempotency key from outside a workflow: the second
		// must be deduplicated rather than delivered as a separate message.
		err = Send(dbosCtx, handle.GetWorkflowID(), "only-once", "idem-dup-topic", WithIdempotencyKey("dup-key"))
		require.NoError(t, err, "first send failed")
		err = Send(dbosCtx, handle.GetWorkflowID(), "only-once", "idem-dup-topic", WithIdempotencyKey("dup-key"))
		require.NoError(t, err, "duplicate send with same key must not error")

		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result from receive workflow")
		require.Equal(t, "only-once|<timeout>", result, "second Recv should time out: the duplicate send must be deduplicated")
	})

	t.Run("DistinctKeysDeliverEach", func(t *testing.T) {
		handle, err := RunWorkflow(dbosCtx, receiveTwiceShortWorkflow, "idem-distinct-topic")
		require.NoError(t, err, "failed to start receive workflow")

		err = Send(dbosCtx, handle.GetWorkflowID(), "msg-a", "idem-distinct-topic", WithIdempotencyKey("key-a"))
		require.NoError(t, err, "send with key-a failed")
		err = Send(dbosCtx, handle.GetWorkflowID(), "msg-b", "idem-distinct-topic", WithIdempotencyKey("key-b"))
		require.NoError(t, err, "send with key-b failed")

		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result from receive workflow")
		// Distinct keys both deliver; ordering between same-millisecond inserts is not guaranteed.
		require.NotContains(t, result, "<timeout>", "both distinct-key messages should be delivered")
		require.Contains(t, result, "msg-a")
		require.Contains(t, result, "msg-b")
	})

	t.Run("SameKeyDistinctDestinationsDeliverEach", func(t *testing.T) {
		// The idempotency key is scoped per destination (message_uuid is key::destinationID):
		// the same key sent to two different workflows must deliver to both.
		handleA, err := RunWorkflow(dbosCtx, receiveOneShortWorkflow, "idem-cross-dest-topic")
		require.NoError(t, err, "failed to start first receive workflow")
		handleB, err := RunWorkflow(dbosCtx, receiveOneShortWorkflow, "idem-cross-dest-topic")
		require.NoError(t, err, "failed to start second receive workflow")

		err = Send(dbosCtx, handleA.GetWorkflowID(), "to-a", "idem-cross-dest-topic", WithIdempotencyKey("cross-dest-key"))
		require.NoError(t, err, "send to first destination failed")
		err = Send(dbosCtx, handleB.GetWorkflowID(), "to-b", "idem-cross-dest-topic", WithIdempotencyKey("cross-dest-key"))
		require.NoError(t, err, "send to second destination failed")

		resultA, err := handleA.GetResult()
		require.NoError(t, err, "failed to get result from first receive workflow")
		require.Equal(t, "to-a", resultA)
		resultB, err := handleB.GetResult()
		require.NoError(t, err, "failed to get result from second receive workflow")
		require.Equal(t, "to-b", resultB, "same key to a different destination must still deliver")
	})
}

var (
	setEventStart                 = NewEvent()
	setSecondEventSignal          = NewEvent()
	setThirdEventSignal           = NewEvent()
	getEventWorkflowStartedSignal = NewEvent()
	firstEventSetSignal           = NewEvent()
	secondEventSetSignal          = NewEvent()
	thirdEventSetSignal           = NewEvent()
)

type setEventWorkflowInput struct {
	Key     string
	Message string
}

func setEventWorkflow(ctx Context, input setEventWorkflowInput) (string, error) {
	err := SetEvent(ctx, input.Key, input.Message)
	if err != nil {
		return "", err
	}
	setEventStart.Set()
	return "event-set", nil
}

type getEventWorkflowInput struct {
	TargetWorkflowID string
	Key              string
}

func getEventWorkflow(ctx Context, input getEventWorkflowInput) (string, error) {
	getEventWorkflowStartedSignal.Set()
	result, err := GetEvent[string](ctx, input.TargetWorkflowID, input.Key, 3*time.Second)
	if err != nil {
		return "", err
	}
	return result, nil
}

func setTwoEventsWorkflow(ctx Context, input setEventWorkflowInput) (string, error) {
	// Set the first event
	err := SetEvent(ctx, "event", "first-event-message")
	if err != nil {
		return "", err
	}
	firstEventSetSignal.Set()

	// Wait for external signal before setting the second event
	setSecondEventSignal.Wait()

	// Set the second event
	err = SetEvent(ctx, "event", "second-event-message")
	if err != nil {
		return "", err
	}
	secondEventSetSignal.Set()

	setThirdEventSignal.Wait()

	// Set the third event
	err = SetEvent(ctx, "anotherevent", "third-event-message")
	if err != nil {
		return "", err
	}
	thirdEventSetSignal.Set()

	return "two-events-set", nil
}

func setEventIdempotencyWorkflow(ctx Context, input setEventWorkflowInput) (string, error) {
	err := SetEvent(ctx, input.Key, input.Message)
	if err != nil {
		return "", err
	}
	return "idempotent-set-completed", nil
}

func getEventIdempotencyWorkflow(ctx Context, input setEventWorkflowInput) (string, error) {
	result, err := GetEvent[string](ctx, input.Key, input.Message, 3*time.Second)
	if err != nil {
		return "", err
	}
	return result, nil
}

func getEventForkReplayWorkflow(ctx Context, input getEventWorkflowInput) (string, error) {
	return GetEvent[string](ctx, input.TargetWorkflowID, input.Key, 60*time.Minute)
}

// setManyEventsWorkflow sets one event per (key, value) pair, so a single
// workflow ID can back concurrent getters spread across several keys.
type setManyEventsInput struct {
	Values map[string]string
}

func setManyEventsWorkflow(ctx Context, input setManyEventsInput) (string, error) {
	for key, value := range input.Values {
		if err := SetEvent(ctx, key, value); err != nil {
			return "", err
		}
	}
	return "many-events-set", nil
}

func TestSetGetEvent(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	// Register all set/get event workflows with executor
	RegisterWorkflow(dbosCtx, setEventWorkflow)
	RegisterWorkflow(dbosCtx, getEventWorkflow)
	RegisterWorkflow(dbosCtx, setTwoEventsWorkflow)
	RegisterWorkflow(dbosCtx, setEventIdempotencyWorkflow)
	RegisterWorkflow(dbosCtx, getEventIdempotencyWorkflow)
	RegisterWorkflow(dbosCtx, getEventForkReplayWorkflow)
	RegisterWorkflow(dbosCtx, setManyEventsWorkflow)

	Launch(dbosCtx)

	t.Run("SetGetEventFromWorkflow", func(t *testing.T) {
		// Clear all signal events before starting
		setSecondEventSignal.Clear()
		setThirdEventSignal.Clear()
		firstEventSetSignal.Clear()
		secondEventSetSignal.Clear()
		thirdEventSetSignal.Clear()

		setWorkflowID := uuid.NewString()

		// Start the workflow that sets events first
		setHandle, err := RunWorkflow(dbosCtx, setTwoEventsWorkflow, setEventWorkflowInput{
			Key:     setWorkflowID,
			Message: "unused",
		}, WithWorkflowID(setWorkflowID))
		require.NoError(t, err, "failed to start set two events workflow")

		// Define test cases for the three events
		testCases := []struct {
			name           string
			key            string
			expectedValue  string
			setEventSignal *Event
			eventSetSignal *Event
		}{
			{
				name:           "first",
				key:            "event",
				expectedValue:  "first-event-message",
				setEventSignal: nil, // First event is set immediately
				eventSetSignal: firstEventSetSignal,
			},
			{
				name:           "second",
				key:            "event",
				expectedValue:  "second-event-message",
				setEventSignal: setSecondEventSignal,
				eventSetSignal: secondEventSetSignal,
			},
			{
				name:           "third",
				key:            "anotherevent",
				expectedValue:  "third-event-message",
				setEventSignal: setThirdEventSignal,
				eventSetSignal: thirdEventSetSignal,
			},
		}

		var getEventHandles []WorkflowHandle[string]

		// Loop through test cases
		for _, tc := range testCases {
			// If this event requires a signal to be set, signal the set workflow
			if tc.setEventSignal != nil {
				tc.setEventSignal.Set()
			}

			// Wait for the event to be set by the set workflow
			tc.eventSetSignal.Wait()

			// Now start the get event workflow - the event is already set, so sleep will not happen
			getEventHandle, err := RunWorkflow(dbosCtx, getEventWorkflow, getEventWorkflowInput{
				TargetWorkflowID: setWorkflowID,
				Key:              tc.key,
			})
			require.NoError(t, err, "failed to start get %s event workflow", tc.name)
			getEventHandles = append(getEventHandles, getEventHandle)

			// Verify we can get the event
			message, err := getEventHandle.GetResult()
			require.NoError(t, err, "failed to get result from %s event workflow", tc.name)
			assert.Equal(t, tc.expectedValue, message, "expected %s message to be '%s'", tc.name, tc.expectedValue)
		}

		// Wait for the set workflow to complete
		result, err := setHandle.GetResult()
		require.NoError(t, err, "failed to get result from set two events workflow")
		assert.Equal(t, "two-events-set", result, "expected result to be 'two-events-set'")

		// Verify step counting for setTwoEventsWorkflow (calls SetEvent 3 times)
		setSteps, err := GetWorkflowSteps(dbosCtx, setHandle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps for set two events workflow")
		require.Len(t, setSteps, 3, "expected 3 steps in set two events workflow (3 SetEvent calls), got %d", len(setSteps))
		for i, step := range setSteps {
			assert.Equal(t, i, step.StepID, "expected step %d to have StepID %d", i, i)
			assert.Equal(t, "DBOS.setEvent", step.StepName, "expected step %d to have StepName 'DBOS.setEvent'", i)
			require.False(t, step.StartedAt.IsZero(), "expected setEvent step %d to have StartedAt set", i)
			require.False(t, step.CompletedAt.IsZero(), "expected setEvent step %d to have CompletedAt set", i)
			require.True(t, step.CompletedAt.After(step.StartedAt) || step.CompletedAt.Equal(step.StartedAt),
				"expected setEvent step %d CompletedAt to be after or equal to StartedAt", i)
		}

		// Verify step counting for all get event workflows (all should have only 1 step, no sleep)
		for i, getEventHandle := range getEventHandles {
			steps, err := GetWorkflowSteps(dbosCtx, getEventHandle.GetWorkflowID())
			require.NoError(t, err, "failed to get workflow steps for get event workflow %d", i)
			require.Len(t, steps, 1, "expected 1 step in get event workflow %d (getEvent only, no sleep), got %d", i, len(steps))
			assert.Equal(t, 0, steps[0].StepID, "expected step to have StepID 0")
			assert.Equal(t, "DBOS.getEvent", steps[0].StepName, "expected step to have StepName 'DBOS.getEvent'")
			require.False(t, steps[0].StartedAt.IsZero(), "expected getEvent step to have StartedAt set")
			require.False(t, steps[0].CompletedAt.IsZero(), "expected getEvent step to have CompletedAt set")
			require.True(t, steps[0].CompletedAt.After(steps[0].StartedAt) || steps[0].CompletedAt.Equal(steps[0].StartedAt),
				"expected getEvent step CompletedAt to be after or equal to StartedAt")
		}
	})

	t.Run("GetEventFromOutsideWorkflow", func(t *testing.T) {
		// Start a workflow that sets an event
		setHandle, err := RunWorkflow(dbosCtx, setEventWorkflow, setEventWorkflowInput{
			Key:     "test-key",
			Message: "test-message",
		})
		if err != nil {
			t.Fatalf("failed to start set event workflow: %v", err)
		}

		// Wait for the event to be set
		_, err = setHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from set event workflow: %v", err)
		}

		// Start a workflow that gets the event from outside the original workflow
		message, err := GetEvent[string](dbosCtx, setHandle.GetWorkflowID(), "test-key", 3*time.Second)
		if err != nil {
			t.Fatalf("failed to get event from outside workflow: %v", err)
		}
		if message != "test-message" {
			t.Fatalf("expected received message to be 'test-message', got '%s'", message)
		}

		// Verify step counting for setEventWorkflow (calls SetEvent 1 time)
		setSteps, err := GetWorkflowSteps(dbosCtx, setHandle.GetWorkflowID())
		if err != nil {
			t.Fatalf("failed to get workflow steps for set event workflow: %v", err)
		}
		require.Len(t, setSteps, 1, "expected 1 step in set event workflow (1 SetEvent call), got %d", len(setSteps))
		if setSteps[0].StepID != 0 {
			t.Fatalf("expected step to have StepID 0, got %d", setSteps[0].StepID)
		}
		if setSteps[0].StepName != "DBOS.setEvent" {
			t.Fatalf("expected step to have StepName 'DBOS.setEvent', got '%s'", setSteps[0].StepName)
		}
		require.False(t, setSteps[0].StartedAt.IsZero(), "expected setEvent step to have StartedAt set")
		require.False(t, setSteps[0].CompletedAt.IsZero(), "expected setEvent step to have CompletedAt set")
		require.True(t, setSteps[0].CompletedAt.After(setSteps[0].StartedAt) || setSteps[0].CompletedAt.Equal(setSteps[0].StartedAt),
			"expected setEvent step CompletedAt to be after or equal to StartedAt")
	})

	t.Run("GetEventTimeout", func(t *testing.T) {
		// Try to get an event from a non-existent workflow
		nonExistentID := uuid.NewString()
		_, err := GetEvent[string](dbosCtx, nonExistentID, "test-key", 3*time.Second)
		require.Error(t, err, "expected timeout error when getting event from non-existent workflow, but got none")

		// Check that the error is a ErrorCodeTimeout
		dbosErr, ok := err.(*Error)
		require.True(t, ok, "expected error to be of type *Error, got %T", err)
		require.Equal(t, ErrorCodeTimeout, dbosErr.Code, "expected ErrorCodeTimeout code")
		require.Contains(t, err.Error(), "no event found for key 'test-key' within 3s", "expected error message to contain 'no event found for key 'test-key' within 3s'")

		// Try to get an event from an existing workflow but with a key that doesn't exist
		setHandle, err := RunWorkflow(dbosCtx, setEventWorkflow, setEventWorkflowInput{
			Key:     "test-key",
			Message: "test-message",
		})
		require.NoError(t, err, "failed to set event")
		_, err = setHandle.GetResult()
		require.NoError(t, err, "failed to get result from set event workflow")
		_, err = GetEvent[string](dbosCtx, setHandle.GetWorkflowID(), "non-existent-key", 3*time.Second)
		require.Error(t, err, "expected timeout error when getting event with non-existent key, but got none")
		require.Contains(t, err.Error(), "no event found for key 'non-existent-key' within 3s", "expected error message to contain 'no event found for key 'non-existent-key' within 3s'")

		// Check that the error is a ErrorCodeTimeout
		dbosErr, ok = err.(*Error)
		require.True(t, ok, "expected error to be of type *Error, got %T", err)
		require.Equal(t, ErrorCodeTimeout, dbosErr.Code, "expected ErrorCodeTimeout code")
		require.Contains(t, err.Error(), "no event found for key 'non-existent-key' within 3s", "expected error message to contain 'no event found for key 'non-existent-key' within 3s'")
	})

	t.Run("GetEventTimeoutInWorkflow", func(t *testing.T) {
		// GetEvent waits and times out inside a workflow: the timeout error is
		// checkpointed on the getEvent step and a sleep step records the deadline.
		getHandle, err := RunWorkflow(dbosCtx, getEventWorkflow, getEventWorkflowInput{
			TargetWorkflowID: uuid.NewString(),
			Key:              "no-such-key",
		})
		require.NoError(t, err, "failed to start get event workflow")
		_, err = getHandle.GetResult()
		require.Error(t, err, "expected timeout error")
		dbosErr, ok := err.(*Error)
		require.True(t, ok, "expected error to be of type *Error, got %T", err)
		require.Equal(t, ErrorCodeTimeout, dbosErr.Code, "expected ErrorCodeTimeout code")
		require.Contains(t, err.Error(), "no event found for key 'no-such-key'")

		steps, err := GetWorkflowSteps(dbosCtx, getHandle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps")
		require.Len(t, steps, 2, "expected 2 steps (getEvent that timed out and its sleep), got %d", len(steps))
		require.Equal(t, "DBOS.getEvent", steps[0].StepName, "expected step 0 to have StepName 'DBOS.getEvent'")
		require.NotNil(t, steps[0].Error, "expected getEvent step to record the timeout error")
		require.Contains(t, steps[0].Error.Error(), "no event found for key 'no-such-key'")
		require.Equal(t, "DBOS.sleep", steps[1].StepName, "expected step 1 to have StepName 'DBOS.sleep'")

		// Fork past both steps: the checkpointed timeout error must round-trip through
		// the recorded errStr with its concrete type and code preserved.
		forkedHandle, err := ForkWorkflow[string](dbosCtx, ForkWorkflowInput{OriginalWorkflowID: getHandle.GetWorkflowID(), StartStep: 2})
		require.NoError(t, err, "failed to fork get event workflow")
		_, err = forkedHandle.GetResult()
		require.Error(t, err, "expected replayed timeout error")
		dbosErr, ok = err.(*Error)
		require.True(t, ok, "expected error to be of type *Error, got %T", err)
		require.Equal(t, ErrorCodeTimeout, dbosErr.Code, "expected ErrorCodeTimeout code")
		require.Contains(t, err.Error(), "no event found for key 'no-such-key'")
	})

	t.Run("GetEventForkReplay", func(t *testing.T) {
		setHandle, err := RunWorkflow(dbosCtx, setEventWorkflow, setEventWorkflowInput{
			Key:     "fork-replay-key",
			Message: "fork-me",
		})
		require.NoError(t, err, "failed to start set event workflow")
		_, err = setHandle.GetResult()
		require.NoError(t, err, "failed to get result from set event workflow")

		getHandle, err := RunWorkflow(dbosCtx, getEventForkReplayWorkflow, getEventWorkflowInput{
			TargetWorkflowID: setHandle.GetWorkflowID(),
			Key:              "fork-replay-key",
		})
		require.NoError(t, err, "failed to start get event workflow")
		result, err := getHandle.GetResult()
		require.NoError(t, err, "failed to get result from get event workflow")
		require.Equal(t, "fork-me", result)

		originalSteps, err := GetWorkflowSteps(dbosCtx, getHandle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps")
		require.Len(t, originalSteps, 1, "expected only the getEvent step when the event was already set")

		// Fork past the getEvent step: its checkpoint is copied and the getEvent must replay
		// from it, without waiting (the timeout is 60 minutes) and without recording new steps.
		start := time.Now()
		forkedHandle, err := ForkWorkflow[string](dbosCtx, ForkWorkflowInput{OriginalWorkflowID: getHandle.GetWorkflowID(), StartStep: 2})
		require.NoError(t, err, "failed to fork get event workflow")
		forkedResult, err := forkedHandle.GetResult()
		require.NoError(t, err, "failed to get result from forked get event workflow")
		require.Equal(t, "fork-me", forkedResult, "forked getEvent should replay the checkpointed value")
		require.Less(t, time.Since(start), 30*time.Second, "forked getEvent replay should not wait on the timeout")

		forkedSteps, err := GetWorkflowSteps(dbosCtx, forkedHandle.GetWorkflowID())
		require.NoError(t, err, "failed to get forked workflow steps")
		require.Len(t, forkedSteps, 1, "getEvent replay must not record extra steps")
		require.Equal(t, "DBOS.getEvent", forkedSteps[0].StepName)
	})

	t.Run("SetGetEventMustRunInsideWorkflows", func(t *testing.T) {
		// Attempt to run SetEvent outside of a workflow context
		err := SetEvent(dbosCtx, "test-key", "test-message")
		require.Error(t, err, "expected error when running SetEvent outside of workflow context, but got none")

		// Check the error type
		dbosErr, ok := err.(*Error)
		require.True(t, ok, "expected error to be of type *Error, got %T", err)
		require.Equal(t, ErrorCodeStepExecution, dbosErr.Code)

		// Test the specific message from the error
		expectedMessagePart := "workflow state not found in context: are you running this step within a workflow?"
		require.Contains(t, err.Error(), expectedMessagePart)
	})

	t.Run("SetGetEventIdempotency", func(t *testing.T) {

		// Run set event workflow to completion first
		setHandle, err := RunWorkflow(dbosCtx, setEventIdempotencyWorkflow, setEventWorkflowInput{
			Key:     "idempotency-key",
			Message: "idempotency-message",
		})
		if err != nil {
			t.Fatalf("failed to start set event idempotency workflow: %v", err)
		}
		setResult, err := setHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from set event idempotency workflow: %v", err)
		}
		require.Equal(t, "idempotent-set-completed", setResult, "set workflow result")

		// Now start get event workflow (event is already set) and run to completion
		getHandle, err := RunWorkflow(dbosCtx, getEventIdempotencyWorkflow, setEventWorkflowInput{
			Key:     setHandle.GetWorkflowID(),
			Message: "idempotency-key",
		})
		if err != nil {
			t.Fatalf("failed to start get event idempotency workflow: %v", err)
		}
		getResult, err := getHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from get event idempotency workflow: %v", err)
		}
		require.Equal(t, "idempotency-message", getResult, "get workflow result (event content)")

		// Flip both workflow statuses to PENDING, then recover
		setWorkflowStatusPending(t, dbosCtx, setHandle.GetWorkflowID())
		setWorkflowStatusPending(t, dbosCtx, getHandle.GetWorkflowID())

		// Attempt recovering both workflows. Each should have exactly 1 step.
		recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "failed to recover pending workflows")
		require.Len(t, recoveredHandles, 2, "expected 2 recovered handles, got %d", len(recoveredHandles))

		// Verify step counts (1 step each: setEvent / getEvent)
		setSteps, err := GetWorkflowSteps(dbosCtx, setHandle.GetWorkflowID())
		require.NoError(t, err, "get steps for set event idempotency workflow")
		require.Len(t, setSteps, 1, "expected 1 step in set event idempotency workflow")
		require.Equal(t, 0, setSteps[0].StepID, "set step StepID")
		require.Equal(t, "DBOS.setEvent", setSteps[0].StepName, "set step StepName")
		require.False(t, setSteps[0].StartedAt.IsZero(), "setEvent step StartedAt set")
		require.False(t, setSteps[0].CompletedAt.IsZero(), "setEvent step CompletedAt set")

		getSteps, err := GetWorkflowSteps(dbosCtx, getHandle.GetWorkflowID())
		require.NoError(t, err, "get steps for get event idempotency workflow")
		require.Len(t, getSteps, 1, "expected 1 step in get event idempotency workflow")
		require.Equal(t, 0, getSteps[0].StepID, "get step StepID")
		require.Equal(t, "DBOS.getEvent", getSteps[0].StepName, "get step StepName")
		require.False(t, getSteps[0].StartedAt.IsZero(), "getEvent step StartedAt set")
		require.False(t, getSteps[0].CompletedAt.IsZero(), "getEvent step CompletedAt set")

		// Recovered handles must return the same results
		for _, recoveredHandle := range recoveredHandles {
			if recoveredHandle.GetWorkflowID() == setHandle.GetWorkflowID() {
				recoveredSetResult, err := recoveredHandle.GetResult()
				require.NoError(t, err, "recovered set workflow GetResult")
				require.Equal(t, "idempotent-set-completed", recoveredSetResult, "recovered set result")
			}
			if recoveredHandle.GetWorkflowID() == getHandle.GetWorkflowID() {
				recoveredGetResult, err := recoveredHandle.GetResult()
				require.NoError(t, err, "recovered get workflow GetResult")
				require.Equal(t, "idempotency-message", recoveredGetResult, "recovered get result (event content)")
			}
		}
	})

	t.Run("ConcurrentGetEvent", func(t *testing.T) {
		// Spread more getters than keys: several getters register on each key
		// (within-key concurrency) while distinct keys are independent (across-key
		// concurrency). Getters start before the events are set so they block rather
		// than taking the already-set fast path.
		setWorkflowID := uuid.NewString()
		const numKeys = 3
		const numGoroutines = 12 // > numKeys, so multiple getters land on each key
		keyFor := func(i int) string { return fmt.Sprintf("concurrent-event-key-%d", i%numKeys) }
		valueFor := func(key string) string { return "value-for-" + key }

		values := make(map[string]string, numKeys)
		for k := range numKeys {
			key := fmt.Sprintf("concurrent-event-key-%d", k)
			values[key] = valueFor(key)
		}

		var wg sync.WaitGroup
		errs := make(chan error, numGoroutines)
		wg.Add(numGoroutines)
		for i := range numGoroutines {
			go func(i int) {
				defer wg.Done()
				key := keyFor(i)
				res, err := GetEvent[string](dbosCtx, setWorkflowID, key, 30*time.Second)
				if err != nil {
					errs <- fmt.Errorf("goroutine %d (key %s): %w", i, key, err)
					return
				}
				if want := valueFor(key); res != want {
					errs <- fmt.Errorf("goroutine %d (key %s): expected %q, got %q", i, key, want, res)
				}
			}(i)
		}

		// Wait until every key has a registered waiter before setting the events.
		sysDB := dbosCtx.(*dbosContext).systemDB.(*sysdb.SysDB)
		require.Eventually(t, func() bool {
			for k := range numKeys {
				payload := fmt.Sprintf("%s::%s", setWorkflowID, fmt.Sprintf("concurrent-event-key-%d", k))
				if !sysDB.EventNotifier.Has(payload) {
					return false
				}
			}
			return true
		}, 5*time.Second, 10*time.Millisecond, "not all keys registered event waiters")

		setHandle, err := RunWorkflow(dbosCtx, setManyEventsWorkflow, setManyEventsInput{Values: values}, WithWorkflowID(setWorkflowID))
		require.NoError(t, err, "failed to start set-many-events workflow")
		_, err = setHandle.GetResult()
		require.NoError(t, err, "failed to get result from set-many-events workflow")

		wg.Wait()
		close(errs)
		for err := range errs {
			require.FailNow(t, "goroutine error", err)
		}
	})

	t.Run("GetEventMixedTimeoutAndDelivery", func(t *testing.T) {
		// On a single key, some waiters time out before the event is set and others
		// block long enough to receive it. A departing waiter must not disturb the
		// survivors: they must still receive the value once it is set, not return a
		// premature nil. (Regression test for the per-waiter notify fix.)
		setWorkflowID := uuid.NewString()
		key := "mixed-timeout-key"
		const numLong = 3
		const numShort = 3

		var longWg, shortWg sync.WaitGroup
		longErrs := make(chan error, numLong)
		shortErrs := make(chan error, numShort)

		// Long waiters register first.
		longWg.Add(numLong)
		for i := range numLong {
			go func(i int) {
				defer longWg.Done()
				res, err := GetEvent[string](dbosCtx, setWorkflowID, key, 30*time.Second)
				if err != nil {
					longErrs <- fmt.Errorf("long waiter %d: %w", i, err)
					return
				}
				if res != "mixed-value" {
					longErrs <- fmt.Errorf("long waiter %d: expected %q, got %q", i, "mixed-value", res)
				}
			}(i)
		}

		sysDB := dbosCtx.(*dbosContext).systemDB.(*sysdb.SysDB)
		payload := fmt.Sprintf("%s::%s", setWorkflowID, key)
		require.Eventually(t, func() bool {
			return sysDB.EventNotifier.Has(payload)
		}, 5*time.Second, 10*time.Millisecond, "long waiters never registered")

		// Short waiters register on the same key, then time out and leave.
		shortWg.Add(numShort)
		for i := range numShort {
			go func(i int) {
				defer shortWg.Done()
				_, err := GetEvent[string](dbosCtx, setWorkflowID, key, 300*time.Millisecond)
				if err == nil {
					shortErrs <- fmt.Errorf("short waiter %d: expected a timeout", i)
					return
				}
				dbosErr, ok := err.(*Error)
				if !ok || dbosErr.Code != ErrorCodeTimeout {
					shortErrs <- fmt.Errorf("short waiter %d: expected ErrorCodeTimeout, got %v", i, err)
				}
			}(i)
		}

		// Let the short waiters time out and leave before setting the event.
		shortWg.Wait()
		close(shortErrs)
		for err := range shortErrs {
			require.FailNow(t, "short waiter error", err)
		}

		setHandle, err := RunWorkflow(dbosCtx, setEventWorkflow, setEventWorkflowInput{
			Key:     key,
			Message: "mixed-value",
		}, WithWorkflowID(setWorkflowID))
		require.NoError(t, err, "failed to start set event workflow")
		_, err = setHandle.GetResult()
		require.NoError(t, err, "failed to get result from set event workflow")

		longWg.Wait()
		close(longErrs)
		for err := range longErrs {
			require.FailNow(t, "long waiter error", err)
		}
	})

	t.Run("GetEventSiblingCancellationDoesNotWakeOthers", func(t *testing.T) {
		// A sibling whose context is cancelled mid-wait must not wake the others into
		// returning early: the survivor still receives the value once it is set.
		setWorkflowID := uuid.NewString()
		key := "cancel-sibling-key"

		var survivorWg sync.WaitGroup
		survivorErr := make(chan error, 1)
		survivorWg.Add(1)
		go func() {
			defer survivorWg.Done()
			res, err := GetEvent[string](dbosCtx, setWorkflowID, key, 30*time.Second)
			if err != nil {
				survivorErr <- fmt.Errorf("survivor: %w", err)
				return
			}
			if res != "survivor-value" {
				survivorErr <- fmt.Errorf("survivor: expected %q, got %q", "survivor-value", res)
			}
		}()

		sysDB := dbosCtx.(*dbosContext).systemDB.(*sysdb.SysDB)
		payload := fmt.Sprintf("%s::%s", setWorkflowID, key)
		require.Eventually(t, func() bool {
			return sysDB.EventNotifier.Has(payload)
		}, 5*time.Second, 10*time.Millisecond, "survivor never registered")

		// A sibling on the same key that gets cancelled while waiting.
		cancelCtx, cancel := WithCancel(dbosCtx)
		siblingDone := make(chan struct{})
		go func() {
			defer close(siblingDone)
			_, _ = GetEvent[string](cancelCtx, setWorkflowID, key, 30*time.Second)
		}()
		// Wait until the sibling has actually registered, then cancel it.
		require.Eventually(t, func() bool {
			return sysDB.EventNotifier.WaiterCount(payload) == 2
		}, 5*time.Second, 10*time.Millisecond, "sibling never registered")
		cancel()
		<-siblingDone

		// The survivor must still be waiting; setting the event delivers it.
		setHandle, err := RunWorkflow(dbosCtx, setEventWorkflow, setEventWorkflowInput{
			Key:     key,
			Message: "survivor-value",
		}, WithWorkflowID(setWorkflowID))
		require.NoError(t, err, "failed to start set event workflow")
		_, err = setHandle.GetResult()
		require.NoError(t, err, "failed to get result from set event workflow")

		survivorWg.Wait()
		close(survivorErr)
		for err := range survivorErr {
			require.FailNow(t, "survivor error", err)
		}
	})
}

// Test workflows and steps for parameter mismatch validation
func conflictWorkflowA(dbosCtx Context, input string) (string, error) {
	return RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
		return conflictStepA(ctx)
	})
}

func conflictWorkflowB(dbosCtx Context, input string) (string, error) {
	return RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
		return conflictStepB(ctx)
	})
}

func conflictStepA(_ context.Context) (string, error) {
	return "step-a-result", nil
}

func conflictStepB(_ context.Context) (string, error) {
	return "step-b-result", nil
}

func workflowWithMultipleSteps(dbosCtx Context, input string) (string, error) {
	// First step
	result1, err := RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
		return conflictStepA(ctx)
	})
	if err != nil {
		return "", err
	}

	// Second step - this is where we'll test step name conflicts
	result2, err := RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
		return conflictStepB(ctx)
	})
	if err != nil {
		return "", err
	}

	return result1 + "-" + result2, nil
}

func TestWorkflowExecutionMismatch(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	// Register workflows for testing
	RegisterWorkflow(dbosCtx, conflictWorkflowA)
	RegisterWorkflow(dbosCtx, conflictWorkflowB)
	RegisterWorkflow(dbosCtx, workflowWithMultipleSteps)
	require.NoError(t, Launch(dbosCtx), "failed to launch DBOS")

	t.Run("WorkflowNameConflict", func(t *testing.T) {
		workflowID := uuid.NewString()

		// First, run conflictWorkflowA with a specific workflow ID
		handle, err := RunWorkflow(dbosCtx, conflictWorkflowA, "test-input", WithWorkflowID(workflowID))
		require.NoError(t, err, "failed to start first workflow")

		// Get the result to ensure it completes
		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result from first workflow")
		require.Equal(t, "step-a-result", result)

		// Now try to run conflictWorkflowB with the same workflow ID
		// This should return a ErrorCodeUnexpectedWorkflow
		_, err = RunWorkflow(dbosCtx, conflictWorkflowB, "test-input", WithWorkflowID(workflowID))
		require.Error(t, err, "expected ErrorCodeUnexpectedWorkflow when running different workflow with same ID, but got none")

		// Check that it's the correct error type
		require.True(t, errors.Is(err, ErrUnexpectedWorkflow), "expected error to be ErrorCodeUnexpectedWorkflow, got %T", err)

		// Check that the error message contains the workflow names
		expectedMsgPart := "Workflow already exists with a different name"
		require.Contains(t, err.Error(), expectedMsgPart)
	})

	t.Run("StepNameConflict", func(t *testing.T) {
		handle, err := RunWorkflow(dbosCtx, workflowWithMultipleSteps, "test-input")
		require.NoError(t, err, "failed to start workflow")
		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result from workflow")
		require.Equal(t, "step-a-result-step-b-result", result)

		// Check operation execution with a different step name for the same step ID
		workflowID := handle.GetWorkflowID()

		// This directly tests the CheckOperationExecution method with mismatched step name
		wrongStepName := "wrong-step-name"
		_, err = dbosCtx.(*dbosContext).systemDB.CheckOperationExecution(dbosCtx, sysdb.CheckOperationExecutionDBInput{
			WorkflowID: workflowID,
			StepID:     0,
			StepName:   wrongStepName,
		})

		require.Error(t, err, "expected ErrorCodeUnexpectedStep error when checking operation with wrong step name, but got none")

		// Check that it's the correct error type
		dbosErr, ok := err.(*Error)
		require.True(t, ok, "expected error to be of type *Error, got %T", err)
		require.Equal(t, ErrorCodeUnexpectedStep, dbosErr.Code)

		// Check that the error message contains step information
		require.Contains(t, err.Error(), "Check that your workflow is deterministic")
		require.Contains(t, err.Error(), wrongStepName)
	})
}

func sleepRecoveryWorkflow(dbosCtx Context, duration time.Duration) (time.Duration, error) {
	return Sleep(dbosCtx, duration)
}

func TestSleep(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	RegisterWorkflow(dbosCtx, sleepRecoveryWorkflow)

	// Cancelling Sleep's context mid-sleep should interrupt it and return the
	// partial duration actually slept (less than the full requested duration).
	sleepCancelWorkflow := func(ctx Context, _ string) (time.Duration, error) {
		cancelCtx, cancel := WithCancel(ctx)
		defer cancel()
		go func() {
			time.Sleep(500 * time.Millisecond)
			cancel()
		}()
		return Sleep(cancelCtx, time.Minute)
	}
	RegisterWorkflow(dbosCtx, sleepCancelWorkflow)

	require.NoError(t, Launch(dbosCtx), "failed to launch DBOS")

	t.Run("SleepDurableRecovery", func(t *testing.T) {
		sleepDuration := 2 * time.Second
		workflowID := uuid.NewString()

		handle1, err := RunWorkflow(dbosCtx, sleepRecoveryWorkflow, sleepDuration, WithWorkflowID(workflowID))
		require.NoError(t, err, "failed to start sleep recovery workflow")
		_, err = handle1.GetResult()
		require.NoError(t, err, "failed to get result from first run")

		setWorkflowStatusPending(t, dbosCtx, workflowID)

		// Run the workflow again; sleep step should be replayed from DB so return time is less than durable sleep
		startTime := time.Now()
		handles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "failed to start second sleep recovery workflow")
		require.Len(t, handles, 1, "expected 1 recovered handle")
		handle2 := handles[0]
		_, err = handle2.GetResult()
		require.NoError(t, err, "failed to get result from second run")
		elapsed := time.Since(startTime)
		assert.Less(t, elapsed, sleepDuration, "expected elapsed time to be less than sleep duration")

		// Verify the sleep step was recorded correctly
		steps, err := GetWorkflowSteps(dbosCtx, workflowID)
		require.NoError(t, err, "failed to get workflow steps")
		require.Len(t, steps, 1, "expected 1 step (the sleep), got %d", len(steps))
		step := steps[0]
		assert.Equal(t, 0, step.StepID, "expected step to have StepID 0")
		assert.Equal(t, "DBOS.sleep", step.StepName, "expected step name to be 'DBOS.sleep'")
		assert.Nil(t, step.Error, "expected step to have no error")
		require.False(t, step.StartedAt.IsZero(), "expected sleep step to have StartedAt set")
		require.False(t, step.CompletedAt.IsZero(), "expected sleep step to have CompletedAt set")
		require.True(t, step.CompletedAt.After(step.StartedAt) || step.CompletedAt.Equal(step.StartedAt),
			"expected sleep step CompletedAt to be after or equal to StartedAt")
	})

	t.Run("SleepCannotBeCalledOutsideWorkflow", func(t *testing.T) {
		// Attempt to call Sleep outside of a workflow context
		_, err := Sleep(dbosCtx, 1*time.Second)
		require.Error(t, err, "expected error when calling Sleep outside of workflow context, but got none")

		// Check the error type
		dbosErr, ok := err.(*Error)
		require.True(t, ok, "expected error to be of type *Error, got %T", err)
		require.Equal(t, ErrorCodeStepExecution, dbosErr.Code)

		// Test the specific message from the error
		expectedMessagePart := "workflow state not found in context: are you running this step within a workflow?"
		require.Contains(t, err.Error(), expectedMessagePart)
	})

	t.Run("SleepContextCancellation", func(t *testing.T) {
		handle, err := RunWorkflow(dbosCtx, sleepCancelWorkflow, "")
		require.NoError(t, err, "failed to start sleep workflow")

		slept, err := handle.GetResult()
		require.Error(t, err, "expected a cancellation error from the interrupted Sleep")
		assert.Less(t, slept, time.Minute, "expected interrupted Sleep to return a partial duration, got %v", slept)
	})
}

func TestWorkflowTimeout(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	waitForCancelWorkflow := func(ctx Context, _ string) (string, error) {
		// This workflow will wait indefinitely until it is cancelled
		<-ctx.Done()
		assert.True(t, errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded),
			"workflow was cancelled, but context error is not context.Canceled nor context.DeadlineExceeded: %v", ctx.Err())
		return "", ctx.Err()
	}
	RegisterWorkflow(dbosCtx, waitForCancelWorkflow)

	runWorkflowTimeout := func(t *testing.T) {
		// The reason this sequence works is that the timeout is so fast that the workflow AfterFunc
		// triggers as soon as it is set, likely even before the workflow goroutine is started
		// So we are almost guaranteed that the workflow will be cancelled before returning, hence GetStatus will show it as cancelled
		// Start a workflow that will wait indefinitely
		cancelCtx, cancelFunc := WithTimeout(dbosCtx, 1*time.Millisecond)
		defer cancelFunc() // Ensure we clean up the context
		handle, err := RunWorkflow(cancelCtx, waitForCancelWorkflow, "wait-for-cancel")
		require.NoError(t, err, "failed to start wait for cancel workflow")

		// Wait for the workflow to complete and get the result
		result, err := handle.GetResult()
		assert.True(t, errors.Is(err, context.DeadlineExceeded), "Expected deadline exceeded error, got: %v", err)
		assert.Equal(t, "", result, "expected result to be an empty string")

		// Check the workflow status: should be cancelled
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		assert.Equal(t, WorkflowStatusCancelled, status.Status, "expected workflow status to be WorkflowStatusCancelled")
	}

	wfcStart := NewEvent()
	wfcStop := NewEvent()
	waitForCancelWorkflowManual := func(ctx Context, _ string) (string, error) {
		// This workflow will wait indefinitely until it is cancelled
		<-ctx.Done()
		assert.True(t, errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded),
			"workflow was cancelled, but context error is not context.Canceled nor context.DeadlineExceeded: %v", ctx.Err())
		wfcStart.Set()
		wfcStop.Wait()
		return "", ctx.Err()
	}
	RegisterWorkflow(dbosCtx, waitForCancelWorkflowManual)

	runManuallyCancelWorkflow := func(t *testing.T) {
		// This test requires an event to prevent the workflow for returning before we GetStatus
		// This is because direct cancellation through the cancel function can happen faster than the timeout context AfterFunc
		// This is even more likely in contended environments with few CPU resources
		// When this happens, the workflow will complete first with an error status, and the AfterFunc cancelWorkflow will be a no-op
		// Thus the workflow status will be "Error" instead of "Cancelled" and the test fail
		cancelCtx, cancelFunc := WithTimeout(dbosCtx, 5*time.Hour)
		defer cancelFunc() // Ensure we clean up the context
		handle, err := RunWorkflow(cancelCtx, waitForCancelWorkflowManual, "manual-cancel")
		require.NoError(t, err, "failed to start manual cancel workflow")

		// Cancel the workflow manually
		cancelFunc()
		wfcStart.Wait()

		// Check the workflow status: should be cancelled
		require.Eventually(t, func() bool {
			status, err := handle.GetStatus()
			require.NoError(t, err, "failed to get workflow status")
			return status.Status == WorkflowStatusCancelled
		}, 5*time.Second, 100*time.Millisecond, "workflow did not reach cancelled status in time")

		wfcStop.Set()

		result, err := handle.GetResult()
		assert.True(t, errors.Is(err, context.Canceled), "expected context.Canceled error, got: %v", err)
		assert.Equal(t, "", result, "expected result to be an empty string")
	}

	waitForCancelStep := func(ctx context.Context) (string, error) {
		// This step will trigger cancellation of the entire workflow context
		<-ctx.Done()
		if !errors.Is(ctx.Err(), context.Canceled) && !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return "", fmt.Errorf("step was cancelled, but context error is not context.Canceled nor context.DeadlineExceeded: %v", ctx.Err())
		}
		return "", ctx.Err()
	}

	waitForCancelWorkflowWithStep := func(ctx Context, _ string) (string, error) {
		return RunAsStep(ctx, func(context context.Context) (string, error) {
			return waitForCancelStep(context)
		})
	}
	RegisterWorkflow(dbosCtx, waitForCancelWorkflowWithStep)

	runWorkflowWithStepTimeout := func(t *testing.T) {
		// Start a workflow that will run a step that triggers cancellation
		cancelCtx, cancelFunc := WithTimeout(dbosCtx, 100*time.Millisecond)
		defer cancelFunc() // Ensure we clean up the context
		handle, err := RunWorkflow(cancelCtx, waitForCancelWorkflowWithStep, "wf-with-step-timeout")
		require.NoError(t, err, "failed to start workflow with step timeout")

		// Wait for the workflow to complete and get the result
		result, err := handle.GetResult()
		assert.True(t, errors.Is(err, context.DeadlineExceeded), "Expected deadline exceeded error, got: %v", err)
		assert.Equal(t, "", result, "expected result to be an empty string")

		// Check the workflow status: should be cancelled
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		assert.Equal(t, WorkflowStatusCancelled, status.Status, "expected workflow status to be WorkflowStatusCancelled")
	}

	waitForCancelWorkflowWithStepAfterCancel := func(ctx Context, _ string) (string, error) {
		uncancellableCtx := WithoutCancel(ctx)
		// Wait for cancellation
		<-ctx.Done()
		// Check that we have the correct cancellation error
		if !errors.Is(ctx.Err(), context.Canceled) && !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return "", fmt.Errorf("workflow was cancelled, but context error is not context.Canceled nor context.DeadlineExceeded: %v", ctx.Err())
		}

		// The status of this workflow should transition to cancelled
		wfid, err := GetWorkflowID(uncancellableCtx)
		if err != nil {
			return "", fmt.Errorf("failed to get workflow ID: %w", err)
		}
		dbosCtxInternal, ok := uncancellableCtx.(*dbosContext)
		if !ok {
			return "", fmt.Errorf("failed to cast Context to dbosContext")
		}
		sysDB, ok := dbosCtxInternal.systemDB.(*sysdb.SysDB)
		if !ok {
			return "", fmt.Errorf("failed to cast systemDB to sysDB")
		}
		query := sysDB.RenderSQL(`SELECT status FROM %sworkflow_status WHERE workflow_uuid = $1`, sysDB.Dialect().SchemaPrefix(sysDB.Schema()))
		require.Eventually(t, func() bool {
			var status WorkflowStatusType
			err := sysDB.Pool().QueryRow(uncancellableCtx, query, wfid).Scan(&status)
			if err != nil {
				return false
			}
			return status == WorkflowStatusCancelled
		}, 5*time.Second, 50*time.Millisecond, "workflow did not transition to cancelled status in time")

		// After cancellation, try to run a simple step
		// This should return a ErrorCodeWorkflowCancelled error
		return RunAsStep(ctx, simpleStep)
	}
	RegisterWorkflow(dbosCtx, waitForCancelWorkflowWithStepAfterCancel)

	runWorkflowWithStepAfterTimeout := func(t *testing.T) {
		// Start a workflow that waits for cancellation then tries to run a step
		cancelCtx, cancelFunc := WithTimeout(dbosCtx, 1*time.Millisecond)
		defer cancelFunc() // Ensure we clean up the context
		handle, err := RunWorkflow(cancelCtx, waitForCancelWorkflowWithStepAfterCancel, "wf-with-step-after-timeout")
		require.NoError(t, err, "failed to start workflow with step after timeout")

		// Wait for the workflow to complete and get the result
		result, err := handle.GetResult()
		// The workflow should return a ErrorCodeWorkflowCancelled error from the step
		require.Error(t, err, "expected error from workflow")

		assert.True(t, errors.Is(err, ErrWorkflowCancelled), "expected ErrorCodeWorkflowCancelled error, got: %v", err)
		assert.Equal(t, "", result, "expected result to be an empty string")

		// Check the workflow status: should be cancelled
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		assert.Equal(t, WorkflowStatusCancelled, status.Status, "expected workflow status to be WorkflowStatusCancelled")
	}

	shorterStepTimeoutWorkflow := func(ctx Context, _ string) (string, error) {
		// This workflow will run a step that has a shorter timeout than the workflow itself
		// The timeout will trigger a step error, the workflow can do whatever it wants with that error
		stepCtx, stepCancelFunc := WithTimeout(ctx, 1*time.Millisecond)
		defer stepCancelFunc() // Ensure we clean up the context
		_, err := RunAsStep(stepCtx, func(context context.Context) (string, error) {
			return waitForCancelStep(context)
		})
		assert.True(t, errors.Is(err, context.DeadlineExceeded), "expected step to timeout, got: %v", err)
		return "step-timed-out", nil
	}
	RegisterWorkflow(dbosCtx, shorterStepTimeoutWorkflow)

	runShorterStepTimeout := func(t *testing.T) {
		// Start a workflow that runs a step with a shorter timeout than the workflow itself
		cancelCtx, cancelFunc := WithTimeout(dbosCtx, 5*time.Second)
		defer cancelFunc() // Ensure we clean up the context
		handle, err := RunWorkflow(cancelCtx, shorterStepTimeoutWorkflow, "shorter-step-timeout")
		require.NoError(t, err, "failed to start shorter step timeout workflow")
		// Wait for the workflow to complete and get the result
		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result from shorter step timeout workflow")
		assert.Equal(t, "step-timed-out", result, "expected result to be 'step-timed-out'")
		// Status is SUCCESS
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		assert.Equal(t, WorkflowStatusSuccess, status.Status, "expected workflow status to be WorkflowStatusSuccess")
	}

	detachedStep := func(ctx context.Context, timeout time.Duration) (string, error) {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(timeout):
		}
		return "detached-step-completed", nil
	}

	detachedStepWorkflow := func(ctx Context, timeout time.Duration) (string, error) {
		// This workflow will run a step that is not cancelable.
		// What this means is the workflow *will* be cancelled, but the step will run normally
		stepCtx := WithoutCancel(ctx)
		res, err := RunAsStep(stepCtx, func(context context.Context) (string, error) {
			return detachedStep(context, timeout*2)
		})
		require.NoError(t, err, "failed to run detached step")
		assert.Equal(t, "detached-step-completed", res, "expected detached step result to be 'detached-step-completed'")
		return res, ctx.Err()
	}
	RegisterWorkflow(dbosCtx, detachedStepWorkflow)

	runDetachedStepWorkflow := func(t *testing.T) {
		// Start a workflow that runs a detached (uncancelable) step.
		// A detached step only ignores *context* cancellation; its start is still
		// gated by checkOperationExecution, which refuses the step if the workflow
		// is already marked CANCELLED in the DB. A 1ms deadline races the step's
		// first DB read against the deadline's cancel-DB-write, so under connection
		// contention the step can be refused with ErrorCodeWorkflowCancelled ("failed to run
		// detached step"). Give the step a comfortable head start to pass that check
		// before the deadline fires; the step still runs 2s (input*2), well past the
		// deadline, so the workflow is genuinely cancelled mid-step.
		cancelCtx, cancelFunc := WithTimeout(dbosCtx, 250*time.Millisecond)
		defer cancelFunc() // Ensure we clean up the context

		handle, err := RunWorkflow(cancelCtx, detachedStepWorkflow, 1*time.Second)
		require.NoError(t, err, "failed to start detached step workflow")
		// Wait for the workflow to complete and get the result
		result, err := handle.GetResult()
		assert.True(t, errors.Is(err, context.DeadlineExceeded), "Expected deadline exceeded error, got: %v", err)
		assert.Equal(t, "detached-step-completed", result, "expected result to be 'detached-step-completed'")
		// Check the workflow status: should be cancelled
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		assert.Equal(t, WorkflowStatusCancelled, status.Status, "expected workflow status to be WorkflowStatusCancelled")
	}

	waitForCancelParent := func(ctx Context, childWorkflowID string) (string, error) {
		// This workflow will run a child workflow that waits indefinitely until it is cancelled
		childHandle, err := RunWorkflow(ctx, waitForCancelWorkflow, "child-wait-for-cancel", WithWorkflowID(childWorkflowID))
		require.NoError(t, err, "failed to start child workflow")

		// Wait for the child workflow to complete. The terminal error may come
		// back either wrapped as a DBOS ErrorCodeWorkflowCancelled error or as the raw
		// context deadline-exceeded, depending on which side observed the
		// cancellation first — both are valid signals that the child was
		// cancelled.
		result, err := childHandle.GetResult()
		assert.True(t,
			errors.Is(err, ErrWorkflowCancelled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled),
			"expected child workflow to be cancelled, got: %v", err)
		return result, ctx.Err()
	}
	RegisterWorkflow(dbosCtx, waitForCancelParent)

	runChildWorkflowTimesout := func(t *testing.T) {
		// Start a parent workflow that runs a child workflow that waits indefinitely
		cancelCtx, cancelFunc := WithTimeout(dbosCtx, 1*time.Millisecond)
		defer cancelFunc() // Ensure we clean up the context

		childWorkflowID := "child-wait-for-cancel-" + uuid.NewString()
		handle, err := RunWorkflow(cancelCtx, waitForCancelParent, childWorkflowID)
		require.NoError(t, err, "failed to start parent workflow")

		// Wait for the parent workflow to complete and get the result
		result, err := handle.GetResult()
		assert.True(t, errors.Is(err, context.DeadlineExceeded), "Expected deadline exceeded error, got: %v", err)
		assert.Equal(t, "", result, "expected result to be an empty string")

		// Check the workflow status: should be cancelled
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		assert.Equal(t, WorkflowStatusCancelled, status.Status, "expected workflow status to be WorkflowStatusCancelled")

		// Check the child workflow status: should be cancelled
		childHandle, err := RetrieveWorkflow[string](dbosCtx, childWorkflowID)
		require.NoError(t, err, "failed to get child workflow handle")
		require.Eventually(t, func() bool {
			s, err := childHandle.GetStatus()
			return err == nil && s.Status == WorkflowStatusCancelled
		}, 5*time.Second, 50*time.Millisecond, "expected child workflow status to be WorkflowStatusCancelled")
	}

	detachedChild := func(ctx Context, timeout time.Duration) (string, error) {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(timeout):
		}
		return "detached-step-completed", nil
	}
	RegisterWorkflow(dbosCtx, detachedChild)

	detachedChildWorkflowParent := func(ctx Context, timeout time.Duration) (string, error) {
		childCtx := WithoutCancel(ctx)
		myID, err := GetWorkflowID(ctx)
		require.NoError(t, err, "failed to get parent workflow ID")
		childWorkflowID := fmt.Sprintf("%s-detached-child", myID)
		childHandle, err := RunWorkflow(childCtx, detachedChild, timeout*2, WithWorkflowID(childWorkflowID))
		require.NoError(t, err, "failed to start child workflow")

		// Wait for the child workflow to complete
		result, err := childHandle.GetResult()
		require.NoError(t, err, "failed to get result from child workflow")
		// The child spun for timeout*2 so ctx.Err() should be context.DeadlineExceeded
		return result, ctx.Err()
	}
	RegisterWorkflow(dbosCtx, detachedChildWorkflowParent)

	require.NoError(t, Launch(dbosCtx), "failed to launch DBOS")

	t.Run("WorkflowTimeout", runWorkflowTimeout)
	t.Run("ManuallyCancelWorkflow", runManuallyCancelWorkflow)
	t.Run("WorkflowWithStepTimeout", runWorkflowWithStepTimeout)
	t.Run("WorkflowWithStepAfterTimeout", runWorkflowWithStepAfterTimeout)
	t.Run("ShorterStepTimeout", runShorterStepTimeout)
	t.Run("DetachedStepWorkflow", runDetachedStepWorkflow)
	t.Run("ChildWorkflowTimesout", runChildWorkflowTimesout)

	t.Run("ChildWorkflowDetached", func(t *testing.T) {
		timeout := 500 * time.Millisecond
		cancelCtx, cancelFunc := WithTimeout(dbosCtx, timeout)
		defer cancelFunc()
		handle, err := RunWorkflow(cancelCtx, detachedChildWorkflowParent, timeout)
		require.NoError(t, err, "failed to start parent workflow with detached child")

		// Wait for the parent workflow to complete and get the result
		result, err := handle.GetResult()
		assert.True(t, errors.Is(err, context.DeadlineExceeded), "Expected deadline exceeded error, got: %v", err)
		assert.Equal(t, "detached-step-completed", result, "expected result to be 'detached-step-completed'")

		// Check the workflow status: should be cancelled
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		assert.Equal(t, WorkflowStatusCancelled, status.Status, "expected workflow status to be WorkflowStatusCancelled")

		// Check the child workflow status: should be cancelled
		childHandle, err := RetrieveWorkflow[string](dbosCtx, fmt.Sprintf("%s-detached-child", handle.GetWorkflowID()))
		require.NoError(t, err, "failed to get child workflow handle")
		status, err = childHandle.GetStatus()
		require.NoError(t, err, "failed to get child workflow status")
		assert.Equal(t, WorkflowStatusSuccess, status.Status, "expected child workflow status to be WorkflowStatusSuccess")
	})

	t.Run("RecoverWaitForCancelWorkflow", func(t *testing.T) {
		start := time.Now()
		timeout := 1 * time.Second
		cancelCtx, cancelFunc := WithTimeout(dbosCtx, timeout)
		defer cancelFunc()
		handle, err := RunWorkflow(cancelCtx, waitForCancelWorkflow, "recover-wait-for-cancel")
		require.NoError(t, err, "failed to start wait for cancel workflow")

		// Wait for the workflow to complete (timeout cancels the workflow)
		_, err = handle.GetResult()
		require.True(t, errors.Is(err, context.DeadlineExceeded), "expected context.DeadlineExceeded, got: %v", err)
		// Check the workflow status: should be cancelled
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		assert.Equal(t, WorkflowStatusCancelled, status.Status, "expected workflow status to be WorkflowStatusCancelled")

		// Flip the state
		setWorkflowStatusPending(t, dbosCtx, handle.GetWorkflowID())

		// Recover the pending workflow
		recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "failed to recover pending workflows")
		require.Len(t, recoveredHandles, 1, "expected 1 recovered handle, got %d", len(recoveredHandles))
		recoveredHandle := recoveredHandles[0]
		assert.Equal(t, handle.GetWorkflowID(), recoveredHandle.GetWorkflowID(), "expected recovered handle to have same ID")

		// Wait for the workflow to complete. Should return an ErrorCodeAwaitedWorkflowCancelled error.
		_, err = recoveredHandle.GetResult()
		// Check the error type
		dbosErr, ok := err.(*Error)
		require.True(t, ok, "expected error to be of type *Error, got %T", err)
		require.Equal(t, ErrorCodeAwaitedWorkflowCancelled, dbosErr.Code)

		// Check the workflow status: should be cancelled
		recoveredStatus, err := recoveredHandle.GetStatus()
		require.NoError(t, err, "failed to get recovered workflow status")
		assert.Equal(t, WorkflowStatusCancelled, recoveredStatus.Status, "expected recovered workflow status to be WorkflowStatusCancelled")

		// The durable deadline is insert time + timeout; allow tolerance for insert latency and ms truncation
		assert.WithinDuration(t, start.Add(timeout), status.Deadline, 500*time.Millisecond,
			"expected workflow deadline to be about %v, got %v", start.Add(timeout), status.Deadline)
	})

}

func notificationWaiterWorkflow(ctx Context, pairID int) (string, error) {
	result, err := GetEvent[string](ctx, fmt.Sprintf("notification-setter-%d", pairID), "event-key", 10*time.Second)
	if err != nil {
		return "", err
	}
	return result, nil
}

func notificationSetterWorkflow(ctx Context, pairID int) (string, error) {
	err := SetEvent(ctx, "event-key", fmt.Sprintf("notification-message-%d", pairID))
	if err != nil {
		return "", err
	}
	return "event-set", nil
}

func sendRecvReceiverWorkflow(ctx Context, pairID int) (string, error) {
	result, err := Recv[string](ctx, "send-recv-topic", 10*time.Second)
	if err != nil {
		return "", err
	}
	return result, nil
}

func sendRecvSenderWorkflow(ctx Context, pairID int) (string, error) {
	err := Send(ctx, fmt.Sprintf("send-recv-receiver-%d", pairID), fmt.Sprintf("send-recv-message-%d", pairID), "send-recv-topic")
	if err != nil {
		return "", err
	}
	return "message-sent", nil
}

func concurrentSimpleWorkflow(dbosCtx Context, input int) (int, error) {
	return RunAsStep(dbosCtx, func(ctx context.Context) (int, error) {
		return input * 2, nil
	})
}

func TestConcurrentWorkflows(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	RegisterWorkflow(dbosCtx, concurrentSimpleWorkflow)
	RegisterWorkflow(dbosCtx, notificationWaiterWorkflow)
	RegisterWorkflow(dbosCtx, notificationSetterWorkflow)
	RegisterWorkflow(dbosCtx, sendRecvReceiverWorkflow)
	RegisterWorkflow(dbosCtx, sendRecvSenderWorkflow)
	require.NoError(t, Launch(dbosCtx), "failed to launch DBOS")

	t.Run("SimpleWorkflow", func(t *testing.T) {
		const numGoroutines = 500
		var wg sync.WaitGroup
		results := make(chan int, numGoroutines)
		errors := make(chan error, numGoroutines)

		wg.Add(numGoroutines)
		for i := range numGoroutines {
			go func(input int) {
				defer wg.Done()
				handle, err := RunWorkflow(dbosCtx, concurrentSimpleWorkflow, input)
				if err != nil {
					errors <- fmt.Errorf("failed to start workflow %d: %w", input, err)
					return
				}
				result, err := handle.GetResult()
				if err != nil {
					errors <- fmt.Errorf("failed to get result for workflow %d: %w", input, err)
					return
				}
				expectedResult := input * 2
				if result != expectedResult {
					errors <- fmt.Errorf("workflow %d: expected result %d, got %d", input, expectedResult, result)
					return
				}
				results <- result
			}(i)
		}

		wg.Wait()
		close(results)
		close(errors)

		if len(errors) > 0 {
			for err := range errors {
				t.Errorf("Error from send/recv workflows: %v", err)
			}
		}

		resultCount := 0
		receivedResults := make(map[int]bool)
		for result := range results {
			resultCount++
			if result < 0 || result >= numGoroutines*2 || result%2 != 0 {
				t.Errorf("Unexpected result %d", result)
			} else {
				receivedResults[result] = true
			}
		}

		assert.Equal(t, numGoroutines, resultCount, "Expected correct number of results")
	})

	t.Run("NotificationWorkflows", func(t *testing.T) {
		const numPairs = 500
		var wg sync.WaitGroup
		waiterResults := make(chan string, numPairs)
		setterResults := make(chan string, numPairs)
		errors := make(chan error, numPairs*2)

		wg.Add(numPairs * 2)

		for i := range numPairs {
			go func(pairID int) {
				defer wg.Done()
				handle, err := RunWorkflow(dbosCtx, notificationSetterWorkflow, pairID, WithWorkflowID(fmt.Sprintf("notification-setter-%d", pairID)))
				if err != nil {
					errors <- fmt.Errorf("failed to start setter workflow %d: %w", pairID, err)
					return
				}
				result, err := handle.GetResult()
				if err != nil {
					errors <- fmt.Errorf("failed to get result for setter workflow %d: %w", pairID, err)
					return
				}
				setterResults <- result
			}(i)

			go func(pairID int) {
				defer wg.Done()
				handle, err := RunWorkflow(dbosCtx, notificationWaiterWorkflow, pairID)
				if err != nil {
					errors <- fmt.Errorf("failed to start waiter workflow %d: %w", pairID, err)
					return
				}
				result, err := handle.GetResult()
				if err != nil {
					errors <- fmt.Errorf("failed to get result for waiter workflow %d: %w", pairID, err)
					return
				}
				expectedMessage := fmt.Sprintf("notification-message-%d", pairID)
				if result != expectedMessage {
					errors <- fmt.Errorf("waiter workflow %d: expected message '%s', got '%s'", pairID, expectedMessage, result)
					return
				}
				waiterResults <- result
			}(i)
		}

		wg.Wait()
		close(waiterResults)
		close(setterResults)
		close(errors)

		if len(errors) > 0 {
			for err := range errors {
				t.Errorf("Error from send/recv workflows: %v", err)
			}
		}

		waiterCount := 0
		receivedWaiterResults := make(map[string]bool)
		for result := range waiterResults {
			waiterCount++
			receivedWaiterResults[result] = true
		}

		setterCount := 0
		for result := range setterResults {
			setterCount++
			assert.Equal(t, "event-set", result, "Expected setter result to be 'event-set'")
		}

		assert.Equal(t, numPairs, waiterCount, "Expected correct number of waiter results")
		assert.Equal(t, numPairs, setterCount, "Expected correct number of setter results")

		for i := range numPairs {
			expectedWaiterResult := fmt.Sprintf("notification-message-%d", i)
			assert.True(t, receivedWaiterResults[expectedWaiterResult], "Expected waiter result '%s' not found", expectedWaiterResult)
		}
	})

	t.Run("SendRecvWorkflows", func(t *testing.T) {
		numPairs := 500
		if useSqliteBackend() {
			numPairs = 100
		}
		var wg sync.WaitGroup
		receiverResults := make(chan string, numPairs)
		senderResults := make(chan string, numPairs)
		errors := make(chan error, numPairs*2)

		// Phase 1: register all receivers in parallel so their workflow_status
		// rows exist before any sender does its Send-target lookup. Sequential
		// registration would let early receivers' 10s Recv timer expire before
		// later receivers were even registered (and before phase 2 launches
		// senders), causing every receiver to time out.
		receiverHandles := make([]WorkflowHandle[string], numPairs)
		regErrs := make([]error, numPairs)
		var regWg sync.WaitGroup
		regWg.Add(numPairs)
		for i := range numPairs {
			go func(pairID int) {
				defer regWg.Done()
				h, err := RunWorkflow(dbosCtx, sendRecvReceiverWorkflow, pairID, WithWorkflowID(fmt.Sprintf("send-recv-receiver-%d", pairID)))
				if err != nil {
					regErrs[pairID] = err
					return
				}
				receiverHandles[pairID] = h
			}(i)
		}
		regWg.Wait()
		for i, err := range regErrs {
			if err != nil {
				t.Fatalf("failed to start receiver workflow %d: %v", i, err)
			}
		}

		wg.Add(numPairs * 2)

		for i := range numPairs {
			go func(pairID int) {
				defer wg.Done()
				handle := receiverHandles[pairID]
				result, err := handle.GetResult()
				if err != nil {
					errors <- fmt.Errorf("failed to get result for receiver workflow %d: %w", pairID, err)
					return
				}
				expectedMessage := fmt.Sprintf("send-recv-message-%d", pairID)
				if result != expectedMessage {
					errors <- fmt.Errorf("receiver workflow %d: expected message '%s', got '%s'", pairID, expectedMessage, result)
					return
				}
				receiverResults <- result
			}(i)

			go func(pairID int) {
				defer wg.Done()
				handle, err := RunWorkflow(dbosCtx, sendRecvSenderWorkflow, pairID)
				if err != nil {
					errors <- fmt.Errorf("failed to start sender workflow %d: %w", pairID, err)
					return
				}
				result, err := handle.GetResult()
				if err != nil {
					errors <- fmt.Errorf("failed to get result for sender workflow %d: %w", pairID, err)
					return
				}
				senderResults <- result
			}(i)
		}

		wg.Wait()
		close(receiverResults)
		close(senderResults)
		close(errors)

		if len(errors) > 0 {
			for err := range errors {
				t.Errorf("Error from send/recv workflows: %v", err)
			}
		}

		receiverCount := 0
		receivedReceiverResults := make(map[string]bool)
		for result := range receiverResults {
			receiverCount++
			receivedReceiverResults[result] = true
		}

		senderCount := 0
		for result := range senderResults {
			senderCount++
			assert.Equal(t, "message-sent", result, "Expected sender result to be 'message-sent'")
		}

		assert.Equal(t, numPairs, receiverCount, "Expected correct number of receiver results")
		assert.Equal(t, numPairs, senderCount, "Expected correct number of sender results")

		for i := range numPairs {
			expectedReceiverResult := fmt.Sprintf("send-recv-message-%d", i)
			assert.True(t, receivedReceiverResults[expectedReceiverResult], "Expected receiver result '%s' not found", expectedReceiverResult)
		}
	})
}

func TestWorkflowAtVersion(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	RegisterWorkflow(dbosCtx, simpleWorkflow)
	require.NoError(t, Launch(dbosCtx), "failed to launch DBOS")

	version := "test-app-version-12345"
	handle, err := RunWorkflow(dbosCtx, simpleWorkflow, "input", WithApplicationVersion(version))
	require.NoError(t, err, "failed to start workflow")

	_, err = handle.GetResult()
	require.NoError(t, err, "failed to get workflow result")

	retrieved, err := RetrieveWorkflow[string](dbosCtx, handle.GetWorkflowID())
	require.NoError(t, err, "failed to retrieve workflow")

	status, err := retrieved.GetStatus()
	require.NoError(t, err, "failed to get workflow status")
	assert.Equal(t, version, status.ApplicationVersion, "expected correct application version")
}

func TestWorkflowCancel(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	blockingEvent := NewEvent()

	// Workflow that waits for an event, then calls Recv(). Returns raw error if Recv fails
	blockingWorkflow := func(ctx Context, topic string) (string, error) {
		// Wait for the event
		blockingEvent.Wait()

		// Now call Recv() - this should fail if the workflow is cancelled
		msg, err := Recv[string](ctx, topic, 5*time.Second)
		if err != nil {
			return "", err // Return the raw error from Recv
		}
		return msg, nil
	}
	RegisterWorkflow(dbosCtx, blockingWorkflow)

	blockingEventNoError := NewEvent()

	// Workflow that waits for an event, then calls Recv(). Does NOT return error when Recv times out
	blockingWorkflowNoError := func(ctx Context, topic string) (string, error) {
		// Wait for the event
		blockingEventNoError.Wait()
		Recv[string](ctx, topic, 5*time.Second)
		// Ignore the error
		return "", nil
	}
	RegisterWorkflow(dbosCtx, blockingWorkflowNoError)

	require.NoError(t, Launch(dbosCtx), "failed to launch DBOS")

	t.Run("TestWorkflowCancelWithRecvError", func(t *testing.T) {
		topic := "cancel-test-topic"

		// Start the blocking workflow
		handle, err := RunWorkflow(dbosCtx, blockingWorkflow, topic)
		require.NoError(t, err, "failed to start blocking workflow")

		// Cancel the workflow using DBOS.CancelWorkflow
		err = CancelWorkflow(dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err, "failed to cancel workflow")

		// Signal the event so the workflow can move on to Recv()
		blockingEvent.Set()

		// Check the return values of the workflow
		result, err := handle.GetResult()
		require.Error(t, err, "expected error from cancelled workflow")
		assert.Equal(t, "", result, "expected empty result from cancelled workflow")

		// Check that we get a Error with ErrorCodeWorkflowCancelled code
		var dbosErr *Error
		require.ErrorAs(t, err, &dbosErr, "expected error to be of type *Error, got %T", err)
		assert.Equal(t, ErrorCodeWorkflowCancelled, dbosErr.Code, "expected ErrorCodeAwaitedWorkflowCancelled error code, got: %v", dbosErr.Code)

		// Ensure the workflow status is of an error type
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		assert.Equal(t, WorkflowStatusCancelled, status.Status, "expected workflow status to be WorkflowStatusCancelled")
	})

	t.Run("TestWorkflowCancelWithSuccess", func(t *testing.T) {
		topic := "cancel-no-error-test-topic"

		// Start the blocking workflow
		handle, err := RunWorkflow(dbosCtx, blockingWorkflowNoError, topic)
		require.NoError(t, err, "failed to start blocking workflow")

		// Cancel the workflow using DBOS.CancelWorkflow
		err = CancelWorkflow(dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err, "failed to cancel workflow")

		// Signal the event so the workflow can move on to Recv()
		blockingEventNoError.Set()

		// The workflow swallowed the cancellation and returned success, but the
		// outcome gate detected the CANCELLED row and propagates the cancellation
		// to the caller instead of reporting success.
		result, err := handle.GetResult()
		require.Error(t, err, "expected cancellation error from direct handle")
		var directErr *Error
		require.ErrorAs(t, err, &directErr, "expected error to be of type *Error, got %T", err)
		assert.Equal(t, ErrorCodeWorkflowCancelled, directErr.Code, "expected ErrorCodeWorkflowCancelled error code, got: %v", directErr.Code)
		assert.Equal(t, "", result, "expected empty result from cancelled workflow")

		// Now use a polling handle to get result -- observe the error
		pollingHandle, err := RetrieveWorkflow[string](dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err, "failed to retrieve workflow with polling handle")

		result, err = pollingHandle.GetResult()
		require.Error(t, err, "expected error from cancelled workflow even when workflow returns success")
		assert.Equal(t, "", result, "expected empty result from cancelled workflow")

		// Check that we still get a Error with ErrorCodeAwaitedWorkflowCancelled code
		// The gate prevents CANCELLED -> SUCCESS transition
		var dbosErr *Error
		require.ErrorAs(t, err, &dbosErr, "expected error to be of type *Error, got %T", err)
		assert.Equal(t, ErrorCodeAwaitedWorkflowCancelled, dbosErr.Code, "expected ErrorCodeAwaitedWorkflowCancelled error code, got: %v", dbosErr.Code)

		// Ensure the workflow status remains CANCELLED
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		assert.Equal(t, WorkflowStatusCancelled, status.Status, "expected workflow status to remain WorkflowStatusCancelled due to gate")
	})
}

var cancelAllBeforeBlockEvent = NewEvent()

func cancelAllBeforeBlockingWorkflow(ctx Context, input string) (string, error) {
	cancelAllBeforeBlockEvent.Wait()
	return input, nil
}

func TestCancelAllBefore(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	RegisterWorkflow(dbosCtx, cancelAllBeforeBlockingWorkflow)
	RegisterWorkflow(dbosCtx, simpleWorkflow)

	// Create a queue for testing enqueued workflows
	queue, err := RegisterQueue(dbosCtx, "test-cancel-queue")
	require.NoError(t, err, "failed to register queue")
	require.NoError(t, Launch(dbosCtx), "failed to launch DBOS")

	t.Run("CancelAllBefore", func(t *testing.T) {
		now := time.Now()
		cutoffTime := now.Add(3 * time.Second)

		// Create workflows that should be cancelled (PENDING/ENQUEUED before cutoff)
		shouldBeCancelledIDs := make([]string, 0)

		// Create 2 PENDING workflows before cutoff time
		for i := range 2 {
			handle, err := RunWorkflow(dbosCtx, cancelAllBeforeBlockingWorkflow, fmt.Sprintf("pending-before-%d", i))
			require.NoError(t, err, "failed to start pending workflow %d", i)
			shouldBeCancelledIDs = append(shouldBeCancelledIDs, handle.GetWorkflowID())
		}

		// Create 2 ENQUEUED workflows before cutoff time
		for i := range 2 {
			handle, err := RunWorkflow(dbosCtx, cancelAllBeforeBlockingWorkflow, fmt.Sprintf("enqueued-before-%d", i), WithQueue(queue))
			require.NoError(t, err, "failed to start enqueued workflow %d", i)
			shouldBeCancelledIDs = append(shouldBeCancelledIDs, handle.GetWorkflowID())
		}

		// Create workflows that should NOT be cancelled

		// Create 1 SUCCESS workflow before cutoff time (but complete it)
		successHandle, err := RunWorkflow(dbosCtx, simpleWorkflow, "success-before")
		require.NoError(t, err, "failed to start success workflow")
		_, err = successHandle.GetResult()
		require.NoError(t, err, "failed to complete success workflow")
		shouldNotBeCancelledIDs := []string{successHandle.GetWorkflowID()}

		// Sleep to ensure we pass the cutoff time
		time.Sleep(4 * time.Second)

		// Create 2 PENDING/ENQUEUED workflows after cutoff time
		for i := range 2 {
			handle, err := RunWorkflow(dbosCtx, cancelAllBeforeBlockingWorkflow, fmt.Sprintf("pending-after-%d", i))
			require.NoError(t, err, "failed to start pending workflow after cutoff %d", i)
			shouldNotBeCancelledIDs = append(shouldNotBeCancelledIDs, handle.GetWorkflowID())
		}

		// Call cancelAllBefore
		err = dbosCtx.(*dbosContext).systemDB.CancelAllBefore(dbosCtx, cutoffTime)
		require.NoError(t, err, "failed to call cancelAllBefore")

		// Verify workflows that should be cancelled
		for _, wfID := range shouldBeCancelledIDs {
			handle, err := RetrieveWorkflow[string](dbosCtx, wfID)
			require.NoError(t, err, "failed to retrieve workflow %s", wfID)

			status, err := handle.GetStatus()
			require.NoError(t, err, "failed to get status for workflow %s", wfID)
			assert.Equal(t, WorkflowStatusCancelled, status.Status, "workflow %s should be cancelled", wfID)
		}

		// Verify workflows that should NOT be cancelled
		for _, wfID := range shouldNotBeCancelledIDs {
			handle, err := RetrieveWorkflow[string](dbosCtx, wfID)
			require.NoError(t, err, "failed to retrieve workflow %s", wfID)

			status, err := handle.GetStatus()
			require.NoError(t, err, "failed to get status for workflow %s", wfID)
			assert.NotEqual(t, WorkflowStatusCancelled, status.Status, "workflow %s should NOT be cancelled", wfID)
		}

		// Unblock any remaining workflows
		cancelAllBeforeBlockEvent.Set()

		// Wait for workflows to complete and verify they were cancelled
		for _, wfID := range shouldBeCancelledIDs {
			handle, err := RetrieveWorkflow[string](dbosCtx, wfID)
			require.NoError(t, err, "failed to retrieve cancelled workflow %s", wfID)

			_, err = handle.GetResult()
			if err != nil {
				// Should get a Error with ErrorCodeAwaitedWorkflowCancelled code
				var dbosErr *Error
				if errors.As(err, &dbosErr) {
					assert.Equal(t, ErrorCodeAwaitedWorkflowCancelled, dbosErr.Code, "expected ErrorCodeAwaitedWorkflowCancelled error code for workflow %s, got: %v", wfID, dbosErr.Code)
				} else {
					// Fallback: check if error message contains "cancelled"
					assert.Contains(t, err.Error(), "cancelled", "expected cancellation error for workflow %s", wfID)
				}
			}
		}
	})
}

func gcTestStep(_ context.Context, x int) (int, error) {
	return x, nil
}

func gcTestWorkflow(dbosCtx Context, x int) (int, error) {
	result, err := RunAsStep(dbosCtx, func(ctx context.Context) (int, error) {
		return gcTestStep(ctx, x)
	})
	if err != nil {
		return 0, err
	}
	return result, nil
}

func gcBlockedWorkflow(dbosCtx Context, event *Event) (string, error) {
	event.Wait()
	workflowID, err := GetWorkflowID(dbosCtx)
	if err != nil {
		return "", err
	}
	return workflowID, nil
}

func TestGarbageCollect(t *testing.T) {
	t.Run("GarbageCollectWithOffset", func(t *testing.T) {
		// Start with clean database for precise workflow counting
		databaseURL := backendDatabaseURL(t)
		resetTestDatabase(t, databaseURL)
		dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: false, checkLeaks: true})
		gcTestEvent := NewEvent()

		// Ensure the event is set at the end to unblock any remaining workflows
		t.Cleanup(func() {
			gcTestEvent.Set()
		})

		RegisterWorkflow(dbosCtx, gcTestWorkflow)
		RegisterWorkflow(dbosCtx, gcBlockedWorkflow)

		require.NoError(t, Launch(dbosCtx), "failed to launch DBOS")

		gcTestEvent.Clear()
		numWorkflows := 10

		// Start one blocked workflow and 10 normal workflows
		blockedHandle, err := RunWorkflow(dbosCtx, gcBlockedWorkflow, gcTestEvent)
		require.NoError(t, err, "failed to start blocked workflow")

		var completedHandles []WorkflowHandle[int]
		for i := range numWorkflows {
			handle, err := RunWorkflow(dbosCtx, gcTestWorkflow, i)
			require.NoError(t, err, "failed to start test workflow %d", i)
			result, err := handle.GetResult()
			require.NoError(t, err, "failed to get result from test workflow %d", i)
			require.Equal(t, i, result, "expected result %d, got %d", i, result)
			completedHandles = append(completedHandles, handle)
		}

		// Verify exactly 11 workflows exist before GC (1 blocked + 10 completed)
		workflows, err := ListWorkflows(dbosCtx)
		require.NoError(t, err, "failed to list workflows")
		require.Equal(t, numWorkflows+1, len(workflows), "expected exactly %d workflows before GC", numWorkflows+1)

		// Garbage collect keeping only the 5 newest workflows
		// The blocked workflow won't be deleted because it's pending
		threshold := 5
		err = dbosCtx.(*dbosContext).systemDB.GarbageCollectWorkflows(dbosCtx, sysdb.GarbageCollectWorkflowsInput{
			RowsThreshold: &threshold,
		})
		require.NoError(t, err, "failed to garbage collect workflows")

		// Verify workflows after GC - should have 6 workflows:
		// - 5 newest workflows (by creation time cutoff determined by threshold)
		// - 1 blocked workflow (preserved because it's pending)
		workflows, err = ListWorkflows(dbosCtx)
		require.NoError(t, err, "failed to list workflows after GC")
		require.Equal(t, 6, len(workflows), "expected exactly 6 workflows after GC (5 from threshold + 1 pending)")

		// Create a map of remaining workflow IDs for easy lookup
		remainingIDs := make(map[string]bool)
		for _, wf := range workflows {
			remainingIDs[wf.ID] = true
		}

		// Verify blocked workflow still exists (since it's pending)
		require.True(t, remainingIDs[blockedHandle.GetWorkflowID()], "blocked workflow should still exist after GC")

		// Find status of blocked workflow
		for _, wf := range workflows {
			if wf.ID == blockedHandle.GetWorkflowID() {
				require.Equal(t, WorkflowStatusPending, wf.Status, "blocked workflow should still be pending")
				break
			}
		}

		// Verify that the 5 newest completed workflows are preserved
		// The completedHandles slice is in order of creation (0 is oldest, 9 is newest)
		// So indices 5-9 (the last 5) should be preserved
		for i := range numWorkflows {
			wfID := completedHandles[i].GetWorkflowID()
			if i < numWorkflows-threshold {
				// Older workflows (indices 0-4) should be deleted
				require.False(t, remainingIDs[wfID], "older workflow at index %d (ID: %s) should have been deleted", i, wfID)
			} else {
				// Newer workflows (indices 5-9) should be preserved
				require.True(t, remainingIDs[wfID], "newer workflow at index %d (ID: %s) should have been preserved", i, wfID)
			}
		}

		// Complete the blocked workflow
		gcTestEvent.Set()
		result, err := blockedHandle.GetResult()
		require.NoError(t, err, "failed to get result from blocked workflow")
		require.Equal(t, blockedHandle.GetWorkflowID(), result, "expected blocked workflow to return its ID")
	})

	t.Run("GarbageCollectWithCutoffTime", func(t *testing.T) {
		// Start with clean database for precise workflow counting
		databaseURL := backendDatabaseURL(t)
		resetTestDatabase(t, databaseURL)
		dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: false, checkLeaks: true})
		gcTestEvent := NewEvent()

		// Ensure the event is set at the end to unblock any remaining workflows
		t.Cleanup(func() {
			gcTestEvent.Set()
		})

		RegisterWorkflow(dbosCtx, gcTestWorkflow)
		RegisterWorkflow(dbosCtx, gcBlockedWorkflow)

		require.NoError(t, Launch(dbosCtx), "failed to launch DBOS")

		gcTestEvent.Clear()
		numWorkflows := 10

		// Start blocked workflow BEFORE cutoff to verify pending workflows are preserved
		blockedHandle, err := RunWorkflow(dbosCtx, gcBlockedWorkflow, gcTestEvent)
		require.NoError(t, err, "failed to start blocked workflow")

		// Execute first batch of workflows (before cutoff)
		var beforeCutoffHandles []WorkflowHandle[int]
		for i := range numWorkflows {
			handle, err := RunWorkflow(dbosCtx, gcTestWorkflow, i)
			require.NoError(t, err, "failed to start test workflow %d", i)
			result, err := handle.GetResult()
			require.NoError(t, err, "failed to get result from test workflow %d", i)
			require.Equal(t, i, result, "expected result %d, got %d", i, result)
			beforeCutoffHandles = append(beforeCutoffHandles, handle)
		}

		// Wait to ensure clear time separation between batches
		time.Sleep(500 * time.Millisecond)
		cutoffTime := time.Now()
		// Additional small delay to ensure cutoff is after all first batch workflows
		time.Sleep(100 * time.Millisecond)

		// Execute second batch of workflows after cutoff
		var afterCutoffHandles []WorkflowHandle[int]
		for i := numWorkflows; i < numWorkflows*2; i++ {
			handle, err := RunWorkflow(dbosCtx, gcTestWorkflow, i)
			require.NoError(t, err, "failed to start test workflow %d", i)
			result, err := handle.GetResult()
			require.NoError(t, err, "failed to get result from test workflow %d", i)
			require.Equal(t, i, result, "expected result %d, got %d", i, result)
			afterCutoffHandles = append(afterCutoffHandles, handle)
		}

		// Verify exactly 21 workflows exist before GC (1 blocked + 10 old + 10 new)
		workflows, err := ListWorkflows(dbosCtx)
		require.NoError(t, err, "failed to list workflows")
		require.Equal(t, 21, len(workflows), "expected exactly 21 workflows before GC (1 blocked + 10 old + 10 new)")

		// Garbage collect workflows completed before cutoff time
		cutoffTimestamp := cutoffTime.UnixMilli()
		err = dbosCtx.(*dbosContext).systemDB.GarbageCollectWorkflows(dbosCtx, sysdb.GarbageCollectWorkflowsInput{
			CutoffEpochTimestampMs: &cutoffTimestamp,
		})
		require.NoError(t, err, "failed to garbage collect workflows by time")

		// Verify exactly 11 workflows remain after GC (1 blocked + 10 new completed)
		workflows, err = ListWorkflows(dbosCtx)
		require.NoError(t, err, "failed to list workflows after time-based GC")
		require.Equal(t, 11, len(workflows), "expected exactly 11 workflows after time-based GC (1 blocked + 10 new)")

		// Create a map of remaining workflow IDs for easy lookup
		remainingIDs := make(map[string]bool)
		for _, wf := range workflows {
			remainingIDs[wf.ID] = true
		}

		// Verify blocked workflow still exists (even though it was created before cutoff)
		require.True(t, remainingIDs[blockedHandle.GetWorkflowID()], "blocked workflow should still exist after GC")

		// Verify that all workflows created before cutoff were deleted (except the blocked one)
		for _, handle := range beforeCutoffHandles {
			wfID := handle.GetWorkflowID()
			require.False(t, remainingIDs[wfID], "workflow created before cutoff (ID: %s) should have been deleted", wfID)
		}

		// Verify that all workflows created after cutoff were preserved
		for _, handle := range afterCutoffHandles {
			wfID := handle.GetWorkflowID()
			require.True(t, remainingIDs[wfID], "workflow created after cutoff (ID: %s) should have been preserved", wfID)
		}

		// Complete the blocked workflow
		gcTestEvent.Set()
		result, err := blockedHandle.GetResult()
		require.NoError(t, err, "failed to get result from blocked workflow")
		require.Equal(t, blockedHandle.GetWorkflowID(), result, "expected blocked workflow to return its ID")

		// Wait a moment to ensure the completed workflow timestamp is after creation
		time.Sleep(100 * time.Millisecond)

		// Garbage collect all workflows - use a future cutoff to catch everything
		futureTimestamp := time.Now().Add(1 * time.Hour).UnixMilli()
		err = dbosCtx.(*dbosContext).systemDB.GarbageCollectWorkflows(dbosCtx, sysdb.GarbageCollectWorkflowsInput{
			CutoffEpochTimestampMs: &futureTimestamp,
		})
		require.NoError(t, err, "failed to garbage collect all completed workflows")

		// Verify exactly 0 workflows remain
		workflows, err = ListWorkflows(dbosCtx)
		require.NoError(t, err, "failed to list workflows after final GC")
		require.Equal(t, 0, len(workflows), "expected exactly 0 workflows after final GC")
	})

	t.Run("GarbageCollectEmptyDatabase", func(t *testing.T) {
		// Start with clean database for precise workflow counting
		databaseURL := backendDatabaseURL(t)
		resetTestDatabase(t, databaseURL)
		dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: false, checkLeaks: true})

		RegisterWorkflow(dbosCtx, gcTestWorkflow)
		RegisterWorkflow(dbosCtx, gcBlockedWorkflow)

		require.NoError(t, Launch(dbosCtx), "failed to launch DBOS")

		// Verify exactly 0 workflows exist initially
		workflows, err := ListWorkflows(dbosCtx)
		require.NoError(t, err, "failed to list workflows")
		require.Equal(t, 0, len(workflows), "expected exactly 0 workflows in empty database")

		// Verify GC runs without errors on a blank table
		threshold := 1
		err = dbosCtx.(*dbosContext).systemDB.GarbageCollectWorkflows(dbosCtx, sysdb.GarbageCollectWorkflowsInput{
			RowsThreshold: &threshold,
		})
		require.NoError(t, err, "garbage collect should work on empty database")

		// Verify still 0 workflows after row-based GC
		workflows, err = ListWorkflows(dbosCtx)
		require.NoError(t, err, "failed to list workflows after row-based GC")
		require.Equal(t, 0, len(workflows), "expected exactly 0 workflows after row-based GC on empty database")

		currentTimestamp := time.Now().UnixMilli()
		err = dbosCtx.(*dbosContext).systemDB.GarbageCollectWorkflows(dbosCtx, sysdb.GarbageCollectWorkflowsInput{
			CutoffEpochTimestampMs: &currentTimestamp,
		})
		require.NoError(t, err, "time-based garbage collect should work on empty database")

		// Verify still 0 workflows after time-based GC
		workflows, err = ListWorkflows(dbosCtx)
		require.NoError(t, err, "failed to list workflows after time-based GC")
		require.Equal(t, 0, len(workflows), "expected exactly 0 workflows after time-based GC on empty database")
	})

	t.Run("GarbageCollectOnlyCompletedWorkflows", func(t *testing.T) {
		// Start with clean database for precise workflow counting
		databaseURL := backendDatabaseURL(t)
		resetTestDatabase(t, databaseURL)
		dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: false, checkLeaks: true})
		gcTestEvent := NewEvent()

		// Ensure the event is set at the end to unblock any remaining workflows
		t.Cleanup(func() {
			gcTestEvent.Set()
		})

		RegisterWorkflow(dbosCtx, gcTestWorkflow)
		RegisterWorkflow(dbosCtx, gcBlockedWorkflow)

		require.NoError(t, Launch(dbosCtx), "failed to launch DBOS")

		gcTestEvent.Clear()
		numWorkflows := 5

		// Start blocked workflow that will remain pending
		blockedHandle, err := RunWorkflow(dbosCtx, gcBlockedWorkflow, gcTestEvent)
		require.NoError(t, err, "failed to start blocked workflow")

		// Execute normal workflows to completion
		for i := range numWorkflows {
			handle, err := RunWorkflow(dbosCtx, gcTestWorkflow, i)
			require.NoError(t, err, "failed to start test workflow %d", i)
			result, err := handle.GetResult()
			require.NoError(t, err, "failed to get result from test workflow %d", i)
			require.Equal(t, i, result, "expected result %d, got %d", i, result)
		}

		// Verify exactly 6 workflows exist (1 blocked + 5 completed)
		workflows, err := ListWorkflows(dbosCtx)
		require.NoError(t, err, "failed to list workflows")
		require.Equal(t, numWorkflows+1, len(workflows), "expected exactly %d workflows", numWorkflows+1)

		// Count pending vs completed workflows
		pendingCount := 0
		completedCount := 0
		for _, wf := range workflows {
			switch wf.Status {
			case WorkflowStatusPending:
				pendingCount++
			case WorkflowStatusSuccess:
				completedCount++
			}
		}
		require.Equal(t, 1, pendingCount, "expected exactly 1 pending workflow")
		require.Equal(t, numWorkflows, completedCount, "expected exactly %d completed workflows", numWorkflows)

		// GC keeping only the 1 newest workflow
		// The blocked workflow is the oldest but won't be deleted because it's pending
		// So we should have 2 workflows: 1 newest completed + 1 pending
		threshold := 1
		err = dbosCtx.(*dbosContext).systemDB.GarbageCollectWorkflows(dbosCtx, sysdb.GarbageCollectWorkflowsInput{
			RowsThreshold: &threshold,
		})
		require.NoError(t, err, "failed to garbage collect workflows")

		// Verify exactly 2 workflows remain (1 newest + 1 pending)
		workflows, err = ListWorkflows(dbosCtx)
		require.NoError(t, err, "failed to list workflows after GC")
		require.Equal(t, 2, len(workflows), "expected exactly 2 workflows after GC (1 newest + 1 pending)")

		// Verify pending workflow still exists
		found := false
		pendingCount = 0
		completedCount = 0
		for _, wf := range workflows {
			if wf.ID == blockedHandle.GetWorkflowID() {
				found = true
				require.Equal(t, WorkflowStatusPending, wf.Status, "blocked workflow should still be pending")
			}
			switch wf.Status {
			case WorkflowStatusPending:
				pendingCount++
			case WorkflowStatusSuccess:
				completedCount++
			}
		}
		require.True(t, found, "pending workflow should remain")
		require.Equal(t, 1, pendingCount, "expected exactly 1 pending workflow after GC")
		require.Equal(t, 1, completedCount, "expected exactly 1 completed workflow after GC")

		// Complete the blocked workflow and verify GC works
		gcTestEvent.Set()
		result, err := blockedHandle.GetResult()
		require.NoError(t, err, "failed to get result from blocked workflow")
		require.Equal(t, blockedHandle.GetWorkflowID(), result, "expected blocked workflow to return its ID")

		// Wait a moment to ensure the completed workflow timestamp is after creation
		time.Sleep(100 * time.Millisecond)

		// Now GC everything using future timestamp
		futureTimestamp := time.Now().Add(1 * time.Hour).UnixMilli()
		err = dbosCtx.(*dbosContext).systemDB.GarbageCollectWorkflows(dbosCtx, sysdb.GarbageCollectWorkflowsInput{
			CutoffEpochTimestampMs: &futureTimestamp,
		})
		require.NoError(t, err, "failed to garbage collect all workflows")

		// Verify exactly 0 workflows remain
		workflows, err = ListWorkflows(dbosCtx)
		require.NoError(t, err, "failed to list workflows after final GC")
		require.Equal(t, 0, len(workflows), "expected exactly 0 workflows after final GC")
	})

	t.Run("ThresholdAndCutoffTimestampInteraction", func(t *testing.T) {
		// Reset database for clean test environment
		databaseURL := backendDatabaseURL(t)
		resetTestDatabase(t, databaseURL)
		dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: false, checkLeaks: true})

		// Register the test workflow
		RegisterWorkflow(dbosCtx, gcTestWorkflow)

		require.NoError(t, Launch(dbosCtx), "failed to launch DBOS")

		// This test verifies that when both threshold and cutoff timestamp are provided,
		// the more stringent (restrictive) one applies - i.e., the one that keeps more workflows

		// Create 10 workflows with different timestamps
		numWorkflows := 10
		handles := make([]WorkflowHandle[int], numWorkflows)

		for i := range numWorkflows {
			handle, err := RunWorkflow(dbosCtx, gcTestWorkflow, i)
			require.NoError(t, err, "failed to start workflow %d", i)
			handles[i] = handle

			// Add small delay to ensure distinct timestamps
			time.Sleep(10 * time.Millisecond)
		}

		// Wait for all workflows to complete
		for i, handle := range handles {
			result, err := handle.GetResult()
			require.NoError(t, err, "failed to get result from workflow %d", i)
			require.Equal(t, i, result)
		}

		// Get timestamps for testing
		workflows, err := ListWorkflows(dbosCtx, WithFilterSortDesc())
		require.NoError(t, err, "failed to list workflows")
		require.Equal(t, numWorkflows, len(workflows))

		// Workflows are ordered newest first in ListWorkflows
		var cutoff1 int64 // Will keep 5 newest when used as cutoff
		var cutoff2 int64 // Will keep 8 newest when used as cutoff

		cutoff1 = workflows[7].CreatedAt.UnixMilli() // 3rd oldest workflow
		cutoff2 = workflows[1].CreatedAt.UnixMilli() // 9th oldest workflow

		// Case 1: Threshold is more restrictive (higher/more recent cutoff)
		// Threshold would keep 6 newest, timestamp would keep 8 newest
		// Result: threshold wins (higher timestamp), only 6 workflows remain
		threshold := 6
		err = dbosCtx.(*dbosContext).systemDB.GarbageCollectWorkflows(dbosCtx, sysdb.GarbageCollectWorkflowsInput{
			RowsThreshold:          &threshold,
			CutoffEpochTimestampMs: &cutoff1,
		})
		require.NoError(t, err, "failed to garbage collect with threshold 6 and 7th newest timestamp")

		workflows, err = ListWorkflows(dbosCtx, WithFilterSortDesc())
		require.NoError(t, err, "failed to list workflows after first GC")
		require.Equal(t, threshold, len(workflows), "expected 6 workflows when threshold has more recent cutoff than timestamp")

		for i := 0; i < len(workflows)-threshold; i++ {
			require.Equal(t, workflows[i].ID, handles[i].GetWorkflowID(), "expected workflow %d to remain", i)
		}

		// Case2: Threshold is less restrictive (lower cutoff)
		threshold = 3
		err = dbosCtx.(*dbosContext).systemDB.GarbageCollectWorkflows(dbosCtx, sysdb.GarbageCollectWorkflowsInput{
			RowsThreshold:          &threshold,
			CutoffEpochTimestampMs: &cutoff2,
		})
		require.NoError(t, err, "failed to garbage collect with threshold 3 and 2nd newest timestamp")

		workflows, err = ListWorkflows(dbosCtx, WithFilterSortDesc())
		require.NoError(t, err, "failed to list workflows after second GC")
		require.Equal(t, 2, len(workflows), "expected 2 workflows after second GC")
		require.Equal(t, workflows[0].ID, handles[numWorkflows-1].GetWorkflowID(), "expected newest workflow to remain")
		require.Equal(t, workflows[1].ID, handles[numWorkflows-2].GetWorkflowID(), "expected 2nd newest workflow to remain")
	})
}

// TestSpecialSteps tests that special workflow functions (ListWorkflows, CancelWorkflow,
// CancelWorkflows, ResumeWorkflow, ForkWorkflow, GetWorkflowSteps, WriteStream,
// SetWorkflowDelay, DeleteWorkflows) work correctly as durable steps
func TestSpecialSteps(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	childEvent := NewEvent()
	child2Event := NewEvent()

	// Child workflow that blocks on an event (for cancellation testing)
	childWorkflow := func(dbosCtx Context, input string) (string, error) {
		// Wait for event to be set (will be cancelled before this happens)
		childEvent.Wait()
		return fmt.Sprintf("auxiliary-result-%s", input), nil
	}

	// Second child workflow that blocks on its own event (for bulk cancel/delete testing)
	child2Workflow := func(dbosCtx Context, input string) (string, error) {
		child2Event.Wait()
		return fmt.Sprintf("auxiliary-result-2-%s", input), nil
	}

	// Workflow enqueued with a delay (for SetWorkflowDelay testing)
	delayedWorkflow := func(dbosCtx Context, input string) (string, error) {
		return input, nil
	}

	specialStepsQueue, err := RegisterQueue(dbosCtx, "special-steps-queue")
	require.NoError(t, err, "failed to register queue")

	// Main workflow that uses all special steps
	specialStepsWorkflow := func(dbosCtx Context, input string) (string, error) {
		currentWorkflowID, err := GetWorkflowID(dbosCtx)
		if err != nil {
			return "", fmt.Errorf("failed to get current workflow ID: %w", err)
		}

		// Step 0: Start a child workflow to use in other operations
		childHandle, err := RunWorkflow(dbosCtx, childWorkflow, "test")
		if err != nil {
			return "", fmt.Errorf("failed to start child workflow: %w", err)
		}

		// Step 1: Use CancelWorkflow on the child workflow (should be cancelled while waiting)
		err = CancelWorkflow(dbosCtx, childHandle.GetWorkflowID())
		if err != nil {
			return "", fmt.Errorf("CancelWorkflow failed: %w", err)
		}

		// Step 2: Use RetrieveWorkflow (list workflows under the hood)
		retrievedHandle, err := RetrieveWorkflow[string](dbosCtx, childHandle.GetWorkflowID())
		if err != nil {
			return "", fmt.Errorf("RetrieveWorkflow failed: %w", err)
		}
		if retrievedHandle.GetWorkflowID() != childHandle.GetWorkflowID() {
			return "", fmt.Errorf("RetrieveWorkflow returned wrong workflow ID")
		}

		// Step 3: Check status of cancelled workflow (calls listWorkflows under the hood)
		status, err := retrievedHandle.GetStatus()
		if err != nil {
			return "", fmt.Errorf("failed to get status of retrieved workflow: %w", err)
		}
		if status.Status != WorkflowStatusCancelled {
			return "", fmt.Errorf("expected cancelled workflow status, got %v", status.Status)
		}

		// Step 4: resume the cancelled workflow
		resumeHandle, err := ResumeWorkflow[string](dbosCtx, childHandle.GetWorkflowID())
		if err != nil {
			return "", fmt.Errorf("ResumeWorkflow failed: %w", err)
		}
		if resumeHandle.GetWorkflowID() != childHandle.GetWorkflowID() {
			return "", fmt.Errorf("ResumeWorkflow returned wrong workflow ID")
		}

		// Step 5: Use ForkWorkflow
		forkHandle, err := ForkWorkflow[string](dbosCtx, ForkWorkflowInput{OriginalWorkflowID: currentWorkflowID})
		if err != nil {
			return "", fmt.Errorf("ForkWorkflow failed: %w", err)
		}
		if forkHandle.GetWorkflowID() == "" {
			return "", fmt.Errorf("ForkWorkflow returned empty workflow ID")
		}

		// Step 6: Use GetWorkflowSteps on current workflow
		steps, err := GetWorkflowSteps(dbosCtx, currentWorkflowID)
		if err != nil {
			return "", fmt.Errorf("GetWorkflowSteps failed: %w", err)
		}
		if len(steps) != 6 {
			t.Logf("Expected 6 steps so far, got %d", len(steps))
			for step := range steps {
				t.Logf("Step %d: %s (Error: %v)\n", steps[step].StepID, steps[step].StepName, steps[step].Error)
			}
			return "", fmt.Errorf("Expected 6 steps so far, got %d", len(steps))
		}

		// Step 7: Use ListWorkflows at the end to check expected count
		workflows, err := ListWorkflows(dbosCtx, WithFilterLimit(100))
		if err != nil {
			return "", fmt.Errorf("ListWorkflows failed: %w", err)
		}
		// We should have at least 3 workflows: main, child, and forked
		foundMain := false
		foundChild := false
		foundForked := false
		for _, wf := range workflows {
			if wf.ID == currentWorkflowID {
				foundMain = true
			}
			if wf.ID == childHandle.GetWorkflowID() {
				foundChild = true
			}
			if wf.ID == forkHandle.GetWorkflowID() {
				foundForked = true
			}
		}
		if !foundMain || !foundChild || !foundForked {
			return "", fmt.Errorf("ListWorkflows did not return expected workflows. Found main: %v, child: %v, forked: %v", foundMain, foundChild, foundForked)
		}

		// Step 8: Use WriteStream
		if err := WriteStream(dbosCtx, "special-steps-stream", "stream-value"); err != nil {
			return "", fmt.Errorf("WriteStream failed: %w", err)
		}

		// Step 9: Start a second child workflow to use for bulk cancel/delete
		child2Handle, err := RunWorkflow(dbosCtx, child2Workflow, "test-2")
		if err != nil {
			return "", fmt.Errorf("failed to start second child workflow: %w", err)
		}

		// Step 10: Use CancelWorkflows (bulk) on the second child workflow
		if err := CancelWorkflows(dbosCtx, []string{child2Handle.GetWorkflowID()}); err != nil {
			return "", fmt.Errorf("CancelWorkflows failed: %w", err)
		}

		// Step 11: Use DeleteWorkflows on the (now cancelled) second child workflow
		if err := DeleteWorkflows(dbosCtx, []string{child2Handle.GetWorkflowID()}); err != nil {
			return "", fmt.Errorf("DeleteWorkflows failed: %w", err)
		}

		// Step 12: Enqueue a delayed workflow to use for SetWorkflowDelay
		delayedHandle, err := RunWorkflow(dbosCtx, delayedWorkflow, "delayed-input", WithQueue(specialStepsQueue), WithDelay(600*time.Second))
		if err != nil {
			return "", fmt.Errorf("failed to enqueue delayed workflow: %w", err)
		}

		// Step 13: Use SetWorkflowDelay to shorten the delay
		if err := SetWorkflowDelay(dbosCtx, delayedHandle.GetWorkflowID(), WithDelayDuration(300*time.Second)); err != nil {
			return "", fmt.Errorf("SetWorkflowDelay failed: %w", err)
		}

		// Unblock the children
		childEvent.Set()
		child2Event.Set()

		return "success", nil
	}

	RegisterWorkflow(dbosCtx, childWorkflow, WithWorkflowName("child-workflow"))
	RegisterWorkflow(dbosCtx, child2Workflow, WithWorkflowName("child-workflow-2"))
	RegisterWorkflow(dbosCtx, delayedWorkflow, WithWorkflowName("delayed-workflow"))
	RegisterWorkflow(dbosCtx, specialStepsWorkflow)
	require.NoError(t, Launch(dbosCtx), "failed to launch DBOS")

	t.Run("SpecialStepsExecution", func(t *testing.T) {
		workflowID := uuid.NewString()
		handle, err := RunWorkflow(dbosCtx, specialStepsWorkflow, "test-input", WithWorkflowID(workflowID))
		require.NoError(t, err, "failed to start special steps workflow")

		// Wait for the workflow to complete
		result, err := handle.GetResult()
		require.NoError(t, err, "workflow should complete successfully")
		require.Equal(t, "success", result, "workflow should return success")

		// Flip status and trigger recovery
		setWorkflowStatusPending(t, dbosCtx, handle.GetWorkflowID())
		recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "failed to recover pending workflows")

		var recoveredHandle WorkflowHandle[any]
		for _, h := range recoveredHandles {
			if h.GetWorkflowID() == workflowID {
				recoveredHandle = h
				break
			}
		}
		require.NotNil(t, recoveredHandle, "workflow should be recovered")

		// Check the result is the same
		recoveredResult, err := recoveredHandle.GetResult()
		require.NoError(t, err, "recovered workflow should complete successfully")
		require.Equal(t, "success", recoveredResult, "recovered workflow should return same result")

		// Check the steps are as expected
		steps, err := GetWorkflowSteps(dbosCtx, workflowID)
		require.NoError(t, err, "failed to get workflow steps")
		require.Len(t, steps, 14, "expected 14 steps")
		require.Equal(t, "child-workflow", steps[0].StepName, "first step should be child-workflow")
		require.Equal(t, "DBOS.cancelWorkflow", steps[1].StepName, "second step should be DBOS.cancelWorkflow")
		require.Equal(t, "DBOS.retrieveWorkflow", steps[2].StepName, "third step should be DBOS.retrieveWorkflow")
		require.Equal(t, "DBOS.getStatus", steps[3].StepName, "fourth step should be DBOS.getStatus")
		require.Equal(t, "DBOS.resumeWorkflow", steps[4].StepName, "fifth step should be DBOS.resumeWorkflow")
		require.Equal(t, "DBOS.forkWorkflow", steps[5].StepName, "sixth step should be DBOS.forkWorkflow")
		require.Equal(t, "DBOS.getWorkflowSteps", steps[6].StepName, "seventh step should be DBOS.getWorkflowSteps")
		require.Equal(t, "DBOS.listWorkflows", steps[7].StepName, "eighth step should be DBOS.listWorkflows")
		require.Equal(t, "DBOS.writeStream", steps[8].StepName, "ninth step should be DBOS.writeStream")
		require.Equal(t, "child-workflow-2", steps[9].StepName, "tenth step should be child-workflow-2")
		require.Equal(t, "DBOS.cancelWorkflows", steps[10].StepName, "eleventh step should be DBOS.cancelWorkflows")
		require.Equal(t, "DBOS.deleteWorkflows", steps[11].StepName, "twelfth step should be DBOS.deleteWorkflows")
		require.Equal(t, "delayed-workflow", steps[12].StepName, "thirteenth step should be delayed-workflow")
		require.Equal(t, "DBOS.setWorkflowDelay", steps[13].StepName, "fourteenth step should be DBOS.setWorkflowDelay")
	})
}

func TestRegisteredWorkflowListing(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	// Register some regular workflows
	RegisterWorkflow(dbosCtx, simpleWorkflow)
	RegisterWorkflow(dbosCtx, simpleWorkflowError, WithMaxRecoveryAttempts(5))
	RegisterWorkflow(dbosCtx, simpleWorkflowWithStep, WithWorkflowName("CustomStepWorkflow"))

	err := Launch(dbosCtx)
	require.NoError(t, err, "failed to launch DBOS")

	t.Run("ListRegisteredWorkflows", func(t *testing.T) {
		workflows := ListRegisteredWorkflows(dbosCtx)

		// Should have 3 registered workflows
		require.GreaterOrEqual(t, len(workflows), 3, "Should have 3 registered workflows")

		// Create a map for easier lookup
		workflowMap := make(map[string]WorkflowRegistryEntry)
		for _, wf := range workflows {
			workflowMap[wf.FQN] = wf
		}

		// Check that simpleWorkflow is registered
		simpleWorkflowFQN := runtime.FuncForPC(reflect.ValueOf(simpleWorkflow).Pointer()).Name()
		simpleWf, exists := workflowMap[simpleWorkflowFQN]
		require.True(t, exists, "simpleWorkflow should be registered")
		require.Equal(t, _DEFAULT_MAX_RECOVERY_ATTEMPTS, simpleWf.MaxRetries, "simpleWorkflow should have default max retries")

		// Check that simpleWorkflowError is registered with custom max retries
		simpleWorkflowErrorFQN := runtime.FuncForPC(reflect.ValueOf(simpleWorkflowError).Pointer()).Name()
		errorWf, exists := workflowMap[simpleWorkflowErrorFQN]
		require.True(t, exists, "simpleWorkflowError should be registered")
		require.Equal(t, 5, errorWf.MaxRetries, "simpleWorkflowError should have custom max retries")

		// Check that custom named workflow is registered
		customStepWorkflowFQN := runtime.FuncForPC(reflect.ValueOf(simpleWorkflowWithStep).Pointer()).Name()
		customWf, exists := workflowMap[customStepWorkflowFQN]
		require.True(t, exists, "CustomStepWorkflow should be found")
		require.Equal(t, "CustomStepWorkflow", customWf.Name, "CustomStepWorkflow should have the correct name")
	})
}

func TestWorkflowIdentity(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	RegisterWorkflow(dbosCtx, simpleWorkflow)
	require.NoError(t, Launch(dbosCtx), "failed to launch DBOS")
	handle, err := RunWorkflow(
		dbosCtx,
		simpleWorkflow,
		"test",
		WithWorkflowID("my-workflow-id"),
		WithAuthenticatedUser("user123"),
		WithAssumedRole("admin"),
		WithAuthenticatedRoles("reader", "writer"))
	require.NoError(t, err, "failed to start workflow")

	// Retrieve the workflow's status.
	status, err := handle.GetStatus()
	require.NoError(t, err)

	t.Run("CheckAuthenticatedUser", func(t *testing.T) {
		assert.Equal(t, "user123", status.AuthenticatedUser)
	})

	t.Run("CheckAssumedRole", func(t *testing.T) {
		assert.Equal(t, "admin", status.AssumedRole)
	})

	t.Run("CheckAuthenticatedRoles", func(t *testing.T) {
		assert.Equal(t, []string{"reader", "writer"}, status.AuthenticatedRoles)
	})
}

// authSnapshot holds identity fields read from a workflow's DB row.
type authSnapshot struct {
	User  string
	Role  string
	Roles []string
}

// captureAuthFromDB reads the calling workflow's own identity from workflow_status.
func captureAuthFromDB(ctx Context) (authSnapshot, error) {
	wfID, err := GetWorkflowID(ctx)
	if err != nil {
		return authSnapshot{}, err
	}
	rows, err := ctx.(*dbosContext).systemDB.ListWorkflows(ctx, sysdb.ListWorkflowsDBInput{
		WorkflowIDs: []string{wfID},
	})
	if err != nil || len(rows) == 0 {
		return authSnapshot{}, err
	}
	return authSnapshot{
		User:  rows[0].AuthenticatedUser,
		Role:  rows[0].AssumedRole,
		Roles: rows[0].AuthenticatedRoles,
	}, nil
}

// authChildWorkflow returns its own auth snapshot as output.
func authChildWorkflow(ctx Context, _ string) (authSnapshot, error) {
	return captureAuthFromDB(ctx)
}

// authParentWorkflow spawns authChildWorkflow without passing any auth opts.
// Propagation from workflowState should carry the parent's identity.
func authParentWorkflow(ctx Context, _ string) (authSnapshot, error) {
	handle, err := RunWorkflow(ctx, authChildWorkflow, "")
	if err != nil {
		return authSnapshot{}, err
	}
	return handle.GetResult()
}

// authGrandparentWorkflow tests three-level propagation.
func authGrandparentWorkflow(ctx Context, _ string) (authSnapshot, error) {
	handle, err := RunWorkflow(ctx, authParentWorkflow, "")
	if err != nil {
		return authSnapshot{}, err
	}
	return handle.GetResult()
}

// authParentWithOverrideWorkflow spawns a child that explicitly sets different auth.
func authParentWithOverrideWorkflow(ctx Context, _ string) (authSnapshot, error) {
	handle, err := RunWorkflow(ctx, authChildWorkflow, "",
		WithAuthenticatedUser("service-account"),
		WithAssumedRole("service"),
		WithAuthenticatedRoles("internal"),
	)
	if err != nil {
		return authSnapshot{}, err
	}
	return handle.GetResult()
}

func TestAuthPropagation(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	RegisterWorkflow(dbosCtx, authChildWorkflow)
	RegisterWorkflow(dbosCtx, authParentWorkflow)
	RegisterWorkflow(dbosCtx, authGrandparentWorkflow)
	RegisterWorkflow(dbosCtx, authParentWithOverrideWorkflow)
	require.NoError(t, Launch(dbosCtx), "failed to launch DBOS")

	t.Run("PropagatesFromParentToChild", func(t *testing.T) {
		handle, err := RunWorkflow(dbosCtx, authParentWorkflow, "",
			WithAuthenticatedUser("alice@example.com"),
			WithAssumedRole("customer"),
			WithAuthenticatedRoles("read", "write"),
		)
		require.NoError(t, err)
		childAuth, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "alice@example.com", childAuth.User)
		assert.Equal(t, "customer", childAuth.Role)
		assert.Equal(t, []string{"read", "write"}, childAuth.Roles)
	})

	t.Run("ChildExplicitOverridesParent", func(t *testing.T) {
		handle, err := RunWorkflow(dbosCtx, authParentWithOverrideWorkflow, "",
			WithAuthenticatedUser("alice@example.com"),
			WithAssumedRole("customer"),
			WithAuthenticatedRoles("read", "write"),
		)
		require.NoError(t, err)
		childAuth, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "service-account", childAuth.User)
		assert.Equal(t, "service", childAuth.Role)
		assert.Equal(t, []string{"internal"}, childAuth.Roles)
	})

	t.Run("EmptyParentDoesNotPropagateNoise", func(t *testing.T) {
		handle, err := RunWorkflow(dbosCtx, authParentWorkflow, "")
		require.NoError(t, err)
		childAuth, err := handle.GetResult()
		require.NoError(t, err)
		assert.Empty(t, childAuth.User)
		assert.Empty(t, childAuth.Role)
		assert.Empty(t, childAuth.Roles)
	})

	t.Run("PropagatesMultipleLevels", func(t *testing.T) {
		handle, err := RunWorkflow(dbosCtx, authGrandparentWorkflow, "",
			WithAuthenticatedUser("alice@example.com"),
			WithAssumedRole("customer"),
			WithAuthenticatedRoles("read", "write"),
		)
		require.NoError(t, err)
		grandchildAuth, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "alice@example.com", grandchildAuth.User)
		assert.Equal(t, "customer", grandchildAuth.Role)
		assert.Equal(t, []string{"read", "write"}, grandchildAuth.Roles)
	})

	t.Run("PropagatesAfterRecovery", func(t *testing.T) {
		const wfID = "auth-recovery-test-wf"

		handle, err := RunWorkflow(dbosCtx, authParentWorkflow, "",
			WithWorkflowID(wfID),
			WithAuthenticatedUser("alice@example.com"),
			WithAssumedRole("customer"),
			WithAuthenticatedRoles("read", "write"),
		)
		require.NoError(t, err)
		_, err = handle.GetResult()
		require.NoError(t, err)

		// Simulate crash: reset parent to PENDING so recovery re-runs it.
		setWorkflowStatusPending(t, dbosCtx, wfID)

		recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err)
		require.Len(t, recoveredHandles, 1)

		// Use a typed handle so the JSON output decodes into authSnapshot.
		typedHandle, err := RetrieveWorkflow[authSnapshot](dbosCtx, wfID)
		require.NoError(t, err)
		childAuth, err := typedHandle.GetResult()
		require.NoError(t, err)

		assert.Equal(t, "alice@example.com", childAuth.User)
		assert.Equal(t, "customer", childAuth.Role)
		assert.Equal(t, []string{"read", "write"}, childAuth.Roles)
	})
}

func TestWorkflowHandles(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	RegisterWorkflow(dbosCtx, slowWorkflow)
	require.NoError(t, Launch(dbosCtx), "failed to launch DBOS")

	workflowSleep := 1 * time.Second

	t.Run("WorkflowHandleTimeout", func(t *testing.T) {
		handle, err := RunWorkflow(dbosCtx, slowWorkflow, workflowSleep)
		require.NoError(t, err, "failed to start workflow")

		start := time.Now()
		_, err = handle.GetResult(WithHandleTimeout(10*time.Millisecond), WithHandlePollingInterval(1*time.Millisecond))
		duration := time.Since(start)

		require.Error(t, err, "expected timeout error")
		assert.Contains(t, err.Error(), "workflow result timeout")
		assert.True(t, duration < 100*time.Millisecond, "timeout should occur quickly")
		assert.True(t, errors.Is(err, ErrTimeout), "expected a DBOS timeout error, got: %v", err)
		assert.True(t, errors.Is(err, context.DeadlineExceeded),
			"expected error to be detectable as context.DeadlineExceeded, got: %v", err)
	})

	t.Run("WorkflowPollingHandleTimeout", func(t *testing.T) {
		// Start a workflow that will block on the first signal
		originalHandle, err := RunWorkflow(dbosCtx, slowWorkflow, workflowSleep)
		require.NoError(t, err, "failed to start workflow")

		pollingHandle, err := RetrieveWorkflow[string](dbosCtx, originalHandle.GetWorkflowID())
		require.NoError(t, err, "failed to retrieve workflow")

		_, ok := pollingHandle.(*workflowPollingHandle[string])
		require.True(t, ok, "expected polling handle, got %T", pollingHandle)

		start := time.Now()
		_, err = pollingHandle.GetResult(WithHandleTimeout(10*time.Millisecond), WithHandlePollingInterval(1*time.Millisecond))
		duration := time.Since(start)

		assert.True(t, duration < 100*time.Millisecond, "timeout should occur quickly")
		require.Error(t, err, "expected timeout error")
		assert.True(t, errors.Is(err, context.DeadlineExceeded),
			"expected error to be detectable as context.DeadlineExceeded, got: %v", err)
	})
}

func TestWorkflowHandleContextCancel(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	RegisterWorkflow(dbosCtx, getEventWorkflow)
	require.NoError(t, Launch(dbosCtx), "failed to launch DBOS")

	t.Run("WorkflowHandleContextCancel", func(t *testing.T) {
		getEventWorkflowStartedSignal.Clear()
		handle, err := RunWorkflow(dbosCtx, getEventWorkflow, getEventWorkflowInput{
			TargetWorkflowID: "test-workflow-id",
			Key:              "test-key",
		})
		require.NoError(t, err, "failed to start workflow")

		resultChan := make(chan error)
		go func() {
			_, err := handle.GetResult()
			resultChan <- err
		}()

		getEventWorkflowStartedSignal.Wait()
		getEventWorkflowStartedSignal.Clear()

		dbosCtx.Shutdown(dbosCtx, 1 * time.Second)

		err = <-resultChan
		require.Error(t, err, "expected error from cancelled context")
		assert.True(t, errors.Is(err, context.Canceled),
			"expected error to be detectable as context.Canceled, got: %v", err)
	})
}

// stubPatchCheckDB answers every DoesPatchExists call made through the
// SystemDatabase interface with a fixed result. Only DeprecatePatch reaches
// it that way — SysDB.Patch invokes its own method directly, so Patch keeps
// working when the embedded SystemDatabase is real.
type stubPatchCheckDB struct {
	sysdb.SystemDatabase
	name string
	err  error
}

func (f *stubPatchCheckDB) DoesPatchExists(context.Context, sysdb.PatchDBInput) (string, error) {
	return f.name, f.err
}

// TestDeprecatePatchStepID exercises DeprecatePatch's step-ID accounting
// directly: the step ID may only advance when the history lookup confirms
// the patch marker occupies the next slot. In particular a failed lookup
// must leave it untouched — guessing either way corrupts step numbering.
func TestDeprecatePatchStepID(t *testing.T) {
	run := func(db sysdb.SystemDatabase) (*workflowState, error) {
		wfState := &workflowState{workflowID: "wf-id", stepID: 3}
		c := &dbosContext{
			ctx:      context.WithValue(context.Background(), workflowStateKey, wfState),
			config:   &Config{EnablePatching: true},
			logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
			systemDB: db,
		}
		return wfState, c.DeprecatePatch(nil, "my-patch")
	}

	t.Run("MarkerPresentConsumesStepID", func(t *testing.T) {
		ws, err := run(&stubPatchCheckDB{name: _DBOS_PATCH_PREFIX + "my-patch"})
		require.NoError(t, err)
		require.Equal(t, 4, ws.stepID)
	})

	t.Run("NoRowLeavesStepIDUntouched", func(t *testing.T) {
		ws, err := run(&stubPatchCheckDB{err: sysdb.ErrNoRows})
		require.NoError(t, err)
		require.Equal(t, 3, ws.stepID)
	})

	t.Run("OtherMarkerLeavesStepIDUntouched", func(t *testing.T) {
		ws, err := run(&stubPatchCheckDB{name: _DBOS_PATCH_PREFIX + "other-patch"})
		require.NoError(t, err)
		require.Equal(t, 3, ws.stepID)
	})

	t.Run("LookupErrorPropagatesAndLeavesStepIDUntouched", func(t *testing.T) {
		injected := errors.New("simulated patch lookup failure")
		ws, err := run(&stubPatchCheckDB{err: injected})
		require.ErrorIs(t, err, injected)
		require.Equal(t, 3, ws.stepID)
	})
}

func TestPatching(t *testing.T) {
	t.Run("PatchingEnabled", func(t *testing.T) {
		// Create a DBOS context with patching enabled
		databaseURL := backendDatabaseURL(t)
		resetTestDatabase(t, databaseURL)
		dbosCtx, err := NewContext(context.Background(), Config{
			DatabaseURL:        databaseURL,
			AppName:            "test-app-patching-enabled",
			EnablePatching:     true,
			ApplicationVersion: "PATCHING_ENABLED",
		})
		require.NoError(t, err, "failed to create DBOS context with patching enabled")
		require.Equal(t, "PATCHING_ENABLED", dbosCtx.GetApplicationVersion(), "expected application version to be PATCHING_ENABLED")

		// Register cleanup
		t.Cleanup(func() {
			if dbosCtx != nil {
				Shutdown(dbosCtx, 30*time.Second)
			}
		})

		step := func(input int) (int, error) {
			return input + 1, nil
		}

		stepPatched := func(input int) (int, error) {
			return input + 2, nil
		}

		wf := func(ctx Context, input int) (int, error) {
			// step < step to patch
			RunAsStep(ctx, func(ctx context.Context) (int, error) {
				return step(input)
			}, WithStepName("firstStep"))
			// step to patch
			res, err := RunAsStep(ctx, func(ctx context.Context) (int, error) {
				return step(input)
			}, WithStepName("patch-step"))
			if err != nil {
				return 0, err
			}
			// step > step to patch
			RunAsStep(ctx, func(ctx context.Context) (int, error) {
				return step(input)
			}, WithStepName("lastStep"))
			return res, nil
		}

		RegisterWorkflow(dbosCtx, wf, WithWorkflowName("wf"))
		require.NoError(t, Launch(dbosCtx))

		handle, err := RunWorkflow(dbosCtx, wf, 1)
		require.NoError(t, err, "failed to start workflow")
		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result")
		require.Equal(t, 2, result, "expected result to be 2")

		wfPatched := func(ctx Context, input int) (int, error) {
			// step < step to patch
			RunAsStep(ctx, func(ctx context.Context) (int, error) {
				return step(input)
			}, WithStepName("firstStep"))

			// step to patch
			patched, err := Patch(ctx, "my-patch")
			if err != nil {
				return 0, err
			}
			var res int
			if patched {
				res, err = RunAsStep(ctx, func(ctx context.Context) (int, error) {
					return stepPatched(input)
				}, WithStepName("patched-step"))
				if err != nil {
					return 0, err
				}
			} else {
				res, err = RunAsStep(ctx, func(ctx context.Context) (int, error) {
					return step(input)
				}, WithStepName("patch-step"))
				if err != nil {
					return 0, err
				}
			}

			// step > step to patch
			RunAsStep(ctx, func(ctx context.Context) (int, error) {
				return step(input)
			}, WithStepName("lastStep"))

			return res, nil
		}

		// Clear the context registries and re-register the patched wf with the same name
		dbosCtx.(*dbosContext).launched.Store(false)
		clearRegistries(dbosCtx)
		RegisterWorkflow(dbosCtx, wfPatched, WithWorkflowName("wf"))
		dbosCtx.(*dbosContext).launched.Store(true)

		// new invocation takes the new code and has the patch step recorded
		patchedHandle, err := RunWorkflow(dbosCtx, wfPatched, 1)
		require.NoError(t, err, "failed to start workflow")
		result, err = patchedHandle.GetResult()
		require.NoError(t, err, "failed to get result")
		require.Equal(t, 3, result, "expected result to be 3")
		steps, err := GetWorkflowSteps(dbosCtx, patchedHandle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps")
		require.Equal(t, 4, len(steps), "expected 4 steps")
		require.Equal(t, "DBOS.patch-my-patch", steps[1].StepName, "expected step name to be DBOS.patch-my-patch")

		// Fork the workflow at different steps and verify behavior
		// Steps 0 and 1 should take the new code (patched), step 2 should take the old code
		for startStep := 0; startStep <= 2; startStep++ {
			forkHandle, err := ForkWorkflow[int](dbosCtx, ForkWorkflowInput{OriginalWorkflowID: handle.GetWorkflowID(), StartStep: uint(startStep)})
			require.NoError(t, err, "failed to fork workflow at step %d", startStep)
			result, err := forkHandle.GetResult()
			require.NoError(t, err, "failed to get result for fork at step %d", startStep)
			steps, err := GetWorkflowSteps(dbosCtx, forkHandle.GetWorkflowID())
			require.NoError(t, err, "failed to get workflow steps for fork at step %d", startStep)

			if startStep < 2 {
				// Forking before step 2 should take the new code
				require.Equal(t, 3, result, "expected result to be 3 when forking at step %d", startStep)
				require.Equal(t, 4, len(steps), "expected 4 steps when forking at step %d", startStep)
				require.Equal(t, "DBOS.patch-my-patch", steps[1].StepName, "expected step name to be DBOS.patch-my-patch when forking at step %d", startStep)
			} else {
				// Forking at step 2 should take the old code
				require.Equal(t, 2, result, "expected result to be 2 when forking at step %d", startStep)
				require.Equal(t, 3, len(steps), "expected 3 steps when forking at step %d", startStep)
			}
		}

		wfDeprecatePatch := func(ctx Context, input int) (int, error) {
			RunAsStep(ctx, func(ctx context.Context) (int, error) {
				return step(input)
			}, WithStepName("firstStep"))
			DeprecatePatch(ctx, "my-patch")
			res, err := RunAsStep(ctx, func(ctx context.Context) (int, error) {
				return stepPatched(input)
			}, WithStepName("patched-step"))
			if err != nil {
				return 0, err
			}
			RunAsStep(ctx, func(ctx context.Context) (int, error) {
				return step(input)
			}, WithStepName("lastStep"))
			return res, nil
		}

		// Clear the context registries and register the deprecated wf with the same name
		dbosCtx.(*dbosContext).launched.Store(false)
		clearRegistries(dbosCtx)
		RegisterWorkflow(dbosCtx, wfDeprecatePatch, WithWorkflowName("wf"))
		dbosCtx.(*dbosContext).launched.Store(true)

		// deprecated invocation skips the patch deprecation entirely
		deprecatedHandle, err := RunWorkflow(dbosCtx, wfDeprecatePatch, 1)
		require.NoError(t, err, "failed to start workflow")
		result, err = deprecatedHandle.GetResult()
		require.NoError(t, err, "failed to get result")
		require.Equal(t, 3, result, "expected result to be 3")
		steps, err = GetWorkflowSteps(dbosCtx, deprecatedHandle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps")
		require.Equal(t, 3, len(steps), "expected 3 steps")

		// Forking an old workflow (post-patch), at or after the patch step, on the new code should work without non-determinism errors
		// Because step 1 (the patch) is matched by DeprecatePatch in the new code
		for _, startStep := range []uint{2, 3} {
			forkHandle, err := ForkWorkflow[int](dbosCtx, ForkWorkflowInput{OriginalWorkflowID: patchedHandle.GetWorkflowID(), StartStep: startStep})
			require.NoError(t, err, "failed to fork workflow")
			result, err = forkHandle.GetResult()
			require.NoError(t, err, "failed to get result")
			require.Equal(t, 3, result, "expected result to be 3")
			steps, err = GetWorkflowSteps(dbosCtx, forkHandle.GetWorkflowID())
			require.NoError(t, err, "failed to get workflow steps")
			require.Equal(t, 4, len(steps), "expected 4 steps")
			require.Equal(t, "DBOS.patch-my-patch", steps[1].StepName, "expected step name to be DBOS.patch-my-patch")
		}

		// Forking an old workflow (pre-patch), after the patch step, on the new code will result in a non-determinism error, because the 2nd step name changed
		// Because the patch step now has a new name
		forkHandle, err := ForkWorkflow[int](dbosCtx, ForkWorkflowInput{OriginalWorkflowID: handle.GetWorkflowID(), StartStep: 2})
		require.NoError(t, err, "failed to fork workflow")
		_, err = forkHandle.GetResult()
		require.Error(t, err, "expected error when forking old workflow onto new workflow")
		require.Contains(t, err.Error(), fmt.Sprintf("DBOS Error %s", ErrorCodeUnexpectedStep))
	})

	// A DB error during DeprecatePatch's history check must surface, not be
	// swallowed. Swallowing it skips the patch marker's step ID, so every
	// subsequent step drifts one slot back into the marker's position and the
	// re-execution dies with a spurious ErrorCodeUnexpectedStep non-determinism error.
	t.Run("DeprecatePatchDBErrorPropagates", func(t *testing.T) {
		databaseURL := backendDatabaseURL(t)
		resetTestDatabase(t, databaseURL)
		dbosCtx, err := NewContext(context.Background(), Config{
			DatabaseURL:        databaseURL,
			AppName:            "test-app-deprecate-patch-db-error",
			EnablePatching:     true,
			ApplicationVersion: "PATCHING_ENABLED",
		})
		require.NoError(t, err, "failed to create DBOS context")
		t.Cleanup(func() {
			Shutdown(dbosCtx, 30*time.Second)
		})

		injectedErr := errors.New("simulated patch lookup failure")
		c := dbosCtx.(*dbosContext)
		c.systemDB = &stubPatchCheckDB{SystemDatabase: c.systemDB, err: injectedErr}

		step := func(input int) (int, error) {
			return input + 1, nil
		}

		wfPatched := func(ctx Context, input int) (int, error) {
			RunAsStep(ctx, func(ctx context.Context) (int, error) {
				return step(input)
			}, WithStepName("firstStep"))
			if _, err := Patch(ctx, "my-patch"); err != nil {
				return 0, err
			}
			return RunAsStep(ctx, func(ctx context.Context) (int, error) {
				return step(input)
			}, WithStepName("secondStep"))
		}

		RegisterWorkflow(dbosCtx, wfPatched, WithWorkflowName("wf"))
		require.NoError(t, Launch(dbosCtx))

		handle, err := RunWorkflow(dbosCtx, wfPatched, 1)
		require.NoError(t, err, "failed to start workflow")
		_, err = handle.GetResult()
		require.NoError(t, err, "failed to get result")
		// History: 0=firstStep, 1=DBOS.patch-my-patch, 2=secondStep

		wfDeprecated := func(ctx Context, input int) (int, error) {
			RunAsStep(ctx, func(ctx context.Context) (int, error) {
				return step(input)
			}, WithStepName("firstStep"))
			if err := DeprecatePatch(ctx, "my-patch"); err != nil {
				return 0, err
			}
			return RunAsStep(ctx, func(ctx context.Context) (int, error) {
				return step(input)
			}, WithStepName("secondStep"))
		}

		dbosCtx.(*dbosContext).launched.Store(false)
		clearRegistries(dbosCtx)
		RegisterWorkflow(dbosCtx, wfDeprecated, WithWorkflowName("wf"))
		dbosCtx.(*dbosContext).launched.Store(true)

		// Re-execute the patched history on the deprecated code: after
		// firstStep replays, DeprecatePatch checks the marker at step 1 and
		// the check fails with the injected error.
		forkHandle, err := ForkWorkflow[int](dbosCtx, ForkWorkflowInput{OriginalWorkflowID: handle.GetWorkflowID(), StartStep: 2})
		require.NoError(t, err, "failed to fork workflow")
		_, err = forkHandle.GetResult()
		require.Error(t, err, "expected the patch lookup error to surface")
		require.Contains(t, err.Error(), "simulated patch lookup failure")
		require.NotContains(t, err.Error(), fmt.Sprintf("DBOS Error %s", ErrorCodeUnexpectedStep), "step IDs drifted into the patch marker's slot")
	})

	t.Run("PatchingNotEnabledError", func(t *testing.T) {
		// Create a DBOS context without enabling patching
		databaseURL := backendDatabaseURL(t)
		dbosCtxNoPatching, err := NewContext(context.Background(), Config{
			DatabaseURL:    databaseURL,
			AppName:        "test-app-no-patching",
			EnablePatching: false, // Explicitly disable patching
		})
		require.NoError(t, err, "failed to create DBOS context without patching")
		require.False(t, dbosCtxNoPatching.GetApplicationVersion() == "PATCHING_ENABLED", "expected application version to not be PATCHING_ENABLED")

		// Register a workflow that calls Patch
		wfWithPatch := func(ctx Context, input int) (int, error) {
			patched, err := Patch(ctx, "test-patch")
			if err != nil {
				return 0, err
			}
			if patched {
				return input + 10, nil
			}
			return input, nil
		}
		RegisterWorkflow(dbosCtxNoPatching, wfWithPatch)

		// Test DeprecatePatch as well
		wfWithDeprecatePatch := func(ctx Context, input int) (int, error) {
			err := DeprecatePatch(ctx, "test-patch")
			if err != nil {
				return 0, err
			}
			return input + 10, nil
		}
		RegisterWorkflow(dbosCtxNoPatching, wfWithDeprecatePatch)

		err = Launch(dbosCtxNoPatching)
		require.NoError(t, err, "failed to launch DBOS context")
		defer Shutdown(dbosCtxNoPatching, 10*time.Second)

		// Run the workflow - it should fail with ErrorCodePatchingNotEnabled error
		handle, err := RunWorkflow(dbosCtxNoPatching, wfWithPatch, 1)
		require.NoError(t, err, "failed to start workflow")
		_, err = handle.GetResult()
		require.Error(t, err, "expected error when calling Patch without EnablePatching")

		// Verify it's the correct error type
		dbosErr, ok := err.(*Error)
		require.True(t, ok, "expected error to be of type *Error, got %T", err)
		require.Equal(t, ErrorCodePatchingNotEnabled, dbosErr.Code, "expected error code to be ErrorCodePatchingNotEnabled")
		require.Contains(t, dbosErr.Message, "Patching system is not enabled", "expected error message to mention patching is not enabled")
		require.Contains(t, dbosErr.Message, "EnablePatching", "expected error message to mention EnablePatching")

		// Deprecate path
		handle2, err := RunWorkflow(dbosCtxNoPatching, wfWithDeprecatePatch, 1)
		require.NoError(t, err, "failed to start workflow with DeprecatePatch")
		_, err = handle2.GetResult()
		require.Error(t, err, "expected error when calling DeprecatePatch without EnablePatching")

		// Verify it's the correct error type
		dbosErr2, ok := err.(*Error)
		require.True(t, ok, "expected error to be of type *Error, got %T", err)
		require.Equal(t, ErrorCodePatchingNotEnabled, dbosErr2.Code, "expected error code to be ErrorCodePatchingNotEnabled")
		require.Contains(t, dbosErr2.Message, "Patching system is not enabled", "expected error message to mention patching is not enabled")
		require.Contains(t, dbosErr2.Message, "EnablePatching", "expected error message to mention EnablePatching")
	})

	t.Run("PatchingEnabledWithVersioning", func(t *testing.T) {
		t.Run("PreservesApplicationVersionWhenSetInConfig", func(t *testing.T) {
			// Clear env vars to ensure we're testing config values
			t.Setenv("DBOS__APPVERSION", "")
			databaseURL := backendDatabaseURL(t)
			resetTestDatabase(t, databaseURL)

			// Create a DBOS context with patching enabled and a custom application version
			dbosCtx, err := NewContext(context.Background(), Config{
				DatabaseURL:        databaseURL,
				AppName:            "test-app-patching-with-version",
				EnablePatching:     true,
				ApplicationVersion: "custom-version-1.2.3",
			})
			require.NoError(t, err, "failed to create DBOS context with patching enabled and custom version")
			require.Equal(t, "custom-version-1.2.3", dbosCtx.GetApplicationVersion(), "expected application version to be preserved from config")

			// Register cleanup
			t.Cleanup(func() {
				if dbosCtx != nil {
					Shutdown(dbosCtx, 30*time.Second)
				}
			})
		})

		t.Run("EnvironmentVariableOverridesPatchingEnabled", func(t *testing.T) {
			// Set environment variable to override the automatic PATCHING_ENABLED value
			t.Setenv("DBOS__APPVERSION", "env-override-version-2.0.0")
			databaseURL := backendDatabaseURL(t)
			resetTestDatabase(t, databaseURL)

			// Create a DBOS context with patching enabled but no application version in config
			// The env var should override the automatic "PATCHING_ENABLED" value
			dbosCtx, err := NewContext(context.Background(), Config{
				DatabaseURL:    databaseURL,
				AppName:        "test-app-patching-env-override",
				EnablePatching: true,
				// ApplicationVersion left empty - should default to "PATCHING_ENABLED" but env var overrides it
			})
			require.NoError(t, err, "failed to create DBOS context with patching enabled")
			require.Equal(t, "env-override-version-2.0.0", dbosCtx.GetApplicationVersion(), "expected environment variable to override PATCHING_ENABLED")

			// Register cleanup
			t.Cleanup(func() {
				if dbosCtx != nil {
					Shutdown(dbosCtx, 30*time.Second)
				}
			})
		})
	})
}

// Helper workflows for stream testing

var (
	streamBlockEvent   *Event
	streamStartedEvent *Event
	streamWriteGate    chan struct{}
)

// gatedWriteStreamWorkflow writes one value each time the gate is signaled,
// letting the test measure the reader's wake-up latency per write.
func gatedWriteStreamWorkflow(ctx Context, input struct {
	StreamKey string
	NumValues int
}) (string, error) {
	for i := range input.NumValues {
		<-streamWriteGate
		if err := WriteStream(ctx, input.StreamKey, fmt.Sprintf("value-%d", i)); err != nil {
			return "", err
		}
	}
	if err := CloseStream(ctx, input.StreamKey); err != nil {
		return "", err
	}
	return "done", nil
}

func writeStreamWorkflow(ctx Context, input struct {
	StreamKey string
	Values    []string
	Close     bool
}) (string, error) {
	// Write from workflow level
	for _, value := range input.Values {
		if err := WriteStream(ctx, input.StreamKey, value); err != nil {
			return "", err
		}
	}

	if streamStartedEvent != nil {
		streamStartedEvent.Set()
	}

	if streamBlockEvent != nil {
		streamBlockEvent.Wait()
	}

	// Write from step level with custom step name
	_, err := RunAsStep(ctx, func(stepCtx context.Context) (string, error) {
		return "", WriteStream(stepCtx.(Context), input.StreamKey, "step-value")
	}, WithStepName("not-just-write"))
	if err != nil {
		return "", err
	}

	if input.Close {
		if err := CloseStream(ctx, input.StreamKey); err != nil {
			return "", err
		}
		// Try to write after close - should error
		return "", WriteStream(ctx, input.StreamKey, "should-fail")
	}
	return "done", nil
}

// readStreamFunc is a function type that reads from a stream and returns values, closed status, and error
type readStreamFunc func(ctx Context, workflowID string, key string) ([]string, bool, error)

// syncReadStream wraps ReadStream for use in test table
func syncReadStream(ctx Context, workflowID string, key string) ([]string, bool, error) {
	return ReadStream[string](ctx, workflowID, key)
}

// asyncReadStream wraps ReadStreamAsync and collects values for use in test table
func asyncReadStream(ctx Context, workflowID string, key string) ([]string, bool, error) {
	ch, err := ReadStreamAsync[string](ctx, workflowID, key)
	if err != nil {
		return nil, false, err
	}
	return collectStreamValues(ch)
}

func TestStreams(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	// Register all stream workflows
	RegisterWorkflow(dbosCtx, writeStreamWorkflow)
	RegisterWorkflow(dbosCtx, gatedWriteStreamWorkflow)

	Launch(dbosCtx)

	// Test table for sync and async versions
	readFuncs := map[string]readStreamFunc{
		"Sync":  syncReadStream,
		"Async": asyncReadStream,
	}

	for name, readFunc := range readFuncs {
		t.Run(name, func(t *testing.T) {
			t.Run("SimpleReadWrite", func(t *testing.T) {
				streamBlockEvent = NewEvent()
				streamBlockEvent.Set()   // Set immediately so workflow proceeds
				streamStartedEvent = nil // Not needed for this test

				streamKey := "test-stream"
				writerHandle, err := RunWorkflow(dbosCtx, writeStreamWorkflow, struct {
					StreamKey string
					Values    []string
					Close     bool
				}{
					StreamKey: streamKey,
					Values:    []string{"value1", "value2", "value3"},
					Close:     true,
				})
				require.NoError(t, err, "failed to start writer workflow")

				// Wait for writer to complete
				_, err = writerHandle.GetResult()
				require.Error(t, err, "expected error when writing to closed stream")
				require.Contains(t, err.Error(), "stream 'test-stream' is already closed")

				// Read from the stream
				values, closed, err := ReadStream[string](dbosCtx, writerHandle.GetWorkflowID(), streamKey)
				require.NoError(t, err, "failed to read stream")
				// Should have: value1, value2, value3 (from workflow level), step-value (from RunAsStep)
				require.Equal(t, []string{"value1", "value2", "value3", "step-value"}, values, "expected 4 values")
				require.True(t, closed, "expected stream to be closed")

				// Verify steps were recorded correctly
				steps, err := GetWorkflowSteps(dbosCtx, writerHandle.GetWorkflowID())
				require.NoError(t, err, "failed to get workflow steps")
				// Should have 3 WriteStream steps (workflow-level) + 1 RunAsStep with custom name (containing step-level write) + 1 CloseStream step + 1 failed writeStream step = 6 steps
				require.Len(t, steps, 6, "expected 6 steps (3 workflow writes + 1 RunAsStep with step write + 1 close + 1 failed writeStream step)")
				require.Equal(t, "DBOS.writeStream", steps[0].StepName, "expected first step to be DBOS.writeStream")
				require.Equal(t, "DBOS.writeStream", steps[1].StepName, "expected second step to be DBOS.writeStream")
				require.Equal(t, "DBOS.writeStream", steps[2].StepName, "expected third step to be DBOS.writeStream")
				require.Equal(t, "not-just-write", steps[3].StepName, "expected fourth step to be 'not-just-write' (RunAsStep with step-level write)")
				require.Equal(t, "DBOS.closeStream", steps[4].StepName, "expected last step to be DBOS.closeStream")
				require.Equal(t, "DBOS.writeStream", steps[5].StepName, "expected fifth step to be DBOS.writeStream")
			})

			t.Run("ReadWorkflowTermination", func(t *testing.T) {
				// Initialize global events for this test
				streamBlockEvent = NewEvent()
				streamBlockEvent.Set() // Set immediately so workflow proceeds

				streamKey := "test-stream-termination"
				writerHandle, err := RunWorkflow(dbosCtx, writeStreamWorkflow, struct {
					StreamKey string
					Values    []string
					Close     bool
				}{
					StreamKey: streamKey,
					Values:    []string{"value1", "value2", "value3"},
					Close:     false, // Do NOT close stream
				})
				require.NoError(t, err, "failed to start writer workflow")

				// Wait for writer to complete (workflow terminates)
				_, err = writerHandle.GetResult()
				require.NoError(t, err, "failed to get result from writer workflow")

				// Read from the stream
				values, closed, err := readFunc(dbosCtx, writerHandle.GetWorkflowID(), streamKey)
				require.NoError(t, err, "failed to read stream")
				// Should have: value1, value2, value3 (from workflow level), step-value (from RunAsStep)
				require.Equal(t, []string{"value1", "value2", "value3", "step-value"}, values, "expected 4 values")
				require.True(t, closed, "expected stream to be closed when workflow terminates")

				// Verify steps were recorded correctly
				steps, err := GetWorkflowSteps(dbosCtx, writerHandle.GetWorkflowID())
				require.NoError(t, err, "failed to get workflow steps")
				// Should have 3 WriteStream steps (workflow-level) + 1 RunAsStep with custom name (containing step-level write) = 4 steps (no CloseStream step)
				require.Len(t, steps, 4, "expected 4 steps (3 workflow writes + 1 RunAsStep with step write, no close)")
				require.Equal(t, "DBOS.writeStream", steps[0].StepName, "expected first step to be DBOS.writeStream")
				require.Equal(t, "DBOS.writeStream", steps[1].StepName, "expected second step to be DBOS.writeStream")
				require.Equal(t, "DBOS.writeStream", steps[2].StepName, "expected third step to be DBOS.writeStream")
				require.Equal(t, "not-just-write", steps[3].StepName, "expected fourth step to be 'not-just-write' (RunAsStep with step-level write)")
			})

			t.Run("StreamWorkflowRecovery", func(t *testing.T) {
				streamBlockEvent = NewEvent()
				streamBlockEvent.Set() // Unblock so workflow runs to completion
				streamStartedEvent = nil

				streamKey := "test-stream-recovery"
				workflowID := uuid.NewString()
				writerHandle, err := RunWorkflow(dbosCtx, writeStreamWorkflow, struct {
					StreamKey string
					Values    []string
					Close     bool
				}{
					StreamKey: streamKey,
					Values:    []string{"value1", "value2", "value3"},
					Close:     false, // Do NOT close stream
				}, WithWorkflowID(workflowID))
				require.NoError(t, err, "failed to start writer workflow")

				// Wait for workflow to complete
				_, err = writerHandle.GetResult()
				require.NoError(t, err, "failed to get result from writer workflow")

				// Flip writer workflow to PENDING and recover
				setWorkflowStatusPending(t, dbosCtx, workflowID)
				recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
				require.NoError(t, err, "failed to recover pending workflows")
				require.Len(t, recoveredHandles, 1, "expected 1 recovered workflow")
				require.Equal(t, workflowID, recoveredHandles[0].GetWorkflowID(), "expected recovered workflow to have same ID")

				_, err = recoveredHandles[0].GetResult()
				require.NoError(t, err, "failed to get result from recovered workflow")

				// Verify values: value1, value2, value3 once each; step-value
				values, closed, err := readFunc(dbosCtx, workflowID, streamKey)
				require.NoError(t, err, "failed to read stream")
				require.True(t, closed, "expected stream to be closed when workflow terminates")
				require.Equal(t, []string{"value1", "value2", "value3", "step-value"}, values, "expected value1, value2, value3 and step-value once each")
				steps, err := GetWorkflowSteps(dbosCtx, workflowID)
				require.NoError(t, err, "failed to get workflow steps")
				require.Len(t, steps, 4, "expected less than or equal to 5 steps (3 workflow writes + 1 RunAsStep with step write that can concurrently write)")
				require.Equal(t, "DBOS.writeStream", steps[0].StepName, "expected first step to be DBOS.writeStream")
				require.Equal(t, "DBOS.writeStream", steps[1].StepName, "expected second step to be DBOS.writeStream")
				require.Equal(t, "DBOS.writeStream", steps[2].StepName, "expected third step to be DBOS.writeStream")
				require.Equal(t, "not-just-write", steps[3].StepName, "expected fourth step to be 'not-just-write' (RunAsStep with step-level write)")
			})

			t.Run("ForkStreams", func(t *testing.T) {
				streamBlockEvent = NewEvent()
				streamStartedEvent = NewEvent()

				streamKey := "test-stream-fork"
				originalHandle, err := RunWorkflow(dbosCtx, writeStreamWorkflow, struct {
					StreamKey string
					Values    []string
					Close     bool
				}{
					StreamKey: streamKey,
					Values:    []string{"value1", "value2"},
					Close:     false,
				})
				require.NoError(t, err, "failed to start original workflow")

				// Wait for workflow to start and do a few writes
				streamStartedEvent.Wait()

				// Fork workflow from step 2 (after the two first writes)
				forkHandle, err := ForkWorkflow[string](dbosCtx, ForkWorkflowInput{OriginalWorkflowID: originalHandle.GetWorkflowID(), StartStep: 2})
				require.NoError(t, err, "failed to fork workflow")

				// Verify forked workflow has stream entries up to step 2 (stream history copied)
				// Query database directly to avoid blocking (ReadStream would block)
				dbosCtxInternal, ok := dbosCtx.(*dbosContext)
				require.True(t, ok, "expected dbosContext")
				sysDB, ok := dbosCtxInternal.systemDB.(*sysdb.SysDB)
				require.True(t, ok, "expected sysDB")

				entries, closed, err := sysDB.ReadStream(context.Background(), sysdb.ReadStreamDBInput{
					WorkflowID: forkHandle.GetWorkflowID(),
					Key:        streamKey,
					FromOffset: 0,
				})
				require.NoError(t, err, "failed to read stream from database")
				require.False(t, closed, "expected stream not to be closed")
				require.Len(t, entries, 2, "expected 2 stream entries in forked workflow")

				// Decode base64-encoded JSON values from database
				serializer := newJSONSerializer[string]()
				decodedValue1, err := serializer.Decode(&entries[0].Value)
				require.NoError(t, err, "failed to decode first stream entry")
				require.Equal(t, "value1", decodedValue1, "expected first entry to be value1")

				decodedValue2, err := serializer.Decode(&entries[1].Value)
				require.NoError(t, err, "failed to decode second stream entry")
				require.Equal(t, "value2", decodedValue2, "expected second entry to be value2")

				// Now unblock both workflows to let them complete
				streamBlockEvent.Set()
				_, err = originalHandle.GetResult()
				require.NoError(t, err, "failed to get result from original workflow")
				_, err = forkHandle.GetResult()
				require.NoError(t, err, "failed to get result from forked workflow")
			})

			t.Run("WriteReadToClosedStream", func(t *testing.T) {
				streamBlockEvent = NewEvent()
				streamBlockEvent.Set()   // Set immediately so workflow proceeds
				streamStartedEvent = nil // Not needed for this test

				streamKey := "test-stream-closed"
				writerHandle, err := RunWorkflow(dbosCtx, writeStreamWorkflow, struct {
					StreamKey string
					Values    []string
					Close     bool
				}{
					StreamKey: streamKey,
					Values:    []string{"value1"},
					Close:     true, // Close stream, then try to write again
				})
				require.NoError(t, err, "failed to start writer workflow")

				// Wait for writer to complete (should error on second write)
				_, err = writerHandle.GetResult()
				require.Error(t, err, "expected error when writing to closed stream")
				require.Contains(t, err.Error(), "stream 'test-stream-closed' is already closed")

				// Verify the stream is closed
				_, closed, err := ReadStream[string](dbosCtx, writerHandle.GetWorkflowID(), streamKey)
				require.NoError(t, err, "failed to read stream")
				require.True(t, closed, "expected stream to be closed")
			})

			t.Run("StreamWithStruct", func(t *testing.T) {
				streamBlockEvent = NewEvent()
				streamBlockEvent.Set() // Set immediately so workflow proceeds

				streamKey := "test-stream-struct"
				// Use struct values in the workflow
				testData := []string{"value1", "value2", "value3"}
				writerHandle, err := RunWorkflow(dbosCtx, writeStreamWorkflow, struct {
					StreamKey string
					Values    []string
					Close     bool
				}{
					StreamKey: streamKey,
					Values:    testData,
					Close:     false,
				})
				require.NoError(t, err, "failed to start writer workflow")

				// Wait for writer to complete
				_, err = writerHandle.GetResult()
				require.NoError(t, err, "failed to get result from writer workflow")

				// Read the values from stream
				values, closed, err := readFunc(dbosCtx, writerHandle.GetWorkflowID(), streamKey)
				require.NoError(t, err, "failed to read stream")
				// Should have: value1, value2, value3 (from workflow level), step-value (from RunAsStep)
				require.Equal(t, []string{"value1", "value2", "value3", "step-value"}, values, "expected all 4 values")
				require.True(t, closed, "expected stream to be closed")
			})
		})
	}

	t.Run("ForkStreams", func(t *testing.T) {
		streamBlockEvent = NewEvent()
		streamStartedEvent = NewEvent()

		streamKey := "test-stream-fork"
		originalHandle, err := RunWorkflow(dbosCtx, writeStreamWorkflow, struct {
			StreamKey string
			Values    []string
			Close     bool
		}{
			StreamKey: streamKey,
			Values:    []string{"value1", "value2"},
			Close:     false,
		})
		require.NoError(t, err, "failed to start original workflow")

		// Wait for workflow to start and do a few writes
		streamStartedEvent.Wait()

		// Fork workflow from step 2 (after the two first writes)
		forkHandle, err := ForkWorkflow[string](dbosCtx, ForkWorkflowInput{OriginalWorkflowID: originalHandle.GetWorkflowID(), StartStep: 2})
		require.NoError(t, err, "failed to fork workflow")

		// Verify forked workflow has stream entries up to step 2 (stream history copied)
		// Query database directly to avoid blocking (ReadStream would block)
		dbosCtxInternal, ok := dbosCtx.(*dbosContext)
		require.True(t, ok, "expected dbosContext")
		sysDB, ok := dbosCtxInternal.systemDB.(*sysdb.SysDB)
		require.True(t, ok, "expected sysDB")

		entries, closed, err := sysDB.ReadStream(context.Background(), sysdb.ReadStreamDBInput{
			WorkflowID: forkHandle.GetWorkflowID(),
			Key:        streamKey,
			FromOffset: 0,
		})
		require.NoError(t, err, "failed to read stream from database")
		require.False(t, closed, "expected stream not to be closed")
		require.Len(t, entries, 2, "expected 2 stream entries in forked workflow")

		// Decode base64-encoded JSON values from database
		serializer := newJSONSerializer[string]()
		decodedValue1, err := serializer.Decode(&entries[0].Value)
		require.NoError(t, err, "failed to decode first stream entry")
		require.Equal(t, "value1", decodedValue1, "expected first entry to be value1")

		decodedValue2, err := serializer.Decode(&entries[1].Value)
		require.NoError(t, err, "failed to decode second stream entry")
		require.Equal(t, "value2", decodedValue2, "expected second entry to be value2")

		// Now unblock both workflows to let them complete
		streamBlockEvent.Set()
		_, err = originalHandle.GetResult()
		require.NoError(t, err, "failed to get result from original workflow")
		_, err = forkHandle.GetResult()
		require.NoError(t, err, "failed to get result from forked workflow")
	})

	t.Run("GoroutineLeakOnContextCancel", func(t *testing.T) {
		// Verifies that the readStream goroutine exits when the consumer's context is
		// cancelled, even if the consumer stops reading from the channel.
		// goleak (via checkLeaks:true on this test) will fail if the goroutine leaks.
		streamBlockEvent = NewEvent()   // not set — workflow blocks after initial writes
		streamStartedEvent = NewEvent() // signals that initial writes are done

		streamKey := "test-stream-leak"
		writerHandle, err := RunWorkflow(dbosCtx, writeStreamWorkflow, struct {
			StreamKey string
			Values    []string
			Close     bool
		}{
			StreamKey: streamKey,
			Values:    []string{"value1", "value2", "value3"},
			Close:     false,
		})
		require.NoError(t, err)

		// Wait until the workflow has written its values and is blocked
		streamStartedEvent.Wait()

		cancelCtx, cancel := WithCancelCause(dbosCtx)
		defer cancel(nil)

		ch, err := ReadStreamAsync[string](cancelCtx, writerHandle.GetWorkflowID(), streamKey)
		require.NoError(t, err)

		// Read one value to confirm the stream is working
		streamValue := <-ch
		require.NoError(t, streamValue.Err)
		require.Equal(t, "value1", streamValue.Value)

		// Cancel the context and abandon the channel — the goroutine must exit on its own
		cancel(nil)

		// Verify the channel closes within a reasonable time (goroutine unblocked)
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatal("readStream goroutine did not exit after context cancellation")
		}

		// Unblock and complete the workflow for clean test teardown
		streamBlockEvent.Set()
		_, err = writerHandle.GetResult()
		require.NoError(t, err)
	})

	t.Run("Snapshot", func(t *testing.T) {
		// A snapshot read drains all currently-available values and returns
		// immediately, even while the writer workflow is still active (PENDING) —
		// where a normal read would block until the workflow becomes inactive.
		streamBlockEvent = NewEvent()   // not set — workflow blocks after initial writes
		streamStartedEvent = NewEvent() // signals that initial writes are done

		streamKey := "test-stream-snapshot"
		writerHandle, err := RunWorkflow(dbosCtx, writeStreamWorkflow, struct {
			StreamKey string
			Values    []string
			Close     bool
		}{
			StreamKey: streamKey,
			Values:    []string{"value1", "value2", "value3"},
			Close:     false,
		})
		require.NoError(t, err)

		// Wait until the workflow has written its values and is blocked (still PENDING)
		streamStartedEvent.Wait()

		// Sync snapshot from the beginning: returns available values, reports not closed
		values, closed, err := ReadStream[string](dbosCtx, writerHandle.GetWorkflowID(), streamKey, WithReadStreamSnapshot())
		require.NoError(t, err)
		require.False(t, closed, "snapshot of an active workflow should report not closed")
		require.Equal(t, []string{"value1", "value2", "value3"}, values)

		// Snapshot honoring a base offset: skips the first two entries
		values, closed, err = ReadStream[string](dbosCtx, writerHandle.GetWorkflowID(), streamKey, WithReadStreamSnapshot(), WithReadStreamFromOffset(2))
		require.NoError(t, err)
		require.False(t, closed)
		require.Equal(t, []string{"value3"}, values)

		// Unblock and complete the workflow for clean teardown
		streamBlockEvent.Set()
		_, err = writerHandle.GetResult()
		require.NoError(t, err)
	})

	t.Run("AsyncErrorHandling", func(t *testing.T) {
		// Test reading from non-existent workflow
		nonExistentWorkflowID := uuid.NewString()
		ch, err := ReadStreamAsync[string](dbosCtx, nonExistentWorkflowID, "non-existent-stream")
		require.NoError(t, err, "failed to start async stream read")

		// Read from channel - should get error
		var receivedError error
		for streamValue := range ch {
			if streamValue.Err != nil {
				receivedError = streamValue.Err
				break
			}
		}

		require.Error(t, receivedError, "expected error for non-existent workflow")
		require.Contains(t, receivedError.Error(), "workflow", "error should mention workflow")

		// Test that channel closes after error
		_, ok := <-ch
		require.False(t, ok, "channel should be closed after error")
	})

	t.Run("NotificationLatency", func(t *testing.T) {
		// A blocked reader must be woken by the streams LISTEN/NOTIFY trigger,
		// not the bounded-wait fallback that fires every sysdb.DBRetryInterval (1s).
		if dbosCtx.(*dbosContext).systemDB.(*sysdb.SysDB).ListenNotifyPool() == nil {
			t.Skip("backend does not support LISTEN/NOTIFY")
		}

		const numValues = 5
		streamWriteGate = make(chan struct{})
		streamKey := "test-stream-latency"
		writerHandle, err := RunWorkflow(dbosCtx, gatedWriteStreamWorkflow, struct {
			StreamKey string
			NumValues int
		}{
			StreamKey: streamKey,
			NumValues: numValues,
		})
		require.NoError(t, err, "failed to start writer workflow")

		ch, err := ReadStreamAsync[string](dbosCtx, writerHandle.GetWorkflowID(), streamKey)
		require.NoError(t, err, "failed to start async stream read")

		var totalLatency time.Duration
		for i := range numValues {
			// Let the reader drain the stream and enter its blocked wait, so the
			// value below is delivered by a notification, not the read pass that
			// follows consuming the previous value.
			time.Sleep(100 * time.Millisecond)
			start := time.Now()
			streamWriteGate <- struct{}{}
			streamValue := <-ch
			require.NoError(t, streamValue.Err)
			require.Equal(t, fmt.Sprintf("value-%d", i), streamValue.Value)
			totalLatency += time.Since(start)
		}

		// Each write is delivered to a reader blocked mid-interval, so with the
		// polling fallback alone the five reads would take ~4.5s combined.
		t.Logf("%d stream values delivered in %v total", numValues, totalLatency)
		require.Less(t, totalLatency, 1*time.Second,
			"blocked readers should be woken by NOTIFY, not the polling fallback")

		streamValue := <-ch
		require.NoError(t, streamValue.Err)
		require.True(t, streamValue.Closed, "expected stream closed after CloseStream")

		_, err = writerHandle.GetResult()
		require.NoError(t, err)
	})
}

// collectStreamValues is a helper function to collect values from an async stream channel
func collectStreamValues[R any](ch <-chan StreamValue[R]) ([]R, bool, error) {
	var values []R
	var closed bool
	var err error

	for streamValue := range ch {
		if streamValue.Err != nil {
			return nil, false, streamValue.Err
		}
		if streamValue.Closed {
			closed = true
			break
		}
		values = append(values, streamValue.Value)
	}

	return values, closed, err
}

// Complex nested struct for testing rich input/output serialization in export/import
type exportTestAddress struct {
	Street  string `json:"street"`
	City    string `json:"city"`
	ZipCode int    `json:"zip_code"`
}

type exportTestPerson struct {
	Name      string              `json:"name"`
	Age       int                 `json:"age"`
	Addresses []exportTestAddress `json:"addresses"`
	Tags      map[string]string   `json:"tags"`
	Scores    []float64           `json:"scores"`
}

func TestExportImportWorkflow(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	eventKey := "export-event-key"
	streamKey := "export-stream-key"

	stepCounter := 0
	exportStep := func(_ context.Context) (string, error) {
		stepCounter++
		return fmt.Sprintf("step-result-%d", stepCounter), nil
	}

	grandchildWf := func(ctx Context, input string) (string, error) {
		return input + "-grandchild", nil
	}

	childWf := func(ctx Context, input exportTestPerson) (exportTestPerson, error) {
		gcHandle, err := RunWorkflow(ctx, grandchildWf, input.Name)
		if err != nil {
			return exportTestPerson{}, err
		}
		gcResult, err := gcHandle.GetResult()
		if err != nil {
			return exportTestPerson{}, err
		}
		input.Tags["grandchild_result"] = gcResult
		return input, nil
	}

	parentWf := func(ctx Context, input exportTestPerson) (exportTestPerson, error) {
		// Step 0: spawn child workflow
		childHandle, err := RunWorkflow(ctx, childWf, input)
		if err != nil {
			return exportTestPerson{}, err
		}
		// Step 1: get child result
		childResult, err := childHandle.GetResult()
		if err != nil {
			return exportTestPerson{}, err
		}

		// Steps 2-6: run 5 steps
		for i := 0; i < 5; i++ {
			_, err := RunAsStep(ctx, func(sctx context.Context) (string, error) {
				return exportStep(sctx)
			})
			if err != nil {
				return exportTestPerson{}, err
			}
		}

		// Step 7: set event with nested value
		eventValue := map[string]any{
			"status":  "completed",
			"details": map[string]any{"count": float64(42), "flag": true},
		}
		if err := SetEvent(ctx, eventKey, eventValue); err != nil {
			return exportTestPerson{}, err
		}

		// Step 8: write stream with structured data
		streamValue := map[string]any{
			"batch": float64(1),
			"items": []any{"alpha", "beta", "gamma"},
		}
		if err := WriteStream(ctx, streamKey, streamValue); err != nil {
			return exportTestPerson{}, err
		}

		childResult.Scores = append(childResult.Scores, 100.0)
		return childResult, nil
	}

	RegisterWorkflow(dbosCtx, parentWf)
	RegisterWorkflow(dbosCtx, childWf)
	RegisterWorkflow(dbosCtx, grandchildWf)

	Launch(dbosCtx)

	input := exportTestPerson{
		Name: "Alice",
		Age:  30,
		Addresses: []exportTestAddress{
			{Street: "123 Main St", City: "Springfield", ZipCode: 62701},
			{Street: "456 Oak Ave", City: "Shelbyville", ZipCode: 62702},
		},
		Tags:   map[string]string{"role": "admin", "department": "engineering"},
		Scores: []float64{95.5, 87.3, 92.1},
	}

	parentID := uuid.NewString()
	handle, err := RunWorkflow(dbosCtx, parentWf, input, WithWorkflowID(parentID))
	require.NoError(t, err)

	result, err := handle.GetResult()
	require.NoError(t, err)
	assert.Equal(t, "Alice", result.Name)
	assert.Equal(t, "Alice-grandchild", result.Tags["grandchild_result"])
	assert.Equal(t, float64(100.0), result.Scores[len(result.Scores)-1])

	childID := fmt.Sprintf("%s-0", parentID)
	grandchildID := fmt.Sprintf("%s-0", childID)

	// Parent: spawn child (0) + getResult (1) + 5 steps (2-6) + setEvent (7) + writeStream (8) = 9 steps
	// Child: spawn grandchild (0) + getResult (1) = 2 steps
	// Grandchild: no steps (just returns)
	originalParentSteps, err := GetWorkflowSteps(dbosCtx, parentID)
	require.NoError(t, err)
	require.Len(t, originalParentSteps, 9, "parent should have 9 steps")

	originalChildSteps, err := GetWorkflowSteps(dbosCtx, childID)
	require.NoError(t, err)
	require.Len(t, originalChildSteps, 2, "child should have 2 steps")

	originalGrandchildSteps, err := GetWorkflowSteps(dbosCtx, grandchildID)
	require.NoError(t, err)
	require.Len(t, originalGrandchildSteps, 0, "grandchild should have 0 steps")

	sdb := dbosCtx.(*dbosContext).systemDB.(*sysdb.SysDB)

	t.Run("ExportWithChildren", func(t *testing.T) {
		exported, err := sdb.ExportWorkflow(dbosCtx, parentID, true)
		require.NoError(t, err)
		require.Len(t, exported, 3, "expected 3 exported workflows (parent + child + grandchild)")

		parentExport := exported[0]
		assert.Equal(t, parentID, *parentExport.WorkflowStatus["workflow_uuid"].(*string))
		assert.NotEmpty(t, parentExport.OperationOutputs, "expected operation outputs for parent")
		assert.NotEmpty(t, parentExport.WorkflowEvents, "expected workflow events for parent")
		assert.NotEmpty(t, parentExport.WorkflowEventsHistory, "expected workflow events history for parent")
		assert.NotEmpty(t, parentExport.Streams, "expected streams for parent")
	})

	t.Run("ExportWithoutChildren", func(t *testing.T) {
		exported, err := sdb.ExportWorkflow(dbosCtx, parentID, false)
		require.NoError(t, err)
		require.Len(t, exported, 1, "expected only 1 exported workflow without children")
	})

	t.Run("ExportNonExistentWorkflow", func(t *testing.T) {
		_, err := sdb.ExportWorkflow(dbosCtx, "non-existent-wf-id", false)
		require.Error(t, err)
		var dbosErr *Error
		require.ErrorAs(t, err, &dbosErr)
		assert.Equal(t, ErrorCodeNonExistentWorkflow, dbosErr.Code)
	})

	t.Run("ImportConflict", func(t *testing.T) {
		exported, err := sdb.ExportWorkflow(dbosCtx, parentID, true)
		require.NoError(t, err)

		err = sdb.ImportWorkflow(dbosCtx, exported)
		require.Error(t, err, "expected error when importing duplicate workflow")
	})

	t.Run("ImportIntoCleanDB", func(t *testing.T) {
		exported, err := sdb.ExportWorkflow(dbosCtx, parentID, true)
		require.NoError(t, err)
		require.Len(t, exported, 3)

		// Delete all workflows so we can re-import
		err = sdb.DeleteWorkflows(dbosCtx, sysdb.DeleteWorkflowsDBInput{
			WorkflowIDs:    []string{parentID},
			DeleteChildren: true,
		})
		require.NoError(t, err)

		// Verify all 3 workflows are gone
		wfs, err := sdb.ListWorkflows(dbosCtx, sysdb.ListWorkflowsDBInput{
			WorkflowIDs: []string{parentID, childID, grandchildID},
		})
		require.NoError(t, err)
		require.Empty(t, wfs, "expected no workflows after deletion")

		// Import the exported workflows
		err = sdb.ImportWorkflow(dbosCtx, exported)
		require.NoError(t, err)

		// Verify all 3 workflows are present with correct input/output
		wfs, err = sdb.ListWorkflows(dbosCtx, sysdb.ListWorkflowsDBInput{
			WorkflowIDs: []string{parentID, childID, grandchildID},
			LoadInput:   true,
			LoadOutput:  true,
		})
		require.NoError(t, err)
		require.Len(t, wfs, 3, "expected 3 workflows after import")

		// Build a map for easy lookup
		wfByID := make(map[string]WorkflowStatus)
		for _, wf := range wfs {
			wfByID[wf.ID] = wf
		}

		// Check parent workflow status and output
		parentWF := wfByID[parentID]
		assert.Equal(t, WorkflowStatusSuccess, parentWF.Status)
		require.NotNil(t, parentWF.Output)
		require.NotNil(t, parentWF.Input)
		// Deserialize and verify the parent output using our serializer
		ser := newJSONSerializer[exportTestPerson]()
		outputPtr, ok := parentWF.Output.(*string)
		require.True(t, ok)
		parentOutput, err := ser.Decode(outputPtr)
		require.NoError(t, err)
		assert.Equal(t, "Alice", parentOutput.Name)
		assert.Equal(t, "Alice-grandchild", parentOutput.Tags["grandchild_result"])
		assert.Equal(t, float64(100.0), parentOutput.Scores[len(parentOutput.Scores)-1])
		// Deserialize and verify the parent input
		inputPtr, ok := parentWF.Input.(*string)
		require.True(t, ok)
		parentInput, err := ser.Decode(inputPtr)
		require.NoError(t, err)
		assert.Equal(t, "Alice", parentInput.Name)
		assert.Equal(t, 30, parentInput.Age)
		assert.Len(t, parentInput.Addresses, 2)
		assert.Equal(t, "123 Main St", parentInput.Addresses[0].Street)

		// Check child workflow
		childWF := wfByID[childID]
		assert.Equal(t, WorkflowStatusSuccess, childWF.Status)
		assert.Equal(t, parentID, childWF.ParentWorkflowID)

		// Check grandchild workflow
		grandchildWF := wfByID[grandchildID]
		assert.Equal(t, WorkflowStatusSuccess, grandchildWF.Status)
		assert.Equal(t, childID, grandchildWF.ParentWorkflowID)

		// Verify steps for all 3 workflows
		importedParentSteps, err := GetWorkflowSteps(dbosCtx, parentID)
		require.NoError(t, err)
		require.Len(t, importedParentSteps, 9, "imported parent should have 9 steps")
		for i, imported := range importedParentSteps {
			assert.Equal(t, originalParentSteps[i].StepID, imported.StepID, "parent step ID mismatch at index %d", i)
			assert.Equal(t, originalParentSteps[i].StepName, imported.StepName, "parent step name mismatch at index %d", i)
		}

		importedChildSteps, err := GetWorkflowSteps(dbosCtx, childID)
		require.NoError(t, err)
		require.Len(t, importedChildSteps, 2, "imported child should have 2 steps")
		for i, imported := range importedChildSteps {
			assert.Equal(t, originalChildSteps[i].StepID, imported.StepID, "child step ID mismatch at index %d", i)
			assert.Equal(t, originalChildSteps[i].StepName, imported.StepName, "child step name mismatch at index %d", i)
		}

		importedGrandchildSteps, err := GetWorkflowSteps(dbosCtx, grandchildID)
		require.NoError(t, err)
		require.Len(t, importedGrandchildSteps, 0, "imported grandchild should have 0 steps")

		// Verify events, streams, and history via direct DB queries
		schemaPrefix := sdb.Dialect().SchemaPrefix(sdb.Schema())

		var eventCount int
		err = sdb.Pool().QueryRow(dbosCtx,
			sdb.RenderSQL(`SELECT COUNT(*) FROM %sworkflow_events WHERE workflow_uuid = $1`, schemaPrefix),
			parentID).Scan(&eventCount)
		require.NoError(t, err)
		assert.Greater(t, eventCount, 0, "expected events to be imported")

		var streamCount int
		err = sdb.Pool().QueryRow(dbosCtx,
			sdb.RenderSQL(`SELECT COUNT(*) FROM %sstreams WHERE workflow_uuid = $1`, schemaPrefix),
			parentID).Scan(&streamCount)
		require.NoError(t, err)
		assert.Greater(t, streamCount, 0, "expected streams to be imported")

		var historyCount int
		err = sdb.Pool().QueryRow(dbosCtx,
			sdb.RenderSQL(`SELECT COUNT(*) FROM %sworkflow_events_history WHERE workflow_uuid = $1`, schemaPrefix),
			parentID).Scan(&historyCount)
		require.NoError(t, err)
		assert.Greater(t, historyCount, 0, "expected workflow events history to be imported")

		// Verify the imported workflow can be forked
		forkHandle, err := ForkWorkflow[exportTestPerson](dbosCtx, ForkWorkflowInput{OriginalWorkflowID: parentID, StartStep: uint(len(importedParentSteps))})
		require.NoError(t, err)

		forkResult, err := forkHandle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "Alice", forkResult.Name)
		assert.Equal(t, "Alice-grandchild", forkResult.Tags["grandchild_result"])
	})
}

func aggregatesWorkflowSuccess(_ Context, _ string) (string, error) {
	return "ok", nil
}

func aggregatesWorkflowFail(_ Context, _ string) (string, error) {
	return "", fmt.Errorf("aggregate-fail")
}

func TestGetWorkflowAggregates(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	RegisterWorkflow(dbosCtx, aggregatesWorkflowSuccess)
	RegisterWorkflow(dbosCtx, aggregatesWorkflowFail)

	require.NoError(t, Launch(dbosCtx), "failed to launch DBOS instance")

	successFQN := runtime.FuncForPC(reflect.ValueOf(aggregatesWorkflowSuccess).Pointer()).Name()
	failFQN := runtime.FuncForPC(reflect.ValueOf(aggregatesWorkflowFail).Pointer()).Name()

	// Run 3 successful workflows and 2 failing workflows
	for i := 0; i < 3; i++ {
		handle, err := RunWorkflow(dbosCtx, aggregatesWorkflowSuccess, fmt.Sprintf("ok-%d", i))
		require.NoError(t, err)
		_, err = handle.GetResult()
		require.NoError(t, err)
	}
	for i := 0; i < 2; i++ {
		handle, err := RunWorkflow(dbosCtx, aggregatesWorkflowFail, fmt.Sprintf("fail-%d", i))
		require.NoError(t, err)
		_, err = handle.GetResult()
		require.Error(t, err)
	}

	t.Run("GroupByStatus", func(t *testing.T) {
		rows, err := GetWorkflowAggregates(dbosCtx, GetWorkflowAggregatesInput{GroupByStatus: true, SelectCount: true})
		require.NoError(t, err)
		statusCounts := map[string]int64{}
		for _, r := range rows {
			require.NotNil(t, r.Group["status"], "status grouping key should be non-nil")
			require.NotNil(t, r.Count)
			statusCounts[*r.Group["status"]] = *r.Count
		}
		assert.Equal(t, int64(3), statusCounts[string(WorkflowStatusSuccess)])
		assert.Equal(t, int64(2), statusCounts[string(WorkflowStatusError)])
	})

	t.Run("GroupByName", func(t *testing.T) {
		rows, err := GetWorkflowAggregates(dbosCtx, GetWorkflowAggregatesInput{GroupByName: true, SelectCount: true})
		require.NoError(t, err)
		nameCounts := map[string]int64{}
		for _, r := range rows {
			require.NotNil(t, r.Group["name"])
			require.NotNil(t, r.Count)
			nameCounts[*r.Group["name"]] = *r.Count
		}
		assert.Equal(t, int64(3), nameCounts[successFQN])
		assert.Equal(t, int64(2), nameCounts[failFQN])
	})

	t.Run("GroupByStatusAndName", func(t *testing.T) {
		rows, err := GetWorkflowAggregates(dbosCtx, GetWorkflowAggregatesInput{
			GroupByStatus: true,
			GroupByName:   true,
			SelectCount:   true,
		})
		require.NoError(t, err)
		type key struct {
			status string
			name   string
		}
		combo := map[key]int64{}
		for _, r := range rows {
			require.NotNil(t, r.Group["status"])
			require.NotNil(t, r.Group["name"])
			require.NotNil(t, r.Count)
			combo[key{status: *r.Group["status"], name: *r.Group["name"]}] = *r.Count
		}
		assert.Equal(t, int64(3), combo[key{status: string(WorkflowStatusSuccess), name: successFQN}])
		assert.Equal(t, int64(2), combo[key{status: string(WorkflowStatusError), name: failFQN}])
	})

	t.Run("FilterByStatus", func(t *testing.T) {
		rows, err := GetWorkflowAggregates(dbosCtx, GetWorkflowAggregatesInput{
			GroupByName: true,
			SelectCount: true,
			Status:      []WorkflowStatusType{WorkflowStatusSuccess},
		})
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.NotNil(t, rows[0].Group["name"])
		require.NotNil(t, rows[0].Count)
		assert.Equal(t, successFQN, *rows[0].Group["name"])
		assert.Equal(t, int64(3), *rows[0].Count)
	})

	t.Run("FilterByName", func(t *testing.T) {
		rows, err := GetWorkflowAggregates(dbosCtx, GetWorkflowAggregatesInput{
			GroupByStatus: true,
			SelectCount:   true,
			Name:          []string{failFQN},
		})
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.NotNil(t, rows[0].Group["status"])
		require.NotNil(t, rows[0].Count)
		assert.Equal(t, string(WorkflowStatusError), *rows[0].Group["status"])
		assert.Equal(t, int64(2), *rows[0].Count)
	})

	t.Run("FilterByWorkflowIDPrefix", func(t *testing.T) {
		// Run workflows with known prefixes
		for i := 0; i < 2; i++ {
			handle, err := RunWorkflow(dbosCtx, aggregatesWorkflowSuccess, fmt.Sprintf("prefix-%d", i),
				WithWorkflowID(fmt.Sprintf("agg-prefix-%d", i)))
			require.NoError(t, err)
			_, err = handle.GetResult()
			require.NoError(t, err)
		}

		rows, err := GetWorkflowAggregates(dbosCtx, GetWorkflowAggregatesInput{
			GroupByName:      true,
			SelectCount:      true,
			WorkflowIDPrefix: []string{"agg-prefix"},
		})
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.NotNil(t, rows[0].Group["name"])
		require.NotNil(t, rows[0].Count)
		assert.Equal(t, successFQN, *rows[0].Group["name"])
		assert.Equal(t, int64(2), *rows[0].Count)

		// Filter by nonexistent prefix yields no rows
		rows, err = GetWorkflowAggregates(dbosCtx, GetWorkflowAggregatesInput{
			GroupByStatus:    true,
			SelectCount:      true,
			WorkflowIDPrefix: []string{"nonexistent-prefix-"},
		})
		require.NoError(t, err)
		assert.Empty(t, rows)
	})

	t.Run("FilterByAttributes", func(t *testing.T) {
		skipIfSqlite(t, "attribute filters require JSONB containment")

		for i := 0; i < 2; i++ {
			handle, err := RunWorkflow(dbosCtx, aggregatesWorkflowSuccess, fmt.Sprintf("attr-%d", i),
				WithWorkflowAttributes(map[string]any{"customer": "acme-agg", "tier": 1}))
			require.NoError(t, err)
			_, err = handle.GetResult()
			require.NoError(t, err)
		}

		rows, err := GetWorkflowAggregates(dbosCtx, GetWorkflowAggregatesInput{
			GroupByName: true,
			SelectCount: true,
			Attributes:  map[string]any{"customer": "acme-agg"},
		})
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.NotNil(t, rows[0].Group["name"])
		require.NotNil(t, rows[0].Count)
		assert.Equal(t, successFQN, *rows[0].Group["name"])
		assert.Equal(t, int64(2), *rows[0].Count)

		// A non-matching attribute value yields no rows
		rows, err = GetWorkflowAggregates(dbosCtx, GetWorkflowAggregatesInput{
			GroupByStatus: true,
			SelectCount:   true,
			Attributes:    map[string]any{"customer": "nobody"},
		})
		require.NoError(t, err)
		assert.Empty(t, rows)
	})

	t.Run("FilterByAttributesUnsupportedOnSQLite", func(t *testing.T) {
		if !useSqliteBackend() {
			t.Skip("tests the SQLite-only error path")
		}
		_, err := GetWorkflowAggregates(dbosCtx, GetWorkflowAggregatesInput{
			GroupByStatus: true,
			SelectCount:   true,
			Attributes:    map[string]any{"customer": "acme"},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not supported")
	})

	t.Run("SelectMinCreatedAtAndLatency", func(t *testing.T) {
		// Selecting only the timestamp/latency aggregates leaves Count nil.
		rows, err := GetWorkflowAggregates(dbosCtx, GetWorkflowAggregatesInput{
			GroupByStatus:           true,
			SelectMinCreatedAt:      true,
			SelectMaxTotalLatencyMs: true,
			Status:                  []WorkflowStatusType{WorkflowStatusSuccess},
		})
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Nil(t, rows[0].Count, "count should not be selected")
		require.NotNil(t, rows[0].MinCreatedAt)
		assert.Greater(t, *rows[0].MinCreatedAt, int64(0))
		require.NotNil(t, rows[0].MaxTotalLatencyMs)
		assert.GreaterOrEqual(t, *rows[0].MaxTotalLatencyMs, int64(0))
	})

	t.Run("NoSelectFlagErrors", func(t *testing.T) {
		_, err := GetWorkflowAggregates(dbosCtx, GetWorkflowAggregatesInput{GroupByStatus: true})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "at least one select_")
	})

	t.Run("NoGroupByErrors", func(t *testing.T) {
		_, err := GetWorkflowAggregates(dbosCtx, GetWorkflowAggregatesInput{SelectCount: true})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "at least one group_by")
	})

	t.Run("NegativeTimeBucketErrors", func(t *testing.T) {
		_, err := GetWorkflowAggregates(dbosCtx, GetWorkflowAggregatesInput{
			GroupByStatus:  true,
			SelectCount:    true,
			TimeBucketSize: -time.Minute,
		})
		require.Error(t, err)
	})

	t.Run("TimeBucketAlone", func(t *testing.T) {
		oneHour := time.Hour
		rows, err := GetWorkflowAggregates(dbosCtx, GetWorkflowAggregatesInput{
			TimeBucketSize: oneHour,
			SelectCount:    true,
		})
		require.NoError(t, err)
		require.NotEmpty(t, rows)
		var total int64
		for _, r := range rows {
			require.NotNil(t, r.Group["time_bucket"])
			tb := *r.Group["time_bucket"]
			// Each bucket value must be a multiple of the bucket size in ms
			var bucketMs int64
			_, scanErr := fmt.Sscanf(tb, "%d", &bucketMs)
			require.NoError(t, scanErr, "expected numeric time_bucket value, got %q", tb)
			assert.Equal(t, int64(0), bucketMs%oneHour.Milliseconds(),
				"bucket %d must be a multiple of %d", bucketMs, oneHour.Milliseconds())
			require.NotNil(t, r.Count)
			total += *r.Count
		}
		// At least 3 + 2 + 2 prefix runs
		assert.GreaterOrEqual(t, total, int64(7))
	})

	t.Run("TimeBucketWithGroupByStatus", func(t *testing.T) {
		oneHour := time.Hour
		rows, err := GetWorkflowAggregates(dbosCtx, GetWorkflowAggregatesInput{
			GroupByStatus:  true,
			SelectCount:    true,
			TimeBucketSize: oneHour,
		})
		require.NoError(t, err)
		var successTotal, errorTotal int64
		for _, r := range rows {
			require.NotNil(t, r.Group["status"])
			require.NotNil(t, r.Group["time_bucket"])
			require.NotNil(t, r.Count)
			switch *r.Group["status"] {
			case string(WorkflowStatusSuccess):
				successTotal += *r.Count
			case string(WorkflowStatusError):
				errorTotal += *r.Count
			}
		}
		assert.GreaterOrEqual(t, successTotal, int64(5))
		assert.Equal(t, int64(2), errorTotal)
	})

	t.Run("TimeBucketWithStatusFilter", func(t *testing.T) {
		oneMinute := time.Minute
		rows, err := GetWorkflowAggregates(dbosCtx, GetWorkflowAggregatesInput{
			TimeBucketSize: oneMinute,
			SelectCount:    true,
			Status:         []WorkflowStatusType{WorkflowStatusError},
		})
		require.NoError(t, err)
		require.NotEmpty(t, rows)
		var total int64
		for _, r := range rows {
			require.NotNil(t, r.Group["time_bucket"])
			tb := *r.Group["time_bucket"]
			var bucketMs int64
			_, scanErr := fmt.Sscanf(tb, "%d", &bucketMs)
			require.NoError(t, scanErr)
			assert.Equal(t, int64(0), bucketMs%oneMinute.Milliseconds())
			require.NotNil(t, r.Count)
			total += *r.Count
		}
		assert.Equal(t, int64(2), total)
	})
}

func stepAggOK(_ context.Context) (string, error) { return "ok", nil }

func stepAggBad(_ context.Context) (string, error) { return "", errors.New("boom") }

// stepAggregatesWorkflow runs two successful steps (aggStepOK) and one failing step
// (aggStepBad, whose error is caught) so the operation_outputs table holds a known mix
// of SUCCESS and ERROR steps.
func stepAggregatesWorkflow(ctx Context, _ string) (string, error) {
	if _, err := RunAsStep(ctx, stepAggOK, WithStepName("aggStepOK")); err != nil {
		return "", err
	}
	if _, err := RunAsStep(ctx, stepAggOK, WithStepName("aggStepOK")); err != nil {
		return "", err
	}
	_, _ = RunAsStep(ctx, stepAggBad, WithStepName("aggStepBad"))
	return "done", nil
}

func TestGetStepAggregates(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	RegisterWorkflow(dbosCtx, stepAggregatesWorkflow)
	require.NoError(t, Launch(dbosCtx), "failed to launch DBOS instance")

	// Run 3 workflows: 6 aggStepOK (SUCCESS), 3 aggStepBad (ERROR).
	for i := 0; i < 3; i++ {
		handle, err := RunWorkflow(dbosCtx, stepAggregatesWorkflow, fmt.Sprintf("in-%d", i))
		require.NoError(t, err)
		_, err = handle.GetResult()
		require.NoError(t, err)
	}

	t.Run("GroupByFunctionName", func(t *testing.T) {
		rows, err := GetStepAggregates(dbosCtx, GetStepAggregatesInput{
			GroupByFunctionName: true,
			SelectCount:         true,
		})
		require.NoError(t, err)
		counts := map[string]int64{}
		for _, r := range rows {
			require.NotNil(t, r.Group["function_name"])
			require.NotNil(t, r.Count)
			counts[*r.Group["function_name"]] = *r.Count
		}
		assert.Equal(t, int64(6), counts["aggStepOK"])
		assert.Equal(t, int64(3), counts["aggStepBad"])
	})

	t.Run("GroupByStatus", func(t *testing.T) {
		rows, err := GetStepAggregates(dbosCtx, GetStepAggregatesInput{
			GroupByStatus: true,
			SelectCount:   true,
		})
		require.NoError(t, err)
		counts := map[string]int64{}
		for _, r := range rows {
			require.NotNil(t, r.Group["status"])
			require.NotNil(t, r.Count)
			counts[*r.Group["status"]] = *r.Count
		}
		assert.Equal(t, int64(6), counts["SUCCESS"])
		assert.Equal(t, int64(3), counts["ERROR"])
	})

	t.Run("FilterByFunctionName", func(t *testing.T) {
		rows, err := GetStepAggregates(dbosCtx, GetStepAggregatesInput{
			GroupByStatus: true,
			SelectCount:   true,
			FunctionName:  []string{"aggStepBad"},
		})
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.NotNil(t, rows[0].Group["status"])
		assert.Equal(t, "ERROR", *rows[0].Group["status"])
		require.NotNil(t, rows[0].Count)
		assert.Equal(t, int64(3), *rows[0].Count)
	})

	t.Run("FilterByStatus", func(t *testing.T) {
		rows, err := GetStepAggregates(dbosCtx, GetStepAggregatesInput{
			GroupByFunctionName: true,
			SelectCount:         true,
			Status:              []string{"SUCCESS"},
		})
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.NotNil(t, rows[0].Group["function_name"])
		assert.Equal(t, "aggStepOK", *rows[0].Group["function_name"])
		require.NotNil(t, rows[0].Count)
		assert.Equal(t, int64(6), *rows[0].Count)
	})

	t.Run("SelectMaxDuration", func(t *testing.T) {
		rows, err := GetStepAggregates(dbosCtx, GetStepAggregatesInput{
			GroupByFunctionName: true,
			SelectMaxDurationMs: true,
		})
		require.NoError(t, err)
		require.NotEmpty(t, rows)
		for _, r := range rows {
			assert.Nil(t, r.Count, "count should not be selected")
			require.NotNil(t, r.MaxDurationMs, "max_duration_ms should be selected")
			assert.GreaterOrEqual(t, *r.MaxDurationMs, int64(0))
		}
	})

	t.Run("NoGroupByReturnsError", func(t *testing.T) {
		_, err := GetStepAggregates(dbosCtx, GetStepAggregatesInput{SelectCount: true})
		require.Error(t, err)
	})

	t.Run("NoSelectReturnsError", func(t *testing.T) {
		_, err := GetStepAggregates(dbosCtx, GetStepAggregatesInput{GroupByFunctionName: true})
		require.Error(t, err)
	})
}

var (
	attrBlockingStartEvent = NewEvent()
	attrBlockingBlockEvent = NewEvent()
)

func attrChildWorkflow(dbosCtx Context, _ string) (string, error) {
	return "child", nil
}

// attrParentWorkflow runs a child workflow and returns the child's workflow ID
func attrParentWorkflow(dbosCtx Context, _ string) (string, error) {
	handle, err := RunWorkflow(dbosCtx, attrChildWorkflow, "")
	if err != nil {
		return "", err
	}
	if _, err := handle.GetResult(); err != nil {
		return "", err
	}
	return handle.GetWorkflowID(), nil
}

func attrNoopWorkflow(dbosCtx Context, x int) (int, error) {
	return x, nil
}

func attrBlockingWorkflow(dbosCtx Context, _ string) (string, error) {
	attrBlockingStartEvent.Set()
	attrBlockingBlockEvent.Wait()
	return "done", nil
}

func attrDebouncedWorkflow(dbosCtx Context, x int) (int, error) {
	return x, nil
}

func TestWorkflowAttributes(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	RegisterWorkflow(dbosCtx, attrParentWorkflow)
	RegisterWorkflow(dbosCtx, attrChildWorkflow)
	RegisterWorkflow(dbosCtx, attrNoopWorkflow)
	RegisterWorkflow(dbosCtx, attrBlockingWorkflow)
	RegisterWorkflow(dbosCtx, attrDebouncedWorkflow)

	queue, err := RegisterQueue(dbosCtx, "attr-test-queue")
	require.NoError(t, err)

	debouncer, err := NewDebouncer(dbosCtx, attrDebouncedWorkflow)
	require.NoError(t, err, "failed to create the debouncer")

	require.NoError(t, Launch(dbosCtx), "failed to launch DBOS")

	// matchedIDs returns the set of workflow IDs whose attributes contain all the given key-value pairs
	matchedIDs := func(t *testing.T, attributes map[string]any) map[string]bool {
		t.Helper()
		statuses, err := ListWorkflows(dbosCtx, WithFilterAttributes(attributes))
		require.NoError(t, err)
		ids := make(map[string]bool, len(statuses))
		for _, s := range statuses {
			ids[s.ID] = true
		}
		return ids
	}

	t.Run("DirectInvocation", func(t *testing.T) {
		wfid := uuid.NewString()
		attributes := map[string]any{"customer": "acme", "tier": 3}
		handle, err := RunWorkflow(dbosCtx, attrParentWorkflow, "", WithWorkflowID(wfid), WithWorkflowAttributes(attributes))
		require.NoError(t, err)
		childID, err := handle.GetResult()
		require.NoError(t, err)

		status, err := handle.GetStatus()
		require.NoError(t, err)
		// Numbers round-trip through JSON as float64
		assert.Equal(t, map[string]any{"customer": "acme", "tier": float64(3)}, status.Attributes)

		// Child workflows do not inherit their parent's attributes
		childStatuses, err := ListWorkflows(dbosCtx, WithFilterWorkflowIDs(childID))
		require.NoError(t, err)
		require.Len(t, childStatuses, 1)
		assert.Nil(t, childStatuses[0].Attributes)
		// Workflows not enqueued by a named schedule have no schedule name and
		// are never returned by the schedule name filter.
		assert.Empty(t, childStatuses[0].ScheduleName)

		// Workflows started without the option have no attributes
		plainHandle, err := RunWorkflow(dbosCtx, attrParentWorkflow, "")
		require.NoError(t, err)
		_, err = plainHandle.GetResult()
		require.NoError(t, err)
		plainStatus, err := plainHandle.GetStatus()
		require.NoError(t, err)
		assert.Nil(t, plainStatus.Attributes)
	})

	t.Run("Enqueue", func(t *testing.T) {
		handle, err := RunWorkflow(dbosCtx, attrNoopWorkflow, 5, WithQueue(queue), WithWorkflowAttributes(map[string]any{"source": "queue"}))
		require.NoError(t, err)
		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, 5, result)
		status, err := handle.GetStatus()
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"source": "queue"}, status.Attributes)
	})

	t.Run("Fork", func(t *testing.T) {
		wfid := uuid.NewString()
		handle, err := RunWorkflow(dbosCtx, attrNoopWorkflow, 7, WithWorkflowID(wfid), WithWorkflowAttributes(map[string]any{"customer": "acme-fork"}))
		require.NoError(t, err)
		_, err = handle.GetResult()
		require.NoError(t, err)

		forkedHandle, err := ForkWorkflow[int](dbosCtx, ForkWorkflowInput{OriginalWorkflowID: wfid})
		require.NoError(t, err)
		_, err = forkedHandle.GetResult()
		require.NoError(t, err)
		forkedStatus, err := forkedHandle.GetStatus()
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"customer": "acme-fork"}, forkedStatus.Attributes)
	})

	t.Run("ClientEnqueue", func(t *testing.T) {
		config := ClientConfig{
			DatabaseURL: backendDatabaseURL(t),
		}
		client, err := NewClient(dbosCtx, config)
		require.NoError(t, err)
		t.Cleanup(func() {
			client.Shutdown(client, 30 * time.Second)
		})

		// Enqueue to a queue nothing consumes; the workflow stays ENQUEUED, which
		// is enough to check the attributes recorded at creation.
		handle, err := client.Enqueue(client, "unconsumed-queue", "client-workflow", 1, WithEnqueueAttributes(map[string]any{"source": "client", "n": 1}))
		require.NoError(t, err)
		status, err := handle.GetStatus()
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"source": "client", "n": float64(1)}, status.Attributes)

		if !useSqliteBackend() {
			handle2, err := client.Enqueue(client, "unconsumed-queue", "client-workflow", 2, WithEnqueueAttributes(map[string]any{"source": "client", "n": 2}))
			require.NoError(t, err)
			statuses, err := client.ListWorkflows(client, WithFilterAttributes(map[string]any{"source": "client"}))
			require.NoError(t, err)
			ids := make(map[string]bool, len(statuses))
			for _, s := range statuses {
				ids[s.ID] = true
			}
			assert.Equal(t, map[string]bool{handle.GetWorkflowID(): true, handle2.GetWorkflowID(): true}, ids)
			queued, err := client.ListWorkflows(client, WithFilterQueuesOnly(), WithFilterAttributes(map[string]any{"n": 2}))
			require.NoError(t, err)
			require.Len(t, queued, 1)
			assert.Equal(t, handle2.GetWorkflowID(), queued[0].ID)
		}
	})

	t.Run("ListFilter", func(t *testing.T) {
		skipIfSqlite(t, "attribute filters require JSONB containment")

		h1, err := RunWorkflow(dbosCtx, attrNoopWorkflow, 1, WithWorkflowAttributes(map[string]any{"customer": "acme-list", "tier": 1, "beta": true, "note": nil}))
		require.NoError(t, err)
		h2, err := RunWorkflow(dbosCtx, attrNoopWorkflow, 2, WithWorkflowAttributes(map[string]any{"customer": "bigco-list", "tier": 2, "meta": map[string]any{"region": "us-east-1"}}))
		require.NoError(t, err)
		_, err = h1.GetResult()
		require.NoError(t, err)
		_, err = h2.GetResult()
		require.NoError(t, err)

		// Single key
		assert.Equal(t, map[string]bool{h1.GetWorkflowID(): true}, matchedIDs(t, map[string]any{"customer": "acme-list"}))
		// Multiple keys AND together
		assert.Equal(t, map[string]bool{h2.GetWorkflowID(): true}, matchedIDs(t, map[string]any{"customer": "bigco-list", "tier": 2}))
		// Value mismatch on one key matches nothing
		assert.Empty(t, matchedIDs(t, map[string]any{"customer": "acme-list", "tier": 2}))
		// Non-string value types
		assert.Equal(t, map[string]bool{h1.GetWorkflowID(): true}, matchedIDs(t, map[string]any{"beta": true}))
		assert.Equal(t, map[string]bool{h1.GetWorkflowID(): true}, matchedIDs(t, map[string]any{"note": nil}))
		assert.Equal(t, map[string]bool{h2.GetWorkflowID(): true}, matchedIDs(t, map[string]any{"meta": map[string]any{"region": "us-east-1"}}))
		// Composes with other filters
		composed, err := ListWorkflows(dbosCtx, WithFilterAttributes(map[string]any{"tier": 1}), WithFilterWorkflowIDs(h2.GetWorkflowID()))
		require.NoError(t, err)
		assert.Empty(t, composed)
		// Workflows without attributes never match
		assert.Empty(t, matchedIDs(t, map[string]any{"missing": "key"}))
	})

	t.Run("ListQueued", func(t *testing.T) {
		skipIfSqlite(t, "attribute filters require JSONB containment")

		handle, err := RunWorkflow(dbosCtx, attrBlockingWorkflow, "", WithQueue(queue), WithWorkflowAttributes(map[string]any{"side": "queued"}))
		require.NoError(t, err)
		attrBlockingStartEvent.Wait()

		queued, err := ListWorkflows(dbosCtx, WithFilterQueuesOnly(), WithFilterAttributes(map[string]any{"side": "queued"}))
		require.NoError(t, err)
		require.Len(t, queued, 1)
		assert.Equal(t, handle.GetWorkflowID(), queued[0].ID)

		other, err := ListWorkflows(dbosCtx, WithFilterQueuesOnly(), WithFilterAttributes(map[string]any{"side": "other"}))
		require.NoError(t, err)
		assert.Empty(t, other)

		attrBlockingBlockEvent.Set()
		_, err = handle.GetResult()
		require.NoError(t, err)
	})

	t.Run("FilterUnsupportedOnSQLite", func(t *testing.T) {
		if !useSqliteBackend() {
			t.Skip("tests the SQLite-only error path")
		}
		_, err := ListWorkflows(dbosCtx, WithFilterAttributes(map[string]any{"customer": "acme"}))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not supported")
	})

	t.Run("NonSerializableAttributesRejected", func(t *testing.T) {
		_, err := RunWorkflow(dbosCtx, attrNoopWorkflow, 1, WithWorkflowAttributes(map[string]any{"bad": make(chan int)}))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to marshal workflow attributes")
	})

	t.Run("Debouncer", func(t *testing.T) {
		handle, err := debouncer.Debounce(dbosCtx, "attr-key", 100*time.Millisecond, 9, WithWorkflowAttributes(map[string]any{"source": "debouncer"}))
		require.NoError(t, err)
		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, 9, result)
		status, err := handle.GetStatus()
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"source": "debouncer"}, status.Attributes)
	})

	t.Run("Update", func(t *testing.T) {
		wfid := uuid.NewString()
		handle, err := RunWorkflow(dbosCtx, attrNoopWorkflow, 1, WithWorkflowID(wfid), WithWorkflowAttributes(map[string]any{"customer": "acme-upd", "tier": 1}))
		require.NoError(t, err)
		_, err = handle.GetResult()
		require.NoError(t, err)

		// Replace the attributes entirely
		require.NoError(t, SetWorkflowAttributes(dbosCtx, wfid, map[string]any{"customer": "globex-upd"}))
		status, err := handle.GetStatus()
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"customer": "globex-upd"}, status.Attributes)

		// The old attribute value no longer matches; the new one does (Postgres only)
		if !useSqliteBackend() {
			assert.NotContains(t, matchedIDs(t, map[string]any{"customer": "acme-upd"}), wfid)
			assert.Contains(t, matchedIDs(t, map[string]any{"customer": "globex-upd"}), wfid)
		}

		// Passing nil clears all attributes
		require.NoError(t, SetWorkflowAttributes(dbosCtx, wfid, nil))
		status, err = handle.GetStatus()
		require.NoError(t, err)
		assert.Nil(t, status.Attributes)
	})

	t.Run("UpdateNonExistentWorkflow", func(t *testing.T) {
		err := SetWorkflowAttributes(dbosCtx, "does-not-exist-"+uuid.NewString(), map[string]any{"k": "v"})
		require.Error(t, err)
		var dbosErr *Error
		require.ErrorAs(t, err, &dbosErr)
		assert.Equal(t, ErrorCodeNonExistentWorkflow, dbosErr.Code)
	})
}

func TestFork(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	// Workflow whose second and third steps fail on their first call, for fork-from-failure tests.
	var failStepOneCount, failStepTwoCount, failStepThreeCount atomic.Int64
	failableWorkflow := func(ctx Context, _ string) (int, error) {
		one, err := RunAsStep(ctx, func(ctx context.Context) (int, error) {
			failStepOneCount.Add(1)
			return 1, nil
		}, WithStepName("stepOne"))
		if err != nil {
			return 0, err
		}
		two, err := RunAsStep(ctx, func(ctx context.Context) (int, error) {
			if failStepTwoCount.Add(1) == 1 { // fail on first call only (wf1)
				return 0, errors.New("step two failed")
			}
			return 2, nil
		}, WithStepName("stepTwo"))
		if err != nil {
			return 0, err
		}
		three, err := RunAsStep(ctx, func(ctx context.Context) (int, error) {
			if failStepThreeCount.Add(1) == 1 { // fail on first call only (wf2)
				return 0, errors.New("step three failed")
			}
			return 3, nil
		}, WithStepName("stepThree"))
		if err != nil {
			return 0, err
		}
		return one + two + three, nil
	}

	// Always-succeeding workflow for bulk fork tests.
	var stepOneCount, stepTwoCount, stepThreeCount atomic.Int64
	threeStepWorkflow := func(ctx Context, _ string) (int, error) {
		one, err := RunAsStep(ctx, func(ctx context.Context) (int, error) {
			stepOneCount.Add(1)
			return 1, nil
		}, WithStepName("stepOne"))
		if err != nil {
			return 0, err
		}
		two, err := RunAsStep(ctx, func(ctx context.Context) (int, error) {
			stepTwoCount.Add(1)
			return 2, nil
		}, WithStepName("stepTwo"))
		if err != nil {
			return 0, err
		}
		three, err := RunAsStep(ctx, func(ctx context.Context) (int, error) {
			stepThreeCount.Add(1)
			return 3, nil
		}, WithStepName("stepThree"))
		if err != nil {
			return 0, err
		}
		return one + two + three, nil
	}

	// Workflow that catches its second step's error and continues, so its last
	// failed step (1) differs from its last step (2) — distinguishing
	// fromLastFailure from fromLastStep.
	var caughtStepTwoCount atomic.Int64
	caughtFailureWorkflow := func(ctx Context, _ string) (int, error) {
		one, err := RunAsStep(ctx, func(ctx context.Context) (int, error) {
			return 1, nil
		}, WithStepName("stepOne"))
		if err != nil {
			return 0, err
		}
		two, err := RunAsStep(ctx, func(ctx context.Context) (int, error) {
			if caughtStepTwoCount.Add(1) == 1 { // fail on first call only
				return 0, errors.New("step two failed")
			}
			return 2, nil
		}, WithStepName("stepTwo"))
		if err != nil {
			two = 0 // swallow the error and continue
		}
		three, err := RunAsStep(ctx, func(ctx context.Context) (int, error) {
			return 3, nil
		}, WithStepName("stepThree"))
		if err != nil {
			return 0, err
		}
		return one + two + three, nil
	}

	RegisterWorkflow(dbosCtx, failableWorkflow, WithWorkflowName("failableThreeStepWorkflow"))
	RegisterWorkflow(dbosCtx, threeStepWorkflow, WithWorkflowName("bulkForkWorkflow"))
	RegisterWorkflow(dbosCtx, caughtFailureWorkflow, WithWorkflowName("caughtFailureWorkflow"))
	require.NoError(t, Launch(dbosCtx))

	sysDB := dbosCtx.(*dbosContext).systemDB

	t.Run("FromFailure", func(t *testing.T) {
		runToFailure := func(expectedErr string) string {
			wfID := uuid.NewString()
			handle, err := RunWorkflow(dbosCtx, failableWorkflow, "", WithWorkflowID(wfID))
			require.NoError(t, err)
			_, err = handle.GetResult()
			require.ErrorContains(t, err, expectedErr)
			return wfID
		}

		awaitForks := func(forkedIDs []string) {
			for _, fid := range forkedIDs {
				fh, err := RetrieveWorkflow[int](dbosCtx, fid)
				require.NoError(t, err)
				res, err := fh.GetResult()
				require.NoError(t, err)
				require.Equal(t, 6, res)
			}
		}

		// wf1: step two fails -> last failed step is 1
		wf1ID := runToFailure("step two failed")
		require.Equal(t, int64(1), failStepOneCount.Load())
		require.Equal(t, int64(1), failStepTwoCount.Load())
		require.Equal(t, int64(0), failStepThreeCount.Load())

		// wf2: step two succeeds, step three fails -> last failed step is 2
		wf2ID := runToFailure("step three failed")
		require.Equal(t, int64(2), failStepOneCount.Load())
		require.Equal(t, int64(2), failStepTwoCount.Load())
		require.Equal(t, int64(1), failStepThreeCount.Load())

		// wf3: all steps succeed -> no failed step, falls back to last step (2)
		wf3ID := uuid.NewString()
		handle, err := RunWorkflow(dbosCtx, failableWorkflow, "", WithWorkflowID(wf3ID))
		require.NoError(t, err)
		res, err := handle.GetResult()
		require.NoError(t, err)
		require.Equal(t, 6, res)
		require.Equal(t, int64(3), failStepOneCount.Load())
		require.Equal(t, int64(3), failStepTwoCount.Load())
		require.Equal(t, int64(2), failStepThreeCount.Load())

		t.Run("FromLastFailure", func(t *testing.T) {
			forkedIDs, err := sysDB.ForkFrom(dbosCtx, sysdb.ForkFromDBInput{
				WorkflowIDs:     []string{wf1ID, wf2ID, wf3ID},
				FromLastFailure: true,
			})
			require.NoError(t, err)
			require.Len(t, forkedIDs, 3)
			awaitForks(forkedIDs)

			require.Equal(t, int64(3), failStepOneCount.Load())   // replayed for all three forks
			require.Equal(t, int64(4), failStepTwoCount.Load())   // re-run for wf1's fork only
			require.Equal(t, int64(5), failStepThreeCount.Load()) // re-run for all three forks

			// A fork also marks its source was_forked_from.
			srcs, err := sysDB.ListWorkflows(dbosCtx, sysdb.ListWorkflowsDBInput{WorkflowIDs: []string{wf1ID}})
			require.NoError(t, err)
			require.Len(t, srcs, 1)
			require.True(t, srcs[0].WasForkedFrom, "a forked-from workflow should be marked was_forked_from")
		})

		t.Run("FromLastStep", func(t *testing.T) {
			forkedIDs, err := sysDB.ForkFrom(dbosCtx, sysdb.ForkFromDBInput{
				WorkflowIDs:  []string{wf1ID, wf2ID, wf3ID},
				FromLastStep: true,
			})
			require.NoError(t, err)
			require.Len(t, forkedIDs, 3)
			awaitForks(forkedIDs)

			// wf1's last step is stepTwo (stepThree never ran), so stepTwo re-runs
			require.Equal(t, int64(5), failStepTwoCount.Load())
			// all three forks re-run stepThree
			require.Equal(t, int64(8), failStepThreeCount.Load())
		})

		t.Run("FromStep", func(t *testing.T) {
			startStep := 0
			forkedIDs, err := sysDB.ForkFrom(dbosCtx, sysdb.ForkFromDBInput{
				WorkflowIDs: []string{wf3ID},
				FromStep:    &startStep,
			})
			require.NoError(t, err)
			require.Len(t, forkedIDs, 1)
			awaitForks(forkedIDs)

			require.Equal(t, int64(4), failStepOneCount.Load())
			require.Equal(t, int64(6), failStepTwoCount.Load())
			require.Equal(t, int64(9), failStepThreeCount.Load())
		})

		t.Run("FromStepName", func(t *testing.T) {
			stepName := "stepTwo"
			forkedIDs, err := sysDB.ForkFrom(dbosCtx, sysdb.ForkFromDBInput{
				WorkflowIDs:  []string{wf3ID},
				FromStepName: &stepName,
			})
			require.NoError(t, err)
			require.Len(t, forkedIDs, 1)
			awaitForks(forkedIDs)

			require.Equal(t, int64(4), failStepOneCount.Load())    // replayed
			require.Equal(t, int64(7), failStepTwoCount.Load())    // re-run
			require.Equal(t, int64(10), failStepThreeCount.Load()) // re-run
		})

		t.Run("Validation", func(t *testing.T) {
			// wf1 never ran stepThree
			missingName := "stepThree"
			_, err := sysDB.ForkFrom(dbosCtx, sysdb.ForkFromDBInput{
				WorkflowIDs:  []string{wf1ID},
				FromStepName: &missingName,
			})
			require.ErrorContains(t, err, "has no step named")

			nonexistent := "nonexistentStep"
			_, err = sysDB.ForkFrom(dbosCtx, sysdb.ForkFromDBInput{
				WorkflowIDs:  []string{wf3ID},
				FromStepName: &nonexistent,
			})
			require.ErrorContains(t, err, "has no step named")

			// no mode specified
			_, err = sysDB.ForkFrom(dbosCtx, sysdb.ForkFromDBInput{
				WorkflowIDs: []string{wf3ID},
			})
			require.ErrorContains(t, err, "exactly one")

			// multiple modes specified
			_, err = sysDB.ForkFrom(dbosCtx, sysdb.ForkFromDBInput{
				WorkflowIDs:     []string{wf3ID},
				FromLastFailure: true,
				FromLastStep:    true,
			})
			require.ErrorContains(t, err, "exactly one")
		})

		t.Run("LastFailureVsLastStep", func(t *testing.T) {
			wfID := uuid.NewString()
			handle, err := RunWorkflow(dbosCtx, caughtFailureWorkflow, "", WithWorkflowID(wfID))
			require.NoError(t, err)
			res, err := handle.GetResult()
			require.NoError(t, err)
			require.Equal(t, 4, res) // stepTwo's error was caught, so two contributes 0

			forkAndGet := func(input sysdb.ForkFromDBInput) int {
				input.WorkflowIDs = []string{wfID}
				forkedIDs, err := sysDB.ForkFrom(dbosCtx, input)
				require.NoError(t, err)
				require.Len(t, forkedIDs, 1)
				fh, err := RetrieveWorkflow[int](dbosCtx, forkedIDs[0])
				require.NoError(t, err)
				res, err := fh.GetResult()
				require.NoError(t, err)
				return res
			}

			// fromLastStep starts at the last step (stepThree): stepTwo's
			// checkpointed error replays and is caught again.
			require.Equal(t, 4, forkAndGet(sysdb.ForkFromDBInput{FromLastStep: true}))
			// fromLastFailure starts at the failed step (stepTwo) even though a
			// later step succeeded: stepTwo re-runs and succeeds this time.
			require.Equal(t, 6, forkAndGet(sysdb.ForkFromDBInput{FromLastFailure: true}))
		})
	})

	t.Run("ForkWorkflows", func(t *testing.T) {
		// Run three workflows to completion
		originalIDs := make([]string, 3)
		for i := range originalIDs {
			originalIDs[i] = uuid.NewString()
			handle, err := RunWorkflow(dbosCtx, threeStepWorkflow, "", WithWorkflowID(originalIDs[i]))
			require.NoError(t, err)
			res, err := handle.GetResult()
			require.NoError(t, err)
			require.Equal(t, 6, res)
		}
		require.Equal(t, int64(3), stepOneCount.Load())
		require.Equal(t, int64(3), stepTwoCount.Load())
		require.Equal(t, int64(3), stepThreeCount.Load())

		awaitForks := func(forkedIDs []string) {
			for i, fid := range forkedIDs {
				fh, err := RetrieveWorkflow[int](dbosCtx, fid)
				require.NoError(t, err)
				res, err := fh.GetResult()
				require.NoError(t, err)
				require.Equal(t, 6, res)
				status, err := fh.GetStatus()
				require.NoError(t, err)
				require.Equal(t, originalIDs[i], status.ForkedFrom)
			}
		}

		t.Run("MixedStartSteps", func(t *testing.T) {
			forkedIDs, err := sysDB.ForkWorkflows(dbosCtx, sysdb.ForkWorkflowsDBInput{
				OriginalWorkflowIDs: originalIDs,
				StartSteps:          []int{0, 1, 2},
			})
			require.NoError(t, err)
			require.Len(t, forkedIDs, 3)
			require.Len(t, map[string]bool{forkedIDs[0]: true, forkedIDs[1]: true, forkedIDs[2]: true}, 3)
			awaitForks(forkedIDs)

			// fork 1 re-runs all steps, fork 2 replays stepOne, fork 3 replays stepOne and stepTwo
			require.Equal(t, int64(4), stepOneCount.Load())
			require.Equal(t, int64(5), stepTwoCount.Load())
			require.Equal(t, int64(6), stepThreeCount.Load())

			// The originals are marked as forked from
			for _, id := range originalIDs {
				oh, err := RetrieveWorkflow[int](dbosCtx, id)
				require.NoError(t, err)
				status, err := oh.GetStatus()
				require.NoError(t, err)
				require.True(t, status.WasForkedFrom)
			}
		})

		t.Run("CustomForkedIDs", func(t *testing.T) {
			customID := "custom-forked-" + uuid.NewString()
			forkedIDs, err := sysDB.ForkWorkflows(dbosCtx, sysdb.ForkWorkflowsDBInput{
				OriginalWorkflowIDs: originalIDs[:2],
				ForkedWorkflowIDs:   []string{customID, ""}, // empty entry is auto-generated
				StartSteps:          []int{2, 2},
			})
			require.NoError(t, err)
			require.Len(t, forkedIDs, 2)
			require.Equal(t, customID, forkedIDs[0])
			require.NotEmpty(t, forkedIDs[1])
			awaitForks(forkedIDs)
		})

		t.Run("PublicAPI", func(t *testing.T) {
			// Exercise the public batch fork API end-to-end, including a custom
			// forked ID, mixed start steps, and an auto-generated ID.
			customID := "public-forked-" + uuid.NewString()
			handles, err := ForkWorkflows[int](dbosCtx, ForkWorkflowsInput{
				Workflows: []ForkWorkflowSpec{
					{OriginalWorkflowID: originalIDs[0], ForkedWorkflowID: customID, StartStep: 2},
					{OriginalWorkflowID: originalIDs[1], StartStep: 1},
					{OriginalWorkflowID: originalIDs[2]},
				},
			})
			require.NoError(t, err)
			require.Len(t, handles, 3)
			// Handles are returned in the same order as the input specs.
			require.Equal(t, customID, handles[0].GetWorkflowID())
			require.NotEmpty(t, handles[1].GetWorkflowID())

			forkedIDs := make([]string, len(handles))
			for i, h := range handles {
				res, err := h.GetResult()
				require.NoError(t, err)
				require.Equal(t, 6, res)
				forkedIDs[i] = h.GetWorkflowID()
			}
			awaitForks(forkedIDs)

			t.Run("Validation", func(t *testing.T) {
				_, err := ForkWorkflows[int](nil, ForkWorkflowsInput{})
				require.ErrorContains(t, err, "ctx cannot be nil")

				_, err = ForkWorkflows[int](dbosCtx, ForkWorkflowsInput{})
				require.ErrorContains(t, err, "at least one workflow")

				_, err = ForkWorkflows[int](dbosCtx, ForkWorkflowsInput{
					Workflows: []ForkWorkflowSpec{{OriginalWorkflowID: ""}},
				})
				require.ErrorContains(t, err, "original workflow ID cannot be empty")

				_, err = ForkWorkflows[int](dbosCtx, ForkWorkflowsInput{
					Workflows:         []ForkWorkflowSpec{{OriginalWorkflowID: originalIDs[0]}},
					QueuePartitionKey: "pk",
				})
				require.ErrorContains(t, err, "queue partition key requires a queue name")
			})
		})

		t.Run("Validation", func(t *testing.T) {
			// Empty input is a no-op
			forkedIDs, err := sysDB.ForkWorkflows(dbosCtx, sysdb.ForkWorkflowsDBInput{})
			require.NoError(t, err)
			require.Empty(t, forkedIDs)

			// startSteps length mismatch
			_, err = sysDB.ForkWorkflows(dbosCtx, sysdb.ForkWorkflowsDBInput{
				OriginalWorkflowIDs: originalIDs,
				StartSteps:          []int{0},
			})
			require.ErrorContains(t, err, "same length")

			// forkedWorkflowIDs length mismatch
			_, err = sysDB.ForkWorkflows(dbosCtx, sysdb.ForkWorkflowsDBInput{
				OriginalWorkflowIDs: originalIDs,
				ForkedWorkflowIDs:   []string{"only-one"},
				StartSteps:          []int{0, 0, 0},
			})
			require.ErrorContains(t, err, "same length")

			// Negative start step
			_, err = sysDB.ForkWorkflows(dbosCtx, sysdb.ForkWorkflowsDBInput{
				OriginalWorkflowIDs: originalIDs[:1],
				StartSteps:          []int{-1},
			})
			require.ErrorContains(t, err, "startStep must be >= 0")
		})

		t.Run("Atomicity", func(t *testing.T) {
			// Count existing forks of the first original
			listForks := func() int {
				wfs, err := sysDB.ListWorkflows(dbosCtx, sysdb.ListWorkflowsDBInput{ForkedFrom: originalIDs[:1]})
				require.NoError(t, err)
				return len(wfs)
			}
			forksBefore := listForks()

			// A batch containing a nonexistent workflow fails and forks nothing
			_, err := sysDB.ForkWorkflows(dbosCtx, sysdb.ForkWorkflowsDBInput{
				OriginalWorkflowIDs: []string{originalIDs[0], "nonexistent-workflow-id"},
				StartSteps:          []int{2, 2},
			})
			require.ErrorContains(t, err, "nonexistent-workflow-id does not exist")
			require.Equal(t, forksBefore, listForks())
		})
	})
}

// TestConcurrentStartRaceSameExecutor reproduces B5: the active-workflow-ID check
// (Load inside the insert tx) and set (LoadOrStore in the spawned goroutine) are not
// atomic, and recovery/dequeue starts bypass the ownerXID guard. Two concurrent
// recovery requests for the same PENDING workflow can both pass the Load check before
// either goroutine runs LoadOrStore, so both execute the workflow body; the loser then
// deleted the winner's active entry on completion. The workflow body must never run
// concurrently with itself on one executor, and every handle must still resolve.
func TestConcurrentStartRaceSameExecutor(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	c := dbosCtx.(*dbosContext)

	var running atomic.Int32
	var doubleExecutions atomic.Int32
	raceWorkflow := func(ctx Context, _ string) (string, error) {
		if running.Add(1) > 1 {
			doubleExecutions.Add(1)
		}
		defer running.Add(-1)
		// Keep the body open long enough for an overlapping execution to be observed.
		time.Sleep(50 * time.Millisecond)
		return "done", nil
	}
	RegisterWorkflow(dbosCtx, raceWorkflow, WithWorkflowName("start-race-workflow"))
	require.NoError(t, Launch(dbosCtx), "failed to launch DBOS")

	const attempts = 20
	for i := range attempts {
		workflowID := fmt.Sprintf("start-race-%d", i)
		handle, err := RunWorkflow(dbosCtx, raceWorkflow, "in", WithWorkflowID(workflowID))
		require.NoError(t, err, "failed to run workflow")
		_, err = handle.GetResult()
		require.NoError(t, err, "failed to get seed result")

		setWorkflowStatusPending(t, dbosCtx, workflowID)

		// Fire two concurrent recovery requests for the same PENDING workflow. Both
		// bypass the ownerXID guard, so both can land in the Load/LoadOrStore window.
		var wg sync.WaitGroup
		recoveredHandles := make([][]WorkflowHandle[any], 2)
		for j := range 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				handles, err := recoverPendingWorkflows(c, []string{"local"})
				assert.NoError(t, err, "recoverPendingWorkflows failed")
				recoveredHandles[j] = handles
			}()
		}
		wg.Wait()

		// Join every recovered execution before asserting; a handle that never
		// resolves (workflow stuck PENDING) fails the test via the suite timeout.
		var handleErrs []error
		for _, handles := range recoveredHandles {
			for _, h := range handles {
				if _, err := h.GetResult(); err != nil {
					handleErrs = append(handleErrs, err)
				}
			}
		}

		require.Zero(t, doubleExecutions.Load(),
			"attempt %d: workflow body executed concurrently with itself on the same executor", i)
		for _, err := range handleErrs {
			require.NoError(t, err, "attempt %d: a recovered handle returned an error", i)
		}

		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		require.Equal(t, WorkflowStatusSuccess, status.Status, "attempt %d: workflow must end SUCCESS", i)
	}
}
