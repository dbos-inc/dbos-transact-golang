package dbos

import (
	"testing"
)

// TestInitializeReturnsExecutor verifies that our updated Initialize function works correctly
func TestInitializeReturnsExecutor(t *testing.T) {
	databaseURL := getDatabaseURL()

	// Test that Initialize returns a DBOSExecutor
	ctx, err := NewDBOSContext(Config{
		DatabaseURL: databaseURL,
		AppName:     "test-initialize",
	})
	if err != nil {
		t.Fatalf("Failed to initialize DBOS: %v", err)
	}
	defer func() {
		if ctx != nil {
			ctx.Shutdown()
		}
	}() // Clean up executor

	if ctx == nil {
		t.Fatal("Initialize returned nil executor")
	}

	// Test that executor implements DBOSContext interface
	var _ DBOSContext = ctx

	// Test that we can call methods on the executor
	appVersion := ctx.GetApplicationVersion()
	if appVersion == "" {
		t.Fatal("GetApplicationVersion returned empty string")
	}

	scheduler := ctx.(*dbosContext).getWorkflowScheduler()
	if scheduler == nil {
		t.Fatal("getWorkflowScheduler returned nil")
	}
}

// TestWithWorkflowWithExecutor verifies that WithWorkflow works with an executor
func TestWithWorkflowWithExecutor(t *testing.T) {
	ctx := setupDBOS(t)

	// Test workflow function
	testWorkflow := func(ctx DBOSContext, input string) (string, error) {
		return "hello " + input, nil
	}

	// Test that RegisterWorkflow works with executor
	RegisterWorkflow(ctx, testWorkflow)

	// Test executing the workflow
	handle, err := RunAsWorkflow(ctx, testWorkflow, "world")
	if err != nil {
		t.Fatalf("Failed to execute workflow: %v", err)
	}

	result, err := handle.GetResult()
	if err != nil {
		t.Fatalf("Failed to get workflow result: %v", err)
	}

	expected := "hello world"
	if result != expected {
		t.Fatalf("Expected %q, got %q", expected, result)
	}
}

// TestSetupDBOSReturnsExecutor verifies that setupDBOS returns an executor
func TestSetupDBOSReturnsExecutor(t *testing.T) {
	executor := setupDBOS(t)

	if executor == nil {
		t.Fatal("setupDBOS returned nil executor")
	}

	// Test succeeded - executor is valid
}
