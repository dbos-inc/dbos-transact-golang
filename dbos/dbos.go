package dbos

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"sort"
	"time"

	"github.com/robfig/cron/v3"
)

var (
	APP_VERSION string
	EXECUTOR_ID string
	APP_ID      string
)

func computeApplicationVersion() string {
	if len(registry) == 0 {
		fmt.Println("DBOS: No registered workflows found, cannot compute application version")
		return ""
	}

	// Collect all function names and sort them for consistent hashing
	var functionNames []string
	for fqn := range registry {
		functionNames = append(functionNames, fqn)
	}
	sort.Strings(functionNames)

	hasher := sha256.New()

	for _, fqn := range functionNames {
		workflowEntry := registry[fqn]

		// Try to get function source location and other identifying info
		if pc := runtime.FuncForPC(reflect.ValueOf(workflowEntry.wrappedFunction).Pointer()); pc != nil {
			// Get the function's entry point - this reflects the actual compiled code
			entry := pc.Entry()
			fmt.Fprintf(hasher, "%x", entry)
		}
	}

	return hex.EncodeToString(hasher.Sum(nil))

}

type Executor interface {
	Shutdown()
}

var workflowScheduler *cron.Cron

// DBOS represents the main DBOS instance
type executor struct {
	systemDB              SystemDatabase
	queueRunnerCtx        context.Context
	queueRunnerCancelFunc context.CancelFunc
	queueRunnerDone       chan struct{}
}

// New creates a new DBOS instance with an initialized system database
var dbos *executor

func getExecutor() *executor {
	if dbos == nil {
		fmt.Println("warning: DBOS instance not initiliazed")
		return nil
	}
	return dbos
}

func Launch() error {
	if dbos != nil {
		fmt.Println("warning: DBOS instance already initialized, skipping re-initialization")
		return NewInitializationError("DBOS already initialized")
	}

	// Initialize with environment variables, providing defaults if not set
	APP_VERSION = os.Getenv("DBOS__APPVERSION")
	if APP_VERSION == "" {
		APP_VERSION = computeApplicationVersion()
		fmt.Println("DBOS: DBOS__APPVERSION not set, using computed hash")
	}

	EXECUTOR_ID = os.Getenv("DBOS__VMID")
	if EXECUTOR_ID == "" {
		EXECUTOR_ID = "local"
		fmt.Printf("DBOS: DBOS__VMID not set, using default: %s\n", EXECUTOR_ID)
	}

	APP_ID = os.Getenv("DBOS__APPID")

	// Create the system database
	systemDB, err := NewSystemDatabase()
	if err != nil {
		return NewInitializationError(fmt.Sprintf("failed to create system database: %v", err))
	}
	fmt.Println("DBOS: System database initialized")

	// Create context with cancel function for queue runner
	ctx, cancel := context.WithCancel(context.Background())

	dbos = &executor{
		systemDB:              systemDB,
		queueRunnerCtx:        ctx,
		queueRunnerCancelFunc: cancel,
		queueRunnerDone:       make(chan struct{}),
	}

	// Start the queue runner in a goroutine
	go func() {
		defer close(dbos.queueRunnerDone)
		queueRunner(ctx)
	}()
	fmt.Println("DBOS: Queue runner started")

	// Start the workflow scheduler if it has been initialized
	if workflowScheduler != nil {
		workflowScheduler.Start()
		fmt.Println("DBOS: Workflow scheduler started")
	}

	// Run a round of recovery on the local executor
	_, err = recoverPendingWorkflows(context.Background(), []string{EXECUTOR_ID}) // XXX maybe use the queue runner context here to allow Shutdown to cancel it?
	if err != nil {
		return NewInitializationError(fmt.Sprintf("failed to recover pending workflows during launch: %v", err))
	}

	fmt.Printf("DBOS: Initialized with APP_VERSION=%s, EXECUTOR_ID=%s\n", APP_VERSION, EXECUTOR_ID)
	return nil
}

// Close closes the DBOS instance and its resources
func Shutdown() {
	if dbos == nil {
		fmt.Println("warning: DBOS instance is nil, cannot destroy")
		return
	}

	// Cancel the context to stop the queue runner
	if dbos.queueRunnerCancelFunc != nil {
		dbos.queueRunnerCancelFunc()
		// Wait for queue runner to finish
		<-dbos.queueRunnerDone
		fmt.Println("DBOS: Queue runner stopped")
	}

	if workflowScheduler != nil {
		ctx := workflowScheduler.Stop()
		// Wait for all running jobs to complete with 5-second timeout
		timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		select {
		case <-ctx.Done():
			fmt.Println("DBOS: All scheduled jobs completed")
		case <-timeoutCtx.Done():
			fmt.Println("DBOS: Timeout waiting for jobs to complete (5s)")
		}
	}

	if dbos.systemDB != nil {
		dbos.systemDB.Shutdown()
	}

	dbos = nil // Mark the DBOS instance for garbage collection
}
