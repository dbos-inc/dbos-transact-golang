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
		fn := registry[fqn]

		// Try to get function source location and other identifying info
		if pc := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()); pc != nil {
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

// DBOS represents the main DBOS instance
type executor struct {
	systemDB              SystemDatabase
	queueRunnerCtx        context.Context
	queueRunnerCancelFunc context.CancelFunc
}

// New creates a new DBOS instance with an initialized system database
var dbos *executor

func getExecutor() *executor {
	if dbos == nil {
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
	}

	// Start the queue runner in a goroutine
	go queueRunner(ctx)
	fmt.Println("DBOS: Queue runner started")

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
	}

	if dbos.systemDB != nil {
		dbos.systemDB.Shutdown()
	}
	dbos = nil // Mark the DBOS instance for garbage collection
}
