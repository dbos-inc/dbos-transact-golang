package dbos

import (
	"context"
	"fmt"
	"os"
)

var (
	APP_VERSION string
	EXECUTOR_ID string
)

func init() {
	// Initialize with environment variables, providing defaults if not set
	APP_VERSION := os.Getenv("DBOS__APPVERSION")
	if APP_VERSION == "" {
		APP_VERSION = "unknown" // TODO compute a version based on code hash
		fmt.Printf("DBOS: DBOS__APPVERSION not set, using default: %s\n", APP_VERSION)
	}

	EXECUTOR_ID = os.Getenv("DBOS__VMID")
	if EXECUTOR_ID == "" {
		// Generate a default ID or leave empty based on your requirements
		EXECUTOR_ID = "local"
		fmt.Printf("DBOS: DBOS__VMID not set, using default: %s\n", EXECUTOR_ID)
	}

	fmt.Printf("DBOS: Initialized with APP_VERSION=%s, EXECUTOR_ID=%s\n", APP_VERSION, EXECUTOR_ID)
}

type Executor interface {
	Destroy()
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
	// TODO find a good strategy
	if dbos == nil {
		panic("DBOS instance is not initialized")
	}
	return dbos
}

func Launch() error {
	if dbos != nil {
		// XXX: maybe just log a warning instead of returning an error
		return fmt.Errorf("DBOS already initialized")
	}
	// Create the system database
	systemDB, err := NewSystemDatabase()
	if err != nil {
		return fmt.Errorf("failed to create system database: %w", err)
	}

	// Create context with cancel function for queue runner
	ctx, cancel := context.WithCancel(context.Background())

	dbos = &executor{
		systemDB:              systemDB,
		queueRunnerCtx:        ctx,
		queueRunnerCancelFunc: cancel,
	}

	// Start the queue runner in a goroutine
	go queueRunner(ctx)

	return nil
}

// Close closes the DBOS instance and its resources
// TODO: rename destroy
func Destroy() {
	if dbos == nil {
		fmt.Println("warning: DBOS instance is nil, cannot destroy")
		return
	}

	// Cancel the context to stop the queue runner
	if dbos.queueRunnerCancelFunc != nil {
		dbos.queueRunnerCancelFunc()
	}

	if dbos.systemDB != nil {
		dbos.systemDB.Destroy()
	}
	dbos = nil // Mark the DBOS instance for garbage collection
}
