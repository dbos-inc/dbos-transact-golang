package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
)

// Configuration structure
type Config struct {
	Name        string `yaml:"name"`
	Language    string `yaml:"language"`
	DatabaseURL string `yaml:"database_url"`
}

// Global variables
var dbosCtx dbos.DBOSContext

/*****************************/
/**** WORKFLOWS AND STEPS ****/
/*****************************/

func ExampleWorkflow(ctx dbos.DBOSContext, _ string) (string, error) {
	_, err := dbos.RunAsStep(ctx, func(stepCtx context.Context) (string, error) {
		return stepOne(stepCtx)
	})
	if err != nil {
		return "", err
	}
	return dbos.RunAsStep(ctx, func(stepCtx context.Context) (string, error) {
		return stepTwo(stepCtx)
	})
}

func stepOne(ctx context.Context) (string, error) {
	fmt.Println("Step one completed!")
	return "Step 1 completed", nil
}

func stepTwo(ctx context.Context) (string, error) {
	fmt.Println("Step two completed!")
	return "Step 2 completed - Workflow finished successfully", nil
}

/*****************************/
/**** QUEUES *****************/
/*****************************/

func QueuedStepWorkflow(ctx dbos.DBOSContext, i int) (int, error) {
	dbos.Sleep(ctx, 5*time.Second)
	fmt.Printf("Step %d completed!\n", i)
	return i, nil
}

func QueueWorkflow(ctx dbos.DBOSContext, _ string) (string, error) {
	fmt.Println("Enqueueing steps")
	handles := make([]dbos.WorkflowHandle[int], 10)
	for i := range 10 {
		handle, err := dbos.RunWorkflow(ctx, QueuedStepWorkflow, i, dbos.WithQueue("example-queue"))
		if err != nil {
			return "", fmt.Errorf("failed to enqueue step %d: %w", i, err)
		}
		handles[i] = handle
	}
	results := make([]int, 10)
	for i, handle := range handles {
		result, err := handle.GetResult()
		if err != nil {
			return "", fmt.Errorf("failed to get result for step %d: %w", i, err)
		}
		results[i] = result
	}
	fmt.Printf("Successfully completed %d steps\n", len(results))
	return fmt.Sprintf("Successfully completed %d steps", len(results)), nil
}

/*****************************/
/**** SCHEDULED WORKFLOWS ****/
/*****************************/

func ScheduledWorkflow(ctx dbos.DBOSContext, scheduledTime time.Time) (string, error) {
	fmt.Printf("I am a scheduled workflow scheduled at %v and running at %v\n", scheduledTime, time.Now())
	return "", nil
}

func main() {
	// Create DBOS context
	var err error
	dbosCtx, err = dbos.NewDBOSContext(context.Background(), dbos.Config{
		DatabaseURL: os.Getenv("DBOS_SYSTEM_DATABASE_URL"),
		AppName:     "tt",
		AdminServer: true,
	})
	if err != nil {
		panic(err)
	}

	// Register workflows
	dbos.RegisterWorkflow(dbosCtx, ExampleWorkflow)
	dbos.RegisterWorkflow(dbosCtx, QueueWorkflow)
	dbos.RegisterWorkflow(dbosCtx, QueuedStepWorkflow)
	dbos.RegisterWorkflow(dbosCtx, ScheduledWorkflow, dbos.WithSchedule("*/15 * * * * *"))

	// Create queue
	dbos.NewWorkflowQueue(dbosCtx, "example-queue")

	// Launch DBOS
	err = dbosCtx.Launch()
	if err != nil {
		panic(err)
	}
	defer dbosCtx.Shutdown(10 * time.Second)

	// HTTP Handlers
	http.HandleFunc("/", homepageHandler)
	http.HandleFunc("/workflow", workflowHandler)
	http.HandleFunc("/queue", queueHandler)

	fmt.Println("Server starting on http://localhost:8080")
	err = http.ListenAndServe(":8080", nil)
	if err != nil {
		fmt.Printf("Error starting server: %s\n", err)
	}
}

/*****************************/
/**** HTTP HANDLERS **********/
/*****************************/

func workflowHandler(w http.ResponseWriter, r *http.Request) {
	handle, err := dbos.RunWorkflow(dbosCtx, ExampleWorkflow, "")
	if err != nil {
		http.Error(w, fmt.Sprintf("Error: %s", err), http.StatusInternalServerError)
		return
	}
	res, err := handle.GetResult()
	if err != nil {
		http.Error(w, fmt.Sprintf("Error: %s", err), http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, "Workflow result: %s", res)
}

func queueHandler(w http.ResponseWriter, r *http.Request) {
	handle, err := dbos.RunWorkflow(dbosCtx, QueueWorkflow, "")
	if err != nil {
		http.Error(w, fmt.Sprintf("Error: %s", err), http.StatusInternalServerError)
		return
	}
	res, err := handle.GetResult()
	if err != nil {
		http.Error(w, fmt.Sprintf("Error: %s", err), http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, "Workflow result: %s", res)
}

func homepageHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	fmt.Fprint(w, `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link rel="icon" href="https://dbos-blog-posts.s3.us-west-1.amazonaws.com/live-demo/favicon.ico" type="image/x-icon">
    <script src="https://cdn.tailwindcss.com"></script>
    <title>DBOS Toolbox</title>
</head>
<body class="bg-gray-100 min-h-screen font-sans">
    <div class="max-w-2xl mx-auto py-12 px-4">
        <div class="bg-white rounded-lg shadow-lg p-8 space-y-8">
            <h1 class="text-3xl font-bold text-gray-900">Welcome to the DBOS Toolbox!</h1>

            <p class="text-gray-600">
                This app contains example code for many DBOS features. You can use it as a template when starting a new DBOS app—start by editing <code class="bg-gray-100 px-2 py-0.5 rounded font-mono text-sm">main.go</code>.
            </p>

            <p class="text-gray-600">
                Each endpoint launches a new workflow—<strong>view the app logs to see them run.</strong>
            </p>

            <div class="space-y-4">
                <div class="text-gray-600">
                    Workflows: <button onclick="fetch('/workflow').then(r=>r.text()).then(t=>alert(t))" class="text-blue-600 hover:text-blue-800 font-medium">/workflow</button>
                </div>
                <div class="text-gray-600">
                    Queues: <button onclick="fetch('/queue').then(r=>r.text()).then(t=>alert(t))" class="text-blue-600 hover:text-blue-800 font-medium">/queue</button>
                </div>
            </div>

            <div class="space-y-6">
                <p class="text-gray-800 font-medium">To get started developing locally:</p>
                <ul class="space-y-4">
                    <li class="flex items-start">
                        <span class="flex-shrink-0 h-6 w-6 rounded-full bg-blue-100 flex items-center justify-center mr-3">
                            <span class="text-blue-600 text-sm font-medium">1</span>
                        </span>
                        <code class="bg-gray-100 px-3 py-1 rounded font-mono text-sm">go get github.com/dbos-inc/dbos-transact-go</code>
                    </li>
                    <li class="flex items-start">
                        <span class="flex-shrink-0 h-6 w-6 rounded-full bg-blue-100 flex items-center justify-center mr-3">
                            <span class="text-blue-600 text-sm font-medium">2</span>
                        </span>
                        <code class="bg-gray-100 px-3 py-1 rounded font-mono text-sm">git clone https://github.com/dbos-inc/dbos-demo-apps</code>
                    </li>
                    <li class="flex items-start">
                        <span class="flex-shrink-0 h-6 w-6 rounded-full bg-blue-100 flex items-center justify-center mr-3">
                            <span class="text-blue-600 text-sm font-medium">3</span>
                        </span>
                        Edit <code class="bg-gray-100 px-3 py-1 rounded font-mono text-sm">main.go</code> to start building!
                    </li>
                </ul>
            </div>

            <p class="text-gray-600">
                Check out the
                <a href="https://docs.dbos.dev/go/programming-guide" target="_blank" class="text-blue-600 hover:text-blue-800 hover:underline">
                    programming guide
                </a>
                to learn how to build with DBOS!
            </p>
        </div>
    </div>
</body>
</html>
    `)
}