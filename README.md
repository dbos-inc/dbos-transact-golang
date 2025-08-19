<div align="center">

[![Go Reference](https://pkg.go.dev/badge/github.com/dbos-inc/dbos-transact-go.svg)](https://pkg.go.dev/github.com/dbos-inc/dbos-transact-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/dbos-inc/dbos-transact-go)](https://goreportcard.com/report/github.com/dbos-inc/dbos-transact-go)
[![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/dbos-inc/dbos-transact-go?sort=semver)](https://github.com/dbos-inc/dbos-transact-go/releases)
[![Join Discord](https://img.shields.io/badge/Discord-Join%20Chat-5865F2?logo=discord&logoColor=white)](https://discord.com/invite/jsmC6pXGgX)


# DBOS Transact: Lightweight Durable Workflow Orchestration with Postgres

#### [Documentation](https://docs.dbos.dev/) &nbsp;&nbsp;•&nbsp;&nbsp;  [Examples](https://docs.dbos.dev/examples) &nbsp;&nbsp;•&nbsp;&nbsp; [Github](https://github.com/dbos-inc)
</div>

#### This Golang version of DBOS Transact is in Alpha!
For production ready Transacts, check our [Python](https://github.com/dbos-inc/dbos-transact-py) and [TypeScript](https://github.com/dbos-inc/dbos-transact-ts) versions.

---

## What is DBOS?

DBOS provides lightweight durable workflow orchestration on top of Postgres. Instead of managing your own workflow orchestrator or task queue system, you can use DBOS to add durable workflows and queues to your program in just a few lines of code.


## When Should I Use DBOS?

You should consider using DBOS if your application needs to **reliably handle failures**.
For example, you might be building a payments service that must reliably process transactions even if servers crash mid-operation, or a long-running data pipeline that needs to resume seamlessly from checkpoints rather than restart from the beginning when interrupted.

Handling failures is costly and complicated, requiring complex state management and recovery logic as well as heavyweight tools like external orchestration services.
DBOS makes it simpler: annotate your code to checkpoint it in Postgres and automatically recover from any failure.
DBOS also provides powerful Postgres-backed primitives that makes it easier to write and operate reliable code, including durable queues, notifications, scheduling, event processing, and programmatic workflow management.


## Features
<details open><summary><strong>💾 Durable Workflows</strong></summary>
 
DBOS workflows make your program **durable** by checkpointing its state in Postgres.
If your program ever fails, when it restarts all your workflows will automatically resume from the last completed step.

You add durable workflows to your existing Golang program by registering ordinary functions as workflows or running them as steps:

```golang
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/dbos-inc/dbos-transact-go/dbos"
)

func workflow(dbosCtx dbos.DBOSContext, _ string) (string, error) {
	_, err := dbos.RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
		return stepOne(ctx)
	})
	if err != nil {
		return "", err
	}
	return dbos.RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
		return stepTwo(ctx)
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
func main() {
    // Initialize a DBOS context
	ctx, err := dbos.NewDBOSContext(dbos.Config{
		DatabaseURL: os.Getenv("DBOS_SYSTEM_DATABASE_URL"),
		AppName:     "myapp",
	})
	if err != nil {
		panic(err)
	}

    // Register a workflow
	dbos.RegisterWorkflow(ctx, workflow)

    // Launch DBOS
	err = ctx.Launch()
	if err != nil {
		panic(err)
	}
	defer ctx.Cancel()

    // Run a durable workflow and get its result
	handle, err := dbos.RunAsWorkflow(ctx, workflow, "")
	if err != nil {
		panic(err)
	}
	res, err := handle.GetResult()
	if err != nil {
		panic(err)
	}
	fmt.Println("Workflow result:", res)
}
```


Workflows are particularly useful for 

- Orchestrating business processes so they seamlessly recover from any failure.
- Building observable and fault-tolerant data pipelines.
- Operating an AI agent, or any application that relies on unreliable or non-deterministic APIs.

</details>

<details><summary><strong>📒 Durable Queues</strong></summary>

####

DBOS queues help you **durably** run tasks in the background.
When you enqueue a workflow, one of your processes will pick it up for execution.
DBOS manages the execution of your tasks: it guarantees that tasks complete, and that their callers get their results without needing to resubmit them, even if your application is interrupted.

Queues also provide flow control, so you can limit the concurrency of your tasks on a per-queue or per-process basis.
You can also set timeouts for tasks, rate limit how often queued tasks are executed, deduplicate tasks, or prioritize tasks.

You can add queues to your workflows in just a couple lines of code.
They don't require a separate queueing service or message broker&mdash;just Postgres.

```golang
package main

import (
    "fmt"
    "os"
    "time"

    "github.com/dbos-inc/dbos-transact-go/dbos"
)

func task(ctx dbos.DBOSContext, i int) (int, error) {
    ctx.Sleep(5 * time.Second)
    fmt.Printf("Task %d completed\n", i)
    return i, nil
}

func main() {
    // Initialize a DBOS context
    ctx, err := dbos.NewDBOSContext(dbos.Config{
        DatabaseURL: os.Getenv("DBOS_SYSTEM_DATABASE_URL"),
        AppName:     "myapp",
    })
    if err != nil {
        panic(err)
    }

    // Register the workflow and create a durable queue
    dbos.RegisterWorkflow(ctx, task)
    queue := dbos.NewWorkflowQueue(ctx, "queue")

    // Launch DBOS
    err = ctx.Launch()
    if err != nil {
        panic(err)
    }
    defer ctx.Cancel()

    // Enqueue tasks and gather results
    fmt.Println("Enqueuing workflows")
    handles := make([]dbos.WorkflowHandle[int], 10)
    for i := range 10 {
        handle, err := dbos.RunAsWorkflow(ctx, task, i, dbos.WithQueue(queue.Name))
        if err != nil {
            panic(fmt.Sprintf("failed to enqueue step %d: %v", i, err))
        }
        handles[i] = handle
    }
    results := make([]int, 10)
    for i, handle := range handles {
        result, err := handle.GetResult()
        if err != nil {
            panic(fmt.Sprintf("failed to get result for step %d: %v", i, err))
        }
        results[i] = result
    }
    fmt.Printf("Successfully completed %d steps\n", len(results))
}
```
</details>

<details><summary><strong>🎫 Exactly-Once Event Processing</strong></summary>

####

Use DBOS to build reliable webhooks, event listeners, or Kafka consumers by starting a workflow exactly-once in response to an event.
Acknowledge the event immediately while reliably processing it in the background.

For example:

```golang
_, err := dbos.RunAsWorkflow(ctx, task, i, dbos.WithWorkflowID(exactlyOnceEventID))
```
</details>

<details><summary><strong>📅 Durable Scheduling</strong></summary>

####

Schedule workflows using cron syntax, or use durable sleep to pause workflows for as long as you like (even days or weeks) before executing.

```golang
dbos.RegisterWorkflow(dbosCtx, func(ctx dbos.DBOSContext, scheduledTime time.Time) (string, error) {
    return fmt.Sprintf("Workflow executed at %s", scheduledTime), nil
}, dbos.WithSchedule("* * * * * *")) // Every second
```

You can add a durable sleep to any workflow with a single line of code.
It stores its wakeup time in Postgres so the workflow sleeps through any interruption or restart, then always resumes on schedule.

```golang
func workflow(ctx dbos.DBOSContext, duration time.Duration) (string, error) {
    ctx.Sleep(duration)
    return fmt.Sprintf("Workflow slept for %s", duration), nil
}

handle, err := dbos.RunAsWorkflow(dbosCtx, workflow, time.Second*5)
_, err = handle.GetResult()
```

</details>

<details><summary><strong>📫 Durable Notifications</strong></summary>

####

Pause your workflow executions until a notification is received, or emit events from your workflow to send progress updates to external clients.
All notifications are stored in Postgres, so they can be sent and received with exactly-once semantics.
Set durable timeouts when waiting for events, so you can wait for as long as you like (even days or weeks) through interruptions or restarts, then resume once a notification arrives or the timeout is reached.

For example, build a reliable billing workflow that durably waits for a notification from a payments service, processing it exactly-once:

```golang
func sendWorkflow(ctx dbos.DBOSContext, message string) (string, error) {
    err := dbos.Send(ctx, "receiverID", message, "topic")
    return "sent", err
}

func receiveWorkflow(ctx dbos.DBOSContext, topic string) (string, error) {
    return dbos.Recv[string](ctx, topic, 48 * time.Hour)
}

// Start a receiver in the background
recvHandle, err := dbos.RunAsWorkflow(dbosCtx, receiveWorkflow, "topic", dbos.WithWorkflowID("receiverID"))

// Send a message
sendHandle, err := dbos.RunAsWorkflow(dbosCtx, sendWorkflow, "hola!")
_, err = sendHandle.GetResult()

// Eventually get the response
recvResult, err := recvHandle.GetResult()
```

</details>


## DBOS workflows

A workflow can be any function with the following signature:
```golang
type GenericWorkflowFunc[P any, R any] func(ctx context.Context, input P) (R, error)
```

To register a workflow call `dbos.RegisterWorkflow(dbosCtx, workflow)` after having initialized a DBOS Context. Workflows can only be registered before DBOS is launched.


Workflows can run steps, which can be any function with the following signature:
```golang
type GenericStepFunc[R any] func(ctx context.Context) (R, error)
```

To run a step within a workflow, use `RunAsStep`. Importantly, you must pass to `RunAsStep` the context received in the workflow function (see examples above.)

The input and output of workflows and steps are memoized in your Postgres database for workflow recovery. Under the hood, DBOS uses the [encoding/gob](https://pkg.go.dev/encoding/gob) package for serialization (this means that only exported fields will be memoized and types without exported fields will generate an error.)

## Getting started

Install the DBOS Transact package in your program:

```shell
go get github.com/dbos-inc/dbos-transact-go
```

You can store and export a Postgres connection string in the `DBOS_SYSTEM_DATABASE_URL` environment variable for DBOS to manage your workflows state. By default, DBOS will use `postgres://postgres:${PGPASSWORD}@localhost:5432/dbos?sslmode=disable`.


## ⭐️ Like this project?

[Star it on GitHub](https://github.com/dbos-inc/dbos-transact-go)  
[![GitHub Stars](https://img.shields.io/github/stars/dbos-inc/dbos-transact-go?style=social)](https://github.com/dbos-inc/dbos-transact-go)
