<div align="center">

[![Go Reference](https://pkg.go.dev/badge/github.com/dbos-inc/dbos-transact-golang.svg)](https://pkg.go.dev/github.com/dbos-inc/dbos-transact-golang)
[![Go Report Card](https://goreportcard.com/badge/github.com/dbos-inc/dbos-transact-golang)](https://goreportcard.com/report/github.com/dbos-inc/dbos-transact-golang)
[![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/dbos-inc/dbos-transact-golang?sort=semver)](https://github.com/dbos-inc/dbos-transact-golang/releases)
[![Join Discord](https://img.shields.io/badge/Discord-Join%20Chat-5865F2?logo=discord&logoColor=white)](https://discord.com/invite/jsmC6pXGgX)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![GitHub Stars](https://img.shields.io/github/stars/dbos-inc/dbos-transact-golang?style=social)](https://github.com/dbos-inc/dbos-transact-golang)


# DBOS Transact: Lightweight Durable Workflow Orchestration with Postgres

#### [Documentation](https://docs.dbos.dev/) &nbsp;&nbsp;•&nbsp;&nbsp;  [Examples](https://docs.dbos.dev/examples) &nbsp;&nbsp;•&nbsp;&nbsp; [Github](https://github.com/dbos-inc)
</div>

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
    "time"

    "github.com/dbos-inc/dbos-transact-golang/dbos"
)

func workflow(dbosCtx dbos.DBOSContext, _ string) (string, error) {
    _, err := dbos.RunAsStep(dbosCtx, stepOne)
    if err != nil {
        return "", err
    }
    return dbos.RunAsStep(dbosCtx, stepTwo)
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
    ctx, err := dbos.NewDBOSContext(context.Background(), dbos.Config{
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
    defer ctx.Shutdown(2 * time.Second)

    // Run a durable workflow and get its result
    handle, err := dbos.RunWorkflow(ctx, workflow, "")
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
    "context"
    "fmt"
    "os"
    "time"

    "github.com/dbos-inc/dbos-transact-golang/dbos"
)

func task(ctx dbos.DBOSContext, i int) (int, error) {
    dbos.Sleep(ctx, 5*time.Second)
    fmt.Printf("Task %d completed\n", i)
    return i, nil
}

func main() {
    // Initialize a DBOS context
    ctx, err := dbos.NewDBOSContext(context.Background(), dbos.Config{
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
    defer ctx.Shutdown(2 * time.Second)

    // Enqueue tasks and gather results
    fmt.Println("Enqueuing workflows")
    handles := make([]dbos.WorkflowHandle[int], 10)
    for i := range 10 {
        handle, err := dbos.RunWorkflow(ctx, task, i, dbos.WithQueue(queue.Name))
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
_, err := dbos.RunWorkflow(ctx, task, i, dbos.WithWorkflowID(exactlyOnceEventID))
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
    dbos.Sleep(ctx, duration)
    return fmt.Sprintf("Workflow slept for %s", duration), nil
}

handle, err := dbos.RunWorkflow(dbosCtx, workflow, time.Second*5)
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
recvHandle, err := dbos.RunWorkflow(dbosCtx, receiveWorkflow, "topic", dbos.WithWorkflowID("receiverID"))

// Send a message
sendHandle, err := dbos.RunWorkflow(dbosCtx, sendWorkflow, "hola!")
_, err = sendHandle.GetResult()

// Eventually get the response
recvResult, err := recvHandle.GetResult()
```

</details>

## Getting Started

To get started, follow the [quickstart](https://docs.dbos.dev/quickstart) to install this open-source library and connect it to a Postgres database.
Then, check out the [programming guide](https://docs.dbos.dev/python/programming-guide) to learn how to build with durable workflows and queues.

## Documentation

[https://docs.dbos.dev](https://docs.dbos.dev)

## Examples

[https://docs.dbos.dev/examples](https://docs.dbos.dev/examples)

## DBOS vs. Other Systems

<details><summary><strong>DBOS vs. Temporal</strong></summary>

####

Both DBOS and Temporal provide durable execution, but DBOS is implemented in a lightweight Postgres-backed library whereas Temporal is implemented in an externally orchestrated server.

You can add DBOS to your program by installing this open-source library, connecting it to Postgres, and annotating workflows and steps.
By contrast, to add Temporal to your program, you must rearchitect your program to move your workflows and steps (activities) to a Temporal worker, configure a Temporal server to orchestrate those workflows, and access your workflows only through a Temporal client.
[This blog post](https://www.dbos.dev/blog/durable-execution-coding-comparison) makes the comparison in more detail.

**When to use DBOS:** You need to add durable workflows to your applications with minimal rearchitecting, or you are using Postgres.

**When to use Temporal:** You don't want to add Postgres to your stack, or you need a language DBOS doesn't support yet.

</details>

<details><summary><strong>DBOS vs. Airflow</strong></summary>

####

DBOS and Airflow both provide workflow abstractions.
Airflow is targeted at data science use cases, providing many out-of-the-box connectors but requiring workflows be written as explicit DAGs and externally orchestrating them from an Airflow cluster.
Airflow is designed for batch operations and does not provide good performance for streaming or real-time use cases.
DBOS is general-purpose, but is often used for data pipelines, allowing developers to write workflows as code and requiring no infrastructure except Postgres.

**When to use DBOS:** You need the flexibility of writing workflows as code, or you need higher performance than Airflow is capable of (particularly for streaming or real-time use cases).

**When to use Airflow:** You need Airflow's ecosystem of connectors.

</details>

<details><summary><strong>DBOS vs. Celery/BullMQ</strong></summary>

####

DBOS provides a similar queue abstraction to dedicated queueing systems like Celery or BullMQ: you can declare queues, submit tasks to them, and control their flow with concurrency limits, rate limits, timeouts, prioritization, etc.
However, DBOS queues are **durable and Postgres-backed** and integrate with durable workflows.
For example, in DBOS you can write a durable workflow that enqueues a thousand tasks and waits for their results.
DBOS checkpoints the workflow and each of its tasks in Postgres, guaranteeing that even if failures or interruptions occur, the tasks will complete and the workflow will collect their results.
By contrast, Celery/BullMQ are Redis-backed and don't provide workflows, so they provide fewer guarantees but better performance.

**When to use DBOS:** You need the reliability of enqueueing tasks from durable workflows.

**When to use Celery/BullMQ**: You don't need durability, or you need very high throughput beyond what your Postgres server can support.
</details>