<div align="center">

# DBOS Transact: Lightweight Durable Workflows

#### [Documentation](https://docs.dbos.dev/) &nbsp;&nbsp;•&nbsp;&nbsp;  [Examples](https://docs.dbos.dev/examples) &nbsp;&nbsp;•&nbsp;&nbsp; [Github](https://github.com/dbos-inc) &nbsp;&nbsp;•&nbsp;&nbsp; [Discord](https://discord.com/invite/jsmC6pXGgX)
</div>

---

## What is DBOS?

DBOS provides lightweight durable workflows on top of Postgres. Instead of managing your own workflow orchestrator or task queue system, you can use DBOS to add durable workflows and queues to your program in just a few lines of code.


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
var (
	wf = dbos.WithWorkflow(workflow)
)

func workflow(ctx context.Context, _ string) (string, error) {
	_, err := dbos.RunAsStep(ctx, step1, "")
	if err != nil {
		return "", err
	}
	return dbos.RunAsStep(ctx, step2, "")
}

func step1(ctx context.Context, _ string) (string, error) {
	fmt.Println("Executing step 1")
	return "Step 1 completed", nil
}

func step2(ctx context.Context, _ string) (string, error) {
	fmt.Println("Executing step 2")
	return "Step 2 completed - Workflow finished successfully", nil
}

func main() {
	err := dbos.Launch()
	if err != nil {
		panic(err)
	}
	defer dbos.Shutdown()

    wf(context.Background(), "hello DBOS")
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
var (
	queue  = dbos.NewWorkflowQueue("example-queue")
	taskWf = dbos.WithWorkflow(task)
)

func task(ctx context.Context, i int) (int, error) {
	time.Sleep(5 * time.Second)
	fmt.Printf("Task %d completed\n", i)
	return i, nil
}

func main() {
	err := dbos.Launch()
	if err != nil {
		panic(err)
	}
	defer dbos.Shutdown()

	fmt.Println("Enqueuing workflows")
	handles := make([]dbos.WorkflowHandle[int], 10)
	for i := range 10 {
		handle, err := taskWf(ctx, i, dbos.WithQueue(queue.Name))
		if err != nil {
			return "", fmt.Errorf("failed to enqueue step %d: %w", i, err)
		}
		handles[i] = handle
	}
	results := make([]int, 10)
	for i, handle := range handles {
		result, err := handle.GetResult(ctx)
		if err != nil {
			return "", fmt.Errorf("failed to get result for step %d: %w", i, err)
		}
		results[i] = result
	}
	fmt.Printf("Successfully completed %d steps\n", len(results)), nil
}
```
</details>