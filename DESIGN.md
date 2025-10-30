

# DBOS package

The dbos package is named github.com/dbos-inc/dbos-transact-go and must be imported as github.com/dbos-inc/dbos-transact-go/dbos

DBOS requires users to declare their workflows and queues as package variables, such that they can be registered in internal registries before anything else happens.

In their main function, a user should call dbos.Launch() to initialize DBOS. Launch is idempotent. Shutdown() can be called to reset that idempotent state. Shutdown will terminate all resources creates by DBOS at Launch.

Launch does a couple of things:
- Compute the version of your application, either using an env var or hashing the content of your registered workflows.
- Determine the process ID (executor ID)
- Initialize a _system database_, i.e., the subsystem responsible for managing workflow states in the DBOS persistence stores (postgres)
- Starts a queue runner goroutine. This goroutine is responsible for monitoring and dequeueing tasks from each registered queue
- Start a workflow scheduler, which, under the hood, starts scheduled workflows in their own goroutine at a desired interval
- Runs as round of _recovery_ to, well, recover pending workflows for the current process at startup

Shutting down DBOS will cleanup all DBOS resources
- queue runner
- workflow scheduler
- system database

# Registering worklows

Uses the WithWorkflow function. A workflow must have the following signature: type WorkflowFunc[P any, R any] func(ctx context.Context, input P) (R, error)
This means:
- One input and one output type of your choice, in addition to a return error
- No "any" input or output types
- For encoding to happen, user defined types must have at least one exported field, throughout the type hierarchy. This means that

Invalid / invalid pairs

NOK
type workflowInput struct {
    a int
}

OK
type workflowInput struct {
    A int
    b int
}


NOK
type workflowInput struct {
    A mystruct
}

type mystruct struct {
    a int
}

OK
type workflowInput struct {
    A mystruct
}

type mystruct struct {
    A int
    b int
}

Note that unexported fields won't be available when doing GetResult() -- because they'll be ignored by encoding/gob during encoding

Under the hood, with workflow:
- Registers the input/output types with encoding/gob for sez/dez
- Register a cron job if needed
- Returns a wrapped handler that will run the workflow durably and accept runtime options

During registration, a workflow can be configured to:
- Have a maximum amount of retries (when exhausted, the workflow lands in a DLQ)
- Be a scheduled workflow and execute at a specified interval


Registration is needed for recovery, so an automated restart of a DBOS application can find the correct functions to invoke again for durable execution.
Note that functions stored in the registry are typeless, i.e., their input/output values are typed "any". This is because golang maps cannot be generic.
Functions stored in the registry are called at two sites: recovery and queue runner. These do not involve the programmer, so compile time type safety is not required.
We do check input types at runtime in the wrapped function
Because of the nature of the wrapper, i.e., it returns a type erased polling handler, acquiring the result is in the hands of the programmer, and we cannot do runtime check for them.
We can entice them to do it if needed and/or provide them with a helper function?

# Durable workflows

What makes a function a durable workflow is the logic in runAsWorkflow:
- Persist in the database every attempt to start / enqueue a workflow. This is somewhat idempotent, and only the workflow updated time is updated everytime
- If the previous step found the workflow already completed, it'll return a _polling handle_ the user can use to get the results.
- Run the user provided function inside a goroutine. Store the outcome of the function in the database, updating the workflow invokation status.
- Return a handle to the user, which they can use to retrieve their function outcome from a channel (all handled under the hood)

# Child workflows
workflows can be at most 1 step - deep (we don't allow nested Steps). To increase the depth, one can program workflows to start other workflows.
When a workflow calls a child more than once in its code, it'll get a polling handler the second time.

# Steps

DBOS workflows can run durable steps. A durable step has its input/output recorded to Postgres. DBOS will reuse an existing result from postgres when re-running a step.
Steps have an internal ID (step ID), which is used to identify their ordering within a workflow.
A step can only be ran within a workflow, and a step ran inside another step reuse the context of the parent step.
DBOS can automatically retry a step, using an exponential backoff policy, if the user sets a max attempt value (defaults to 1)


# Workflow queues
Durable queues are one of the most powerful of DBOS. They let you distribute and scale work reliably.

Queues can be tuned for a certain amount of per-process (worker) and global concurrency. These knobs will help you scale how much work can work across processes. A queue can also be configured with a rate limiter.

- dedup
- priority

To enqueue a workflow, call it with `WithQueue(queueName)`.


# Workflow recovery
