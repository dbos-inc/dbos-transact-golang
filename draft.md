
Golang Native Durable Workflows
Introduction

We built a Golang-native Durable Workflow library. Our goal was to make it as simple and idiosyncratic as possible while providing state-of-the-art workflow orchestration capabilities. That meant embracing the best of Go (e.g., context.Context) while working around its limits (e.g., lack of variadic generics). Getting the interface right was challenging. This post tells the tale. In particular, we’ll look at:

How we encapsulated workflow orchestration state in a single interface while providing compile-time type safety to workflow and step functions.
How we augmented Go native context interface to implement durable timeouts and propagate workflow metadata
A Durable Program
Here is a first look at a program using Durable Context, the central piece of the library. This program initializes the context, registers and executes a workflow:

// A workflow with one step.
func ExampleWorkflow(ctx durable.Context, _ string) (string, error) {
  return durable.RunAsStep(ctx, step)
}

func step(ctx context.Context) (string, error) {
  return "Step completed", nil
}

func main() {
  // Create a Durable Context.
  ctx, _ := durable.NewContext(context.Background(), durable.Config{
    DatabaseURL: os.Getenv("SYSTEM_DATABASE_URL"),
    AppName: "go-starter",
  })

  // Register a workflow.
  durable.RegisterWorkflow(ctx, ExampleWorkflow)

  // Launch the Context
  durable.Launch(ctx)
  defer durable.Shutdown(ctx, 10 * time.Second)

  // Run a workflow and wait for the result
  handle, _ := durable.RunWorkflow(ctx, ExampleWorkflow, "some-input")
  res, _ := handle.GetResult()
}
Durable Context

Durable Context is the central piece of the library. It encapsulates orchestration state (e.g., workflow registry, workflow scheduler) and is used to propagate orchestration metadata during workflow execution. 

When designing Context, we had four goals:
Single interface: Context should be the only interface the user has to understand.
Single root: one root Context manages everything.
Feels like Go: Context should feel like a normal Go context.Context.
Type-safety: Workflows and steps should get compile-time checking.

The challenge is that #4 is at odds with #1 and #2. If you want type-safe workflow calls, you need generics. But if your interface is generic, you end up with one Context per signature. That breaks goal #2, the “single root” design. One tempting solution was to introduce Workflow and Step generic interfaces, but this would fail #1.

Let’s take a concrete example: if have have this workflow signature:

func ExampleWorkflow(ctx durable.Context, input MyInputType) (MyReturnType, error)

We want the build to fail, with a clear error, if you write:

durable.RunWorkflow(ctx, ExampleWorkflow, WrongType{})
Generic package functions
Our solution is to expose generic package functions mirroring the Context methods, handling type conversions at runtime and calling typeless Context methods under the hood. In the case of RunAsStep:

func RunAsStep[R any](ctx Context, fn Step[R], opts ...StepOption) (R, error) {
  typeErasedFn := StepFunc(func(ctx context.Context) (any, error) { return fn(ctx) })
  result, err := ctx.RunAsStep(ctx, typeErasedFn, opts...)
  return result.(R), err
}

You are probably noticing that Context.RunAsStep takes a Context as the first argument. This is to help with another aspect of the UX, very important to Golang users: mocking. If I write a program that does:

durable.RunWorkflow(ctx, ExampleWorkflow, "some-input")

And I want to mock ctx, let’s say, with mockery, I want to be able to do mockCtx.On(“RunWorkflow”, arg1, arg2, arg3), because that’s how my program reads: 3 arguments to RunWorkflow.
Workflows and Steps Signature
The ideal workflow and step signature, for our use case (wrapping user provided functions with durability logic) would have been:

type Workflow[P any, R any] func(ctx Context, input ...P) (...R, error). 

But Go does not support variadic generics (the topic of long conversations with the community). Instead, we settled on flexible signatures that should feel familiar to Go developers:

type Workflow[P any, R any] func(ctx Context, input P) (R, error)
type Step[R any] func(ctx context.Context) (R, error)

Many Go functions use a context as the first parameter and return an error. The input and output values can be struct with an arbitrary amount of parameters. Steps are flexible and allow users to bring existing functions, provide them with compile-time type checking for return values, and allow our library to propagate Context. For example, you can do:

durable.RunAsStep(durableCtx, func(ctx context.Context) (MyType, error) {
  return myExistingFunction(ctx, arg1, arg2)
})

We considered a few alternatives signatures. One of them was very flexible in terms of what users can bring:

func RunWorkflow(ctx durable.Context, workflowFunc any, args ...any) (...any, error)

But this would have lost compile-time type checking (failing goal #4) and requires overly complex usage of reflection, which is both error prone and super complicated. Is workflowFunc a function? Are args matching the signature of workflowFunc?
Serialization
Durable workflows must have their input/output stored in the database. We decided to encode these with gob, the native Go solution to (de)serialize go objects. Compared to JSON encoding - the main alternative - gob is faster, produces smaller payloads, and has rich Go type support.

For a type to be gob-encodable, it must have been previously registered. Our package functions can do that automatically for users. For example, during workflow registration:

func RegisterWorkflow[P any, R any](ctx Context, fn Workflow[P, R], opts ...WorkflowRegistrationOption) {

  var p P
  var r R
  gob.Register(p)
  gob.Register(r)
Leveraging context.Context
Inside RunWorkflow and RunAsStep, we use Context to seed orchestration metadata. We also use the native context.Context Deadline and cancellation mechanisms to implement durable workflow timeouts.

Before executing the user-provided workflow function, RunWorkflow creates a new child Context, seeded with metadata for the current workflow, including the new workflow ID and a deterministically incremented step counter. This new context is passed as the first argument of the workflow. RunAsStep performs a similar logic (and increments the counter).

wfState := &workflowState{
	workflowID: workflowID,
	stepID:     -1, 
}
workflowCtx := WithValue(c, workflowStateKey, wfState)

When a workflow is called within a workflow, RunWorkflow can detect this by introspecting its provided Context and manage the state accordingly, recording the new workflow as a child workflow.
Workflow execution
After performing the orchestration work, RunWorkflow starts the user provided function in a goroutine, and returns a WorkflowHandle to the user. The handle wraps a channel that receives the outcome of the workflow function and exposes it through a GetResult method.

outcomeChan := make(chan workflowOutcome[any], 1)
go func() {
  var result any
  var err error

  result, err = fn(workflowCtx, input)
  c.systemDB.updateWorkflowOutcome(uncancellableCtx, updateWorkflowOutcomeDBInput{
    workflowID: workflowID,
    status:     status,
    err:        err,
    output:     result,
  })
  outcomeChan <- workflowOutcome[any]{result: result, err: err}
  close(outcomeChan)
}()
return newWorkflowHandle(uncancellableCtx, workflowID, outcomeChan), nil

Note that Context.RunWorkflow returns a typeless workflowOutcome[any] channel. The package RunWorkflow pipes the results to a generic channel, such that end-user programs can get compile-time type checking when consuming the result.
Deadlines and cancellation
One compelling reason for us to design Context around Go native context.Context was to leverage its deadline and cancellation facilities. If RunWorkflow detects a deadline in its provided Context, using the Deadline() method, it registers an AfterFunc with the workflow context.

var stopFunc func() bool
cancelFuncCompleted := make(chan struct{})
if !durableDeadline.IsZero() {
workflowCtx, _ = WithTimeout(workflowCtx, time.Until(durableDeadline))
	// Trigger durable workflow cancel as soon as the context is cancelled
	workflowCancelFunction := func() {
		c.systemDB.cancelWorkflow(uncancellableCtx, workflowID)
		close(cancelFuncCompleted)
	}
	stopFunc = context.AfterFunc(workflowCtx, workflowCancelFunction)
}

AfterFunc will run in its own goroutine and be triggered whenever the workflow context is cancelled. But remember that cancellation is not preemption: cancelling a Go context is just a way to signal that work done on the context’s behalf should stop. Workflow and step functions can access the context’s Done() channel and act upon it. We do not attempt to preempt them.

Thanks to the stopFunc returned by AfterFunc, we can learn whether the workflow was cancelled, when the function returns.
Learn more
If you like Go, we’d love to hear from you. At DBOS, our goal is to make durable workflows as lightweight and easy to work with as possible. Check it out:
Quickstart: https://docs.dbos.dev/quickstart 
GitHub: https://github.com/dbos-inc


