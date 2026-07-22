# DBOS Transact Go — v1.0.0-rc.1 Migration Guide

This guide covers migrating an application from the last v0.x release to v1.0.0-rc.1.

---

## 1. Core renames: `DBOSContext` → `Context`

The central interface was renamed. This is the highest-volume change and is purely mechanical:

| v0 | v1 |
|---|---|
| `dbos.DBOSContext` | `dbos.Context` |
| `dbos.NewDBOSContext(ctx, cfg)` | `dbos.NewContext(ctx, cfg)` |
| `Config.SqliteSystemDB` | `Config.SQLiteSystemDB` |

Workflow and step signatures change accordingly:

```go
// v0
func myWorkflow(ctx dbos.DBOSContext, input string) (string, error)
// v1
func myWorkflow(ctx dbos.Context, input string) (string, error)
```

`Shutdown` now returns an error (non-nil if the timeout expired before all resources stopped). Use the package-level functions for launch and shutdown — like the other interface methods, `Shutdown` now takes a leading `Client` receiver-argument, so method-form calls must be migrated:

```go
// v0
ctx.Launch()
ctx.Shutdown(30 * time.Second)
// v1
err := dbos.Launch(ctx)
err := dbos.Shutdown(ctx, 30*time.Second)   // accepts Client or Context
```

Behavioral (no code change): `Launch()` now recovers this executor's PENDING workflows *before* starting the queue runner and scheduler.

Behavioral (no code change): a failed `Launch()` is now terminal — the context is torn down (system database closed) and cannot be relaunched. Code that retried `Launch` on the same context after a transient failure must create a fresh context with `NewContext` for each attempt.

## 2. Client redesign — `Client` is now a subset of `Context`

`Client` is no longer a separate wrapper type with its own method shapes. It is a formal sub-interface of `Context` (both embed `context.Context`), owning every operation that doesn't require `Launch()`: enqueue, send/get-event/read-stream, workflow management, queue management, schedule management, app-version management, `Shutdown`. Every `Context` **is** a `Client`, so a launched `Context` can be passed anywhere a `Client` is accepted.

- `dbos.NewClient(ctx, dbos.ClientConfig{...})` still exists and returns a `Client`. `ClientConfig` gained `SystemDBStartupTimeout` and renamed `SqliteSystemDB` → `SQLiteSystemDB`.
- Client interface methods now take a leading `Client` receiver-argument like `Context` methods always did (`c.ListWorkflows(c, ...)`). In practice, always use the package-level generic functions instead.

### The `Client*` generic helpers are gone

The duplicated client-side generics were removed. The single package-level generic functions now accept a `Client` (and therefore also a `Context`):

| v0 (client) | v1 (unified) |
|---|---|
| `dbos.ClientRetrieveWorkflow[R](client, id)` | `dbos.RetrieveWorkflow[R](client, id)` |
| `dbos.ClientGetEvent[R](client, wfid, key, timeout)` | `dbos.GetEvent[R](client, wfid, key, timeout)` |
| `dbos.ClientReadStream[R](client, wfid, key)` | `dbos.ReadStream[R](client, wfid, key)` |
| `dbos.ClientReadStreamAsync[R](client, wfid, key)` | `dbos.ReadStreamAsync[R](client, wfid, key)` |
| `dbos.ClientResumeWorkflow[R](client, id)` | `dbos.ResumeWorkflow[R](client, id)` |
| `dbos.ClientResumeWorkflows[R](client, ids)` | `dbos.ResumeWorkflows[R](client, ids)` |
| `dbos.ClientForkWorkflow[R](client, input)` | `dbos.ForkWorkflow[R](client, input)` |
| `dbos.ClientForkWorkflows[R](client, input)` | `dbos.ForkWorkflows[R](client, input)` |
| `dbos.ClientTriggerSchedule[R](client, name)` | `dbos.TriggerSchedule[R](client, name)` |
| `client.Enqueue(queue, wf, input)` (method) | `dbos.Enqueue[R](client, queue, wf, input)` |

### `Enqueue` type parameters reordered

`Enqueue[P, R]` became `Enqueue[R, P]` so the input type is inferred and you name only the result type:

```go
// v0
handle, err := dbos.Enqueue[string, int](client, "queue", "MyWorkflow", "input")
// v1
handle, err := dbos.Enqueue[int](client, "queue", "MyWorkflow", "input")
```

Same reorder applies to both debouncer types: `Debouncer[P, R]` / `NewDebouncer[P, R]` → `Debouncer[R, P]` / `NewDebouncer[R, P]`, and `DebouncerClient[P, R]` / `NewDebouncerClient[P, R]` → `DebouncerClient[R, P]` / `NewDebouncerClient[R, P]`. `NewDebouncer` call sites are unaffected (both parameters are inferred from the workflow function), but explicitly-typed variables must swap: `var d *dbos.Debouncer[string, int]` → `*dbos.Debouncer[int, string]`. `NewDebouncerClient` cannot infer its parameters — every call site must swap them by hand; watch for cases where input and result types are mutually assignable (e.g. both `string`), which compile unchanged with swapped meaning.

### Functions that widened from `Context` to `Client`

These package-level functions now take a `Client` first argument. Existing call sites passing a `Context` keep compiling — listed for completeness: `Send`, `GetEvent`, `ReadStream`, `ReadStreamAsync`, `RetrieveWorkflow`, `CancelWorkflow(s)`, `ResumeWorkflow(s)`, `ForkWorkflow(s)`, `DeleteWorkflows`, `SetWorkflowDelay`, `ListWorkflows`, `GetWorkflowSteps`, `GetWorkflowAggregates`, `GetStepAggregates`, all queue-management, all schedule-management, all app-version functions.

Still `Context`-only (require a launched runtime / workflow scope): `RunWorkflow`, `RunAsStep`, `RunAsTransaction`, `Go`, `Select`, `Recv`, `SetEvent`, `WriteStream`, `CloseStream`, `Sleep`, `Patch`, `DeprecatePatch`, `GetWorkflowID`, `GetStepID`, `RegisterWorkflow`, `ListenQueues`.

## 3. Queues

### `NewWorkflowQueue` removed → `RegisterQueue`

In-memory queues are gone; all queues are database-backed. The public `WorkflowQueue` struct was unexported; the `Queue` interface (`GetName()`, `GetGlobalConcurrency()`, `Set*` methods, …) is the public handle.

```go
// v0
queue := dbos.NewWorkflowQueue(ctx, "queue", dbos.WithWorkerConcurrency(5))   // before Launch only
// v1
queue, err := dbos.RegisterQueue(ctx, "queue", dbos.WithWorkerConcurrency(5)) // works before or after Launch, and from a Client
```

- `RegisterQueue` returns `(Queue, error)` — handle the error. This typically ripples outward: per-module registration helpers that were `func Register(ctx dbos.Context)` become error-returning, and every host binary that calls them needs the plumbing. Budget for that in apps with per-agent registration functions.
- `WithMaxTasksPerIteration` option removed (no replacement).
- Field access via struct (`queue.Name`) → interface getters (`queue.GetName()`).
- Queue `Set*` methods now take a `Client` (any `Context` works).
- Registering the reserved internal queue name is an error.

### `WithQueue` takes a `Queue`, not a name

```go
// v0
handle, err := dbos.RunWorkflow(ctx, task, i, dbos.WithQueue(queue.Name))
// v1
handle, err := dbos.RunWorkflow(ctx, task, i, dbos.WithQueue(queue))
```

Apps that only kept the queue *name* (a common v0 pattern, since `WithQueue` took a string) must now retain the `Queue` handle returned by `RegisterQueue` — typically in the package var where the name constant used to suffice. If only the name is at hand, fetch the handle first with `dbos.RetrieveQueue(ctx, name)` — which now returns an error matching `dbos.ErrQueueNotFound` when absent, instead of `(nil, nil)`. Update any `q == nil` existence checks:

```go
// v1
q, err := dbos.RetrieveQueue(ctx, name)
if errors.Is(err, dbos.ErrQueueNotFound) { /* absent */ }
```

### Listen set

- `ListenQueues(ctx, queues ...WorkflowQueue)` → `ListenQueues(ctx, names ...string)`. Each call **replaces** the whole set; empty set = listen to every queue.
- New: `ListenedQueues(ctx) []string`.
- `ListRegisteredQueues` (deprecated in v0) removed — use `ListQueues(ctx)`.
- `DeleteQueue` on a nonexistent queue is not an error.

## 4. Scheduled workflows

### `WithSchedule` registration option removed

Cron scheduling is no longer a registration-time option; schedules are database-backed rows created **after Launch** (or from a Client). The workflow must accept `dbos.ScheduledWorkflowInput`:

```go
// v0
dbos.RegisterWorkflow(ctx, func(ctx dbos.DBOSContext, scheduledTime time.Time) (string, error) {
    ...
}, dbos.WithSchedule("* * * * * *"))

// v1
reportWorkflow := func(ctx dbos.Context, input dbos.ScheduledWorkflowInput) (any, error) {
    // input.ScheduledTime is the cron tick time
    ...
}
dbos.RegisterWorkflow(ctx, reportWorkflow)
// ... after dbos.Launch(ctx):
err := dbos.CreateSchedule(ctx, dbos.ScheduleSpec{
    ScheduleName: "report-schedule",
    Schedule:     "* * * * * *",
    Workflow:     reportWorkflow,   // or WorkflowName: "..."
})
```

### One input struct: `ScheduleSpec`

`CreateScheduleRequest` + its functional options (`WithScheduleContext`, `WithAutomaticBackfill`, `WithCronTimezone`, `WithScheduleQueueName`, `WithScheduleWorkflowClassName`), `ApplySchedulesRequest`, and `ClientScheduleInput` all collapse into `ScheduleSpec`:

```go
type ScheduleSpec struct {
    ScheduleName      string // required
    Schedule          string // required, cron expression
    WorkflowName      string // required unless Workflow is set
    Workflow          any    // registered Go workflow fn (Context only; wins over WorkflowName)
    WorkflowClassName string
    Context           any    // serialized as JSON
    AutomaticBackfill bool
    CronTimezone      string
    QueueName         string // defaults to the internal queue
}
```

- `CreateSchedule(ctx, fn, req, opts...)` → `CreateSchedule(ctx, spec)`; `ApplySchedules` takes `[]ScheduleSpec`.
- Field rename: `ApplySchedulesRequest.WorkflowFn` → `ScheduleSpec.Workflow`. Renaming the *type* alone leaves `WorkflowFn:` behind at every schedule literal — rename the field too (included in the §10 sed list).
- `ScheduledWorkflowInput.Context` is now `json.RawMessage`; decode with the new `dbos.DecodeScheduleContext[T](input)`. *Creating* schedules is unaffected — `ScheduleSpec.Context` stays `any` and still takes your struct directly; the SDK serializes it. Only code that hand-constructs a `ScheduledWorkflowInput` (test harnesses, manual-trigger paths that invoke a scheduled workflow directly to simulate a tick) must now `json.Marshal` the context into the field.

> **Gob-serializer apps: drain scheduled firings before upgrading.** `ScheduledWorkflowInput.Context` changed from `any` to `json.RawMessage` under the same gob registration, and gob cannot decode the old interface-encoded field into the new concrete type. Any schedule firing still ENQUEUED (or otherwise not yet dequeued) at upgrade time in an app using `NewGobSerializer` will fail input decoding and error when it runs — those workflows do not make it through the upgrade. Let pending firings drain (or cancel them) before deploying v1. Apps on the default JSON serializer are unaffected.
- `GetSchedule` returns `(WorkflowSchedule, error)` by value (was `*WorkflowSchedule`), with `dbos.ErrScheduleNotFound` when absent.
- `TriggerSchedule` is now generic: `TriggerSchedule[R](ctx, name)`.

## 5. Errors

`DBOSError`/`DBOSErrorCode` renamed and sentinels introduced:

| v0 | v1 |
|---|---|
| `*dbos.DBOSError` | `*dbos.Error` |
| `dbos.DBOSErrorCode` | `dbos.ErrorCode` |
| `ConflictingIDError` | `ErrorCodeConflictingID` |
| `InitializationError` | `ErrorCodeInitialization` |
| `NonExistentWorkflowError` | `ErrorCodeNonExistentWorkflow` |
| `ConflictingWorkflowError` | `ErrorCodeUnexpectedWorkflow` (renamed concept) |
| `WorkflowCancelled` | `ErrorCodeWorkflowCancelled` |
| `UnexpectedStep` | `ErrorCodeUnexpectedStep` |
| `AwaitedWorkflowCancelled` | `ErrorCodeAwaitedWorkflowCancelled` |
| `ConflictingRegistrationError` | `ErrorCodeConflictingRegistration` |
| `WorkflowUnexpectedTypeError` | `ErrorCodeWorkflowUnexpectedType` |
| `WorkflowExecutionError` | `ErrorCodeWorkflowExecution` |
| `StepExecutionError` | `ErrorCodeStepExecution` |
| `DeadLetterQueueError` | `ErrorCodeDeadLetterQueue` |
| `MaxStepRetriesExceeded` | `ErrorCodeMaxStepRetriesExceeded` |
| `QueueDeduplicated` | `ErrorCodeQueueDeduplicated` |
| `PatchingNotEnabled` | `ErrorCodePatchingNotEnabled` |
| `TimeoutError` | `ErrorCodeTimeout` |
| `NoApplicationVersions` | `ErrorCodeNoApplicationVersions` |
| — (new) | `ErrorCodeQueueNotFound`, `ErrorCodeScheduleNotFound`, `ErrorCodeInvalidOption` |

Prefer the new sentinels with `errors.Is` over constructing an `&Error{Code: ...}` probe:

```go
// v0
if errors.Is(err, &dbos.DBOSError{Code: dbos.QueueDeduplicated}) { ... }
// v1
if errors.Is(err, dbos.ErrQueueDeduplicated) { ... }
```

Sentinels: `ErrWorkflowCancelled`, `ErrAwaitedWorkflowCancelled`, `ErrQueueDeduplicated`, `ErrNonExistentWorkflow`, `ErrConflictingWorkflowID`, `ErrUnexpectedWorkflow`, `ErrMaxStepRetriesExceeded`, `ErrDeadLetterQueue`, `ErrTimeout`, `ErrQueueNotFound`, `ErrScheduleNotFound`, `ErrNoApplicationVersions`, `ErrInvalidOption`. Timeout errors from an expired deadline also match `context.DeadlineExceeded`; cancellations carry a cause matching `context.Canceled`.

Type assertions on the error struct: `var e *dbos.DBOSError; errors.As(err, &e)` → `var e *dbos.Error; errors.As(err, &e)`.

## 6. Option renames

### `ListWorkflows` filters: `With*` → `WithFilter*` (several became variadic)

| v0 | v1 |
|---|---|
| `WithWorkflowIDs(ids []string)` | `WithFilterWorkflowIDs(ids ...string)` |
| `WithStatus(s []WorkflowStatusType)` | `WithFilterStatus(s ...WorkflowStatusType)` |
| `WithStartTime(t)` | `WithFilterCreatedAfter(t)` |
| `WithEndTime(t)` | `WithFilterCreatedBefore(t)` |
| `WithName(n ...string)` | `WithFilterName(n ...string)` |
| `WithAppVersion(v ...string)` | `WithFilterAppVersion(v ...string)` |
| `WithUser(u ...string)` | `WithFilterUser(u ...string)` |
| `WithLimit(n)` / `WithOffset(n)` | `WithFilterLimit(n)` / `WithFilterOffset(n)` |
| `WithSortDesc()` | `WithFilterSortDesc()` |
| `WithWorkflowIDPrefix(p ...string)` | `WithFilterWorkflowIDPrefix(p ...string)` |
| `WithLoadInput(b)` / `WithLoadOutput(b)` | `WithFilterLoadInput(b)` / `WithFilterLoadOutput(b)` |
| `WithQueueName(q ...string)` | `WithFilterQueueName(q ...string)` |
| `WithQueuesOnly()` | `WithFilterQueuesOnly()` |
| `WithExecutorIDs(ids []string)` | `WithFilterExecutorIDs(ids ...string)` |
| `WithForkedFrom(f ...string)` | `WithFilterForkedFrom(f ...string)` |
| `WithParentWorkflowID(p ...string)` | `WithFilterParentWorkflowID(p ...string)` |
| `WithCompletedAfter/Before(t)` | `WithFilterCompletedAfter/Before(t)` |
| `WithDequeuedAfter/Before(t)` | `WithFilterDequeuedAfter/Before(t)` |
| `WithWasForkedFrom(b)` | `WithFilterWasForkedFrom(b)` |
| `WithHasParent(b)` | `WithFilterHasParent(b)` |

Slice-taking filters became variadic — call sites passing a literal slice need `...` or unwrapping: `WithStatus([]dbos.WorkflowStatusType{dbos.WorkflowStatusSuccess})` → `WithFilterStatus(dbos.WorkflowStatusSuccess)`.

### Step retry options

| v0 | v1 |
|---|---|
| `WithBackoffFactor(f)` | `WithStepBackoffFactor(f)` |
| `WithBaseInterval(d)` | `WithStepBaseInterval(d)` |
| `WithMaxInterval(d)` | `WithStepMaxInterval(d)` |
| `WithRetryPredicate(fn)` | `WithStepRetryPredicate(fn)` |
| — (new) | `WithTxIsolation(level IsoLevel)` |

### Other option/function renames

| v0 | v1 |
|---|---|
| `WithMaxRetries(n)` (workflow registration) | `WithMaxRecoveryAttempts(n)` |
| `CancelWorkflowOptions` (type) | `CancelWorkflowOption` |
| `WithAuthenticatedRoles(roles []string)` | `WithAuthenticatedRoles(roles ...string)` |
| `WithEnqueueAuthenticatedRoles(roles []string)` | `WithEnqueueAuthenticatedRoles(roles ...string)` |
| `UpdateWorkflowAttributes(ctx, id, attrs)` | `SetWorkflowAttributes(ctx, id, attrs)` |
| `WithReadStreamSnapshot(fromOffset int)` | `WithReadStreamSnapshot()` + `WithReadStreamFromOffset(offset int)` |

## 7. Miscellaneous API changes

- `Go[R](ctx, fn)` now returns a receive-only `<-chan StepOutcome[R]` (was `chan`). Only breaks call sites that stored it in an explicitly-typed bidirectional variable.
- `GobSerializer` struct unexported — construct via `dbos.NewGobSerializer()` (unchanged). Persisted v0 gob error payloads remain decodable.
- `ListRegisteredWorkflows(ctx)` returns `[]WorkflowRegistryEntry` — no error, no options; `WithScheduledOnly` / `ListRegisteredWorkflowsOption` removed.
- `GetLatestApplicationVersion` returns `(VersionInfo, error)` by value (was `*VersionInfo`). `nil` checks → `errors.Is(err, dbos.ErrNoApplicationVersions)`.
- `ClearRegistries` removed from the public API (was testing-only).
- New package-level accessors: `dbos.GetApplicationVersion(ctx)`, `dbos.GetExecutorID(ctx)`, `dbos.GetApplicationID(ctx)`.
- `Config.DatabaseURL` accepts Postgres/CockroachDB URLs or key=value DSNs, and sqlite URLs (`sqlite:/path/to.db`, `sqlite:relative.db`, `sqlite::memory:`).
- **SQLite now requires a driver import.** The SQLite driver moved out of the core package into `dbos/driver/sqlite`, registered database/sql-style. Apps using a sqlite URL or `Config.SQLiteSystemDB` must add `import _ "github.com/dbos-inc/dbos-transact-golang/dbos/driver/sqlite"` (one blank import, anywhere in the binary); without it, startup fails with an error naming this import. Postgres-only apps need no change and no longer compile or link `modernc.org/sqlite`.
- **One additive system-database schema change.** The first v1 `Launch()` runs migration 42, adding two defaulted columns to `workflow_status` (`is_debounced`, `debounce_deadline_epoch_ms`) for the debouncer redesign (§8). Existing workflows (including long-DELAYED ones), queues, and schedule rows carry over as-is, and v0.x and v1 executors can still share a system database during a rolling upgrade: old executors ignore the new columns and skip migration versions they don't know.

### Custom serializers must handle non-user values

Every checkpoint written to the system database is encoded with the configured custom serializer — including engine-internal step outputs: the `int64` deadline recorded by `DBOS.sleep` (written by `Sleep` and, new in v1, by `Recv`/`GetEvent` timeouts), `DBOS.writeStream`'s empty-string step output, and the `ScheduledWorkflowInput` struct for DB-backed schedule firings. This keeps every row in one format when inspecting the database from another program.

A `Serializer[any]` must therefore be total: it must encode and decode arbitrary Go values, and decoding must preserve concrete types (decoded step results are type-asserted, so an `int64` must round-trip as `int64`, not `float64`). Strictly-typed serializers need a mapping for bare Go scalars — see `golang/protobuf-serializer` in dbos-demo-apps, which packs `proto.Message` values into `anypb.Any` directly and wraps scalars in the protobuf well-known wrapper types (`wrapperspb.Int64Value`, `StringValue`, …), unwrapping them back to native scalars on decode. This keeps every stored row decodable by standard proto tooling in any language.

One deliberate exception: `Recv` and `GetEvent` checkpoint the *sender's* encoded payload verbatim under the sender's recorded format — the receiver's serializer is never asked to re-encode a message or event it didn't produce.

## 8. Debouncer redesign

Debouncing no longer runs through an internal coordinator workflow. A debounced workflow is now enqueued directly in the `DELAYED` status, holding its debounce key as its deduplication ID; each `Debounce` call atomically pushes back its start time and replaces its input, and the key is released when the delay elapses and the workflow is released to its queue. The `Debouncer` / `DebouncerClient` API is unchanged apart from the §2 type-parameter reorder, with these differences:

- New `WithDebouncerQueue(name)` runs the debounced workflow on a named queue instead of the DBOS internal queue. The queue is fixed per debouncer (debounce keys are scoped to it); `Debounce` calls cannot override it.
- A deadline on the `Debounce` context is recorded as the debounced workflow's execution timeout, never as an absolute deadline: like an enqueue timeout, the clock starts when the workflow is dequeued, so the debounce delay does not count against it. The timeout is captured by the call that enqueues the workflow; later calls coalescing on the same pending workflow do not change it.
- `Debounce` rejects options a debounce owns or cannot support: `WithQueue`, `WithDeduplicationID`, `WithDelay`, `WithPriority`, `WithQueuePartitionKey`, `WithDeduplicationPolicy`.
- Debouncers can be created at any time, including after `Launch()` (v0 required creation before launch).
- The debounce timeout is captured when the first call for a key enqueues the workflow; later calls extend the delay up to that fixed deadline but never move it.
- Pending debounced workflows are visible to workflow management: they appear as `DELAYED` on their queue with `IsDebounced` set, and can be listed with `dbos.WithFilterIsDebounced(true)`.

**Upgrade note: drain debouncers before upgrading.** v0 debouncing ran through internal `internalDebouncerWF` workflow registrations that v1 removes, so a v0 debouncer workflow still `PENDING` at upgrade cannot be recovered by a v1 executor — recovery logs "Workflow not found in registry" and leaves it `PENDING`, and its target workflow never starts. Before upgrading, stop calling `Debounce` and let in-flight debounces fire (at most the debounce delay or timeout); cancel any stragglers left after the upgrade with `CancelWorkflow`.

## 9. Mocks / tests

Mocks of the old `DBOSContext` must be regenerated from `dbos.Context` (which now embeds `Client`). With mockery, point the config at `Context` (see `integration/.mockery.yml` upstream for reference config). Assertions on error types/codes need the §5 renames.

## 10. Suggested migration order per app

1. `go mod edit -replace` to the local `go/` checkout; `go build ./...` to enumerate breakage.
2. Mechanical renames (safe to sed, word-boundary matched):
   `DBOSContext`→`Context`, `NewDBOSContext`→`NewContext`, `DBOSError`→`Error`, `DBOSErrorCode`→`ErrorCode`, `SqliteSystemDB`→`SQLiteSystemDB`, `UpdateWorkflowAttributes`→`SetWorkflowAttributes`, `CancelWorkflowOptions`→`CancelWorkflowOption`, step-retry `With*`→`WithStep*`, list-filter `With*`→`WithFilter*` (per §6 table — names collide with unrelated options, so match exact identifiers), error-code constants per §5, `Client<Fn>`→`<Fn>` per §2, `WorkflowFn:`→`Workflow:` (schedule-spec literals, per §4).
3. Structural fixes by hand: queue registration (`NewWorkflowQueue`→`RegisterQueue` + error handling + `WithQueue(queue)`), scheduled workflows (`WithSchedule`→`CreateSchedule`+`ScheduleSpec`, input signature), `Enqueue` type-param reorder, `RetrieveQueue`/`GetSchedule`/`GetLatestApplicationVersion` return-shape changes, `Shutdown` error handling.
4. `go vet ./...` && `go build ./...` && run tests.
