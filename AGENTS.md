### 0 — Purpose
These rules ensure maintainability, safety, and developer velocity.  
**MUST** rules are enforced by CI/review; **SHOULD** rules are strong recommendations; **CAN** rules are allowed without extra approval.

---

### 1 — Before Coding
- **BP-1 (MUST)** Ask clarifying questions for ambiguous requirements.
- **BP-2 (MUST)** Draft and confirm an approach (API shape, data flow, failure modes) before writing code.
- **BP-3 (SHOULD)** When >2 approaches exist, list pros/cons and rationale.
- **BP-4 (SHOULD)** Define testing strategy (unit/integration) and observability signals up front.

---

### 2 — Modules & Dependencies
- **MD-1 (SHOULD)** Prefer stdlib; introduce deps only with clear payoff; track transitive size and licenses.
- **MD-2 (CAN)** Use `govulncheck` for updates.

---

### 3 — Code Style
- **CS-1 (MUST)** Run `gofmt`, `go vet`
- **CS-2 (MUST)** Avoid stutter in names: `package kv; type Store` (not `KVStore` in `kv`).
- **CS-3 (SHOULD)** Small interfaces near consumers; prefer composition over inheritance.
- **CS-4 (SHOULD)** Avoid reflection on hot paths; prefer generics when it clarifies and speeds.
- **CS-5 (MUST)** Use input structs for function receiving more than 2 arguments (ignoring context)
- **CS-6 (SHOULD)** Declare function input structs before the function consuming them.

---

### 4 — Errors
- **ERR-1 (MUST)** Wrap with `%w` and context: `fmt.Errorf("open %s: %w", p, err)`.
- **ERR-2 (MUST)** Use `errors.Is`/`errors.As` for control flow; no string matching.
- **ERR-3 (SHOULD)** Define sentinel errors in the package; document behavior.
- **ERR-4 (CAN)** Use `context.WithCancelCause` and `context.Cause` for propagating error causes.

---

### 5 — Concurrency
- **CC-1 (MUST)** The **sender** closes channels; receivers never close.
- **CC-2 (MUST)** Tie goroutine lifetime to a `context.Context`; prevent leaks.
- **CC-3 (MUST)** Protect shared state with `sync.Mutex`/`atomic`; no "probably safe" races.
- **CC-4 (SHOULD)** Use `errgroup` for fan‑out work; cancel on first error.
- **CC-5 (CAN)** Prefer buffered channels only with rationale (throughput/back‑pressure).

---

### 6 — Contexts
- **CTX-1 (MUST)** If a function takes in `ctx context.Context` it must be the first parameter; never store ctx in structs.
- **CTX-2 (MUST)** Propagate non‑nil `ctx`; honor `Done`/deadlines/timeouts.
- **CTX-3 (CAN)** Expose `WithX(ctx)` helpers that derive deadlines from config.

---

### 7 — Testing
- **T-1 (MUST)** Table‑driven tests; deterministic and hermetic by default.
- **T-2 (MUST)** Run `-race` in CI; add `t.Cleanup` for teardown.
- **T-3 (SHOULD)** Mark safe tests with `t.Parallel()`.

---

### 8 — Logging & Observability
- **OBS-1 (MUST)** Structured logging (`slog`) with levels and consistent fields.
- **OBS-2 (SHOULD)** Correlate logs/metrics/traces via request IDs from context.
- **OBS-3 (CAN)** Provide debug/pprof endpoints guarded by auth or local‑only access.

---

### 9 — Performance
- **PERF-1 (MUST)** Measure before optimizing: `pprof`, `go test -bench`, `benchstat`.
- **PERF-2 (SHOULD)** Avoid allocations on hot paths; reuse buffers with care; prefer `bytes`/`strings` APIs.
- **PERF-3 (CAN)** Add microbenchmarks for critical functions and track regressions in CI.

---

### 10 — Configuration
- **CFG-1 (MUST)** Config via env/flags; validate on startup; fail fast.
- **CFG-2 (MUST)** Treat config as immutable after init; pass explicitly (not via globals).
- **CFG-3 (SHOULD)** Provide sane defaults and clear docs.
- **CFG-4 (CAN)** Support config hot‑reload only when correctness is preserved and tested.

---

### 11 — APIs & Boundaries
- **API-1 (MUST)** Document exported items: `// Foo does …`; keep exported surface minimal.
- **API-2 (MUST)** Accept interfaces where variation is needed; **return concrete types** unless abstraction is required.
- **API-3 (SHOULD)** Keep functions small, orthogonal, and composable.
- **API-5 (CAN)** Use constructor options pattern for extensibility.

---

### 12 — Security
- **SEC-1 (MUST)** Validate inputs; set explicit I/O timeouts; prefer TLS everywhere.
- **SEC-2 (MUST)** Never log secrets; manage secrets outside code (env/secret manager).
- **SEC-3 (SHOULD)** Limit filesystem/network access by default; principle of least privilege.
- **SEC-4 (CAN)** Add fuzz tests for untrusted inputs.

---

### 13 — CI/CD
- **CI-1 (MUST)** Lint, vet, test (`-race`), and build on every PR; cache modules/builds.
- **CI-2 (MUST)** Reproducible builds with `-trimpath`; embed version via `-ldflags "-X main.version=$TAG"`.
- **CI-3 (SHOULD)** Require review sign‑off for rules labeled (MUST).
- **CI-4 (CAN)** Publish SBOM and run `govulncheck`/license checks in CI.

---

### 14 — Tooling
- **TL-1 (CAN)** Linters: `golangci-lint`, `staticcheck`, `gofumpt`.
- **TL-2 (CAN)** Security: `govulncheck`, dependency scanners.
- **TL-3 (CAN)** Testing: `gotestsum`, `mockgen`/`counterfeiter`.
- **TL-4 (CAN)** APIs: `buf` for Protobuf; `oapi-codegen` for OpenAPI.

---

### 16 — Tooling Gates (examples)
- **G-1 (MUST)** `go vet ./...` passes.
- **G-2 (MUST)** `golangci-lint run` passes with project config.
- **G-3 (MUST)** `go test -race ./...` passes.
- **G-4 (CAN)** Allow `gh pr view <num>`, `gh pr diff <num>` for review context (no direct pushes from automation).

---

### Appendix — Rationale for IDs
Stable IDs (e.g., **BP-1**, **ERR-2**) enable precise code‑review comments, changelogs, and automated policy checks (e.g., “violates **CC-2**”). Keep IDs stable; deprecate with notes instead of renumbering.
"""

---

## Writing Functions Best Practices
1. Can you read the function and HONESTLY easily follow what it's doing? If yes, then stop here.
2. Does the function have very high cyclomatic complexity? (number of independent paths, or, in a lot of cases, number of nesting if if-else as a proxy). If it does, then it's probably sketchy.
3. Are there any common data structures and algorithms that would make this function much easier to follow and more robust? Parsers, trees, stacks / queues, etc.
4. Does it have any hidden untested dependencies or any values that can be factored out into the arguments instead? Only care about non-trivial dependencies that can actually change or affect the function.
5. Brainstorm 3 better function names and see if the current name is the best, consistent with rest of codebase.