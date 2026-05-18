# spawner

`spawner` is a lightweight execution backend for submitting and observing Kubernetes Jobs.

This repository currently provides:

- A small `driver.Driver` abstraction for execution backends
- `DriverK8s` for Kubernetes Job execution
- `BoundedDriver` for burst control on `Start()`
- `K8sObserver` for Kueue admission and kube-scheduler observation
- Dispatcher / Actor / FrontDoor experiments for ingress and worker orchestration
- Durable-lite `RunStore` implementations for restart-oriented experiments

## Current Status

The repository is usable as a PoC execution backend and is already being exercised from the sibling `poc` project.

From the `poc` integration point of view, the most valuable parts of this repository are:

- `cmd/imp/k8s_driver.go`
- `cmd/imp/bounded_driver.go`
- `cmd/imp/k8s_observer.go`
- `pkg/driver`
- `pkg/dispatcher`
- `pkg/store`

The codebase is strong enough for architecture validation, but it is not yet a fully productized execution service.

## Productization Review

Based on the `poc` integration review, the main reinforcement areas are:

- Replace `Wait()` polling with watch/informer-based observation
- Separate logical run identity from physical job/attempt identity
- Strengthen duplicate submit, retry, and recovery semantics
- Expand `RunSpec` for production needs
- Turn bootstrap/recovery into real executable behavior
- Upgrade observability from helper methods to operator-facing state/event models

Detailed bilingual review documents:

- Korean: [docs/SPAWNER_REINFORCEMENT_REVIEW.ko.md](docs/SPAWNER_REINFORCEMENT_REVIEW.ko.md)
- English: [docs/SPAWNER_REINFORCEMENT_REVIEW.en.md](docs/SPAWNER_REINFORCEMENT_REVIEW.en.md)
- Korean sprint plan: [docs/SPAWNER_SPRINT_PLAN.ko.md](docs/SPAWNER_SPRINT_PLAN.ko.md)
- English sprint plan: [docs/SPAWNER_SPRINT_PLAN.en.md](docs/SPAWNER_SPRINT_PLAN.en.md)
- Korean recovery contract: [docs/SPAWNER_RECOVERY_CONTRACT.ko.md](docs/SPAWNER_RECOVERY_CONTRACT.ko.md)
- English recovery contract: [docs/SPAWNER_RECOVERY_CONTRACT.en.md](docs/SPAWNER_RECOVERY_CONTRACT.en.md)

## Testing And Coverage

Current validation baseline:

- `TMPDIR=/tmp GOTMPDIR=/tmp GOCACHE=/tmp/go-build GOLANGCI_LINT_CACHE=/tmp/golangci-lint ./bin/golangci-lint run`
- `go test ./...`
- `TMPDIR=/tmp GOTMPDIR=/tmp GOCACHE=/tmp/go-build go test -coverprofile=/tmp/spawner.cover ./...`
- `TMPDIR=/tmp GOTMPDIR=/tmp GOCACHE=/tmp/go-build go tool cover -func=/tmp/spawner.cover`

Regression guardrail:

- local push flow should be `lint -> test -> coverage`
- if regression tests fail, the change is treated as failed and must be fixed before push
- GitHub Actions runs the same checks on every push and pull request
- the repository-local linter binary is expected at `./bin/golangci-lint`

The repository now includes direct tests for:

- `cmd/imp/BoundedDriver`
- `pkg/dispatcher` ingress boundary behavior
- `pkg/store` state machine and durable-lite persistence
- `pkg/factory` actor reuse via `Bind/Register/Unbind`
- `pkg/frontdoor` predicate composition and cancel-rule construction

## Placement Surface

Current `RunSpec.Placement` semantics are intentionally narrow.

- `NodeSelector`: direct hard constraint passthrough
- `RequiredNodeName`: mapped to `kubernetes.io/hostname` nodeSelector
- `PreferredNodes`: mapped to Kubernetes `nodeAffinity.preferredDuringSchedulingIgnoredDuringExecution`

Important notes:

- `RequiredNodeName` is a hard placement constraint.
- `PreferredNodes` are soft hints only. The Kubernetes scheduler is not required to honor them.
- `RequiredNodeName` and `PreferredNodes` must not be used together in the current v0 surface.
- `MaterializationPlan`, init-container wiring, shared-volume acquisition, and generic PodSpec extension are intentionally deferred.

Coverage snapshot:

- Overall statement coverage: `75.5%`
- `cmd/imp`: `71.4%`
- `cmd/server`: `29.5%`
- `pkg/actor`: `89.5%`
- `pkg/api`: `93.5%`
- `pkg/dispatcher`: `74.2%`
- `pkg/driver`: `100.0%`
- `pkg/store`: `79.8%`
- `pkg/factory`: `96.8%`
- `pkg/frontdoor`: `91.7%`
- `pkg/policy`: `74.7%`

These numbers are intentionally documented as a moving baseline, not as a completion claim.
The weakest areas are still the server entrypoint, actor lifecycle edge cases, and live K8s integration paths.

## Immediate Notes

The previous TODO list is still valid in spirit, but it is now superseded by the structured review documents above.

Short version:

- gRPC and integration tests need to be expanded
- Actor lifecycle and reuse policy should be clarified
- Driver errors and execution state should be surfaced consistently
- Recovery semantics should move from comments/assumptions to real code
