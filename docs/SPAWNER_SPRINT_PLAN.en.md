# Spawner Sprint Plan

This document recalibrates the original `spawner` productization plan against the current repository state.

This is not a cosmetic rewrite. It reflects what is already implemented in code today.

- Recently completed: lifecycle guards, command validation, replay-safe envelopes, fast-fail recovery, policy-driven attempts, run/attempt history
- Main remaining gaps: state contract documentation, stricter duplicate/replay semantics, backend-neutral naming, `MetaContext` / `policy` / `RunSpec` surface hardening
- Explicitly out of scope here: informer migration, real cluster integration hardening, deployment automation

## Recheck Summary

As of `2026-04-09`, the current assessment is:

- The execution path is already fairly solid for a PoC backend.
- Most of the original Sprint 1 and Sprint 2 goals are already implemented.
- Sprint 3 was partially pulled forward: recovery helpers, replay helpers, and attempt policy already exist.
- Because of that, the old 4-sprint plan is no longer the right shape. The remaining work should be repackaged into `3 residual sprints`.

## Current Status Snapshot

Completed or mostly completed:

- dispatcher lifecycle release and semaphore recovery
- empty spawn key / invalid command guards
- `Command` constructors and `Validate()`
- `RunSpec.Validate()`
- logical run id / attempt id separation
- replay-safe `RunEnvelope`
- fast-fail recovery candidate filtering
- `RecoverableRuns()` / replay helpers
- attempt policy introduction
- `RunStore` summary + attempt history structure

Main remaining gaps:

- canonical mapping between persisted state and event state
- stricter duplicate submit / replay / manual requeue semantics
- typed attempt reason/cause model
- removal of backend-specific names such as `k8sAvailable` and `ErrK8sUnavailable`
- `MetaContext` copy semantics or immutability rules
- stronger `policy` validation/defaulting
- product-oriented `RunSpec` expansion
- turning `cmd/server` from example-ish bootstrap code into a clearer service contract example

## Planning Assumptions

- Reference date: `2026-04-09`
- Sprint length: `2 weeks`
- Assumed start date: `2026-04-13`
- Team assumption: one primary implementer
- Current baseline: core replay/recovery path already exists

## Revised Sprint Summary

| Sprint | Dates | Theme | Primary Goal |
| --- | --- | --- | --- |
| Sprint R1 | 2026-04-13 ~ 2026-04-24 | Recovery Contract Hardening | lock down recovery/state/duplicate semantics in code and docs |
| Sprint R2 | 2026-04-27 ~ 2026-05-08 | Library Surface Neutralization | remove backend-specific terminology and harden meta/policy contracts |
| Sprint R3 | 2026-05-11 ~ 2026-05-22 | Product Surface Expansion | expand `RunSpec` and align examples/docs/API surface |

## Sprint R1

Dates: `2026-04-13 ~ 2026-04-24`

Goals:

- Define recovery as a real service contract, not just a helper path.
- Make persisted state and transient event state agree in code, tests, and docs.
- Preserve fast-fail while clearly separating duplicate submit, recovery replay, and manual requeue.

Work items:

- document persisted lifecycle vs transient event lifecycle mapping
- write a decision table for duplicate submit / recovery replay / manual requeue / auto retry
- lift `AttemptRecord.Reason` into an enum or typed cause model
- lock down schema mismatch / malformed envelope handling during replay
- extend tests for recoverable vs non-recoverable state handling
- define terminal semantics from an operator-facing perspective

Deliverables:

- state contract document
- recovery semantics regression tests
- typed attempt cause model
- duplicate/replay decision table

Exit criteria:

- the code and docs answer "why is this state recoverable?" consistently
- replay, manual requeue, and duplicate submit are covered as distinct tested paths
- attempt history reasons are no longer just free-form strings

Risks:

- pushing too hard toward a single unified state model may blur persisted and transient concepts again
- duplicate semantics may conflict later with orchestrator requirements if locked down too early

## Sprint R2

Dates: `2026-04-27 ~ 2026-05-08`

Goals:

- remove Kubernetes-specific terminology from dispatcher/driver-facing library contracts
- make mutable meta bag and policy surfaces safer and easier to reason about

Work items:

- rename `k8sAvailable` to a generic executor/backend availability term
- replace `ErrK8sUnavailable` with a backend-neutral unavailable error
- add `MetaContext` copy semantics or immutable usage rules
- add `policy` validation/defaulting/merge rules
- document `AttemptPolicy` and `AdmitPolicy` usage examples
- document hold semantics for `NopDriver` and dispatcher behavior

Deliverables:

- backend-neutral naming update
- safer `MetaContext` contract
- stronger `policy` tests and docs
- migration note

Exit criteria:

- dispatcher is less coupled to a Kubernetes-specific name
- meta propagation side-effect risks are reduced in code or docs
- policy defaulting and validation are covered by tests

Risks:

- naming changes can widen the external change footprint
- moving toward value-copy meta semantics may affect ergonomics before it affects performance

## Sprint R3

Dates: `2026-05-11 ~ 2026-05-22`

Goals:

- build a more product-ready API surface on top of the now-stabilized core contract
- clarify intended usage through `RunSpec`, examples, and documentation

Work items:

- review minimal `RunSpec` expansion: version, annotations, cleanup, retry correlation, etc.
- clean up `cmd/server` so bootstrap/replay examples reflect the real contract more clearly
- refresh README and bilingual review docs
- write a migration note from the sibling `poc` integration perspective
- refine public constructors/helpers if needed

Deliverables:

- expanded `RunSpec`
- updated examples and docs
- migration note
- first-pass product usage guide

Exit criteria:

- `RunSpec` no longer looks like a PoC-only payload
- bootstrap/replay examples demonstrate the intended contract without misleading shortcuts
- the sibling `poc` repo has a clear path to follow the revised contract

Risks:

- expanding the API surface too aggressively can blur the core contract again
- example code can be mistaken for production service code unless documented carefully

## Deferred Items

Important, but intentionally outside these residual sprints:

- `DriverK8s.Wait()` polling -> watch/informer migration
- real Kueue/cluster integration hardening
- deployment, HA, and observability pipeline automation
- init-container / shared-volume materialization surfaces
- generic acquisition contract injection

## Current Placement Note

As of the current library surface:

- `RequiredNodeName` is supported as a hard placement constraint via `kubernetes.io/hostname`
- `PreferredNodes` is supported as a soft placement hint via Kubernetes preferred node affinity
- this does not imply storage/materialization semantics
- `MaterializationPlan` remains a downstream design concern and is intentionally not modeled by `spawner` yet

## Effort Estimate

Rough estimate for one primary implementer:

- Sprint R1: `7 ~ 9` working days
- Sprint R2: `6 ~ 8` working days
- Sprint R3: `6 ~ 8` working days

Total remaining effort:

- focused implementation: `19 ~ 25` working days
- calendar duration: about `6 weeks`

## Suggested Milestones

- `2026-04-24`: recovery/state/duplicate semantics locked down
- `2026-05-08`: backend-neutral naming + policy/meta contracts aligned
- `2026-05-22`: first product-facing `RunSpec`/docs/examples pass complete

## Final Recommendation

`spawner` has already moved well past the earliest stabilization phase.

So the right move now is not to replay the original early sprints, but to narrow the remaining contract gaps.

The most important remaining work is not feature expansion.

- make recovery semantics explicit and explainable
- lock down duplicate/replay/manual requeue behavior
- remove backend-specific naming from library contracts
- harden policy/meta/API surfaces

Once those are in place, informer migration and operational hardening will have a much cleaner foundation.
