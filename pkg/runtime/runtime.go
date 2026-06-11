package runtime

import "context"

// Runtime is the spawner boundary that JUMI calls to execute a node attempt.
// It is an actor-based runtime firewall between JUMI's execution semantics
// and the underlying backend (Kubernetes).
//
// Runtime responsibilities:
//   - AttemptRequest validation before forwarding
//   - Actor lifecycle and session management
//   - Bounded concurrency (max simultaneous jobs in flight)
//   - Start/cancel race: cancel that arrives before Start completes
//   - Transient error retry with bounded backoff
//   - AttemptTimeout enforcement → Failed + Reason=ReasonDeadlineExceeded
//   - JobEvent → AttemptEvent normalization
//
// NOT Runtime responsibilities (JUMI):
//   - Whether to retry a failed attempt (retry policy, attempt count)
//   - Artifact resolution and AH handoff
//   - Fail-fast / DAG dependency propagation
//   - Run/Node/Attempt record updates
//
// NOT Runtime responsibilities (JobClient implementation):
//   - K8s JobSpec construction
//   - resource.ParseQuantity
//   - Placement → NodeSelector / Affinity translation
//   - ServiceAccountName / WorkingDir / Volume application
//   - System label/annotation injection
//
// Durability note: spawner.Runtime is NOT a durable queue.
// If the backend is unavailable, Runtime returns ErrBackendUnavailable and
// JUMI decides whether to reattempt later.
type Runtime interface {
	// SubmitAttempt validates the request, hands it to a runtime actor, and
	// calls JobClient.Create. Returns only after the backend job is created.
	//
	// The returned AttemptHandle contains a fully populated BackendRef.
	// JUMI should persist it to the execution store before proceeding.
	//
	// Once the Runtime actor accepts the request, JobClient.Create is driven
	// by a Runtime-owned context (not the caller's ctx). If the caller's ctx
	// is cancelled after Create succeeds but before the handle is returned,
	// ErrSubmitOutcomeUnknown is returned. JUMI must retry with the same
	// AttemptID — the Runtime will recover the existing BackendRef via the
	// deterministic (Namespace, JobName) pair.
	//
	// Errors:
	//   ErrBackendUnavailable    backend temporarily unreachable after bounded retries
	//   ErrCapacityExceeded      bounded concurrency limit reached
	//   ErrSubmitOutcomeUnknown  ctx cancelled after possible Create; retry same AttemptID
	//   validation error         AttemptRequest.Validate() failed
	SubmitAttempt(ctx context.Context, req AttemptRequest) (AttemptHandle, error)

	// WatchAttempt returns a channel of AttemptEvents until terminal state
	// or ctx cancellation. The channel is closed after the terminal event.
	// Safe to call after a restart using a persisted AttemptHandle.
	//
	// First event:
	//   Normal path (attempt in memory): AttemptStateSubmitted.
	//   Recovery path (after restart, Snapshot-based): current observed state
	//   (may be Running, Succeeded, etc.). JUMI must tolerate any state as first event.
	//
	// AttemptStateAccepted is never emitted.
	WatchAttempt(ctx context.Context, h AttemptHandle) (<-chan AttemptEvent, error)

	// CancelAttempt requests cancellation of a non-terminal attempt.
	// Emits AttemptStateCancelled with Reason=ReasonUserCancel.
	//
	// Idempotent and terminal-safe:
	//   - Non-terminal: cancel backend job, emit Cancelled.
	//   - Already terminal: no-op. Existing terminal outcome is NOT overwritten.
	CancelAttempt(ctx context.Context, h AttemptHandle) error
}
