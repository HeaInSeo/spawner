package runtime

import "errors"

var (
	// ErrBackendUnavailable is returned by SubmitAttempt when the backend
	// is temporarily unreachable after bounded retries.
	ErrBackendUnavailable = errors.New("runtime: backend unavailable")

	// ErrCapacityExceeded is returned by SubmitAttempt when the Runtime's
	// bounded concurrency limit is reached.
	ErrCapacityExceeded = errors.New("runtime: capacity exceeded")

	// ErrSubmitOutcomeUnknown is returned by SubmitAttempt when the caller's ctx
	// was cancelled after the Runtime accepted the request but before a definitive
	// result could be returned. The backend job may or may not have been created.
	//
	// JUMI must retry SubmitAttempt with the same AttemptID. The Runtime will
	// regenerate the same (Namespace, JobName, AttemptMarker) and recover the
	// existing BackendRef if Create had already succeeded.
	//
	// JUMI must NOT create a new AttemptID on this error, and must NOT
	// increment node retry count.
	ErrSubmitOutcomeUnknown = errors.New("runtime: submit outcome unknown; retry same attempt id")

	// ErrJobConflict is returned by JobClient.Create when a job with the same
	// (Namespace, JobName) exists but the stored AttemptMarker differs from req.AttemptMarker.
	//
	// Indicates a system invariant violation (namingSalt or Namespace config changed).
	// The Runtime must NOT reuse or delete the existing job.
	// Surface as a system/config error and alert. Operator intervention required.
	ErrJobConflict = errors.New("jobclient: job name collision with different attempt")

	// ErrAttemptNotFound is returned by WatchAttempt or CancelAttempt only when
	// the AttemptHandle is irrecoverable:
	//   - AttemptID or BackendRef is empty/malformed
	//   - JobClient.Snapshot returns Exists=false with no reconciliation path
	//
	// "Runtime does not have this attempt in memory" is NOT a valid reason.
	// Runtime must use BackendRef to reconnect via JobClient before giving up.
	ErrAttemptNotFound = errors.New("runtime: attempt not found")
)
