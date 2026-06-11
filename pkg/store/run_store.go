// Package store provides RunStore: a boundary between the service front-end
// and the Actor/Driver execution layer.
//
// Queued runs live here until admitted to a DagSession or Actor.
// This enforces the invariant that a burst of user submissions does NOT
// become a burst of K8s Job creations — the Run queue absorbs the spike.
//
// ASSUMPTION: production replaces InMemoryRunStore/JsonRunStore with
// PostgreSQL/Redis. The interface is identical.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// RunState is the lifecycle state of a submitted DAG run.
type RunState string

const (
	// StateQueued: received from user, waiting for admission.
	StateQueued RunState = "queued"
	// StateHeld: explicitly paused (backend unavailable, rate-limit, manual hold).
	StateHeld RunState = "held"
	// StateResumed: hold lifted, waiting for re-admission.
	StateResumed RunState = "resumed"
	// StateAdmittedToDag: handed to a DagSession/Actor; execution is driving.
	StateAdmittedToDag RunState = "admitted-to-dag"
	// StateRunning: at least one node attempt is active in Kueue/K8s.
	StateRunning RunState = "running"
	// StateFinished: all nodes completed (terminal).
	StateFinished RunState = "finished"
	// StateFailed: execution failed (terminal).
	StateFailed RunState = "failed"
	// StateCanceled: run was canceled (terminal).
	StateCanceled RunState = "canceled"
)

// validTransitions defines allowed state machine edges.
// Reverse transitions and terminal→any are rejected.
var validTransitions = map[RunState][]RunState{
	StateQueued:        {StateHeld, StateAdmittedToDag, StateCanceled},
	StateHeld:          {StateResumed, StateCanceled},
	StateResumed:       {StateAdmittedToDag, StateCanceled},
	StateAdmittedToDag: {StateRunning, StateFailed, StateCanceled},
	StateRunning:       {StateFinished, StateFailed, StateCanceled},
	StateFinished:      {}, // terminal
	StateFailed:        {}, // terminal
	StateCanceled:      {}, // terminal
}

var (
	ErrInvalidTransition = errors.New("invalid state transition")
	ErrNotFound          = errors.New("run not found")
	ErrAlreadyExists     = errors.New("run already exists")
	ErrInvalidAttempt    = errors.New("invalid attempt record")
)

// RunRecord is the persisted representation of a submitted DAG run.
type RunRecord struct {
	RunID           string
	State           RunState
	Payload         []byte // latest serialized run envelope; kept for compatibility
	LatestAttemptID string
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

type AttemptCause string

const (
	AttemptCauseInitialSubmit  AttemptCause = "initial-submit"
	AttemptCauseRecoveryReplay AttemptCause = "recovery-replay"
	AttemptCauseManualRequeue  AttemptCause = "manual-requeue"
	AttemptCauseAutoRetry      AttemptCause = "auto-retry"
)

type AttemptRecord struct {
	AttemptID string
	RunID     string
	State     RunState
	Payload   []byte
	Cause     AttemptCause
	CreatedAt time.Time
	UpdatedAt time.Time
}

// RunStore is the contract for the front-end run store.
//
// ASSUMPTION: production will use PostgreSQL/Redis. The interface is identical.
type RunStore interface {
	Enqueue(ctx context.Context, rec RunRecord) error
	Get(ctx context.Context, runID string) (RunRecord, bool, error)
	UpdateState(ctx context.Context, runID string, from, to RunState) error
	ListByState(ctx context.Context, state RunState) ([]RunRecord, error)
	AppendAttempt(ctx context.Context, attempt AttemptRecord) error
	ListAttempts(ctx context.Context, runID string) ([]AttemptRecord, error)
	GetLatestAttempt(ctx context.Context, runID string) (AttemptRecord, bool, error)
	Delete(ctx context.Context, runID string) error
}

// ValidateTransition returns ErrInvalidTransition if the from→to edge is
// not in the state machine.
func ValidateTransition(from, to RunState) error {
	for _, allowed := range validTransitions[from] {
		if allowed == to {
			return nil
		}
	}
	return fmt.Errorf("%w: %s → %s", ErrInvalidTransition, from, to)
}

// IsTerminal reports whether s is a terminal state (no further transitions).
func IsTerminal(s RunState) bool {
	return s == StateFinished || s == StateFailed || s == StateCanceled
}

// IsRecoverable reports whether a persisted run state should be considered a
// replay candidate after a service restart.
//
// Fast-fail policy is preserved here: terminal failures are not retried by
// recovery, and explicitly held runs remain an operator/policy decision.
func IsRecoverable(s RunState) bool {
	return s == StateQueued || s == StateAdmittedToDag
}

func (c AttemptCause) IsValid() bool {
	switch c {
	case AttemptCauseInitialSubmit, AttemptCauseRecoveryReplay, AttemptCauseManualRequeue, AttemptCauseAutoRetry:
		return true
	default:
		return false
	}
}

func ValidateAttempt(attempt AttemptRecord) error {
	if attempt.RunID == "" || attempt.AttemptID == "" {
		return ErrInvalidAttempt
	}
	if !attempt.Cause.IsValid() {
		return ErrInvalidAttempt
	}
	return nil
}
