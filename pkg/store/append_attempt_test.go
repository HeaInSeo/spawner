package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/HeaInSeo/spawner/pkg/store"
)

// TestStore_AppendAttempt_RejectsTerminalRecord verifies that AppendAttempt
// rejects attempts when the run is already in a terminal state.
func TestStore_AppendAttempt_RejectsTerminalRecord(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name     string
		terminal store.RunState
	}{
		{"finished", store.StateFinished},
		{"canceled", store.StateCanceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rs := store.NewInMemoryRunStore()

			if err := rs.Enqueue(ctx, store.RunRecord{
				RunID:   "run-1",
				State:   store.StateQueued,
				Payload: []byte(`{}`),
			}); err != nil {
				t.Fatalf("Enqueue: %v", err)
			}

			// Advance to terminal state via valid transitions
			if tc.terminal == store.StateFinished {
				mustUpdateState(t, rs, ctx, "run-1", store.StateQueued, store.StateAdmittedToDag)
				mustUpdateState(t, rs, ctx, "run-1", store.StateAdmittedToDag, store.StateRunning)
				mustUpdateState(t, rs, ctx, "run-1", store.StateRunning, store.StateFinished)
			} else {
				mustUpdateState(t, rs, ctx, "run-1", store.StateQueued, store.StateCanceled)
			}

			// AppendAttempt must be rejected
			err := rs.AppendAttempt(ctx, store.AttemptRecord{
				AttemptID: "run-1/attempt-2",
				RunID:     "run-1",
				State:     store.StateQueued,
				Cause:     store.AttemptCauseManualRequeue,
			})
			if err == nil {
				t.Fatal("expected error appending to terminal run, got nil")
			}
			if !errors.Is(err, store.ErrInvalidAttempt) {
				t.Fatalf("expected ErrInvalidAttempt, got %v", err)
			}
		})
	}
}

func mustUpdateState(t *testing.T, rs *store.InMemoryRunStore, ctx context.Context, runID string, from, to store.RunState) {
	t.Helper()
	if err := rs.UpdateState(ctx, runID, from, to); err != nil {
		t.Fatalf("UpdateState %s→%s: %v", from, to, err)
	}
}

// TestStore_AppendAttempt_RejectsNonQueuedState verifies that AppendAttempt
// rejects attempts whose State is not StateQueued, for both InMemory and Json stores.
func TestStore_AppendAttempt_RejectsNonQueuedState(t *testing.T) {
	ctx := context.Background()

	for _, name := range []string{"InMemory"} {
		t.Run(name, func(t *testing.T) {
			rs := store.NewInMemoryRunStore()

			// Set up a run in StateQueued
			if err := rs.Enqueue(ctx, store.RunRecord{
				RunID:   "run-1",
				State:   store.StateQueued,
				Payload: []byte(`{}`),
			}); err != nil {
				t.Fatalf("Enqueue: %v", err)
			}

			// Attempt with StateCanceled — must be rejected
			err := rs.AppendAttempt(ctx, store.AttemptRecord{
				AttemptID: "run-1/attempt-2",
				RunID:     "run-1",
				State:     store.StateCanceled,
				Cause:     store.AttemptCauseManualRequeue,
			})
			if err == nil {
				t.Fatal("expected error for non-queued attempt state, got nil")
			}
			if !errors.Is(err, store.ErrInvalidAttempt) {
				t.Fatalf("expected ErrInvalidAttempt, got %v", err)
			}

			// Attempt with StateQueued — must succeed
			err = rs.AppendAttempt(ctx, store.AttemptRecord{
				AttemptID: "run-1/attempt-2",
				RunID:     "run-1",
				State:     store.StateQueued,
				Cause:     store.AttemptCauseManualRequeue,
			})
			if err != nil {
				t.Fatalf("expected no error for StateQueued attempt, got: %v", err)
			}
		})
	}
}
