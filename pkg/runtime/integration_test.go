package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// ── crash recovery: ErrSubmitOutcomeUnknown → retry → BackendRef recovered ──

func TestIntegration_CrashRecovery_SubmitOutcomeUnknown(t *testing.T) {
	// Simulate: Create succeeds, but caller ctx is cancelled before handle is returned.
	// JUMI retries with same AttemptID. Runtime must return the same BackendRef.
	started := make(chan struct{})
	proceed := make(chan struct{})

	var createdUID string
	var mu sync.Mutex

	client := &fakeJobClient{
		createFn: func(ctx context.Context, req JobCreateRequest) (BackendRef, error) {
			close(started)
			<-proceed
			ref := NewK8sJobBackendRef(req.Namespace, req.JobName, "recovered-uid")
			mu.Lock()
			createdUID = ref.UID
			mu.Unlock()
			return ref, nil
		},
	}
	rt := newTestRuntime(t, client)

	ctx1, cancel1 := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := rt.SubmitAttempt(ctx1, minimalReq("crash-attempt"))
		errCh <- err
	}()

	<-started
	cancel1() // simulate crash: caller context cancelled

	if err := <-errCh; !errors.Is(err, ErrSubmitOutcomeUnknown) {
		t.Fatalf("expected ErrSubmitOutcomeUnknown, got %v", err)
	}

	close(proceed) // Create finishes

	// Give Create goroutine time to store the entry.
	time.Sleep(20 * time.Millisecond)

	// Retry with same AttemptID — must recover handle.
	handle, err := rt.SubmitAttempt(context.Background(), minimalReq("crash-attempt"))
	if err != nil {
		t.Fatalf("retry after crash: %v", err)
	}
	mu.Lock()
	wantUID := createdUID
	mu.Unlock()
	if handle.BackendRef.UID != wantUID {
		t.Errorf("recovered UID = %q, want %q", handle.BackendRef.UID, wantUID)
	}
}

// ── restart recovery: WatchAttempt uses Snapshot ─────────────────────────────

func TestIntegration_RestartRecovery_SnapshotRunning(t *testing.T) {
	// Simulate: after restart, attempt is not in memory.
	// WatchAttempt must call Snapshot, emit Running as first event, then watch.
	snap := JobSnapshot{
		State:     AttemptStateRunning,
		Exists:    true,
		Timestamp: time.Now(),
	}
	jw := makeJobWatch([]JobEvent{
		{State: AttemptStateSucceeded, Timestamp: time.Now()},
	}, nil)

	client := &fakeJobClient{
		snapshotFn: func(_ context.Context, _ BackendRef) (JobSnapshot, error) { return snap, nil },
		watchFn:    func(_ context.Context, _ BackendRef) (JobWatch, error) { return jw, nil },
	}
	rt := newTestRuntime(t, client)

	// AttemptHandle as if persisted by JUMI.
	h := AttemptHandle{
		AttemptID:  "restart-attempt",
		BackendRef: NewK8sJobBackendRef("test-ns", "spw-abc123", "uid-xyz"),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ch, err := rt.WatchAttempt(ctx, h)
	if err != nil {
		t.Fatalf("WatchAttempt (recovery): %v", err)
	}

	evs := collectEvents(ctx, ch)
	if len(evs) == 0 {
		t.Fatal("expected events, got none")
	}

	// First event must be the Snapshot state (Running), not Submitted.
	if evs[0].State != AttemptStateRunning {
		t.Errorf("first event = %s, want Running (from Snapshot)", evs[0].State)
	}

	last := evs[len(evs)-1]
	if last.State != AttemptStateSucceeded {
		t.Errorf("last event = %s, want Succeeded", last.State)
	}
}

func TestIntegration_RestartRecovery_SnapshotTerminal(t *testing.T) {
	// Simulate: after restart, job already completed (Succeeded).
	// WatchAttempt must emit Succeeded once and close channel.
	snap := JobSnapshot{
		State:     AttemptStateSucceeded,
		Exists:    true,
		Timestamp: time.Now(),
	}

	client := &fakeJobClient{
		snapshotFn: func(_ context.Context, _ BackendRef) (JobSnapshot, error) { return snap, nil },
	}
	rt := newTestRuntime(t, client)

	h := AttemptHandle{
		AttemptID:  "completed-attempt",
		BackendRef: NewK8sJobBackendRef("test-ns", "spw-completed", "uid-done"),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ch, err := rt.WatchAttempt(ctx, h)
	if err != nil {
		t.Fatalf("WatchAttempt (recovery terminal): %v", err)
	}

	evs := collectEvents(ctx, ch)
	if len(evs) != 1 {
		t.Fatalf("expected 1 event, got %d: %v", len(evs), evs)
	}
	if evs[0].State != AttemptStateSucceeded {
		t.Errorf("event = %s, want Succeeded", evs[0].State)
	}
}

func TestIntegration_RestartRecovery_NotFound(t *testing.T) {
	client := &fakeJobClient{
		snapshotFn: func(_ context.Context, _ BackendRef) (JobSnapshot, error) {
			return JobSnapshot{Exists: false}, nil
		},
	}
	rt := newTestRuntime(t, client)

	h := AttemptHandle{
		AttemptID:  "ghost-attempt",
		BackendRef: NewK8sJobBackendRef("test-ns", "spw-ghost", "uid-ghost"),
	}

	_, err := rt.WatchAttempt(context.Background(), h)
	if !errors.Is(err, ErrAttemptNotFound) {
		t.Fatalf("expected ErrAttemptNotFound, got %v", err)
	}
}

// ── ErrJobConflict: fail + alert, no auto-delete ──────────────────────────────

func TestIntegration_ErrJobConflict_NotRetryable(t *testing.T) {
	deleteCalled := make(chan struct{}, 1)

	client := &fakeJobClient{
		createFn: func(_ context.Context, _ JobCreateRequest) (BackendRef, error) {
			return BackendRef{}, ErrJobConflict
		},
		deleteFn: func(_ context.Context, _ BackendRef) error {
			deleteCalled <- struct{}{}
			return nil
		},
	}
	rt := newTestRuntime(t, client)

	_, err := rt.SubmitAttempt(context.Background(), minimalReq("conflict-attempt"))
	if !errors.Is(err, ErrJobConflict) {
		t.Fatalf("expected ErrJobConflict, got %v", err)
	}

	// Runtime must NOT auto-delete on conflict.
	select {
	case <-deleteCalled:
		t.Error("Runtime must not delete on ErrJobConflict")
	case <-time.After(100 * time.Millisecond):
		// correct: no delete
	}

	// active must return to 0 (entry cleaned up).
	rt.mu.RLock()
	active := rt.active
	rt.mu.RUnlock()
	if active != 0 {
		t.Errorf("active = %d after ErrJobConflict, want 0", active)
	}
}

// ── AttemptTimeout: exact deadline, active decremented ───────────────────────

func TestIntegration_AttemptTimeout_WithWatch(t *testing.T) {
	evCh := make(chan JobEvent) // never sends
	errCh := make(chan JobWatchError, 1)
	jw := JobWatch{Events: evCh, Errs: errCh}

	client := &fakeJobClient{
		watchFn: func(_ context.Context, _ BackendRef) (JobWatch, error) { return jw, nil },
	}
	rt := newTestRuntime(t, client)

	req := minimalReq("timeout-attempt")
	req.AttemptTimeout = 80 * time.Millisecond

	handle, err := rt.SubmitAttempt(context.Background(), req)
	if err != nil {
		t.Fatalf("SubmitAttempt: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ch, _ := rt.WatchAttempt(ctx, handle)
	evs := collectEvents(ctx, ch)

	// Must receive Submitted + Failed/deadline-exceeded.
	var submitted, failed bool
	for _, ev := range evs {
		if ev.State == AttemptStateSubmitted {
			submitted = true
		}
		if ev.State == AttemptStateFailed && ev.Reason == ReasonDeadlineExceeded {
			failed = true
		}
	}
	if !submitted {
		t.Error("expected Submitted event")
	}
	if !failed {
		t.Errorf("expected Failed/deadline-exceeded, got: %v", evs)
	}

	rt.mu.RLock()
	active := rt.active
	rt.mu.RUnlock()
	if active != 0 {
		t.Errorf("active = %d after timeout, want 0", active)
	}
}

// ── concurrent watch + cancel race ───────────────────────────────────────────

func TestIntegration_CancelWhileWatching(t *testing.T) {
	// Watch is in progress when cancel arrives; must see exactly one terminal event.
	evCh := make(chan JobEvent)
	errCh := make(chan JobWatchError, 1)
	jw := JobWatch{Events: evCh, Errs: errCh}

	client := &fakeJobClient{
		watchFn: func(_ context.Context, _ BackendRef) (JobWatch, error) { return jw, nil },
	}
	rt := newTestRuntime(t, client)

	handle, _ := rt.SubmitAttempt(context.Background(), minimalReq("cancel-race"))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ch, _ := rt.WatchAttempt(ctx, handle)
	time.Sleep(20 * time.Millisecond) // let watchLoop start

	rt.CancelAttempt(context.Background(), handle) //nolint

	evs := collectEvents(ctx, ch)

	var terminalCount int
	for _, ev := range evs {
		if ev.State.IsTerminal() {
			terminalCount++
		}
	}
	if terminalCount != 1 {
		t.Errorf("expected exactly 1 terminal event, got %d: %v", terminalCount, evs)
	}
	if evs[len(evs)-1].State != AttemptStateCancelled {
		t.Errorf("last event = %s, want Cancelled", evs[len(evs)-1].State)
	}
}
