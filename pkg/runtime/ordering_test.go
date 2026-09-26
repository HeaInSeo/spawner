package runtime

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// Layer-2 evidence for spawner #8: terminal ordering across the production
// Runtime paths (SubmitAttempt → WatchAttempt → runTimeout / CancelAttempt).
// These drive the real runtimeImpl through its public API; the fake JobClient
// only supplies backend events and records Delete calls.

// waitUntil polls cond until it holds or d elapses.
func waitUntil(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", d, what)
		}
		time.Sleep(time.Millisecond)
	}
}

// runLateBackendTerminal ends an attempt through the Runtime (AttemptTimeout
// when end is nil, otherwise end) while the watch loop is blocked delivering
// to a full event channel, then queues a backend Succeeded behind it. When the
// loop resumes, both the closed terminalCh and the queued backend event are
// ready, so its select may pick either one. Whichever it picks, the watcher
// must see exactly one terminal event, carrying the recorded outcome.
func runLateBackendTerminal(
	t *testing.T, attemptID string, attemptTimeout time.Duration,
	end func(*runtimeImpl, AttemptHandle), wantState AttemptState, wantReason string,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Submitted plus 15 Running fill the 16-slot watcher channel; the 16th
	// Running blocks the loop in trySend.
	events := make(chan JobEvent, 32)
	for i := 0; i < 16; i++ {
		events <- JobEvent{State: AttemptStateRunning, Timestamp: time.Now()}
	}
	deleted := make(chan struct{}, 1)
	client := &fakeJobClient{
		watchFn: func(context.Context, BackendRef) (JobWatch, error) {
			return JobWatch{Events: events, Errs: make(chan JobWatchError)}, nil
		},
		deleteFn: func(context.Context, BackendRef) error {
			select {
			case deleted <- struct{}{}:
			default:
			}
			return nil
		},
	}
	rt := newTestRuntime(t, client)

	req := minimalReq(attemptID)
	req.AttemptTimeout = attemptTimeout
	h, err := rt.SubmitAttempt(ctx, req)
	if err != nil {
		t.Fatalf("SubmitAttempt: %v", err)
	}
	out, err := rt.WatchAttempt(ctx, h)
	if err != nil {
		t.Fatalf("WatchAttempt: %v", err)
	}
	waitUntil(t, 5*time.Second, "watch loop to block on a full event channel", func() bool {
		return len(out) == cap(out)
	})

	if end == nil {
		waitFor(t, deleted, 5*time.Second, "AttemptTimeout cleanup Delete")
	} else {
		end(rt, h)
	}
	events <- JobEvent{State: AttemptStateSucceeded, Timestamp: time.Now()}

	evs := collectEvents(ctx, out)
	if ctx.Err() != nil {
		t.Fatalf("event channel did not close: %v", evs)
	}
	if evs[0].State != AttemptStateSubmitted {
		t.Fatalf("first event = %s, want Submitted", evs[0].State)
	}
	var terminal []AttemptEvent
	for _, ev := range evs {
		if ev.State.IsTerminal() {
			terminal = append(terminal, ev)
		}
	}
	if len(terminal) != 1 {
		t.Fatalf("terminal events = %v, want exactly one", terminal)
	}
	last := evs[len(evs)-1]
	if !last.State.IsTerminal() {
		t.Fatalf("last event = %s, want the terminal event last", last.State)
	}
	if last.State != wantState || last.Reason != wantReason {
		t.Fatalf("watcher saw terminal (%s, %q), want recorded outcome (%s, %q)",
			last.State, last.Reason, wantState, wantReason)
	}

	rt.mu.RLock()
	entry := rt.attempts[attemptID]
	active := rt.active
	rt.mu.RUnlock()
	entry.mu.Lock()
	stored := entry.state
	entry.mu.Unlock()
	if stored != wantState {
		t.Fatalf("stored state = %s, want %s", stored, wantState)
	}
	if active != 0 {
		t.Fatalf("active = %d, want 0", active)
	}
}

func TestWatch_LateBackendTerminalAfterAttemptTimeout_ReportsDeadlineExceeded(t *testing.T) {
	for i := 0; i < 20; i++ {
		runLateBackendTerminal(t, fmt.Sprintf("timeout-%d", i), 200*time.Millisecond,
			nil, AttemptStateFailed, ReasonDeadlineExceeded)
	}
}

func TestWatch_LateBackendTerminalAfterCancel_ReportsCancelled(t *testing.T) {
	cancelAttempt := func(rt *runtimeImpl, h AttemptHandle) {
		if err := rt.CancelAttempt(context.Background(), h); err != nil {
			t.Fatalf("CancelAttempt: %v", err)
		}
	}
	for i := 0; i < 40; i++ {
		runLateBackendTerminal(t, fmt.Sprintf("cancel-%d", i), 0,
			cancelAttempt, AttemptStateCancelled, ReasonUserCancel)
	}
}

// TestWatch_EarlyBackendTerminalStopsAttemptTimeout proves an attempt that
// the backend finishes before AttemptTimeout keeps that outcome: the timer is
// stopped, so it neither deletes the finished Job nor rewrites the terminal
// state once the timeout would have elapsed.
func TestWatch_EarlyBackendTerminalStopsAttemptTimeout(t *testing.T) {
	const attemptTimeout = 100 * time.Millisecond
	var deletes atomic.Int32
	client := &fakeJobClient{
		watchFn: func(context.Context, BackendRef) (JobWatch, error) {
			return makeJobWatch([]JobEvent{{State: AttemptStateSucceeded, Timestamp: time.Now()}}, nil), nil
		},
		deleteFn: func(context.Context, BackendRef) error {
			deletes.Add(1)
			return nil
		},
	}
	rt := newTestRuntime(t, client)

	req := minimalReq("early")
	req.AttemptTimeout = attemptTimeout
	h, err := rt.SubmitAttempt(context.Background(), req)
	if err != nil {
		t.Fatalf("SubmitAttempt: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := rt.WatchAttempt(ctx, h)
	if err != nil {
		t.Fatalf("WatchAttempt: %v", err)
	}
	evs := collectEvents(ctx, out)
	if len(evs) != 2 || evs[0].State != AttemptStateSubmitted || evs[1].State != AttemptStateSucceeded {
		t.Fatalf("events = %v, want [Submitted Succeeded]", evs)
	}

	// Outlive the configured timeout; a still-armed timer would fire here.
	time.Sleep(3 * attemptTimeout)

	if n := deletes.Load(); n != 0 {
		t.Fatalf("Delete called %d times after the backend already succeeded", n)
	}
	rt.mu.RLock()
	entry := rt.attempts["early"]
	active := rt.active
	rt.mu.RUnlock()
	entry.mu.Lock()
	state, reason := entry.state, entry.terminalReason
	entry.mu.Unlock()
	if state != AttemptStateSucceeded || reason != "" {
		t.Fatalf("terminal = (%s, %q), want (Succeeded, \"\") — the timeout rewrote the outcome", state, reason)
	}
	if active != 0 {
		t.Fatalf("active = %d, want 0", active)
	}
}
