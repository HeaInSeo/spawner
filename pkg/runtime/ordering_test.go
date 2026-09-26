package runtime

import (
	"context"
	"errors"
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

// lateBackendInput is what the backend delivers after the Runtime has already
// ended the attempt: a queued event, a watch error, or a closed event stream.
type lateBackendInput func(events chan JobEvent, errs chan JobWatchError)

func lateSucceeded(events chan JobEvent, _ chan JobWatchError) {
	events <- JobEvent{State: AttemptStateSucceeded, Timestamp: time.Now()}
}

func lateWatchErr(_ chan JobEvent, errs chan JobWatchError) {
	errs <- JobWatchError{Reason: "watch-gone", Message: "watch stream failed"}
}

func lateStreamClose(events chan JobEvent, _ chan JobWatchError) {
	close(events)
}

// runLateBackendTerminal ends an attempt through the Runtime (AttemptTimeout
// when end is nil, otherwise end) while the watch loop is blocked delivering
// to a full event channel, then applies late behind it. When the loop
// resumes, both the closed terminalCh and the late backend input are ready, so
// its select may pick either one. Whichever it picks, the watcher must see
// exactly one terminal event, last, carrying the recorded outcome.
func runLateBackendTerminal(
	t *testing.T, attemptID string, attemptTimeout time.Duration,
	end func(*runtimeImpl, AttemptHandle), late lateBackendInput,
	wantState AttemptState, wantReason string,
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
	errs := make(chan JobWatchError, 1)
	deleted := make(chan struct{}, 1)
	client := &fakeJobClient{
		watchFn: func(context.Context, BackendRef) (JobWatch, error) {
			return JobWatch{Events: events, Errs: errs}, nil
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
	late(events, errs)

	assertRecordedTerminal(t, ctx, rt, attemptID, out, wantState, wantReason)
}

// assertRecordedTerminal drains out and checks the watcher saw Submitted first
// and exactly one terminal event, last, equal to the stored outcome, and that
// the attempt was counted out of active exactly once.
func assertRecordedTerminal(
	t *testing.T, ctx context.Context, rt *runtimeImpl, attemptID string,
	out <-chan AttemptEvent, wantState AttemptState, wantReason string,
) {
	t.Helper()
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
	stored, storedReason := entry.state, entry.terminalReason
	entry.mu.Unlock()
	if stored != wantState || storedReason != wantReason {
		t.Fatalf("stored outcome = (%s, %q), want (%s, %q)", stored, storedReason, wantState, wantReason)
	}
	if active != 0 {
		t.Fatalf("active = %d, want 0", active)
	}
}

func cancelAttemptFn(t *testing.T) func(*runtimeImpl, AttemptHandle) {
	return func(rt *runtimeImpl, h AttemptHandle) {
		if err := rt.CancelAttempt(context.Background(), h); err != nil {
			t.Fatalf("CancelAttempt: %v", err)
		}
	}
}

func TestWatch_LateBackendTerminalAfterAttemptTimeout_ReportsDeadlineExceeded(t *testing.T) {
	for i := 0; i < 20; i++ {
		runLateBackendTerminal(t, fmt.Sprintf("timeout-%d", i), 200*time.Millisecond,
			nil, lateSucceeded, AttemptStateFailed, ReasonDeadlineExceeded)
	}
}

func TestWatch_LateBackendTerminalAfterCancel_ReportsCancelled(t *testing.T) {
	for i := 0; i < 40; i++ {
		runLateBackendTerminal(t, fmt.Sprintf("cancel-%d", i), 0,
			cancelAttemptFn(t), lateSucceeded, AttemptStateCancelled, ReasonUserCancel)
	}
}

// The error and close exits of the watch loop must honor the recorded
// terminal the same way: a late permanent watch error must not replace it with
// a generic Failed, and a stream close must not end the watch without it.

func TestWatch_LateWatchErrAfterAttemptTimeout_ReportsDeadlineExceeded(t *testing.T) {
	for i := 0; i < 20; i++ {
		runLateBackendTerminal(t, fmt.Sprintf("timeout-err-%d", i), 200*time.Millisecond,
			nil, lateWatchErr, AttemptStateFailed, ReasonDeadlineExceeded)
	}
}

func TestWatch_LateWatchErrAfterCancel_ReportsCancelled(t *testing.T) {
	for i := 0; i < 40; i++ {
		runLateBackendTerminal(t, fmt.Sprintf("cancel-err-%d", i), 0,
			cancelAttemptFn(t), lateWatchErr, AttemptStateCancelled, ReasonUserCancel)
	}
}

func TestWatch_StreamCloseAfterAttemptTimeout_ReportsDeadlineExceeded(t *testing.T) {
	for i := 0; i < 20; i++ {
		runLateBackendTerminal(t, fmt.Sprintf("timeout-close-%d", i), 200*time.Millisecond,
			nil, lateStreamClose, AttemptStateFailed, ReasonDeadlineExceeded)
	}
}

func TestWatch_StreamCloseAfterCancel_ReportsCancelled(t *testing.T) {
	for i := 0; i < 40; i++ {
		runLateBackendTerminal(t, fmt.Sprintf("cancel-close-%d", i), 0,
			cancelAttemptFn(t), lateStreamClose, AttemptStateCancelled, ReasonUserCancel)
	}
}

// runWatchOpenFailsAfterEnd ends the attempt while JobClient.Watch is still
// opening the stream, then fails that Watch call. The loop's Watch() error
// exit must report the recorded terminal rather than ending without one.
func runWatchOpenFailsAfterEnd(
	t *testing.T, attemptID string, attemptTimeout time.Duration,
	end func(*runtimeImpl, AttemptHandle), wantState AttemptState, wantReason string,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	opening := make(chan struct{})
	release := make(chan struct{})
	deleted := make(chan struct{}, 1)
	client := &fakeJobClient{
		watchFn: func(wctx context.Context, _ BackendRef) (JobWatch, error) {
			close(opening)
			select {
			case <-release:
			case <-wctx.Done():
			}
			return JobWatch{}, errors.New("watch open failed")
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
	waitFor(t, opening, 5*time.Second, "watch loop to call JobClient.Watch")

	if end == nil {
		waitFor(t, deleted, 5*time.Second, "AttemptTimeout cleanup Delete")
	} else {
		end(rt, h)
	}
	close(release)

	assertRecordedTerminal(t, ctx, rt, attemptID, out, wantState, wantReason)
}

func TestWatch_WatchOpenErrAfterAttemptTimeout_ReportsDeadlineExceeded(t *testing.T) {
	runWatchOpenFailsAfterEnd(t, "timeout-open", 100*time.Millisecond,
		nil, AttemptStateFailed, ReasonDeadlineExceeded)
}

func TestWatch_WatchOpenErrAfterCancel_ReportsCancelled(t *testing.T) {
	runWatchOpenFailsAfterEnd(t, "cancel-open", 0,
		cancelAttemptFn(t), AttemptStateCancelled, ReasonUserCancel)
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
