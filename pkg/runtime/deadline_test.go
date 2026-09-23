package runtime

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Layer-1 evidence for spawner #8: the Runtime-owned CreateTimeout and
// DeleteTimeout must reach a blocking JobClient as a real context deadline,
// the blocked call must be released by that deadline (not by a test sleep),
// and neither must be shortened or cancelled by the caller's ctx. Watch is the
// contrast case: it is caller-owned, so the caller's cancellation reaches it.
//
// Every fake below blocks on <-ctx.Done() and reports what it observed, so a
// missing or wrong deadline shows up as a hang (bounded by waitFor) or as a
// wrong ctx.Err(), never as a silent PASS.

// ctxObservation is what a blocking fake saw on the context it was given.
type ctxObservation struct {
	callStart   time.Time
	deadline    time.Time
	hasDeadline bool
	err         error // ctx.Err() after <-ctx.Done()
}

// observeUntilDone records the ctx deadline, blocks until ctx is done, and
// returns the observation. started is closed once the call is in flight.
func observeUntilDone(ctx context.Context, started chan<- struct{}) ctxObservation {
	obs := ctxObservation{callStart: time.Now()}
	obs.deadline, obs.hasDeadline = ctx.Deadline()
	if started != nil {
		close(started)
	}
	<-ctx.Done()
	obs.err = ctx.Err()
	return obs
}

func waitFor[T any](t *testing.T, ch <-chan T, d time.Duration, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(d):
		t.Fatalf("timed out after %v waiting for %s", d, what)
		var zero T
		return zero
	}
}

// assertDeadlineBound checks that the observed deadline is the configured
// timeout measured from (just before) the call, not a caller or default bound.
func assertDeadlineBound(t *testing.T, obs ctxObservation, timeout time.Duration, what string) {
	t.Helper()
	if !obs.hasDeadline {
		t.Fatalf("%s: ctx has no deadline; the Runtime-owned timeout was not applied", what)
	}
	remaining := obs.deadline.Sub(obs.callStart)
	if remaining > timeout {
		t.Errorf("%s: deadline %v after call start exceeds configured timeout %v", what, remaining, timeout)
	}
	if remaining < timeout/2 {
		t.Errorf("%s: deadline %v after call start is far below configured timeout %v", what, remaining, timeout)
	}
	if !errors.Is(obs.err, context.DeadlineExceeded) {
		t.Errorf("%s: ctx.Err() = %v, want context.DeadlineExceeded", what, obs.err)
	}
}

func assertAttemptReleased(t *testing.T, rt *runtimeImpl, attemptID string) {
	t.Helper()
	rt.mu.RLock()
	active := rt.active
	_, inMap := rt.attempts[attemptID]
	rt.mu.RUnlock()
	if active != 0 {
		t.Errorf("active = %d, want 0", active)
	}
	if inMap {
		t.Errorf("attempt %q still registered after Create failure", attemptID)
	}
}

// ── CreateTimeout ─────────────────────────────────────────────────────────────

func TestCreateTimeout_DeadlineReleasesBlockingCreate(t *testing.T) {
	const createTimeout = 150 * time.Millisecond

	observed := make(chan ctxObservation, 1)
	client := &fakeJobClient{
		createFn: func(ctx context.Context, _ JobCreateRequest) (BackendRef, error) {
			obs := observeUntilDone(ctx, nil)
			observed <- obs
			return BackendRef{}, obs.err
		},
	}
	rt := newTestRuntime(t, client, func(c *RuntimeConfig) {
		c.CreateTimeout = createTimeout
		c.SubmitTimeout = 10 * time.Second // must not be what ends the call
	})

	errCh := make(chan error, 1)
	go func() {
		_, err := rt.SubmitAttempt(context.Background(), minimalReq("a1"))
		errCh <- err
	}()

	obs := waitFor(t, observed, 5*time.Second, "blocking Create to be released by CreateTimeout")
	assertDeadlineBound(t, obs, createTimeout, "Create")

	err := waitFor(t, errCh, 5*time.Second, "SubmitAttempt to return")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("SubmitAttempt err = %v, want the Create deadline error", err)
	}
	assertAttemptReleased(t, rt, "a1")
}

func TestCreateTimeout_CallerCancelDoesNotCancelCreate_SameAttemptRetry(t *testing.T) {
	// The caller abandoning SubmitAttempt yields ErrSubmitOutcomeUnknown but
	// must leave the in-flight Create running on the Runtime-owned ctx until
	// CreateTimeout. A retry for the same AttemptID afterwards must re-issue
	// Create with the identical deterministic identity.
	const createTimeout = 1 * time.Second

	type createCall struct {
		ctx context.Context
		req JobCreateRequest
	}
	calls := make(chan createCall, 2)
	started := make(chan struct{})
	observed := make(chan ctxObservation, 1)
	n := 0
	client := &fakeJobClient{
		createFn: func(ctx context.Context, req JobCreateRequest) (BackendRef, error) {
			n++ // createFn calls are serialised by the test (second only after first returns)
			calls <- createCall{ctx: ctx, req: req}
			if n == 1 {
				obs := observeUntilDone(ctx, started)
				observed <- obs
				return BackendRef{}, obs.err
			}
			return NewK8sJobBackendRef(req.Namespace, req.JobName, "uid-retry"), nil
		},
	}
	rt := newTestRuntime(t, client, func(c *RuntimeConfig) {
		c.CreateTimeout = createTimeout
		c.SubmitTimeout = 10 * time.Second
	})

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := rt.SubmitAttempt(ctx, minimalReq("a1"))
		errCh <- err
	}()

	first := waitFor(t, calls, 5*time.Second, "first Create call")
	waitFor(t, started, 5*time.Second, "first Create to start blocking")
	cancel()

	if err := waitFor(t, errCh, 5*time.Second, "abandoned SubmitAttempt"); !errors.Is(err, ErrSubmitOutcomeUnknown) {
		t.Fatalf("abandoned SubmitAttempt err = %v, want ErrSubmitOutcomeUnknown", err)
	}
	if err := first.ctx.Err(); err != nil {
		t.Fatalf("Create ctx was cancelled with the caller (%v); it must be Runtime-owned", err)
	}

	obs := waitFor(t, observed, 5*time.Second, "first Create to be released by CreateTimeout")
	assertDeadlineBound(t, obs, createTimeout, "Create after caller cancel")
	// runCreate unregisters the failed entry after Create returns; wait for it
	// so the retry below starts a fresh Create instead of joining the old one.
	deadline := time.Now().Add(5 * time.Second)
	for {
		rt.mu.RLock()
		_, inMap := rt.attempts["a1"]
		rt.mu.RUnlock()
		if !inMap {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("failed Create was not unregistered")
		}
		time.Sleep(time.Millisecond)
	}
	assertAttemptReleased(t, rt, "a1")

	h, err := rt.SubmitAttempt(context.Background(), minimalReq("a1"))
	if err != nil {
		t.Fatalf("same-Attempt retry: %v", err)
	}
	if h.BackendRef.UID != "uid-retry" {
		t.Errorf("retry handle UID = %q, want uid-retry", h.BackendRef.UID)
	}
	second := waitFor(t, calls, time.Second, "retry Create call")
	if second.req.JobName != first.req.JobName || second.req.AttemptMarker != first.req.AttemptMarker {
		t.Errorf("retry identity (%q, %q) differs from first attempt (%q, %q)",
			second.req.JobName, second.req.AttemptMarker, first.req.JobName, first.req.AttemptMarker)
	}
}

func TestCreateTimeout_EarlyCompletionReleasesContext(t *testing.T) {
	// A Create that finishes well before CreateTimeout must have its context
	// cancelled on return, so the timer is not left running for the full timeout.
	ctxCh := make(chan context.Context, 1)
	client := &fakeJobClient{
		createFn: func(ctx context.Context, req JobCreateRequest) (BackendRef, error) {
			ctxCh <- ctx
			return NewK8sJobBackendRef(req.Namespace, req.JobName, "uid"), nil
		},
	}
	rt := newTestRuntime(t, client, func(c *RuntimeConfig) {
		c.CreateTimeout = time.Minute
	})

	if _, err := rt.SubmitAttempt(context.Background(), minimalReq("a1")); err != nil {
		t.Fatalf("SubmitAttempt: %v", err)
	}
	createCtx := <-ctxCh
	waitFor(t, createCtx.Done(), 5*time.Second, "Create ctx to be released after early completion")
	if !errors.Is(createCtx.Err(), context.Canceled) {
		t.Errorf("Create ctx err = %v, want context.Canceled (released, not expired)", createCtx.Err())
	}
}

// ── DeleteTimeout ─────────────────────────────────────────────────────────────

func TestDeleteTimeout_AttemptTimeoutCleanupDeleteIsBounded(t *testing.T) {
	const deleteTimeout = 150 * time.Millisecond

	observed := make(chan ctxObservation, 1)
	var deletedRef BackendRef
	client := &fakeJobClient{
		deleteFn: func(ctx context.Context, ref BackendRef) error {
			deletedRef = ref
			obs := observeUntilDone(ctx, nil)
			observed <- obs
			return obs.err
		},
	}
	rt := newTestRuntime(t, client, func(c *RuntimeConfig) {
		c.DeleteTimeout = deleteTimeout
	})

	req := minimalReq("a1")
	req.AttemptTimeout = 20 * time.Millisecond
	h, err := rt.SubmitAttempt(context.Background(), req)
	if err != nil {
		t.Fatalf("SubmitAttempt: %v", err)
	}

	obs := waitFor(t, observed, 5*time.Second, "cleanup Delete to be released by DeleteTimeout")
	assertDeadlineBound(t, obs, deleteTimeout, "timeout cleanup Delete")
	if deletedRef.ID != h.BackendRef.ID {
		t.Errorf("Delete ref = %q, want %q", deletedRef.ID, h.BackendRef.ID)
	}

	rt.mu.RLock()
	entry := rt.attempts["a1"]
	active := rt.active
	rt.mu.RUnlock()
	entry.mu.Lock()
	state, reason := entry.state, entry.terminalReason
	entry.mu.Unlock()
	if state != AttemptStateFailed || reason != ReasonDeadlineExceeded {
		t.Errorf("terminal = (%v, %q), want (%v, %q)", state, reason, AttemptStateFailed, ReasonDeadlineExceeded)
	}
	if active != 0 {
		t.Errorf("active = %d after timeout, want 0", active)
	}
}

func TestDeleteTimeout_CancelAttemptIgnoresCallerCtx(t *testing.T) {
	// CancelAttempt's Delete runs on the Runtime-owned ctx: an already-cancelled
	// caller ctx must neither skip nor cut short the Delete; only DeleteTimeout
	// ends it, and CancelAttempt returns once that bounded Delete returns.
	const deleteTimeout = 150 * time.Millisecond

	observed := make(chan ctxObservation, 1)
	client := &fakeJobClient{
		deleteFn: func(ctx context.Context, _ BackendRef) error {
			obs := observeUntilDone(ctx, nil)
			observed <- obs
			return obs.err
		},
	}
	rt := newTestRuntime(t, client, func(c *RuntimeConfig) {
		c.DeleteTimeout = deleteTimeout
	})

	h, err := rt.SubmitAttempt(context.Background(), minimalReq("a1"))
	if err != nil {
		t.Fatalf("SubmitAttempt: %v", err)
	}

	callerCtx, cancel := context.WithCancel(context.Background())
	cancel()

	cancelErr := make(chan error, 1)
	go func() { cancelErr <- rt.CancelAttempt(callerCtx, h) }()

	obs := waitFor(t, observed, 5*time.Second, "CancelAttempt Delete to be released by DeleteTimeout")
	assertDeadlineBound(t, obs, deleteTimeout, "CancelAttempt Delete")
	if err := waitFor(t, cancelErr, 5*time.Second, "CancelAttempt to return"); err != nil {
		t.Fatalf("CancelAttempt: %v", err)
	}
}

func TestDeleteTimeout_EarlyCompletionReleasesContext(t *testing.T) {
	ctxCh := make(chan context.Context, 1)
	client := &fakeJobClient{
		deleteFn: func(ctx context.Context, _ BackendRef) error {
			ctxCh <- ctx
			return nil
		},
	}
	rt := newTestRuntime(t, client, func(c *RuntimeConfig) {
		c.DeleteTimeout = time.Minute
	})

	h, err := rt.SubmitAttempt(context.Background(), minimalReq("a1"))
	if err != nil {
		t.Fatalf("SubmitAttempt: %v", err)
	}
	if err := rt.CancelAttempt(context.Background(), h); err != nil {
		t.Fatalf("CancelAttempt: %v", err)
	}
	// CancelAttempt's deferred cancel has run by the time it returns.
	deleteCtx := waitFor(t, ctxCh, time.Second, "Delete call")
	if !errors.Is(deleteCtx.Err(), context.Canceled) {
		t.Errorf("Delete ctx err = %v, want context.Canceled (released, not expired)", deleteCtx.Err())
	}
}

// ── Watch contrast: caller-owned ──────────────────────────────────────────────

func TestWatch_CallerOwnedCtx_CancelReachesClientWithoutEndingAttempt(t *testing.T) {
	// Unlike Create/Delete, Watch is driven by the caller's ctx: the Runtime
	// adds no deadline of its own, and cancelling the caller's ctx cancels the
	// JobClient.Watch ctx and closes the event channel. The attempt itself is
	// not terminated by that cancellation.
	watchCtxCh := make(chan context.Context, 1)
	client := &fakeJobClient{
		watchFn: func(ctx context.Context, _ BackendRef) (JobWatch, error) {
			watchCtxCh <- ctx
			// Streams stay open; only ctx cancellation can end consumption.
			return JobWatch{Events: make(chan JobEvent), Errs: make(chan JobWatchError)}, nil
		},
	}
	rt := newTestRuntime(t, client)

	h, err := rt.SubmitAttempt(context.Background(), minimalReq("a1"))
	if err != nil {
		t.Fatalf("SubmitAttempt: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	events, err := rt.WatchAttempt(ctx, h)
	if err != nil {
		t.Fatalf("WatchAttempt: %v", err)
	}
	if ev := waitFor(t, events, 5*time.Second, "Submitted event"); ev.State != AttemptStateSubmitted {
		t.Fatalf("first event = %v, want Submitted", ev.State)
	}

	watchCtx := waitFor(t, watchCtxCh, 5*time.Second, "JobClient.Watch call")
	if _, ok := watchCtx.Deadline(); ok {
		t.Error("Watch ctx has a deadline; Watch must be caller-owned with no Runtime timeout")
	}
	if watchCtx.Err() != nil {
		t.Fatalf("Watch ctx done before caller cancel: %v", watchCtx.Err())
	}

	cancel()
	waitFor(t, watchCtx.Done(), 5*time.Second, "caller cancel to reach JobClient.Watch ctx")
	if !errors.Is(watchCtx.Err(), context.Canceled) {
		t.Errorf("Watch ctx err = %v, want context.Canceled", watchCtx.Err())
	}
	closed := make(chan struct{})
	go func() {
		for range events {
			continue // drain; the channel must close without further input
		}
		close(closed)
	}()
	waitFor(t, closed, 5*time.Second, "event channel to close after caller cancel")

	rt.mu.RLock()
	entry := rt.attempts["a1"]
	active := rt.active
	rt.mu.RUnlock()
	entry.mu.Lock()
	terminal := entry.state.IsTerminal()
	entry.mu.Unlock()
	if terminal || active != 1 {
		t.Errorf("attempt ended by watch cancel (terminal=%v active=%d); want non-terminal, active=1", terminal, active)
	}
}
