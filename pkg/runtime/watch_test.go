package runtime

import (
	"context"
	"errors"
	"testing"
	"time"
)

// collectEvents drains ch into a slice until it is closed or ctx expires.
func collectEvents(ctx context.Context, ch <-chan AttemptEvent) []AttemptEvent {
	var evs []AttemptEvent
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return evs
			}
			evs = append(evs, ev)
		case <-ctx.Done():
			return evs
		}
	}
}

// makeJobWatch constructs a JobWatch from pre-built slices of events and errors.
func makeJobWatch(events []JobEvent, errs []JobWatchError) JobWatch {
	evCh := make(chan JobEvent, len(events))
	errCh := make(chan JobWatchError, len(errs)+1)
	for _, e := range events {
		evCh <- e
	}
	for _, e := range errs {
		errCh <- e
	}
	close(evCh)
	close(errCh)
	return JobWatch{Events: evCh, Errs: errCh}
}

// ── WatchAttempt normal path ──────────────────────────────────────────────────

func TestWatchAttempt_SubmittedToSucceeded(t *testing.T) {
	jw := makeJobWatch([]JobEvent{
		{State: AttemptStatePending, Timestamp: time.Now()},
		{State: AttemptStateRunning, Timestamp: time.Now()},
		{State: AttemptStateSucceeded, Timestamp: time.Now()},
	}, nil)

	client := &fakeJobClient{
		watchFn: func(_ context.Context, _ BackendRef) (JobWatch, error) {
			return jw, nil
		},
	}
	rt := newTestRuntime(t, client)

	handle, err := rt.SubmitAttempt(context.Background(), minimalReq("a1"))
	if err != nil {
		t.Fatalf("SubmitAttempt: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ch, err := rt.WatchAttempt(ctx, handle)
	if err != nil {
		t.Fatalf("WatchAttempt: %v", err)
	}

	evs := collectEvents(ctx, ch)
	if len(evs) == 0 {
		t.Fatal("expected events, got none")
	}

	first := evs[0]
	if first.State != AttemptStateSubmitted {
		t.Errorf("first event = %s, want Submitted", first.State)
	}

	last := evs[len(evs)-1]
	if last.State != AttemptStateSucceeded {
		t.Errorf("last event = %s, want Succeeded", last.State)
	}

	// Channel must be closed after terminal event.
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("channel should be closed after terminal event")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("channel not closed after terminal event")
	}
}

func TestWatchAttempt_FailedWithReason(t *testing.T) {
	jw := makeJobWatch([]JobEvent{
		{State: AttemptStatePending, Timestamp: time.Now()},
		{State: AttemptStateFailed, Reason: ReasonOOMKilled, Message: "container OOM killed", Timestamp: time.Now()},
	}, nil)

	client := &fakeJobClient{
		watchFn: func(_ context.Context, _ BackendRef) (JobWatch, error) { return jw, nil },
	}
	rt := newTestRuntime(t, client)

	handle, _ := rt.SubmitAttempt(context.Background(), minimalReq("a1"))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ch, _ := rt.WatchAttempt(ctx, handle)
	evs := collectEvents(ctx, ch)

	last := evs[len(evs)-1]
	if last.State != AttemptStateFailed {
		t.Errorf("last state = %s, want Failed", last.State)
	}
	if last.Reason != ReasonOOMKilled {
		t.Errorf("Reason = %q, want %q", last.Reason, ReasonOOMKilled)
	}
}

func TestWatchAttempt_BackwardTransitionsDropped(t *testing.T) {
	// Running → Pending is a backward transition and should be ignored.
	jw := makeJobWatch([]JobEvent{
		{State: AttemptStateRunning, Timestamp: time.Now()},
		{State: AttemptStatePending, Timestamp: time.Now()}, // backward, drop
		{State: AttemptStateSucceeded, Timestamp: time.Now()},
	}, nil)

	client := &fakeJobClient{
		watchFn: func(_ context.Context, _ BackendRef) (JobWatch, error) { return jw, nil },
	}
	rt := newTestRuntime(t, client)
	handle, _ := rt.SubmitAttempt(context.Background(), minimalReq("a1"))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ch, _ := rt.WatchAttempt(ctx, handle)
	evs := collectEvents(ctx, ch)

	for _, ev := range evs {
		if ev.State == AttemptStatePending {
			t.Error("backward Pending event should have been dropped")
		}
	}
}

// ── WatchAttempt ctx cancellation ────────────────────────────────────────────

func TestWatchAttempt_CtxCancel_ClosesChannel(t *testing.T) {
	block := make(chan struct{})
	evCh := make(chan JobEvent)
	errCh := make(chan JobWatchError, 1)
	jw := JobWatch{Events: evCh, Errs: errCh}

	client := &fakeJobClient{
		watchFn: func(_ context.Context, _ BackendRef) (JobWatch, error) {
			<-block
			return jw, nil
		},
	}
	rt := newTestRuntime(t, client)
	handle, _ := rt.SubmitAttempt(context.Background(), minimalReq("a1"))

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := rt.WatchAttempt(ctx, handle)
	if err != nil {
		t.Fatalf("WatchAttempt: %v", err)
	}

	cancel()
	close(block)

	select {
	case _, ok := <-ch:
		if ok {
			// drain
			for range ch {
			}
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("channel not closed after ctx cancel")
	}
}

// ── Temporary watch error → reconnect ────────────────────────────────────────

func TestWatchAttempt_TemporaryError_Reconnects(t *testing.T) {
	callCount := 0

	client := &fakeJobClient{
		watchFn: func(_ context.Context, _ BackendRef) (JobWatch, error) {
			callCount++
			if callCount == 1 {
				// First watch: temporary error
				evCh := make(chan JobEvent)
				errCh := make(chan JobWatchError, 1)
				errCh <- JobWatchError{Reason: ReasonWatchDisconnected, Temporary: true}
				close(evCh)
				close(errCh)
				return JobWatch{Events: evCh, Errs: errCh}, nil
			}
			// Second watch: success
			return makeJobWatch([]JobEvent{
				{State: AttemptStateSucceeded, Timestamp: time.Now()},
			}, nil), nil
		},
	}
	rt := newTestRuntime(t, client, func(c *RuntimeConfig) {
		// No config changes needed; default backoff is 500ms which is fine for test
	})

	handle, _ := rt.SubmitAttempt(context.Background(), minimalReq("a1"))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch, _ := rt.WatchAttempt(ctx, handle)
	evs := collectEvents(ctx, ch)

	last := evs[len(evs)-1]
	if last.State != AttemptStateSucceeded {
		t.Errorf("last event = %s, want Succeeded (after reconnect)", last.State)
	}
	if callCount < 2 {
		t.Errorf("Watch should have been called at least twice (reconnect), got %d", callCount)
	}
}

// ── Unrecoverable watch error ─────────────────────────────────────────────────

func TestWatchAttempt_UnrecoverableError_EmitsFailed(t *testing.T) {
	jw := JobWatch{
		Events: func() chan JobEvent { ch := make(chan JobEvent); close(ch); return ch }(),
		Errs: func() chan JobWatchError {
			ch := make(chan JobWatchError, 1)
			ch <- JobWatchError{Reason: ReasonPermissionDenied, Temporary: false}
			close(ch)
			return ch
		}(),
	}

	client := &fakeJobClient{
		watchFn: func(_ context.Context, _ BackendRef) (JobWatch, error) { return jw, nil },
	}
	rt := newTestRuntime(t, client)
	handle, _ := rt.SubmitAttempt(context.Background(), minimalReq("a1"))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ch, _ := rt.WatchAttempt(ctx, handle)
	evs := collectEvents(ctx, ch)

	var failed *AttemptEvent
	for i := range evs {
		if evs[i].State == AttemptStateFailed {
			failed = &evs[i]
		}
	}
	if failed == nil {
		t.Fatal("expected Failed event on unrecoverable watch error")
	}
	if failed.Reason != ReasonPermissionDenied {
		t.Errorf("Reason = %q, want %q", failed.Reason, ReasonPermissionDenied)
	}
}

// ── CancelAttempt ─────────────────────────────────────────────────────────────

func TestCancelAttempt_NonTerminal_EmitsCancelled(t *testing.T) {
	// Watch blocks until cancelled.
	evCh := make(chan JobEvent)
	errCh := make(chan JobWatchError, 1)
	jw := JobWatch{Events: evCh, Errs: errCh}

	client := &fakeJobClient{
		watchFn: func(_ context.Context, _ BackendRef) (JobWatch, error) {
			return jw, nil
		},
	}
	rt := newTestRuntime(t, client)
	handle, _ := rt.SubmitAttempt(context.Background(), minimalReq("a1"))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ch, _ := rt.WatchAttempt(ctx, handle)

	// Let watchLoop start and emit Submitted.
	time.Sleep(20 * time.Millisecond)

	if err := rt.CancelAttempt(context.Background(), handle); err != nil {
		t.Fatalf("CancelAttempt: %v", err)
	}

	evs := collectEvents(ctx, ch)

	var cancelled *AttemptEvent
	for i := range evs {
		if evs[i].State == AttemptStateCancelled {
			cancelled = &evs[i]
		}
	}
	if cancelled == nil {
		t.Fatalf("expected Cancelled event, got: %v", evs)
	}
	if cancelled.Reason != ReasonUserCancel {
		t.Errorf("Reason = %q, want %q", cancelled.Reason, ReasonUserCancel)
	}
}

func TestCancelAttempt_AlreadySucceeded_NoOp(t *testing.T) {
	jw := makeJobWatch([]JobEvent{
		{State: AttemptStateSucceeded, Timestamp: time.Now()},
	}, nil)

	client := &fakeJobClient{
		watchFn: func(_ context.Context, _ BackendRef) (JobWatch, error) { return jw, nil },
		deleteFn: func(_ context.Context, _ BackendRef) error {
			return errors.New("delete should not be called after Succeeded")
		},
	}
	rt := newTestRuntime(t, client)
	handle, _ := rt.SubmitAttempt(context.Background(), minimalReq("a1"))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ch, _ := rt.WatchAttempt(ctx, handle)
	evs := collectEvents(ctx, ch)

	// Confirm Succeeded was the terminal state.
	last := evs[len(evs)-1]
	if last.State != AttemptStateSucceeded {
		t.Fatalf("expected Succeeded, got %s", last.State)
	}

	// CancelAttempt after terminal must be no-op.
	if err := rt.CancelAttempt(context.Background(), handle); err != nil {
		t.Fatalf("CancelAttempt after Succeeded: %v", err)
	}

	// active should be 0 (decremented by natural completion, not by cancel).
	rt.mu.RLock()
	active := rt.active
	rt.mu.RUnlock()
	if active != 0 {
		t.Errorf("active = %d after Succeeded+CancelAttempt, want 0", active)
	}
}

func TestCancelAttempt_AlreadyCancelled_NoOp(t *testing.T) {
	evCh := make(chan JobEvent)
	errCh := make(chan JobWatchError, 1)
	jw := JobWatch{Events: evCh, Errs: errCh}

	client := &fakeJobClient{
		watchFn: func(_ context.Context, _ BackendRef) (JobWatch, error) { return jw, nil },
	}
	rt := newTestRuntime(t, client)
	handle, _ := rt.SubmitAttempt(context.Background(), minimalReq("a1"))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	rt.WatchAttempt(ctx, handle) //nolint
	time.Sleep(10 * time.Millisecond)

	rt.CancelAttempt(context.Background(), handle) //nolint

	// Second cancel must not error or double-decrement active.
	if err := rt.CancelAttempt(context.Background(), handle); err != nil {
		t.Fatalf("second CancelAttempt: %v", err)
	}

	rt.mu.RLock()
	active := rt.active
	rt.mu.RUnlock()
	if active != 0 {
		t.Errorf("active = %d after double cancel, want 0", active)
	}
}

// ── WatchAttempt on already-terminal entry ────────────────────────────────────

func TestWatchAttempt_AlreadyTerminal_EmitsAndCloses(t *testing.T) {
	jw := makeJobWatch([]JobEvent{
		{State: AttemptStateSucceeded, Timestamp: time.Now()},
	}, nil)

	client := &fakeJobClient{
		watchFn: func(_ context.Context, _ BackendRef) (JobWatch, error) { return jw, nil },
	}
	rt := newTestRuntime(t, client)
	handle, _ := rt.SubmitAttempt(context.Background(), minimalReq("a1"))

	// First watcher: wait for Succeeded.
	ctx1, cancel1 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel1()
	ch1, _ := rt.WatchAttempt(ctx1, handle)
	collectEvents(ctx1, ch1) // drain

	// Second watcher: attempt is already terminal.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel2()
	ch2, err := rt.WatchAttempt(ctx2, handle)
	if err != nil {
		t.Fatalf("WatchAttempt on terminal entry: %v", err)
	}
	evs := collectEvents(ctx2, ch2)
	if len(evs) == 0 {
		t.Fatal("expected terminal event from already-terminal entry")
	}
	if evs[0].State != AttemptStateSucceeded {
		t.Errorf("event = %s, want Succeeded", evs[0].State)
	}
}

// ── AttemptTimeout emits via watchLoop ───────────────────────────────────────

func TestWatchAttempt_TimeoutEmitsFailed(t *testing.T) {
	evCh := make(chan JobEvent) // never sends
	errCh := make(chan JobWatchError, 1)
	jw := JobWatch{Events: evCh, Errs: errCh}

	client := &fakeJobClient{
		watchFn: func(_ context.Context, _ BackendRef) (JobWatch, error) { return jw, nil },
	}
	rt := newTestRuntime(t, client)

	req := minimalReq("a1")
	req.AttemptTimeout = 100 * time.Millisecond

	handle, err := rt.SubmitAttempt(context.Background(), req)
	if err != nil {
		t.Fatalf("SubmitAttempt: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ch, _ := rt.WatchAttempt(ctx, handle)
	evs := collectEvents(ctx, ch)

	var failed *AttemptEvent
	for i := range evs {
		if evs[i].State == AttemptStateFailed {
			failed = &evs[i]
		}
	}
	if failed == nil {
		t.Fatalf("expected Failed event from timeout, got: %v", evs)
	}
	if failed.Reason != ReasonDeadlineExceeded {
		t.Errorf("Reason = %q, want %q", failed.Reason, ReasonDeadlineExceeded)
	}
}
