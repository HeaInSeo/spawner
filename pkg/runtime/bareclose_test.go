package runtime

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// spawner #18: a live attempt's JobWatch.Events closing with no pending error
// (a bare close) is a temporary watch-disconnected. The Runtime reconnects the
// same BackendRef after the existing backoff; it never synthesizes a terminal,
// decrements active or opens a replacement attempt. AttemptTimeout,
// CancelAttempt and ctx keep ownership of termination.

// bareCloseClient serves a scripted sequence of watches. Each call to Watch
// returns the next script entry; once the scripts run out, every further watch
// is a bare close. It records the BackendRef of every Watch call and counts
// Create calls.
type bareCloseClient struct {
	fakeJobClient
	mu      sync.Mutex
	scripts []func() JobWatch
	refs    []BackendRef
	creates atomic.Int32
	deletes atomic.Int32
}

func bareClosedWatch() JobWatch {
	events := make(chan JobEvent)
	errs := make(chan JobWatchError)
	close(events)
	close(errs)
	return JobWatch{Events: events, Errs: errs}
}

func newBareCloseClient(scripts ...func() JobWatch) *bareCloseClient {
	c := &bareCloseClient{scripts: scripts}
	c.createFn = func(_ context.Context, req JobCreateRequest) (BackendRef, error) {
		c.creates.Add(1)
		return NewK8sJobBackendRef(req.Namespace, req.JobName, "fake-uid"), nil
	}
	c.watchFn = func(_ context.Context, ref BackendRef) (JobWatch, error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.refs = append(c.refs, ref)
		if len(c.scripts) == 0 {
			return bareClosedWatch(), nil
		}
		next := c.scripts[0]
		c.scripts = c.scripts[1:]
		return next(), nil
	}
	c.deleteFn = func(context.Context, BackendRef) error {
		c.deletes.Add(1)
		return nil
	}
	return c
}

func (c *bareCloseClient) watchRefs() []BackendRef {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]BackendRef(nil), c.refs...)
}

// assertSameBackendRef checks every Watch targeted the one submitted Job and
// that no replacement Job was created.
func (c *bareCloseClient) assertSameBackendRef(t *testing.T, want BackendRef, minWatches int) {
	t.Helper()
	refs := c.watchRefs()
	if len(refs) < minWatches {
		t.Fatalf("Watch calls = %d, want >= %d (bare close must reconnect)", len(refs), minWatches)
	}
	for i, ref := range refs {
		if !reflect.DeepEqual(ref, want) {
			t.Fatalf("Watch call %d targeted %+v, want the submitted BackendRef %+v", i, ref, want)
		}
	}
	if n := c.creates.Load(); n != 1 {
		t.Fatalf("Create calls = %d, want 1 (no replacement attempt)", n)
	}
}

// (1) A bare close followed by a watch that reports the terminal: the watcher
// stays open across the reconnect and sees Submitted … Succeeded, once, last.
func TestWatch_BareCloseThenTerminal_ReconnectsSameBackendRef(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := newBareCloseClient(
		func() JobWatch {
			return makeJobWatch([]JobEvent{{State: AttemptStateRunning, Timestamp: time.Now()}}, nil)
		},
		func() JobWatch {
			return makeJobWatch([]JobEvent{{State: AttemptStateSucceeded, Timestamp: time.Now()}}, nil)
		},
	)
	rt := newTestRuntime(t, client)
	h, err := rt.SubmitAttempt(ctx, minimalReq("bare-close-terminal"))
	if err != nil {
		t.Fatalf("SubmitAttempt: %v", err)
	}
	out, err := rt.WatchAttempt(ctx, h)
	if err != nil {
		t.Fatalf("WatchAttempt: %v", err)
	}
	assertRecordedTerminal(t, ctx, rt, "bare-close-terminal", out, AttemptStateSucceeded, "")
	client.assertSameBackendRef(t, h.BackendRef, 2)
	if n := client.deletes.Load(); n != 0 {
		t.Fatalf("Delete calls = %d, want 0 for a backend-reported terminal", n)
	}
}

// (2)/(3) Repeated bare closes, then AttemptTimeout or CancelAttempt ends the
// attempt: exactly one terminal, last, carrying the recorded outcome.
func runRepeatedBareCloseThenEnd(
	t *testing.T, attemptID string, attemptTimeout time.Duration,
	end func(*runtimeImpl, AttemptHandle), wantState AttemptState, wantReason string,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := newBareCloseClient() // every watch is a bare close
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
	if end != nil {
		waitUntil(t, 5*time.Second, "two bare-close reconnects", func() bool {
			return len(client.watchRefs()) >= 2
		})
		end(rt, h)
	}
	assertRecordedTerminal(t, ctx, rt, attemptID, out, wantState, wantReason)
	client.assertSameBackendRef(t, h.BackendRef, 2)
}

func TestWatch_RepeatedBareCloseThenAttemptTimeout_ReportsDeadlineExceeded(t *testing.T) {
	runRepeatedBareCloseThenEnd(t, "bare-close-timeout", 1200*time.Millisecond,
		nil, AttemptStateFailed, ReasonDeadlineExceeded)
}

func TestWatch_RepeatedBareCloseThenCancel_ReportsCancelled(t *testing.T) {
	runRepeatedBareCloseThenEnd(t, "bare-close-cancel", 0,
		cancelAttemptFn(t), AttemptStateCancelled, ReasonUserCancel)
}

// (4) Context cancel during bare-close reconnects closes the watcher without a
// synthetic terminal and leaves the attempt live and counted active.
func TestWatch_BareCloseThenContextCancel_NoSyntheticTerminal(t *testing.T) {
	client := newBareCloseClient()
	rt := newTestRuntime(t, client)
	h, err := rt.SubmitAttempt(context.Background(), minimalReq("bare-close-ctx"))
	if err != nil {
		t.Fatalf("SubmitAttempt: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	out, err := rt.WatchAttempt(ctx, h)
	if err != nil {
		t.Fatalf("WatchAttempt: %v", err)
	}
	waitUntil(t, 5*time.Second, "a bare-close reconnect", func() bool {
		return len(client.watchRefs()) >= 2
	})
	cancel()

	drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer drainCancel()
	evs := collectEvents(drainCtx, out)
	if drainCtx.Err() != nil {
		t.Fatalf("watcher did not close after ctx cancel: %v", evs)
	}
	for _, ev := range evs {
		if ev.State.IsTerminal() {
			t.Fatalf("ctx cancel produced a terminal event %v; termination belongs to timeout/cancel only", ev)
		}
	}
	rt.mu.RLock()
	entry := rt.attempts["bare-close-ctx"]
	active := rt.active
	rt.mu.RUnlock()
	entry.mu.Lock()
	state := entry.state
	entry.mu.Unlock()
	if state.IsTerminal() {
		t.Fatalf("stored state = %s, want the attempt still live", state)
	}
	if active != 1 {
		t.Fatalf("active = %d, want 1 (no decrement without a terminal)", active)
	}
	client.assertSameBackendRef(t, h.BackendRef, 2)
}

// (5) Many attempts with interleaved bare closes: every attempt ends with one
// terminal and active returns to exactly zero (no double decrement).
func TestWatch_BareCloseAcrossAttempts_ActiveNeverDoubleDecrements(t *testing.T) {
	const n = 4
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var calls sync.Map // attemptID → *atomic.Int32
	client := &fakeJobClient{
		watchFn: func(_ context.Context, ref BackendRef) (JobWatch, error) {
			v, _ := calls.LoadOrStore(ref.ID, new(atomic.Int32))
			if v.(*atomic.Int32).Add(1) == 1 {
				return bareClosedWatch(), nil
			}
			return makeJobWatch([]JobEvent{{State: AttemptStateSucceeded, Timestamp: time.Now()}}, nil), nil
		},
	}
	rt := newTestRuntime(t, client)
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		h, err := rt.SubmitAttempt(ctx, minimalReq(fmt.Sprintf("bare-close-many-%d", i)))
		if err != nil {
			t.Fatalf("SubmitAttempt: %v", err)
		}
		out, err := rt.WatchAttempt(ctx, h)
		if err != nil {
			t.Fatalf("WatchAttempt: %v", err)
		}
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			evs := collectEvents(ctx, out)
			if len(evs) == 0 || !evs[len(evs)-1].State.IsTerminal() {
				errs <- fmt.Errorf("%s: events = %v, want a terminal last", id, evs)
			}
		}(h.AttemptID)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	rt.mu.RLock()
	active := rt.active
	rt.mu.RUnlock()
	if active != 0 {
		t.Fatalf("active = %d, want 0", active)
	}
}
