package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// ── fake JobClient ────────────────────────────────────────────────────────────

type fakeJobClient struct {
	mu sync.Mutex

	createFn   func(ctx context.Context, req JobCreateRequest) (BackendRef, error)
	watchFn    func(ctx context.Context, ref BackendRef) (JobWatch, error)
	deleteFn   func(ctx context.Context, ref BackendRef) error
	snapshotFn func(ctx context.Context, ref BackendRef) (JobSnapshot, error)
}

func (f *fakeJobClient) Create(ctx context.Context, req JobCreateRequest) (BackendRef, error) {
	f.mu.Lock()
	fn := f.createFn
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, req)
	}
	return NewK8sJobBackendRef(req.Namespace, req.JobName, "fake-uid"), nil
}

func (f *fakeJobClient) Watch(ctx context.Context, ref BackendRef) (JobWatch, error) {
	f.mu.Lock()
	fn := f.watchFn
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, ref)
	}
	events := make(chan JobEvent)
	errs := make(chan JobWatchError, 1)
	close(events)
	close(errs)
	return JobWatch{Events: events, Errs: errs}, nil
}

func (f *fakeJobClient) Delete(ctx context.Context, ref BackendRef) error {
	f.mu.Lock()
	fn := f.deleteFn
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, ref)
	}
	return nil
}

func (f *fakeJobClient) Snapshot(ctx context.Context, ref BackendRef) (JobSnapshot, error) {
	f.mu.Lock()
	fn := f.snapshotFn
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, ref)
	}
	return JobSnapshot{Exists: false}, nil
}

// ── helpers ───────────────────────────────────────────────────────────────────

func newTestRuntime(t *testing.T, client JobClient, overrides ...func(*RuntimeConfig)) *runtimeImpl {
	t.Helper()
	cfg := RuntimeConfig{
		NamingSalt:     "test-salt-stable",
		Namespace:      "test-ns",
		MaxConcurrency: 5,
		CreateTimeout:  5 * time.Second,
		DeleteTimeout:  5 * time.Second,
		SubmitTimeout:  10 * time.Second,
	}
	for _, fn := range overrides {
		fn(&cfg)
	}
	rt, err := NewRuntime(client, cfg)
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	return rt.(*runtimeImpl)
}

func minimalReq(attemptID string) AttemptRequest {
	return AttemptRequest{
		AttemptID: attemptID,
		ImageRef:  "busybox:1.36",
		Command:   []string{"sh", "-c", "echo hi"},
	}
}

// ── NewRuntime constructor ────────────────────────────────────────────────────

func TestNewRuntime_MissingNamingSalt(t *testing.T) {
	_, err := NewRuntime(&fakeJobClient{}, RuntimeConfig{Namespace: "ns"})
	if err == nil {
		t.Fatal("expected error for missing NamingSalt")
	}
}

func TestNewRuntime_MissingNamespace(t *testing.T) {
	_, err := NewRuntime(&fakeJobClient{}, RuntimeConfig{NamingSalt: "s"})
	if err == nil {
		t.Fatal("expected error for missing Namespace")
	}
}

// ── SubmitAttempt happy path ──────────────────────────────────────────────────

func TestSubmitAttempt_HappyPath(t *testing.T) {
	client := &fakeJobClient{}
	rt := newTestRuntime(t, client)

	handle, err := rt.SubmitAttempt(context.Background(), minimalReq("a1"))
	if err != nil {
		t.Fatalf("SubmitAttempt: %v", err)
	}
	if handle.AttemptID != "a1" {
		t.Errorf("AttemptID = %q, want a1", handle.AttemptID)
	}
	if handle.BackendRef.ID == "" {
		t.Error("BackendRef.ID must not be empty")
	}
	if handle.BackendRef.Kind != "k8s-job" {
		t.Errorf("BackendRef.Kind = %q, want k8s-job", handle.BackendRef.Kind)
	}
	if err := handle.BackendRef.Validate(); err != nil {
		t.Errorf("BackendRef.Validate: %v", err)
	}
}

func TestSubmitAttempt_JobNameDeterministic(t *testing.T) {
	var capturedReqs []JobCreateRequest
	var mu sync.Mutex

	client := &fakeJobClient{
		createFn: func(ctx context.Context, req JobCreateRequest) (BackendRef, error) {
			mu.Lock()
			capturedReqs = append(capturedReqs, req)
			mu.Unlock()
			return NewK8sJobBackendRef(req.Namespace, req.JobName, "uid"), nil
		},
	}
	rt := newTestRuntime(t, client)

	// Submit twice with different attempts; verify names are deterministic.
	h1, _ := rt.SubmitAttempt(context.Background(), minimalReq("attempt-001"))
	h2, _ := rt.SubmitAttempt(context.Background(), minimalReq("attempt-002"))

	if h1.BackendRef.Name == h2.BackendRef.Name {
		t.Error("different AttemptIDs should produce different JobNames")
	}

	mu.Lock()
	defer mu.Unlock()
	for _, r := range capturedReqs {
		expected := jobNameFor("test-salt-stable", r.AttemptID)
		if r.JobName != expected {
			t.Errorf("JobName = %q, want %q", r.JobName, expected)
		}
		if r.Namespace != "test-ns" {
			t.Errorf("Namespace = %q, want test-ns", r.Namespace)
		}
		if r.AttemptMarker != attemptMarkerFor("test-salt-stable", r.AttemptID) {
			t.Errorf("AttemptMarker mismatch for %s", r.AttemptID)
		}
	}
}

func TestSubmitAttempt_ValidationError(t *testing.T) {
	rt := newTestRuntime(t, &fakeJobClient{})

	// Missing AttemptID
	_, err := rt.SubmitAttempt(context.Background(), AttemptRequest{ImageRef: "img"})
	if err == nil {
		t.Fatal("expected validation error for missing AttemptID")
	}

	// Missing ImageRef
	_, err = rt.SubmitAttempt(context.Background(), AttemptRequest{AttemptID: "a1"})
	if err == nil {
		t.Fatal("expected validation error for missing ImageRef")
	}
}

// ── ErrSubmitOutcomeUnknown ───────────────────────────────────────────────────

func TestSubmitAttempt_ErrSubmitOutcomeUnknown_CtxCancelledBeforeCreate(t *testing.T) {
	started := make(chan struct{})
	block := make(chan struct{})

	client := &fakeJobClient{
		createFn: func(ctx context.Context, req JobCreateRequest) (BackendRef, error) {
			close(started)
			<-block // block until test unblocks
			return NewK8sJobBackendRef(req.Namespace, req.JobName, "uid"), nil
		},
	}
	rt := newTestRuntime(t, client)

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		_, err := rt.SubmitAttempt(ctx, minimalReq("a1"))
		errCh <- err
	}()

	<-started // Create is in progress
	cancel()  // cancel caller ctx

	err := <-errCh
	if !errors.Is(err, ErrSubmitOutcomeUnknown) {
		t.Fatalf("expected ErrSubmitOutcomeUnknown, got %v", err)
	}

	close(block) // let Create finish (cleanup)
}

func TestSubmitAttempt_RetryAfterOutcomeUnknown_RecoversHandle(t *testing.T) {
	// Simulate: first caller's ctx cancelled, Create succeeds, second caller retries.
	started := make(chan struct{})
	proceed := make(chan struct{})

	client := &fakeJobClient{
		createFn: func(ctx context.Context, req JobCreateRequest) (BackendRef, error) {
			close(started)
			<-proceed
			return NewK8sJobBackendRef(req.Namespace, req.JobName, "uid-recovered"), nil
		},
	}
	rt := newTestRuntime(t, client)

	ctx1, cancel1 := context.WithCancel(context.Background())

	h1ch := make(chan error, 1)
	go func() {
		_, err := rt.SubmitAttempt(ctx1, minimalReq("a1"))
		h1ch <- err
	}()

	<-started // Create is blocking
	cancel1() // caller 1 abandons

	err1 := <-h1ch
	if !errors.Is(err1, ErrSubmitOutcomeUnknown) {
		t.Fatalf("first caller: expected ErrSubmitOutcomeUnknown, got %v", err1)
	}

	// Retry from caller 2 (same AttemptID) while Create is still in progress.
	h2ch := make(chan AttemptHandle, 1)
	errc2 := make(chan error, 1)
	go func() {
		h, err := rt.SubmitAttempt(context.Background(), minimalReq("a1"))
		h2ch <- h
		errc2 <- err
	}()

	// Now let Create finish.
	close(proceed)

	err2 := <-errc2
	if err2 != nil {
		t.Fatalf("retry caller: unexpected error: %v", err2)
	}
	h2 := <-h2ch
	if h2.BackendRef.UID != "uid-recovered" {
		t.Errorf("expected recovered UID, got %q", h2.BackendRef.UID)
	}
}

// ── ErrCapacityExceeded ───────────────────────────────────────────────────────

func TestSubmitAttempt_ErrCapacityExceeded(t *testing.T) {
	block := make(chan struct{})

	client := &fakeJobClient{
		createFn: func(ctx context.Context, req JobCreateRequest) (BackendRef, error) {
			<-block
			return NewK8sJobBackendRef(req.Namespace, req.JobName, "uid"), nil
		},
	}
	rt := newTestRuntime(t, client, func(c *RuntimeConfig) {
		c.MaxConcurrency = 2
	})

	started := make(chan struct{}, 2)

	for i := range 2 {
		go func(n int) {
			started <- struct{}{}
			rt.SubmitAttempt(context.Background(), minimalReq("attempt-"+string(rune('A'+n)))) //nolint
		}(i)
	}

	// Wait for both in-flight Creates.
	<-started
	<-started
	// Give goroutines time to register in the map.
	time.Sleep(10 * time.Millisecond)

	_, err := rt.SubmitAttempt(context.Background(), minimalReq("attempt-C"))
	if !errors.Is(err, ErrCapacityExceeded) {
		t.Fatalf("expected ErrCapacityExceeded, got %v", err)
	}

	close(block) // cleanup
}

// ── Create failure ────────────────────────────────────────────────────────────

func TestSubmitAttempt_CreateFailure(t *testing.T) {
	createErr := errors.New("backend unavailable")
	client := &fakeJobClient{
		createFn: func(_ context.Context, _ JobCreateRequest) (BackendRef, error) {
			return BackendRef{}, createErr
		},
	}
	rt := newTestRuntime(t, client)

	_, err := rt.SubmitAttempt(context.Background(), minimalReq("a1"))
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, createErr) {
		t.Errorf("got %v, want wrapped %v", err, createErr)
	}

	// After failure, active count should return to 0 and a retry is possible.
	rt.mu.RLock()
	active := rt.active
	_, inMap := rt.attempts["a1"]
	rt.mu.RUnlock()

	if active != 0 {
		t.Errorf("active = %d after failure, want 0", active)
	}
	if inMap {
		t.Error("failed attempt should be removed from map")
	}
}

func TestSubmitAttempt_RetryAfterCreateFailure(t *testing.T) {
	calls := 0
	var mu sync.Mutex

	client := &fakeJobClient{
		createFn: func(ctx context.Context, req JobCreateRequest) (BackendRef, error) {
			mu.Lock()
			n := calls
			calls++
			mu.Unlock()
			if n == 0 {
				return BackendRef{}, errors.New("transient error")
			}
			return NewK8sJobBackendRef(req.Namespace, req.JobName, "uid"), nil
		},
	}
	rt := newTestRuntime(t, client)

	_, err := rt.SubmitAttempt(context.Background(), minimalReq("a1"))
	if err == nil {
		t.Fatal("first call should fail")
	}

	handle, err := rt.SubmitAttempt(context.Background(), minimalReq("a1"))
	if err != nil {
		t.Fatalf("retry after failure: %v", err)
	}
	if handle.BackendRef.ID == "" {
		t.Error("BackendRef should be populated on successful retry")
	}
}

// ── AttemptTimeout ────────────────────────────────────────────────────────────

func TestSubmitAttempt_AttemptTimeout_TriggersDelete(t *testing.T) {
	deleted := make(chan BackendRef, 1)

	client := &fakeJobClient{
		deleteFn: func(_ context.Context, ref BackendRef) error {
			deleted <- ref
			return nil
		},
	}
	rt := newTestRuntime(t, client)

	req := minimalReq("a1")
	req.AttemptTimeout = 50 * time.Millisecond

	handle, err := rt.SubmitAttempt(context.Background(), req)
	if err != nil {
		t.Fatalf("SubmitAttempt: %v", err)
	}

	select {
	case ref := <-deleted:
		if ref.ID != handle.BackendRef.ID {
			t.Errorf("deleted ref ID = %q, want %q", ref.ID, handle.BackendRef.ID)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout: Delete was not called after AttemptTimeout expired")
	}

	// active count should be decremented.
	rt.mu.RLock()
	active := rt.active
	rt.mu.RUnlock()
	if active != 0 {
		t.Errorf("active = %d after timeout, want 0", active)
	}
}

func TestSubmitAttempt_NoTimeout_WhenZero(t *testing.T) {
	deleteCalled := make(chan struct{}, 1)

	client := &fakeJobClient{
		deleteFn: func(_ context.Context, _ BackendRef) error {
			deleteCalled <- struct{}{}
			return nil
		},
	}
	rt := newTestRuntime(t, client)

	req := minimalReq("a1")
	req.AttemptTimeout = 0 // no timeout

	_, err := rt.SubmitAttempt(context.Background(), req)
	if err != nil {
		t.Fatalf("SubmitAttempt: %v", err)
	}

	select {
	case <-deleteCalled:
		t.Fatal("Delete should not be called when AttemptTimeout == 0")
	case <-time.After(100 * time.Millisecond):
		// expected: no delete
	}
}
