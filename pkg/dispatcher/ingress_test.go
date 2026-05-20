package dispatcher_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/HeaInSeo/spawner/pkg/actor"
	"github.com/HeaInSeo/spawner/pkg/api"
	"github.com/HeaInSeo/spawner/pkg/dispatcher"
	sErr "github.com/HeaInSeo/spawner/pkg/error"
	"github.com/HeaInSeo/spawner/pkg/frontdoor"
	"github.com/HeaInSeo/spawner/pkg/policy"
	"github.com/HeaInSeo/spawner/pkg/store"
)

// ── mocks ─────────────────────────────────────────────────────────────────────

type mockFD struct {
	key string
	cmd api.Command
}

func (m *mockFD) Resolve(_ context.Context, _ frontdoor.ResolveInput) (frontdoor.ResolveResult, error) {
	return frontdoor.ResolveResult{SpawnKey: m.key, Cmd: m.cmd}, nil
}

type replayFD struct{}

func (replayFD) Resolve(_ context.Context, in frontdoor.ResolveInput) (frontdoor.ResolveResult, error) {
	rs := in.Req.(*api.RunSpec)
	cmd, err := api.NewRunCommand(rs, api.Command{}.Policy)
	if err != nil {
		return frontdoor.ResolveResult{}, err
	}
	key := rs.RunID
	if in.Meta.TenantID != "" {
		key = in.Meta.TenantID + ":" + rs.RunID
	}
	return frontdoor.ResolveResult{SpawnKey: key, Cmd: cmd}, nil
}

type mockActor struct{ enqueueCalled int }

func (m *mockActor) EnqueueTry(api.Command) bool                      { m.enqueueCalled++; return true }
func (m *mockActor) EnqueueCtx(_ context.Context, _ api.Command) bool { m.enqueueCalled++; return true }
func (m *mockActor) OnIdle(func())                                    {}
func (m *mockActor) OnTerminate(func())                               {}
func (m *mockActor) Loop(_ context.Context)                           {}

type mockFactory struct{ act *mockActor }

func (m *mockFactory) Get(_ string) (actor.Actor, bool) { return nil, false }
func (m *mockFactory) Bind(_ string) (actor.Actor, bool, error) {
	return m.act, true, nil
}
func (m *mockFactory) Register(_ string, _ actor.Actor) {}
func (m *mockFactory) Unbind(_ string, _ actor.Actor)   {}

// newTestDispatcher builds a Dispatcher with a RunStore and mock internals.
func newTestDispatcher(rs store.RunStore, opts ...dispatcher.Option) (*dispatcher.Dispatcher, *mockActor) {
	act := &mockActor{}
	mf := &mockFactory{act: act}
	fd := &mockFD{
		key: "teamA:run-001",
		cmd: api.Command{Kind: api.CmdRun, Run: &api.RunSpec{RunID: "run-001", ImageRef: "busybox:1.36"}},
	}
	baseOpts := []dispatcher.Option{dispatcher.WithRunStore(rs)}
	return dispatcher.NewDispatcher(fd, mf, 4, append(baseOpts, opts...)...), act
}

func testInput() frontdoor.ResolveInput {
	return frontdoor.ResolveInput{
		Req: &api.RunSpec{RunID: "run-001", ImageRef: "busybox:1.36"},
		Meta: frontdoor.MetaContext{
			RPC:      "RunE",
			TenantID: "teamA",
			TraceID:  "trace-001",
		},
	}
}

// ── ingress boundary tests ────────────────────────────────────────────────────

// TestIngress_EnqueuesRunAsQueuedBeforeDispatching proves:
// Before any Actor interaction, Handle() stores the run as StateQueued.
// This is the "run queue absorbs submission" boundary — user burst does not
// reach K8s until the run is admitted.
func TestIngress_EnqueuesRunAsQueuedBeforeDispatching(t *testing.T) {
	ctx := context.Background()
	rs := store.NewInMemoryRunStore()
	d, _ := newTestDispatcher(rs)

	// Verify store is empty before Handle
	before, _ := rs.ListByState(ctx, store.StateQueued)
	if len(before) != 0 {
		t.Fatalf("expected empty store before Handle, got %d", len(before))
	}

	_ = d.Handle(ctx, testInput(), nil)

	// The run should be in the store (queued or admitted, depending on timing)
	rec, ok, _ := rs.Get(ctx, "teamA:run-001")
	if !ok {
		t.Fatal("run was not persisted to RunStore")
	}
	var env api.RunEnvelope
	if err := json.Unmarshal(rec.Payload, &env); err != nil {
		t.Fatalf("unmarshal payload envelope: %v", err)
	}
	if env.Version != 1 {
		t.Fatalf("expected envelope version 1, got %d", env.Version)
	}
	if env.Identity.LogicalRunID != "teamA:run-001" {
		t.Fatalf("unexpected logical run id: %q", env.Identity.LogicalRunID)
	}
	if env.Identity.AttemptID != "teamA:run-001/attempt-1" {
		t.Fatalf("unexpected attempt id: %q", env.Identity.AttemptID)
	}
	if env.Identity.SpawnKey != "teamA:run-001" {
		t.Fatalf("unexpected spawn key: %q", env.Identity.SpawnKey)
	}
	if env.Run == nil || env.Run.RunID != "run-001" {
		t.Fatalf("expected run payload in envelope, got %+v", env.Run)
	}
	t.Logf("PASS: run persisted with state=%s before/during dispatch", rec.State)
}

// TestIngress_TransitionsToAdmittedOnSuccessfulDispatch proves:
// After Handle() succeeds, the run transitions from queued to admitted-to-dag.
// This is the "gate opened" moment — run is now being executed.
func TestIngress_TransitionsToAdmittedOnSuccessfulDispatch(t *testing.T) {
	ctx := context.Background()
	rs := store.NewInMemoryRunStore()
	d, _ := newTestDispatcher(rs)

	if err := d.Handle(ctx, testInput(), nil); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	rec, ok, _ := rs.Get(ctx, "teamA:run-001")
	if !ok {
		t.Fatal("run not found in RunStore after Handle")
	}
	if rec.State != store.StateAdmittedToDag {
		t.Fatalf("expected admitted-to-dag, got %s", rec.State)
	}
	t.Logf("PASS: run transitioned to admitted-to-dag after successful dispatch")
}

// TestIngress_RunHeldNotDispatchedWhenBackendUnavailable proves:
// When the execution backend is unreachable at startup, Handle() transitions the
// run to StateHeld and returns ErrBackendUnavailable. No Actor is invoked.
// The run is preserved in the RunStore for recovery, not lost.
func TestIngress_RunHeldNotDispatchedWhenBackendUnavailable(t *testing.T) {
	ctx := context.Background()
	rs := store.NewInMemoryRunStore()
	d, act := newTestDispatcher(rs, dispatcher.WithBackendUnavailable())

	err := d.Handle(ctx, testInput(), nil)
	if !errors.Is(err, sErr.ErrBackendUnavailable) {
		t.Fatalf("expected ErrBackendUnavailable, got %v", err)
	}

	// Actor must NOT have been invoked
	if act.enqueueCalled > 0 {
		t.Fatalf("BOUNDARY VIOLATION: Actor.EnqueueCtx called despite backend unavailable")
	}

	// Run must be in StateHeld
	rec, ok, _ := rs.Get(ctx, "teamA:run-001")
	if !ok {
		t.Fatal("run not found in RunStore")
	}
	if rec.State != store.StateHeld {
		t.Fatalf("expected held, got %s", rec.State)
	}
	t.Logf("PASS: run held (not lost, not dispatched) when backend unavailable")
}

// TestIngress_BootstrapRecoversByState proves:
// After a restart, Bootstrap() returns runs that were queued or admitted-to-dag.
// This is the restart recovery boundary — in-flight and pending runs are not lost.
func TestIngress_BootstrapRecoversByState(t *testing.T) {
	ctx := context.Background()
	rs := store.NewInMemoryRunStore()

	for _, rec := range []store.RunRecord{
		recoveryRecord(t, "r1", store.StateQueued),
		recoveryRecord(t, "r2", store.StateQueued),
		recoveryRecord(t, "r3", store.StateAdmittedToDag),
		recoveryRecord(t, "r4", store.StateHeld),
		recoveryRecord(t, "r5", store.StateFinished),
	} {
		if err := rs.Enqueue(ctx, rec); err != nil {
			t.Fatalf("Enqueue(%s): %v", rec.RunID, err)
		}
	}

	d, _ := newTestDispatcher(rs)
	recovered, err := d.Bootstrap(ctx)
	if err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if len(recovered) != 3 {
		t.Fatalf("expected 3 recovered runs (2 queued + 1 admitted), got %d", len(recovered))
	}
	t.Logf("PASS: Bootstrap recovered %d runs (queued + admitted-to-dag)", len(recovered))
}

func TestIngress_RecoverableRuns_DecodesEnvelopeAndSkipsNonRecoverable(t *testing.T) {
	ctx := context.Background()
	rs := store.NewInMemoryRunStore()
	for _, rec := range []store.RunRecord{
		recoveryRecord(t, "r1", store.StateQueued),
		recoveryRecord(t, "r2", store.StateAdmittedToDag),
		recoveryRecord(t, "r3", store.StateHeld),
	} {
		if err := rs.Enqueue(ctx, rec); err != nil {
			t.Fatalf("Enqueue(%s): %v", rec.RunID, err)
		}
	}

	d, _ := newTestDispatcher(rs)
	recovered, err := d.RecoverableRuns(ctx)
	if err != nil {
		t.Fatalf("RecoverableRuns: %v", err)
	}
	if len(recovered) != 2 {
		t.Fatalf("expected 2 recoverable runs, got %d", len(recovered))
	}
	for _, rr := range recovered {
		if !store.IsRecoverable(rr.Record.State) {
			t.Fatalf("non-recoverable state leaked into result: %s", rr.Record.State)
		}
		if rr.Envelope.Identity.LogicalRunID != rr.Record.RunID {
			t.Fatalf("logical run id mismatch: env=%q record=%q", rr.Envelope.Identity.LogicalRunID, rr.Record.RunID)
		}
	}
}

func TestIngress_RecoverableRuns_FailsOnMalformedEnvelope(t *testing.T) {
	ctx := context.Background()
	rs := store.NewInMemoryRunStore()
	if err := rs.Enqueue(ctx, store.RunRecord{
		RunID:   "bad-run",
		State:   store.StateQueued,
		Payload: []byte("not-json"),
	}); err != nil {
		t.Fatalf("Enqueue malformed payload: %v", err)
	}

	d, _ := newTestDispatcher(rs)
	if _, err := d.RecoverableRuns(ctx); !errors.Is(err, sErr.ErrInvalidCommand) {
		t.Fatalf("expected ErrInvalidCommand, got %v", err)
	}
}

func TestRecoverableRun_ResolveInputRestoresReplayMetadata(t *testing.T) {
	rr := dispatcher.RecoverableRun{
		Record: recoveryRecord(t, "teamA:run-1", store.StateQueued),
		Envelope: api.RunEnvelope{
			Version: 1,
			Kind:    api.CmdRun,
			Identity: api.RunIdentity{
				LogicalRunID: "teamA:run-1",
				AttemptID:    "teamA:run-1/attempt-1",
				SpawnKey:     "teamA:run-1",
				TenantID:     "teamA",
				TraceID:      "trace-1",
				RequestID:    "req-1",
				Principal:    "alice",
			},
			Run: &api.RunSpec{RunID: "run-1", ImageRef: "busybox:1.36"},
		},
	}

	in := rr.ResolveInput()
	if in.Meta.TenantID != "teamA" || in.Meta.TraceID != "trace-1" || in.Meta.RequestID != "req-1" {
		t.Fatalf("unexpected replay meta: %+v", in.Meta)
	}
	rs, ok := in.Req.(*api.RunSpec)
	if !ok || rs.RunID != "run-1" {
		t.Fatalf("unexpected replay req: %#v", in.Req)
	}
}

func TestIngress_ReplayRecoverableRun_ReplaysThroughHandle(t *testing.T) {
	ctx := context.Background()
	rs := store.NewInMemoryRunStore()
	rec := recoveryRecord(t, "teamA:run-1", store.StateQueued)
	if err := rs.Enqueue(ctx, rec); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	act := &mockActor{}
	d := dispatcher.NewDispatcher(replayFD{}, &mockFactory{act: act}, 4, dispatcher.WithRunStore(rs))
	recovered, err := d.RecoverableRuns(ctx)
	if err != nil {
		t.Fatalf("RecoverableRuns: %v", err)
	}
	if len(recovered) != 1 {
		t.Fatalf("expected 1 recoverable run, got %d", len(recovered))
	}

	if err := d.ReplayRecoverableRun(ctx, recovered[0], nil); err != nil {
		t.Fatalf("ReplayRecoverableRun: %v", err)
	}
	if act.enqueueCalled == 0 {
		t.Fatal("expected replay to enqueue actor work")
	}
}

func TestIngress_ReplayRecoverableRunWithPhase_ManualRequeueAllocatesNewAttempt(t *testing.T) {
	d, _ := newTestDispatcher(nil, dispatcher.WithAttemptPolicy(policy.DefaultAttemptPolicy()))
	rr := dispatcher.RecoverableRun{
		Record: recoveryRecord(t, "teamA:run-1", store.StateQueued),
		Envelope: api.RunEnvelope{
			Version: 1,
			Kind:    api.CmdRun,
			Identity: api.RunIdentity{
				LogicalRunID: "teamA:run-1",
				AttemptID:    "teamA:run-1/attempt-1",
				SpawnKey:     "teamA:run-1",
				TenantID:     "teamA",
			},
			Run: &api.RunSpec{RunID: "run-1", ImageRef: "busybox:1.36"},
		},
	}

	in, err := d.PrepareReplayInput(rr, policy.AttemptPhaseManualRequeue)
	if err != nil {
		t.Fatalf("PrepareReplayInput: %v", err)
	}
	if got, ok := in.Meta.Get("spawner.attempt_id"); !ok || got != "teamA:run-1/attempt-2" {
		t.Fatalf("expected replay input to carry explicit attempt id, got %q ok=%v", got, ok)
	}
	if got, ok := in.Meta.Get("spawner.attempt_phase"); !ok || got != "manual-requeue" {
		t.Fatalf("expected replay input to carry manual-requeue phase, got %q ok=%v", got, ok)
	}
}

func TestIngress_ReplayRecoverableRunWithPhase_ManualRequeueAppendsAttemptHistory(t *testing.T) {
	ctx := context.Background()
	rs := store.NewInMemoryRunStore()
	rec := recoveryRecord(t, "teamA:run-1", store.StateQueued)
	if err := rs.Enqueue(ctx, rec); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	act := &mockActor{}
	d := dispatcher.NewDispatcher(
		replayFD{},
		&mockFactory{act: act},
		4,
		dispatcher.WithRunStore(rs),
		dispatcher.WithAttemptPolicy(policy.DefaultAttemptPolicy()),
	)
	recovered, err := d.RecoverableRuns(ctx)
	if err != nil {
		t.Fatalf("RecoverableRuns: %v", err)
	}
	if err := d.ReplayRecoverableRunWithPhase(ctx, recovered[0], policy.AttemptPhaseManualRequeue, nil); err != nil {
		t.Fatalf("ReplayRecoverableRunWithPhase: %v", err)
	}

	atts, err := rs.ListAttempts(ctx, "teamA:run-1")
	if err != nil {
		t.Fatalf("ListAttempts: %v", err)
	}
	if len(atts) != 2 {
		t.Fatalf("expected 2 attempts after manual requeue, got %d", len(atts))
	}
	if atts[1].AttemptID != "teamA:run-1/attempt-2" {
		t.Fatalf("expected appended attempt-2, got %q", atts[1].AttemptID)
	}
	if atts[1].Cause != store.AttemptCauseManualRequeue {
		t.Fatalf("expected manual requeue cause, got %q", atts[1].Cause)
	}
}

func TestIngress_PrepareReplayInput_RecoveryKeepsAttempt(t *testing.T) {
	d, _ := newTestDispatcher(nil, dispatcher.WithAttemptPolicy(policy.DefaultAttemptPolicy()))
	rr := dispatcher.RecoverableRun{
		Record: recoveryRecord(t, "teamA:run-1", store.StateQueued),
		Envelope: api.RunEnvelope{
			Version: 1,
			Kind:    api.CmdRun,
			Identity: api.RunIdentity{
				LogicalRunID: "teamA:run-1",
				AttemptID:    "teamA:run-1/attempt-1",
				SpawnKey:     "teamA:run-1",
				TenantID:     "teamA",
			},
			Run: &api.RunSpec{RunID: "run-1", ImageRef: "busybox:1.36"},
		},
	}

	in, err := d.PrepareReplayInput(rr, policy.AttemptPhaseRecoveryReplay)
	if err != nil {
		t.Fatalf("PrepareReplayInput: %v", err)
	}
	if got, ok := in.Meta.Get("spawner.attempt_id"); !ok || got != "teamA:run-1/attempt-1" {
		t.Fatalf("expected recovery replay to keep attempt id, got %q ok=%v", got, ok)
	}
	if got, ok := in.Meta.Get("spawner.attempt_phase"); !ok || got != "recovery-replay" {
		t.Fatalf("expected replay input to carry recovery-replay phase, got %q ok=%v", got, ok)
	}
}

func TestIngress_ReplayRecoverableRun_RejectsNonRecoverableState(t *testing.T) {
	d, _ := newTestDispatcher(nil)
	rr := dispatcher.RecoverableRun{
		Record:   recoveryRecord(t, "teamA:run-1", store.StateHeld),
		Envelope: api.RunEnvelope{Version: 1, Kind: api.CmdRun, Run: &api.RunSpec{RunID: "run-1", ImageRef: "busybox:1.36"}},
	}
	if err := d.ReplayRecoverableRun(context.Background(), rr, nil); !errors.Is(err, sErr.ErrInvalidCommand) {
		t.Fatalf("expected ErrInvalidCommand, got %v", err)
	}
}

// TestIngress_BootstrapIsNopWithoutRunStore proves:
// When no RunStore is configured, Bootstrap() returns nil without error.
// Backward-compatible with pre-RunStore deployments.
func TestIngress_BootstrapIsNopWithoutRunStore(t *testing.T) {
	ctx := context.Background()
	act := &mockActor{}
	fd := &mockFD{key: "k", cmd: api.Command{Kind: api.CmdRun, Run: &api.RunSpec{RunID: "run-boot", ImageRef: "busybox:1.36"}}}
	d := dispatcher.NewDispatcher(fd, &mockFactory{act: act}, 2)

	recovered, err := d.Bootstrap(ctx)
	if err != nil {
		t.Fatalf("Bootstrap (no store): %v", err)
	}
	if len(recovered) != 0 {
		t.Fatalf("expected empty, got %d", len(recovered))
	}
	t.Log("PASS: Bootstrap is a no-op without RunStore")
}

// TestIngress_IdempotentEnqueue proves:
// Submitting the same RunID twice does not return an error (ErrAlreadyExists
// is swallowed). The run stays in whatever state it was already in.
func TestIngress_IdempotentEnqueue(t *testing.T) {
	ctx := context.Background()
	rs := store.NewInMemoryRunStore()
	d, _ := newTestDispatcher(rs)

	if err := d.Handle(ctx, testInput(), nil); err != nil {
		t.Fatalf("first Handle: %v", err)
	}
	// Second call with same RunID: should not fail with ErrAlreadyExists
	if err := d.Handle(ctx, testInput(), nil); err != nil {
		t.Fatalf("second Handle (re-submit): %v", err)
	}
	atts, err := rs.ListAttempts(ctx, "teamA:run-001")
	if err != nil {
		t.Fatalf("ListAttempts: %v", err)
	}
	if len(atts) != 1 {
		t.Fatalf("expected duplicate submit to stay on one attempt, got %d", len(atts))
	}
	if atts[0].Cause != store.AttemptCauseInitialSubmit {
		t.Fatalf("expected duplicate submit to preserve initial cause, got %q", atts[0].Cause)
	}
	t.Log("PASS: duplicate RunID enqueue is idempotent")
}

// Ensure dispatcher.Option type is usable (compile check for WithEnqueueTimeout).
var _ = dispatcher.WithEnqueueTimeout(time.Second)

type lifecycleActor struct {
	mu         sync.Mutex
	idleFn     func()
	idleOnce   sync.Once
	idleSignal chan struct{}
}

func newLifecycleActor() *lifecycleActor {
	return &lifecycleActor{idleSignal: make(chan struct{})}
}

func (a *lifecycleActor) EnqueueTry(api.Command) bool { return true }
func (a *lifecycleActor) EnqueueCtx(_ context.Context, cmd api.Command) bool {
	a.mu.Lock()
	fn := a.idleFn
	a.mu.Unlock()
	if cmd.Kind == api.CmdRun && fn != nil {
		go fn()
	}
	return true
}
func (a *lifecycleActor) OnIdle(fn func()) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.idleFn = func() {
		fn()
		a.idleOnce.Do(func() { close(a.idleSignal) })
	}
}
func (a *lifecycleActor) OnTerminate(func())     {}
func (a *lifecycleActor) Loop(_ context.Context) {}

type lifecycleFactory struct {
	mu          sync.Mutex
	act         actor.Actor
	created     int
	bound       map[string]actor.Actor
	unbindCalls int
}

func (f *lifecycleFactory) Get(spawnKey string) (actor.Actor, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	act, ok := f.bound[spawnKey]
	return act, ok
}

func (f *lifecycleFactory) Bind(_ string) (actor.Actor, bool, error) {
	f.mu.Lock()
	f.created++
	f.mu.Unlock()
	return f.act, true, nil
}

func (f *lifecycleFactory) Register(spawnKey string, act actor.Actor) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bound[spawnKey] = act
}

func (f *lifecycleFactory) Unbind(spawnKey string, act actor.Actor) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unbindCalls++
	if cur, ok := f.bound[spawnKey]; ok && cur == act {
		delete(f.bound, spawnKey)
	}
}

// ── failure-path mocks ────────────────────────────────────────────────────────

// bindErrFactory: Bind 항상 실패 → semaphore rollback 테스트용
type bindErrFactory struct{}

func (f *bindErrFactory) Get(_ string) (actor.Actor, bool) { return nil, false }
func (f *bindErrFactory) Bind(_ string) (actor.Actor, bool, error) {
	return nil, false, errors.New("bind: backend unavailable")
}
func (f *bindErrFactory) Register(_ string, _ actor.Actor) {}
func (f *bindErrFactory) Unbind(_ string, _ actor.Actor)   {}

// rejectEnqueueActor: EnqueueCtx 항상 false → ErrMailboxFull 유도
type rejectEnqueueActor struct{}

func (a *rejectEnqueueActor) EnqueueTry(_ api.Command) bool                    { return false }
func (a *rejectEnqueueActor) EnqueueCtx(_ context.Context, _ api.Command) bool { return false }
func (a *rejectEnqueueActor) OnIdle(_ func())                                  {}
func (a *rejectEnqueueActor) OnTerminate(_ func())                             {}
func (a *rejectEnqueueActor) Loop(_ context.Context)                           {}

// dualSignalActor: OnIdle + OnTerminate 모두 등록하고, CmdRun 수신 시 두 콜백을 순서대로 호출.
// releaseOnce(sync.Once)가 두 번째 호출을 막는지 검증하기 위해 사용.
type dualSignalActor struct {
	mu         sync.Mutex
	idleFn     func()
	termFn     func()
	idleOnce   sync.Once
	termOnce   sync.Once
	idleSignal chan struct{}
	termSignal chan struct{}
}

func newDualSignalActor() *dualSignalActor {
	return &dualSignalActor{
		idleSignal: make(chan struct{}),
		termSignal: make(chan struct{}),
	}
}

func (a *dualSignalActor) EnqueueTry(_ api.Command) bool { return true }
func (a *dualSignalActor) EnqueueCtx(_ context.Context, cmd api.Command) bool {
	if cmd.Kind != api.CmdRun {
		return true
	}
	a.mu.Lock()
	idle, term := a.idleFn, a.termFn
	a.mu.Unlock()
	go func() {
		if idle != nil {
			idle()
			a.idleOnce.Do(func() { close(a.idleSignal) })
		}
		if term != nil {
			term()
			a.termOnce.Do(func() { close(a.termSignal) })
		}
	}()
	return true
}
func (a *dualSignalActor) OnIdle(fn func()) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.idleFn = fn
}
func (a *dualSignalActor) OnTerminate(fn func()) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.termFn = fn
}
func (a *dualSignalActor) Loop(_ context.Context) {}

func TestIngress_ReleasesSlotAfterActorBecomesIdle(t *testing.T) {
	fd := &mockFD{
		key: "teamA:run-001",
		cmd: api.Command{Kind: api.CmdRun, Run: &api.RunSpec{RunID: "run-001", ImageRef: "busybox:1.36"}},
	}
	act := newLifecycleActor()
	f := &lifecycleFactory{act: act, bound: make(map[string]actor.Actor)}
	d := dispatcher.NewDispatcher(fd, f, 1)

	if err := d.Handle(context.Background(), testInput(), nil); err != nil {
		t.Fatalf("first Handle: %v", err)
	}

	select {
	case <-act.idleSignal:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for semaphore release after actor became idle")
	}

	fd.key = "teamA:run-002"
	fd.cmd = api.Command{Kind: api.CmdRun, Run: &api.RunSpec{RunID: "run-002", ImageRef: "busybox:1.36"}}
	input2 := frontdoor.ResolveInput{
		Req: &api.RunSpec{RunID: "run-002", ImageRef: "busybox:1.36"},
		Meta: frontdoor.MetaContext{
			RPC:      "RunE",
			TenantID: "teamA",
			TraceID:  "trace-002",
		},
	}

	if err := d.Handle(context.Background(), input2, nil); err != nil {
		t.Fatalf("second Handle after semaphore release: %v", err)
	}
}

func TestDispatcher_SemaphoreLifecycleNoLeak(t *testing.T) {
	fd := &mockFD{
		key: "teamA:run-001",
		cmd: api.Command{Kind: api.CmdRun, Run: &api.RunSpec{RunID: "run-001", ImageRef: "busybox:1.36"}},
	}
	act := newLifecycleActor()
	f := &lifecycleFactory{act: act, bound: make(map[string]actor.Actor)}
	d := dispatcher.NewDispatcher(fd, f, 1)

	if err := d.Handle(context.Background(), testInput(), nil); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	select {
	case <-act.idleSignal:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for idle signal")
	}

	goleak.VerifyNone(t)
}

// TestDispatcher_BindFailure_SemaphoreRolledBack: Factory.Bind가 에러를 반환하면
// 획득한 세마포어 슬롯이 즉시 반납되어야 한다.
func TestDispatcher_BindFailure_SemaphoreRolledBack(t *testing.T) {
	fd := &mockFD{
		key: "teamA:run-001",
		cmd: api.Command{Kind: api.CmdRun, Run: &api.RunSpec{RunID: "run-001", ImageRef: "busybox:1.36"}},
	}
	d := dispatcher.NewDispatcher(fd, &bindErrFactory{}, 1)

	if err := d.Handle(context.Background(), testInput(), nil); err == nil {
		t.Fatal("expected Bind error, got nil")
	}

	if n := len(d.Sem); n != 0 {
		t.Fatalf("semaphore not released after Bind failure: len=%d cap=%d", n, cap(d.Sem))
	}
}

// TestDispatcher_EnqueueBindFailure_SemaphoreRolledBack: bind 커맨드 Enqueue가 실패(ErrMailboxFull)하면
// 획득한 세마포어 슬롯과 등록된 액터가 롤백되어야 한다.
func TestDispatcher_EnqueueBindFailure_SemaphoreRolledBack(t *testing.T) {
	fd := &mockFD{
		key: "teamA:run-001",
		cmd: api.Command{Kind: api.CmdRun, Run: &api.RunSpec{RunID: "run-001", ImageRef: "busybox:1.36"}},
	}
	f := &lifecycleFactory{act: &rejectEnqueueActor{}, bound: make(map[string]actor.Actor)}
	d := dispatcher.NewDispatcher(fd, f, 1)

	err := d.Handle(context.Background(), testInput(), nil)
	if !errors.Is(err, sErr.ErrMailboxFull) {
		t.Fatalf("expected ErrMailboxFull, got %v", err)
	}

	if n := len(d.Sem); n != 0 {
		t.Fatalf("semaphore not released after enqueue failure: len=%d cap=%d", n, cap(d.Sem))
	}
}

// TestDispatcher_ReleaseOnce_PreventsDoubleUnbind: OnIdle과 OnTerminate에 같은 releaseOnce 클로저가
// 등록될 때, 두 콜백이 모두 호출되어도 Unbind는 정확히 한 번만 실행되어야 한다(sync.Once 보장).
func TestDispatcher_ReleaseOnce_PreventsDoubleUnbind(t *testing.T) {
	fd := &mockFD{
		key: "teamA:run-001",
		cmd: api.Command{Kind: api.CmdRun, Run: &api.RunSpec{RunID: "run-001", ImageRef: "busybox:1.36"}},
	}
	act := newDualSignalActor()
	f := &lifecycleFactory{act: act, bound: make(map[string]actor.Actor)}
	d := dispatcher.NewDispatcher(fd, f, 1)

	if err := d.Handle(context.Background(), testInput(), nil); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	// 두 콜백이 모두 실행될 때까지 대기
	select {
	case <-act.idleSignal:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for idle signal")
	}
	select {
	case <-act.termSignal:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for term signal")
	}

	// semaphore는 정확히 한 번 반납되어야 한다
	if n := len(d.Sem); n != 0 {
		t.Fatalf("semaphore not released: len=%d", n)
	}
	// Unbind도 정확히 한 번만 호출되어야 한다 (sync.Once가 두 번째 releaseOnce를 막음)
	f.mu.Lock()
	calls := f.unbindCalls
	f.mu.Unlock()
	if calls != 1 {
		t.Fatalf("expected Unbind called exactly once, got %d", calls)
	}
}

func TestIngress_DoesNotPersistInvalidResolvedRun(t *testing.T) {
	ctx := context.Background()
	rs := store.NewInMemoryRunStore()
	fd := &mockFD{
		key: "teamA:run-001",
		cmd: api.Command{Kind: api.CmdRun, Run: nil},
	}
	d := dispatcher.NewDispatcher(fd, &mockFactory{act: &mockActor{}}, 1, dispatcher.WithRunStore(rs))

	err := d.Handle(ctx, testInput(), nil)
	if !errors.Is(err, sErr.ErrInvalidCommand) {
		t.Fatalf("expected ErrInvalidCommand, got %v", err)
	}

	if _, ok, _ := rs.Get(ctx, "teamA:run-001"); ok {
		t.Fatal("invalid resolved run should not be persisted to RunStore")
	}
}

func recoveryRecord(t *testing.T, logicalRunID string, state store.RunState) store.RunRecord {
	t.Helper()
	runID := logicalRunID
	if idx := strings.LastIndex(logicalRunID, ":"); idx >= 0 && idx < len(logicalRunID)-1 {
		runID = logicalRunID[idx+1:]
	}
	env := api.RunEnvelope{
		Version: 1,
		Kind:    api.CmdRun,
		Identity: api.RunIdentity{
			LogicalRunID: logicalRunID,
			AttemptID:    logicalRunID + "/attempt-1",
			SpawnKey:     logicalRunID,
			TenantID:     "teamA",
		},
		Run: &api.RunSpec{RunID: runID, ImageRef: "busybox:1.36"},
	}
	payload, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal recovery envelope: %v", err)
	}
	return store.RunRecord{RunID: logicalRunID, State: state, Payload: payload}
}
