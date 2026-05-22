package imp

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/HeaInSeo/spawner/pkg/actor"
	"github.com/HeaInSeo/spawner/pkg/api"
	"github.com/HeaInSeo/spawner/pkg/driver"
	"github.com/HeaInSeo/spawner/pkg/policy"
)

type actorTestSink struct {
	mu     sync.Mutex
	events []api.Event
}

func (s *actorTestSink) Send(ev api.Event) {
	s.mu.Lock()
	s.events = append(s.events, ev)
	s.mu.Unlock()
}

func (s *actorTestSink) snapshot() []api.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]api.Event, len(s.events))
	copy(out, s.events)
	return out
}

// chanSink: 이벤트를 슬라이스에 누적하고 notify 채널로 수신자에게 알린다.
// time.Sleep 없이 특정 State가 도착할 때까지 결정론적으로 대기할 수 있다.
type chanSink struct {
	mu     sync.Mutex
	events []api.Event
	notify chan struct{}
}

func newChanSink() *chanSink { return &chanSink{notify: make(chan struct{}, 64)} }

func (s *chanSink) Send(ev api.Event) {
	s.mu.Lock()
	s.events = append(s.events, ev)
	s.mu.Unlock()
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func (s *chanSink) waitFor(t *testing.T, pred func(api.Event) bool, timeout time.Duration) api.Event {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		s.mu.Lock()
		for _, ev := range s.events {
			if pred(ev) {
				s.mu.Unlock()
				return ev
			}
		}
		s.mu.Unlock()
		select {
		case <-s.notify:
		case <-timer.C:
			t.Fatal("timed out waiting for expected event")
			return api.Event{}
		}
	}
}

func (s *chanSink) waitState(t *testing.T, state api.State, timeout time.Duration) api.Event {
	t.Helper()
	return s.waitFor(t, func(ev api.Event) bool { return ev.State == state }, timeout)
}

type actorTestDriver struct {
	driver.UnimplementedDriver
	mu          sync.Mutex
	prepareErr  error
	startErr    error
	waitErr     error
	cancelErr   error
	handle      driver.Handle
	waitStarted chan struct{}
	waitBlock   chan struct{}
	cancelCalls int
	signalCalls int
}

func (d *actorTestDriver) Prepare(_ context.Context, _ api.RunSpec) (driver.Prepared, error) {
	if d.prepareErr != nil {
		return nil, d.prepareErr
	}
	return testPrepared{}, nil
}

func (d *actorTestDriver) Start(_ context.Context, _ driver.Prepared) (driver.Handle, error) {
	if d.startErr != nil {
		return nil, d.startErr
	}
	if d.handle != nil {
		return d.handle, nil
	}
	return testHandle{}, nil
}

func (d *actorTestDriver) Wait(ctx context.Context, _ driver.Handle) (api.Event, error) {
	if d.waitStarted != nil {
		select {
		case <-d.waitStarted:
		default:
			close(d.waitStarted)
		}
	}
	if d.waitBlock != nil {
		select {
		case <-ctx.Done():
			return api.Event{}, ctx.Err()
		case <-d.waitBlock:
		}
	}
	if d.waitErr != nil {
		return api.Event{}, d.waitErr
	}
	return api.Event{State: api.StateSucceeded}, nil
}

func (d *actorTestDriver) Cancel(_ context.Context, _ driver.Handle) error {
	d.mu.Lock()
	d.cancelCalls++
	err := d.cancelErr
	d.mu.Unlock()
	return err
}

func (d *actorTestDriver) Signal(_ context.Context, _ driver.Handle, _ api.Signal) error {
	d.mu.Lock()
	d.signalCalls++
	d.mu.Unlock()
	return nil
}

func TestK8sActor_RunWithoutBindFails(t *testing.T) {
	sink := &actorTestSink{}
	a := NewK8sActor("spawn-1", &actorTestDriver{}, 8)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.Loop(ctx)
	}()

	ok := a.EnqueueCtx(context.Background(), api.Command{
		Kind:   api.CmdRun,
		Run:    &api.RunSpec{RunID: "run-1", ImageRef: "busybox:1.36"},
		Policy: policy.DefaultPolicyB(time.Second),
		Sink:   sink,
	})
	if !ok {
		t.Fatal("expected enqueue to succeed")
	}

	waitForEventState(t, sink, api.StateFailed, 2*time.Second)
	assertEventMessageContains(t, sink.snapshot(), api.StateFailed, "not bound")

	cancel()
	<-done
}

func TestK8sActor_BindRunSuccessLifecycle(t *testing.T) {
	sink := &actorTestSink{}
	a := NewK8sActor("spawn-1", &actorTestDriver{}, 8)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.Loop(ctx)
	}()

	mustEnqueue(t, a, api.Command{
		Kind: api.CmdBind,
		Bind: &api.Bind{SpawnKey: "spawn-1"},
		Sink: sink,
	})
	waitForEventState(t, sink, api.StateStarting, 2*time.Second)

	mustEnqueue(t, a, api.Command{
		Kind:   api.CmdRun,
		Run:    &api.RunSpec{RunID: "run-1", ImageRef: "busybox:1.36"},
		Policy: policy.DefaultPolicyB(time.Second),
		Sink:   sink,
	})

	waitForEventWithState(t, sink, api.StateRunning, 2*time.Second)
	waitForEventWithState(t, sink, api.StateSucceeded, 2*time.Second)

	cancel()
	<-done
}

func TestK8sActor_CancelAllActiveRuns(t *testing.T) {
	sink := &actorTestSink{}
	drv := &actorTestDriver{
		waitStarted: make(chan struct{}),
		waitBlock:   make(chan struct{}),
	}
	a := NewK8sActor("spawn-1", drv, 8)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.Loop(ctx)
	}()

	mustEnqueue(t, a, api.Command{
		Kind: api.CmdBind,
		Bind: &api.Bind{SpawnKey: "spawn-1"},
		Sink: sink,
	})
	waitForEventState(t, sink, api.StateStarting, 2*time.Second)

	mustEnqueue(t, a, api.Command{
		Kind:   api.CmdRun,
		Run:    &api.RunSpec{RunID: "run-1", ImageRef: "busybox:1.36"},
		Policy: policy.DefaultPolicyB(5 * time.Second),
		Sink:   sink,
	})

	select {
	case <-drv.waitStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not start")
	}

	waitForEventWithState(t, sink, api.StateRunning, 2*time.Second)

	mustEnqueue(t, a, api.Command{
		Kind:   api.CmdCancel,
		Cancel: &api.CancelReq{},
		Sink:   sink,
	})

	waitForEventWithState(t, sink, api.StateCancelling, 2*time.Second)
	close(drv.waitBlock)

	cancel()
	<-done

	drv.mu.Lock()
	cancelCalls := drv.cancelCalls
	drv.mu.Unlock()
	if cancelCalls == 0 {
		t.Fatal("expected driver Cancel to be called")
	}
}

func TestK8sActor_LoopExitsCleanly_NoLeak(t *testing.T) {
	sink := &actorTestSink{}
	drv := &actorTestDriver{
		waitStarted: make(chan struct{}),
		waitBlock:   make(chan struct{}),
	}
	a := NewK8sActor("spawn-1", drv, 8)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.Loop(ctx)
	}()

	mustEnqueue(t, a, api.Command{
		Kind: api.CmdBind,
		Bind: &api.Bind{SpawnKey: "spawn-1"},
		Sink: sink,
	})
	waitForEventState(t, sink, api.StateStarting, 2*time.Second)

	mustEnqueue(t, a, api.Command{
		Kind:   api.CmdRun,
		Run:    &api.RunSpec{RunID: "run-1", ImageRef: "busybox:1.36"},
		Policy: policy.DefaultPolicyB(5 * time.Second),
		Sink:   sink,
	})

	select {
	case <-drv.waitStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not start within timeout")
	}
	waitForEventWithState(t, sink, api.StateRunning, 2*time.Second)

	cancel()
	<-done

	goleak.VerifyNone(t)
}

// ── failure-path / lifecycle invariant tests (chanSink 기반, sleep 없음) ──────

// TestK8sActor_PrepareFailure_EmitsFailedEvent: driver.Prepare가 에러를 반환하면
// StateFailed 이벤트가 발생하고 Loop가 정상 종료되어야 한다.
func TestK8sActor_PrepareFailure_EmitsFailedEvent(t *testing.T) {
	sink := newChanSink()
	drv := &actorTestDriver{prepareErr: errors.New("disk quota exceeded")}
	a := NewK8sActor("spawn-1", drv, 8)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); a.Loop(ctx) }()

	mustEnqueue(t, a, api.Command{Kind: api.CmdBind, Bind: &api.Bind{SpawnKey: "spawn-1"}, Sink: sink})
	sink.waitState(t, api.StateStarting, 2*time.Second)

	mustEnqueue(t, a, api.Command{
		Kind:   api.CmdRun,
		Run:    &api.RunSpec{RunID: "run-1", ImageRef: "busybox:1.36"},
		Policy: policy.DefaultPolicyB(time.Second),
		Sink:   sink,
	})

	sink.waitState(t, api.StateFailed, 2*time.Second)

	cancel()
	<-done
	goleak.VerifyNone(t)
}

// TestK8sActor_StartFailure_EmitsFailedEvent: driver.Start가 에러를 반환하면
// StateFailed 이벤트가 발생하고 Loop가 정상 종료되어야 한다.
func TestK8sActor_StartFailure_EmitsFailedEvent(t *testing.T) {
	sink := newChanSink()
	drv := &actorTestDriver{startErr: errors.New("image pull failed")}
	a := NewK8sActor("spawn-1", drv, 8)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); a.Loop(ctx) }()

	mustEnqueue(t, a, api.Command{Kind: api.CmdBind, Bind: &api.Bind{SpawnKey: "spawn-1"}, Sink: sink})
	sink.waitState(t, api.StateStarting, 2*time.Second)

	mustEnqueue(t, a, api.Command{
		Kind:   api.CmdRun,
		Run:    &api.RunSpec{RunID: "run-1", ImageRef: "busybox:1.36"},
		Policy: policy.DefaultPolicyB(time.Second),
		Sink:   sink,
	})

	sink.waitState(t, api.StateFailed, 2*time.Second)

	cancel()
	<-done
	goleak.VerifyNone(t)
}

// TestK8sActor_OnTermCalledExactlyOnce: Loop가 종료될 때 onTerm 콜백은
// 정확히 한 번만 호출되어야 한다.
func TestK8sActor_OnTermCalledExactlyOnce(t *testing.T) {
	var mu sync.Mutex
	termCount := 0

	a := NewK8sActor("spawn-1", &actorTestDriver{}, 8)
	a.OnTerminate(func() {
		mu.Lock()
		termCount++
		mu.Unlock()
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); a.Loop(ctx) }()

	cancel()
	<-done

	mu.Lock()
	n := termCount
	mu.Unlock()
	if n != 1 {
		t.Fatalf("expected onTerm called exactly once, got %d", n)
	}
	goleak.VerifyNone(t)
}

// TestK8sActor_CancelDuringRun_NoDeadlock: 실행 중인 run에 CmdCancel을 전송하면
// StateCancelling → StateFailed 이벤트 순서가 보장되고 Loop가 정상 종료되어야 한다.
func TestK8sActor_CancelDuringRun_NoDeadlock(t *testing.T) {
	sink := newChanSink()
	drv := &actorTestDriver{
		waitStarted: make(chan struct{}),
		waitBlock:   make(chan struct{}),
	}
	a := NewK8sActor("spawn-1", drv, 8)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); a.Loop(ctx) }()

	mustEnqueue(t, a, api.Command{Kind: api.CmdBind, Bind: &api.Bind{SpawnKey: "spawn-1"}, Sink: sink})
	sink.waitState(t, api.StateStarting, 2*time.Second)

	mustEnqueue(t, a, api.Command{
		Kind:   api.CmdRun,
		Run:    &api.RunSpec{RunID: "run-1", ImageRef: "busybox:1.36"},
		Policy: policy.DefaultPolicyB(5 * time.Second),
		Sink:   sink,
	})

	select {
	case <-drv.waitStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not start within timeout")
	}
	sink.waitState(t, api.StateRunning, 2*time.Second)

	mustEnqueue(t, a, api.Command{
		Kind:   api.CmdCancel,
		Cancel: &api.CancelReq{RunID: "run-1"},
		Sink:   sink,
	})

	sink.waitState(t, api.StateCancelling, 2*time.Second)
	// runCtx가 취소되면 Wait이 ctx.Err()를 반환 → StateCancelled
	sink.waitState(t, api.StateCancelled, 2*time.Second)

	cancel()
	<-done
	goleak.VerifyNone(t)
}

// TestK8sActor_WaitFailure_EmitsFailedEvent: driver.Wait가 non-nil 에러를 반환하면
// StateFailed 이벤트가 방출되고 goroutine이 누수되지 않아야 한다.
func TestK8sActor_WaitFailure_EmitsFailedEvent(t *testing.T) {
	sink := newChanSink()
	drv := &actorTestDriver{waitErr: errors.New("node disconnected")}
	a := NewK8sActor("spawn-1", drv, 8)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); a.Loop(ctx) }()

	mustEnqueue(t, a, api.Command{Kind: api.CmdBind, Bind: &api.Bind{SpawnKey: "spawn-1"}, Sink: sink})
	sink.waitState(t, api.StateStarting, 2*time.Second)

	mustEnqueue(t, a, api.Command{
		Kind:   api.CmdRun,
		Run:    &api.RunSpec{RunID: "run-1", ImageRef: "busybox:1.36"},
		Policy: policy.DefaultPolicyB(time.Second),
		Sink:   sink,
	})

	sink.waitState(t, api.StateFailed, 2*time.Second)

	cancel()
	<-done
	goleak.VerifyNone(t)
}

// TestK8sActor_LoopCancel_ActiveRunEmitsCancelledEvent: Loop ctx가 취소될 때
// Wait 중인 active run에 StateCancelled 이벤트가 방출된 후 Loop이 정상 종료되어야 한다.
func TestK8sActor_LoopCancel_ActiveRunEmitsCancelledEvent(t *testing.T) {
	sink := newChanSink()
	drv := &actorTestDriver{
		waitStarted: make(chan struct{}),
		waitBlock:   make(chan struct{}),
	}
	a := NewK8sActor("spawn-1", drv, 8)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); a.Loop(ctx) }()

	mustEnqueue(t, a, api.Command{Kind: api.CmdBind, Bind: &api.Bind{SpawnKey: "spawn-1"}, Sink: sink})
	sink.waitState(t, api.StateStarting, 2*time.Second)

	mustEnqueue(t, a, api.Command{
		Kind:   api.CmdRun,
		Run:    &api.RunSpec{RunID: "run-1", ImageRef: "busybox:1.36"},
		Policy: policy.DefaultPolicyB(5 * time.Second),
		Sink:   sink,
	})

	select {
	case <-drv.waitStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not start within timeout")
	}
	sink.waitState(t, api.StateRunning, 2*time.Second)

	// Loop ctx 취소 → runCtx도 취소 → Wait이 ctx.Err()를 반환 → StateCancelled
	cancel()
	sink.waitState(t, api.StateCancelled, 2*time.Second)

	<-done
	goleak.VerifyNone(t)
}

// TestK8sActor_CancelFailure_LoopSurvives: driver.Cancel가 에러를 반환해도
// 에러는 production code에서 _ =로 무시되므로 Loop이 정상 종료되어야 한다.
func TestK8sActor_CancelFailure_LoopSurvives(t *testing.T) {
	sink := newChanSink()
	drv := &actorTestDriver{
		waitStarted: make(chan struct{}),
		waitBlock:   make(chan struct{}),
		cancelErr:   errors.New("connection refused"),
	}
	a := NewK8sActor("spawn-1", drv, 8)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); a.Loop(ctx) }()

	mustEnqueue(t, a, api.Command{Kind: api.CmdBind, Bind: &api.Bind{SpawnKey: "spawn-1"}, Sink: sink})
	sink.waitState(t, api.StateStarting, 2*time.Second)

	mustEnqueue(t, a, api.Command{
		Kind:   api.CmdRun,
		Run:    &api.RunSpec{RunID: "run-1", ImageRef: "busybox:1.36"},
		Policy: policy.DefaultPolicyB(5 * time.Second),
		Sink:   sink,
	})

	select {
	case <-drv.waitStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not start within timeout")
	}
	sink.waitState(t, api.StateRunning, 2*time.Second)

	// driver.Cancel이 에러를 반환해도 runCtx는 st.cancel()로 취소됨 → StateCancelled
	mustEnqueue(t, a, api.Command{
		Kind:   api.CmdCancel,
		Cancel: &api.CancelReq{RunID: "run-1"},
		Sink:   sink,
	})
	sink.waitState(t, api.StateCancelling, 2*time.Second)
	sink.waitState(t, api.StateCancelled, 2*time.Second)

	drv.mu.Lock()
	calls := drv.cancelCalls
	drv.mu.Unlock()
	if calls == 0 {
		t.Fatal("expected driver.Cancel to be called despite returning error")
	}

	cancel()
	<-done
	goleak.VerifyNone(t)
}

// TestK8sActor_WaitJobFailed_EmitsFailedNotCancelled: driver.Wait가 non-context 에러를 반환하면
// StateFailed가 방출되어야 한다 (StateCancelled가 아님).
func TestK8sActor_WaitJobFailed_EmitsFailedNotCancelled(t *testing.T) {
	sink := newChanSink()
	drv := &actorTestDriver{waitErr: errors.New("job failed: exit code 1")}
	a := NewK8sActor("spawn-1", drv, 8)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); a.Loop(ctx) }()

	mustEnqueue(t, a, api.Command{Kind: api.CmdBind, Bind: &api.Bind{SpawnKey: "spawn-1"}, Sink: sink})
	sink.waitState(t, api.StateStarting, 2*time.Second)

	mustEnqueue(t, a, api.Command{
		Kind:   api.CmdRun,
		Run:    &api.RunSpec{RunID: "run-1", ImageRef: "busybox:1.36"},
		Policy: policy.DefaultPolicyB(time.Second),
		Sink:   sink,
	})

	sink.waitState(t, api.StateFailed, 2*time.Second)

	// Make sure StateCancelled was NOT emitted
	sink.mu.Lock()
	events := make([]api.Event, len(sink.events))
	copy(events, sink.events)
	sink.mu.Unlock()
	for _, ev := range events {
		if ev.RunID == "run-1" && ev.State == api.StateCancelled {
			t.Fatal("expected StateFailed not StateCancelled for job failure")
		}
	}

	cancel()
	<-done
	goleak.VerifyNone(t)
}

// TestK8sActor_ExecSemSaturated_LoopProcessesCancel verifies that when execSem
// is saturated (all slots occupied), the actor Loop can still process CmdCancel
// without deadlock.
func TestK8sActor_ExecSemSaturated_LoopProcessesCancel(t *testing.T) {
	sink := newChanSink()

	drv := &actorTestDriver{
		waitStarted: make(chan struct{}),
		waitBlock:   make(chan struct{}),
	}
	// Create actor with execSem size 1 so run-1 saturates it
	a := &K8sActor{
		key:     "spawn-1",
		drv:     drv,
		mb:      actor.NewMailbox[api.Command](8),
		execSem: make(chan struct{}, 1),
		active: make(map[string]struct {
			h      driver.Handle
			cancel context.CancelFunc
		}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); a.Loop(ctx) }()

	mustEnqueue(t, a, api.Command{Kind: api.CmdBind, Bind: &api.Bind{SpawnKey: "spawn-1"}, Sink: sink})
	sink.waitState(t, api.StateStarting, 2*time.Second)

	// run-1: will occupy the single execSem slot and block in Wait
	mustEnqueue(t, a, api.Command{
		Kind:   api.CmdRun,
		Run:    &api.RunSpec{RunID: "run-1", ImageRef: "busybox:1.36"},
		Policy: policy.DefaultPolicyB(5 * time.Second),
		Sink:   sink,
	})
	select {
	case <-drv.waitStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("run-1 Wait did not start within timeout")
	}

	// run-2: Loop must accept this without blocking (execSem is full but Loop doesn't wait)
	mustEnqueue(t, a, api.Command{
		Kind:   api.CmdRun,
		Run:    &api.RunSpec{RunID: "run-2", ImageRef: "busybox:1.36"},
		Policy: policy.DefaultPolicyB(5 * time.Second),
		Sink:   sink,
	})

	// Cancel run-2 — Loop must process this without deadlock
	mustEnqueue(t, a, api.Command{
		Kind:   api.CmdCancel,
		Cancel: &api.CancelReq{RunID: "run-2"},
		Sink:   sink,
	})

	// run-2 must get StateCancelled (cancelled before acquiring semaphore or during start)
	sink.waitFor(t, func(ev api.Event) bool {
		return ev.RunID == "run-2" && ev.State == api.StateCancelled
	}, 2*time.Second)

	// Release run-1
	close(drv.waitBlock)
	sink.waitFor(t, func(ev api.Event) bool {
		return ev.RunID == "run-1" && (ev.State == api.StateSucceeded || ev.State == api.StateFailed || ev.State == api.StateCancelled)
	}, 2*time.Second)

	cancel()
	<-done
	goleak.VerifyNone(t)
}

// TestK8sActor_RunComplete_LoopExits verifies that after all runs complete the
// actor becomes idle, closes its mailbox, and the Loop goroutine terminates.
func TestK8sActor_RunComplete_LoopExits(t *testing.T) {
	sink := newChanSink()
	drv := &actorTestDriver{} // no errors, no blocking
	a := NewK8sActor("spawn-1", drv, 8)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); a.Loop(ctx) }()

	// Set up OnIdle — the actor will call mb.Close() on idle regardless of this
	a.OnIdle(func() {})

	mustEnqueue(t, a, api.Command{Kind: api.CmdBind, Bind: &api.Bind{SpawnKey: "spawn-1"}, Sink: sink})
	sink.waitState(t, api.StateStarting, 2*time.Second)

	mustEnqueue(t, a, api.Command{
		Kind:   api.CmdRun,
		Run:    &api.RunSpec{RunID: "run-1", ImageRef: "busybox:1.36"},
		Policy: policy.DefaultPolicyB(5 * time.Second),
		Sink:   sink,
	})
	sink.waitState(t, api.StateSucceeded, 2*time.Second)

	// After run completes, actor becomes idle and mailbox is closed → Loop exits
	select {
	case <-done:
		// success: Loop terminated
	case <-time.After(2 * time.Second):
		t.Fatal("Loop goroutine did not terminate after actor became idle")
	}

	goleak.VerifyNone(t)
}

func mustEnqueue(t *testing.T, a *K8sActor, cmd api.Command) {
	t.Helper()
	if ok := a.EnqueueCtx(context.Background(), cmd); !ok {
		t.Fatal("enqueue failed")
	}
}

func waitForEventState(t *testing.T, sink *actorTestSink, state api.State, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, ev := range sink.snapshot() {
			if ev.State == state {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for event state=%s", state)
}

func waitForEventWithState(t *testing.T, sink *actorTestSink, state api.State, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, ev := range sink.snapshot() {
			if ev.RunID != "" && ev.State == state {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for state=%s", state)
}

func assertEventMessageContains(t *testing.T, events []api.Event, state api.State, want string) {
	t.Helper()
	for _, ev := range events {
		if ev.State == state && ev.Message != "" && strings.Contains(ev.Message, want) {
			return
		}
	}
	t.Fatalf("no %s event contained %q", state, want)
}
