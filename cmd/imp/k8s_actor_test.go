package imp

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

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

type actorTestDriver struct {
	driver.UnimplementedDriver
	mu          sync.Mutex
	prepareErr  error
	startErr    error
	waitErr     error
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
	d.mu.Unlock()
	return nil
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
