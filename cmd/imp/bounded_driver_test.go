package imp_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/HeaInSeo/spawner/cmd/imp"
	"github.com/HeaInSeo/spawner/pkg/api"
	"github.com/HeaInSeo/spawner/pkg/driver"
)

// ── helpers ──────────────────────────────────────────────────────────────────

// slowDriver simulates a DriverK8s whose Start() takes a configurable duration.
// It counts the maximum observed concurrency so tests can assert burst control.
type slowDriver struct {
	driver.UnimplementedDriver
	delay      time.Duration
	mu         sync.Mutex
	concurrent int64
	maxSeen    int64
}

func (d *slowDriver) Start(_ context.Context, _ driver.Prepared) (driver.Handle, error) {
	cur := atomic.AddInt64(&d.concurrent, 1)
	d.mu.Lock()
	if cur > d.maxSeen {
		d.maxSeen = cur
	}
	d.mu.Unlock()

	time.Sleep(d.delay)
	atomic.AddInt64(&d.concurrent, -1)
	return dummyHandle{}, nil
}

type dummyHandle struct{ driver.BaseHandle }
type dummyPrepared struct{ driver.BasePrepared }

// ── tests ─────────────────────────────────────────────────────────────────────

// TestBoundedDriver_LimitsConcurrentStart proves that with sem=k,
// no more than k goroutines are inside inner.Start() simultaneously.
func TestBoundedDriver_LimitsConcurrentStart(t *testing.T) {
	const sem = 3
	const goroutines = 10

	inner := &slowDriver{delay: 30 * time.Millisecond}
	bd := imp.NewBoundedDriver(inner, sem)

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = bd.Start(context.Background(), dummyPrepared{})
		}()
	}
	wg.Wait()

	if inner.maxSeen > sem {
		t.Fatalf("BoundedDriver(sem=%d): inner.Start() had max concurrency %d — burst NOT controlled",
			sem, inner.maxSeen)
	}
	t.Logf("Q2 PASS: BoundedDriver(sem=%d) limited inner.Start() to max=%d concurrent",
		sem, inner.maxSeen)
	t.Logf("  goroutines=%d, inner_max_concurrent=%d (capped at sem=%d)",
		goroutines, inner.maxSeen, sem)
	t.Logf("  PROOF: BoundedDriver controls K8s Job creation burst")
}

// TestBoundedDriver_ContextCancelWhileWaiting proves that a goroutine blocked
// waiting for a slot returns ctx.Err() when its context is cancelled.
func TestBoundedDriver_ContextCancelWhileWaiting(t *testing.T) {
	const sem = 1

	// Fill the single slot with a Start that never returns (until released)
	hold := make(chan struct{})
	inner := &heldDriver{hold: hold}
	bd := imp.NewBoundedDriver(inner, sem)

	// Slot-holder
	holderDone := make(chan struct{})
	go func() {
		defer close(holderDone)
		_, _ = bd.Start(context.Background(), dummyPrepared{})
	}()
	time.Sleep(5 * time.Millisecond) // let slot-holder acquire the slot

	// This goroutine should block then be cancelled
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := bd.Start(ctx, dummyPrepared{})
	if err == nil {
		t.Fatal("expected error when context cancelled while waiting for slot")
	}
	t.Logf("PASS: blocked goroutine received error on ctx cancel: %v", err)

	close(hold)
	<-holderDone
}

// TestBoundedDriver_Stats proves Stats() reflects live concurrency.
func TestBoundedDriver_Stats(t *testing.T) {
	bd := imp.NewBoundedDriver(&imp.NopDriver{}, 5)
	stats := bd.Stats()
	if stats.MaxConcurrent != 5 {
		t.Fatalf("expected MaxConcurrent=5, got %d", stats.MaxConcurrent)
	}
	if stats.Available != 5 {
		t.Fatalf("expected Available=5 (idle), got %d", stats.Available)
	}
	if stats.Inflight != 0 {
		t.Fatalf("expected Inflight=0, got %d", stats.Inflight)
	}
	t.Logf("PASS: Stats() reports MaxConcurrent=%d Available=%d Inflight=%d",
		stats.MaxConcurrent, stats.Available, stats.Inflight)
}

// TestBoundedDriver_PreparePassthrough proves Prepare() is delegated to inner
// and the semaphore is NOT consumed (slot count unchanged after call).
func TestBoundedDriver_PreparePassthrough(t *testing.T) {
	const sem = 2
	bd := imp.NewBoundedDriver(&imp.NopDriver{}, sem)

	statsBefore := bd.Stats()
	_, _ = bd.Prepare(context.Background(), api.RunSpec{})
	statsAfter := bd.Stats()

	// Semaphore must not have been consumed by Prepare
	if statsBefore.Available != statsAfter.Available {
		t.Fatalf("Prepare consumed a semaphore slot (before=%d, after=%d)",
			statsBefore.Available, statsAfter.Available)
	}
	t.Logf("PASS: Prepare() is pass-through, slot count unchanged (%d)", statsAfter.Available)
}

func TestBoundedDriver_PassthroughMethods(t *testing.T) {
	inner := &passthroughDriver{}
	bd := imp.NewBoundedDriver(inner, 2)

	h := dummyHandle{}
	if _, err := bd.Wait(context.Background(), h); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if err := bd.Signal(context.Background(), h, api.Signal{RunID: "run-1", Name: "term"}); err != nil {
		t.Fatalf("Signal: %v", err)
	}
	if err := bd.Cancel(context.Background(), h); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	if inner.waitCalls != 1 || inner.signalCalls != 1 || inner.cancelCalls != 1 {
		t.Fatalf("expected passthrough calls = 1/1/1, got wait=%d signal=%d cancel=%d",
			inner.waitCalls, inner.signalCalls, inner.cancelCalls)
	}
}

// ── additional helpers ────────────────────────────────────────────────────────

// heldDriver blocks inside Start() until hold is closed.
type heldDriver struct {
	driver.UnimplementedDriver
	hold chan struct{}
}

func (h *heldDriver) Start(_ context.Context, _ driver.Prepared) (driver.Handle, error) {
	<-h.hold
	return dummyHandle{}, nil
}

type passthroughDriver struct {
	driver.UnimplementedDriver
	waitCalls   int
	signalCalls int
	cancelCalls int
}

func (p *passthroughDriver) Wait(context.Context, driver.Handle) (api.Event, error) {
	p.waitCalls++
	return api.Event{State: api.StateSucceeded}, nil
}

func (p *passthroughDriver) Signal(context.Context, driver.Handle, api.Signal) error {
	p.signalCalls++
	return nil
}

func (p *passthroughDriver) Cancel(context.Context, driver.Handle) error {
	p.cancelCalls++
	return nil
}
