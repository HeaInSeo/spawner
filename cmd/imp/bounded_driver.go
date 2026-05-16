package imp

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/HeaInSeo/spawner/pkg/api"
	"github.com/HeaInSeo/spawner/pkg/driver"
)

// BoundedDriver wraps a driver.Driver and limits the number of concurrent
// Start() calls (= concurrent K8s Job creations) to a configurable maximum.
//
// Motivation: K8s API servers have rate limits. Without a semaphore, a burst
// of dag-go readiness events can flood the API server with simultaneous Job
// Create requests. BoundedDriver sits between SpawnerNode and DriverK8s
// and absorbs that burst.
//
// Semaphore scope: Start() only. Prepare/Wait/Signal/Cancel are pass-through.
// This matches the bottleneck: job creation is the K8s write burst; waiting
// for completion does not count against the slot.
//
// BaseDriver is embedded to satisfy the driver.Driver sealed interface
// (driverMarker(_sealed) must be provided by an embedded BaseDriver).
type BoundedDriver struct {
	driver.BaseDriver
	inner       driver.Driver
	slots       chan struct{} // counting semaphore: len(slots) = available
	inflight    atomic.Int64  // current Start() calls in progress
	maxInflight int
}

var _ driver.Driver = (*BoundedDriver)(nil)

// NewBoundedDriver returns a BoundedDriver that allows at most maxConcurrent
// simultaneous Start() calls. Panics if maxConcurrent < 1.
func NewBoundedDriver(inner driver.Driver, maxConcurrent int) *BoundedDriver {
	if maxConcurrent < 1 {
		panic(fmt.Sprintf("BoundedDriver: maxConcurrent must be >= 1, got %d", maxConcurrent))
	}
	slots := make(chan struct{}, maxConcurrent)
	for i := 0; i < maxConcurrent; i++ {
		slots <- struct{}{}
	}
	return &BoundedDriver{
		inner:       inner,
		slots:       slots,
		maxInflight: maxConcurrent,
	}
}

// Prepare is a pure read/build step — no K8s write — so it is not bounded.
func (b *BoundedDriver) Prepare(ctx context.Context, spec api.RunSpec) (driver.Prepared, error) {
	return b.inner.Prepare(ctx, spec)
}

// Start acquires a semaphore slot before calling inner.Start(), then releases
// the slot when Start() returns (success or error).
// If ctx is cancelled while waiting for a slot, returns ctx.Err() immediately.
func (b *BoundedDriver) Start(ctx context.Context, p driver.Prepared) (driver.Handle, error) {
	// Acquire slot
	select {
	case <-b.slots:
	case <-ctx.Done():
		return nil, fmt.Errorf("BoundedDriver: context cancelled waiting for slot: %w", ctx.Err())
	}

	b.inflight.Add(1)
	defer func() {
		b.inflight.Add(-1)
		b.slots <- struct{}{} // release slot
	}()

	return b.inner.Start(ctx, p)
}

// Wait, Signal, Cancel are pass-through — they don't create K8s resources.

func (b *BoundedDriver) Wait(ctx context.Context, h driver.Handle) (api.Event, error) {
	return b.inner.Wait(ctx, h)
}

func (b *BoundedDriver) Signal(ctx context.Context, h driver.Handle, sig api.Signal) error {
	return b.inner.Signal(ctx, h, sig)
}

func (b *BoundedDriver) Cancel(ctx context.Context, h driver.Handle) error {
	return b.inner.Cancel(ctx, h)
}

// BoundedDriverStats holds a snapshot of BoundedDriver observability metrics.
type BoundedDriverStats struct {
	Inflight      int64 // current number of Start() calls in progress
	Available     int   // semaphore slots currently available
	MaxConcurrent int   // configured upper bound
}

// Stats returns a snapshot of current concurrency metrics.
// Available = MaxConcurrent - Inflight (approximately; read without locking).
func (b *BoundedDriver) Stats() BoundedDriverStats {
	inflight := b.inflight.Load()
	return BoundedDriverStats{
		Inflight:      inflight,
		Available:     len(b.slots),
		MaxConcurrent: b.maxInflight,
	}
}
