package runtime

import (
	"context"
	"sync"
	"time"
)

// attemptEntry holds per-attempt state inside the Runtime.
type attemptEntry struct {
	mu sync.Mutex

	handle    AttemptHandle // populated after Create succeeds
	createErr error         // set on Create failure; cleared on retry

	req   AttemptRequest
	state AttemptState

	// Terminal state metadata (set atomically by transitionToTerminal).
	terminalReason  string
	terminalMessage string
	terminalTime    time.Time
	// terminalCh is closed when the attempt first reaches a terminal state.
	// Watchers and the timeout goroutine select on this to detect termination.
	terminalCh chan struct{}

	// doneCh is closed when JobClient.Create completes (success or failure).
	// Read-only after construction. Used by concurrent SubmitAttempt callers to
	// wait for an in-progress Create before reading handle/createErr.
	doneCh chan struct{}

	// timeoutCancel cancels the AttemptTimeout timer goroutine.
	// Called automatically by transitionToTerminal to stop the timer.
	timeoutCancel context.CancelFunc

	// eventCh is the output channel registered by WatchAttempt.
	eventCh chan<- AttemptEvent
}

// transitionToTerminal atomically moves the entry to a terminal state.
// Returns true if this call won the race (first to set terminal state).
// On success: records terminal metadata, cancels the timeout timer, closes terminalCh.
func (e *attemptEntry) transitionToTerminal(state AttemptState, reason, msg string, ts time.Time) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state.IsTerminal() {
		return false
	}
	e.state = state
	e.terminalReason = reason
	e.terminalMessage = msg
	e.terminalTime = ts
	if e.timeoutCancel != nil {
		e.timeoutCancel()
		e.timeoutCancel = nil
	}
	close(e.terminalCh)
	return true
}

type runtimeImpl struct {
	client JobClient
	cfg    RuntimeConfig

	mu       sync.RWMutex
	attempts map[string]*attemptEntry
	active   int // count of non-terminal attempts; protected by mu

	ctx    context.Context
	cancel context.CancelFunc
}

// NewRuntime creates a new Runtime with the given JobClient and config.
// Returns error if cfg is invalid (NamingSalt or Namespace missing).
func NewRuntime(client JobClient, cfg RuntimeConfig) (Runtime, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg.applyDefaults()

	ctx, cancel := context.WithCancel(context.Background())
	return &runtimeImpl{
		client:   client,
		cfg:      cfg,
		attempts: make(map[string]*attemptEntry),
		ctx:      ctx,
		cancel:   cancel,
	}, nil
}

// SubmitAttempt validates the request, initiates job creation, and blocks until
// JobClient.Create returns. Uses a per-attempt goroutine driven by a Runtime-owned
// context, so cancelling the caller's ctx does not abort Create.
//
// Idempotent retry contract:
//   - If Create is in progress for the same AttemptID (ErrSubmitOutcomeUnknown retry),
//     the caller waits for the in-flight Create to finish and returns its result.
//   - If Create already succeeded for the same AttemptID, returns the stored handle immediately.
func (r *runtimeImpl) SubmitAttempt(ctx context.Context, req AttemptRequest) (AttemptHandle, error) {
	if err := req.Validate(); err != nil {
		return AttemptHandle{}, err
	}

	r.mu.Lock()

	if existing, ok := r.attempts[req.AttemptID]; ok {
		r.mu.Unlock()
		// Wait for in-flight Create (or return immediately if already done).
		select {
		case <-existing.doneCh:
		case <-ctx.Done():
			return AttemptHandle{}, ErrSubmitOutcomeUnknown
		}
		existing.mu.Lock()
		handle, err := existing.handle, existing.createErr
		existing.mu.Unlock()
		return handle, err
	}

	if r.active >= r.cfg.MaxConcurrency {
		r.mu.Unlock()
		return AttemptHandle{}, ErrCapacityExceeded
	}

	entry := &attemptEntry{
		req:        req,
		state:      AttemptStateAccepted,
		doneCh:     make(chan struct{}),
		terminalCh: make(chan struct{}),
	}
	r.attempts[req.AttemptID] = entry
	r.active++
	r.mu.Unlock()

	go r.runCreate(entry)

	select {
	case <-entry.doneCh:
		entry.mu.Lock()
		handle, err := entry.handle, entry.createErr
		entry.mu.Unlock()
		return handle, err
	case <-ctx.Done():
		return AttemptHandle{}, ErrSubmitOutcomeUnknown
	}
}

// runCreate drives JobClient.Create with a Runtime-owned context.
// On failure, removes the entry from the map so the next SubmitAttempt
// with the same AttemptID starts fresh.
// On success, starts the AttemptTimeout timer and closes entry.doneCh.
func (r *runtimeImpl) runCreate(entry *attemptEntry) {
	req := entry.req
	jobName := jobNameFor(r.cfg.NamingSalt, req.AttemptID)
	marker := attemptMarkerFor(r.cfg.NamingSalt, req.AttemptID)

	createCtx, createCancel := context.WithTimeout(r.ctx, r.cfg.CreateTimeout)
	defer createCancel()

	ref, err := r.client.Create(createCtx, JobCreateRequest{
		AttemptRequest: req,
		JobName:        jobName,
		Namespace:      r.cfg.Namespace,
		AttemptMarker:  marker,
	})
	if err != nil {
		entry.mu.Lock()
		entry.createErr = err
		entry.state = AttemptStateFailed
		entry.mu.Unlock()

		r.mu.Lock()
		delete(r.attempts, req.AttemptID)
		r.active--
		r.mu.Unlock()

		close(entry.doneCh)
		return
	}

	handle := AttemptHandle{
		AttemptID:  req.AttemptID,
		RuntimeID:  r.cfg.RuntimeID,
		BackendRef: ref,
	}

	entry.mu.Lock()
	entry.handle = handle
	entry.state = AttemptStateSubmitted
	if req.AttemptTimeout > 0 {
		tCtx, tCancel := context.WithCancel(r.ctx)
		entry.timeoutCancel = tCancel
		go r.runTimeout(tCtx, entry, req.AttemptTimeout)
	}
	entry.mu.Unlock()

	close(entry.doneCh)
}

// runTimeout enforces the AttemptTimeout deadline.
// Starts after Create succeeds (Submitted state). Covers queue + scheduling + run time.
// Uses transitionToTerminal to set the terminal state; the watchLoop goroutine detects
// the terminalCh close and emits the terminal event to watchers.
func (r *runtimeImpl) runTimeout(ctx context.Context, entry *attemptEntry, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		if !entry.transitionToTerminal(AttemptStateFailed, ReasonDeadlineExceeded, "attempt timeout exceeded", time.Now()) {
			return // already terminal; another goroutine won the race
		}

		r.mu.Lock()
		r.active--
		r.mu.Unlock()

		deleteCtx, cancel := context.WithTimeout(r.ctx, r.cfg.DeleteTimeout)
		defer cancel()
		_ = r.client.Delete(deleteCtx, entry.handle.BackendRef)
		// watchLoop detects terminalCh close and emits the terminal event.

	case <-ctx.Done():
		// Attempt completed or was cancelled; timer was cancelled via transitionToTerminal.
	}
}
