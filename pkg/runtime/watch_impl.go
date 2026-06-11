package runtime

import (
	"context"
	"time"
)

// WatchAttempt returns a channel of AttemptEvents until terminal state or ctx cancellation.
// The channel is closed after the terminal event is emitted.
//
// Normal path: first event is AttemptStateSubmitted.
// Recovery path (attempt not in memory): uses Snapshot-based reconciliation (S4).
func (r *runtimeImpl) WatchAttempt(ctx context.Context, h AttemptHandle) (<-chan AttemptEvent, error) {
	if h.AttemptID == "" || h.BackendRef.ID == "" {
		return nil, ErrAttemptNotFound
	}

	r.mu.RLock()
	entry, ok := r.attempts[h.AttemptID]
	r.mu.RUnlock()

	if !ok {
		return r.watchRecovery(ctx, h)
	}

	outCh := make(chan AttemptEvent, 16)

	entry.mu.Lock()
	if entry.state.IsTerminal() {
		// Already terminal: emit once and close.
		ev := AttemptEvent{
			AttemptID: h.AttemptID,
			State:     entry.state,
			Reason:    entry.terminalReason,
			Message:   entry.terminalMessage,
			Timestamp: entry.terminalTime,
		}
		entry.mu.Unlock()
		go func() {
			defer close(outCh)
			select {
			case outCh <- ev:
			case <-ctx.Done():
			}
		}()
		return outCh, nil
	}
	entry.eventCh = outCh
	entry.mu.Unlock()

	first := AttemptEvent{
		AttemptID: h.AttemptID,
		State:     AttemptStateSubmitted,
		Timestamp: time.Now(),
	}
	go r.watchLoop(ctx, entry, h, outCh, first)
	return outCh, nil
}

// CancelAttempt requests cancellation of a non-terminal attempt.
// Idempotent: if the attempt is already terminal, this is a no-op.
// The terminal outcome is never overwritten.
func (r *runtimeImpl) CancelAttempt(ctx context.Context, h AttemptHandle) error {
	if h.AttemptID == "" {
		return ErrAttemptNotFound
	}

	r.mu.RLock()
	entry, ok := r.attempts[h.AttemptID]
	r.mu.RUnlock()

	if !ok {
		return nil // not in memory: already terminal or Create failed; no-op
	}

	if !entry.transitionToTerminal(AttemptStateCancelled, ReasonUserCancel, "cancelled by caller", time.Now()) {
		return nil // already terminal
	}

	r.mu.Lock()
	r.active--
	r.mu.Unlock()

	deleteCtx, cancel := context.WithTimeout(r.ctx, r.cfg.DeleteTimeout)
	defer cancel()
	_ = r.client.Delete(deleteCtx, h.BackendRef)
	// watchLoop detects terminalCh close and emits the Cancelled event.
	return nil
}

// watchLoop drives the JobClient.Watch event loop for a single attempt.
// firstEvent is emitted immediately: Submitted on the normal path, or the
// Snapshot state on the recovery path. The loop then forwards JobEvents from
// JobClient.Watch and handles temporary errors with backoff reconnect.
func (r *runtimeImpl) watchLoop(ctx context.Context, entry *attemptEntry, h AttemptHandle, outCh chan<- AttemptEvent, firstEvent AttemptEvent) {
	defer func() {
		close(outCh)
		entry.mu.Lock()
		if entry.eventCh == outCh {
			entry.eventCh = nil
		}
		entry.mu.Unlock()
	}()

	if !r.trySend(ctx, outCh, firstEvent) {
		return
	}

	backoff := 500 * time.Millisecond

	for {
		jw, err := r.client.Watch(ctx, h.BackendRef)
		if err != nil {
			if entry.transitionToTerminal(AttemptStateFailed, "", err.Error(), time.Now()) {
				r.decrementActive()
				_ = r.trySend(ctx, outCh, AttemptEvent{
					AttemptID: h.AttemptID,
					State:     AttemptStateFailed,
					Message:   err.Error(),
					Timestamp: time.Now(),
				})
			}
			return
		}

		done, reconnect := r.consumeWatch(ctx, entry, h, outCh, jw)
		if done {
			return
		}
		if !reconnect {
			return
		}

		// Temporary error: backoff then reconnect.
		select {
		case <-time.After(backoff):
			if backoff < 30*time.Second {
				backoff *= 2
			}
		case <-ctx.Done():
			return
		case <-entry.terminalCh:
			r.emitTerminal(ctx, entry, h, outCh)
			return
		}
	}
}

// consumeWatch reads from a single JobWatch until the stream ends, the attempt
// reaches terminal state, or ctx is cancelled.
// Returns (done=true, reconnect=false) on terminal or ctx cancel.
// Returns (done=false, reconnect=true) on a temporary watch error.
//
// errs is tracked locally and set to nil on close. A nil channel blocks forever
// in select, which prevents a closed Errs from repeatedly winning the select race
// against a still-live Events channel.
func (r *runtimeImpl) consumeWatch(ctx context.Context, entry *attemptEntry, h AttemptHandle, outCh chan<- AttemptEvent, jw JobWatch) (done bool, reconnect bool) {
	errs := jw.Errs // set to nil on close to remove from select

	for {
		select {
		case ev, ok := <-jw.Events:
			if !ok {
				// Events closed. Per JobWatch contract, errors are sent to Errs
				// before Events is closed, so check for a buffered error first.
				if errs != nil {
					select {
					case watchErr, hasErr := <-errs:
						if hasErr {
							return r.handleWatchErr(ctx, entry, h, outCh, watchErr)
						}
					default:
					}
				}
				return true, false
			}

			// Drop backward state transitions.
			entry.mu.Lock()
			cur := entry.state
			entry.mu.Unlock()
			if stateOrder(ev.State) < stateOrder(cur) {
				continue
			}

			ae := AttemptEvent{
				AttemptID: h.AttemptID,
				State:     ev.State,
				Reason:    ev.Reason,
				Message:   ev.Message,
				Timestamp: ev.Timestamp,
			}

			if ev.State.IsTerminal() {
				if entry.transitionToTerminal(ev.State, ev.Reason, ev.Message, ev.Timestamp) {
					r.decrementActive()
				}
				_ = r.trySend(ctx, outCh, ae)
				return true, false
			}

			entry.mu.Lock()
			entry.state = ev.State
			entry.mu.Unlock()
			if !r.trySend(ctx, outCh, ae) {
				return true, false
			}

		case watchErr, ok := <-errs:
			if !ok {
				errs = nil // exhausted; stop selecting on this channel
				continue
			}
			return r.handleWatchErr(ctx, entry, h, outCh, watchErr)

		case <-entry.terminalCh:
			// Terminal from timeout or CancelAttempt.
			r.emitTerminal(ctx, entry, h, outCh)
			return true, false

		case <-ctx.Done():
			return true, false
		}
	}
}

// handleWatchErr processes a single JobWatchError.
func (r *runtimeImpl) handleWatchErr(ctx context.Context, entry *attemptEntry, h AttemptHandle, outCh chan<- AttemptEvent, watchErr JobWatchError) (done bool, reconnect bool) {
	if watchErr.Temporary {
		return false, true
	}
	if entry.transitionToTerminal(AttemptStateFailed, watchErr.Reason, watchErr.Message, time.Now()) {
		r.decrementActive()
	}
	_ = r.trySend(ctx, outCh, AttemptEvent{
		AttemptID: h.AttemptID,
		State:     AttemptStateFailed,
		Reason:    watchErr.Reason,
		Message:   watchErr.Message,
		Timestamp: time.Now(),
	})
	return true, false
}

// watchRecovery handles WatchAttempt for attempts not found in memory.
// Calls JobClient.Snapshot to determine the current state, then either emits
// the terminal event (if already done) or starts a watchLoop with the snapshot
// state as the first event.
//
// Per spec: JUMI must tolerate receiving Running or a terminal state as the
// first event on the recovery path.
func (r *runtimeImpl) watchRecovery(ctx context.Context, h AttemptHandle) (<-chan AttemptEvent, error) {
	if err := h.BackendRef.Validate(); err != nil {
		return nil, ErrAttemptNotFound
	}

	snapCtx, snapCancel := context.WithTimeout(r.ctx, r.cfg.CreateTimeout)
	snap, err := r.client.Snapshot(snapCtx, h.BackendRef)
	snapCancel()
	if err != nil {
		return nil, ErrAttemptNotFound
	}
	if !snap.Exists {
		return nil, ErrAttemptNotFound
	}

	outCh := make(chan AttemptEvent, 16)

	r.mu.Lock()
	// Another goroutine may have added this attempt while we were snapshotting.
	if existing, ok := r.attempts[h.AttemptID]; ok {
		r.mu.Unlock()
		existing.mu.Lock()
		existing.eventCh = outCh
		existing.mu.Unlock()
		first := AttemptEvent{
			AttemptID: h.AttemptID,
			State:     snap.State,
			Reason:    snap.Reason,
			Message:   snap.Message,
			Timestamp: snap.Timestamp,
		}
		go r.watchLoop(ctx, existing, h, outCh, first)
		return outCh, nil
	}

	entry := &attemptEntry{
		req:        AttemptRequest{AttemptID: h.AttemptID},
		handle:     h,
		state:      snap.State,
		doneCh:     make(chan struct{}),
		terminalCh: make(chan struct{}),
	}
	close(entry.doneCh) // Create already completed (BackendRef is populated).

	if snap.State.IsTerminal() {
		entry.terminalReason = snap.Reason
		entry.terminalMessage = snap.Message
		entry.terminalTime = snap.Timestamp
		close(entry.terminalCh)
	} else {
		entry.eventCh = outCh
		r.active++
	}
	r.attempts[h.AttemptID] = entry
	r.mu.Unlock()

	first := AttemptEvent{
		AttemptID: h.AttemptID,
		State:     snap.State,
		Reason:    snap.Reason,
		Message:   snap.Message,
		Timestamp: snap.Timestamp,
	}

	if snap.State.IsTerminal() {
		go func() {
			defer close(outCh)
			select {
			case outCh <- first:
			case <-ctx.Done():
			}
		}()
		return outCh, nil
	}

	go r.watchLoop(ctx, entry, h, outCh, first)
	return outCh, nil
}

// trySend sends ev to outCh or returns false if ctx is cancelled.
func (r *runtimeImpl) trySend(ctx context.Context, outCh chan<- AttemptEvent, ev AttemptEvent) bool {
	select {
	case outCh <- ev:
		return true
	case <-ctx.Done():
		return false
	}
}

func (r *runtimeImpl) decrementActive() {
	r.mu.Lock()
	r.active--
	r.mu.Unlock()
}

// emitTerminal reads the current terminal state from entry and sends it to outCh.
func (r *runtimeImpl) emitTerminal(ctx context.Context, entry *attemptEntry, h AttemptHandle, outCh chan<- AttemptEvent) {
	entry.mu.Lock()
	ev := AttemptEvent{
		AttemptID: h.AttemptID,
		State:     entry.state,
		Reason:    entry.terminalReason,
		Message:   entry.terminalMessage,
		Timestamp: entry.terminalTime,
	}
	entry.mu.Unlock()
	_ = r.trySend(ctx, outCh, ev)
}

// stateOrder returns the ordinal used to validate state transition direction.
// Terminal states share order 4; we never compare terminal-to-terminal.
func stateOrder(s AttemptState) int {
	switch s {
	case AttemptStateAccepted:
		return 0
	case AttemptStateSubmitted:
		return 1
	case AttemptStatePending:
		return 2
	case AttemptStateRunning:
		return 3
	default:
		return 4
	}
}
