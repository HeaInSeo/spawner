package imp

import (
	"log"
	"time"

	"github.com/HeaInSeo/spawner/pkg/api"
)

func emitState(s api.EventSink, spawnKey, runID string, st api.State, msg string) {
	ev := api.Event{SpawnKey: spawnKey, RunID: runID, When: time.Now(), State: st, Message: msg}
	sendWithTimeout(s, ev, 3*time.Second)
}
func emitErr(s api.EventSink, spawnKey, runID string, err error) {
	ev := api.Event{SpawnKey: spawnKey, RunID: runID, When: time.Now(), State: api.StateFailed, Message: err.Error()}
	sendWithTimeout(s, ev, 3*time.Second)
}

// sendWithTimeout requires s to implement api.TryEventSink (non-blocking send).
// Blocking EventSink implementations must wrap themselves in a TryEventSink.
func sendWithTimeout(s api.EventSink, ev api.Event, d time.Duration) {
	if s == nil {
		return
	}
	if ts, ok := s.(api.TryEventSink); ok {
		if !ts.TrySend(ev, d) {
			log.Printf("[warn] event dropped: runID=%s state=%s", ev.RunID, ev.State)
		}
		return
	}
	// Fallback for sinks that don't implement TryEventSink.
	// Send must be non-blocking; blocking sinks must implement TryEventSink.
	s.Send(ev)
}
