package dispatcher_test

import (
	"testing"
	"time"

	"github.com/HeaInSeo/spawner/pkg/api"
	"github.com/HeaInSeo/spawner/pkg/dispatcher"
)

func TestBufferedSink_TrySendDoesNotBlockWhenBufferFull(t *testing.T) {
	sink := dispatcher.NewBufferedSink(1)

	if !sink.TrySend(api.Event{RunID: "run-1"}, 0) {
		t.Fatal("expected first TrySend to succeed")
	}

	start := time.Now()
	if sink.TrySend(api.Event{RunID: "run-2"}, 20*time.Millisecond) {
		t.Fatal("expected TrySend to fail when buffer remains full")
	}
	if elapsed := time.Since(start); elapsed < 15*time.Millisecond {
		t.Fatalf("expected TrySend to wait for timeout window, elapsed=%s", elapsed)
	}
}

func TestBufferedSink_ChannelReceivesEvent(t *testing.T) {
	sink := dispatcher.NewBufferedSink(1)

	if !sink.TrySend(api.Event{RunID: "run-1"}, time.Second) {
		t.Fatal("expected TrySend to succeed")
	}

	select {
	case ev := <-sink.C():
		if ev.RunID != "run-1" {
			t.Fatalf("unexpected run id: %q", ev.RunID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for buffered sink event")
	}
}
