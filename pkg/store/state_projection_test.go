package store_test

import (
	"testing"

	"github.com/HeaInSeo/spawner/pkg/api"
	"github.com/HeaInSeo/spawner/pkg/store"
)

func TestProjectState(t *testing.T) {
	cases := []struct {
		state        store.RunState
		wantEvent    api.State
		wantHasEvent bool
		wantRecover  bool
		wantTerminal bool
	}{
		{state: store.StateQueued, wantEvent: api.StateQueued, wantHasEvent: true, wantRecover: true, wantTerminal: false},
		{state: store.StateHeld, wantHasEvent: false, wantRecover: false, wantTerminal: false},
		{state: store.StateResumed, wantHasEvent: false, wantRecover: false, wantTerminal: false},
		{state: store.StateAdmittedToDag, wantEvent: api.StateStarting, wantHasEvent: true, wantRecover: true, wantTerminal: false},
		{state: store.StateRunning, wantEvent: api.StateRunning, wantHasEvent: true, wantRecover: false, wantTerminal: false},
		{state: store.StateFinished, wantEvent: api.StateSucceeded, wantHasEvent: true, wantRecover: false, wantTerminal: true},
		{state: store.StateCanceled, wantEvent: api.StateCancelled, wantHasEvent: true, wantRecover: false, wantTerminal: true},
	}

	for _, tc := range cases {
		got := store.ProjectState(tc.state)
		if got.HasPrimaryEvent != tc.wantHasEvent {
			t.Fatalf("ProjectState(%q).HasPrimaryEvent = %v, want %v", tc.state, got.HasPrimaryEvent, tc.wantHasEvent)
		}
		if got.PrimaryEvent != tc.wantEvent {
			t.Fatalf("ProjectState(%q).PrimaryEvent = %q, want %q", tc.state, got.PrimaryEvent, tc.wantEvent)
		}
		if got.Recoverable != tc.wantRecover {
			t.Fatalf("ProjectState(%q).Recoverable = %v, want %v", tc.state, got.Recoverable, tc.wantRecover)
		}
		if got.Terminal != tc.wantTerminal {
			t.Fatalf("ProjectState(%q).Terminal = %v, want %v", tc.state, got.Terminal, tc.wantTerminal)
		}
	}
}
