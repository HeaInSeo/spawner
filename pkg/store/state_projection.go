package store

import "github.com/HeaInSeo/spawner/pkg/api"

// StateProjection describes how a persisted RunState should be interpreted
// at the operator-facing event layer.
//
// Persisted states and event states are intentionally not identical:
// persisted states carry durable ingress/recovery meaning, while event states
// carry transient execution observation meaning.
type StateProjection struct {
	RunState        RunState
	PrimaryEvent    api.State
	HasPrimaryEvent bool
	Recoverable     bool
	Terminal        bool
}

// ProjectState returns the canonical operator-facing interpretation of a
// persisted run state.
//
// Not every persisted state has a direct event equivalent:
// held/resumed are durable control states rather than emitted execution events.
func ProjectState(s RunState) StateProjection {
	p := StateProjection{
		RunState:    s,
		Recoverable: IsRecoverable(s),
		Terminal:    IsTerminal(s),
	}

	switch s {
	case StateQueued:
		p.PrimaryEvent = api.StateQueued
		p.HasPrimaryEvent = true
	case StateAdmittedToDag:
		p.PrimaryEvent = api.StateStarting
		p.HasPrimaryEvent = true
	case StateRunning:
		p.PrimaryEvent = api.StateRunning
		p.HasPrimaryEvent = true
	case StateFinished:
		p.PrimaryEvent = api.StateSucceeded
		p.HasPrimaryEvent = true
	case StateCanceled:
		p.PrimaryEvent = api.StateCancelled
		p.HasPrimaryEvent = true
	}

	return p
}
