package runtime

import (
	"fmt"
	"time"
)

// RuntimeConfig holds configuration for a Runtime instance.
// All fields affecting job naming (NamingSalt, Namespace) must be stable
// across process restarts. Changing them orphans existing K8s jobs.
type RuntimeConfig struct {
	// RuntimeID identifies this Runtime instance.
	// Reserved for future multi-Runtime routing. May be empty.
	RuntimeID string

	// NamingSalt is the stable secret used to derive JobName and AttemptMarker.
	// Required. Must be stable across process restarts — never generated randomly
	// per process start. A salt change makes existing K8s jobs unrecoverable.
	NamingSalt string

	// Namespace is the K8s namespace for all jobs submitted by this Runtime.
	// v0: fixed per Runtime instance. v1: NamespaceResolver interface (future).
	// Must be stable across process restarts for the same NamingSalt.
	Namespace string

	// MaxConcurrency is the maximum number of simultaneous jobs in flight.
	// 0 = use default (10).
	MaxConcurrency int

	// SubmitTimeout is how long SubmitAttempt waits after the actor accepts
	// the request before returning ErrSubmitOutcomeUnknown.
	// 0 = use default (30s).
	SubmitTimeout time.Duration

	// CreateTimeout is the context deadline for JobClient.Create API calls.
	// 0 = use default (20s).
	CreateTimeout time.Duration

	// DeleteTimeout is the context deadline for JobClient.Delete API calls.
	// 0 = use default (15s).
	DeleteTimeout time.Duration
}

func (c *RuntimeConfig) applyDefaults() {
	if c.MaxConcurrency <= 0 {
		c.MaxConcurrency = 10
	}
	if c.SubmitTimeout <= 0 {
		c.SubmitTimeout = 30 * time.Second
	}
	if c.CreateTimeout <= 0 {
		c.CreateTimeout = 20 * time.Second
	}
	if c.DeleteTimeout <= 0 {
		c.DeleteTimeout = 15 * time.Second
	}
}

func (c RuntimeConfig) validate() error {
	if c.NamingSalt == "" {
		return fmt.Errorf("RuntimeConfig.NamingSalt is required")
	}
	if c.Namespace == "" {
		return fmt.Errorf("RuntimeConfig.Namespace is required")
	}
	return nil
}
