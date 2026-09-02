package runtime

import (
	"crypto/sha256"
	"encoding/hex"
)

// jobNameFor derives the deterministic K8s Job name for a given attempt.
// Formula: "spw-" + hex(sha256(namingSalt + attemptID))[:32]
//
// Properties:
//   - Same inputs always produce the same output (deterministic).
//   - Result is lowercase hex, compatible with K8s name constraints.
//   - 32 hex chars = 128-bit collision resistance.
func jobNameFor(namingSalt, attemptID string) string {
	h := sha256.Sum256([]byte(namingSalt + attemptID))
	return "spw-" + hex.EncodeToString(h[:])[:32]
}

// attemptMarkerFor derives the opaque ownership token for a given attempt.
// Formula: hex(sha256(namingSalt + attemptID))
//
// The marker is the full 64-char hex string. JobName uses the first 32 chars
// of the same hash, so the marker is a superset — JobName is always a prefix
// of the marker (after stripping the "spw-" prefix).
func attemptMarkerFor(namingSalt, attemptID string) string {
	h := sha256.Sum256([]byte(namingSalt + attemptID))
	return hex.EncodeToString(h[:])
}

// BackendIdentity is the deterministic, side-effect-free backend identity of an
// attempt: the (Namespace, JobName) locator plus the AttemptMarker ownership
// token. It carries no live backend state — it is purely derived from the
// Runtime's stable NamingSalt/Namespace and the AttemptID, and is the single
// source a consumer uses to perform a read-only find-by-identity: Get JobName in
// Namespace, then compare the found resource's AUTHORITATIVE full-marker
// annotation ("jumi.io/attempt-marker") against AttemptMarker for ownership
// equality. A label-safe projection (e.g. a truncated "spawner.io/attempt-marker"
// label) is never used for this equality check.
type BackendIdentity struct {
	// Namespace is the K8s namespace the attempt's Job lives in.
	Namespace string
	// JobName is the deterministic Job name: "spw-"+hex(sha256(salt+attemptID))[:32].
	JobName string
	// AttemptMarker is the full 64-char ownership token: hex(sha256(salt+attemptID)).
	// It is the authoritative ownership value, stored on the backend resource as
	// the "jumi.io/attempt-marker" annotation. A found resource whose annotation
	// marker does not equal this is NOT this attempt (CONFLICT). It is intentionally
	// 64 chars (a K8s label cannot hold it), so ownership is verified against the
	// annotation, never a truncated label.
	AttemptMarker string
}

// deriveBackendIdentity computes the BackendIdentity from the same single-source
// naming functions used by SubmitAttempt/runCreate. Pure and side-effect free.
func deriveBackendIdentity(namingSalt, namespace, attemptID string) BackendIdentity {
	return BackendIdentity{
		Namespace:     namespace,
		JobName:       jobNameFor(namingSalt, attemptID),
		AttemptMarker: attemptMarkerFor(namingSalt, attemptID),
	}
}
