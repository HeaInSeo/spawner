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
