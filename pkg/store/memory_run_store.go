package store

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// InMemoryRunStore holds all records in a Go map.
// Fast and simple but loses all data when the process restarts.
type InMemoryRunStore struct {
	mu       sync.RWMutex
	records  map[string]RunRecord
	attempts map[string][]AttemptRecord
}

var _ RunStore = (*InMemoryRunStore)(nil)

func NewInMemoryRunStore() *InMemoryRunStore {
	return &InMemoryRunStore{
		records:  make(map[string]RunRecord),
		attempts: make(map[string][]AttemptRecord),
	}
}

func (s *InMemoryRunStore) Enqueue(_ context.Context, rec RunRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.records[rec.RunID]; ok {
		return ErrAlreadyExists
	}
	now := time.Now()
	rec.CreatedAt = now
	rec.UpdatedAt = now
	if rec.LatestAttemptID == "" {
		rec.LatestAttemptID = rec.RunID + "/attempt-1"
	}
	s.records[rec.RunID] = rec
	s.attempts[rec.RunID] = append(s.attempts[rec.RunID], AttemptRecord{
		AttemptID: rec.LatestAttemptID,
		RunID:     rec.RunID,
		State:     rec.State,
		Payload:   rec.Payload,
		Cause:     AttemptCauseInitialSubmit,
		CreatedAt: now,
		UpdatedAt: now,
	})
	return nil
}

func (s *InMemoryRunStore) Get(_ context.Context, runID string) (RunRecord, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.records[runID]
	return r, ok, nil
}

func (s *InMemoryRunStore) UpdateState(_ context.Context, runID string, from, to RunState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[runID]
	if !ok {
		return ErrNotFound
	}
	if r.State != from {
		return fmt.Errorf("state mismatch: expected %s, got %s", from, r.State)
	}
	if err := ValidateTransition(from, to); err != nil {
		return err
	}
	r.State = to
	r.UpdatedAt = time.Now()
	s.records[runID] = r
	if atts := s.attempts[runID]; len(atts) > 0 {
		atts[len(atts)-1].State = to
		atts[len(atts)-1].UpdatedAt = r.UpdatedAt
		s.attempts[runID] = atts
	}
	return nil
}

func (s *InMemoryRunStore) ListByState(_ context.Context, state RunState) ([]RunRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []RunRecord
	for _, r := range s.records {
		if r.State == state {
			out = append(out, r)
		}
	}
	return out, nil
}

func (s *InMemoryRunStore) Delete(_ context.Context, runID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.records[runID]; !ok {
		return ErrNotFound
	}
	delete(s.records, runID)
	delete(s.attempts, runID)
	return nil
}

func (s *InMemoryRunStore) AppendAttempt(_ context.Context, attempt AttemptRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[attempt.RunID]
	if !ok {
		return ErrNotFound
	}
	if err := ValidateAttempt(attempt); err != nil {
		return err
	}
	// AppendAttempt is only for new attempts; the attempt must start in StateQueued.
	if attempt.State != StateQueued {
		return fmt.Errorf("%w: AppendAttempt only accepts StateQueued; got %s", ErrInvalidAttempt, attempt.State)
	}
	if IsTerminal(r.State) {
		return fmt.Errorf("%w: cannot append attempt to run in terminal state %s", ErrInvalidAttempt, r.State)
	}
	now := time.Now()
	attempt.CreatedAt = now
	attempt.UpdatedAt = now
	s.attempts[attempt.RunID] = append(s.attempts[attempt.RunID], attempt)
	r.LatestAttemptID = attempt.AttemptID
	r.State = attempt.State
	r.Payload = attempt.Payload
	r.UpdatedAt = now
	s.records[attempt.RunID] = r
	return nil
}

func (s *InMemoryRunStore) ListAttempts(_ context.Context, runID string) ([]AttemptRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	atts, ok := s.attempts[runID]
	if !ok {
		return nil, ErrNotFound
	}
	out := make([]AttemptRecord, len(atts))
	copy(out, atts)
	return out, nil
}

func (s *InMemoryRunStore) GetLatestAttempt(_ context.Context, runID string) (AttemptRecord, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	atts, ok := s.attempts[runID]
	if !ok || len(atts) == 0 {
		return AttemptRecord{}, false, nil
	}
	return atts[len(atts)-1], true, nil
}
