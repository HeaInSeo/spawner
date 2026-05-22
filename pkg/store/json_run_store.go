package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// JsonRunStore is a durable-lite RunStore backed by a single JSON file.
//
// Writes are atomic via tmp-file + rename: a crash during write never leaves
// a partial/corrupt file. Either the old version remains or the new one is complete.
//
// LIMITATION: atomic only on POSIX same-filesystem renames.
// ASSUMPTION: production replaces this with PostgreSQL or equivalent.
type JsonRunStore struct {
	mu       sync.Mutex
	path     string
	records  map[string]RunRecord
	attempts map[string][]AttemptRecord
}

var _ RunStore = (*JsonRunStore)(nil)

func NewJsonRunStore(path string) (*JsonRunStore, error) {
	s := &JsonRunStore{
		path:     path,
		records:  make(map[string]RunRecord),
		attempts: make(map[string][]AttemptRecord),
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

type jsonRunStoreFile struct {
	Records  map[string]RunRecord       `json:"records"`
	Attempts map[string][]AttemptRecord `json:"attempts"`
}

func (s *JsonRunStore) load() error {
	data, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("json store load %s: %w", s.path, err)
	}
	if len(data) == 0 {
		return nil
	}
	var file jsonRunStoreFile
	if err := json.Unmarshal(data, &file); err == nil && file.Records != nil {
		s.records = file.Records
		if file.Attempts != nil {
			s.attempts = file.Attempts
		}
		return nil
	}
	// backward compatibility with older map-only files
	if err := json.Unmarshal(data, &s.records); err != nil {
		return err
	}
	for runID, rec := range s.records {
		if rec.LatestAttemptID == "" {
			rec.LatestAttemptID = runID + "/attempt-1"
			s.records[runID] = rec
		}
		s.attempts[runID] = []AttemptRecord{{
			AttemptID: rec.LatestAttemptID,
			RunID:     rec.RunID,
			State:     rec.State,
			Payload:   rec.Payload,
			Cause:     AttemptCauseInitialSubmit,
			CreatedAt: rec.CreatedAt,
			UpdatedAt: rec.UpdatedAt,
		}}
	}
	return nil
}

// persist writes records atomically via tmp-file + rename (POSIX atomic).
func (s *JsonRunStore) persist() error {
	data, err := json.Marshal(jsonRunStoreFile{
		Records:  s.records,
		Attempts: s.attempts,
	})
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".runstore-*.tmp")
	if err != nil {
		return fmt.Errorf("persist tmp create: %w", err)
	}
	tmpPath := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("persist tmp write: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("persist tmp close: %w", err)
	}
	if err := os.Rename(tmpPath, s.path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("persist rename: %w", err)
	}
	return nil
}

func (s *JsonRunStore) Enqueue(_ context.Context, rec RunRecord) error {
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
	return s.persist()
}

func (s *JsonRunStore) Get(_ context.Context, runID string) (RunRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[runID]
	return r, ok, nil
}

func (s *JsonRunStore) UpdateState(_ context.Context, runID string, from, to RunState) error {
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
	return s.persist()
}

func (s *JsonRunStore) ListByState(_ context.Context, state RunState) ([]RunRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []RunRecord
	for _, r := range s.records {
		if r.State == state {
			out = append(out, r)
		}
	}
	return out, nil
}

func (s *JsonRunStore) Delete(_ context.Context, runID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.records[runID]; !ok {
		return ErrNotFound
	}
	delete(s.records, runID)
	delete(s.attempts, runID)
	return s.persist()
}

func (s *JsonRunStore) AppendAttempt(_ context.Context, attempt AttemptRecord) error {
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
	return s.persist()
}

func (s *JsonRunStore) ListAttempts(_ context.Context, runID string) ([]AttemptRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	atts, ok := s.attempts[runID]
	if !ok {
		return nil, ErrNotFound
	}
	out := make([]AttemptRecord, len(atts))
	copy(out, atts)
	return out, nil
}

func (s *JsonRunStore) GetLatestAttempt(_ context.Context, runID string) (AttemptRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	atts, ok := s.attempts[runID]
	if !ok || len(atts) == 0 {
		return AttemptRecord{}, false, nil
	}
	return atts[len(atts)-1], true, nil
}
