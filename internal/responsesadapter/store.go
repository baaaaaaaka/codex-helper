package responsesadapter

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrResponseNotFound = errors.New("response not found")
	ErrScopeMismatch    = errors.New("response scope mismatch")
	ErrActiveTurn       = errors.New("scope already has an active turn")
)

type MemoryStore struct {
	mu             sync.Mutex
	records        map[string]storedResponse
	active         map[string]string
	reasoning      map[string]storedReasoning
	reasoningOrder []string
	now            func() time.Time
	ttl            time.Duration
	max            int
}

type ResponseStore interface {
	BeginTurn(scope Scope, turnID string) (func(), error)
	Store(record ResponseRecord) error
	Get(id string, scope Scope) (ResponseRecord, error)
	ResolveChain(previousResponseID string, scope Scope) ([]ResponseRecord, error)
}

type storedResponse struct {
	ResponseRecord
	storedAt time.Time
}

type storedReasoning struct {
	text     string
	storedAt time.Time
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		records:   make(map[string]storedResponse),
		active:    make(map[string]string),
		reasoning: make(map[string]storedReasoning),
		now:       time.Now,
		ttl:       time.Hour,
		max:       500,
	}
}

func (s *MemoryStore) BeginTurn(scope Scope, turnID string) (func(), error) {
	scope = scope.withDefaults()
	key := scope.key()
	s.mu.Lock()
	defer s.mu.Unlock()
	if owner := s.active[key]; owner != "" {
		return nil, fmt.Errorf("%w: %s", ErrActiveTurn, key)
	}
	s.active[key] = turnID
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.active[key] == turnID {
			delete(s.active, key)
		}
	}, nil
}

func (s *MemoryStore) Store(record ResponseRecord) error {
	record.Scope = record.Scope.withDefaults()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evictLocked()
	s.records[record.ID] = storedResponse{
		ResponseRecord: record,
		storedAt:       s.now(),
	}
	s.rememberReasoningLocked(record)
	return nil
}

func (s *MemoryStore) Get(id string, scope Scope) (ResponseRecord, error) {
	scope = scope.withDefaults()
	s.mu.Lock()
	defer s.mu.Unlock()
	stored, ok := s.records[id]
	if !ok {
		return ResponseRecord{}, ErrResponseNotFound
	}
	if stored.Scope.key() != scope.key() {
		return ResponseRecord{}, ErrScopeMismatch
	}
	return stored.ResponseRecord, nil
}

func (s *MemoryStore) ResolveChain(previousResponseID string, scope Scope) ([]ResponseRecord, error) {
	scope = scope.withDefaults()
	var reversed []ResponseRecord
	seen := map[string]bool{}
	current := previousResponseID
	for current != "" {
		if seen[current] {
			break
		}
		seen[current] = true
		record, err := s.Get(current, scope)
		if err != nil {
			return nil, err
		}
		reversed = append(reversed, record)
		current = record.PreviousResponseID
	}
	for i, j := 0, len(reversed)-1; i < j; i, j = i+1, j-1 {
		reversed[i], reversed[j] = reversed[j], reversed[i]
	}
	return reversed, nil
}

func (s *MemoryStore) LookupReasoning(scope Scope, message ProviderMessage) (string, error) {
	keys := reasoningCacheKeys(scope, message)
	if len(keys) == 0 {
		return "", nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, key := range keys {
		if stored, ok := s.reasoning[key]; ok {
			return stored.text, nil
		}
	}
	return "", nil
}

func (s *MemoryStore) rememberReasoningLocked(record ResponseRecord) {
	if record.ReasoningText == "" {
		return
	}
	now := s.now()
	for _, key := range reasoningKeysForRecord(record.Scope, record) {
		if _, exists := s.reasoning[key]; !exists {
			s.reasoningOrder = append(s.reasoningOrder, key)
		}
		s.reasoning[key] = storedReasoning{text: record.ReasoningText, storedAt: now}
	}
	for s.max > 0 && len(s.reasoningOrder) > s.max*4 {
		oldest := s.reasoningOrder[0]
		s.reasoningOrder = s.reasoningOrder[1:]
		delete(s.reasoning, oldest)
	}
}

func (s *MemoryStore) evictLocked() {
	if s.max <= 0 || len(s.records) < s.max {
		return
	}
	now := s.now()
	for id, record := range s.records {
		if s.ttl > 0 && now.Sub(record.storedAt) > s.ttl {
			delete(s.records, id)
		}
	}
	if s.ttl > 0 {
		kept := s.reasoningOrder[:0]
		for _, key := range s.reasoningOrder {
			record, ok := s.reasoning[key]
			if !ok {
				continue
			}
			if now.Sub(record.storedAt) > s.ttl {
				delete(s.reasoning, key)
				continue
			}
			kept = append(kept, key)
		}
		s.reasoningOrder = kept
	}
	if len(s.records) < s.max {
		return
	}
	var oldestID string
	var oldest time.Time
	for id, record := range s.records {
		if oldestID == "" || record.storedAt.Before(oldest) {
			oldestID = id
			oldest = record.storedAt
		}
	}
	if oldestID != "" {
		delete(s.records, oldestID)
	}
}
