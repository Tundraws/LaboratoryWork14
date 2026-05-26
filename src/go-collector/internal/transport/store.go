package transport

import (
	"sync"

	"github.com/tundraws/laboratorywork14/src/go-collector/internal/model"
)

type AggregateStore struct {
	mu      sync.RWMutex
	records []model.WindowAggregate
	limit   int
}

func NewAggregateStore(limit int) *AggregateStore {
	return &AggregateStore{limit: limit}
}

func (s *AggregateStore) Add(batch []model.WindowAggregate) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, batch...)
	if len(s.records) > s.limit {
		s.records = s.records[len(s.records)-s.limit:]
	}
}

func (s *AggregateStore) Snapshot() []model.WindowAggregate {
	s.mu.RLock()
	defer s.mu.RUnlock()
	copyRecords := make([]model.WindowAggregate, len(s.records))
	copy(copyRecords, s.records)
	return copyRecords
}
