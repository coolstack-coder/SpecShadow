package raft

import "sync"

func dropThrough(ents []Entry, idx uint64) []Entry {
	i := 0
	for i < len(ents) && ents[i].Index <= idx {
		i++
	}
	return ents[i:]
}

func truncateFrom(ents []Entry, idx uint64) []Entry {
	for i, e := range ents {
		if e.Index >= idx {
			return ents[:i]
		}
	}
	return ents
}

// MemoryStorage is used by tests. Reusing the same instance across a
// simulated restart behaves like a disk that survived the crash.
type MemoryStorage struct {
	mu   sync.Mutex
	hs   HardState
	meta SnapshotMeta
	snap []byte
	ents []Entry
}

func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }

func (m *MemoryStorage) Load() (HardState, SnapshotMeta, []byte, []Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hs, m.meta, append([]byte(nil), m.snap...), append([]Entry(nil), m.ents...), nil
}

func (m *MemoryStorage) Save(hs *HardState, ents []Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if hs != nil {
		m.hs = *hs
	}
	if len(ents) > 0 {
		m.ents = truncateFrom(m.ents, ents[0].Index)
		m.ents = append(m.ents, ents...)
	}
	return nil
}

func (m *MemoryStorage) SaveSnapshot(meta SnapshotMeta, data []byte, hs HardState, retained []Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.meta = meta
	m.snap = append([]byte(nil), data...)
	m.hs = hs
	m.ents = append([]Entry(nil), retained...)
	return nil
}

func (m *MemoryStorage) Close() error { return nil }
