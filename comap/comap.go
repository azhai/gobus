package comap

import (
	"iter"
	"maps"
	"slices"
	"sync"
)

// UniqueStrings sort and unique strings in the slice.
func UniqueStrings(words []string) []string {
	slices.Sort(words)
	return slices.Compact(words)
}

// CoMap is a thread-safe concurrent map with read-write locking
// It supports generic key types (int, int64, string) and any value type

type CoMap[K int | int64 | string, V any] struct {
	mu   sync.RWMutex // Read-write mutex for thread safety
	data map[K]*V     // Underlying map storage
}

// NewCoMap creates a new thread-safe concurrent map
// It initializes the map with the specified key and value types
func NewCoMap[K int | int64 | string, V any]() *CoMap[K, V] {
	return NewCoMapSize[K, V](0)
}

// NewCoMapSize creates a new thread-safe concurrent map
// It initializes the map with the specified key and value types
func NewCoMapSize[K int | int64 | string, V any](capacity int) *CoMap[K, V] {
	return &CoMap[K, V]{
		data: make(map[K]*V, capacity),
	}
}

// Size returns the number of elements in the map
func (m *CoMap[K, V]) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.data)
}

// Get retrieves a value from the map by key
// It returns the value and a boolean indicating if the key was found
func (m *CoMap[K, V]) Get(key K) (*V, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.data[key]
	return v, ok
}

// Set adds or updates a value in the map
func (m *CoMap[K, V]) Set(key K, value *V) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = value
}

// Update updates the map with multiple key-value pairs
// It copies all entries from the provided map to the current map
func (m *CoMap[K, V]) Update(data map[K]*V) {
	m.mu.Lock()
	defer m.mu.Unlock()
	maps.Copy(m.data, data)
}

// Delete removes a value from the map by key
// It uses a write lock for thread safety
func (m *CoMap[K, V]) Delete(key K) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
}

// Keys returns all keys in the map as a slice
func (m *CoMap[K, V]) Keys() []K {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keys := make([]K, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	return keys
}

// SortedKeys returns sorted keys from the map
func (m *CoMap[K, V]) SortedKeys() []K {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return slices.Sorted(maps.Keys(m.data))
}

// Each returns an iterator over all key-value pairs in the map.
// It takes a snapshot of the map under a read lock, then iterates over the snapshot.
// Note: the snapshot is shallow — pointer values (*V) are shared with the original map.
// Callers should not mutate the pointed-to values during iteration if concurrent writes are possible.
func (m *CoMap[K, V]) Each() iter.Seq2[K, *V] {
	m.mu.RLock()
	snapshot := maps.Clone(m.data)
	m.mu.RUnlock()
	return func(yield func(K, *V) bool) {
		for k, v := range snapshot {
			if !yield(k, v) {
				return
			}
		}
	}
}
