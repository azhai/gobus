//go:build go1.23

package comap

import (
	"iter"
	"maps"
)

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
