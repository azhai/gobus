package comap

import "maps"

// ForEach walks every key-value pair on a snapshot (the Go 1.22-friendly face
// of Each). Returning false from fn stops the walk.
func (m *CoMap[K, V]) ForEach(fn func(K, *V) bool) {
	m.mu.RLock()
	snapshot := maps.Clone(m.data)
	m.mu.RUnlock()
	for k, v := range snapshot {
		if !fn(k, v) {
			return
		}
	}
}
