//go:build go1.23

package comap

import (
	"fmt"
	"sync"
	"testing"
)

func BenchmarkCoMap_Each(b *testing.B) {
	m := NewCoMap[string, int]()
	for i := 0; i < 1000; i++ {
		m.Set(fmt.Sprintf("key%d", i), ptr(i))
	}
	for i := 0; i < b.N; i++ {
		count := 0
		for range m.Each() {
			count++
		}
		_ = count
	}
}

func TestRace_CoMapConcurrentEach(t *testing.T) {
	m := NewCoMap[string, int]()
	for i := 0; i < 50; i++ {
		m.Set(fmt.Sprintf("key%d", i), ptr(i))
	}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			m.Set(fmt.Sprintf("race%d", i), ptr(i))
		}(i)
		go func() {
			defer wg.Done()
			count := 0
			for range m.Each() {
				count++
			}
		}()
	}
	wg.Wait()
}

func TestCoMap_Each(t *testing.T) {
	m := NewCoMap[string, int]()
	m.Set("a", ptr(1))
	m.Set("b", ptr(2))
	m.Set("c", ptr(3))

	count := 0
	for k, v := range m.Each() {
		if k == "" {
			t.Error("Key should not be empty")
		}
		if v == nil {
			t.Error("Value should not be nil")
		}
		count++
	}
	if count != 3 {
		t.Errorf("Expected 3 iterations, got %d", count)
	}
}

func TestCoMap_EachSnapshotIsolation(t *testing.T) {
	m := NewCoMap[string, int]()
	m.Set("x", ptr(10))

	for range m.Each() {
		m.Set("y", ptr(20))
		break
	}

	if _, ok := m.Get("y"); !ok {
		t.Error("Key 'y' should exist after modification")
	}
}
