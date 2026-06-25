package comap

import (
	"fmt"
	"sync"
	"testing"
)

func ptr(v int) *int {
	return &v
}

func TestCoMap_SetGetDelete(t *testing.T) {
	m := NewCoMap[string, int]()

	m.Set("key", ptr(42))
	v, ok := m.Get("key")
	if !ok {
		t.Error("Expected key to exist")
	}
	if v == nil || *v != 42 {
		t.Errorf("Expected value=42, got %v", v)
	}

	m.Delete("key")
	_, ok = m.Get("key")
	if ok {
		t.Error("Expected key to be deleted")
	}
}

func TestCoMap_Size(t *testing.T) {
	m := NewCoMap[string, int]()
	if m.Size() != 0 {
		t.Fatalf("Expected size=0, got %d", m.Size())
	}

	m.Set("a", ptr(1))
	m.Set("b", ptr(2))
	if m.Size() != 2 {
		t.Fatalf("Expected size=2, got %d", m.Size())
	}

	m.Delete("a")
	if m.Size() != 1 {
		t.Fatalf("Expected size=1, got %d", m.Size())
	}
}

func TestCoMap_Keys(t *testing.T) {
	m := NewCoMap[string, int]()
	m.Set("c", ptr(3))
	m.Set("a", ptr(1))
	m.Set("b", ptr(2))

	keys := m.Keys()
	if len(keys) != 3 {
		t.Fatalf("Expected 3 keys, got %d", len(keys))
	}

	sortedKeys := m.SortedKeys()
	if sortedKeys[0] != "a" || sortedKeys[1] != "b" || sortedKeys[2] != "c" {
		t.Errorf("Expected sorted keys [a b c], got %v", sortedKeys)
	}
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

func TestCoMap_Update(t *testing.T) {
	m := NewCoMap[string, int]()
	m.Set("a", ptr(1))

	m.Update(map[string]*int{"b": ptr(2), "c": ptr(3)})

	if m.Size() != 3 {
		t.Fatalf("Expected size=3, got %d", m.Size())
	}
	if v, ok := m.Get("b"); !ok || *v != 2 {
		t.Errorf("Expected b=2, got %v", v)
	}
}

func TestCoMap_IntKeys(t *testing.T) {
	m := NewCoMap[int, string]()
	m.Set(1, strPtr("one"))
	m.Set(2, strPtr("two"))
	m.Set(3, strPtr("three"))

	keys := m.SortedKeys()
	if len(keys) != 3 || keys[0] != 1 || keys[1] != 2 || keys[2] != 3 {
		t.Errorf("Expected [1 2 3], got %v", keys)
	}
}

func TestCoMap_Int64Keys(t *testing.T) {
	m := NewCoMap[int64, string]()
	m.Set(100, strPtr("a"))
	m.Set(200, strPtr("b"))

	if m.Size() != 2 {
		t.Fatalf("Expected size=2, got %d", m.Size())
	}
}

func TestCoMap_NewCoMapSize(t *testing.T) {
	m := NewCoMapSize[string, int](10)
	if m == nil {
		t.Fatal("NewCoMapSize should return non-nil")
	}
	m.Set("x", ptr(1))
	if v, ok := m.Get("x"); !ok || *v != 1 {
		t.Errorf("Expected x=1, got %v", v)
	}
}

func TestCoMap_DeleteNonExistent(t *testing.T) {
	m := NewCoMap[string, int]()
	m.Delete("nonexistent")
	if m.Size() != 0 {
		t.Fatalf("Expected size=0, got %d", m.Size())
	}
}

func TestCoMap_GetNonExistent(t *testing.T) {
	m := NewCoMap[string, int]()
	v, ok := m.Get("nonexistent")
	if ok {
		t.Error("Expected key not found")
	}
	if v != nil {
		t.Error("Expected nil value for non-existent key")
	}
}

func TestCoMap_UniqueStrings(t *testing.T) {
	result := UniqueStrings([]string{"c", "a", "b", "a", "c"})
	if len(result) != 3 {
		t.Fatalf("Expected 3 unique strings, got %d", len(result))
	}
	if result[0] != "a" || result[1] != "b" || result[2] != "c" {
		t.Errorf("Expected [a b c], got %v", result)
	}
}

// ============================================================
// 数据竞争测试
// ============================================================

func TestRace_CoMapConcurrentReadWrite(t *testing.T) {
	m := NewCoMap[int, int]()
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				m.Set(i%100, ptr(j))
			}
		}(i)
	}

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = m.Size()
				_, _ = m.Get(50)
			}
		}()
	}

	wg.Wait()
}

func TestRace_CoMapConcurrentDelete(t *testing.T) {
	m := NewCoMap[string, int]()
	for i := 0; i < 100; i++ {
		m.Set(fmt.Sprintf("key%d", i), ptr(i))
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			m.Delete(fmt.Sprintf("key%d", i))
		}(i)
		go func(i int) {
			defer wg.Done()
			_, _ = m.Get(fmt.Sprintf("key%d", i))
		}(i)
	}
	wg.Wait()
}

func TestRace_CoMapConcurrentKeys(t *testing.T) {
	m := NewCoMap[string, int]()
	for i := 0; i < 50; i++ {
		m.Set(fmt.Sprintf("key%d", i), ptr(i))
	}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			m.Set(fmt.Sprintf("new%d", i), ptr(i))
		}(i)
		go func() {
			defer wg.Done()
			_ = m.Keys()
			_ = m.SortedKeys()
		}()
	}
	wg.Wait()
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

func TestRace_CoMapConcurrentUpdate(t *testing.T) {
	m := NewCoMap[string, int]()
	for i := 0; i < 20; i++ {
		m.Set(fmt.Sprintf("key%d", i), ptr(i))
	}

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			data := map[string]*int{fmt.Sprintf("upd%d", i): ptr(i * 10)}
			m.Update(data)
		}(i)
		go func() {
			defer wg.Done()
			_ = m.Size()
		}()
	}
	wg.Wait()
}

func TestRace_CoMapMixedOperations(t *testing.T) {
	m := NewCoMap[string, int]()
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m.Set(fmt.Sprintf("key%d", i%20), ptr(i))
		}(i)
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m.Delete(fmt.Sprintf("key%d", i%20))
		}(i)
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _ = m.Get(fmt.Sprintf("key%d", i%20))
		}(i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = m.Keys()
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = m.Size()
		}()
	}
	wg.Wait()
}

func TestRace_CoMapIntKeys(t *testing.T) {
	m := NewCoMap[int, int]()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m.Set(i%50, ptr(i))
		}(i)
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _ = m.Get(i % 50)
		}(i)
	}
	wg.Wait()
}

func TestRace_CoMapInt64Keys(t *testing.T) {
	m := NewCoMap[int64, int]()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m.Set(int64(i%50), ptr(i))
		}(i)
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _ = m.Get(int64(i % 50))
		}(i)
	}
	wg.Wait()
}

// ============================================================
// Benchmark
// ============================================================

func BenchmarkCoMap_Set(b *testing.B) {
	m := NewCoMap[string, int]()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Set(fmt.Sprintf("key%d", i%100), ptr(i))
			i++
		}
	})
}

func BenchmarkCoMap_Get(b *testing.B) {
	m := NewCoMap[string, int]()
	for i := 0; i < 1000; i++ {
		m.Set(fmt.Sprintf("key%d", i), ptr(i))
	}
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.Get(fmt.Sprintf("key%d", i%1000))
			i++
		}
	})
}

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

func strPtr(v string) *string {
	return &v
}
