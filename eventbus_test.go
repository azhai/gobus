package gobus

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ============================================================
// Publish 校验
// ============================================================

func TestPublish_Validation(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	_, err := eb.Publish("", map[string]any{"k": "v"}, 1, false)
	if err == nil {
		t.Fatal("期望返回错误：topic为空")
	}

	_, err = eb.Publish("topic", map[string]any{}, 1, false)
	if err == nil {
		t.Fatal("期望返回错误：data为空")
	}

	_, err = eb.Publish("topic", map[string]any{"k": "v"}, 0, false)
	if err == nil {
		t.Fatal("期望返回错误：priority越界(0)")
	}

	_, err = eb.Publish("topic", map[string]any{"k": "v"}, 6, false)
	if err == nil {
		t.Fatal("期望返回错误：priority越界(6)")
	}

	_, err = eb.Publish("topic", map[string]any{"k": "v"}, -1, false)
	if err == nil {
		t.Fatal("期望返回错误：priority越界(-1)")
	}
}

func TestPublish_PriorityBoundary(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	for _, p := range []int{1, 2, 3, 4, 5} {
		eb.Subscribe("t", Fanout, "s1", func(e *Event) {})
		_, err := eb.Publish("t", map[string]any{"k": "v"}, p, false)
		if err != nil {
			t.Fatalf("priority=%d 应合法: %v", p, err)
		}
	}
}

func TestPublish_DefaultNeedAck(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	eb.Subscribe("t", Fanout, "s1", func(e *Event) {})

	event, _ := eb.Publish("t", map[string]any{"k": "v"}, 1, false)
	if event.NeedAck != false {
		t.Fatal("needAck 默认应为 false")
	}
}

func TestPublish_CreatedTimestamp(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	eb.Subscribe("t", Fanout, "s1", func(e *Event) {})

	before := time.Now().Unix()
	event, _ := eb.Publish("t", map[string]any{"k": "v"}, 1, false)
	after := time.Now().Unix()

	if event.Created < before || event.Created > after {
		t.Fatalf("created 时间戳应在当前时间范围内, before=%d, created=%d, after=%d", before, event.Created, after)
	}
}

func TestPublish_InitialStatus(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	eb.Subscribe("t", Fanout, "s1", func(e *Event) {})

	// needAck=false 时，分发后自动完成，状态为 completed
	event, _ := eb.Publish("t", map[string]any{"k": "v"}, 1, false)
	if event.Status != StatusCompleted {
		t.Fatalf("needAck=false 时状态应为 completed, 实际=%s", event.Status)
	}
}

func TestPublish_IncrementalID(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	eb.Subscribe("t", Fanout, "s1", func(e *Event) {})

	e1, _ := eb.Publish("t", map[string]any{"k": "1"}, 1, false)
	e2, _ := eb.Publish("t", map[string]any{"k": "2"}, 1, false)
	e3, _ := eb.Publish("t", map[string]any{"k": "3"}, 1, false)

	if e1.ID == e2.ID || e2.ID == e3.ID || e1.ID == e3.ID {
		t.Fatal("每条消息的ID应唯一")
	}
}

func TestPublish_DataVariousTypes(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	eb.Subscribe("t", Fanout, "s1", func(e *Event) {})

	data := map[string]any{
		"stringVal": "hello",
		"intVal":    42,
		"floatVal":  3.14,
		"nilVal":    nil,
		"boolVal":   true,
	}
	event, err := eb.Publish("t", data, 1, false)
	if err != nil {
		t.Fatalf("发布含多种类型data的消息失败: %v", err)
	}
	if len(event.Data) != 5 {
		t.Fatalf("期望5个data字段，实际=%d", len(event.Data))
	}
}

// ============================================================
// 无订阅者 = no-op
// ============================================================

func TestPublish_NoSubscriber_NoOp(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	event, err := eb.Publish("db:create", map[string]any{"k": "v"}, 1, false)
	if err != nil {
		t.Fatalf("不应返回错误: %v", err)
	}
	if event != nil {
		t.Fatal("无订阅者时应返回nil (no-op)")
	}

	// 不应存储任何数据
	eb.mu.RLock()
	pendingLen := len(eb.pendingQueue["db:create"])
	topicsLen := len(eb.topics)
	dataKeysLen := len(eb.topicDataKeys["db:create"])
	eb.mu.RUnlock()

	if pendingLen != 0 {
		t.Fatalf("无订阅者时pending队列应为空，实际=%d", pendingLen)
	}
	if topicsLen != 0 {
		t.Fatalf("无订阅者时topics不应注册，实际=%d", topicsLen)
	}
	if dataKeysLen != 0 {
		t.Fatalf("无订阅者时topicDataKeys不应记录，实际=%d", dataKeysLen)
	}
}

func TestPublish_NoSubscriber_MultipleTimes(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	for i := 0; i < 10; i++ {
		event, err := eb.Publish("cache:set", map[string]any{"k": i}, 1, false)
		if err != nil {
			t.Fatalf("第%d次发布失败: %v", i, err)
		}
		if event != nil {
			t.Fatalf("第%d次无订阅者应返回nil", i)
		}
	}

	// events sync.Map 也应为空
	count := 0
	eb.events.Range(func(key, value any) bool {
		count++
		return true
	})
	if count != 0 {
		t.Fatalf("无订阅者时events索引应为空，实际=%d", count)
	}
}

// ============================================================
// Fanout 模式
// ============================================================

func TestFanout_AllSubscribers(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var count atomic.Int32
	handler := func(e *Event) { count.Add(1) }

	eb.Subscribe("order:created", Fanout, "sub1", handler)
	eb.Subscribe("order:created", Fanout, "sub2", handler)
	eb.Subscribe("order:created", Fanout, "sub3", handler)

	_, err := eb.Publish("order:created", map[string]any{"orderId": "1"}, 1, false)
	if err != nil {
		t.Fatalf("发布失败: %v", err)
	}

	if count.Load() != 3 {
		t.Fatalf("Fanout模式期望3个订阅者都收到，实际=%d", count.Load())
	}
}

func TestFanout_NeedAckFalse_AutoRemoveFromPending(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	handler := func(e *Event) {}
	eb.Subscribe("order:created", Fanout, "sub1", handler)

	event, _ := eb.Publish("order:created", map[string]any{"k": "v"}, 1, false)

	key := "order:created"
	eb.mu.RLock()
	pending := eb.pendingQueue[key]
	eb.mu.RUnlock()
	for _, e := range pending {
		if e.ID == event.ID {
			t.Fatal("Fanout+无ACK消息应从待分发队列移除")
		}
	}
}

func TestFanout_NeedAckTrue_StaysInPending(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	handler := func(e *Event) {}
	eb.Subscribe("order:created", Fanout, "sub1", handler)

	event, _ := eb.Publish("order:created", map[string]any{"k": "v"}, 1, true)

	key := "order:created"
	eb.mu.RLock()
	pending := eb.pendingQueue[key]
	eb.mu.RUnlock()
	found := false
	for _, e := range pending {
		if e.ID == event.ID {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("Fanout+needAck=true消息应保留在待分发队列")
	}

	if event.Status != StatusDelivered {
		t.Fatalf("期望status=delivered, 实际=%s", event.Status)
	}
}

func TestFanout_NeedAckTrue_ACKRemovesFromPending(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	handler := func(e *Event) {}
	eb.Subscribe("order:created", Fanout, "sub1", handler)

	event, _ := eb.Publish("order:created", map[string]any{"k": "v"}, 1, true)

	eb.Ack(event.ID)

	key := "order:created"
	eb.mu.RLock()
	pending := eb.pendingQueue[key]
	eb.mu.RUnlock()
	for _, e := range pending {
		if e.ID == event.ID {
			t.Fatal("ACK后消息应从待分发队列移除")
		}
	}
}

func TestFanout_EachSubscriberGetsSameEvent(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var mu sync.Mutex
	received := make(map[string]string)

	handler := func(sid string) HandlerFunc {
		return func(e *Event) {
			mu.Lock()
			received[sid] = e.ID
			mu.Unlock()
		}
	}

	eb.Subscribe("g:t", Fanout, "s1", handler("s1"))
	eb.Subscribe("g:t", Fanout, "s2", handler("s2"))

	event, _ := eb.Publish("g:t", map[string]any{"k": "v"}, 1, false)

	if received["s1"] != event.ID || received["s2"] != event.ID {
		t.Fatalf("每个订阅者应收到同一条消息, got: %v", received)
	}
}

// ============================================================
// Anyone 模式
// ============================================================

func TestAnyone_OneSubscriber(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var count atomic.Int32
	handler := func(e *Event) { count.Add(1) }

	eb.Subscribe("order:created", Anyone, "sub1", handler)
	eb.Subscribe("order:created", Anyone, "sub2", handler)
	eb.Subscribe("order:created", Anyone, "sub3", handler)

	_, err := eb.Publish("order:created", map[string]any{"orderId": "1"}, 1, false)
	if err != nil {
		t.Fatalf("发布失败: %v", err)
	}

	if count.Load() != 1 {
		t.Fatalf("Anyone模式期望仅1个订阅者收到，实际=%d", count.Load())
	}
}

func TestAnyone_NoAck_AutoComplete(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var received atomic.Pointer[Event]
	handler := func(e *Event) { received.Store(e) }

	eb.Subscribe("order:created", Anyone, "sub1", handler)

	event, _ := eb.Publish("order:created", map[string]any{"orderId": "1"}, 1, false)

	r := received.Load()
	if r == nil {
		t.Fatal("未收到消息")
	}
	if r.Status != StatusCompleted {
		t.Fatalf("Anyone+无ACK期望status=completed, 实际=%s", r.Status)
	}

	key := "order:created"
	eb.mu.RLock()
	pending := eb.pendingQueue[key]
	eb.mu.RUnlock()
	for _, e := range pending {
		if e.ID == event.ID {
			t.Fatal("Anyone+无ACK消息应从待分发队列移除")
		}
	}
}

func TestAnyone_NeedAckTrue_StatusDelivered(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var received atomic.Pointer[Event]
	handler := func(e *Event) { received.Store(e) }

	eb.Subscribe("order:created", Anyone, "sub1", handler)

	event, _ := eb.Publish("order:created", map[string]any{"orderId": "1"}, 1, true)

	r := received.Load()
	if r == nil {
		t.Fatal("未收到消息")
	}
	if r.Status != StatusDelivered {
		t.Fatalf("Anyone+needAck=true 期望status=delivered, 实际=%s", r.Status)
	}

	eb.Ack(event.ID)
	if event.Status != StatusCompleted {
		t.Fatalf("ACK后期望status=completed, 实际=%s", event.Status)
	}
}

func TestAnyone_NeedAckTrue_StaysInPendingUntilAck(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	handler := func(e *Event) {}
	eb.Subscribe("order:created", Anyone, "sub1", handler)

	event, _ := eb.Publish("order:created", map[string]any{"k": "v"}, 1, true)

	key := "order:created"
	eb.mu.RLock()
	pending := eb.pendingQueue[key]
	eb.mu.RUnlock()
	found := false
	for _, e := range pending {
		if e.ID == event.ID {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("Anyone+needAck=true消息在ACK前应保留在待分发队列")
	}

	eb.Ack(event.ID)
	eb.mu.RLock()
	pending = eb.pendingQueue[key]
	eb.mu.RUnlock()
	for _, e := range pending {
		if e.ID == event.ID {
			t.Fatal("ACK后消息应从待分发队列移除")
		}
	}
}

// ============================================================
// ACK 回执
// ============================================================

func TestAck_NeedAck(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var ackEvent atomic.Pointer[Event]
	eb.SetAckCallback(func(e *Event) { ackEvent.Store(e) })

	var received atomic.Pointer[Event]
	handler := func(e *Event) { received.Store(e) }

	eb.Subscribe("order:created", Fanout, "sub1", handler)

	event, _ := eb.Publish("order:created", map[string]any{"orderId": "1"}, 1, true)

	r := received.Load()
	if r.Status != StatusDelivered {
		t.Fatalf("期望status=delivered, 实际=%s", r.Status)
	}

	err := eb.Ack(event.ID)
	if err != nil {
		t.Fatalf("ACK失败: %v", err)
	}

	if event.Status != StatusCompleted {
		t.Fatalf("期望status=completed, 实际=%s", event.Status)
	}

	if ackEvent.Load() == nil {
		t.Fatal("ACK回调未被触发")
	}
}

func TestAck_CallbackReceivesCorrectEvent(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var acked atomic.Pointer[Event]
	eb.SetAckCallback(func(e *Event) { acked.Store(e) })

	handler := func(e *Event) {}
	eb.Subscribe("g:t", Fanout, "s1", handler)

	event, _ := eb.Publish("g:t", map[string]any{"key1": "val1"}, 3, true)
	eb.Ack(event.ID)

	a := acked.Load()
	if a == nil {
		t.Fatal("ACK回调未触发")
	}
	if a.ID != event.ID {
		t.Fatalf("ACK回调应收到正确的event, 期望ID=%s, 实际=%s", event.ID, a.ID)
	}
	if a.Topic != "g:t" {
		t.Fatalf("ACK回调应收到正确的topic, got %s", a.Topic)
	}
}

func TestAck_NoNeedAck_Ignored(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var ackCount atomic.Int32
	eb.SetAckCallback(func(e *Event) { ackCount.Add(1) })

	handler := func(e *Event) {}
	eb.Subscribe("order:created", Fanout, "sub1", handler)

	event, _ := eb.Publish("order:created", map[string]any{"orderId": "1"}, 1, false)

	err := eb.Ack(event.ID)
	if err != nil {
		t.Fatalf("不应返回错误: %v", err)
	}

	if ackCount.Load() != 0 {
		t.Fatalf("needAck=false的ACK不应触发回调, 实际=%d", ackCount.Load())
	}
}

func TestAck_Duplicate(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var ackCount atomic.Int32
	eb.SetAckCallback(func(e *Event) { ackCount.Add(1) })

	handler := func(e *Event) {}
	eb.Subscribe("order:created", Fanout, "sub1", handler)

	event, _ := eb.Publish("order:created", map[string]any{"orderId": "1"}, 1, true)

	eb.Ack(event.ID)
	eb.Ack(event.ID)
	eb.Ack(event.ID)

	if ackCount.Load() != 1 {
		t.Fatalf("重复ACK应只触发一次回调，实际=%d", ackCount.Load())
	}
}

func TestAck_NonExistentMessage(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	err := eb.Ack("nonexistent-id")
	if err == nil {
		t.Fatal("对不存在的消息ACK应返回错误")
	}
}

func TestAck_NoCallback(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	handler := func(e *Event) {}
	eb.Subscribe("g:t", Fanout, "s1", handler)

	event, _ := eb.Publish("g:t", map[string]any{"k": "v"}, 1, true)

	err := eb.Ack(event.ID)
	if err != nil {
		t.Fatalf("无回调时ACK不应报错: %v", err)
	}
}

func TestAck_StatusTransition(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	handler := func(e *Event) {}
	eb.Subscribe("g:t", Fanout, "s1", handler)

	event, _ := eb.Publish("g:t", map[string]any{"k": "v"}, 1, true)

	if event.Status != StatusDelivered {
		t.Fatalf("分发后status应为delivered, 实际=%s", event.Status)
	}

	eb.Ack(event.ID)
	if event.Status != StatusCompleted {
		t.Fatalf("ACK后status应为completed, 实际=%s", event.Status)
	}
}

// ============================================================
// Subscribe / Unsubscribe
// ============================================================

func TestSubscribe_Duplicate(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var count atomic.Int32
	handler1 := func(e *Event) { count.Add(1) }
	handler2 := func(e *Event) { count.Add(10) }

	eb.Subscribe("order:created", Fanout, "sub1", handler1)
	eb.Subscribe("order:created", Fanout, "sub1", handler2) // 覆盖

	eb.Publish("order:created", map[string]any{"k": "v"}, 1, false)

	if count.Load() != 10 {
		t.Fatalf("重复订阅应覆盖，期望=10, 实际=%d", count.Load())
	}
}

func TestSubscribe_Validation(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	handler := func(e *Event) {}

	if err := eb.Subscribe("", Fanout, "sub1", handler); err == nil {
		t.Fatal("期望返回错误：topic为空")
	}
	if err := eb.Subscribe("topic", Fanout, "", handler); err == nil {
		t.Fatal("期望返回错误：subscriberID为空")
	}
	if err := eb.Subscribe("topic", Fanout, "sub1", nil); err == nil {
		t.Fatal("期望返回错误：handler为空")
	}
}

func TestUnsubscribe(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var count atomic.Int32
	handler := func(e *Event) { count.Add(1) }

	eb.Subscribe("order:created", Fanout, "sub1", handler)
	eb.Subscribe("order:created", Fanout, "sub2", handler)

	eb.Unsubscribe("order:created", "sub1")

	_, err := eb.Publish("order:created", map[string]any{"orderId": "1"}, 1, false)
	if err != nil {
		t.Fatalf("发布失败: %v", err)
	}

	if count.Load() != 1 {
		t.Fatalf("取消订阅后期望仅1个订阅者收到，实际=%d", count.Load())
	}
}

func TestUnsubscribe_NonExist(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	eb.Unsubscribe("order:created", "sub1")
	eb.Unsubscribe("nonexist", "sub1")
}

func TestUnsubscribe_AllSubscribers(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	handler := func(e *Event) {}

	eb.Subscribe("g:t", Fanout, "s1", handler)
	eb.Subscribe("g:t", Fanout, "s2", handler)

	eb.Unsubscribe("g:t", "s1")
	eb.Unsubscribe("g:t", "s2")

	eb.mu.RLock()
	subs := eb.subscriptions["g:t"]
	eb.mu.RUnlock()
	if len(subs) != 0 {
		t.Fatalf("所有订阅者取消后，订阅列表应为空, 实际=%d", len(subs))
	}

	// 发布到无订阅者的 topic 应 no-op
	event, err := eb.Publish("g:t", map[string]any{"k": "v"}, 1, false)
	if err != nil {
		t.Fatalf("发布失败: %v", err)
	}
	if event != nil {
		t.Fatal("所有订阅者取消后发布应no-op")
	}
}

func TestSubscribe_ReceivesPendingMessages(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	// 先订阅再发布（有订阅者，正常分发）
	eb.Subscribe("g:t", Fanout, "s1", func(e *Event) {})

	var received atomic.Pointer[Event]
	handler := func(e *Event) { received.Store(e) }
	eb.Subscribe("g:t", Fanout, "s2", handler)

	// 发布消息（已有订阅者）
	eb.Publish("g:t", map[string]any{"k": "v1"}, 1, false)

	r := received.Load()
	if r == nil {
		t.Fatal("新订阅者应收到已发布的消息")
	}
}

// ============================================================
// 前缀分组 Topic
// ============================================================

func TestTopic_PrefixGrouping(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var dbCreate, dbInsert, cacheSet atomic.Int32

	eb.Subscribe("db:create", Fanout, "s1", func(e *Event) { dbCreate.Add(1) })
	eb.Subscribe("db:insert", Fanout, "s2", func(e *Event) { dbInsert.Add(1) })
	eb.Subscribe("cache:set", Fanout, "s3", func(e *Event) { cacheSet.Add(1) })

	eb.Publish("db:create", map[string]any{"table": "users"}, 1, false)
	eb.Publish("db:insert", map[string]any{"table": "orders"}, 1, false)
	eb.Publish("cache:set", map[string]any{"key": "session"}, 1, false)

	if dbCreate.Load() != 1 {
		t.Fatalf("db:create 期望1次, 实际=%d", dbCreate.Load())
	}
	if dbInsert.Load() != 1 {
		t.Fatalf("db:insert 期望1次, 实际=%d", dbInsert.Load())
	}
	if cacheSet.Load() != 1 {
		t.Fatalf("cache:set 期望1次, 实际=%d", cacheSet.Load())
	}
}

func TestTopic_MultiLevelPrefix(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var count atomic.Int32

	eb.Subscribe("db:primary:create", Fanout, "s1", func(e *Event) { count.Add(1) })
	eb.Subscribe("db:replica:create", Fanout, "s2", func(e *Event) { count.Add(1) })
	eb.Subscribe("db:primary:drop", Fanout, "s3", func(e *Event) { count.Add(1) })

	eb.Publish("db:primary:create", map[string]any{"k": "v"}, 1, false)
	eb.Publish("db:replica:create", map[string]any{"k": "v"}, 1, false)
	eb.Publish("db:primary:drop", map[string]any{"k": "v"}, 1, false)

	if count.Load() != 3 {
		t.Fatalf("多层前缀topic期望3次接收, 实际=%d", count.Load())
	}
}

func TestTopic_DifferentPrefixesIsolated(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var orderCount, userCount atomic.Int32

	eb.Subscribe("order:created", Fanout, "s1", func(e *Event) { orderCount.Add(1) })
	eb.Subscribe("user:login", Fanout, "s2", func(e *Event) { userCount.Add(1) })

	eb.Publish("order:created", map[string]any{"id": "1"}, 1, false)
	eb.Publish("user:login", map[string]any{"uid": "100"}, 1, false)

	if orderCount.Load() != 1 {
		t.Fatalf("order:created 期望1次, 实际=%d", orderCount.Load())
	}
	if userCount.Load() != 1 {
		t.Fatalf("user:login 期望1次, 实际=%d", userCount.Load())
	}
}

// ============================================================
// ListTopics 前缀搜索
// ============================================================

func TestListTopics_PrefixSearch(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	eb.Subscribe("db:create", Fanout, "s1", func(e *Event) {})
	eb.Subscribe("db:insert", Fanout, "s2", func(e *Event) {})
	eb.Subscribe("db:update", Fanout, "s3", func(e *Event) {})
	eb.Subscribe("cache:set", Fanout, "s4", func(e *Event) {})
	eb.Subscribe("cache:get", Fanout, "s5", func(e *Event) {})

	eb.Publish("db:create", map[string]any{"k": "v"}, 1, false)
	eb.Publish("db:insert", map[string]any{"k": "v"}, 1, false)
	eb.Publish("db:update", map[string]any{"k": "v"}, 1, false)
	eb.Publish("cache:set", map[string]any{"k": "v"}, 1, false)
	eb.Publish("cache:get", map[string]any{"k": "v"}, 1, false)

	dbTopics := eb.ListTopics("db")
	sort.Strings(dbTopics)
	if len(dbTopics) != 3 {
		t.Fatalf("ListTopics(\"db\") 期望3个, 实际=%d: %v", len(dbTopics), dbTopics)
	}
	expectedDB := []string{"db:create", "db:insert", "db:update"}
	for i, expected := range expectedDB {
		if dbTopics[i] != expected {
			t.Fatalf("db topics[%d]: 期望=%s, 实际=%s", i, expected, dbTopics[i])
		}
	}

	cacheTopics := eb.ListTopics("cache")
	sort.Strings(cacheTopics)
	if len(cacheTopics) != 2 {
		t.Fatalf("ListTopics(\"cache\") 期望2个, 实际=%d: %v", len(cacheTopics), cacheTopics)
	}
}

func TestListTopics_EmptyPrefix(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	eb.Subscribe("a:b", Fanout, "s1", func(e *Event) {})
	eb.Subscribe("c:d", Fanout, "s2", func(e *Event) {})

	eb.Publish("a:b", map[string]any{"k": "v"}, 1, false)
	eb.Publish("c:d", map[string]any{"k": "v"}, 1, false)

	all := eb.ListTopics("")
	sort.Strings(all)
	if len(all) != 2 {
		t.Fatalf("ListTopics(\"\") 期望全部2个, 实际=%d: %v", len(all), all)
	}
}

func TestListTopics_SubPrefix(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	eb.Subscribe("db:primary:create", Fanout, "s1", func(e *Event) {})
	eb.Subscribe("db:replica:create", Fanout, "s2", func(e *Event) {})
	eb.Subscribe("db:primary:drop", Fanout, "s3", func(e *Event) {})

	eb.Publish("db:primary:create", map[string]any{"k": "v"}, 1, false)
	eb.Publish("db:replica:create", map[string]any{"k": "v"}, 1, false)
	eb.Publish("db:primary:drop", map[string]any{"k": "v"}, 1, false)

	topics := eb.ListTopics("db:primary")
	sort.Strings(topics)
	if len(topics) != 2 {
		t.Fatalf("ListTopics(\"db:primary\") 期望2个, 实际=%d: %v", len(topics), topics)
	}
}

func TestListTopics_NoMatch(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	eb.Subscribe("db:create", Fanout, "s1", func(e *Event) {})
	eb.Publish("db:create", map[string]any{"k": "v"}, 1, false)

	topics := eb.ListTopics("xxx")
	if len(topics) != 0 {
		t.Fatalf("不匹配的前缀应返回空, 实际=%v", topics)
	}
}

func TestListTopics_NotRegistered(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	// 订阅了但没发布过消息
	eb.Subscribe("only:subscribed", Fanout, "s1", func(e *Event) {})

	topics := eb.ListTopics("only")
	if len(topics) != 1 || topics[0] != "only:subscribed" {
		t.Fatalf("订阅即注册topic, got: %v", topics)
	}
}

// ============================================================
// ListDataKeys
// ============================================================

func TestListDataKeys_Basic(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	eb.Subscribe("order:created", Fanout, "s1", func(e *Event) {})

	eb.Publish("order:created", map[string]any{
		"orderId": "ORD-001",
		"amount":  99.9,
	}, 1, false)

	keys := eb.ListDataKeys("order:created")
	sort.Strings(keys)
	if len(keys) != 2 {
		t.Fatalf("期望2个key, 实际=%d: %v", len(keys), keys)
	}
	if keys[0] != "amount" || keys[1] != "orderId" {
		t.Fatalf("keys 不匹配: %v", keys)
	}
}

func TestListDataKeys_Accumulation(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	eb.Subscribe("order:created", Fanout, "s1", func(e *Event) {})

	eb.Publish("order:created", map[string]any{"key1": "v1"}, 1, false)
	eb.Publish("order:created", map[string]any{"key2": "v2"}, 1, false)
	eb.Publish("order:created", map[string]any{"key1": "new-v1", "key3": "v3"}, 1, false)

	keys := eb.ListDataKeys("order:created")
	sort.Strings(keys)
	if len(keys) != 3 {
		t.Fatalf("多次发布后应累积3个key, 实际=%d: %v", len(keys), keys)
	}
}

func TestListDataKeys_DifferentTopics(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	eb.Subscribe("a:b", Fanout, "s1", func(e *Event) {})
	eb.Subscribe("c:d", Fanout, "s2", func(e *Event) {})

	eb.Publish("a:b", map[string]any{"x": 1}, 1, false)
	eb.Publish("c:d", map[string]any{"y": 2}, 1, false)

	keysAB := eb.ListDataKeys("a:b")
	keysCD := eb.ListDataKeys("c:d")

	if len(keysAB) != 1 || keysAB[0] != "x" {
		t.Fatalf("a:b keys 错误: %v", keysAB)
	}
	if len(keysCD) != 1 || keysCD[0] != "y" {
		t.Fatalf("c:d keys 错误: %v", keysCD)
	}
}

func TestListDataKeys_EmptyTopic(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	keys := eb.ListDataKeys("")
	if len(keys) != 0 {
		t.Fatalf("空topic应返回空, 实际=%v", keys)
	}
}

func TestListDataKeys_NonExistentTopic(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	keys := eb.ListDataKeys("nonexistent:topic")
	if len(keys) != 0 {
		t.Fatalf("不存在的topic应返回空, 实际=%v", keys)
	}
}

// ============================================================
// 优先级
// ============================================================

func TestPriorityOrder_InQueue(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	eb.Subscribe("g:t", Anyone, "s1", func(e *Event) {}) // Anyone + needAck=false 自动完成移除

	// 改用 needAck=true 让消息留在队列中观察顺序
	eb.Unsubscribe("g:t", "s1")
	eb.Subscribe("g:t", Fanout, "s1", func(e *Event) {})

	e1, _ := eb.Publish("g:t", map[string]any{"k": "1"}, 1, true)
	e2, _ := eb.Publish("g:t", map[string]any{"k": "2"}, 5, true)
	e3, _ := eb.Publish("g:t", map[string]any{"k": "3"}, 3, true)

	key := "g:t"
	eb.mu.RLock()
	pending := eb.pendingQueue[key]
	eb.mu.RUnlock()

	if len(pending) < 3 {
		t.Fatalf("期望3条待分发消息，实际=%d", len(pending))
	}
	if pending[0].ID != e2.ID {
		t.Fatalf("队列首位应为priority=5的消息, 实际priority=%d", pending[0].Priority)
	}
	if pending[1].ID != e3.ID {
		t.Fatalf("队列第二位应为priority=3的消息, 实际priority=%d", pending[1].Priority)
	}
	if pending[2].ID != e1.ID {
		t.Fatalf("队列第三位应为priority=1的消息, 实际priority=%d", pending[2].Priority)
	}
}

func TestPriorityOrder_SamePriority_ByTime(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	eb.Subscribe("g:t", Fanout, "s1", func(e *Event) {})

	e1, _ := eb.Publish("g:t", map[string]any{"k": "1"}, 3, true)
	e2, _ := eb.Publish("g:t", map[string]any{"k": "2"}, 3, true)

	key := "g:t"
	eb.mu.RLock()
	pending := eb.pendingQueue[key]
	eb.mu.RUnlock()

	if len(pending) < 2 {
		t.Fatalf("期望2条待分发消息，实际=%d", len(pending))
	}
	found1, found2 := false, false
	for _, e := range pending {
		if e.ID == e1.ID {
			found1 = true
		}
		if e.ID == e2.ID {
			found2 = true
		}
	}
	if !found1 || !found2 {
		t.Fatal("两条同优先级消息应都在队列中")
	}
}

func TestInsertByPriority(t *testing.T) {
	tests := []struct {
		name     string
		queue    []*Event
		event    *Event
		expected []int
	}{
		{name: "空队列", queue: nil, event: &Event{Priority: 3, Created: 1}, expected: []int{3}},
		{name: "插入到头部", queue: []*Event{{Priority: 1, Created: 1}}, event: &Event{Priority: 5, Created: 2}, expected: []int{5, 1}},
		{name: "插入到尾部", queue: []*Event{{Priority: 5, Created: 1}}, event: &Event{Priority: 1, Created: 2}, expected: []int{5, 1}},
		{name: "插入到中间", queue: []*Event{{Priority: 5, Created: 1}, {Priority: 1, Created: 3}}, event: &Event{Priority: 3, Created: 2}, expected: []int{5, 3, 1}},
		{name: "同优先级按时间排序", queue: []*Event{{Priority: 3, Created: 10}}, event: &Event{Priority: 3, Created: 5}, expected: []int{3, 3}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := insertByPriority(tt.queue, tt.event)
			if len(result) != len(tt.expected) {
				t.Fatalf("长度不一致: 期望=%d, 实际=%d", len(tt.expected), len(result))
			}
			for i, p := range tt.expected {
				if result[i].Priority != p {
					t.Fatalf("[%d] priority: 期望=%d, 实际=%d", i, p, result[i].Priority)
				}
			}
		})
	}
}

// ============================================================
// Event 字段完整性
// ============================================================

func TestEvent_FieldsIntegrity(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	eb.Subscribe("myGroup:myTopic", Fanout, "s1", func(e *Event) {})

	data := map[string]any{
		"key1": "value1",
		"key2": 123,
		"key3": true,
	}
	event, _ := eb.Publish("myGroup:myTopic", data, 4, true)

	if event.Topic != "myGroup:myTopic" {
		t.Fatalf("topic不匹配")
	}
	if event.Priority != 4 {
		t.Fatalf("priority不匹配")
	}
	if event.NeedAck != true {
		t.Fatalf("needAck不匹配")
	}
	if event.Data["key1"] != "value1" {
		t.Fatalf("data[key1]不匹配")
	}
	if event.Data["key2"] != 123 {
		t.Fatalf("data[key2]不匹配")
	}
	if event.Data["key3"] != true {
		t.Fatalf("data[key3]不匹配")
	}
}

func TestMultipleMessages_SameTopic(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var mu sync.Mutex
	var received []*Event
	handler := func(e *Event) {
		mu.Lock()
		received = append(received, e)
		mu.Unlock()
	}
	eb.Subscribe("g:t", Fanout, "s1", handler)

	eb.Publish("g:t", map[string]any{"k": "1"}, 1, false)
	eb.Publish("g:t", map[string]any{"k": "2"}, 2, false)
	eb.Publish("g:t", map[string]any{"k": "3"}, 3, false)

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 3 {
		t.Fatalf("期望收到3条消息，实际=%d", len(received))
	}
}

func TestAnyone_MultipleMessages_RoundRobin(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var mu sync.Mutex
	receivedBy := make(map[string][]string)

	makeHandler := func(sid string) HandlerFunc {
		return func(e *Event) {
			mu.Lock()
			receivedBy[sid] = append(receivedBy[sid], e.ID)
			mu.Unlock()
		}
	}

	eb.Subscribe("g:t", Anyone, "s1", makeHandler("s1"))
	eb.Subscribe("g:t", Anyone, "s2", makeHandler("s2"))

	for i := 0; i < 5; i++ {
		eb.Publish("g:t", map[string]any{"k": fmt.Sprintf("%d", i)}, 1, false)
	}

	mu.Lock()
	defer mu.Unlock()
	total := len(receivedBy["s1"]) + len(receivedBy["s2"])
	if total != 5 {
		t.Fatalf("Anyone模式下5条消息应被消费5次，实际=%d", total)
	}
}

// ============================================================
// 数据竞争测试（需使用 go test -race 运行）
// ============================================================

func TestRace_PublishConcurrent(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var count atomic.Int32
	eb.Subscribe("g:t", Fanout, "s1", func(e *Event) { count.Add(1) })

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eb.Publish("g:t", map[string]any{"k": i}, 1, false)
		}(i)
	}
	wg.Wait()

	if count.Load() != 200 {
		t.Fatalf("期望200条消息，实际=%d", count.Load())
	}
}

func TestRace_PublishAndSubscribe(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var wg sync.WaitGroup
	const n = 50

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eb.Subscribe("g:t", Fanout, fmt.Sprintf("sub%d", i), func(e *Event) {})
		}(i)
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eb.Publish("g:t", map[string]any{"k": i}, 1, false)
		}(i)
	}
	wg.Wait()
}

func TestRace_PublishAndUnsubscribe(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	for i := 0; i < 50; i++ {
		eb.Subscribe("g:t", Fanout, fmt.Sprintf("sub%d", i), func(e *Event) {})
	}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eb.Publish("g:t", map[string]any{"k": i}, 1, false)
		}(i)
	}
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eb.Unsubscribe("g:t", fmt.Sprintf("sub%d", i))
		}(i)
	}
	wg.Wait()
}

func TestRace_PublishAndAck(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var ackCount atomic.Int32
	eb.SetAckCallback(func(e *Event) { ackCount.Add(1) })

	eb.Subscribe("g:t", Fanout, "s1", func(e *Event) {})

	var wg sync.WaitGroup
	ids := make(chan string, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			event, _ := eb.Publish("g:t", map[string]any{"k": i}, 1, true)
			ids <- event.ID
		}(i)
	}
	wg.Wait()
	close(ids)

	var ackWg sync.WaitGroup
	for id := range ids {
		ackWg.Add(1)
		go func(id string) {
			defer ackWg.Done()
			eb.Ack(id)
		}(id)
	}
	ackWg.Wait()

	if ackCount.Load() != 100 {
		t.Fatalf("期望100次ACK回调，实际=%d", ackCount.Load())
	}
}

func TestRace_PublishAndList(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var wg sync.WaitGroup
	const n = 50

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eb.Publish(fmt.Sprintf("prefix:key%d", i), map[string]any{"k": i}, 1, false)
		}(i)
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eb.ListTopics("")
		}(i)
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eb.ListDataKeys("prefix:key0")
		}(i)
	}
	wg.Wait()
}

func TestRace_SubscribeAndUnsubscribe(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var wg sync.WaitGroup
	const rounds = 20

	for r := 0; r < rounds; r++ {
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				eb.Subscribe("g:t", Fanout, fmt.Sprintf("sub%d", i), func(e *Event) {})
			}(i)
		}
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				eb.Unsubscribe("g:t", fmt.Sprintf("sub%d", i))
			}(i)
		}
	}
	wg.Wait()
}

func TestRace_MultipleTopicsConcurrent(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var wg sync.WaitGroup

	for g := 0; g < 5; g++ {
		topic := fmt.Sprintf("group%d:topic", g)
		for s := 0; s < 5; s++ {
			sid := fmt.Sprintf("sub%d", s)
			wg.Add(1)
			go func(topic, sid string) {
				defer wg.Done()
				eb.Subscribe(topic, Fanout, sid, func(e *Event) {})
			}(topic, sid)
		}
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(topic string, i int) {
				defer wg.Done()
				eb.Publish(topic, map[string]any{"k": i}, 1, false)
			}(topic, i)
		}
		wg.Add(1)
		go func(topic string) {
			defer wg.Done()
			eb.ListTopics(topic[:strings.LastIndex(topic, ":")])
			eb.ListDataKeys(topic)
		}(topic)
	}
	wg.Wait()
}

func TestRace_SetCallbackAndAck(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var ackCount atomic.Int32
	eb.Subscribe("g:t", Fanout, "s1", func(e *Event) {})

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eb.SetAckCallback(func(e *Event) { ackCount.Add(1) })
		}(i)
	}

	events := make(chan *Event, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			event, _ := eb.Publish("g:t", map[string]any{"k": i}, 1, true)
			events <- event
		}(i)
	}
	wg.Wait()
	close(events)

	for event := range events {
		eb.Ack(event.ID)
	}
}

func TestRace_AnyonePublishConcurrent(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var count atomic.Int32
	eb.Subscribe("g:t", Anyone, "s1", func(e *Event) { count.Add(1) })
	eb.Subscribe("g:t", Anyone, "s2", func(e *Event) { count.Add(1) })
	eb.Subscribe("g:t", Anyone, "s3", func(e *Event) { count.Add(1) })

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eb.Publish("g:t", map[string]any{"k": i}, 1, false)
		}(i)
	}
	wg.Wait()

	if count.Load() != 100 {
		t.Fatalf("Anyone模式期望100次调用，实际=%d", count.Load())
	}
}

func TestRace_MixedOperations(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		topic := fmt.Sprintf("g:t:%d", i)
		wg.Add(1)
		go func(topic string, i int) {
			defer wg.Done()
			eb.Publish(topic, map[string]any{"k": i}, (i%5)+1, i%3 == 0)
		}(topic, i)
		wg.Add(1)
		go func(topic string, i int) {
			defer wg.Done()
			eb.Subscribe(topic, Fanout, fmt.Sprintf("sub%d", i), func(e *Event) {})
		}(topic, i)
		wg.Add(1)
		go func(topic string, i int) {
			defer wg.Done()
			eb.Unsubscribe(topic, fmt.Sprintf("sub%d", i))
		}(topic, i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			eb.ListTopics("g")
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			eb.ListDataKeys("g:t:0")
		}()
	}
	wg.Wait()
}

func TestRace_FanoutNeedAckConcurrent(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var ackCount atomic.Int32
	eb.SetAckCallback(func(e *Event) { ackCount.Add(1) })

	eb.Subscribe("g:t", Fanout, "s1", func(e *Event) {})
	eb.Subscribe("g:t", Fanout, "s2", func(e *Event) {})

	var wg sync.WaitGroup
	ids := make(chan string, 50)

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			event, _ := eb.Publish("g:t", map[string]any{"k": i}, (i%5)+1, true)
			ids <- event.ID
		}(i)
	}
	wg.Wait()
	close(ids)

	for id := range ids {
		eb.Ack(id)
	}

	if ackCount.Load() != 50 {
		t.Fatalf("期望50次ACK回调，实际=%d", ackCount.Load())
	}
}

func TestRace_LoadEventConcurrent(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	eb.Subscribe("g:t", Fanout, "s1", func(e *Event) {})

	var ids []string
	for i := 0; i < 20; i++ {
		event, _ := eb.Publish("g:t", map[string]any{"k": i}, 1, false)
		ids = append(ids, event.ID)
	}

	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			_, err := eb.loadEvent(id)
			if err != nil {
				t.Errorf("加载事件 %s 失败: %v", id, err)
			}
		}(id)
	}
	wg.Wait()
}

func TestRace_StoreEventConcurrent(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	events := make([]*Event, 20)
	for i := 0; i < 20; i++ {
		events[i] = &Event{
			ID:       fmt.Sprintf("test-%d", i),
			Topic:    "g:t",
			Data:     map[string]any{"k": i},
			Status:   StatusPending,
			Priority: 1,
			Created:  time.Now().Unix(),
		}
	}

	var wg sync.WaitGroup
	for _, event := range events {
		wg.Add(1)
		go func(e *Event) {
			defer wg.Done()
			eb.storeEvent(e)
		}(event)
	}
	wg.Wait()

	for _, event := range events {
		loaded, err := eb.loadEvent(event.ID)
		if err != nil {
			t.Fatalf("加载事件 %s 失败: %v", event.ID, err)
		}
		if loaded.ID != event.ID {
			t.Fatalf("ID不匹配: 期望=%s, 实际=%s", event.ID, loaded.ID)
		}
	}
}

// ============================================================
// 更多数据竞争测试
// ============================================================

func TestRace_PublishNoSubscriber(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			topic := fmt.Sprintf("nosub:topic%d", i%10)
			eb.Publish(topic, map[string]any{"k": i}, 1, false)
		}(i)
	}
	wg.Wait()
}

func TestRace_ConcurrentAckSameEvent(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var ackCount atomic.Int32
	eb.SetAckCallback(func(e *Event) { ackCount.Add(1) })

	eb.Subscribe("g:t", Fanout, "s1", func(e *Event) {})

	event, _ := eb.Publish("g:t", map[string]any{"k": "v"}, 1, true)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			eb.Ack(event.ID)
		}()
	}
	wg.Wait()

	if ackCount.Load() != 1 {
		t.Fatalf("并发ACK同一消息应只触发1次回调，实际=%d", ackCount.Load())
	}
}

func TestRace_PublishDifferentTopics(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		topic := fmt.Sprintf("race:topic%d", i%20)
		wg.Add(1)
		go func(topic string, i int) {
			defer wg.Done()
			eb.Subscribe(topic, Fanout, fmt.Sprintf("sub%d", i), func(e *Event) {})
		}(topic, i)
	}
	for i := 0; i < 200; i++ {
		topic := fmt.Sprintf("race:topic%d", i%20)
		wg.Add(1)
		go func(topic string, i int) {
			defer wg.Done()
			eb.Publish(topic, map[string]any{"k": i}, (i%5)+1, i%2 == 0)
		}(topic, i)
	}
	wg.Wait()
}

func TestRace_SubscribePublishUnsubscribe(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var wg sync.WaitGroup
	const rounds = 50

	for r := 0; r < rounds; r++ {
		wg.Add(3)
		go func() {
			defer wg.Done()
			eb.Subscribe("race:combo", Fanout, "s1", func(e *Event) {})
		}()
		go func() {
			defer wg.Done()
			eb.Publish("race:combo", map[string]any{"k": "v"}, 1, false)
		}()
		go func() {
			defer wg.Done()
			eb.Unsubscribe("race:combo", "s1")
		}()
	}
	wg.Wait()
}

func TestRace_ListTopicsWhilePublish(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	for i := 0; i < 10; i++ {
		topic := fmt.Sprintf("list:topic%d", i)
		eb.Subscribe(topic, Fanout, "s1", func(e *Event) {})
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			topic := fmt.Sprintf("list:topic%d", i%10)
			eb.Publish(topic, map[string]any{"k": i}, 1, false)
		}(i)
		go func() {
			defer wg.Done()
			topics := eb.ListTopics("list")
			_ = topics
		}()
	}
	wg.Wait()
}

func TestRace_ListDataKeysWhilePublish(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	eb.Subscribe("dk:topic", Fanout, "s1", func(e *Event) {})

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			eb.Publish("dk:topic", map[string]any{fmt.Sprintf("key%d", i%5): i}, 1, false)
		}(i)
		go func() {
			defer wg.Done()
			keys := eb.ListDataKeys("dk:topic")
			_ = keys
		}()
	}
	wg.Wait()
}

func TestRace_MultiLevelPrefixPublish(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var wg sync.WaitGroup
	prefixes := []string{"app:db:create", "app:db:delete", "app:cache:set", "app:cache:get", "app:log:info"}

	for _, p := range prefixes {
		eb.Subscribe(p, Fanout, "s1", func(e *Event) {})
	}

	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			topic := prefixes[i%len(prefixes)]
			eb.Publish(topic, map[string]any{"k": i}, (i%5)+1, i%3 == 0)
		}(i)
	}
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			eb.ListTopics("app:db")
		}()
	}
	wg.Wait()
}

func TestRace_PublishAfterFullUnsubscribe(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var wg sync.WaitGroup
	const n = 50

	for i := 0; i < n; i++ {
		eb.Subscribe("race:empty", Fanout, fmt.Sprintf("s%d", i), func(e *Event) {})
	}

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eb.Unsubscribe("race:empty", fmt.Sprintf("s%d", i))
		}(i)
	}
	wg.Wait()

	var publishCount atomic.Int32
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			event, _ := eb.Publish("race:empty", map[string]any{"k": i}, 1, false)
			if event != nil {
				publishCount.Add(1)
			}
		}(i)
	}
	wg.Wait()

	if publishCount.Load() != 0 {
		t.Fatalf("全部取消订阅后发布应为no-op, 实际=%d", publishCount.Load())
	}
}

func TestRace_FanoutMultipleSubscribersConcurrent(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var count atomic.Int32
	for i := 0; i < 10; i++ {
		eb.Subscribe("race:fanout", Fanout, fmt.Sprintf("s%d", i), func(e *Event) {
			count.Add(1)
		})
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eb.Publish("race:fanout", map[string]any{"k": i}, 1, false)
		}(i)
	}
	wg.Wait()

	if count.Load() != 1000 {
		t.Fatalf("10个订阅者x100条消息=1000次, 实际=%d", count.Load())
	}
}

func TestRace_AnyoneMultipleSubscribersConcurrent(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var count atomic.Int32
	for i := 0; i < 10; i++ {
		eb.Subscribe("race:anyone", Anyone, fmt.Sprintf("s%d", i), func(e *Event) {
			count.Add(1)
		})
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eb.Publish("race:anyone", map[string]any{"k": i}, 1, false)
		}(i)
	}
	wg.Wait()

	if count.Load() != 100 {
		t.Fatalf("Anyone模式100条消息应100次调用, 实际=%d", count.Load())
	}
}

func TestRace_SetCallbackConcurrent(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eb.SetAckCallback(func(e *Event) {})
		}(i)
	}
	wg.Wait()
}

func TestRace_PublishAndLoadEvent(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	eb.Subscribe("race:load", Fanout, "s1", func(e *Event) {})

	var ids []string
	for i := 0; i < 20; i++ {
		event, _ := eb.Publish("race:load", map[string]any{"k": i}, 1, true)
		ids = append(ids, event.ID)
	}

	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(2)
		go func(id string) {
			defer wg.Done()
			eb.Ack(id)
		}(id)
		go func(id string) {
			defer wg.Done()
			_, _ = eb.loadEvent(id)
		}(id)
	}
	wg.Wait()
}

func TestRace_SubscribeAndListTopics(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			topic := fmt.Sprintf("sl:topic%d", i%10)
			eb.Subscribe(topic, Fanout, fmt.Sprintf("sub%d", i), func(e *Event) {})
		}(i)
		go func() {
			defer wg.Done()
			eb.ListTopics("sl")
		}()
	}
	wg.Wait()
}

func TestRace_UnsubscribeAndListDataKeys(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	for i := 0; i < 20; i++ {
		eb.Subscribe("ul:topic", Fanout, fmt.Sprintf("s%d", i), func(e *Event) {})
	}
	eb.Publish("ul:topic", map[string]any{"key1": "v1", "key2": "v2"}, 1, false)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			eb.Unsubscribe("ul:topic", fmt.Sprintf("s%d", i))
		}(i)
		go func() {
			defer wg.Done()
			eb.ListDataKeys("ul:topic")
		}()
	}
	wg.Wait()
}

func TestRace_PublishWithMixedNeedAck(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var ackCount atomic.Int32
	eb.SetAckCallback(func(e *Event) { ackCount.Add(1) })

	eb.Subscribe("race:mixack", Fanout, "s1", func(e *Event) {})

	var wg sync.WaitGroup
	ids := make(chan string, 50)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			needAck := i%2 == 0
			event, _ := eb.Publish("race:mixack", map[string]any{"k": i}, (i%5)+1, needAck)
			if needAck && event != nil {
				ids <- event.ID
			}
		}(i)
	}
	wg.Wait()
	close(ids)

	for id := range ids {
		eb.Ack(id)
	}
}

// ============================================================
// SetAckCallback
// ============================================================

func TestSetAckCallback(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	called := false
	eb.SetAckCallback(func(e *Event) { called = true })

	eb.Subscribe("g:t", Fanout, "s1", func(e *Event) {})

	event, _ := eb.Publish("g:t", map[string]any{"k": "v"}, 1, true)
	eb.Ack(event.ID)

	if !called {
		t.Fatal("设置回调后ACK应触发回调")
	}
}

// ============================================================
// FastCache 存储层
// ============================================================

func TestLoadEvent(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	eb.Subscribe("g:t", Fanout, "s1", func(e *Event) {})

	event, _ := eb.Publish("g:t", map[string]any{"k": "v"}, 1, false)

	loaded, err := eb.loadEvent(event.ID)
	if err != nil {
		t.Fatalf("加载事件失败: %v", err)
	}
	if loaded.ID != event.ID {
		t.Fatalf("ID不匹配")
	}
	if loaded.Topic != event.Topic {
		t.Fatalf("Topic不匹配")
	}
}

func TestLoadEvent_NonExistent(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	_, err := eb.loadEvent("nonexistent")
	if err == nil {
		t.Fatal("加载不存在的消息应返回错误")
	}
}

func TestStoreEvent_UpdatesStatus(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	eb.Subscribe("g:t", Fanout, "s1", func(e *Event) {})

	event, _ := eb.Publish("g:t", map[string]any{"k": "v"}, 1, true)

	loaded, _ := eb.loadEvent(event.ID)
	if loaded.Status != StatusDelivered {
		t.Fatalf("存储的状态应为delivered, 实际=%s", loaded.Status)
	}
}

// ============================================================
// NewEventBus
// ============================================================

func TestNewEventBus_DefaultSize(t *testing.T) {
	eb := NewEventBus(0)
	if eb.cache == nil {
		t.Fatal("默认缓存不应为nil")
	}
	eb.cache.Reset()
}

func TestNewEventBus_CustomSize(t *testing.T) {
	eb := NewEventBus(1024)
	if eb.cache == nil {
		t.Fatal("自定义缓存不应为nil")
	}
	eb.cache.Reset()
}

// ============================================================
// Benchmark 压力测试
// ============================================================

// BenchmarkPublish_Fanout 测试 Fanout 模式下的发布性能
func BenchmarkPublish_Fanout(b *testing.B) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	eb.Subscribe("bench:fanout", Fanout, "s1", func(e *Event) {})
	eb.Subscribe("bench:fanout", Fanout, "s2", func(e *Event) {})
	eb.Subscribe("bench:fanout", Fanout, "s3", func(e *Event) {})

	data := map[string]any{"key": "value", "id": 123}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eb.Publish("bench:fanout", data, 1, false)
	}
	b.StopTimer()
}

// BenchmarkPublish_Anyone 测试 Anyone 模式下的发布性能
func BenchmarkPublish_Anyone(b *testing.B) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	eb.Subscribe("bench:anyone", Anyone, "worker1", func(e *Event) {})
	eb.Subscribe("bench:anyone", Anyone, "worker2", func(e *Event) {})

	data := map[string]any{"task": "benchmark"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eb.Publish("bench:anyone", data, 1, false)
	}
	b.StopTimer()
}

// BenchmarkPublish_NoSubscriber 测试无订阅者时的 No-Op 性能
func BenchmarkPublish_NoSubscriber(b *testing.B) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	data := map[string]any{"key": "value"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eb.Publish("bench:nosub", data, 1, false)
	}
	b.StopTimer()
}

// BenchmarkSubscribe_Unsubscribe 测试订阅/取消订阅的性能
func BenchmarkSubscribe_Unsubscribe(b *testing.B) {
	data := map[string]any{"k": "v"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eb := NewEventBus(0)
		topic := fmt.Sprintf("bench:sub:%d", i)
		eb.Subscribe(topic, Fanout, "s1", func(e *Event) {})
		eb.Publish(topic, data, 1, false)
		eb.Unsubscribe(topic, "s1")
		eb.cache.Reset()
	}
	b.StopTimer()
}

// BenchmarkListTopics 测试大量 Topic 下的列表查询性能
func BenchmarkListTopics(b *testing.B) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	// 预先创建 1000 个不同前缀的 topic
	for i := 0; i < 1000; i++ {
		topic := fmt.Sprintf("group%d:topic%d", i%10, i)
		eb.Subscribe(topic, Fanout, "s1", func(e *Event) {})
		eb.Publish(topic, map[string]any{"id": i}, 1, false)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eb.ListTopics("group5")
	}
	b.StopTimer()
}

// BenchmarkListTopics_All 测试获取所有 Topic 的性能
func BenchmarkListTopics_All(b *testing.B) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	// 预先创建 1000 个 topic
	for i := 0; i < 1000; i++ {
		topic := fmt.Sprintf("bench:list:%d", i)
		eb.Subscribe(topic, Fanout, "s1", func(e *Event) {})
		eb.Publish(topic, map[string]any{"id": i}, 1, false)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eb.ListTopics("")
	}
	b.StopTimer()
}

// BenchmarkListDataKeys 测试 Data Keys 查询性能
func BenchmarkListDataKeys(b *testing.B) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	topic := "bench:datakeys"
	eb.Subscribe(topic, Fanout, "s1", func(e *Event) {})

	// 发布多条消息，累积多个 key
	for i := 0; i < 100; i++ {
		eb.Publish(topic, map[string]any{
			fmt.Sprintf("key%d", i): i,
			"common":                "value",
		}, 1, false)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eb.ListDataKeys(topic)
	}
	b.StopTimer()
}

// BenchmarkPublish_Concurrent 测试并发发布的性能
func BenchmarkPublish_Concurrent(b *testing.B) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	eb.Subscribe("bench:concurrent", Fanout, "s1", func(e *Event) {})
	eb.Subscribe("bench:concurrent", Fanout, "s2", func(e *Event) {})

	data := map[string]any{"key": "value"}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			eb.Publish("bench:concurrent", data, 1, false)
			i++
		}
	})
	b.StopTimer()
}

// BenchmarkPublish_MultipleTopics 测试多 Topic 轮询发布的性能
func BenchmarkPublish_MultipleTopics(b *testing.B) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	// 创建 100 个不同的 topic
	topics := make([]string, 100)
	for i := 0; i < 100; i++ {
		topics[i] = fmt.Sprintf("bench:multi:%d", i)
		eb.Subscribe(topics[i], Fanout, "s1", func(e *Event) {})
	}

	data := map[string]any{"key": "value"}
	idx := 0

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eb.Publish(topics[idx], data, 1, false)
		idx = (idx + 1) % len(topics)
	}
	b.StopTimer()
}

// BenchmarkPublish_WithACK 测试带 ACK 的发布性能
func BenchmarkPublish_WithACK(b *testing.B) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	eb.SetAckCallback(func(e *Event) {})

	eb.Subscribe("bench:ack", Fanout, "s1", func(e *Event) {
		eb.Ack(e.ID)
	})

	data := map[string]any{"key": "value"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eb.Publish("bench:ack", data, 1, true)
	}
	b.StopTimer()
}

// BenchmarkPublish_PriorityQueue 测试优先级队列的性能
func BenchmarkPublish_PriorityQueue(b *testing.B) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	eb.Subscribe("bench:priority", Fanout, "s1", func(e *Event) {})

	data := map[string]any{"key": "value"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// 循环使用不同优先级
		priority := (i % 5) + 1
		eb.Publish("bench:priority", data, priority, false)
	}
	b.StopTimer()
}

// BenchmarkPublish_LargeData 测试大数据量发布的性能
func BenchmarkPublish_LargeData(b *testing.B) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	eb.Subscribe("bench:large", Fanout, "s1", func(e *Event) {})

	// 构造大数据量 payload（50个字段）
	data := map[string]any{
		"id":        12345,
		"name":      "benchmark-test-data",
		"email":     "test@example.com",
		"phone":     "+86-13800138000",
		"address":   "Benchmark Street, Test City",
		"score":     99.99,
		"active":    true,
		"tags":      []string{"a", "b", "c", "d", "e"},
		"metadata":  map[string]string{"k1": "v1", "k2": "v2"},
		"timestamp": time.Now().Unix(),
		"field1":    "value1",
		"field2":    "value2",
		"field3":    "value3",
		"field4":    "value4",
		"field5":    "value5",
		"field6":    "value6",
		"field7":    "value7",
		"field8":    "value8",
		"field9":    "value9",
		"field10":   "value10",
		"nested": map[string]any{
			"level2": map[string]any{
				"level3": "deep-value",
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eb.Publish("bench:large", data, 1, false)
	}
	b.StopTimer()
}

// BenchmarkMixedOperations 测试混合操作的吞吐量
func BenchmarkMixedOperations(b *testing.B) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var counter atomic.Int64
	eb.SetAckCallback(func(e *Event) {
		counter.Add(1)
	})

	// 预先创建一些订阅
	for i := 0; i < 10; i++ {
		topic := fmt.Sprintf("bench:mixed:%d", i)
		eb.Subscribe(topic, Fanout, "s1", func(e *Event) {
			if e.NeedAck {
				eb.Ack(e.ID)
			}
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		switch i % 4 {
		case 0:
			// 发布消息
			topic := fmt.Sprintf("bench:mixed:%d", i%10)
			needAck := i%3 == 0
			eb.Publish(topic, map[string]any{"i": i}, 1, needAck)
		case 1:
			// 查询 topics
			eb.ListTopics("bench:mixed")
		case 2:
			// 查询 data keys
			topic := fmt.Sprintf("bench:mixed:%d", i%10)
			eb.ListDataKeys(topic)
		case 3:
			// 动态订阅/取消订阅
			if i%20 == 0 {
				topic := fmt.Sprintf("bench:temp:%d", i)
				eb.Subscribe(topic, Fanout, "temp", func(e *Event) {})
				eb.Unsubscribe(topic, "temp")
			}
		}
	}
	b.StopTimer()
}

// BenchmarkPublish_ManySubscribers 测试大量订阅者场景
func BenchmarkPublish_ManySubscribers(b *testing.B) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	// 创建 100 个订阅者
	for i := 0; i < 100; i++ {
		subID := fmt.Sprintf("subscriber-%d", i)
		eb.Subscribe("bench:many", Fanout, subID, func(e *Event) {})
	}

	data := map[string]any{"key": "value"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eb.Publish("bench:many", data, 1, false)
	}
	b.StopTimer()
}
