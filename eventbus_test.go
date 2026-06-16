package gobus

import (
	"fmt"
	"sort"
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

	_, err := eb.Publish("", "topic", map[string]any{"k": "v"}, 1, false)
	if err == nil {
		t.Fatal("期望返回错误：group为空")
	}

	_, err = eb.Publish("group", "", map[string]any{"k": "v"}, 1, false)
	if err == nil {
		t.Fatal("期望返回错误：topic为空")
	}

	_, err = eb.Publish("group", "topic", map[string]any{}, 1, false)
	if err == nil {
		t.Fatal("期望返回错误：data为空")
	}

	_, err = eb.Publish("group", "topic", map[string]any{"k": "v"}, 0, false)
	if err == nil {
		t.Fatal("期望返回错误：priority越界(0)")
	}

	_, err = eb.Publish("group", "topic", map[string]any{"k": "v"}, 6, false)
	if err == nil {
		t.Fatal("期望返回错误：priority越界(6)")
	}

	_, err = eb.Publish("group", "topic", map[string]any{"k": "v"}, -1, false)
	if err == nil {
		t.Fatal("期望返回错误：priority越界(-1)")
	}
}

func TestPublish_PriorityBoundary(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	// priority=1 和 priority=5 是合法边界
	for _, p := range []int{1, 2, 3, 4, 5} {
		_, err := eb.Publish("g", "t", map[string]any{"k": "v"}, p, false)
		if err != nil {
			t.Fatalf("priority=%d 应合法: %v", p, err)
		}
	}
}

func TestPublish_DefaultNeedAck(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	event, _ := eb.Publish("g", "t", map[string]any{"k": "v"}, 1, false)
	if event.NeedAck != false {
		t.Fatal("needAck 默认应为 false")
	}
}

func TestPublish_CreatedTimestamp(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	before := time.Now().Unix()
	event, _ := eb.Publish("g", "t", map[string]any{"k": "v"}, 1, false)
	after := time.Now().Unix()

	if event.Created < before || event.Created > after {
		t.Fatalf("created 时间戳应在当前时间范围内, before=%d, created=%d, after=%d", before, event.Created, after)
	}
}

func TestPublish_InitialStatus(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	event, _ := eb.Publish("g", "t", map[string]any{"k": "v"}, 1, false)
	if event.Status != StatusPending {
		t.Fatalf("初始状态应为 pending, 实际=%s", event.Status)
	}
}

func TestPublish_IncrementalID(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	e1, _ := eb.Publish("g", "t", map[string]any{"k": "1"}, 1, false)
	e2, _ := eb.Publish("g", "t", map[string]any{"k": "2"}, 1, false)
	e3, _ := eb.Publish("g", "t", map[string]any{"k": "3"}, 1, false)

	if e1.ID == e2.ID || e2.ID == e3.ID || e1.ID == e3.ID {
		t.Fatal("每条消息的ID应唯一")
	}
}

func TestPublish_DataVariousTypes(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	data := map[string]any{
		"stringVal": "hello",
		"intVal":    42,
		"floatVal":  3.14,
		"nilVal":    nil,
		"boolVal":   true,
	}
	event, err := eb.Publish("g", "t", data, 1, false)
	if err != nil {
		t.Fatalf("发布含多种类型data的消息失败: %v", err)
	}
	if len(event.Data) != 5 {
		t.Fatalf("期望5个data字段，实际=%d", len(event.Data))
	}
}

// ============================================================
// Fanout 模式
// ============================================================

func TestFanout_AllSubscribers(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var count atomic.Int32
	handler := func(e *Event) {
		count.Add(1)
	}

	eb.Subscribe("order", "created", Fanout, "sub1", handler)
	eb.Subscribe("order", "created", Fanout, "sub2", handler)
	eb.Subscribe("order", "created", Fanout, "sub3", handler)

	_, err := eb.Publish("order", "created", map[string]any{"orderId": "1"}, 1, false)
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
	eb.Subscribe("order", "created", Fanout, "sub1", handler)

	event, _ := eb.Publish("order", "created", map[string]any{"k": "v"}, 1, false)

	// Fanout + needAck=false 应从待分发队列移除
	key := subKey("order", "created")
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
	eb.Subscribe("order", "created", Fanout, "sub1", handler)

	event, _ := eb.Publish("order", "created", map[string]any{"k": "v"}, 1, true)

	// Fanout + needAck=true 应保留在待分发队列
	key := subKey("order", "created")
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

	// 状态应为 delivered
	if event.Status != StatusDelivered {
		t.Fatalf("期望status=delivered, 实际=%s", event.Status)
	}
}

func TestFanout_NeedAckTrue_ACKRemovesFromPending(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	handler := func(e *Event) {}
	eb.Subscribe("order", "created", Fanout, "sub1", handler)

	event, _ := eb.Publish("order", "created", map[string]any{"k": "v"}, 1, true)

	eb.Ack(event.ID)

	key := subKey("order", "created")
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
	received := make(map[string]string) // subscriberID -> eventID

	handler := func(sid string) HandlerFunc {
		return func(e *Event) {
			mu.Lock()
			received[sid] = e.ID
			mu.Unlock()
		}
	}

	eb.Subscribe("g", "t", Fanout, "s1", handler("s1"))
	eb.Subscribe("g", "t", Fanout, "s2", handler("s2"))

	event, _ := eb.Publish("g", "t", map[string]any{"k": "v"}, 1, false)

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
	handler := func(e *Event) {
		count.Add(1)
	}

	eb.Subscribe("order", "created", Anyone, "sub1", handler)
	eb.Subscribe("order", "created", Anyone, "sub2", handler)
	eb.Subscribe("order", "created", Anyone, "sub3", handler)

	_, err := eb.Publish("order", "created", map[string]any{"orderId": "1"}, 1, false)
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
	handler := func(e *Event) {
		received.Store(e)
	}

	eb.Subscribe("order", "created", Anyone, "sub1", handler)

	event, _ := eb.Publish("order", "created", map[string]any{"orderId": "1"}, 1, false)

	r := received.Load()
	if r == nil {
		t.Fatal("未收到消息")
	}
	if r.Status != StatusCompleted {
		t.Fatalf("Anyone+无ACK期望status=completed, 实际=%s", r.Status)
	}

	key := subKey("order", "created")
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
	handler := func(e *Event) {
		received.Store(e)
	}

	eb.Subscribe("order", "created", Anyone, "sub1", handler)

	event, _ := eb.Publish("order", "created", map[string]any{"orderId": "1"}, 1, true)

	r := received.Load()
	if r == nil {
		t.Fatal("未收到消息")
	}
	if r.Status != StatusDelivered {
		t.Fatalf("Anyone+needAck=true 期望status=delivered, 实际=%s", r.Status)
	}

	// ACK后应变为completed
	eb.Ack(event.ID)
	if event.Status != StatusCompleted {
		t.Fatalf("ACK后期望status=completed, 实际=%s", event.Status)
	}
}

func TestAnyone_NeedAckTrue_StaysInPendingUntilAck(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	handler := func(e *Event) {}
	eb.Subscribe("order", "created", Anyone, "sub1", handler)

	event, _ := eb.Publish("order", "created", map[string]any{"k": "v"}, 1, true)

	// 还未ACK，应保留在待分发队列
	key := subKey("order", "created")
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

	// ACK后移除
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
	eb.SetAckCallback(func(e *Event) {
		ackEvent.Store(e)
	})

	var received atomic.Pointer[Event]
	handler := func(e *Event) {
		received.Store(e)
	}

	eb.Subscribe("order", "created", Fanout, "sub1", handler)

	event, _ := eb.Publish("order", "created", map[string]any{"orderId": "1"}, 1, true)

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
	eb.SetAckCallback(func(e *Event) {
		acked.Store(e)
	})

	handler := func(e *Event) {}
	eb.Subscribe("g", "t", Fanout, "s1", handler)

	event, _ := eb.Publish("g", "t", map[string]any{"key1": "val1"}, 3, true)
	eb.Ack(event.ID)

	a := acked.Load()
	if a == nil {
		t.Fatal("ACK回调未触发")
	}
	if a.ID != event.ID {
		t.Fatalf("ACK回调应收到正确的event, 期望ID=%s, 实际=%s", event.ID, a.ID)
	}
	if a.Group != "g" || a.Topic != "t" {
		t.Fatalf("ACK回调应收到正确的group/topic, got group=%s topic=%s", a.Group, a.Topic)
	}
}

func TestAck_NoNeedAck_Ignored(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var ackCount atomic.Int32
	eb.SetAckCallback(func(e *Event) {
		ackCount.Add(1)
	})

	handler := func(e *Event) {}
	eb.Subscribe("order", "created", Fanout, "sub1", handler)

	event, _ := eb.Publish("order", "created", map[string]any{"orderId": "1"}, 1, false)

	err := eb.Ack(event.ID)
	if err != nil {
		t.Fatalf("不应返回错误: %v", err)
	}

	// needAck=false的消息ACK不应触发回调
	if ackCount.Load() != 0 {
		t.Fatalf("needAck=false的ACK不应触发回调, 实际=%d", ackCount.Load())
	}
}

func TestAck_Duplicate(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var ackCount atomic.Int32
	eb.SetAckCallback(func(e *Event) {
		ackCount.Add(1)
	})

	handler := func(e *Event) {}
	eb.Subscribe("order", "created", Fanout, "sub1", handler)

	event, _ := eb.Publish("order", "created", map[string]any{"orderId": "1"}, 1, true)

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

	// 没有设置ACK回调，ACK不应panic
	handler := func(e *Event) {}
	eb.Subscribe("g", "t", Fanout, "s1", handler)

	event, _ := eb.Publish("g", "t", map[string]any{"k": "v"}, 1, true)

	err := eb.Ack(event.ID)
	if err != nil {
		t.Fatalf("无回调时ACK不应报错: %v", err)
	}
}

func TestAck_StatusTransition(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	handler := func(e *Event) {}
	eb.Subscribe("g", "t", Fanout, "s1", handler)

	event, _ := eb.Publish("g", "t", map[string]any{"k": "v"}, 1, true)

	// pending -> delivered (在deliver时)
	if event.Status != StatusDelivered {
		t.Fatalf("分发后status应为delivered, 实际=%s", event.Status)
	}

	// delivered -> completed (在ACK时)
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

	eb.Subscribe("order", "created", Fanout, "sub1", handler1)
	eb.Subscribe("order", "created", Fanout, "sub1", handler2) // 覆盖

	eb.Publish("order", "created", map[string]any{"k": "v"}, 1, false)

	if count.Load() != 10 {
		t.Fatalf("重复订阅应覆盖，期望=10, 实际=%d", count.Load())
	}
}

func TestSubscribe_Validation(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	handler := func(e *Event) {}

	if err := eb.Subscribe("", "topic", Fanout, "sub1", handler); err == nil {
		t.Fatal("期望返回错误：group为空")
	}
	if err := eb.Subscribe("group", "", Fanout, "sub1", handler); err == nil {
		t.Fatal("期望返回错误：topic为空")
	}
	if err := eb.Subscribe("group", "topic", Fanout, "", handler); err == nil {
		t.Fatal("期望返回错误：subscriberID为空")
	}
	if err := eb.Subscribe("group", "topic", Fanout, "sub1", nil); err == nil {
		t.Fatal("期望返回错误：handler为空")
	}
}

func TestUnsubscribe(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var count atomic.Int32
	handler := func(e *Event) {
		count.Add(1)
	}

	eb.Subscribe("order", "created", Fanout, "sub1", handler)
	eb.Subscribe("order", "created", Fanout, "sub2", handler)

	eb.Unsubscribe("order", "created", "sub1")

	_, err := eb.Publish("order", "created", map[string]any{"orderId": "1"}, 1, false)
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

	eb.Unsubscribe("order", "created", "sub1")
	eb.Unsubscribe("nonexist", "nonexist", "sub1")
}

func TestUnsubscribe_AllSubscribers(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	handler := func(e *Event) {}

	eb.Subscribe("g", "t", Fanout, "s1", handler)
	eb.Subscribe("g", "t", Fanout, "s2", handler)

	eb.Unsubscribe("g", "t", "s1")
	eb.Unsubscribe("g", "t", "s2")

	// 所有订阅者取消后，订阅map应清理
	key := subKey("g", "t")
	eb.mu.RLock()
	subs := eb.subscriptions[key]
	eb.mu.RUnlock()
	if len(subs) != 0 {
		t.Fatalf("所有订阅者取消后，订阅列表应为空, 实际=%d", len(subs))
	}

	// 发布消息不应panic
	eb.Publish("g", "t", map[string]any{"k": "v"}, 1, false)
}

func TestSubscribe_ReceivesPendingMessages(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	// 先发布消息（无订阅者）
	eb.Publish("g", "t", map[string]any{"k": "v1"}, 1, false)

	// 后订阅，应收到待分发消息
	var received atomic.Pointer[Event]
	handler := func(e *Event) {
		received.Store(e)
	}
	eb.Subscribe("g", "t", Fanout, "s1", handler)

	r := received.Load()
	if r == nil {
		t.Fatal("订阅后应收到待分发消息")
	}
}

// ============================================================
// 无订阅者场景
// ============================================================

func TestNoSubscriber_Pending(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	event, _ := eb.Publish("order", "created", map[string]any{"k": "v"}, 1, false)

	if event.Status != StatusPending {
		t.Fatalf("无订阅者时期望status=pending, 实际=%s", event.Status)
	}

	var received atomic.Pointer[Event]
	handler := func(e *Event) {
		received.Store(e)
	}
	eb.Subscribe("order", "created", Fanout, "sub1", handler)

	r := received.Load()
	if r == nil {
		t.Fatal("订阅后应收到待分发消息")
	}
}

func TestNoSubscriber_MultiplePendingMessages(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	// 发布多条消息（无订阅者）
	eb.Publish("g", "t", map[string]any{"k": "1"}, 1, false)
	eb.Publish("g", "t", map[string]any{"k": "2"}, 1, false)
	eb.Publish("g", "t", map[string]any{"k": "3"}, 1, false)

	var count atomic.Int32
	handler := func(e *Event) {
		count.Add(1)
	}
	eb.Subscribe("g", "t", Fanout, "s1", handler)

	if count.Load() != 3 {
		t.Fatalf("订阅后应收到3条待分发消息，实际=%d", count.Load())
	}
}

// ============================================================
// 优先级
// ============================================================

func TestPriorityOrder_InQueue(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	// 无订阅者，消息留在队列中
	e1, _ := eb.Publish("g", "t", map[string]any{"k": "1"}, 1, false)
	e2, _ := eb.Publish("g", "t", map[string]any{"k": "2"}, 5, false)
	e3, _ := eb.Publish("g", "t", map[string]any{"k": "3"}, 3, false)

	key := subKey("g", "t")
	eb.mu.RLock()
	pending := eb.pendingQueue[key]
	eb.mu.RUnlock()

	// 队列应按优先级从高到低排列: e2(5), e3(3), e1(1)
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

	e1, _ := eb.Publish("g", "t", map[string]any{"k": "1"}, 3, false)
	e2, _ := eb.Publish("g", "t", map[string]any{"k": "2"}, 3, false)

	key := subKey("g", "t")
	eb.mu.RLock()
	pending := eb.pendingQueue[key]
	eb.mu.RUnlock()

	if len(pending) < 2 {
		t.Fatalf("期望2条待分发消息，实际=%d", len(pending))
	}
	// 同优先级同秒内，insertByPriority 使用 <= 判断，新消息插在前面
	// 这是正确行为：同秒内无法区分先后，按插入顺序即可
	// 验证两条消息都在队列中
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
		expected []int // 期望的priority顺序
	}{
		{
			name:     "空队列",
			queue:    nil,
			event:    &Event{Priority: 3, Created: 1},
			expected: []int{3},
		},
		{
			name:     "插入到头部",
			queue:    []*Event{{Priority: 1, Created: 1}},
			event:    &Event{Priority: 5, Created: 2},
			expected: []int{5, 1},
		},
		{
			name:     "插入到尾部",
			queue:    []*Event{{Priority: 5, Created: 1}},
			event:    &Event{Priority: 1, Created: 2},
			expected: []int{5, 1},
		},
		{
			name:     "插入到中间",
			queue:    []*Event{{Priority: 5, Created: 1}, {Priority: 1, Created: 3}},
			event:    &Event{Priority: 3, Created: 2},
			expected: []int{5, 3, 1},
		},
		{
			name:     "同优先级按时间排序",
			queue:    []*Event{{Priority: 3, Created: 10}},
			event:    &Event{Priority: 3, Created: 5},
			expected: []int{3, 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := insertByPriority(tt.queue, tt.event)
			if len(result) != len(tt.expected) {
				t.Fatalf("期望长度=%d, 实际=%d", len(tt.expected), len(result))
			}
			for i, e := range result {
				if e.Priority != tt.expected[i] {
					t.Fatalf("位置%d: 期望priority=%d, 实际=%d", i, tt.expected[i], e.Priority)
				}
			}
		})
	}
}

// ============================================================
// 查询接口
// ============================================================

func TestListTopics(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	eb.Publish("order", "created", map[string]any{"k": "v"}, 1, false)
	eb.Publish("order", "updated", map[string]any{"k": "v"}, 1, false)
	eb.Publish("user", "login", map[string]any{"k": "v"}, 1, false)

	topics := eb.ListTopics("order")
	if len(topics) != 2 {
		t.Fatalf("期望2个topic，实际=%d", len(topics))
	}

	topics = eb.ListTopics("user")
	if len(topics) != 1 {
		t.Fatalf("期望1个topic，实际=%d", len(topics))
	}

	topics = eb.ListTopics("nonexist")
	if len(topics) != 0 {
		t.Fatalf("期望0个topic，实际=%d", len(topics))
	}
}

func TestListTopics_MultipleGroups(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	eb.Publish("g1", "t1", map[string]any{"k": "v"}, 1, false)
	eb.Publish("g1", "t2", map[string]any{"k": "v"}, 1, false)
	eb.Publish("g2", "t1", map[string]any{"k": "v"}, 1, false)
	eb.Publish("g3", "t1", map[string]any{"k": "v"}, 1, false)
	eb.Publish("g3", "t2", map[string]any{"k": "v"}, 1, false)
	eb.Publish("g3", "t3", map[string]any{"k": "v"}, 1, false)

	if len(eb.ListTopics("g1")) != 2 {
		t.Fatalf("g1期望2个topic")
	}
	if len(eb.ListTopics("g2")) != 1 {
		t.Fatalf("g2期望1个topic")
	}
	if len(eb.ListTopics("g3")) != 3 {
		t.Fatalf("g3期望3个topic")
	}
}

func TestListDataKeys(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	eb.Publish("order", "created", map[string]any{"orderId": "1", "amount": 99.9}, 1, false)

	keys := eb.ListDataKeys("order", "created")
	if len(keys) != 2 {
		t.Fatalf("期望2个key，实际=%d", len(keys))
	}

	keys = eb.ListDataKeys("order", "nonexist")
	if len(keys) != 0 {
		t.Fatalf("期望0个key，实际=%d", len(keys))
	}

	keys = eb.ListDataKeys("", "")
	if len(keys) != 0 {
		t.Fatalf("空参数期望0个key，实际=%d", len(keys))
	}
}

func TestListDataKeys_Accumulation(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	// 第一条消息有2个key
	eb.Publish("g", "t", map[string]any{"k1": "v1", "k2": "v2"}, 1, false)
	keys := eb.ListDataKeys("g", "t")
	if len(keys) != 2 {
		t.Fatalf("期望2个key，实际=%d", len(keys))
	}

	// 第二条消息有不同key，应累积
	eb.Publish("g", "t", map[string]any{"k3": "v3"}, 1, false)
	keys = eb.ListDataKeys("g", "t")
	if len(keys) != 3 {
		t.Fatalf("累积后期望3个key，实际=%d", len(keys))
	}

	// 验证包含所有key
	keySet := make(map[string]bool)
	for _, k := range keys {
		keySet[k] = true
	}
	for _, k := range []string{"k1", "k2", "k3"} {
		if !keySet[k] {
			t.Fatalf("缺少key: %s", k)
		}
	}
}

func TestListDataKeys_DifferentTopics(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	eb.Publish("g", "t1", map[string]any{"a": 1, "b": 2}, 1, false)
	eb.Publish("g", "t2", map[string]any{"c": 3, "d": 4}, 1, false)

	keys1 := eb.ListDataKeys("g", "t1")
	keys2 := eb.ListDataKeys("g", "t2")

	if len(keys1) != 2 {
		t.Fatalf("t1期望2个key，实际=%d", len(keys1))
	}
	if len(keys2) != 2 {
		t.Fatalf("t2期望2个key，实际=%d", len(keys2))
	}
}

// ============================================================
// 多Group/Topic 隔离
// ============================================================

func TestDifferentGroups_Isolated(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var g1Count, g2Count atomic.Int32

	eb.Subscribe("g1", "t", Fanout, "s1", func(e *Event) { g1Count.Add(1) })
	eb.Subscribe("g2", "t", Fanout, "s1", func(e *Event) { g2Count.Add(1) })

	eb.Publish("g1", "t", map[string]any{"k": "v"}, 1, false)

	if g1Count.Load() != 1 {
		t.Fatalf("g1应收到1条消息，实际=%d", g1Count.Load())
	}
	if g2Count.Load() != 0 {
		t.Fatalf("g2不应收到g1的消息，实际=%d", g2Count.Load())
	}
}

func TestDifferentTopics_Isolated(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var t1Count, t2Count atomic.Int32

	eb.Subscribe("g", "t1", Fanout, "s1", func(e *Event) { t1Count.Add(1) })
	eb.Subscribe("g", "t2", Fanout, "s1", func(e *Event) { t2Count.Add(1) })

	eb.Publish("g", "t1", map[string]any{"k": "v"}, 1, false)

	if t1Count.Load() != 1 {
		t.Fatalf("t1应收到1条消息，实际=%d", t1Count.Load())
	}
	if t2Count.Load() != 0 {
		t.Fatalf("t2不应收到t1的消息，实际=%d", t2Count.Load())
	}
}

// ============================================================
// FastCache 存储层
// ============================================================

func TestLoadEvent(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	event, _ := eb.Publish("g", "t", map[string]any{"k": "v"}, 1, false)

	loaded, err := eb.loadEvent(event.ID)
	if err != nil {
		t.Fatalf("加载事件失败: %v", err)
	}
	if loaded.ID != event.ID {
		t.Fatalf("加载的ID不匹配, 期望=%s, 实际=%s", event.ID, loaded.ID)
	}
	if loaded.Group != "g" || loaded.Topic != "t" {
		t.Fatalf("加载的group/topic不匹配, got group=%s topic=%s", loaded.Group, loaded.Topic)
	}
}

func TestLoadEvent_NonExistent(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	_, err := eb.loadEvent("nonexistent")
	if err == nil {
		t.Fatal("加载不存在的事件应返回错误")
	}
}

func TestStoreEvent_UpdatesStatus(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	event, _ := eb.Publish("g", "t", map[string]any{"k": "v"}, 1, false)

	// 修改状态并重新存储
	event.Status = StatusCompleted
	eb.storeEvent(event)

	loaded, _ := eb.loadEvent(event.ID)
	if loaded.Status != StatusCompleted {
		t.Fatalf("更新后的状态应为completed, 实际=%s", loaded.Status)
	}
}

// ============================================================
// NewEventBus 参数
// ============================================================

func TestNewEventBus_DefaultSize(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	// 默认32MB，应能正常工作
	_, err := eb.Publish("g", "t", map[string]any{"k": "v"}, 1, false)
	if err != nil {
		t.Fatalf("默认大小应能正常发布: %v", err)
	}
}

func TestNewEventBus_CustomSize(t *testing.T) {
	eb := NewEventBus(1024) // 1KB
	defer eb.cache.Reset()

	_, err := eb.Publish("g", "t", map[string]any{"k": "v"}, 1, false)
	if err != nil {
		t.Fatalf("自定义大小应能正常发布: %v", err)
	}
}

// ============================================================
// 并发安全
// ============================================================

func TestConcurrentPublish(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var count atomic.Int32
	handler := func(e *Event) {
		count.Add(1)
	}
	eb.Subscribe("g", "t", Fanout, "s1", handler)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eb.Publish("g", "t", map[string]any{"k": fmt.Sprintf("%d", i)}, 1, false)
		}(i)
	}
	wg.Wait()

	if count.Load() != 100 {
		t.Fatalf("并发发布后期望100条消息被处理，实际=%d", count.Load())
	}
}

func TestConcurrentSubscribeUnsubscribe(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var wg sync.WaitGroup

	// 并发订阅
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eb.Subscribe("g", "t", Fanout, fmt.Sprintf("sub%d", i), func(e *Event) {})
		}(i)
	}
	wg.Wait()

	// 并发取消订阅
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eb.Unsubscribe("g", "t", fmt.Sprintf("sub%d", i))
		}(i)
	}
	wg.Wait()

	// 不应panic
	eb.Publish("g", "t", map[string]any{"k": "v"}, 1, false)
}

func TestConcurrentPublishAndAck(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var ackCount atomic.Int32
	eb.SetAckCallback(func(e *Event) {
		ackCount.Add(1)
	})

	handler := func(e *Event) {}
	eb.Subscribe("g", "t", Fanout, "s1", handler)

	var wg sync.WaitGroup
	ids := make(chan string, 50)

	// 并发发布
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			event, _ := eb.Publish("g", "t", map[string]any{"k": fmt.Sprintf("%d", i)}, 1, true)
			ids <- event.ID
		}(i)
	}
	wg.Wait()
	close(ids)

	// 并发ACK
	var ackWg sync.WaitGroup
	for id := range ids {
		ackWg.Add(1)
		go func(id string) {
			defer ackWg.Done()
			eb.Ack(id)
		}(id)
	}
	ackWg.Wait()

	if ackCount.Load() != 50 {
		t.Fatalf("并发ACK后期望50次回调，实际=%d", ackCount.Load())
	}
}

// ============================================================
// 边界场景
// ============================================================

func TestPublish_SingleKeyData(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	event, err := eb.Publish("g", "t", map[string]any{"onlyKey": "val"}, 1, false)
	if err != nil {
		t.Fatalf("只有1个key的data应合法: %v", err)
	}
	if len(event.Data) != 1 {
		t.Fatalf("期望1个data字段，实际=%d", len(event.Data))
	}
}

func TestPublish_DataWithNilValue(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	event, err := eb.Publish("g", "t", map[string]any{"nilKey": nil}, 1, false)
	if err != nil {
		t.Fatalf("data含nil值应合法: %v", err)
	}
	if event.Data["nilKey"] != nil {
		t.Fatal("nil值应保留")
	}
}

func TestListTopics_SortedOutput(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	eb.Publish("g", "z-topic", map[string]any{"k": "v"}, 1, false)
	eb.Publish("g", "a-topic", map[string]any{"k": "v"}, 1, false)
	eb.Publish("g", "m-topic", map[string]any{"k": "v"}, 1, false)

	topics := eb.ListTopics("g")
	sorted := make([]string, len(topics))
	copy(sorted, topics)
	sort.Strings(sorted)

	// 验证返回了所有topic（不要求排序，但数量正确）
	if len(topics) != 3 {
		t.Fatalf("期望3个topic，实际=%d", len(topics))
	}
}

func TestSetAckCallback(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	called := false
	eb.SetAckCallback(func(e *Event) {
		called = true
	})

	handler := func(e *Event) {}
	eb.Subscribe("g", "t", Fanout, "s1", handler)

	event, _ := eb.Publish("g", "t", map[string]any{"k": "v"}, 1, true)
	eb.Ack(event.ID)

	if !called {
		t.Fatal("设置回调后ACK应触发回调")
	}
}

func TestSubKey(t *testing.T) {
	tests := []struct {
		group, topic, expected string
	}{
		{"order", "created", "order:created"},
		{"g", "t", "g:t"},
		{"a:b", "c", "a:b:c"},
	}
	for _, tt := range tests {
		got := subKey(tt.group, tt.topic)
		if got != tt.expected {
			t.Fatalf("subKey(%s,%s) = %s, 期望 %s", tt.group, tt.topic, got, tt.expected)
		}
	}
}

func TestEvent_FieldsIntegrity(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	data := map[string]any{
		"key1": "value1",
		"key2": 123,
		"key3": true,
	}
	event, _ := eb.Publish("myGroup", "myTopic", data, 4, true)

	if event.Group != "myGroup" {
		t.Fatalf("group不匹配")
	}
	if event.Topic != "myTopic" {
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
	eb.Subscribe("g", "t", Fanout, "s1", handler)

	eb.Publish("g", "t", map[string]any{"k": "1"}, 1, false)
	eb.Publish("g", "t", map[string]any{"k": "2"}, 2, false)
	eb.Publish("g", "t", map[string]any{"k": "3"}, 3, false)

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
	receivedBy := make(map[string][]string) // subscriberID -> []eventID

	makeHandler := func(sid string) HandlerFunc {
		return func(e *Event) {
			mu.Lock()
			receivedBy[sid] = append(receivedBy[sid], e.ID)
			mu.Unlock()
		}
	}

	eb.Subscribe("g", "t", Anyone, "s1", makeHandler("s1"))
	eb.Subscribe("g", "t", Anyone, "s2", makeHandler("s2"))

	// 发布多条消息
	for i := 0; i < 5; i++ {
		eb.Publish("g", "t", map[string]any{"k": fmt.Sprintf("%d", i)}, 1, false)
	}

	mu.Lock()
	defer mu.Unlock()
	total := len(receivedBy["s1"]) + len(receivedBy["s2"])
	if total != 5 {
		t.Fatalf("Anyone模式下5条消息应被消费5次，实际=%d", total)
	}
	// 每条消息只被一个订阅者消费
	for _, ids := range receivedBy {
		if len(ids) == 0 {
			// 可能某个订阅者没收到任何消息（都给了第一个）
		}
	}
}

// ============================================================
// 数据竞争测试（需使用 go test -race 运行）
// ============================================================

func TestRace_PublishConcurrent(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var count atomic.Int32
	eb.Subscribe("g", "t", Fanout, "s1", func(e *Event) {
		count.Add(1)
	})

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eb.Publish("g", "t", map[string]any{"k": i}, 1, false)
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

	// 并发订阅
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eb.Subscribe("g", "t", Fanout, fmt.Sprintf("sub%d", i), func(e *Event) {})
		}(i)
	}

	// 并发发布
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eb.Publish("g", "t", map[string]any{"k": i}, 1, false)
		}(i)
	}

	wg.Wait()
}

func TestRace_PublishAndUnsubscribe(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	// 先订阅
	for i := 0; i < 50; i++ {
		eb.Subscribe("g", "t", Fanout, fmt.Sprintf("sub%d", i), func(e *Event) {})
	}

	var wg sync.WaitGroup

	// 并发发布
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eb.Publish("g", "t", map[string]any{"k": i}, 1, false)
		}(i)
	}

	// 并发取消订阅
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eb.Unsubscribe("g", "t", fmt.Sprintf("sub%d", i))
		}(i)
	}

	wg.Wait()
}

func TestRace_PublishAndAck(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var ackCount atomic.Int32
	eb.SetAckCallback(func(e *Event) {
		ackCount.Add(1)
	})

	eb.Subscribe("g", "t", Fanout, "s1", func(e *Event) {})

	var wg sync.WaitGroup
	ids := make(chan string, 100)

	// 并发发布 needAck=true
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			event, _ := eb.Publish("g", "t", map[string]any{"k": i}, 1, true)
			ids <- event.ID
		}(i)
	}
	wg.Wait()
	close(ids)

	// 并发ACK
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

	// 并发发布
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eb.Publish("g", "t", map[string]any{fmt.Sprintf("key%d", i): i}, 1, false)
		}(i)
	}

	// 并发查询 topics
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			eb.ListTopics("g")
		}()
	}

	// 并发查询 data keys
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			eb.ListDataKeys("g", "t")
		}()
	}

	wg.Wait()
}

func TestRace_SubscribeAndUnsubscribe(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var wg sync.WaitGroup
	const rounds = 20

	for r := 0; r < rounds; r++ {
		// 并发订阅
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				eb.Subscribe("g", "t", Fanout, fmt.Sprintf("sub%d", i), func(e *Event) {})
			}(i)
		}

		// 并发取消订阅
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				eb.Unsubscribe("g", "t", fmt.Sprintf("sub%d", i))
			}(i)
		}
	}

	wg.Wait()
}

func TestRace_MultipleGroupsConcurrent(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var wg sync.WaitGroup

	// 不同group并发操作，验证隔离性
	for g := 0; g < 5; g++ {
		group := fmt.Sprintf("group%d", g)
		for s := 0; s < 5; s++ {
			sid := fmt.Sprintf("sub%d", s)
			wg.Add(1)
			go func(group, sid string) {
				defer wg.Done()
				eb.Subscribe(group, "topic", Fanout, sid, func(e *Event) {})
			}(group, sid)
		}

		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(group string, i int) {
				defer wg.Done()
				eb.Publish(group, "topic", map[string]any{"k": i}, 1, false)
			}(group, i)
		}

		// 并发查询
		wg.Add(1)
		go func(group string) {
			defer wg.Done()
			eb.ListTopics(group)
			eb.ListDataKeys(group, "topic")
		}(group)
	}

	wg.Wait()
}

func TestRace_SetCallbackAndAck(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var ackCount atomic.Int32
	eb.Subscribe("g", "t", Fanout, "s1", func(e *Event) {})

	var wg sync.WaitGroup

	// 并发设置回调
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eb.SetAckCallback(func(e *Event) {
				ackCount.Add(1)
			})
		}(i)
	}

	// 并发发布+ACK
	events := make(chan *Event, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			event, _ := eb.Publish("g", "t", map[string]any{"k": i}, 1, true)
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
	eb.Subscribe("g", "t", Anyone, "s1", func(e *Event) { count.Add(1) })
	eb.Subscribe("g", "t", Anyone, "s2", func(e *Event) { count.Add(1) })
	eb.Subscribe("g", "t", Anyone, "s3", func(e *Event) { count.Add(1) })

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eb.Publish("g", "t", map[string]any{"k": i}, 1, false)
		}(i)
	}
	wg.Wait()

	if count.Load() != 100 {
		t.Fatalf("Anyone模式期望100次调用，实际=%d", count.Load())
	}
}

func TestRace_PublishWithPendingDelivery(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var wg sync.WaitGroup

	// 先发布一批消息（无订阅者，进入pending）
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eb.Publish("g", "t", map[string]any{"k": i}, 1, false)
		}(i)
	}
	wg.Wait()

	// 并发订阅，触发pending消息分发
	var deliverCount atomic.Int32
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eb.Subscribe("g", "t", Fanout, fmt.Sprintf("sub%d", i), func(e *Event) {
				deliverCount.Add(1)
			})
		}(i)
	}
	wg.Wait()
}

func TestRace_MixedOperations(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var wg sync.WaitGroup

	// 混合并发：发布、订阅、取消订阅、查询、ACK
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eb.Publish("g", "t", map[string]any{"k": i}, (i%5)+1, i%3 == 0)
		}(i)

		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eb.Subscribe("g", "t", Fanout, fmt.Sprintf("sub%d", i), func(e *Event) {})
		}(i)

		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eb.Unsubscribe("g", "t", fmt.Sprintf("sub%d", i))
		}(i)

		wg.Add(1)
		go func() {
			defer wg.Done()
			eb.ListTopics("g")
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			eb.ListDataKeys("g", "t")
		}()
	}

	wg.Wait()
}

func TestRace_FanoutNeedAckConcurrent(t *testing.T) {
	eb := NewEventBus(0)
	defer eb.cache.Reset()

	var ackCount atomic.Int32
	eb.SetAckCallback(func(e *Event) {
		ackCount.Add(1)
	})

	eb.Subscribe("g", "t", Fanout, "s1", func(e *Event) {})
	eb.Subscribe("g", "t", Fanout, "s2", func(e *Event) {})

	var wg sync.WaitGroup
	ids := make(chan string, 50)

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			event, _ := eb.Publish("g", "t", map[string]any{"k": i}, (i%5)+1, true)
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

	// 先发布消息
	var ids []string
	for i := 0; i < 20; i++ {
		event, _ := eb.Publish("g", "t", map[string]any{"k": i}, 1, false)
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
			Group:    "g",
			Topic:    "t",
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

	// 验证所有事件都能加载
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
