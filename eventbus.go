package gobus

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/fxamacker/cbor/v2"
)

// 送达模式
type DeliverMode int

const (
	Fanout DeliverMode = iota // 全量分发：所有订阅者都收到
	Anyone                    // 竞争消费：任意一个订阅者收到即可
)

// 消息状态
type Status string

const (
	StatusPending   Status = "pending"
	StatusDelivered Status = "delivered"
	StatusCompleted Status = "completed"
)

// Event 事件/消息
type Event struct {
	ID       string         `cbor:"id"`
	Topic    string         `cbor:"topic"`
	Data     map[string]any `cbor:"data"`
	Status   Status         `cbor:"status"`
	Priority int            `cbor:"priority"`
	NeedAck  bool           `cbor:"needAck"`
	Created  int64          `cbor:"created"`
}

// Subscription 订阅关系
type Subscription struct {
	Topic        string
	Mode         DeliverMode
	SubscriberID string
	Handler      HandlerFunc
}

// HandlerFunc 消息处理函数
type HandlerFunc func(event *Event)

// AckCallback ACK通知回调，当needAck=true且消息完成时调用
type AckCallback func(event *Event)

// EventBus 事件总线
//
// Topic 使用前缀分组，例如 "db:create"、"db:insert"、"cache:set"。
// 前缀部分（冒号前的部分）相当于原来的 group，支持多层前缀如 "db:primary:create"。
type EventBus struct {
	cache *fastcache.Cache
	mu    sync.RWMutex

	// 订阅者管理：key = topic -> []*Subscription
	subscriptions map[string][]*Subscription

	// 待分发消息队列：key = topic -> []*Event（按优先级排序）
	pendingQueue map[string][]*Event

	// 消息索引：key = messageID -> *Event（sync.Map 自身线程安全，无需加锁）
	events sync.Map

	// 所有已注册的 topic 集合（用于 ListTopics 前缀搜索）
	topics map[string]struct{}

	// topic -> data keys（记录每个 topic 下 data 中出现过的 key）
	topicDataKeys map[string]map[string]struct{}

	// 消息ID自增（atomic 自身线程安全，无需加锁）
	msgIDCounter atomic.Int64

	// ACK回调（atomic.Value 线程安全，避免 SetAckCallback 与 Ack 并发竞争）
	ackCallback atomic.Value
}

// NewEventBus 创建事件总线
func NewEventBus(maxBytes int) *EventBus {
	if maxBytes <= 0 {
		maxBytes = 32 * 1024 * 1024 // 默认32MB
	}
	return &EventBus{
		cache:         fastcache.New(maxBytes),
		subscriptions: make(map[string][]*Subscription),
		pendingQueue:  make(map[string][]*Event),
		topics:        make(map[string]struct{}),
		topicDataKeys: make(map[string]map[string]struct{}),
	}
}

// SetAckCallback 设置ACK回调（线程安全）
func (eb *EventBus) SetAckCallback(cb AckCallback) {
	eb.ackCallback.Store(cb)
}

// getAckCallback 获取ACK回调
func (eb *EventBus) getAckCallback() AckCallback {
	val := eb.ackCallback.Load()
	if val == nil {
		return nil
	}
	return val.(AckCallback)
}

// Publish 发布消息到指定 topic。
//
// 如果该 topic 没有任何订阅者，执行空操作（no-op），不存储、不入队，返回 nil。
func (eb *EventBus) Publish(topic string, data map[string]any, priority int, needAck bool) (*Event, error) {
	if topic == "" {
		return nil, fmt.Errorf("topic 不能为空")
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("data 不能为空")
	}
	if priority < 1 || priority > 5 {
		return nil, fmt.Errorf("priority 必须在 1~5 范围内，当前值: %d", priority)
	}

	// 先检查是否有订阅者，无订阅者则 no-op
	subs := eb.getSubscriptions(topic)
	if len(subs) == 0 {
		return nil, nil
	}

	id := fmt.Sprintf("%d", eb.msgIDCounter.Add(1))
	event := &Event{
		ID:       id,
		Topic:    topic,
		Data:     data,
		Status:   StatusPending,
		Priority: priority,
		NeedAck:  needAck,
		Created:  time.Now().Unix(),
	}

	// sync.Map 线程安全，无需加锁
	eb.events.Store(id, event)

	// 更新内存索引 + 入队 + 取订阅者快照（Go map 非线程安全，需加锁）
	eb.mu.Lock()
	eb.topics[topic] = struct{}{}

	if eb.topicDataKeys[topic] == nil {
		eb.topicDataKeys[topic] = make(map[string]struct{})
	}
	for k := range data {
		eb.topicDataKeys[topic][k] = struct{}{}
	}

	eb.pendingQueue[topic] = insertByPriority(eb.pendingQueue[topic], event)
	subsCopy := make([]*Subscription, len(subs))
	copy(subsCopy, subs)
	eb.mu.Unlock()

	if len(subsCopy) > 0 {
		eb.deliver(event, subsCopy)
	}

	return event, nil
}

// getSubscriptions 获取 topic 的订阅者快照（读锁保护）
func (eb *EventBus) getSubscriptions(topic string) []*Subscription {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	subs := eb.subscriptions[topic]
	if len(subs) == 0 {
		return nil
	}
	result := make([]*Subscription, len(subs))
	copy(result, subs)
	return result
}

// Subscribe 订阅消息。
//
// 订阅后如果有待分发消息，会立即触发分发。
func (eb *EventBus) Subscribe(topic string, mode DeliverMode, subscriberID string, handler HandlerFunc) error {
	if topic == "" {
		return fmt.Errorf("topic 不能为空")
	}
	if subscriberID == "" {
		return fmt.Errorf("subscriberID 不能为空")
	}
	if handler == nil {
		return fmt.Errorf("handler 不能为空")
	}

	sub := &Subscription{
		Topic:        topic,
		Mode:         mode,
		SubscriberID: subscriberID,
		Handler:      handler,
	}

	eb.mu.Lock()
	defer eb.mu.Unlock()

	eb.topics[topic] = struct{}{}

	subs := eb.subscriptions[topic]
	for i, s := range subs {
		if s.SubscriberID == subscriberID {
			eb.subscriptions[topic][i] = sub
			return nil
		}
	}
	eb.subscriptions[topic] = append(eb.subscriptions[topic], sub)

	// 如果有待分发消息，立即尝试分发
	if pending := eb.pendingQueue[topic]; len(pending) > 0 {
		pendingCopy := make([]*Event, len(pending))
		copy(pendingCopy, pending)
		subsCopy := make([]*Subscription, len(eb.subscriptions[topic]))
		copy(subsCopy, eb.subscriptions[topic])

		eb.mu.Unlock()
		for _, evt := range pendingCopy {
			eb.deliver(evt, subsCopy)
		}
		eb.mu.Lock()
	}

	return nil
}

// Unsubscribe 取消订阅
func (eb *EventBus) Unsubscribe(topic, subscriberID string) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	subs := eb.subscriptions[topic]
	for i, s := range subs {
		if s.SubscriberID == subscriberID {
			eb.subscriptions[topic] = append(subs[:i], subs[i+1:]...)
			break
		}
	}
	if len(eb.subscriptions[topic]) == 0 {
		delete(eb.subscriptions, topic)
	}
}

// Ack 确认消息
func (eb *EventBus) Ack(messageID string) error {
	val, ok := eb.events.Load(messageID)
	if !ok {
		return fmt.Errorf("消息不存在: %s", messageID)
	}

	event := val.(*Event)

	if !event.NeedAck {
		return nil
	}

	// 一次加锁完成 Status 更新 + pending 移除 + 持久化
	eb.mu.Lock()
	if event.Status == StatusCompleted {
		eb.mu.Unlock()
		return nil
	}
	event.Status = StatusCompleted
	eb.removePendingLocked(event)
	data := eb.marshalEvent(event)
	eb.mu.Unlock()
	eb.cache.Set([]byte("event:"+event.ID), data)

	// ackCallback 通过 atomic.Value 读取，线程安全
	if cb := eb.getAckCallback(); cb != nil {
		cb(event)
	}

	return nil
}

// ListTopics 按前缀搜索 topic 列表。
//
// prefix 为空时返回所有已注册的 topic。
// 例如：ListTopics("db") 返回 ["db:create", "db:insert"]
func (eb *EventBus) ListTopics(prefix string) []string {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	var result []string
	for t := range eb.topics {
		if prefix == "" || strings.HasPrefix(t, prefix) {
			result = append(result, t)
		}
	}
	return result
}

// ListDataKeys 查询 topic 下 data 的 key 列表。
//
// 返回该 topic 所有已发布消息的 data 字段中出现过的不重复 key。
func (eb *EventBus) ListDataKeys(topic string) []string {
	if topic == "" {
		return []string{}
	}

	eb.mu.RLock()
	defer eb.mu.RUnlock()

	keys, ok := eb.topicDataKeys[topic]
	if !ok {
		return []string{}
	}

	result := make([]string, 0, len(keys))
	for k := range keys {
		result = append(result, k)
	}
	return result
}

// deliver 分发消息给订阅者
func (eb *EventBus) deliver(event *Event, subs []*Subscription) {
	if len(subs) == 0 {
		return
	}

	mode := subs[0].Mode

	eb.mu.Lock()
	event.Status = StatusDelivered
	data := eb.marshalEvent(event)
	eb.mu.Unlock()
	eb.cache.Set([]byte("event:"+event.ID), data)

	handlers := make([]HandlerFunc, 0, len(subs))
	switch mode {
	case Fanout:
		for _, sub := range subs {
			handlers = append(handlers, sub.Handler)
		}
	case Anyone:
		handlers = append(handlers, subs[0].Handler)
	}

	for _, h := range handlers {
		h(event)
	}

	if !event.NeedAck {
		eb.mu.Lock()
		event.Status = StatusCompleted
		eb.removePendingLocked(event)
		data := eb.marshalEvent(event)
		eb.mu.Unlock()
		eb.cache.Set([]byte("event:"+event.ID), data)
	}
}

// removePendingLocked 从待分发队列移除消息，调用方必须已持有 eb.mu
func (eb *EventBus) removePendingLocked(event *Event) {
	pending := eb.pendingQueue[event.Topic]
	for i, e := range pending {
		if e.ID == event.ID {
			eb.pendingQueue[event.Topic] = append(pending[:i], pending[i+1:]...)
			return
		}
	}
}

// marshalEvent 序列化事件（调用方需持有锁或独占event）
func (eb *EventBus) marshalEvent(event *Event) []byte {
	data, _ := cbor.Marshal(event)
	return data
}

func (eb *EventBus) storeEvent(event *Event) error {
	data, err := cbor.Marshal(event)
	if err != nil {
		return err
	}
	eb.cache.Set([]byte("event:"+event.ID), data)
	return nil
}

func (eb *EventBus) loadEvent(id string) (*Event, error) {
	data := eb.cache.Get(nil, []byte("event:"+id))
	if len(data) == 0 {
		return nil, fmt.Errorf("消息不存在: %s", id)
	}
	var event Event
	if err := cbor.Unmarshal(data, &event); err != nil {
		return nil, err
	}
	return &event, nil
}

// insertByPriority 按优先级插入（高优先级在前，同优先级按创建时间排序）
func insertByPriority(queue []*Event, event *Event) []*Event {
	result := make([]*Event, 0, len(queue)+1)
	inserted := false
	for _, e := range queue {
		if !inserted && event.Priority > e.Priority {
			result = append(result, event)
			inserted = true
		} else if !inserted && event.Priority == e.Priority && event.Created <= e.Created {
			result = append(result, event)
			inserted = true
		}
		result = append(result, e)
	}
	if !inserted {
		result = append(result, event)
	}
	return result
}
