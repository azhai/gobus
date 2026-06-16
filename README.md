# gobus

基于 FastCache 的事件总线，支持发布-订阅模式，提供 Fanout（全量分发）和 Anyone（竞争消费）两种送达模式。使用 CBOR (RFC 8949) 进行高效二进制序列化。

## 特性

- **Topic 前缀分组**：使用冒号分隔的多层前缀（如 `db:create`、`cache:primary:set`）实现分组管理
- **无订阅者优化**：发布到无订阅者的 Topic 时执行空操作（no-op），不存储不入队
- **前缀搜索**：支持按前缀查询 Topic 列表（如 `ListTopics("db")` 返回所有 `db:*` 的 Topic）
- **数据 Key 追踪**：自动记录每个 Topic 下 data 中的 key 列表
- **优先级队列**：消息按优先级 1~5 排序，同优先级按创建时间排序

## 安装

```bash
go get gobus
```

## 快速开始

```go
package main

import (
    "fmt"
    "gobus"
)

func main() {
    // 创建事件总线（默认32MB缓存）
    bus := gobus.NewEventBus(0)

    // 订阅消息 - 使用 Topic 前缀分组（如 "order:created"）
    bus.Subscribe("order:created", gobus.Fanout, "logger", func(e *gobus.Event) {
        fmt.Printf("[logger] 收到订单: %v\n", e.Data["orderId"])
    })

    bus.Subscribe("order:created", gobus.Fanout, "notifier", func(e *gobus.Event) {
        fmt.Printf("[notifier] 发送通知: %v\n", e.Data["orderId"])
    })

    // 发布消息到 "order:created" topic
    event, err := bus.Publish("order:created", map[string]any{
        "orderId": "ORD-001",
        "amount":  99.9,
    }, 1, false)

    if event != nil {
        fmt.Printf("消息ID: %s, 状态: %s\n", event.ID, event.Status)
    }
}
```

## Topic 前缀分组

使用冒号 `:` 分隔的多层前缀来组织 Topic，替代传统的 Group 字段：

```
db:create          # 数据库创建操作
db:insert          # 数据库插入操作
db:primary:create  # 主库创建操作（多层前缀）
cache:set          # 缓存设置操作
cache:primary:set  # 主缓存设置操作
```

### 前缀搜索

```go
bus := gobus.NewEventBus(0)

bus.Subscribe("db:create", gobus.Fanout, "s1", func(e *gobus.Event) {})
bus.Subscribe("db:insert", gobus.Fanout, "s2", func(e *gobus.Event) {})
bus.Subscribe("db:update", gobus.Fanout, "s3", func(e *gobus.Event) {})
bus.Subscribe("cache:set", gobus.Fanout, "s4", func(e *gobus.Event) {})

// 发布消息后查询
dbTopics := bus.ListTopics("db")
// ["db:create", "db:insert", "db:update"]

allTopics := bus.ListTopics("")
// ["cache:set", "db:create", "db:insert", "db:update"]
```

## 无订阅者 No-Op

当发布的 Topic 没有任何订阅者时，系统会执行空操作（no-op），不存储消息、不进入队列，直接返回 nil：

```go
bus := gobus.NewEventBus(0)

// "unknown:topic" 没有订阅者
event, err := bus.Publish("unknown:topic", map[string]any{"k": "v"}, 1, false)
// event == nil, err == nil（不报错，也不存储）

// 验证：不会产生任何数据残留
topics := bus.ListTopics("")  // []
keys := bus.ListDataKeys("unknown:topic")  // []
```

**优势**：
- 避免无效消息堆积
- 减少内存占用
- 提高发布性能

## 送达模式

### Fanout - 全量分发

所有订阅者都会收到消息。

```go
bus := gobus.NewEventBus(0)

bus.Subscribe("order:created", gobus.Fanout, "service-a", func(e *gobus.Event) {
    fmt.Println("service-a 收到")
})
bus.Subscribe("order:created", gobus.Fanout, "service-b", func(e *gobus.Event) {
    fmt.Println("service-b 收到")
})
bus.Subscribe("order:created", gobus.Fanout, "service-c", func(e *gobus.Event) {
    fmt.Println("service-c 收到")
})

bus.Publish("order:created", map[string]any{"orderId": "1"}, 1, false)
// 输出:
// service-a 收到
// service-b 收到
// service-c 收到
```

### Anyone - 竞争消费

仅一个订阅者收到消息，适合任务分发场景。

```go
bus := gobus.NewEventBus(0)

bus.Subscribe("task:process", gobus.Anyone, "worker-1", func(e *gobus.Event) {
    fmt.Println("worker-1 处理任务")
})
bus.Subscribe("task:process", gobus.Anyone, "worker-2", func(e *gobus.Event) {
    fmt.Println("worker-2 处理任务")
})

bus.Publish("task:process", map[string]any{"taskId": "T-001"}, 1, false)
// 输出（仅其中一个）:
// worker-1 处理任务
```

## ACK 回执

当 `needAck=true` 时，订阅者处理完成后需发送 ACK，事件总线会通知发布方。

```go
bus := gobus.NewEventBus(0)

// 设置 ACK 回调，消息完成时通知发布方
bus.SetAckCallback(func(e *gobus.Event) {
    fmt.Printf("消息 %s 已完成处理\n", e.ID)
})

bus.Subscribe("order:created", gobus.Fanout, "processor", func(e *gobus.Event) {
    fmt.Printf("处理订单: %v\n", e.Data["orderId"])
    // 处理完成后发送 ACK
    bus.Ack(e.ID)
})

// 发布消息，needAck=true
event, _ := bus.Publish("order:created", map[string]any{
    "orderId": "ORD-002",
}, 1, true)

fmt.Printf("状态: %s\n", event.Status) // delivered
// ACK 后状态变为 completed
```

## 优先级

优先级范围 1~5，数值越大优先级越高，默认为 1。消息按优先级从高到低分发。

```go
bus := gobus.NewEventBus(0)

bus.Subscribe("task:run", gobus.Fanout, "worker", func(e *gobus.Event) {
    fmt.Printf("执行: priority=%d, data=%v\n", e.Priority, e.Data)
})

// 低优先级
bus.Publish("task:run", map[string]any{"name": "普通任务"}, 1, false)
// 高优先级
bus.Publish("task:run", map[string]any{"name": "紧急任务"}, 5, false)
// 中优先级
bus.Publish("task:run", map[string]any{"name": "一般任务"}, 3, false)
// 分发顺序: 紧急任务(5) → 一般任务(3) → 普通任务(1)
```

## 取消订阅

```go
bus := gobus.NewEventBus(0)

bus.Subscribe("order:created", gobus.Fanout, "my-service", handler)

// 取消订阅
bus.Unsubscribe("order:created", "my-service")
```

## 查询接口

### ListTopics - 按前缀搜索 Topic 列表

返回所有以指定前缀开头的 Topic，空字符串返回全部。

```go
bus := gobus.NewEventBus(0)

bus.Publish("db:create", map[string]any{"table": "users"}, 1, false)
bus.Publish("db:insert", map[string]any{"table": "orders"}, 1, false)
bus.Publish("cache:set", map[string]any{"key": "session"}, 1, false)

// 搜索 db 前缀的所有 topic
dbTopics := bus.ListTopics("db")
// ["db:create", "db:insert"]

// 搜索全部 topic
allTopics := bus.ListTopics("")
// ["cache:set", "db:create", "db:insert"]
```

### ListDataKeys - 查询 Topic 下 data 的 key 列表

返回指定 Topic 中已发布消息的 data 字段包含的所有 key。

```go
bus := gobus.NewEventBus(0)

bus.Publish("order:created", map[string]any{
    "orderId": "ORD-001",
    "amount":  99.9,
}, 1, false)

keys := bus.ListDataKeys("order:created")
// ["amount", "orderId"]（顺序可能不同）
```

## 消息状态流转

```
pending → delivered → completed
```

- **pending**：消息已创建，等待分发
- **delivered**：消息已推送给订阅者
- **completed**：消息已确认完成（Anyone+无ACK自动完成，或ACK后完成）

## 技术栈

| 组件 | 说明 |
|------|------|
| [FastCache](https://github.com/VictoriaMetrics/fastcache) | 高性能内存缓存，线程安全 |
| [CBOR](https://github.com/fxamacker/cbor) | RFC 8949 二进制序列化，比JSON更快更小 |

## 并发安全

所有公开方法均为并发安全：

- `Publish` / `Subscribe` / `Unsubscribe` / `Ack` — 线程安全
- `ListTopics` / `ListDataKeys` — 线程安全，使用读锁
- `SetAckCallback` — 使用 `atomic.Value`，无锁
- `msgIDCounter` — 使用 `atomic.Int64`，无锁
- `events` 索引 — 使用 `sync.Map`，无锁
- FastCache / CBOR — 自身并发安全

## Event 结构

| 字段 | 类型 | 说明 |
|------|------|------|
| ID | string | 消息唯一ID，系统自动生成 |
| Topic | string | 主题标识（必填），支持前缀分组如 `db:create` |
| Data | map[string]any | 负载数据（必填，至少一个key） |
| Status | Status | 消息状态：Pending/Delivered/Completed |
| Priority | int | 优先级 1~5，默认1，越大越优先 |
| NeedAck | bool | 是否需要ACK回执，默认false |
| Created | int64 | 创建时间戳（Unix秒级），系统自动生成 |

## API 参考

### 核心方法

| 方法 | 签名 | 说明 |
|------|------|------|
| NewEventBus | `NewEventBus(cacheSizeMB int) *EventBus` | 创建事件总线实例 |
| Subscribe | `Subscribe(topic string, mode DeliveryMode, subID string, handler Handler)` | 订阅 Topic |
| Unsubscribe | `Unsubscribe(topic string, subID string)` | 取消订阅 |
| Publish | `Publish(topic string, data map[string]any, priority int, needAck bool) (*Event, error)` | 发布消息（无订阅者返回nil） |
| Ack | `Ack(eventID string)` | 发送ACK回执 |
| SetAckCallback | `SetAckCallback(callback func(*Event))` | 设置ACK回调 |

### 查询方法

| 方法 | 签名 | 说明 |
|------|------|------|
| ListTopics | `ListTopics(prefix string) []string` | 按前缀搜索Topic列表 |
| ListDataKeys | `ListDataKeys(topic string) []string` | 查询Topic下data的key列表 |

### 常量

| 常量 | 值 | 说明 |
|------|-----|------|
| Fanout | - | 全量分发模式 |
| Anyone | - | 竞争消费模式 |

## 性能基准测试

基于 Apple M4 (ARM64) 的基准测试结果（`go test -bench=. -benchmem -benchtime=1s`）：

### 核心操作性能

| 测试场景 | 吞吐量 (ops/s) | 延迟 (ns/op) | 内存分配 (B/op) |
|---------|---------------|-------------|----------------|
| **Publish (Fanout, 3 subscribers)** | ~765K | 1306 | 570 |
| **Publish (Anyone, 2 workers)** | ~868K | 1152 | 562 |
| **Publish (No-Op, 无订阅者)** | **~199M** | **5.019** | **0** |
| **Publish (With ACK)** | ~870K | 1150 | 515 |
| **Publish (Priority Queue)** | ~862K | 1160 | 549 |
| **Publish (Large Data, 20+ fields)** | ~240K | 4171 | 1270 |
| **Publish (100 Subscribers)** | ~531K | 1881 | 3194 |

### 查询操作性能

| 测试场景 | 吞吐量 (ops/s) | 延迟 (ns/op) | 内存分配 (B/op) |
|---------|---------------|-------------|----------------|
| **ListTopics (prefix, 1000 topics)** | ~117K | 8575 | 4416 |
| **ListTopics (all, 1000 topics)** | ~84K | 11902 | 35136 |
| **ListDataKeys (100 keys)** | ~1.7M | 583.3 | 1792 |

### 并发性能

| 测试场景 | 吞吐量 (ops/s) | 延迟 (ns/op) | 内存分配 (B/op) |
|---------|---------------|-------------|----------------|
| **Publish (Concurrent, Parallel)** | ~615K | 1626 | 598 |
| **Mixed Operations** | **~2.3M** | **436.5** | **338** |

### 关键性能特性

1. **No-Op 极致优化**：无订阅者时延迟仅 ~5ns，零内存分配，适合高频发布场景
2. **高吞吐量**：单线程 Publish 可达 765K+ ops/s，并发场景下更高
3. **低内存开销**：典型操作仅需 500-600 B 内存分配
4. **优秀扩展性**：从 2 个到 100 个订阅者，性能下降仅 ~38%
5. **查询高效**：前缀搜索和 key 查询均在微秒级别

运行基准测试：

```bash
# 运行所有 benchmark
go test -bench=. -benchmem -benchtime=1s

# 运行特定 benchmark
go test -bench=BenchmarkPublish_Fanout -benchmem

# 并发 benchmark
go test -bench=BenchmarkPublish_Concurrent -benchmem
```

更多详情请查看 [日志模块文档](./log/README.md)。
