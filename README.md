# gobus

基于 FastCache 的事件总线，支持发布-订阅模式，提供 Fanout（全量分发）和 Anyone（竞争消费）两种送达模式。使用 CBOR (RFC 8949) 进行高效二进制序列化。

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

    // 订阅消息 - Fanout 模式：所有订阅者都收到
    bus.Subscribe("order", "created", gobus.Fanout, "logger", func(e *gobus.Event) {
        fmt.Printf("[logger] 收到订单: %v\n", e.Data["orderId"])
    })

    bus.Subscribe("order", "created", gobus.Fanout, "notifier", func(e *gobus.Event) {
        fmt.Printf("[notifier] 发送通知: %v\n", e.Data["orderId"])
    })

    // 发布消息
    event, _ := bus.Publish("order", "created", map[string]interface{}{
        "orderId": "ORD-001",
        "amount":  99.9,
    }, 1, false)

    fmt.Printf("消息ID: %s, 状态: %s\n", event.ID, event.Status)
}
```

## 送达模式

### Fanout - 全量分发

所有订阅者都会收到消息。

```go
bus := gobus.NewEventBus(0)

bus.Subscribe("order", "created", gobus.Fanout, "service-a", func(e *gobus.Event) {
    fmt.Println("service-a 收到")
})
bus.Subscribe("order", "created", gobus.Fanout, "service-b", func(e *gobus.Event) {
    fmt.Println("service-b 收到")
})
bus.Subscribe("order", "created", gobus.Fanout, "service-c", func(e *gobus.Event) {
    fmt.Println("service-c 收到")
})

bus.Publish("order", "created", map[string]interface{}{"orderId": "1"}, 1, false)
// 输出:
// service-a 收到
// service-b 收到
// service-c 收到
```

### Anyone - 竞争消费

仅一个订阅者收到消息，适合任务分发场景。

```go
bus := gobus.NewEventBus(0)

bus.Subscribe("task", "process", gobus.Anyone, "worker-1", func(e *gobus.Event) {
    fmt.Println("worker-1 处理任务")
})
bus.Subscribe("task", "process", gobus.Anyone, "worker-2", func(e *gobus.Event) {
    fmt.Println("worker-2 处理任务")
})

bus.Publish("task", "process", map[string]interface{}{"taskId": "T-001"}, 1, false)
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

bus.Subscribe("order", "created", gobus.Fanout, "processor", func(e *gobus.Event) {
    fmt.Printf("处理订单: %v\n", e.Data["orderId"])
    // 处理完成后发送 ACK
    bus.Ack(e.ID)
})

// 发布消息，needAck=true
event, _ := bus.Publish("order", "created", map[string]interface{}{
    "orderId": "ORD-002",
}, 1, true)

fmt.Printf("状态: %s\n", event.Status) // delivered
// ACK 后状态变为 completed
```

## 优先级

优先级范围 1~5，数值越大优先级越高，默认为 1。消息按优先级从高到低分发。

```go
bus := gobus.NewEventBus(0)

bus.Subscribe("task", "run", gobus.Fanout, "worker", func(e *gobus.Event) {
    fmt.Printf("执行: priority=%d, data=%v\n", e.Priority, e.Data)
})

// 低优先级
bus.Publish("task", "run", map[string]interface{}{"name": "普通任务"}, 1, false)
// 高优先级
bus.Publish("task", "run", map[string]interface{}{"name": "紧急任务"}, 5, false)
// 中优先级
bus.Publish("task", "run", map[string]interface{}{"name": "一般任务"}, 3, false)
// 分发顺序: 紧急任务(5) → 一般任务(3) → 普通任务(1)
```

## 取消订阅

```go
bus := gobus.NewEventBus(0)

bus.Subscribe("order", "created", gobus.Fanout, "my-service", handler)

// 取消订阅
bus.Unsubscribe("order", "created", "my-service")
```

## 查询接口

### 查询 group 下的 topic 列表

```go
bus := gobus.NewEventBus(0)

bus.Publish("order", "created", map[string]interface{}{"k": "v"}, 1, false)
bus.Publish("order", "updated", map[string]interface{}{"k": "v"}, 1, false)
bus.Publish("user", "login", map[string]interface{}{"k": "v"}, 1, false)

topics := bus.ListTopics("order")
// ["created", "updated"]
```

### 查询 topic 下 data 的 key 列表

```go
bus := gobus.NewEventBus(0)

bus.Publish("order", "created", map[string]interface{}{
    "orderId": "ORD-001",
    "amount":  99.9,
}, 1, false)

keys := bus.ListDataKeys("order", "created")
// ["orderId", "amount"]
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
| Group | string | 分组标识（必填） |
| Topic | string | 主题标识（必填） |
| Data | map[string]any | 负载数据（必填，至少一个key） |
| Status | string | 消息状态：pending/delivered/completed |
| Priority | int | 优先级 1~5，默认1，越大越优先 |
| NeedAck | bool | 是否需要ACK回执，默认false |
| Created | int64 | 创建时间戳（Unix秒级），系统自动生成 |
