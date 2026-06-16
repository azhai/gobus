# gobus log

基于 Go 标准库 `log/slog` 的高性能日志工具，支持文件输出、**多种周期轮转**、自动压缩和全局默认设置。

## 特性

- **JSON/Text 格式**：支持 JSON 和 Text 两种输出格式
- **多周期轮转**：支持月/周/天/小时/分钟 5 种轮转周期，gzip 压缩
- **灵活输出**：支持文件、stdout、stderr、/dev/null
- **全局配置**：一键设置默认 logger 和日志级别（支持字符串）
- **自动创建目录**：自动创建日志文件的父目录

## 快速开始

```go
package main

import (
    "gobus/log"
    "log/slog"
)

func main() {
    // 方式1：最简单 - 设置全局默认 logger（每日轮转，maxBackups=7，自动压缩）
    log.SetDefault("logs/app.log", "info")

    // 使用 slog 全局函数记录日志
    slog.Info("应用启动", "version", "1.0.0")
    slog.Error("连接失败", "host", "db.example.com")
}
```

## API 参考

### 创建 Logger

| 方法 | 签名 | 说明 |
|------|------|------|
| NewFileLogger | `NewFileLogger(logFile string, isJSON bool, addSource bool) (*slog.Logger, error)` | 基础文件 logger |
| NewRotateLogger | `NewRotateLogger(cycle RotateCycle, logFile string, maxBackups int, compress bool) (*slog.Logger, error)` | 多周期轮转 logger ✅ 推荐 |
| NewDailyLogger | `NewDailyLogger(logFile string, maxBackups int) (*slog.Logger, error)` | 每日轮转 logger (便捷方法) |
| SetDefault | `SetDefault(logFile string, logLevel string)` | 设置全局默认 logger (每日轮转, maxBackups=7) |
| SetDefaultByFile | `SetDefaultByFile(cycle RotateCycle, logFile string, logLevel string, maxBackups int)` | 设置全局默认 logger (自定义周期) |

**API 设计说明：**
- **统一命名规范**：使用 `New` 前缀（Go 惯例）
- **cycle 参数放在第一位**（NewRotateLogger 和 SetDefaultByFile）
- **logLevel 支持字符串**：可使用 `"debug"`, `"info"`, `"warn"`, `"error"` (不区分大小写)
- **参数顺序清晰**：配置项从必需到可选

### 轮转周期常量

| 常量 | 值 | 说明 | 备份文件格式示例 |
|------|-----|------|-----------------|
| CycleMonthly | 0 | 按月轮转 | app-202601.log |
| CycleWeekly | 1 | 按周轮转 | app-20260116.log |
| CycleDaily | 2 | 按天轮转 (默认) | app-20260616.log |
| CycleHourly | 3 | 按小时轮转 | app-2026061614.log |
| CycleMinutely | 4 | 按分钟轮转 | app-20260616-1430.log |

### 参数说明

**logFile 支持的值：**
- `"stdout"` - 输出到标准输出
- `"stderr"` - 输出到标准错误
- `"/dev/null"` 或 `""` - 丢弃所有日志
- 其他值 - 写入指定文件路径

**logLevel 支持的值（不区分大小写）：**
- `"debug"` / `"DEBUG"` - 调试级别
- `"info"` / `"INFO"` / `"NOTICE"` - 信息级别（默认）
- `"warn"` / `"WARNING"` - 警告级别
- `"error"` / `"ERR"` / `"FATAL"` - 错误级别

**maxBackups：**
- `0` - 不限制备份数量
- `>0` - 保留最近 N 个备份文件

## 使用示例

### 基础文件日志

```go
// 创建基础文件 logger（JSON 格式，不显示源码位置）
logger, err := log.NewFileLogger("logs/simple.log", true, false)
if err != nil {
    panic(err)
}

logger.Info("用户登录", "userId", 12345, "ip", "192.168.1.1")

// 或使用 Text 格式
textLogger, _ := log.NewFileLogger("logs/text.log", false, false)
```

### 多周期轮转日志 ✅ 推荐

```go
// 按月轮转（保留3个备份，自动压缩）
// 参数顺序：cycle, logFile, maxBackups, compress
monthlyLog, _ := log.NewRotateLogger(log.CycleMonthly, "logs/monthly.log", 3, true)

// 按周轮转（保留4个备份，自动压缩）
weeklyLog, _ := log.NewRotateLogger(log.CycleWeekly, "logs/weekly.log", 4, true)

// 按天轮转（保留7个备份）- 最常用，自动压缩
dailyLog, _ := log.NewRotateLogger(log.CycleDaily, "logs/app.log", 7, true)

// 按小时轮转（保留24个备份）- 高频场景
hourlyLog, _ := log.NewRotateLogger(log.CycleHourly, "logs/hourly.log", 24, true)

// 按分钟轮转（保留60个备份，不压缩）- 调试/高频场景
minutelyLog, _ := log.NewRotateLogger(log.CycleMinutely, "logs/debug.log", 60, false)
```

### 全局默认 Logger

```go
import "log/slog"

// 方式1：使用默认的每日轮转（最简单，maxBackups=7，自动压缩）
// 参数顺序：logFile, logLevel (字符串)
log.SetDefault("logs/production.log", "info")

// 方式2：自定义配置（参数顺序：cycle, logFile, logLevel, maxBackups）
log.SetDefaultByFile(log.CycleHourly, "logs/hourly_production.log", "info", 48)

// 之后可以直接使用 slog 全局函数
slog.Info("请求处理", "path", "/api/users", "duration", "120ms")
slog.Warn("内存使用率高", "usage", "85%")
slog.Error("数据库超时", "query", "SELECT * FROM users")

// Debug 级别消息会被过滤（如果设置了 LevelInfo）
slog.Debug("调试信息") // 不会输出
```

### 不同级别使用

```go
// 设置只记录 Error 及以上级别（maxBackups 默认为 7）
// 使用字符串 logLevel（支持大小写）
log.SetDefault("logs/error_only.log", "error")

slog.Info("这条不会出现")   // 被过滤
slog.Warn("这条也不会出现")  // 被过滤
slog.Error("只有这条会出现") // 会输出
```

### 多实例独立日志

```go
// 可以创建多个独立的 logger，不同周期不同用途
// 参数顺序：cycle, logFile, maxBackups, compress
accessLog, _ := log.NewRotateLogger(log.CycleDaily, "logs/access.log", 7, true)      // 访问日志：按天
errorLog, _ := log.NewRotateLogger(log.CycleHourly, "logs/error.log", 72, true)     // 错误日志：按小时
debugLog, _ := log.NewRotateLogger(log.CycleMinutely, "logs/debug.log", 30, false)   // 调试日志：按分钟

accessLog.Info("API调用", "method", "GET", "path", "/health")
errorLog.Error("认证失败", "token", "invalid")
debugLog.Debug("变量状态", "count", 42)
```

## 日志轮转机制

日志轮转功能由 `RotateWriter` 提供：

### 支持的轮转周期

- `CycleMonthly` - 按月轮转 (200601)
- `CycleWeekly` - 按周轮转 (20060102)
- `CycleDaily` - 按天轮转 (20060102) ✅ 默认
- `CycleHourly` - 按小时轮转 (2006010215)
- `CycleMinutely` - 按分钟轮转 (20060102-1504)

### 自动清理策略

- **MaxBackups**：超过数量限制时删除最旧的未压缩备份
- **MaxAge**：超过天数限制时删除旧备份（0=不限制）
- **Compress**：自动 gzip 压缩旧日志文件

### 自定义配置示例

```go
// 方式1：使用通用 NewRotateWriter（推荐）
writer := log.NewRotateWriter(
    log.CycleDaily,   // 轮转周期: 每日
    "logs/custom.log",
    10,              // MaxBackups: 保留10个备份
    7,               // MaxAge: 保留7天
    true,            // Compress: 启用压缩
)
defer writer.Close()

// 手动触发轮转
writer.Rotate()

// 方式2：按小时轮转（保留48个备份，不限制天数）
hourlyWriter := log.NewRotateWriter(
    log.CycleHourly,
    "logs/hourly.log",
    48,  // MaxBackups: 保留48个文件(2天)
    0,   // MaxAge: 不限制天数
    true,
)
defer hourlyWriter.Close()

// 方式3：使用便捷的 NewDailyRotateWriter（仅每日轮转）
dailyWriter := log.NewDailyRotateWriter(
    "logs/daily.log",
    7,   // MaxBackups: 保留7个备份
    30,  // MaxAge: 保留30天
    true, // Compress: 启用压缩
)
defer dailyWriter.Close()
```

## 文件命名规则

轮转后的备份文件格式：
```
原始文件: app.log
轮转后:   app-20260616.log
压缩后:   app-20260616.log.gz
```

## 工具函数

| 方法 | 说明 |
|------|------|
| `MakeDirForFile(filename)` | 自动创建文件的父目录 |
| `NewFileWriter(logFile)` | 创建文件句柄（追加模式） |
| `ParseLevel(logLevel string)` | 将字符串转换为 slog.Level |
| `WriteToFile(buf, path)` | 格式化并写入 Go 代码文件 |

## 并发安全

所有公开方法均为线程安全：
- `NewFileLogger` / `NewRotateLogger` / `SetDefault` - 无状态，天然安全
- `RotateWriter.Write` / `RotateWriter.Rotate` - 使用 `sync.Mutex` 保护
- `slog.Logger` - Go 标准库保证并发安全

## 性能建议

1. **生产环境推荐**：使用 `NewRotateLogger` + `SetDefaultByFile`（灵活选择轮转周期）
2. **开发环境**：使用 `NewFileLogger("stdout", true, false)` 方便调试
3. **测试环境**：使用 `NewFileLogger("/dev/null", true, false)` 避免磁盘 IO
4. **高频日志**：考虑 `maxBackups=0` + 定期外部清理
5. **快速开始**：可直接使用 `SetDefault`（默认每日轮转，最简单）

## 测试

运行日志模块测试：

```bash
# 运行所有测试
go test -v ./log/...

# 运行特定测试
go test -v ./log/... -run TestNewRotateLogger

# 运行 benchmark
go test -bench=. ./log/
```
