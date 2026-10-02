# canvassync

白板协作引擎的画布同步模块。纯 Go 库，由上层 WebSocket 网关直接调用，
不自带接口层；全部数据只在进程内存中。

## 用法

```go
e := canvassync.NewEngine()

// 进入页面：拿到起点画面（含对应编号）和后续推送的订阅。
view, sub := e.Enter(pageID, clientID)
for env := range sub.C() {
    if env.ResyncRequired {
        // 落后太多被明确踢出，重新 Enter 拿新起点画面。
        view, sub = e.Enter(pageID, clientID)
        continue
    }
    applyToLocalCanvas(env.Op) // env.Op.Seq 严格连续，不漏不重
}

// 受理操作：落笔 / 擦除一笔 / 清除某参与者的笔画。
op, err := e.Apply(pageID, actor, canvassync.OpInput{Kind: canvassync.OpAdd, Stroke: ...})

// 断线重连：带上收到的最后一个编号。
res, sub := e.Reconnect(pageID, clientID, lastSeq)
// res.Kind == ReconnectReplay   → res.Missed 是缺的连续一段，直接补放
// res.Kind == ReconnectSnapshot → res.View 是新的起点画面
```

## 设计要点

- **编号与推送**：每页一把互斥锁，操作在锁内按受理顺序连续编号、
  改状态、入日志、非阻塞扇出。订阅者按编号顺序接收，不漏不重。
- **快照合并**：每页未合并操作累计到 5,000 条时并入快照（O(可见笔画数)，
  微秒级）；擦除/清除通过全局 `alive` 索引作用，已并入快照的笔画照样可擦可清。
- **重连补发**：日志始终保留最近 5,000 条（`ReplayWindow`）。
  断开期间新增 ≤ 5,000 条时只补发缺的一段，否则下发新起点画面。
- **慢消费者**：每个订阅者缓冲 2,048 条操作（通道另预留 1 个信号槽）；
  缓冲写满即被踢出——已缓冲的连续前缀原样保留，显式送入
  `ResyncRequired` 信号后关闭通道，订阅者永远先收完连续前缀再收到
  重同步信号，绝不悄悄跳过一段。踢出后其内存立即释放，
  卡住的人不会让页面内存无限增长。
- **耗时**：`Apply` 的 p99 在卡住消费者、合并快照、多人进入时均远低于
  10 ms（实测约 30 µs）；`Enter`/`Reconnect` 的耗时只取决于当前可见笔画数，
  与页面累计操作数无关。

## 测试

```sh
go test ./...
```

覆盖：多页多人并发操作 + 合并多次 + 进入/重连/卡住，最终与从头重放逐笔比对；
重连 5,000 / 5,001 条边界；慢消费者被明确踢出且他人不受影响；
卡住消费者下落笔 p99 ≤ 10 ms；进入/重连耗时不随历史增长。
