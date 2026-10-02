# websocket-server-go

从零依赖实现的 WebSocket 连接层（[RFC 6455](https://www.rfc-editor.org/rfc/rfc6455)），
建立在 Go 的底层 TCP/net/http 能力之上，负责握手升级、帧解析、消息分片、
控制帧、关闭握手、并发背压、资源限制与心跳回收。

## 快速开始

```go
package main

import (
    "log"
    "net/http"

    ws "github.com/guoyyit/websocket-server-go"
)

func main() {
    srv := ws.NewServer(ws.DefaultConfig(), func(c *ws.Conn) {
        for {
            m, err := c.ReadMessage()
            if err != nil {
                return // 连接已关闭，err 为 *ws.CloseError 或其他终止原因
            }
            if err := c.WriteMessage(m.Type, m.Data); err != nil {
                return
            }
        }
    })
    log.Fatal(http.ListenAndServe(":8080", srv))
}
```

`Server` 实现了 `http.Handler`：它校验握手、Hijack 底层连接、回复 `101`，
随后为每个连接启动内部循环并调用业务 Handler。

## 协议状态机

```
                 ┌──────────────────────────────────────────┐
                 │                                          │
 HTTP 请求        ▼                                          │
┌──────────┐  校验通过+101   ┌────────┐  Close()/收到close/  │
│ Handshake│ ─────────────▶ │  Open  │ 协议错误/超时/IO错误  │
└──────────┘                 └────┬───┘ ───────────────────▶│
     │ 头非法/缺失                │                          │
     └─ 400 + 关闭 TCP            │ 仅触发一次                 │
                                  ▼                          │
                            ┌──────────┐                     │
                            │ Closing  │ 排空待发数据→发close │
                            └────┬─────┘ 等对端close(有超时)  │
                                 ▼                          │
                            ┌──────────┐                     │
                            │  Closed  │ ◀───────────────────┘
                            └──────────┘   close(done) + TCP 关闭
```

- **Handshake**：`GET` + HTTP/1.1+；`Connection`/`Upgrade` 令牌包含 `upgrade`/`websocket`；
  `Sec-WebSocket-Key` 为合法的 16 字节 base64 nonce；`Sec-WebSocket-Version: 13`。
  响应值 `Sec-WebSocket-Accept = base64(SHA1(key + 258EAFA5-E914-47DA-95CA-C5AB0DC85B11))`。
  任一校验失败返回 HTTP 400 并关闭底层连接，不进入帧阶段。
- **Open**：可收发数据帧与控制帧。
- **Closing**：`sync.Once` 保证 close 帧只发送一次；先排空已入队的应用数据，
  再写 close 帧；本端发起的关闭等待对端 close 回复，等待受 `CloseTimeout` 上界约束；
  对端发起的关闭，回复 close 后立即回收。
- **Closed**：`done` 通道关闭、TCP 关闭，`ConnCount` 等可观测状态先于 `done` 更新。

## 帧处理（RFC 6455 §5）

- 支持文本（0x1）、二进制（0x2）帧及 0x0 续帧的消息分片重组；
  服务端出站消息超过 `MaxFrameSize` 时自动分片。
- 客户端入站帧必须掩码，否则以 `1002 Protocol Error` 关闭；掩码/解掩码按 §5.3 处理。
- 长度支持 7 位、16 位（≥126）、64 位（≥65536）三种编码，非最小编码、
  长度高位为 1 均按协议错误处理。
- 未协商扩展时 RSV 位必须为 0；保留/未知 opcode 一律拒绝。
- 文本消息在**整条消息重组完成后**做 UTF-8 校验，非法时以 `1007` 关闭
  （跨分片的非法序列也能识别）。
- close 帧负载校验：1 字节非法；状态码必须合法（1005/1006 等保留码禁止出现在线上）；
  关闭原因必须为合法 UTF-8。回复时回显对端状态码。

### 控制帧（§5.5）

- `Ping` 立即回 `Pong` 并回显负载，控制帧走独立优先队列，不排在大数据帧之后；
- `Pong` 仅作为活跃标记；
- `Close` 触发关闭握手；
- 控制帧不得分片、负载不得超过 125 字节，违规以 `1002` 关闭；
- 分片消息进行中可以插入控制帧；分片未结束就出现新的起始数据帧、
  或收到无对应起始帧的续帧，均以 `1002` 关闭。

### 常见关闭码

| 码值 | 含义 | 触发场景 |
|------|------|----------|
| 1000 | Normal Closure | 正常 `Close()`、回显对端正常关闭 |
| 1001 | Going Away | 心跳超时、空闲回收、服务关闭 |
| 1002 | Protocol Error | RSV/opcode/分片/掩码/close 负载违规 |
| 1007 | Invalid Frame Payload Data | 文本或 close 原因非合法 UTF-8 |
| 1009 | Message Too Big | 单帧超 `MaxFrameSize` 或消息超 `MaxMessageSize` |
| 1006 | Abnormal Closure | 无 close 帧的 TCP 断开（仅在错误信息中出现） |

## 并发模型与背压

每条连接三个 goroutine：

1. **read loop**：唯一的帧读取者。内联处理控制帧、重组消息、执行全部协议校验，
   完整消息投递到容量 1 的 `recv` 通道；应用不消费时 TCP 层形成自然背压。
2. **write loop**：唯一的帧写入者。优先消费控制帧队列，其次消费有界 `send` 队列。
   `WriteMessage` 在队列满时阻塞（背压），连接关闭后阻塞中的写立即以 `ErrClosed` 返回。
3. **watchdog**：心跳 ping 与空闲/失活回收。

单个连接的慢消费者只阻塞它自己的 `send` 通道，不影响其他连接。
每次 flush 受 `WriteTimeout` 约束，对端彻底失活时不会无限卡死写循环。

可观测接口：

```go
c.WriteBufferLen() // 当前出站队列深度（背压水位）
c.WriteBufferCap() // 队列容量
srv.ConnCount()    // 存活连接数
c.Done()           // 连接回收完成的信号
```

资源始终有界：入站单帧、重组消息均有上限；出站队列、控制帧队列均为有界通道；
关闭握手与每次写操作都有超时上界。

## 心跳与空闲回收

- 到点（`PingInterval`）发送 ping；ping 后 `PongWait` 内没有**任何入站帧**
  （数据帧、pong、其他帧都算活跃）则发送 close(1001) 并回收。
- `IdleTimeout` 独立于心跳：任意入站帧都会刷新活跃时间，因此有持续数据活动的
  连接绝不会被误回收；仅在持续静默超过阈值时回收。

## 配置项（`Config` / `DefaultConfig`）

所有字段零值表示“使用默认值”；时长字段填**负数表示禁用**对应机制。

| 字段 | 默认值 | 说明 |
|------|--------|------|
| `MaxMessageSize` | 1 MiB | 单条重组消息的最大字节数；入站超限以 1009 关闭，出站超限返回 `ErrMessageTooBig` |
| `MaxFrameSize` | 64 KiB | 单帧负载上限（控制帧另受 125 字节限制）；出站消息按此分片 |
| `WriteBufferSize` | 64 | 每连接出站帧队列长度，即背压阈值 |
| `WriteTimeout` | 10s | 单次 flush 的写截止时间 |
| `PingInterval` | 30s | 心跳 ping 周期；负数禁用 |
| `PongWait` | 60s | 发出 ping 后等待任意入站活动的超时 |
| `IdleTimeout` | 5m | 无任何入站活动的回收阈值；负数禁用 |
| `CloseTimeout` | 5s | 发出 close 后等待对端 close 回复的上界 |

## 关闭握手的竞态与超时

- 本端 `Close()`、对端 close、协议错误、心跳/空闲超时四条路径通过 `sync.Once`
  汇聚：close 帧只发一次，TCP 只关一次，`Close()` 幂等且可并发调用。
- 对端先 close 时：排空待发应用数据 → 回 close → 立即回收。
- 本端先 close 时：排空待发数据 → 发 close → 等对端回复或 `CloseTimeout`。
- 并发双方同时发起 close 时（`TestConcurrentCloseRace` 覆盖）不会死锁或重复发送。

## 拒绝升级的连接回收

所有未能升级成功的请求，写完错误响应后都会**立即关闭底层 TCP 连接**，客户端读完
错误响应后的下一次读操作立刻得到 EOF/RST（连接关闭信号），同一连接上不能再发起
任何请求。拒绝路径不创建 `Conn`、不启动任何帧处理 goroutine，因此不会进入帧处理
阶段、不会出现在 `ConnCount()` 中，也不残留后台读写活动。

拒绝原因与状态码（保持原有约定）：

| 场景 | 状态码 |
|------|--------|
| 方法/协议版本/`Connection`/`Upgrade`/`Sec-WebSocket-Key`/`Sec-WebSocket-Version` 非法或缺失 | `400 Bad Request` |
| 服务已停摆（`Close` 已开始或完成）后到达的升级请求（无论头是否合法） | `503 Service Unavailable` |
| 运行环境不支持 Hijack（理论场景） | `500 Internal Server Error` |

实现上，服务优先 Hijack 底层连接并手写带 `Connection: close` 的 HTTP 错误响应，
`Flush` 后直接 `net.Conn.Close()`，因此回收行为不依赖 `net/http` 的 keep-alive
决策；写错误响应受 `WriteTimeout` 约束，对端窗口为零时也不会拖住回收。无法
Hijack 的退化场景则通过响应头 `Connection: close` 指示 `net/http` 在响应后关闭
传输层。一次拒绝风暴结束后服务保持可用，后续合法升级与正常连接不受影响
（`TestManyRejectedRequestsLeaveNoResidue`、`TestRejectedUpgradeClosesTCP` 覆盖）。

## 服务停摆（`Server.Close`）语义

- **并发关闭**：停摆时对**每条存活连接并发**执行关闭握手，而不是串行逐个等待。
  因此整体停摆耗时上界接近**单个连接**的关闭耗时——排空待发数据（每帧 flush 受
  `WriteTimeout` 约束）后发送 close 帧，再等待 `CloseTimeout`——与不应答 close
  帧的静默对端数量无关，一个异常对端不会拖慢其他连接退出。实测 2 个与 30 个静默
  对端的停摆耗时都约为单个 `CloseTimeout`
  （`TestShutdownDurationIndependentOfSilentPeerCount` 覆盖并断言两者无倍数关系）。
- **全员收到 close**：每条存活连接都会及时收到 close 帧；静默对端在单个
  `CloseTimeout` 上界到达后被强制回收，正常应答的对端完成握手后立即回收
  （`TestShutdownMixedPeersBounded` 覆盖混合场景）。
- **新连接竞态**：`Close` 在同一把锁内完成“标记停止接受”与快照存活连接。
  停摆开始后到达的升级请求一律得到 `503` 且底层连接被关闭；101 与停摆之间没有
  可逃逸的窗口——升级要么在发送 101 前被注册并由停摆关闭，要么被明确拒绝
  （`TestShutdownRaceWithNewConnections` 覆盖）。
- **返回即清空**：`Close` 返回后 `ConnCount()` 必为 0，所有连接的 read/write/
  watchdog 循环 goroutine 均已退出，不残留任何连接相关后台活动
  （`TestShutdownLeavesNoBackgroundGoroutines` 覆盖）。
- **幂等可并发**：`Close` 可重复调用、可被多个 goroutine 并发调用；只有第一次
  调用发起停摆并触发 close 帧，重复调用不重发 close 帧、不干扰进行中的握手，
  不 panic、不挂起（`TestConcurrentRepeatedShutdown` 覆盖，并逐连接校验恰好收到
  一个 close 帧）。

### 本地复现与验证

用原始 TCP 客户端即可复现“400 后连接不关闭”的问题：连上服务端后只发送
`GET / HTTP/1.1\r\nHost: x\r\n\r\n`，读完 400 响应后再次 `Read`。修复前该读会
一直阻塞直到读超时，连接还可被复用；修复后立即得到 EOF。停摆问题可通过建立 N
个完成握手但从不读/不回复的静默连接、再调用 `Close` 复现：修复前耗时约为
`N × CloseTimeout`，修复后恒为约一个 `CloseTimeout`。

```bash
# 全量测试（含 -race）
go test -race ./...

# 只跑拒绝回收与停摆语义测试
go test -race -run 'Reject|Shutdown' -v ./...

# 重复运行检查偶发竞态
go test -race -count=10 -run 'Reject|Shutdown' ./...
```

关键测试：

- `TestRejectedUpgradeClosesTCP`：表驱动覆盖 8 种非法/缺失头组合，断言状态码为
  400、响应后下一次读立即得到关闭信号（显式区分“超时未关闭”与真正的 EOF/RST）、
  管道内再次发送升级请求不会成功、`ConnCount()` 为 0。
- `TestRejectedUpgradeDuringShutdownClosesTCP`：停摆后合法与非法请求均得到 503
  且 TCP 被关闭。
- `TestManyRejectedRequestsLeaveNoResidue`：200 路并发非法请求后无连接残留、
  goroutine 数不增长，且后续合法升级收发正常。
- `TestShutdownMixedPeersBounded` / `TestShutdownDurationIndependentOfSilentPeerCount`：
  静默与正常对端混合、大量连接，断言每个对端都收到 close 帧、停摆耗时上界与静默
  对端数量无关、停摆后 `ConnCount()` 为 0。
- `TestConcurrentRepeatedShutdown`：8 路并发 + 重复停摆，断言全部及时返回、恰好
  一个 close 帧/连接、计数归零。
- `TestShutdownRaceWithNewConnections`：40 路新建连接与停摆并发，已接受的连接
  收到 close 帧、被拒的连接得到 503 + EOF，停摆返回后新建连接同样被拒。
- `TestShutdownLeavesNoBackgroundGoroutines`：30 个连接停摆后 goroutine 数回落至
  建连前基线。

## 测试

```
go test -race ./...
```

覆盖：握手校验（RFC 示例 accept 值、各类非法头）、帧解析（掩码向量、
三种长度编码、非最小编码、RSV/未知 opcode/分片控制帧）、消息分片与跨分片
UTF-8、控制帧穿插、关闭握手（本端/对端/同时发起、静默超时、幂等、待发数据
先于 close）、背压（队列满阻塞、关闭即解除、慢连接不影响他人）、帧/消息大小
限制、心跳 ping/pong、pong 超时、空闲回收与活动保护，以及基于 `httptest` 的
端到端升级、连接计数和服务关停。
