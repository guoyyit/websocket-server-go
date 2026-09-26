# websocket-server-go

RFC 6455 WebSocket 连接层（服务端）。在被劫持的 TCP 连接之上提供从 HTTP 握手
升级、帧收发、控制帧处理到关闭握手的完整实现，支持并发、背压、大小限制与
心跳/空闲回收。

## 快速开始

```go
srv := websocket.NewServer(websocket.DefaultConfig(), func(c *websocket.Conn) {
    for {
        m, err := c.ReadMessage()
        if err != nil {
            return // 连接已关闭，err 通常是 *websocket.CloseError
        }
        if err := c.WriteMessage(m.Type, m.Data); err != nil {
            return
        }
    }
})

http.ListenAndServe(":8080", srv)
```

也可以在任意 `http.Handler` 中调用 `srv.Upgrade(w, r)` 自行管理连接生命周期。

## 协议状态机

```
        握手成功(101)
              │
              ▼
       ┌────────────┐  任一端发送 close 帧 / 协议错误 / 看门狗超时
       │    open    │ ───────────────────────────────────────┐
       └────────────┘                                        ▼
              │                                        ┌────────────┐
              │  close 帧已排队，拒绝新的应用写          │  closing   │
              │  (WriteMessage 返回 ErrClosed)          └────────────┘
              │                                                │
              │   排干已排队的数据与控制帧 → 发送 close 帧       │
              │                                                ▼
              │                                        ┌────────────┐
              └───────────────────────────────────────▶│   closed   │
                       关闭底层 TCP，Done() 关闭          └────────────┘
```

- **open**：正常收发文本/二进制/控制帧。
- **closing**：由首个 close 触发（只触发一次）。本端发起时先排干发送队列再发
  close，然后等待对端 close 回复，最长等待 `CloseTimeout`；对端发起时回复
  close 后立即拆除。
- **closed**：底层连接关闭，`Conn.Done()` 关闭；重复 `Close()` 幂等。
- 未收到 close 帧而 TCP 断开，上报 1006（abnormal closure）；协议违规先发对应
  关闭码（1002/1007/1009）再关。

## 并发与背压

- 每个连接一个 **writeLoop** 独占 socket 写，一个 **readLoop** 解析帧，一个
  **watchdog** 发心跳、回收空闲/失活连接。
- 应用通过有界 channel 入队发送帧：队列满时 `WriteMessage` 阻塞（背压）。
  某连接发送过慢只会阻塞该连接的生产者，不影响其他连接。
- 控制帧（ping/pong）走独立优先队列，即使数据队列积压也能及时发出。
- `Conn.WriteBufferLen()` / `WriteBufferCap()` 可观测队列水位。

## 限制与心跳

- 单帧超限：关闭码 1009；跨分片重组消息超限同样 1009；控制帧固定 ≤125 字节。
- 任何入站帧（含 pong/数据）都刷新活跃时间；仅在持续无数据时才回收，避免误杀
  活跃连接。

## 配置项（Config）

零值字段自动取 `DefaultConfig()`；持续时间传 **负值** 可关闭对应机制。

| 字段              | 默认值       | 说明                                                              |
|-------------------|--------------|-------------------------------------------------------------------|
| `MaxMessageSize`  | `1 MiB`      | 重组后消息最大字节数。入站超限 → 1009；出站超限直接返回错误。      |
| `MaxFrameSize`    | `64 KiB`     | 单个入站帧最大载荷；出站消息按此大小自动分片。                     |
| `WriteBufferSize` | `64`         | 每连接待发帧队列容量，背压阈值。                                    |
| `WriteTimeout`    | `10s`        | 单次写 flush 的超时上限。                                          |
| `PingInterval`    | `30s`        | 心跳 ping 周期；负值关闭心跳。                                      |
| `PongWait`        | `60s`        | ping 发出后无任何入站活动的失活判定宽限。                          |
| `IdleTimeout`     | `5m`         | 无入站数据多久后回收；负值关闭空闲回收。                            |
| `CloseTimeout`    | `5s`         | 本端发起 close 后等待对端回复的上限，防止对端静默导致连接悬挂。     |

## 文件组织

- `handshake.go` —— Sec-WebSocket-Key 校验、Accept 计算（SHA1+GUID+Base64）、
  101 响应；非法握手写错误响应并关闭底层连接。
- `frame.go` —— 帧头解析/序列化、掩码、7/16/64 位长度扩展（最小编码校验）、
  RSV/opcode/控制帧校验、关闭码常量。
- `conn.go` —— 连接状态机、读写循环、分片重组、关闭握手、背压队列、看门狗。
- `server.go` —— `http.Handler` 升级入口、连接跟踪与并行整体关闭。

## 测试

```sh
go test ./... -race
```

覆盖：握手校验（含端到端 101 与非法握手即关连接）、掩码与长度扩展、分片重组、
UTF-8 跨分片校验、ping/pong/close 控制帧及乱序/非法控制帧、双向关闭握手与竞态、
慢写者背压与连接间隔离、帧/消息大小限制、心跳保活、pong 超时与空闲回收。
