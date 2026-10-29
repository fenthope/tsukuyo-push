# tsukuyo-push

UnifiedPush 链路中 **S–D 段**的 Go 实现：推送服务器（S）与分发器核心（D），
之间的传输是**单条复用 TCP 连接上的 HTTP/2 长轮询**，S 与 A（应用后端）之间
完全遵循 Web Push（RFC 8030/8188/8291/8292），无自研线路协议。

```
A 应用后端 ──upush/sender──▶ S 推送服务器 ══ h2c/TLS,1×TCP,N×流 ══▶ D 分发器 ──▶ C 端应用(连接器解密)
                            cmd/server                        dcore + adapters
```

加解密端到端由 C 的连接器完成；S 与 D 只搬运 RFC 8291 密文。
依赖发布版 `github.com/fenthope/tsukasa-push` v0.0.3：S 端直接用其
`service`/`pushmsg`，D 端不依赖 `receiver`（原因与回馈清单见
[docs/upstream-gap.md](docs/upstream-gap.md)）。

## 组件

| 路径 | 说明 |
| --- | --- |
| `cmd/server` | S 端：touka + tsukasa-push service + bbolt 持久 Store + 调优 h2 监听 |
| `internal/boltstore` | `service.Store` 的 bbolt 实现；未 ack 消息重启后重投（至少一次） |
| `internal/h2srv` | stdlib `http.HTTP2Config` 服务端调优 + h2c + 监听套接字调优 |
| `dcore` | D 核心：订阅管理、轮询监督、JSON 注册表；只转发密文，不持密钥 |
| `dcore/transport` | 单连接客户端：`MaxConnsPerHost=1` + Linux sockopt（NODELAY/KEEP*/USER_TIMEOUT）+ h2 空闲 PING |
| `adapters/dbus` | **可选**（`-tags dbus`）：UnifiedPush D-Bus 规范 DBUS_0.3.0 分发器 |
| `cmd/distributor` | 参考分发器二进制（日志型 dispatcher） |
| `cmd/distributor-linux` | Linux 分发器：session bus + dcore（`-tags dbus`） |

## 构建与运行

```bash
go test ./...                        # 默认构建（无 dbus 依赖）
go build -tags dbus ./... && dbus-run-session -- go test -tags dbus ./...   # 含 D-Bus 适配层

# S 端
go run ./cmd/server -addr :8443 -db up.db [-tls-cert a.pem -tls-key a.key]

# Linux D 端（session bus 上暴露 org.unifiedpush.Distributor.tsukuyo）
go run -tags dbus ./cmd/distributor-linux -url https://push.example -registry registry.json
```

对第三方应用即插即用：任何实现 `org.unifiedpush.Connector2` 的应用在
session bus 上调用 `org.unifiedpush.Distributor2.Register` 即可完成注册
（e2e 测试展示了完整流程，含注册劫持拒绝与规范错误原因映射）。

## 测试

- `internal/boltstore`：与 `service.MemoryStore` 的语义对齐套件 + 重启持久性。
- `internal/h2srv`：32 并发 h2 请求共享 1 条 TCP 的复用断言。
- `dcore/transport`：sockopt getsockopt 白盒断言（Linux）。
- `test/integration`：S+D 全链路（真加密、投递失败重试、掉线清理、分发器重启续投、受限订阅只允许匹配的 VAPID 推送）。
- `adapters/dbus`：dbus-run-session 下真总线端到端（注册/密文投递/注销/拒绝）。
