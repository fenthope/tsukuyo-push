# tsukuyo-push

UnifiedPush 链路中 **S–D 段**的 Go 实现：推送服务器（S）与分发器核心（D），
之间的传输是**单条复用 TCP 连接上的 HTTP/2 长轮询**，S 与 A（应用后端）之间
完全遵循 Web Push（RFC 8030/8188/8291/8292），无自研线路协议。

```
A 应用后端 ──upush/sender──▶ S 推送服务器 ══ h2c/TLS,1×TCP,N×流 ══▶ D 分发器 ──▶ C 端应用(连接器解密)
                            service + h2srv                        dcore + platform/dbus
```

加解密端到端由 C 的连接器完成；S 与 D 只搬运 RFC 8291 密文。
依赖发布版 `github.com/fenthope/tsukasa-push` v0.0.3：S 端直接用其
`service`/`pushmsg`，D 端不依赖 `receiver`（原因与回馈清单见
[docs/upstream-gap.md](docs/upstream-gap.md)）。

## 模块布局

本仓库是**三个 Go 模块**（`go.work` 已提交，本地一起编译）：

| 模块 | 内容 | 额外依赖 |
| --- | --- | --- |
| `.` = `github.com/satomitoka/tsukuyo-push` | **库**：`dcore`（D 核心：订阅管理、轮询监督、内存注册表）、`dcore/transport`（单连接调优客户端）、`h2srv`（S 端调优 HTTP/2 服务）。无 main，无存储 | 无 |
| `./platform/dbus` | UnifiedPush D-Bus 规范 DBUS_0.3.0 分发器适配层，可嵌入 | godbus |
| `./example` | 参考实现：`cmd/{server,distributor,distributor-linux}` 三个二进制 + `boltstore`（`service.Store` 的 bbolt 实现，未 ack 消息重启后重投＝至少一次） | bbolt |

库模块的直接依赖只有 `tsukasa-push` 与 `x/sys`（`touka` 仅测试用到）；
**bbolt 与 godbus 不会传染给 import 本库的人**。库自身不落盘：S 端的存储由调
用方实现 `service.Store`；D 端把注册表放在内存里工作，变更通过
`dcore.Options.OnChanged` 交出去、启动时用 `Distributor.Restore` 收回来（`OnChanged`
失败会让 `Register` 失败，因此不会把重启后留不住端点发给应用）。

自己装配一个 S 端只要三行（`example/cmd/server` 就是这么写的）：

```go
engine := touka.Default()
service.New(service.WithStore(myStore)).Register(engine)
srv := h2srv.New(engine, h2srv.Options{MaxConcurrentStreams: 512})
```

## 构建与运行

```bash
go test ./...                      # 库模块（根目录；./... 不跨模块，需逐个跑）
(cd example && go test ./...)      # 含 bbolt store 的语义对齐与重启持久性
(cd platform/dbus && go test ./...)  # 够不到 session bus 时自动 skip

# S 端（example 模块）
(cd example && go run ./cmd/server -addr :8443 -db up.db [-tls-cert a.pem -tls-key a.key])
# Linux D 端（session bus 上暴露 org.unifiedpush.Distributor.tsukuyo）
(cd example && go run ./cmd/distributor-linux -url https://push.example -registry registry.json)
```

`-tags dbus` 不再需要：模块边界本身就是隔离，比构建标签更难用错。

对第三方应用即插即用：任何实现 `org.unifiedpush.Connector2` 的应用在
session bus 上调用 `org.unifiedpush.Distributor2.Register` 即可完成注册
（e2e 测试展示了完整流程，含注册劫持拒绝与规范错误原因映射）。

## 可调参数

默认值都写在字段的 godoc 里，这里只给地图（按会碰到的频率排序）：

- **D 端 `dcore.Options`**：`PollRetryDelay` / `MaxPollRetryDelay`（1s / 60s，指数退避带抖动；常换网络的设备可调小上限）、`DeliverRetryDelay`（2s）、`OnChanged` / `OnGone` / `Logger`。传输层从 `dcore.WithTransportOptions` 进去，实际落到 socket 的取值见 `dcore/transport.Options` 的注释（`TCP_NODELAY` 显式下发，keepalive 走 `net.Dialer.KeepAliveConfig`，`Proxy` 故意为 nil 即不读 `HTTPS_PROXY`）。
- **S 端 `h2srv.Options` 与 `cmd/server` flags**：`-max-streams`(512)、`-stream-window`(256KiB)、`-conn-window`(8MiB)、`-idle-timeout`(30s)、`-ping-timeout`(15s)、`-listen-recv-buf` / `-listen-send-buf`(4MiB，填 0 交给内核自动调优)、`-sweep-interval`(1m)、`-shutdown-timeout`(10s)。

两条经验规则：

1. 发现死连接要等 `-idle-timeout` + `-ping-timeout`，想让掉线快于 NAT 生命周期，调这两个。
2. `-max-streams` 必须大于最大分发器的常驻订阅数加上其 ack/注册请求的余量；不够时轮询流会被 REFUSED 后重建，表现为投递延迟毛刺而非失败。

每个订阅的常驻成本是实测值：`go test -run '^$' -bench Scale ./dcore/`，约 3.6 KiB 堆 + 1 个轮询 goroutine，其中注册记录本身只占 261 B。

## 测试

- `example/boltstore`：与 `service.MemoryStore` 的语义对齐套件 + 重启持久性。
- `h2srv`：32 并发 h2 请求共享 1 条 TCP 的复用断言。
- `dcore/transport`：sockopt getsockopt 白盒断言（Linux），外加"默认 `Options{}` 经真实 dial 后落到 socket 上的取值"断言——keepalive 由 `net.Dialer` 在 `Control` 之后应用，只有走 dial 路径才看得见。
- `dcore`：注册表基准与每订阅内存足迹（`go test -run '^$' -bench . ./dcore/`）。
- `test/integration`：S+D 全链路（真加密、投递失败重试、掉线清理、分发器重启续投、受限订阅只允许匹配的 VAPID 推送）。
- `platform/dbus`：真总线端到端（注册/密文投递/注销/拒绝）；够不到 session bus 时 skip。
