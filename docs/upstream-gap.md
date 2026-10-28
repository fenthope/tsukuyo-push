# 与 tsukasa-push 上游的差异记录

本文件记录 tsukuyo-push 相对依赖库 `github.com/fenthope/tsukasa-push` 的取舍与上游改进建议。约定：暂不改动上游，本地绕开并在此登记，供日后回馈。

## 1. `receiver` 把“分发器”和“连接器”两种角色混在一起

### 现象

`tsukasa-push/receiver` 的 `Receiver.Subscribe` 在创建订阅时用
`pushmsg.GenerateUAKeys()` **自动生成 UA 的 ECDH 密钥对**，并把私钥塞进返回的
`Subscription.PrivateKey`，随后 `Receiver.Poll` 用该私钥**在本地解密**消息。

这条路径对应的是 UnifiedPush 里的**连接器 / 终端应用**角色：谁持有 ECDH 私钥，谁就能读明文。

### 为什么分发器模式不能复用它

UnifiedPush 的分发器（本项目 `dcore`，链路中的 D）**必须只转发密文**：
密钥由终端应用侧的连接器生成并保管，应用服务器用该公钥加密（RFC 8291），
分发器从头到尾不应看到明文。若分发器复用 `receiver`，就等于让分发器替所有
应用持有并解密私钥——既违反端到端加密的边界，也无法向多个应用分发各自的密文。

因此 `dcore` 自带了一个只做订阅管理 + 密文轮询的轻量 client
（`dcore/client.go`），不生成、不持有、不使用任何密钥。

### 上游改进建议（回馈时用）

把 `receiver` 拆成两层，使分发器可复用“传输/轮询”部分而不被强绑“解密”部分：

1. `Subscription` 的密钥字段改为可选；`Subscribe` 提供
   `WithExistingKeys(*pushmsg.UAKeys)` 与“无密钥订阅”两种模式。
2. `Poll` 拆出 `PollRaw`（返回 `[]byte` 密文，不解密），现有 `Poll`
   在持有私钥时于其上叠加解密。
3. `AppSubscription()` 在分发器模式下才用于“把公钥/端点转交应用服务器”，
   连接器模式下则密钥留在本地——目前该语义已隐含，但没有类型层面的约束。

> 结论：本仓库不依赖 `receiver`/`upush/device`；`dcore` 自持 client 是有意为之，
> 不是重复造轮子。上游若按上述拆分，`dcore/client.go` 可回退为薄封装。

## 2. 长轮询承载 HTTP/2 server push

`tsukasa-push` 服务端 `service` 明确用长轮询替代 RFC 8030 §6 的 H2 server push
（见其 README《与 RFC 的差异》第 1 条）。这与本项目“单条 HTTP/2 连接上开多路
长轮询流”的传输复用目标天然契合，无需改动，直接沿用。

## 3. `/subscribe` 的受限订阅（restricted key）

`POST /subscribe` 接受 `{"vapid": "<b64url P-256>"}` 把订阅限制到单个应用服务器
（RFC 8292 §4.1）。UnifiedPush 的 D-Bus `Register` 携带同名 `vapid` 字段，二者语义
一致，`dcore` 直接透传（`Subscribe(ctx, restrictedKeyB64)`），无差异。
