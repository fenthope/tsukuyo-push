# 与 tsukasa-push 上游的差异记录

本文件记录 tsukuyo-push 相对依赖库 `github.com/fenthope/tsukasa-push` 的取舍与上游改进建议。约定：暂不改动上游，本地绕开并在此登记，供日后回馈。

当前依赖：**发布版 v0.0.3**（不再使用本地 replace）。

## 1. `receiver` 的角色混合 —— ✅ v0.0.3 已拆分

### 原现象

`receiver.Subscribe` 在创建订阅时用 `pushmsg.GenerateUAKeys()` 自动生成 UA 的
ECDH 密钥对，把私钥塞进 `Subscription.PrivateKey`，随后 `Receiver.Poll` 用该
私钥在本地解密。这条路径对应 UnifiedPush 的**连接器/终端应用**角色：谁持有
私钥，谁能读明文。

UnifiedPush 的分发器（本项目 `dcore`，链路中的 D）**必须只转发密文**：密钥由
终端应用侧的连接器生成并保管，应用服务器用该公钥加密（RFC 8291），分发器从
头到尾不应看到明文。若分发器复用旧版 `receiver`，等于让它替所有应用持有并
解密私钥——既违反端到端边界，也无法向多个应用分发各自的密文。

### v0.0.3 落地情况（fenthope/tsukasa-push#1）

拆成两个类型，角色由类型系统强制，比原建议更彻底：

| 原建议 | v0.0.3 实现 |
| --- | --- |
| 密钥字段可选、提供无密钥订阅 | 无密钥的 `Subscription`（`SubscribeBare`）与持钥的 `KeyedSubscription`（`Subscribe`）分为两个类型 |
| 拆出 `PollRaw` | `PollRaw(ctx, sub)` 返回 `*RawMessage` 密文；`Poll` 在其上叠加解密 |
| 类型层面约束角色 | 分发器侧类型（`Subscription`/`RawMessage`）**不存在任何解密路径**；`Ack` 同时接受密文与明文消息 |
| 复用既有密钥 | `Subscribe(WithExistingKeys(*pushmsg.UAKeys))` |

204/404/410 也细分成了 `ErrNoMessage`/`ErrSubscriptionExpired`/`ErrSubscriptionGone`。

### 结论：`dcore/client.go` 仍不能整体回退为薄封装，卡在 1.1 与 1.2

1.1 的订阅（受限密钥）与 1.2 的传输注入必须自持；只把
Poll/Ack/Unsubscribe 交给 receiver，会额外引入 httpc 这第二套 HTTP 栈却省不
下代码。两者落地后可整体回退。生产代码不依赖 `receiver`/`upush`（测试用
`upush` 扮演应用服务器）。

### 1.1 受限密钥只能从**私钥**构造，且作用域是 Receiver 而非订阅（未闭合）

`receiver.WithRestrictedKey(signer *vapid.Signer)` 有两处对不上分发器：

1. `vapid` 包只有 `NewSigner(private *ecdsa.PrivateKey)`，即**必须持有私钥**。
   而分发器收到的是应用服务器的**公钥** b64——UnifiedPush D-Bus `Register` 的
   `vapid` 参数如此，服务端 `parseRestriction` 也如此（`vapid.DecodeKeyB64`
   后存公钥）。分发器无从构造 `Signer`。
2. 该选项作用于 `Receiver` 实例；分发器是"一个 client、N 个应用、N 份各自的
   受限密钥"。

**上游建议**：增加 `WithRestrictedKeyB64(string)`，并把受限密钥下沉为
per-subscription 选项（`SubscribeBare(ctx, WithRestrictedKeyB64(k))`），入参
用 `vapid.DecodeKeyB64` 校验。

### 1.2 无法注入原始 RoundTripper：httpc 的"非零字段合并"会吃掉调优（未闭合）

`receiver` 只接受 `*httpc.Client`，而 `httpc.WithTransport` 是把自定义
`*http.Transport` 的**非零**字段合并进它自己的默认 Transport
（`options.go:mergeTransport`）。D 端传输层是刻意调过的，其中"零值即意图"的
字段会被 httpc 的默认值覆盖。实测（httpc v0.10.0 + `dcore/transport` 默认参
数，反射读取合并后的 Transport）：

| 字段 | 我们的调优 | 经 httpc 合并后 |
| --- | --- | --- |
| `IdleConnTimeout` | `0`＝永不过期（单条连接保持热度） | **90s**（被默认值覆盖） |
| `Proxy` | `nil`＝不经代理 | **`http.ProxyFromEnvironment`** |
| `ForceAttemptHTTP2` | `false` | `true` |

`DialContext`/`HTTP2`/`Protocols`/缓冲大小等非零字段则原样保留。

后果不是纸面的：`deliver` 重试期间轮询请求已经返回、连接处于空闲，90s 后即
被回收，下一条消息要重建 TCP + H2 握手——正是 `dcore/transport` 写明的"多出
一条连接就多耗一份电"。`Proxy` 一项更会让 prior-knowledge h2c 经代理后直接
不通。二者虽然都能靠再套一层 httpc 选项或塞一个"返回 nil 的 Proxy 函数"绕
开，但每个意图都要按 httpc 的怪癖重述一遍，且 httpc 默认值一改就会静默重调
我们的信道。

**上游建议**：`receiver.WithRoundTripper(http.RoundTripper)`，或提供"整体替换
而非合并"的 Transport 选项。

## 2. 长轮询承载 HTTP/2 server push

`tsukasa-push` 服务端 `service` 明确用长轮询替代 RFC 8030 §6 的 H2 server push
（见其 README《与 RFC 的差异》第 1 条）。这与本项目"单条 HTTP/2 连接上开多路
长轮询流"的传输复用目标天然契合，无需改动，直接沿用。

## 3. 订阅 options 的 media type：必须是 RFC 8292 的那个（本地已修）

`POST /subscribe` 携带受限密钥时，Content-Type 必须是
`application/webpush-options+json`（`pushmsg.OptionsMediaType`，RFC 8292 §4.2）。
服务端 `parseRestriction` 以 `strings.HasPrefix(contentType, OptionsMediaType)`
为开关，**不匹配就静默丢弃 body**：订阅照样创建成功，但完全不受限，任何应用
服务器都能往里推——一个不报错的权限缺口。

`dcore` 早期写成了 `application/unifiedpush-options+json`（UnifiedPush 只规定
D-Bus 侧的 `vapid` 参数名，没有为线路协议另立 media type），现改用
`pushmsg.OptionsMediaType` 常量以免漂移，并在
`test/integration/restricted_test.go` 加了回归：外来 VAPID 必须 `ErrVAPIDRejected`、
无 VAPID 必须 `ErrVAPIDRequired`、匹配的密钥才能投递。

> 回馈角度：服务端对"看起来像订阅选项却解析不了"的 body 静默放行，值得在上
> 游考虑收紧（例如 body 非空而 media type 不匹配时报 415），否则任何客户端的
> 拼写错误都会退化成"受限订阅悄悄不受限"。

## 4. `/subscribe` 的受限订阅透传

UnifiedPush D-Bus `Register` 的 `vapid` 字段与 RFC 8292 §4.1 的受限订阅语义一
致，`dcore` 直接透传（`Subscribe(ctx, restrictedKeyB64)`），线路协议层面无差
异；类型层面的错配见 1.1。
