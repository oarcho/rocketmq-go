# RocketMQ Go SDK 最小修订

本模块是 Apache RocketMQ Go Client `v5.1.4` 的独立修订版，模块路径为 `github.com/oarcho/rocketmq-go/v5`，首个修订版本为 `v5.1.4-oarcho.1`。保留原 SDK 的公开接口、协议生成文件、示例和 Apache 2.0 许可；不加入调用方业务、配置或消息协议。

```sh
go get github.com/oarcho/rocketmq-go/v5@v5.1.4-oarcho.1
```

```go
import rmq "github.com/oarcho/rocketmq-go/v5"
```

## 来源与修订范围

原始模块为 [`github.com/apache/rocketmq-clients/golang/v5@v5.1.4`](https://github.com/apache/rocketmq-clients)，模块校验和为 `h1:fGUbM4wpf3G/4UKxpQq7zg1h8eQdCpwcgxvChFJnr+I=`，原始 go.mod 校验和为 `h1:qg/POLGOcuU33gPbi2yA6Ak4kTPydBBamrQU+bl0WMU=`。`LICENSE` 和源文件版权头原样保留；原模块没有独立 NOTICE。生成协议内的上游 `go_package` 描述符保持原样，不构成旧 Go 模块依赖。

运行行为只修正三个位置：

- `simple_consumer.go`：真实 ACK 响应顶层状态非 OK 时返回 SDK 原有的 `ErrRpcStatus`。仍只发送一次 ACK，仍只执行一次原生命周期回调，不修改 Broker 响应。官方 Proxy 的顶层状态已汇总各 entry；没有另加重复字段校验。
- `conn_options.go`：默认完整 gRPC 请求发送预算声明为 4 MiB。protobuf 封装与消息体共享该预算，不能把它理解为任意 4 MiB 消息体均可发送。
- `conn.go`：把连接原已建立的发送／接收 `callOpts` 实际交给同一连接的 `grpc.WithDefaultCallOptions`。上游只安装接收上限，原 2 MiB 发送声明未生效；本修订没有新增客户端、拦截器或重试框架。

其余源码差异仅为模块路径和内部自引用 import 的机械迁移。没有修改协议字段、消费者行为策略或流生命周期。

## 模块图与构建

最低 Go 版本为 1.25。依赖的 genproto api／rpc 子模块对齐到 `3700d4141b60`，对应 gRPC、protobuf 和 `golang.org/x` 版本由标准 Go 模块图选择；不使用本地 replace 或工作区专属依赖修正。原始 SDK 的 Go 1.24 及旧依赖版本可通过上游模块校验和复核。

```sh
GOWORK=off go build ./...
```

原 SDK 的测试 `TestMain` 会安装上游 mock，不用其运行结果证明调用方真实消息行为。可用 `go test -c` 编译测试；ACK 拒绝传播、Broker 回执和应用完整链路须由调用方使用真实资源验证。

当前保留上游 `go vet` 的 13 项 `lostcancel` 告警：`client_manager.go` 12 项、`conn.go` 1 项。其中包含返回长期 stream 的真实资源路径，不能为消除告警直接 `defer cancel`。本修订不把未完成的第三方生命周期整改写成通过，也不扩展这次最小修订范围。
