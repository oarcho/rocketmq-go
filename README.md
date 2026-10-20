# RocketMQ Go SDK 最小修订

本模块是 Apache RocketMQ Go Client `v5.1.4` 的独立修订版，模块路径为 `github.com/oarcho/rocketmq-go/v5`，当前修订版本为 `v5.1.4-oarcho.2`。保留消费者／生产者入口、协议生成文件、示例和 Apache 2.0 许可；不加入调用方业务、配置或消息协议。

```sh
go get github.com/oarcho/rocketmq-go/v5@v5.1.4-oarcho.2
```

```go
import rmq "github.com/oarcho/rocketmq-go/v5"
```

## 来源与修订范围

原始模块为 [`github.com/apache/rocketmq-clients/golang/v5@v5.1.4`](https://github.com/apache/rocketmq-clients)，模块校验和为 `h1:fGUbM4wpf3G/4UKxpQq7zg1h8eQdCpwcgxvChFJnr+I=`，原始 go.mod 校验和为 `h1:qg/POLGOcuU33gPbi2yA6Ak4kTPydBBamrQU+bl0WMU=`。`LICENSE` 和源文件版权头原样保留；原模块没有独立 NOTICE。生成协议内的上游 `go_package` 描述符保持原样，不构成旧 Go 模块依赖。

首个修订 `v5.1.4-oarcho.1` 修正三个位置：

- `simple_consumer.go`：真实 ACK 响应顶层状态非 OK 时返回 SDK 原有的 `ErrRpcStatus`。仍只发送一次 ACK，仍只执行一次原生命周期回调，不修改 Broker 响应。官方 Proxy 的顶层状态已汇总各 entry；没有另加重复字段校验。
- `conn_options.go`：默认完整 gRPC 请求发送预算声明为 4 MiB。protobuf 封装与消息体共享该预算，不能把它理解为任意 4 MiB 消息体均可发送。
- `conn.go`：把连接原已建立的发送／接收 `callOpts` 实际交给同一连接的 `grpc.WithDefaultCallOptions`。上游只安装接收上限，原 2 MiB 发送声明未生效；本修订没有新增客户端、拦截器或重试框架。

`v5.1.4-oarcho.2` 补齐原请求与 Telemetry 流的取消资源归属：

- `client_manager.go`：11 个一元 RPC 在实际返回时取消自己的期限；Telemetry 只在创建失败或 gRPC 的原生 `OnFinish` 终结回调中释放期限，返回流句柄本身不会取消流。
- `conn.go`：阻塞建连返回即释放拨号期限；真实参数拒绝发生在创建连接 Context 之前。已建立连接仍由原 `Close` 结束。
- `client.go`：每条 Telemetry 流独占取消权和发送互斥；恢复与停机先摘除原资源、锁外取消，再等同一流的发送尾部并关闭发送。旧接收者只可释放自己的流，状态锁不跨网络调用，不新增发送队列或后台工作者。
- `rpc_client.go`：`RpcClient.Telemetry` 接受并透传标准 `...grpc.CallOption`，用来承接真实 gRPC 完成回调。直接调用方可以沿用原调用写法；自定义 `RpcClient` 实现需要同步这个方法签名。现有生成 mock 仅由原接口重新生成，不作为实际资源验证。
- 原客户端在 Start 时独占创建的 manager 随 GracefulStop 一起结束；关闭原连接，取消四个原定时任务并等待它们退出。原首次延时可被停止中断；连接在锁外创建／关闭，竞争落选和关闭后返回的连接由创建者释放，旧错误不会删除新连接。

协议目录、Apache 许可、消费者／生产者入口及原调用方 Context、元数据、流期限保持原样。没有通过提前读取流的 `Context()` 来观察完成，因为 gRPC 规定这会提交当前尝试并禁用后续透明重试；也不把 `CloseSend` 的半关闭当作流的完整结束。

## 模块图与构建

最低 Go 版本为 1.25。依赖的 genproto api／rpc 子模块对齐到 `3700d4141b60`，对应 gRPC、protobuf 和 `golang.org/x` 版本由标准 Go 模块图选择；不使用本地 replace 或工作区专属依赖修正。原始 SDK 的 Go 1.24 及旧依赖版本可通过上游模块校验和复核。

```sh
GOWORK=off go build ./...
```

原 SDK 的测试 `TestMain` 会安装上游 mock，不用其运行结果证明调用方真实消息行为。可用 `go test -c` 编译测试；ACK 拒绝传播、Broker 回执和应用完整链路须由调用方使用真实资源验证。

`go vet ./...` 检查本修订的取消路径。编译、静态检查及测试编译不能证明真实 Broker 下的流恢复、停机和调用方完整链路均已通过；本修订也不宣称已审计或重写上游所有后台任务的生命周期。
