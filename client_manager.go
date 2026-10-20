/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package golang

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/oarcho/rocketmq-go/v5/pkg/utils"
	v2 "github.com/oarcho/rocketmq-go/v5/protocol/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ClientManager interface {
	RegisterClient(client Client)
	UnRegisterClient(client Client)
	QueryRoute(ctx context.Context, endpoints *v2.Endpoints, request *v2.QueryRouteRequest, duration time.Duration) (*v2.QueryRouteResponse, error)
	QueryAssignments(ctx context.Context, endpoints *v2.Endpoints, request *v2.QueryAssignmentRequest, duration time.Duration) (*v2.QueryAssignmentResponse, error)
	HeartBeat(ctx context.Context, endpoints *v2.Endpoints, request *v2.HeartbeatRequest, duration time.Duration) (*v2.HeartbeatResponse, error)
	SendMessage(ctx context.Context, endpoints *v2.Endpoints, request *v2.SendMessageRequest, duration time.Duration) (*v2.SendMessageResponse, error)
	Telemetry(ctx context.Context, endpoints *v2.Endpoints, duration time.Duration) (v2.MessagingService_TelemetryClient, error)
	EndTransaction(ctx context.Context, endpoints *v2.Endpoints, request *v2.EndTransactionRequest, duration time.Duration) (*v2.EndTransactionResponse, error)
	NotifyClientTermination(ctx context.Context, endpoints *v2.Endpoints, request *v2.NotifyClientTerminationRequest, duration time.Duration) (*v2.NotifyClientTerminationResponse, error)
	ReceiveMessage(ctx context.Context, endpoints *v2.Endpoints, request *v2.ReceiveMessageRequest) (v2.MessagingService_ReceiveMessageClient, error)
	AckMessage(ctx context.Context, endpoints *v2.Endpoints, request *v2.AckMessageRequest, duration time.Duration) (*v2.AckMessageResponse, error)
	ChangeInvisibleDuration(ctx context.Context, endpoints *v2.Endpoints, request *v2.ChangeInvisibleDurationRequest, duration time.Duration) (*v2.ChangeInvisibleDurationResponse, error)
	ForwardMessageToDeadLetterQueue(ctx context.Context, endpoints *v2.Endpoints, request *v2.ForwardMessageToDeadLetterQueueRequest, duration time.Duration) (*v2.ForwardMessageToDeadLetterQueueResponse, error)
	SyncLiteSubscription(ctx context.Context, endpoints *v2.Endpoints, request *v2.SyncLiteSubscriptionRequest, duration time.Duration) (*v2.SyncLiteSubscriptionResponse, error)
	RecallMessage(ctx context.Context, endpoints *v2.Endpoints, request *v2.RecallMessageRequest, duration time.Duration) (*v2.RecallMessageResponse, error)
}

type clientManagerOptions struct {
	RPC_CLIENT_MAX_IDLE_DURATION time.Duration

	RPC_CLIENT_IDLE_CHECK_INITIAL_DELAY time.Duration
	RPC_CLIENT_IDLE_CHECK_PERIOD        time.Duration

	HEART_BEAT_INITIAL_DELAY time.Duration
	HEART_BEAT_PERIOD        time.Duration

	LOG_STATS_INITIAL_DELAY time.Duration
	LOG_STATS_PERIOD        time.Duration

	SYNC_SETTINGS_DELAY  time.Duration
	SYNC_SETTINGS_PERIOD time.Duration
}

var defaultClientManagerOptions = clientManagerOptions{
	RPC_CLIENT_MAX_IDLE_DURATION: time.Minute * 30,

	RPC_CLIENT_IDLE_CHECK_INITIAL_DELAY: time.Second * 5,
	RPC_CLIENT_IDLE_CHECK_PERIOD:        time.Minute * 1,

	HEART_BEAT_INITIAL_DELAY: time.Second * 1,
	HEART_BEAT_PERIOD:        time.Second * 10,

	LOG_STATS_INITIAL_DELAY: time.Second * 60,
	LOG_STATS_PERIOD:        time.Second * 60,

	SYNC_SETTINGS_DELAY:  time.Second * 1,
	SYNC_SETTINGS_PERIOD: time.Minute * 5,
}

type defaultClientManager struct {
	rpcClientTable     map[string]RpcClient
	rpcClientTableLock sync.RWMutex
	clientTable        sync.Map
	done               chan struct{}
	workers            sync.WaitGroup
	opts               clientManagerOptions
}

var _ = ClientManager(&defaultClientManager{})

var NewDefaultClientManager = func() *defaultClientManager {
	return &defaultClientManager{
		rpcClientTable: make(map[string]RpcClient),
		done:           make(chan struct{}),
		opts:           defaultClientManagerOptions,
	}
}

func (cm *defaultClientManager) RegisterClient(client Client) {
	cm.clientTable.Store(client.GetClientID(), client)
}

func (cm *defaultClientManager) UnRegisterClient(client Client) {
	cm.clientTable.Delete(client.GetClientID())
}

// startUp 仍只启动四个原定时任务；同一 manager 负责它们的停止信号与真实退出。
func (cm *defaultClientManager) startUp() {
	sugarBaseLogger.Info("begin to start the client manager")

	cm.workers.Add(4)
	go cm.runScheduled(cm.opts.RPC_CLIENT_IDLE_CHECK_INITIAL_DELAY, cm.opts.RPC_CLIENT_IDLE_CHECK_PERIOD, cm.clearIdleRpcClients)
	go cm.runScheduled(cm.opts.HEART_BEAT_INITIAL_DELAY, cm.opts.HEART_BEAT_PERIOD, cm.doHeartbeat)
	go cm.runScheduled(cm.opts.LOG_STATS_INITIAL_DELAY, cm.opts.LOG_STATS_PERIOD, cm.doStats)
	go cm.runScheduled(cm.opts.SYNC_SETTINGS_DELAY, cm.opts.SYNC_SETTINGS_PERIOD, cm.syncSettings)

	sugarBaseLogger.Info("the client manager starts successfully")
}

// runScheduled 保留原首次延时与后续 tick 节奏，同时让停机可中断初始等待。
// 不再由首次任务派生第二个后台循环；返回即释放 timer／ticker，并交回本 worker 的归属。
func (cm *defaultClientManager) runScheduled(initialDelay, period time.Duration, action func()) {
	defer cm.workers.Done()
	initial := time.NewTimer(initialDelay)
	defer initial.Stop()
	select {
	case <-cm.done:
		return
	case <-initial.C:
	}
	// 关闭与初始 timer 可能同时就绪，实际副作用紧前再确认原拥有者仍在运行。
	select {
	case <-cm.done:
		return
	default:
	}
	action()
	periodic := time.NewTicker(period)
	defer periodic.Stop()
	for {
		select {
		case <-cm.done:
			return
		case <-periodic.C:
			select {
			case <-cm.done:
				return
			default:
			}
			action()
		}
	}
}

// deleteRpcClient 只摘除仍属同一实例的连接；旧调用的迟到错误不能删除新连接。
// 连接真正关闭在索引锁外，避免网络收尾阻塞同池的其它状态访问。
func (cm *defaultClientManager) deleteRpcClient(rpcClient RpcClient) {
	target := rpcClient.GetTarget()
	cm.rpcClientTableLock.Lock()
	if cm.rpcClientTable[target] != rpcClient {
		cm.rpcClientTableLock.Unlock()
		return
	}
	delete(cm.rpcClientTable, target)
	cm.rpcClientTableLock.Unlock()
	rpcClient.GracefulStop()
}

// clearIdleRpcClients 只快照池中实例，空闲判断和连接关闭不占用全局索引锁。
func (cm *defaultClientManager) clearIdleRpcClients() {
	cm.rpcClientTableLock.RLock()
	clients := make([]RpcClient, 0, len(cm.rpcClientTable))
	for _, rpcClient := range cm.rpcClientTable {
		clients = append(clients, rpcClient)
	}
	cm.rpcClientTableLock.RUnlock()
	for _, rpcClient := range clients {
		idleDuration := rpcClient.idleDuration()
		if idleDuration > cm.opts.RPC_CLIENT_MAX_IDLE_DURATION {
			cm.deleteRpcClient(rpcClient)
			sugarBaseLogger.Warnf("rpc client has been idle for a long time, target=%s, idleDuration=%d, rpcClientMaxIdleDuration=%d\n", rpcClient.GetTarget(), idleDuration, cm.opts.RPC_CLIENT_MAX_IDLE_DURATION)
		}
	}
}
func (cm *defaultClientManager) doHeartbeat() {
	sugarBaseLogger.Debug("clientManager start doHeartbeat")
	cm.clientTable.Range(func(_, v interface{}) bool {
		client := v.(*defaultClient)
		client.Heartbeat()
		return true
	})
}
func (cm *defaultClientManager) doStats() {
	// TODO
}
func (cm *defaultClientManager) syncSettings() {
	sugarBaseLogger.Info("clientManager start syncSettings")
	cm.clientTable.Range(func(_, v interface{}) bool {
		client := v.(*defaultClient)
		client.trySyncSettings()
		return true
	})
}

// shutdown 只由本客户端拥有者调用一次；关闭广播不等待任何 worker 恰好接收信号。
// 先关闭已有连接解除实际 RPC，再等待四个原任务退出，不留下重建连接的后台拥有者。
func (cm *defaultClientManager) shutdown() {
	sugarBaseLogger.Info("begin to shutdown the client manager")
	close(cm.done)
	cm.cleanRpcClient()
	cm.workers.Wait()
	sugarBaseLogger.Info("shutdown the client manager successfully")
}

// cleanRpcClient 先在索引锁内摘除现有连接，锁外关闭，避免全局索引锁跨平台／网络收尾。
func (cm *defaultClientManager) cleanRpcClient() {
	sugarBaseLogger.Info("clientManager start cleanRpcClient")
	cm.rpcClientTableLock.Lock()
	clients := make([]RpcClient, 0, len(cm.rpcClientTable))
	for target, rpcClient := range cm.rpcClientTable {
		delete(cm.rpcClientTable, target)
		clients = append(clients, rpcClient)
	}
	cm.rpcClientTableLock.Unlock()
	for _, rpcClient := range clients {
		rpcClient.GracefulStop()
	}
}

// getRpcClient 在锁外创建真实连接，再以原拥有者和同目标实例双检接纳。
// 竞争落选或关闭后才返回的连接由本创建者关闭，不能漏进已清理的池或持锁 Dial。
func (cm *defaultClientManager) getRpcClient(endpoints *v2.Endpoints) (RpcClient, error) {
	var target string
	if endpoints.GetScheme() == v2.AddressScheme_IPv4 || endpoints.GetScheme() == v2.AddressScheme_IPv6 {
		serviceName := utils.EndpointsToString(endpoints)
		target = fmt.Sprintf("%s:///%s", DefaultScheme, serviceName)
	} else {
		target = utils.ParseAddress(utils.SelectAnAddress(endpoints))
	}
	cm.rpcClientTableLock.RLock()
	select {
	case <-cm.done:
		cm.rpcClientTableLock.RUnlock()
		return nil, fmt.Errorf("client manager has been shut down")
	default:
	}
	item, ok := cm.rpcClientTable[target]
	cm.rpcClientTableLock.RUnlock()
	if ok {
		return item, nil
	}

	candidate, err := NewRpcClient(target)
	if err != nil {
		return nil, err
	}
	cm.rpcClientTableLock.Lock()
	// Dial 期间可能已经关闭；不得在 shutdown 的快照之后重新把连接放入池。
	select {
	case <-cm.done:
		cm.rpcClientTableLock.Unlock()
		candidate.GracefulStop()
		return nil, fmt.Errorf("client manager has been shut down")
	default:
	}
	item, ok = cm.rpcClientTable[target]
	if !ok {
		cm.rpcClientTable[target] = candidate
	}
	cm.rpcClientTableLock.Unlock()
	if ok {
		candidate.GracefulStop()
		return item, nil
	}
	return candidate, nil
}
func (cm *defaultClientManager) handleGrpcError(rpcClient RpcClient, err error) {
	if err != nil {
		if e, ok := status.FromError(err); ok {
			if e.Code() == codes.Unavailable {
				sugarBaseLogger.Errorf("happened unavailable err=%w, close rpcClient=%s", err, rpcClient.GetTarget())
				cm.deleteRpcClient(rpcClient)
			}
		}
	}
}

// QueryRoute 在调用方上下文内查询路由；返回即释放本次 RPC 的期限与父链引用。
func (cm *defaultClientManager) QueryRoute(ctx context.Context, endpoints *v2.Endpoints, request *v2.QueryRouteRequest, duration time.Duration) (*v2.QueryRouteResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	rpcClient, err := cm.getRpcClient(endpoints)
	if err != nil {
		return nil, err
	}
	ret, err := rpcClient.QueryRoute(ctx, request)
	cm.handleGrpcError(rpcClient, err)
	return ret, err
}

// QueryAssignments 查询消费分配；建连失败和正常响应都由本调用释放期限资源。
func (cm *defaultClientManager) QueryAssignments(ctx context.Context, endpoints *v2.Endpoints, request *v2.QueryAssignmentRequest, duration time.Duration) (*v2.QueryAssignmentResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	rpcClient, err := cm.getRpcClient(endpoints)
	if err != nil {
		return nil, err
	}
	ret, err := rpcClient.QueryAssignments(ctx, request)
	cm.handleGrpcError(rpcClient, err)
	return ret, err
}

// SendMessage 等待原发送 RPC 的实际返回；收尾仅释放资源，不增加发送或重试。
func (cm *defaultClientManager) SendMessage(ctx context.Context, endpoints *v2.Endpoints, request *v2.SendMessageRequest, duration time.Duration) (*v2.SendMessageResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	rpcClient, err := cm.getRpcClient(endpoints)
	if err != nil {
		return nil, err
	}
	ret, err := rpcClient.SendMessage(ctx, request)
	cm.handleGrpcError(rpcClient, err)
	return ret, err
}

// Telemetry 的期限属于真实双向流，不能在返回流句柄时就取消。
// gRPC 完成回调覆盖正常结束、接收错误、父取消和连接关闭；创建失败则在本调用释放。
func (cm *defaultClientManager) Telemetry(ctx context.Context, endpoints *v2.Endpoints, duration time.Duration) (v2.MessagingService_TelemetryClient, error) {
	ctx, cancel := context.WithTimeout(ctx, duration)
	rpcClient, err := cm.getRpcClient(endpoints)
	if err != nil {
		cancel()
		return nil, err
	}
	ret, err := rpcClient.Telemetry(ctx, grpc.OnFinish(func(error) { cancel() }))
	if err != nil {
		cancel()
	}
	cm.handleGrpcError(rpcClient, err)
	return ret, err
}

// EndTransaction 保持调用方取消链；事务 RPC 结束时释放其独占期限。
func (cm *defaultClientManager) EndTransaction(ctx context.Context, endpoints *v2.Endpoints, request *v2.EndTransactionRequest, duration time.Duration) (*v2.EndTransactionResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	rpcClient, err := cm.getRpcClient(endpoints)
	if err != nil {
		return nil, err
	}
	ret, err := rpcClient.EndTransaction(ctx, request)
	cm.handleGrpcError(rpcClient, err)
	return ret, err
}

// HeartBeat 发送一次心跳；高频成功不应使已结束 RPC 的计时器存活到超时。
func (cm *defaultClientManager) HeartBeat(ctx context.Context, endpoints *v2.Endpoints, request *v2.HeartbeatRequest, duration time.Duration) (*v2.HeartbeatResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	rpcClient, err := cm.getRpcClient(endpoints)
	if err != nil {
		return nil, err
	}

	ret, err := rpcClient.HeartBeat(ctx, request)
	cm.handleGrpcError(rpcClient, err)
	return ret, err
}

// NotifyClientTermination 在既定预算内通知终止，返回后不保留关闭阶段的期限资源。
func (cm *defaultClientManager) NotifyClientTermination(ctx context.Context, endpoints *v2.Endpoints, request *v2.NotifyClientTerminationRequest, duration time.Duration) (*v2.NotifyClientTerminationResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	rpcClient, err := cm.getRpcClient(endpoints)
	if err != nil {
		return nil, err
	}
	ret, err := rpcClient.NotifyClientTermination(ctx, request)
	cm.handleGrpcError(rpcClient, err)
	return ret, err
}

func (cm *defaultClientManager) ReceiveMessage(ctx context.Context, endpoints *v2.Endpoints, request *v2.ReceiveMessageRequest) (v2.MessagingService_ReceiveMessageClient, error) {
	rpcClient, err := cm.getRpcClient(endpoints)
	if err != nil {
		return nil, err
	}
	ret, err := rpcClient.ReceiveMessage(ctx, request)
	cm.handleGrpcError(rpcClient, err)
	return ret, err
}

// AckMessage 只确认原请求；RPC 返回后释放计时器，不把取消当作 ACK 成功。
func (cm *defaultClientManager) AckMessage(ctx context.Context, endpoints *v2.Endpoints, request *v2.AckMessageRequest, duration time.Duration) (*v2.AckMessageResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	rpcClient, err := cm.getRpcClient(endpoints)
	if err != nil {
		return nil, err
	}
	ret, err := rpcClient.AckMessage(ctx, request)
	cm.handleGrpcError(rpcClient, err)
	return ret, err
}

// ChangeInvisibleDuration 修改本次不可见期；原结果与错误保留，期限在返回时回收。
func (cm *defaultClientManager) ChangeInvisibleDuration(ctx context.Context, endpoints *v2.Endpoints, request *v2.ChangeInvisibleDurationRequest, duration time.Duration) (*v2.ChangeInvisibleDurationResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	rpcClient, err := cm.getRpcClient(endpoints)
	if err != nil {
		return nil, err
	}
	ret, err := rpcClient.ChangeInvisibleDuration(ctx, request)
	cm.handleGrpcError(rpcClient, err)
	return ret, err
}

// ForwardMessageToDeadLetterQueue 沿原请求执行移交，成功与失败均释放本次 RPC 的期限。
func (cm *defaultClientManager) ForwardMessageToDeadLetterQueue(ctx context.Context, endpoints *v2.Endpoints, request *v2.ForwardMessageToDeadLetterQueueRequest, duration time.Duration) (*v2.ForwardMessageToDeadLetterQueueResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	rpcClient, err := cm.getRpcClient(endpoints)
	if err != nil {
		return nil, err
	}
	ret, err := rpcClient.ForwardMessageToDeadLetterQueue(ctx, request)
	cm.handleGrpcError(rpcClient, err)
	return ret, err
}

// SyncLiteSubscription 同步轻量订阅；不让已完成同步继续占用父 Context 与计时器。
func (cm *defaultClientManager) SyncLiteSubscription(ctx context.Context, endpoints *v2.Endpoints, request *v2.SyncLiteSubscriptionRequest,
	duration time.Duration) (*v2.SyncLiteSubscriptionResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	rpcClient, err := cm.getRpcClient(endpoints)
	if err != nil {
		return nil, err
	}
	ret, err := rpcClient.SyncLiteSubscription(ctx, request)
	cm.handleGrpcError(rpcClient, err)
	return ret, err
}

// RecallMessage 保留撤回 RPC 的实际结局，调用返回只回收该期限，不重放撤回。
func (cm *defaultClientManager) RecallMessage(ctx context.Context, endpoints *v2.Endpoints, request *v2.RecallMessageRequest, duration time.Duration) (*v2.RecallMessageResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	rpcClient, err := cm.getRpcClient(endpoints)
	if err != nil {
		return nil, err
	}
	ret, err := rpcClient.RecallMessage(ctx, request)
	cm.handleGrpcError(rpcClient, err)
	return ret, err
}
