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
	"errors"
	"fmt"

	"github.com/oarcho/rocketmq-go/v5/pkg/utils"

	validator "github.com/go-playground/validator/v10"
	"github.com/oarcho/rocketmq-go/v5/pkg/grpc/middleware/zaplog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	CLIENT_ENABLE_SSL = "rocketmq.client.enableSsl"
)

var (
	ErrNoAvailableEndpoints = errors.New("rocketmq: no available endpoints")
	EnableSsl               = true
)

type ClientConnFunc func(string, ...ConnOption) (ClientConn, error)

type ClientConn interface {
	Conn() *grpc.ClientConn
	Close() error
}

var _ = ClientConn(&clientConn{})

type clientConn struct {
	opts     connOptions
	creds    credentials.TransportCredentials
	ctx      context.Context
	cancel   context.CancelFunc
	callOpts []grpc.CallOption
	conn     *grpc.ClientConn
	validate *validator.Validate
}

// NewClientConn 校验真实连接预算后才创建可取消资源；成功连接的取消权交给 Close。
var NewClientConn = func(endpoint string, opts ...ConnOption) (ClientConn, error) {
	client := &clientConn{
		opts:     defaultConnOptions,
		validate: validator.New(),
	}
	if len(endpoint) == 0 {
		return nil, ErrNoAvailableEndpoints
	}
	for _, opt := range opts {
		opt.apply(&client.opts)
	}

	client.creds = credentials.NewTLS(client.opts.TLS)

	if client.opts.MaxCallSendMsgSize > 0 || client.opts.MaxCallRecvMsgSize > 0 {
		if client.opts.MaxCallRecvMsgSize > 0 && client.opts.MaxCallSendMsgSize > client.opts.MaxCallRecvMsgSize {
			return nil, fmt.Errorf("gRPC message recv limit (%d bytes) must be greater than send limit (%d bytes)", client.opts.MaxCallRecvMsgSize, client.opts.MaxCallSendMsgSize)
		}
		if client.opts.MaxCallSendMsgSize > 0 {
			client.callOpts = append(client.callOpts, grpc.MaxCallSendMsgSize(client.opts.MaxCallSendMsgSize))
		}
		if client.opts.MaxCallRecvMsgSize > 0 {
			client.callOpts = append(client.callOpts, grpc.MaxCallRecvMsgSize(client.opts.MaxCallRecvMsgSize))
		}
	}

	// 预算拒绝尚未分配 Context；开始实际建连后，失败由此处取消，成功由连接释放。
	baseCtx := context.TODO()
	if client.opts.Context != nil {
		baseCtx = client.opts.Context
	}
	client.ctx, client.cancel = context.WithCancel(baseCtx)

	conn, err := client.dial(endpoint)
	if err != nil {
		client.cancel()
		return nil, err
	}
	client.conn = conn

	return client, nil
}

func (c *clientConn) Conn() *grpc.ClientConn {
	return c.conn
}

func (c *clientConn) Close() error {
	defer c.cancel()
	return c.conn.Close()
}

func (c *clientConn) dialSetupOpts(dopts ...grpc.DialOption) (opts []grpc.DialOption, err error) {
	opts = append(opts, dopts...)
	if c.creds != nil && EnableSsl {
		opts = append(opts, grpc.WithTransportCredentials(c.creds))
	} else {
		opts = append(opts, grpc.WithInsecure())
	}
	// TODO get requestID in header
	opts = append(opts, grpc.WithBlock(), grpc.WithChainUnaryInterceptor(
		zaplog.UnaryClientInterceptor(c.opts.Logger),
	))
	return
}

// dial 只拥有本次阻塞建连的期限，连接成功后仍由 clientConn.Close 结束其真正生命周期。
func (c *clientConn) dial(target string, dopts ...grpc.DialOption) (*grpc.ClientConn, error) {
	opts, err := c.dialSetupOpts(dopts...)
	if err != nil {
		return nil, fmt.Errorf("failed to configure dialer: %v", err)
	}
	opts = append(opts, c.opts.DialOptions...)

	// 构造时已把本连接发送／接收上限投影到 callOpts；必须交给同一真实连接才能生效。
	// 保留原有接收预算，同时使已声明的发送预算实际约束整条 RPC，不另建客户端或重复编码。
	opts = append(opts, grpc.WithDefaultCallOptions(c.callOpts...))

	dctx := c.ctx
	if c.opts.DialTimeout > 0 {
		// DialContext 返回后不再需要建连计时器；取消它不会关闭已经建立的连接。
		var cancel context.CancelFunc
		dctx, cancel = context.WithTimeout(c.ctx, c.opts.DialTimeout)
		defer cancel()
	}
	conn, err := grpc.DialContext(dctx, target, opts...)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func init() {
	EnableSsl = utils.GetenvWithDef(CLIENT_ENABLE_SSL, "true") == "true"
}
