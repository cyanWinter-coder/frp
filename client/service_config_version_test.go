// Copyright 2026 The frp Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"context"
	"io"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/fatedier/frp/pkg/config/source"
	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/fatedier/frp/pkg/msg"
	clientsdk "github.com/fatedier/frp/pkg/sdk/client"
)

// TestConfigVersionEndpoint exercises the full admin HTTP route and SDK path:
// the running service exposes the version of the currently effective config,
// and the version advances only after a successful update.
func TestConfigVersionEndpoint(t *testing.T) {
	svr, err := NewService(ServiceOptions{
		Common: &v1.ClientCommonConfig{
			Transport: v1.ClientTransportConfig{HeartbeatInterval: -1, HeartbeatTimeout: -1},
			WebServer: v1.WebServerConfig{Addr: "127.0.0.1", Port: getFreeTCPPort(t)},
		},
		ConfigSourceAggregator: source.NewAggregator(source.NewConfigSource()),
		ConnectorCreator: func(context.Context, *v1.ClientCommonConfig) Connector {
			clientConn, serverConn := net.Pipe()
			t.Cleanup(func() {
				_ = clientConn.Close()
				_ = serverConn.Close()
			})
			go func() {
				rw := msg.NewV1ReadWriter(serverConn)
				var login msg.Login
				if err := rw.ReadMsgInto(&login); err != nil {
					return
				}
				_ = rw.WriteMsg(&msg.LoginResp{RunID: "version-test"})
				_, _ = io.Copy(io.Discard, serverConn)
			}()
			return &testConnector{conn: clientConn}
		},
	})
	require.NoError(t, err)
	t.Cleanup(svr.Close)

	svr.ctx, svr.cancel = context.WithCancelCause(context.Background())
	svr.loopLoginUntilSuccess(time.Second, true)
	require.NotNil(t, svr.ctl)

	webServer := svr.webServer
	require.NotNil(t, webServer)
	go func() { _ = webServer.Run() }()

	host, port := "127.0.0.1", svr.common.WebServer.Port
	client := clientsdk.New(host, port)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.Eventually(t, func() bool {
		_, err := client.GetConfigVersion(ctx)
		return err == nil
	}, 3*time.Second, 20*time.Millisecond, "admin endpoint never became reachable")

	version, err := client.GetConfigVersion(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1), version, "startup configuration must be version 1")

	// A successful update advances the effective version.
	require.NoError(t, svr.UpdateAllConfigurer([]v1.ProxyConfigurer{tcpConfig("http", 25001)}, nil))
	version, err = client.GetConfigVersion(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), version)

	// A failed update leaves the exposed version unchanged.
	err = svr.UpdateAllConfigurer([]v1.ProxyConfigurer{failingConfig("boom")}, nil)
	require.Error(t, err)
	version, err = client.GetConfigVersion(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), version)

	// The raw endpoint returns the documented JSON envelope field.
	resp, err := http.Get("http://" + host + ":" + strconv.Itoa(port) + "/api/config/version")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}
