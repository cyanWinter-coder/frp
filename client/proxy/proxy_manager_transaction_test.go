//go:build !frps

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

package proxy

import (
	"context"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/fatedier/frp/pkg/msg"
	"github.com/fatedier/frp/pkg/transport"
)

// fakeTransporter accepts every message without touching a real connection.
type fakeTransporter struct {
	sent atomic.Int64
}

func (f *fakeTransporter) Send(msg.Message) error {
	f.sent.Add(1)
	return nil
}

func (f *fakeTransporter) Do(context.Context, msg.Message, string, string) (msg.Message, error) {
	return nil, nil
}

func (f *fakeTransporter) Dispatch(msg.Message, string) bool                 { return false }
func (f *fakeTransporter) DispatchWithType(msg.Message, string, string) bool { return false }

var _ transport.MessageTransporter = (*fakeTransporter)(nil)

func newTestProxyManager(t *testing.T) *Manager {
	t.Helper()
	return NewManager(
		context.Background(),
		&v1.ClientCommonConfig{},
		nil,
		&fakeTransporter{},
		nil,
		"",
	)
}

func tcpProxyCfg(name string, remotePort int) *v1.TCPProxyConfig {
	return &v1.TCPProxyConfig{
		ProxyBaseConfig: v1.ProxyBaseConfig{Name: name, Type: "tcp"},
		RemotePort:      remotePort,
	}
}

func TestManagerUpdateAllKeepsUnchangedProxies(t *testing.T) {
	pm := newTestProxyManager(t)
	t.Cleanup(pm.Close)

	require.NoError(t, pm.UpdateAll([]v1.ProxyConfigurer{
		tcpProxyCfg("a", 19001),
		tcpProxyCfg("b", 19002),
	}))

	pm.mu.RLock()
	wrapperA := pm.proxies["a"]
	wrapperB := pm.proxies["b"]
	pm.mu.RUnlock()
	require.NotNil(t, wrapperA)
	require.NotNil(t, wrapperB)

	// Add one proxy, remove one, leave "a" untouched.
	require.NoError(t, pm.UpdateAll([]v1.ProxyConfigurer{
		tcpProxyCfg("a", 19001),
		tcpProxyCfg("c", 19003),
	}))

	pm.mu.RLock()
	defer pm.mu.RUnlock()
	require.Same(t, wrapperA, pm.proxies["a"], "unchanged proxy must not be rebuilt")
	require.NotContains(t, pm.proxies, "b")
	require.Contains(t, pm.proxies, "c")
	require.NotSame(t, wrapperB, pm.proxies["c"])
}

func TestManagerUpdateAllRebuildsChangedProxies(t *testing.T) {
	pm := newTestProxyManager(t)
	t.Cleanup(pm.Close)

	require.NoError(t, pm.UpdateAll([]v1.ProxyConfigurer{tcpProxyCfg("a", 19001)}))
	pm.mu.RLock()
	oldWrapper := pm.proxies["a"]
	pm.mu.RUnlock()

	require.NoError(t, pm.UpdateAll([]v1.ProxyConfigurer{tcpProxyCfg("a", 19999)}))

	pm.mu.RLock()
	defer pm.mu.RUnlock()
	newWrapper := pm.proxies["a"]
	require.NotSame(t, oldWrapper, newWrapper, "changed proxy must be rebuilt")
	require.Equal(t, 19999, newWrapper.Cfg.(*v1.TCPProxyConfig).RemotePort)
}

// failingProxyConfig is a proxy config type without a registered factory, so
// NewWrapper rejects it synchronously when the manager tries to start it.
type failingProxyConfig struct {
	*v1.TCPProxyConfig
}

func TestManagerUpdateAllRollsBackOnStartFailure(t *testing.T) {
	pm := newTestProxyManager(t)
	t.Cleanup(pm.Close)

	initial := []v1.ProxyConfigurer{
		tcpProxyCfg("a", 19001),
		tcpProxyCfg("b", 19002),
	}
	require.NoError(t, pm.UpdateAll(initial))
	pm.mu.RLock()
	wrapperA := pm.proxies["a"]
	wrapperB := pm.proxies["b"]
	pm.mu.RUnlock()

	err := pm.UpdateAll([]v1.ProxyConfigurer{
		tcpProxyCfg("a", 19001), // unchanged
		tcpProxyCfg("b", 19999), // changed: stopped first
		&failingProxyConfig{TCPProxyConfig: &v1.TCPProxyConfig{
			ProxyBaseConfig: v1.ProxyBaseConfig{Name: "boom", Type: "tcp"},
		}}, // cannot be constructed
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "not supported")

	pm.mu.RLock()
	defer pm.mu.RUnlock()
	require.Len(t, pm.proxies, 2)
	require.Same(t, wrapperA, pm.proxies["a"], "unchanged proxy must survive rollback")
	restoredB := pm.proxies["b"]
	require.NotNil(t, restoredB)
	require.Same(t, wrapperB, restoredB, "removed proxy must be restored to the same wrapper")
	require.Equal(t, 19002, restoredB.Cfg.(*v1.TCPProxyConfig).RemotePort)
	require.NotContains(t, pm.proxies, "boom")
}

func TestManagerUpdateAllRejectsUnsupportedTypeBeforeMutation(t *testing.T) {
	pm := newTestProxyManager(t)
	t.Cleanup(pm.Close)

	err := pm.UpdateAll([]v1.ProxyConfigurer{
		&failingProxyConfig{TCPProxyConfig: &v1.TCPProxyConfig{
			ProxyBaseConfig: v1.ProxyBaseConfig{Name: "boom", Type: "tcp"},
		}},
	})
	require.Error(t, err)

	pm.mu.RLock()
	defer pm.mu.RUnlock()
	require.Empty(t, pm.proxies)
}

func TestManagerUpdateAllAfterCloseReturnsError(t *testing.T) {
	pm := newTestProxyManager(t)
	require.NoError(t, pm.UpdateAll([]v1.ProxyConfigurer{tcpProxyCfg("a", 19001)}))
	pm.Close()
	err := pm.UpdateAll([]v1.ProxyConfigurer{tcpProxyCfg("a", 19001)})
	require.Error(t, err)
}

// Ensure the failing test config type is distinct from registered TCP configs.
func TestFailingProxyConfigHasDistinctReflectType(t *testing.T) {
	require.NotEqual(t,
		reflect.TypeFor[*v1.TCPProxyConfig](),
		reflect.TypeFor[*failingProxyConfig](),
	)
}
