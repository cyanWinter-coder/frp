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

package visitor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/fatedier/frp/pkg/config/v1"
)

func newTestVisitorManager(t *testing.T) *Manager {
	t.Helper()
	m := NewManager(context.Background(), "run-test", &v1.ClientCommonConfig{},
		nil, nil, nil, "")
	t.Cleanup(m.Close)
	return m
}

func stcpVisitorCfg(name, serverName string) *v1.STCPVisitorConfig {
	return &v1.STCPVisitorConfig{
		VisitorBaseConfig: v1.VisitorBaseConfig{
			Name:       name,
			Type:       "stcp",
			ServerName: serverName,
			BindPort:   -1,
		},
	}
}

// failingVisitorCfg references an unregistered plugin, so startVisitor fails
// synchronously when the manager tries to start it.
func failingVisitorCfg(name string) *v1.STCPVisitorConfig {
	cfg := stcpVisitorCfg(name, "target")
	cfg.Plugin = v1.TypedVisitorPluginOptions{
		Type: "definitely-not-registered-visitor-plugin",
	}
	return cfg
}

func TestManagerUpdateAllKeepsUnchangedVisitors(t *testing.T) {
	vm := newTestVisitorManager(t)

	require.NoError(t, vm.UpdateAll([]v1.VisitorConfigurer{
		stcpVisitorCfg("a", "server-a"),
		stcpVisitorCfg("b", "server-b"),
	}))

	vm.mu.RLock()
	visitorA := vm.visitors["a"]
	cfgA := vm.cfgs["a"]
	vm.mu.RUnlock()
	require.NotNil(t, visitorA)

	require.NoError(t, vm.UpdateAll([]v1.VisitorConfigurer{
		stcpVisitorCfg("a", "server-a"),
		stcpVisitorCfg("c", "server-c"),
	}))

	vm.mu.RLock()
	defer vm.mu.RUnlock()
	require.Same(t, cfgA, vm.cfgs["a"], "unchanged visitor config must be kept")
	require.Same(t, visitorA, vm.visitors["a"], "unchanged visitor must not be restarted")
	require.NotContains(t, vm.cfgs, "b")
	require.Contains(t, vm.cfgs, "c")
}

func TestManagerUpdateAllRollsBackOnStartFailure(t *testing.T) {
	vm := newTestVisitorManager(t)

	initial := []v1.VisitorConfigurer{
		stcpVisitorCfg("a", "server-a"),
		stcpVisitorCfg("b", "server-b"),
	}
	require.NoError(t, vm.UpdateAll(initial))
	vm.mu.RLock()
	visitorA := vm.visitors["a"]
	visitorB := vm.visitors["b"]
	cfgB := vm.cfgs["b"]
	vm.mu.RUnlock()

	err := vm.UpdateAll([]v1.VisitorConfigurer{
		stcpVisitorCfg("a", "server-a"),         // unchanged
		stcpVisitorCfg("b", "server-b-changed"), // changed
		failingVisitorCfg("boom"),               // fails to start
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "not registered")

	vm.mu.RLock()
	defer vm.mu.RUnlock()
	require.Len(t, vm.cfgs, 2)
	require.Len(t, vm.visitors, 2)
	require.Same(t, visitorA, vm.visitors["a"], "unchanged visitor must survive rollback")
	require.Same(t, visitorB, vm.visitors["b"], "removed visitor must be restored")
	require.Same(t, cfgB, vm.cfgs["b"], "previous visitor config must be restored")
	require.NotContains(t, vm.cfgs, "boom")
}

func TestManagerUpdateAllFailureWithEmptyPreviousSetRollsBackToEmpty(t *testing.T) {
	vm := newTestVisitorManager(t)

	err := vm.UpdateAll([]v1.VisitorConfigurer{
		stcpVisitorCfg("ok", "server-ok"),
		failingVisitorCfg("boom"),
	})
	require.Error(t, err)

	vm.mu.RLock()
	defer vm.mu.RUnlock()
	// "ok" started before "boom" failed; rollback must remove it again.
	require.Empty(t, vm.cfgs)
	require.Empty(t, vm.visitors)
}
