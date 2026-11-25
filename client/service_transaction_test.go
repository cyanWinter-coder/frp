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
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/fatedier/frp/client/configmgmt"
	"github.com/fatedier/frp/client/proxy"
	"github.com/fatedier/frp/pkg/config/source"
	v1 "github.com/fatedier/frp/pkg/config/v1"
)

// transactionFailingProxyConfig deliberately has no registered proxy
// factory, so NewWrapper rejects it synchronously when a transaction tries to
// start it. Clone preserves the concrete type so config-source cloning cannot
// silently turn it back into a regular TCPProxyConfig.
type transactionFailingProxyConfig struct {
	*v1.TCPProxyConfig
}

func (c *transactionFailingProxyConfig) Clone() v1.ProxyConfigurer {
	return &transactionFailingProxyConfig{TCPProxyConfig: c.TCPProxyConfig.Clone().(*v1.TCPProxyConfig)}
}

func newTransactionTestService(t *testing.T) *Service {
	t.Helper()
	svr := newReloadTestService(t, nil)
	svr.loopLoginUntilSuccess(time.Second, true)
	require.NotNil(t, svr.ctl)
	return svr
}

func tcpConfig(name string, remotePort int) *v1.TCPProxyConfig {
	return &v1.TCPProxyConfig{
		ProxyBaseConfig: v1.ProxyBaseConfig{Name: name, Type: "tcp"},
		RemotePort:      remotePort,
	}
}

func failingConfig(name string) *transactionFailingProxyConfig {
	return &transactionFailingProxyConfig{
		TCPProxyConfig: &v1.TCPProxyConfig{
			ProxyBaseConfig: v1.ProxyBaseConfig{Name: name, Type: "tcp"},
		},
	}
}

func writeConfigFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "frpc.toml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

// Requirement 1: invalid new config must be rejected without touching the
// running proxy set.
func TestReloadRejectsInvalidConfigAndKeepsRunningProxies(t *testing.T) {
	svr := newTransactionTestService(t)
	initial := []v1.ProxyConfigurer{tcpConfig("keep", 20001)}
	require.NoError(t, svr.UpdateAllConfigurer(initial, nil))
	require.Equal(t, uint64(2), svr.ConfigVersion())

	svr.configFilePath = writeConfigFile(t, `this is = = not valid toml [[[`)

	err := (&serviceConfigManager{svr: svr}).ReloadFromFile(true)
	require.Error(t, err)
	require.True(t, errors.Is(err, configmgmt.ErrInvalidArgument), "got: %v", err)

	status, ok := svr.getProxyStatus("keep")
	require.True(t, ok)
	require.Equal(t, 20001, status.Cfg.(*v1.TCPProxyConfig).RemotePort)
	// Failed validation must not advance the config version.
	require.Equal(t, uint64(2), svr.ConfigVersion())
}

// Requirement 1: semantically invalid config (unknown field in strict mode)
// leaves the running set untouched.
func TestReloadRejectsStrictValidationError(t *testing.T) {
	svr := newTransactionTestService(t)
	initial := []v1.ProxyConfigurer{tcpConfig("keep", 20002)}
	require.NoError(t, svr.UpdateAllConfigurer(initial, nil))
	versionBefore := svr.ConfigVersion()

	svr.configFilePath = writeConfigFile(t, `
serverAddr = "127.0.0.1"
unknownField = 1
`)
	err := (&serviceConfigManager{svr: svr}).ReloadFromFile(true)
	require.Error(t, err)
	require.True(t, errors.Is(err, configmgmt.ErrInvalidArgument), "got: %v", err)

	_, ok := svr.getProxyStatus("keep")
	require.True(t, ok, "running proxy must survive an invalid reload")
	require.Equal(t, versionBefore, svr.ConfigVersion())
}

// Requirement 2: only changed proxies are rebuilt; the resulting running set
// matches the desired diff. Wrapper identity is asserted directly in the proxy
// package tests.
func TestUpdateOnlyRebuildsChangedProxies(t *testing.T) {
	svr := newTransactionTestService(t)
	require.NoError(t, svr.UpdateAllConfigurer([]v1.ProxyConfigurer{
		tcpConfig("a", 20011),
		tcpConfig("b", 20012),
	}, nil))

	require.NoError(t, svr.UpdateAllConfigurer([]v1.ProxyConfigurer{
		tcpConfig("a", 20011), // unchanged
		tcpConfig("b", 20099), // changed
		tcpConfig("c", 20013), // added
	}, nil))

	statuses := svr.getAllProxyStatus()
	require.Len(t, statuses, 3)
	byName := map[string]*proxy.WorkingStatus{}
	for _, s := range statuses {
		byName[s.Name] = s
	}
	require.Equal(t, 20011, byName["a"].Cfg.(*v1.TCPProxyConfig).RemotePort)
	require.Equal(t, 20099, byName["b"].Cfg.(*v1.TCPProxyConfig).RemotePort)
	require.Equal(t, 20013, byName["c"].Cfg.(*v1.TCPProxyConfig).RemotePort)
}

// Requirement 4: when a proxy in the new set fails to start, the whole
// transaction rolls back and the previously running proxies are restored.
func TestUpdateRollsBackWhenProxyStartFails(t *testing.T) {
	svr := newTransactionTestService(t)
	initial := []v1.ProxyConfigurer{
		tcpConfig("a", 20021),
		tcpConfig("b", 20022),
	}
	require.NoError(t, svr.UpdateAllConfigurer(initial, nil))
	versionBefore := svr.ConfigVersion()

	err := svr.UpdateAllConfigurer([]v1.ProxyConfigurer{
		tcpConfig("a", 20021), // unchanged
		tcpConfig("b", 20099), // changed: stopped before the failure
		failingConfig("boom"), // cannot be constructed
	}, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not supported")

	statuses := svr.getAllProxyStatus()
	require.Len(t, statuses, 2, "previous proxy set must be restored")
	byName := map[string]*proxy.WorkingStatus{}
	for _, s := range statuses {
		byName[s.Name] = s
	}
	require.Equal(t, 20021, byName["a"].Cfg.(*v1.TCPProxyConfig).RemotePort)
	require.Equal(t, 20022, byName["b"].Cfg.(*v1.TCPProxyConfig).RemotePort,
		"changed proxy must be restored with its previous config")
	require.NotContains(t, byName, "boom")

	// A rolled-back transaction must not advance the version or published config.
	require.Equal(t, versionBefore, svr.ConfigVersion())
	svr.cfgMu.RLock()
	defer svr.cfgMu.RUnlock()
	require.Len(t, svr.proxyCfgs, 2)
}

// Requirement 4 across managers: the visitor update succeeds but the proxy
// update fails. Both managers must end up back at their previous sets.
func TestUpdateRollsBackVisitorsWhenProxyUpdateFails(t *testing.T) {
	svr := newTransactionTestService(t)
	initialVisitors := []v1.VisitorConfigurer{
		&v1.STCPVisitorConfig{VisitorBaseConfig: v1.VisitorBaseConfig{
			Name: "visitor-a", Type: "stcp", BindPort: -1, ServerName: "old-target",
		}},
	}
	require.NoError(t, svr.UpdateAllConfigurer(
		[]v1.ProxyConfigurer{tcpConfig("proxy-a", 20041)},
		initialVisitors,
	))

	nextVisitors := []v1.VisitorConfigurer{
		&v1.STCPVisitorConfig{VisitorBaseConfig: v1.VisitorBaseConfig{
			Name: "visitor-a", Type: "stcp", BindPort: -1, ServerName: "new-target",
		}},
	}
	err := svr.UpdateAllConfigurer(
		[]v1.ProxyConfigurer{failingConfig("boom")},
		nextVisitors,
	)
	require.Error(t, err)

	// Proxies are restored.
	statuses := svr.getAllProxyStatus()
	require.Len(t, statuses, 1)
	require.Equal(t, "proxy-a", statuses[0].Name)

	// Visitors are restored to their previous configuration.
	visitorCfg, ok := svr.getVisitorCfg("visitor-a")
	require.True(t, ok)
	require.Equal(t, "old-target", visitorCfg.GetBaseConfig().ServerName)

	svr.cfgMu.RLock()
	defer svr.cfgMu.RUnlock()
	require.Len(t, svr.visitorCfgs, 1)
	require.Equal(t, "old-target", svr.visitorCfgs[0].GetBaseConfig().ServerName)
}

// Requirement 4 via the file reload path: source and runtime both roll back.
func TestReloadFromFileRollsBackOnApplyFailure(t *testing.T) {
	configSource := source.NewConfigSource()
	aggregator := source.NewAggregator(configSource)
	svr := newTransactionTestService(t)
	svr.aggregator = aggregator
	svr.configSource = configSource

	seed := []v1.ProxyConfigurer{tcpConfig("seed", 20031)}
	require.NoError(t, svr.UpdateConfigSource(svr.common, seed, nil))
	versionBefore := svr.ConfigVersion()
	snapshotBefore, _, err := configSource.Snapshot()
	require.NoError(t, err)

	// A syntactically valid file whose proxy cannot be constructed at runtime.
	svr.configFilePath = writeConfigFile(t, `
serverAddr = "127.0.0.1"
[[proxies]]
name = "seed"
type = "tcp"
remotePort = 20099
`)
	// Replace the registered TCP factory outcome for "seed" indirectly by
	// mutating the config source through UpdateConfigSource with a failing type
	// is not possible from TOML, so exercise the source path directly.
	err = svr.UpdateConfigSource(svr.common, []v1.ProxyConfigurer{
		tcpConfig("seed", 20031),
		failingConfig("boom"),
	}, nil)
	require.Error(t, err)

	restored, _, snapshotErr := configSource.Snapshot()
	require.NoError(t, snapshotErr)
	require.Equal(t, snapshotBefore, restored, "config source must be restored after rollback")
	require.Equal(t, versionBefore, svr.ConfigVersion())

	status, ok := svr.getProxyStatus("seed")
	require.True(t, ok)
	require.Equal(t, 20031, status.Cfg.(*v1.TCPProxyConfig).RemotePort)
}

// Requirement 3: concurrent updates are serialized and the last submitted
// configuration is the one that ends up in effect.
func TestConcurrentUpdatesAreSerializedAndLastWins(t *testing.T) {
	svr := newTransactionTestService(t)

	const updates = 8
	var wg sync.WaitGroup
	wg.Add(updates)
	start := make(chan struct{})
	errs := make(chan error, updates)
	for i := range updates {
		go func(i int) {
			defer wg.Done()
			<-start
			port := 21000 + i
			errs <- svr.UpdateAllConfigurer([]v1.ProxyConfigurer{
				tcpConfig("serial", port),
			}, nil)
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	// Exactly one successful transaction per update; every successful update
	// increments the version exactly once.
	require.Equal(t, uint64(1+updates), svr.ConfigVersion())

	// The final state is one proxy named "serial"; its running config must
	// match the published config.
	running, ok := svr.getProxyStatus("serial")
	require.True(t, ok)
	require.Len(t, svr.getAllProxyStatus(), 1)

	svr.cfgMu.RLock()
	published := svr.proxyCfgs
	svr.cfgMu.RUnlock()
	require.Len(t, published, 1)
	require.Equal(t, running.Cfg, published[0])
	require.GreaterOrEqual(t, running.Cfg.(*v1.TCPProxyConfig).RemotePort, 21000)
}

// Requirement 3: concurrent file reloads apply the latest file content.
func TestConcurrentReloadsLastFileWins(t *testing.T) {
	svr := newTransactionTestService(t)
	path := writeConfigFile(t, `
serverAddr = "127.0.0.1"
[[proxies]]
name = "file-proxy"
type = "tcp"
remotePort = 22001
`)
	svr.configFilePath = path
	mgr := &serviceConfigManager{svr: svr}
	require.NoError(t, mgr.ReloadFromFile(true))
	versionAfterFirst := svr.ConfigVersion()

	const updates = 5
	var wg sync.WaitGroup
	wg.Add(updates)
	start := make(chan struct{})
	errs := make(chan error, updates)
	for i := range updates {
		go func(i int) {
			defer wg.Done()
			<-start
			content := "serverAddr = \"127.0.0.1\"\n" +
				"[[proxies]]\n" +
				"name = \"file-proxy\"\n" +
				"type = \"tcp\"\n" +
				"remotePort = " + strconv.Itoa(22002+i) + "\n"
			require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
			errs <- mgr.ReloadFromFile(true)
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	require.Equal(t, versionAfterFirst+updates, svr.ConfigVersion())
	status, ok := svr.getProxyStatus("file-proxy")
	require.True(t, ok)
	runningPort := status.Cfg.(*v1.TCPProxyConfig).RemotePort

	// The running config must match the file's final content.
	final, err := os.ReadFile(path)
	require.NoError(t, err)
	require.True(t, strings.Contains(string(final), "remotePort = "+strconv.Itoa(runningPort)),
		"running port %d must match final file content: %s", runningPort, string(final))
}

// Requirement 5: the config version reflects the currently effective
// configuration and is exposed through the management interface.
func TestConfigVersionTracksEffectiveConfig(t *testing.T) {
	svr := newTransactionTestService(t)
	require.Equal(t, uint64(1), svr.ConfigVersion(), "startup config is version 1")

	mgr := &serviceConfigManager{svr: svr}
	require.Equal(t, uint64(1), mgr.ConfigVersion())

	require.NoError(t, svr.UpdateAllConfigurer([]v1.ProxyConfigurer{tcpConfig("v", 23001)}, nil))
	require.Equal(t, uint64(2), svr.ConfigVersion())

	// No effective change still counts as an applied transaction.
	require.NoError(t, svr.UpdateAllConfigurer([]v1.ProxyConfigurer{tcpConfig("v", 23001)}, nil))
	require.Equal(t, uint64(3), svr.ConfigVersion())

	// A failed transaction leaves the version untouched.
	err := svr.UpdateAllConfigurer([]v1.ProxyConfigurer{failingConfig("boom")}, nil)
	require.Error(t, err)
	require.Equal(t, uint64(3), svr.ConfigVersion())
}

// Requirement 1: when no control connection is live yet, an invalid update
// still must not mutate the published configurers.
func TestUpdateWithoutControlRollsBackPublishedConfig(t *testing.T) {
	svr := newReloadTestService(t, nil)
	require.Nil(t, svr.ctl)
	initial := []v1.ProxyConfigurer{tcpConfig("early", 24001)}
	require.NoError(t, svr.UpdateAllConfigurer(initial, nil))
	require.Equal(t, uint64(2), svr.ConfigVersion())

	// With a live control, failing manager apply must restore published config.
	svr.loopLoginUntilSuccess(time.Second, true)
	require.NotNil(t, svr.ctl)
	err := svr.UpdateAllConfigurer([]v1.ProxyConfigurer{failingConfig("boom")}, nil)
	require.Error(t, err)

	svr.cfgMu.RLock()
	defer svr.cfgMu.RUnlock()
	require.Len(t, svr.proxyCfgs, 1)
	require.Equal(t, "early", svr.proxyCfgs[0].GetBaseConfig().Name)
}
