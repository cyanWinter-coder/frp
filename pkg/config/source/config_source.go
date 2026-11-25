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

package source

import v1 "github.com/fatedier/frp/pkg/config/v1"

// ConfigSource implements Source for in-memory configuration.
// All operations are thread-safe.
type ConfigSource struct {
	baseSource
}

func NewConfigSource() *ConfigSource {
	return &ConfigSource{
		baseSource: newBaseSource(),
	}
}

// ReplaceAll replaces all proxy and visitor configurations atomically.
func (s *ConfigSource) ReplaceAll(proxies []v1.ProxyConfigurer, visitors []v1.VisitorConfigurer) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	nextProxies := make(map[string]v1.ProxyConfigurer, len(proxies))
	for _, p := range proxies {
		name, err := validateProxyName(p)
		if err != nil {
			return err
		}
		nextProxies[name] = p
	}
	nextVisitors := make(map[string]v1.VisitorConfigurer, len(visitors))
	for _, v := range visitors {
		name, err := validateVisitorName(v)
		if err != nil {
			return err
		}
		nextVisitors[name] = v
	}
	s.proxies = nextProxies
	s.visitors = nextVisitors
	return nil
}

// Snapshot returns deep copies of every stored proxy and visitor, including
// entries explicitly disabled. It is used to capture the previous state before
// a transaction so a failed ReplaceAll/reload can be restored exactly.
func (s *ConfigSource) Snapshot() (proxies []v1.ProxyConfigurer, visitors []v1.VisitorConfigurer, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	proxies = make([]v1.ProxyConfigurer, 0, len(s.proxies))
	for _, p := range s.proxies {
		proxies = append(proxies, p)
	}
	visitors = make([]v1.VisitorConfigurer, 0, len(s.visitors))
	for _, v := range s.visitors {
		visitors = append(visitors, v)
	}
	return cloneConfigurers(proxies, visitors)
}
