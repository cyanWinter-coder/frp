// Copyright 2018 fatedier, fatedier@gmail.com
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
	"fmt"
	"net"
	"reflect"
	"sync"
	"time"

	"github.com/samber/lo"

	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/fatedier/frp/pkg/msg"
	"github.com/fatedier/frp/pkg/transport"
	"github.com/fatedier/frp/pkg/util/xlog"
	"github.com/fatedier/frp/pkg/vnet"
)

type Manager struct {
	clientCfg *v1.ClientCommonConfig
	cfgs      map[string]v1.VisitorConfigurer
	visitors  map[string]Visitor
	helper    Helper

	checkInterval           time.Duration
	keepVisitorsRunningOnce sync.Once

	mu  sync.RWMutex
	ctx context.Context

	stopCh chan struct{}
}

func NewManager(
	ctx context.Context,
	runID string,
	clientCfg *v1.ClientCommonConfig,
	connectServer func() (*msg.Conn, error),
	msgTransporter transport.MessageTransporter,
	vnetController *vnet.Controller,
	udpPacketCodecs ...string,
) *Manager {
	udpPacketCodec := ""
	if len(udpPacketCodecs) > 0 {
		udpPacketCodec = udpPacketCodecs[0]
	}
	m := &Manager{
		clientCfg:     clientCfg,
		cfgs:          make(map[string]v1.VisitorConfigurer),
		visitors:      make(map[string]Visitor),
		checkInterval: 10 * time.Second,
		ctx:           ctx,
		stopCh:        make(chan struct{}),
	}
	m.helper = &visitorHelperImpl{
		connectServerFn: connectServer,
		msgTransporter:  msgTransporter,
		vnetController:  vnetController,
		transferConnFn:  m.TransferConn,
		runID:           runID,
		udpPacketCodec:  udpPacketCodec,
	}
	return m
}

// keepVisitorsRunning checks all visitors' status periodically, if some visitor is not running, start it.
// It will only start after Reload is called and a new visitor is added.
func (vm *Manager) keepVisitorsRunning() {
	xl := xlog.FromContextSafe(vm.ctx)

	ticker := time.NewTicker(vm.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-vm.stopCh:
			xl.Tracef("gracefully shutdown visitor manager")
			return
		case <-ticker.C:
			vm.mu.Lock()
			for _, cfg := range vm.cfgs {
				name := cfg.GetBaseConfig().Name
				if _, exist := vm.visitors[name]; !exist {
					xl.Infof("try to start visitor [%s]", name)
					_ = vm.startVisitor(cfg)
				}
			}
			vm.mu.Unlock()
		}
	}
}

func (vm *Manager) Close() {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	for _, v := range vm.visitors {
		v.Close()
	}
	select {
	case <-vm.stopCh:
	default:
		close(vm.stopCh)
	}
}

// Hold lock before calling this function.
func (vm *Manager) startVisitor(cfg v1.VisitorConfigurer) (err error) {
	xl := xlog.FromContextSafe(vm.ctx)
	name := cfg.GetBaseConfig().Name
	visitor, err := NewVisitor(vm.ctx, cfg, vm.clientCfg, vm.helper)
	if err != nil {
		xl.Warnf("new visitor error: %v", err)
		return
	}
	err = visitor.Run()
	if err != nil {
		xl.Warnf("start error: %v", err)
	} else {
		vm.visitors[name] = visitor
		xl.Infof("start visitor success")
	}
	return
}

// UpdateAll applies the desired visitor set as a transaction.
//
// Unchanged visitors keep running; removed or changed ones are stopped and new
// ones are started. If any new visitor fails to start, every change made by
// this call is rolled back and the previously running visitor set is restored
// before the error is returned.
func (vm *Manager) UpdateAll(cfgs []v1.VisitorConfigurer) error {
	if len(cfgs) > 0 {
		// Only start keepVisitorsRunning goroutine once and only when there is at least one visitor.
		vm.keepVisitorsRunningOnce.Do(func() {
			go vm.keepVisitorsRunning()
		})
	}

	xl := xlog.FromContextSafe(vm.ctx)
	cfgsMap := lo.KeyBy(cfgs, func(c v1.VisitorConfigurer) string {
		return c.GetBaseConfig().Name
	})
	vm.mu.Lock()
	defer vm.mu.Unlock()

	previousCfgs := make(map[string]v1.VisitorConfigurer, len(vm.cfgs))
	previousVisitors := make(map[string]Visitor, len(vm.visitors))
	for name, cfg := range vm.cfgs {
		previousCfgs[name] = cfg
	}
	for name, v := range vm.visitors {
		previousVisitors[name] = v
	}

	delNames := make([]string, 0)
	for name, oldCfg := range vm.cfgs {
		cfg, ok := cfgsMap[name]
		if !ok || !reflect.DeepEqual(oldCfg, cfg) {
			delNames = append(delNames, name)
			delete(vm.cfgs, name)
			if visitor, ok := vm.visitors[name]; ok {
				visitor.Close()
			}
			delete(vm.visitors, name)
		}
	}
	if len(delNames) > 0 {
		xl.Infof("visitor removed: %v", delNames)
	}

	addedNames := make([]string, 0)
	rollback := func(applyErr error) error {
		xl.Warnf("apply visitors failed: %v, rolling back", applyErr)
		for _, name := range addedNames {
			delete(vm.cfgs, name)
			if visitor, ok := vm.visitors[name]; ok {
				visitor.Close()
				delete(vm.visitors, name)
			}
		}
		// Restore the previous set. Entries still present are unchanged
		// visitors and must not be touched. A previous config whose visitor had
		// failed to start before this transaction has no live instance to
		// restore, so attempt to start it again; keepVisitorsRunning would
		// otherwise retry it only on its slower ticker.
		for name, cfg := range previousCfgs {
			if _, ok := vm.cfgs[name]; ok {
				continue
			}
			vm.cfgs[name] = cfg
			if previous, ok := previousVisitors[name]; ok {
				vm.visitors[name] = previous
			} else if err := vm.startVisitor(cfg); err != nil {
				xl.Warnf("rollback: restart previous visitor %s failed: %v", name, err)
			}
		}
		return applyErr
	}

	addNames := make([]string, 0)
	for _, cfg := range cfgs {
		name := cfg.GetBaseConfig().Name
		if _, ok := vm.cfgs[name]; ok {
			continue
		}
		vm.cfgs[name] = cfg
		addedNames = append(addedNames, name)
		if err := vm.startVisitor(cfg); err != nil {
			return rollback(err)
		}
		addNames = append(addNames, name)
	}
	if len(addNames) > 0 {
		xl.Infof("visitor added: %v", addNames)
	}
	return nil
}

// TransferConn transfers a connection to a visitor.
func (vm *Manager) TransferConn(name string, conn net.Conn) error {
	vm.mu.RLock()
	defer vm.mu.RUnlock()
	v, ok := vm.visitors[name]
	if !ok {
		return fmt.Errorf("visitor [%s] not found", name)
	}
	return v.AcceptConn(conn)
}

func (vm *Manager) GetVisitorCfg(name string) (v1.VisitorConfigurer, bool) {
	vm.mu.RLock()
	defer vm.mu.RUnlock()
	cfg, ok := vm.cfgs[name]
	return cfg, ok
}

type visitorHelperImpl struct {
	connectServerFn func() (*msg.Conn, error)
	msgTransporter  transport.MessageTransporter
	vnetController  *vnet.Controller
	transferConnFn  func(name string, conn net.Conn) error
	runID           string
	udpPacketCodec  string
}

func (v *visitorHelperImpl) ConnectServer() (*msg.Conn, error) {
	return v.connectServerFn()
}

func (v *visitorHelperImpl) TransferConn(name string, conn net.Conn) error {
	return v.transferConnFn(name, conn)
}

func (v *visitorHelperImpl) MsgTransporter() transport.MessageTransporter {
	return v.msgTransporter
}

func (v *visitorHelperImpl) VNetController() *vnet.Controller {
	return v.vnetController
}

func (v *visitorHelperImpl) RunID() string {
	return v.runID
}

func (v *visitorHelperImpl) UDPPacketCodec() string {
	return v.udpPacketCodec
}
