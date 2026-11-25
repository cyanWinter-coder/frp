// Copyright 2020 guylewin, guy@lewin.co.il
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

package group

import (
	"context"
	"fmt"
	"net"

	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/fatedier/frp/pkg/util/tcpmux"
	"github.com/fatedier/frp/pkg/util/vhost"
)

// TCPMuxGroupCtl manages all TCPMuxGroups.
type TCPMuxGroupCtl struct {
	*groupRegistry[*TCPMuxGroup]
	tcpMuxHTTPConnectMuxer *tcpmux.HTTPConnectTCPMuxer
}

// NewTCPMuxGroupCtl returns a new TCPMuxGroupCtl. A maxGroups value of 0
// means no limit on the number of concurrently registered groups.
func NewTCPMuxGroupCtl(tcpMuxHTTPConnectMuxer *tcpmux.HTTPConnectTCPMuxer, maxGroups int) *TCPMuxGroupCtl {
	return &TCPMuxGroupCtl{
		groupRegistry:          newGroupRegistry[*TCPMuxGroup]().withMaxGroups(maxGroups),
		tcpMuxHTTPConnectMuxer: tcpMuxHTTPConnectMuxer,
	}
}

// Listen is the wrapper for TCPMuxGroup's Listen.
// If there is no group, one will be created.
func (tmgc *TCPMuxGroupCtl) Listen(
	ctx context.Context,
	multiplexer, group, groupKey string,
	routeConfig vhost.RouteConfig,
) (l net.Listener, err error) {
	for {
		tcpMuxGroup, createErr := tmgc.getOrCreate(group, func() *TCPMuxGroup {
			return NewTCPMuxGroup(tmgc)
		})
		if createErr != nil {
			return nil, createErr
		}

		switch v1.TCPMultiplexerType(multiplexer) {
		case v1.TCPMultiplexerHTTPConnect:
			l, err = tcpMuxGroup.HTTPConnectListen(ctx, group, groupKey, routeConfig)
			if err == errGroupStale {
				continue
			}
			if err != nil {
				tmgc.removeIfEmpty(group, func(cur *TCPMuxGroup) bool {
					return cur == tcpMuxGroup && tcpMuxGroup.isEmpty()
				})
			}
			return
		default:
			return nil, fmt.Errorf("unknown multiplexer [%s]", multiplexer)
		}
	}
}

// TCPMuxGroup routes connections to different proxies.
type TCPMuxGroup struct {
	baseGroup

	domain          string
	routeByHTTPUser string
	username        string
	password        string
	ctl             *TCPMuxGroupCtl
}

// NewTCPMuxGroup returns a new TCPMuxGroup.
func NewTCPMuxGroup(ctl *TCPMuxGroupCtl) *TCPMuxGroup {
	return &TCPMuxGroup{
		ctl: ctl,
	}
}

// isEmpty reports whether the group has ever held a listener.
func (tmg *TCPMuxGroup) isEmpty() bool {
	tmg.mu.Lock()
	defer tmg.mu.Unlock()
	return len(tmg.lns) == 0 && tmg.cleanupFn == nil
}

// HTTPConnectListen will return a new Listener.
// If TCPMuxGroup already has a listener, just add a new Listener to the queues,
// otherwise listen on the real address.
func (tmg *TCPMuxGroup) HTTPConnectListen(
	ctx context.Context,
	group, groupKey string,
	routeConfig vhost.RouteConfig,
) (ln *Listener, err error) {
	tmg.mu.Lock()
	defer tmg.mu.Unlock()
	if !tmg.ctl.isCurrent(group, func(cur *TCPMuxGroup) bool { return cur == tmg }) {
		return nil, errGroupStale
	}
	if len(tmg.lns) == 0 {
		// the first listener, listen on the real address
		tcpMuxLn, errRet := tmg.ctl.tcpMuxHTTPConnectMuxer.Listen(ctx, &routeConfig)
		if errRet != nil {
			return nil, errRet
		}

		tmg.domain = routeConfig.Domain
		tmg.routeByHTTPUser = routeConfig.RouteByHTTPUser
		tmg.username = routeConfig.Username
		tmg.password = routeConfig.Password
		tmg.initBase(group, groupKey, tcpMuxLn, func() {
			tmg.ctl.removeIf(tmg.group, func(cur *TCPMuxGroup) bool {
				return cur == tmg
			})
		})
		ln = tmg.newListener(tcpMuxLn.Addr())
		go tmg.worker(tcpMuxLn, tmg.acceptCh)
	} else {
		// route config in the same group must be equal
		if tmg.group != group || tmg.domain != routeConfig.Domain ||
			tmg.routeByHTTPUser != routeConfig.RouteByHTTPUser ||
			tmg.username != routeConfig.Username ||
			tmg.password != routeConfig.Password {
			return nil, ErrGroupParamsInvalid
		}
		if tmg.groupKey != groupKey {
			return nil, ErrGroupAuthFailed
		}
		ln = tmg.newListener(tmg.lns[0].Addr())
	}
	return
}
