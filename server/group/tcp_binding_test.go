package group

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/fatedier/frp/pkg/config/types"
	"github.com/fatedier/frp/server/ports"
)

// freeLoopbackTCPPorts reserves n loopback TCP ports by binding them all at
// once and then releasing the listeners. The returned ports are very likely to
// stay free for the immediate bindings the tests perform.
func freeLoopbackTCPPorts(t *testing.T, n int) []int {
	t.Helper()
	listeners := make([]net.Listener, 0, n)
	ports := make([]int, 0, n)
	for range n {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		listeners = append(listeners, ln)
		ports = append(ports, ln.Addr().(*net.TCPAddr).Port)
	}
	t.Cleanup(func() {
		for _, ln := range listeners {
			_ = ln.Close()
		}
	})
	for _, ln := range listeners {
		require.NoError(t, ln.Close())
	}
	return ports
}

func portRangesFromPorts(ps []int) []types.PortsRange {
	ranges := make([]types.PortsRange, 0, len(ps))
	for _, p := range ps {
		ranges = append(ranges, types.PortsRange{Single: p})
	}
	return ranges
}

func newTestTCPGroupCtl(t *testing.T, allowed []int, maxGroups int) *TCPGroupCtl {
	t.Helper()
	pm := ports.NewManager("tcp", "127.0.0.1", portRangesFromPorts(allowed))
	return NewTCPGroupCtl(pm, maxGroups)
}

// closeListenerAndWait closes the virtual listener. Listener.Close runs the
// group cleanup synchronously, so once it returns the port is released and the
// group entry is removed when it was the last listener.
func closeListenerAndWait(t *testing.T, ln net.Listener) {
	t.Helper()
	require.NoError(t, ln.Close())
}

// TestTCPGroupCtl_CreateDeleteCycleReturnsPortToPool reproduces the reported
// port exhaustion: repeatedly creating and deleting the same group must return
// its port to the allocatable pool every time instead of leaking it.
func TestTCPGroupCtl_CreateDeleteCycleReturnsPortToPool(t *testing.T) {
	allowed := freeLoopbackTCPPorts(t, 3)
	ctl := newTestTCPGroupCtl(t, allowed, 0)

	for i := range 50 {
		proxyName := "proxy-" + strconv.Itoa(i)
		ln, realPort, err := ctl.Listen(proxyName, "steady-group", "key", "127.0.0.1", 0)
		require.NoError(t, err, "cycle %d: create group should succeed", i)
		require.Contains(t, allowed, realPort)

		g, ok := ctl.get("steady-group")
		require.True(t, ok)
		assert.Equal(t, realPort, g.realPort, "actual bound port must be recorded on the group")
		assert.Equal(t, 1, ctl.len())

		closeListenerAndWait(t, ln)

		_, ok = ctl.get("steady-group")
		assert.False(t, ok, "deleted group must be removed from the registry, cycle %d", i)
		assert.Equal(t, 0, ctl.len())
		assert.True(t, ctl.portManager.HasFreePort(realPort),
			"port %d must return to the free pool after deletion, cycle %d", realPort, i)
	}
}

// TestTCPGroupCtl_DeletedPortCanBeReallocatedWithoutDuplicateOccupation
// proves a released port is handed to a brand new group and never to two
// live groups at the same time.
func TestTCPGroupCtl_DeletedPortCanBeReallocatedWithoutDuplicateOccupation(t *testing.T) {
	allowed := freeLoopbackTCPPorts(t, 2)
	ctl := newTestTCPGroupCtl(t, allowed, 0)

	ln1, port1, err := ctl.Listen("p1", "g1", "k", "127.0.0.1", 0)
	require.NoError(t, err)
	require.Contains(t, allowed, port1)

	ln2, port2, err := ctl.Listen("p2", "g2", "k", "127.0.0.1", 0)
	require.NoError(t, err)
	require.NotEqual(t, port1, port2)

	// While g1 is live its port cannot be allocated to g3.
	_, _, err = ctl.Listen("p3", "g3", "k", "127.0.0.1", 0)
	require.ErrorIs(t, err, ports.ErrNoAvailablePort)
	_, ok := ctl.get("g3")
	assert.False(t, ok, "failed creation must roll the empty group back")

	// Delete g1: its port becomes reusable by a new group.
	closeListenerAndWait(t, ln1)
	ln3, port3, err := ctl.Listen("p3-retry", "g3", "k", "127.0.0.1", 0)
	require.NoError(t, err)
	assert.Equal(t, port1, port3, "released port should be reallocated")

	closeListenerAndWait(t, ln2)
	closeListenerAndWait(t, ln3)
}

// TestTCPGroupCtl_FailedCreationIsRolledBack verifies that a Listen failure
// removes the placeholder group and immediately restores the group budget.
func TestTCPGroupCtl_FailedCreationIsRolledBack(t *testing.T) {
	allowed := freeLoopbackTCPPorts(t, 1)
	ctl := newTestTCPGroupCtl(t, allowed, 1)

	// Request a port outside the configured allow list: Acquire fails before
	// any resource is held, so creation must be rolled back.
	missingPort := allowed[0] + 1
	_, _, err := ctl.Listen("p-bad", "broken", "k", "127.0.0.1", missingPort)
	require.ErrorIs(t, err, ports.ErrPortNotAllowed)

	_, ok := ctl.get("broken")
	assert.False(t, ok, "failed group must not remain in the registry")
	assert.Equal(t, 0, ctl.len())

	// The single group slot must have been returned: creating a valid group
	// now succeeds instead of reporting the limit.
	ln, realPort, err := ctl.Listen("p-good", "broken", "k", "127.0.0.1", 0)
	require.NoError(t, err)
	require.Contains(t, allowed, realPort)
	assert.Equal(t, 1, ctl.len())
	closeListenerAndWait(t, ln)
	assert.Equal(t, 0, ctl.len())
}

// TestTCPGroupCtl_ExternallyOccupiedPortFallsBackAndRecordsActualPort covers
// fallback through the real controller: externally held candidates are
// skipped and the next bindable configured port is bound and reported.
func TestTCPGroupCtl_ExternallyOccupiedPortFallsBackAndRecordsActualPort(t *testing.T) {
	allowed := freeLoopbackTCPPorts(t, 4)

	// Hold the first three candidates with external listeners.
	held := make([]net.Listener, 0, 3)
	for _, p := range allowed[:3] {
		ln, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(p))
		require.NoError(t, err)
		held = append(held, ln)
	}
	t.Cleanup(func() {
		for _, ln := range held {
			_ = ln.Close()
		}
	})

	ctl := newTestTCPGroupCtl(t, allowed, 0)
	ln, realPort, err := ctl.Listen("scanner", "fallback-group", "k", "127.0.0.1", 0)
	require.NoError(t, err)
	assert.Equal(t, allowed[3], realPort, "should fall through to the only bindable port")

	// The returned listener and the stored binding must show the real port.
	tcpAddr, ok := ln.Addr().(*net.TCPAddr)
	require.True(t, ok)
	assert.Equal(t, allowed[3], tcpAddr.Port)

	g, ok := ctl.get("fallback-group")
	require.True(t, ok)
	assert.Equal(t, 0, g.port, "requested port selector stays zero (random)")
	assert.Equal(t, allowed[3], g.realPort, "actual bound port must be recorded")

	closeListenerAndWait(t, ln)
}

// TestTCPGroupCtl_ConcurrentGroupsAllGetDistinctPorts guarantees that
// concurrent creation of different groups never allocates one port twice.
func TestTCPGroupCtl_ConcurrentGroupsAllGetDistinctPorts(t *testing.T) {
	const n = 12
	allowed := freeLoopbackTCPPorts(t, n)
	ctl := newTestTCPGroupCtl(t, allowed, 0)

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		ports     = make(map[int]struct{}, n)
		errs      []error
		listeners []net.Listener
	)
	wg.Add(n)
	for i := range n {
		i := i
		go func() {
			defer wg.Done()
			ln, p, err := ctl.Listen(
				"proxy-"+strconv.Itoa(i),
				"group-"+strconv.Itoa(i),
				"k", "127.0.0.1", 0,
			)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			if _, dup := ports[p]; dup {
				errs = append(errs, fmt.Errorf("duplicate port allocated: %d", p))
			}
			ports[p] = struct{}{}
			listeners = append(listeners, ln)
		}()
	}
	wg.Wait()

	require.Empty(t, errs)
	assert.Len(t, ports, n)
	assert.Equal(t, n, ctl.len())
	for _, ln := range listeners {
		closeListenerAndWait(t, ln)
	}
	assert.Equal(t, 0, ctl.len())
}

// TestTCPGroupCtl_MaxGroupsReturnsClearError verifies the explicit limit
// error and that deleting a group frees a slot again.
func TestTCPGroupCtl_MaxGroupsReturnsClearError(t *testing.T) {
	allowed := freeLoopbackTCPPorts(t, 3)
	ctl := newTestTCPGroupCtl(t, allowed, 2)

	ln1, _, err := ctl.Listen("p1", "g1", "k", "127.0.0.1", 0)
	require.NoError(t, err)
	ln2, _, err := ctl.Listen("p2", "g2", "k", "127.0.0.1", 0)
	require.NoError(t, err)
	require.Equal(t, 2, ctl.len())

	_, _, err = ctl.Listen("p3", "g3", "k", "127.0.0.1", 0)
	require.ErrorIs(t, err, ErrGroupLimitExceeded)
	assert.Contains(t, err.Error(), "group limit exceeded")
	assert.Equal(t, 2, ctl.len(), "rejected creation must not add an entry")

	// Existing groups still accept additional listeners while the cap is
	// reached; track the returned listener so it can be closed too.
	ln1b, _, err := ctl.Listen("p1-second", "g1", "k", "127.0.0.1", 0)
	require.NoError(t, err)

	// Deleting g1 frees one slot once all of its listeners are closed.
	closeListenerAndWait(t, ln1)
	assert.Equal(t, 2, ctl.len(), "group stays until its last listener closes")
	closeListenerAndWait(t, ln1b)
	assert.Equal(t, 1, ctl.len())

	ln3, _, err := ctl.Listen("p3", "g3", "k", "127.0.0.1", 0)
	require.NoError(t, err)
	assert.Equal(t, 2, ctl.len())

	closeListenerAndWait(t, ln2)
	closeListenerAndWait(t, ln3)
	assert.Equal(t, 0, ctl.len())
}
