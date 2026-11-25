package ports

import (
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/fatedier/frp/pkg/config/types"
)

func TestManagerDoesNotReuseReservedPortWhileAcquired(t *testing.T) {
	for _, network := range []string{"tcp", "udp"} {
		t.Run(network, func(t *testing.T) {
			port, closePort := listenOnLoopback(t, network)
			defer closePort()
			alternate, closeAlternate := listenOnLoopback(t, network)
			defer closeAlternate()
			require.NoError(t, closePort())
			require.NoError(t, closeAlternate())
			pm := NewManager(network, "127.0.0.1", []types.PortsRange{{Single: port}, {Single: alternate}})

			_, err := pm.Acquire("first", port)
			require.NoError(t, err)
			pm.Release(port)

			_, err = pm.Acquire("other", alternate)
			require.NoError(t, err)
			got, err := pm.Acquire("first", 0)
			require.NoError(t, err)
			require.Equal(t, port, got)
			pm.Release(port)

			_, err = pm.Acquire("second", port)
			require.NoError(t, err)
			// The second proxy owns the port, even before it starts listening.
			_, err = pm.Acquire("first", 0)
			require.ErrorIs(t, err, ErrNoAvailablePort)

			pm.Release(alternate)
			got, err = pm.Acquire("first", 0)
			require.NoError(t, err)
			require.Equal(t, alternate, got)
			_, err = pm.Acquire("third", port)
			require.ErrorIs(t, err, ErrPortAlreadyUsed)
		})
	}
}

func TestManagerUsesClockForPortTimestamps(t *testing.T) {
	require := require.New(t)

	port := freeTCPPort(t)
	start := time.Date(2026, time.May, 8, 12, 30, 0, 0, time.UTC)
	clk := clocktesting.NewFakeClock(start)
	pm := newManagerWithClock("tcp", "127.0.0.1", []types.PortsRange{{Single: port}}, clk)

	realPort, err := pm.Acquire("proxy", port)
	require.NoError(err)
	require.Equal(port, realPort)
	require.Equal(start, pm.usedPorts[port].UpdateTime)

	releasedAt := start.Add(time.Minute)
	clk.SetTime(releasedAt)
	pm.Release(port)

	require.Equal(releasedAt, pm.reservedPorts["proxy"].UpdateTime)
}

func TestManagerCleanReservedPortsWorkerUsesClockTicker(t *testing.T) {
	require := require.New(t)

	port := freeTCPPort(t)
	start := time.Date(2026, time.May, 8, 12, 30, 0, 0, time.UTC)
	clk := clocktesting.NewFakeClock(start)
	pm := newManagerWithClock("tcp", "127.0.0.1", []types.PortsRange{{Single: port}}, clk)

	realPort, err := pm.Acquire("proxy", port)
	require.NoError(err)
	require.Equal(port, realPort)
	pm.Release(port)
	require.True(pm.hasReservedPort("proxy"))

	require.Eventually(clk.HasWaiters, time.Second, time.Millisecond)
	clk.Step(MaxPortReservedDuration + CleanReservedPortsInterval + time.Minute)

	require.Eventually(func() bool {
		return !pm.hasReservedPort("proxy")
	}, time.Second, time.Millisecond)
}

func (pm *Manager) hasReservedPort(name string) bool {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	_, ok := pm.reservedPorts[name]
	return ok
}

// TestManagerRandomAcquireSkipsExternallyOccupiedPorts verifies that a random
// acquisition walks past every port held by external processes and returns the
// next bindable candidate instead of giving up after a few tries.
func TestManagerRandomAcquireSkipsExternallyOccupiedPorts(t *testing.T) {
	require := require.New(t)

	allowed := freeTCPPorts(t, 4)
	pm := NewManager("tcp", "127.0.0.1", toRanges(allowed))

	// Hold three candidates externally. Random map iteration means the single
	// bindable port may sit at any position in the scan; the manager must keep
	// scanning until it finds it.
	var held []net.Listener
	for _, p := range allowed[:3] {
		ln, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(p))
		require.NoError(err)
		held = append(held, ln)
	}
	defer func() {
		for _, ln := range held {
			_ = ln.Close()
		}
	}()

	got, err := pm.Acquire("scanner", 0)
	require.NoError(err)
	require.Equal(allowed[3], got)
}

// TestManagerRandomAcquireExhaustionLeavesPoolConsistent verifies that once the
// whole pool is acquired, further acquisitions fail with a clear error and
// releasing one port makes it allocatable again.
func TestManagerRandomAcquireExhaustionLeavesPoolConsistent(t *testing.T) {
	require := require.New(t)

	allowed := freeTCPPorts(t, 3)
	pm := NewManager("tcp", "127.0.0.1", toRanges(allowed))

	got := make(map[int]struct{})
	for range allowed {
		p, err := pm.Acquire("drain", 0)
		require.NoError(err)
		got[p] = struct{}{}
	}
	require.Len(got, len(allowed))

	_, err := pm.Acquire("drain", 0)
	require.ErrorIs(err, ErrNoAvailablePort)

	pm.Release(allowed[1])
	again, err := pm.Acquire("drain", 0)
	require.NoError(err)
	require.Equal(allowed[1], again)
}

// TestManagerConcurrentAcquireReturnsDistinctPorts guarantees that parallel
// acquisitions never hand out the same port.
func TestManagerConcurrentAcquireReturnsDistinctPorts(t *testing.T) {
	require := require.New(t)

	const n = 12
	allowed := freeTCPPorts(t, n)
	pm := NewManager("tcp", "127.0.0.1", toRanges(allowed))

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		seen = make(map[int]struct{})
		errs []error
	)
	wg.Add(n)
	for i := range n {
		i := i
		go func() {
			defer wg.Done()
			p, err := pm.Acquire("p-"+strconv.Itoa(i), 0)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			seen[p] = struct{}{}
		}()
	}
	wg.Wait()

	require.Empty(errs)
	require.Len(seen, n)
}

func toRanges(ports []int) []types.PortsRange {
	ranges := make([]types.PortsRange, 0, len(ports))
	for _, p := range ports {
		ranges = append(ranges, types.PortsRange{Single: p})
	}
	return ranges
}

func freeTCPPort(t *testing.T) int {
	t.Helper()
	port, closePort := listenOnLoopback(t, "tcp")
	require.NoError(t, closePort())
	return port
}

// freeTCPPorts returns n currently-free loopback TCP ports.
func freeTCPPorts(t *testing.T, n int) []int {
	t.Helper()
	listeners := make([]net.Listener, 0, n)
	ports := make([]int, 0, n)
	defer func() {
		for _, ln := range listeners {
			_ = ln.Close()
		}
	}()
	for range n {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		listeners = append(listeners, ln)
		ports = append(ports, ln.Addr().(*net.TCPAddr).Port)
	}
	return ports
}

func listenOnLoopback(t *testing.T, network string) (int, func() error) {
	t.Helper()
	if network == "udp" {
		conn, err := net.ListenPacket(network, "127.0.0.1:0")
		require.NoError(t, err)
		return conn.LocalAddr().(*net.UDPAddr).Port, conn.Close
	}

	listener, err := net.Listen(network, "127.0.0.1:0")
	require.NoError(t, err)
	return listener.Addr().(*net.TCPAddr).Port, listener.Close
}
