package group

import (
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetOrCreate_New(t *testing.T) {
	r := newGroupRegistry[*int]()
	called := 0
	v := 42
	got, err := r.getOrCreate("k", func() *int { called++; return &v })
	require.NoError(t, err)
	assert.Equal(t, 1, called)
	assert.Equal(t, &v, got)
}

func TestGetOrCreate_Existing(t *testing.T) {
	r := newGroupRegistry[*int]()
	v := 42
	_, err := r.getOrCreate("k", func() *int { return &v })
	require.NoError(t, err)

	called := 0
	got, err := r.getOrCreate("k", func() *int { called++; return nil })
	require.NoError(t, err)
	assert.Equal(t, 0, called)
	assert.Equal(t, &v, got)
}

func TestGet_ExistingAndMissing(t *testing.T) {
	r := newGroupRegistry[*int]()
	v := 1
	_, err := r.getOrCreate("k", func() *int { return &v })
	require.NoError(t, err)

	got, ok := r.get("k")
	assert.True(t, ok)
	assert.Equal(t, &v, got)

	_, ok = r.get("missing")
	assert.False(t, ok)
}

func TestIsCurrent(t *testing.T) {
	r := newGroupRegistry[*int]()
	v1 := 1
	v2 := 2
	_, err := r.getOrCreate("k", func() *int { return &v1 })
	require.NoError(t, err)

	assert.True(t, r.isCurrent("k", func(g *int) bool { return g == &v1 }))
	assert.False(t, r.isCurrent("k", func(g *int) bool { return g == &v2 }))
	assert.False(t, r.isCurrent("missing", func(g *int) bool { return true }))
}

func TestRemoveIf(t *testing.T) {
	t.Run("removes when fn returns true", func(t *testing.T) {
		r := newGroupRegistry[*int]()
		v := 1
		_, err := r.getOrCreate("k", func() *int { return &v })
		require.NoError(t, err)
		r.removeIf("k", func(g *int) bool { return g == &v })
		_, ok := r.get("k")
		assert.False(t, ok)
	})

	t.Run("keeps when fn returns false", func(t *testing.T) {
		r := newGroupRegistry[*int]()
		v := 1
		_, err := r.getOrCreate("k", func() *int { return &v })
		require.NoError(t, err)
		r.removeIf("k", func(g *int) bool { return false })
		_, ok := r.get("k")
		assert.True(t, ok)
	})

	t.Run("noop on missing key", func(t *testing.T) {
		r := newGroupRegistry[*int]()
		r.removeIf("missing", func(g *int) bool { return true }) // should not panic
	})
}

func TestGetOrCreate_GroupLimit(t *testing.T) {
	r := newGroupRegistry[*int]().withMaxGroups(2)

	v1, v2, v3 := 1, 2, 3
	g1, err := r.getOrCreate("k1", func() *int { return &v1 })
	require.NoError(t, err)
	require.NotNil(t, g1)
	g2, err := r.getOrCreate("k2", func() *int { return &v2 })
	require.NoError(t, err)
	require.NotNil(t, g2)
	assert.Equal(t, 2, r.len())

	// A new key must be rejected once the cap is reached.
	got, err := r.getOrCreate("k3", func() *int { return &v3 })
	assert.ErrorIs(t, err, ErrGroupLimitExceeded)
	assert.Nil(t, got)
	// Rejection must not leave a placeholder entry behind.
	_, ok := r.get("k3")
	assert.False(t, ok)
	assert.Equal(t, 2, r.len())

	// Existing keys remain accessible even when the cap is reached.
	existing, err := r.getOrCreate("k1", func() *int { return &v3 })
	require.NoError(t, err)
	assert.Equal(t, &v1, existing)
}

func TestRemoveIfEmpty_RollsBackFailedCreation(t *testing.T) {
	r := newGroupRegistry[*int]().withMaxGroups(1)
	v := 1
	g, err := r.getOrCreate("k", func() *int { return &v })
	require.NoError(t, err)

	// Simulate a failed Listen: the entry is still "empty" and gets rolled back.
	r.removeIfEmpty("k", func(cur *int) bool { return cur == g })
	_, ok := r.get("k")
	assert.False(t, ok)
	assert.Equal(t, 0, r.len())

	// The budget is available again after rollback.
	again, err := r.getOrCreate("k", func() *int { return &v })
	require.NoError(t, err)
	assert.Equal(t, &v, again)
}

func TestRemoveIfEmpty_KeepsNonEmptyGroup(t *testing.T) {
	r := newGroupRegistry[*int]()
	v := 1
	g, err := r.getOrCreate("k", func() *int { return &v })
	require.NoError(t, err)

	// The predicate reports the group as initialized, so it must stay.
	r.removeIfEmpty("k", func(cur *int) bool { return cur != g })
	_, ok := r.get("k")
	assert.True(t, ok)
}

func TestConcurrentGetOrCreateAndRemoveIf(t *testing.T) {
	r := newGroupRegistry[*int]()
	const n = 100
	var wg sync.WaitGroup
	wg.Add(n * 2)
	for i := range n {
		v := i
		go func() {
			defer wg.Done()
			_, _ = r.getOrCreate("k", func() *int { return &v })
		}()
		go func() {
			defer wg.Done()
			r.removeIf("k", func(*int) bool { return true })
		}()
	}
	wg.Wait()

	// After all goroutines finish, accessing the key must not panic.
	require.NotPanics(t, func() {
		_, _ = r.get("k")
	})
}

func TestConcurrentGetOrCreate_RespectsLimit(t *testing.T) {
	const limit = 10
	r := newGroupRegistry[*int]().withMaxGroups(limit)
	const n = 100
	var (
		wg       sync.WaitGroup
		success  int
		rejected int
		mu       sync.Mutex
	)
	wg.Add(n)
	for i := range n {
		i := i
		go func() {
			defer wg.Done()
			// Each writer targets its own key so every success consumes one
			// slot of the limited budget.
			_, err := r.getOrCreate("k-"+strconv.Itoa(i), func() *int {
				v := i
				return &v
			})
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				success++
			} else {
				rejected++
				assert.ErrorIs(t, err, ErrGroupLimitExceeded)
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, limit, success)
	assert.Equal(t, n-limit, rejected)
	assert.Equal(t, limit, r.len())
}
