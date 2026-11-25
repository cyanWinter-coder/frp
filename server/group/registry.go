package group

import (
	"sync"
)

// groupRegistry is a concurrent map of named groups with
// automatic creation on first access.
type groupRegistry[G any] struct {
	groups map[string]G
	mu     sync.Mutex

	// maxGroups limits the number of concurrently registered groups. A value
	// of 0 means no limit.
	maxGroups int
}

func newGroupRegistry[G any]() *groupRegistry[G] {
	return &groupRegistry[G]{
		groups: make(map[string]G),
	}
}

// withMaxGroups sets a group-count limit on the registry and returns it for
// chaining. A maxGroups value of 0 means no limit.
func (r *groupRegistry[G]) withMaxGroups(maxGroups int) *groupRegistry[G] {
	r.maxGroups = maxGroups
	return r
}

// getOrCreate returns the group registered under key, creating one with
// newFn when it is missing. When the registry has a group-count limit and
// adding a new entry would exceed it, getOrCreate returns ErrGroupLimitExceeded
// and no entry is created.
func (r *groupRegistry[G]) getOrCreate(key string, newFn func() G) (G, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, ok := r.groups[key]
	if ok {
		return g, nil
	}
	if r.maxGroups > 0 && len(r.groups) >= r.maxGroups {
		var zero G
		return zero, ErrGroupLimitExceeded
	}
	g = newFn()
	r.groups[key] = g
	return g, nil
}

func (r *groupRegistry[G]) get(key string) (G, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, ok := r.groups[key]
	return g, ok
}

// len returns the number of registered groups.
func (r *groupRegistry[G]) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.groups)
}

// isCurrent returns true if key exists in the registry and matchFn
// returns true for the stored value.
func (r *groupRegistry[G]) isCurrent(key string, matchFn func(G) bool) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, ok := r.groups[key]
	return ok && matchFn(g)
}

// removeIf atomically looks up the group for key, calls fn on it,
// and removes the entry if fn returns true.
func (r *groupRegistry[G]) removeIf(key string, fn func(G) bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, ok := r.groups[key]
	if !ok {
		return
	}
	if fn(g) {
		delete(r.groups, key)
	}
}

// removeIfEmpty removes the group for key when matchFn reports it as empty.
// It is used to roll back a getOrCreate whose subsequent Listen failed before
// the group acquired any resource.
func (r *groupRegistry[G]) removeIfEmpty(key string, matchFn func(G) bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, ok := r.groups[key]
	if ok && matchFn(g) {
		delete(r.groups, key)
	}
}
