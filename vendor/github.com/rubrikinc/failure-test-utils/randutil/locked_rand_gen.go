package randutil

import (
	"math/rand"
	"sync"
)

// LockedRandGen wrapper on top of math/rand that leverage locking when
// generating random number and avoid concurrency issue between go-routines
// sharing the generator.
type LockedRandGen struct {
	*rand.Rand
	mu sync.Mutex
}

// NewLockedRandGen creates a new instance of LockedRanGen
func NewLockedRandGen(seed int64) *LockedRandGen {
	return &LockedRandGen{
		Rand: rand.New(rand.NewSource(seed)),
		mu:   sync.Mutex{},
	}
}

// Int31n generates a non-negative pseudo random number between
// [0,n) using synchronization mechanism
func (r *LockedRandGen) Int31n(n int32) int32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.Rand.Int31n(n)
}

// Intn generates a non-negative pseudo random number between
// [0,n) using synchronization mechanism
func (r *LockedRandGen) Intn(n int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.Rand.Intn(n)
}

// Int31nWOLockForTest used for testing purpose only
func (r *LockedRandGen) Int31nWOLockForTest(n int32) int32 {
	return r.Rand.Int31n(n)
}
