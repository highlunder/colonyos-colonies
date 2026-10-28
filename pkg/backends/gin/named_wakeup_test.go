package gin

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colonyos/colonies/pkg/core"
	"github.com/stretchr/testify/assert"
)

// waitForListeners blocks until the handler reports n listeners for the target,
// so a test can control registration order instead of racing goroutines.
//
// Order matters here: Pass 2 of sendSignal walks general listeners in sorted
// listener-ID order from nextExecutor[target], and listener IDs are assigned in
// registration order. A test that starts both waiters at once would therefore
// pass or fail depending on which goroutine registered first.
func waitForListeners(t *testing.T, handler interface {
	NumberOfListeners(string, int, string) (int, int, int)
}, executorType string, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, listeners, _ := handler.NumberOfListeners(executorType, core.WAITING, ""); listeners == n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	_, listeners, _ := handler.NumberOfListeners(executorType, core.WAITING, "")
	t.Fatalf("timed out waiting for %d listeners, have %d", n, listeners)
}

// TestNamedProcessWakesItsTarget verifies that a process addressed to one
// executor by name wakes that executor rather than another executor of the same
// type.
//
// The single-wake property that TestSingleWakeUp and TestNoAmplification pin
// down must survive: this is about WHICH listener is woken, never how many. Both
// assertions are made here, so a fix that woke every matching listener would
// fail this test too.
//
// Both registration orders are exercised on purpose. Round-robin starts at the
// first registered listener, so the case where the named executor registers
// SECOND is the one that fails before the fix — and testing only the other
// order would look green while the defect was untouched.
func TestNamedProcessWakesItsTarget(t *testing.T) {
	const (
		executorType = "test-executor"
		target       = "ice1"
		other        = "hudiksvall"
	)

	for _, tc := range []struct {
		name  string
		order []string
	}{
		{"named executor registers first", []string{target, other}},
		{"named executor registers second", []string{other, target}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := CreateTestableEventHandler(nil)
			defer handler.Stop()

			funcSpec := core.CreateEmptyFunctionSpec()
			funcSpec.Conditions.ExecutorType = executorType
			funcSpec.Conditions.ExecutorNames = []string{target}
			process := core.CreateProcess(funcSpec)
			process.State = core.WAITING

			var woke sync.Map // executor name -> *int32
			for _, n := range tc.order {
				var c int32
				woke.Store(n, &c)
			}
			bump := func(name string) {
				v, _ := woke.Load(name)
				atomic.AddInt32(v.(*int32), 1)
			}
			count := func(name string) int32 {
				v, _ := woke.Load(name)
				return atomic.LoadInt32(v.(*int32))
			}

			var wg sync.WaitGroup
			for i, name := range tc.order {
				wg.Add(1)
				go func(name string) {
					defer wg.Done()
					// Long enough to outlive the signal, short enough that a
					// test which never wakes anyone still finishes quickly.
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					defer cancel()
					if _, err := handler.WaitForProcess(executorType, name, core.WAITING, "", "", ctx); err == nil {
						bump(name)
					}
				}(name)
				// Serialise registration so listener IDs follow tc.order.
				waitForListeners(t, handler, executorType, i+1)
			}

			handler.Signal(process)
			wg.Wait()

			assert.Equal(t, int32(1), count(target)+count(other),
				"exactly one listener must wake per signal — the thundering herd fix must not regress")
			assert.Equal(t, int32(1), count(target),
				"the executor the process is addressed to must be the one woken")
			assert.Equal(t, int32(0), count(other),
				"an executor that cannot satisfy the process's conditions must not be woken: "+
					"the wake is wasted and the process waits out the named executor's long poll")
		})
	}
}

// TestUnnamedProcessWakesOneEligibleListener verifies that a process with no
// name condition is unaffected by name-aware wake-up: one listener wakes per
// signal, and signals are not amplified.
//
// This is the regression guard in the other direction. Preferring a named
// listener must not change what happens when there is no name to prefer.
//
// It deliberately does NOT assert that the two executors each get work.
// nextExecutor is a positional index over listener IDs, and a woken executor
// re-registers with a fresh ID before the next signal, so which listener is
// "next" is reshuffled by that churn: two signals can both land on the same
// executor. That is pre-existing behaviour, not something name-aware wake-up
// introduces, and asserting fairness here would be asserting a property the
// implementation does not offer. Noted rather than bundled into this change.
func TestUnnamedProcessWakesOneEligibleListener(t *testing.T) {
	const executorType = "test-executor"
	const signals = 4
	names := []string{"ice1", "hudiksvall"}

	handler := CreateTestableEventHandler(nil)
	defer handler.Stop()

	funcSpec := core.CreateEmptyFunctionSpec()
	funcSpec.Conditions.ExecutorType = executorType // no ExecutorNames
	process := core.CreateProcess(funcSpec)
	process.State = core.WAITING

	var mu sync.Mutex
	woken := map[string]int{}

	var wg sync.WaitGroup
	for i, name := range names {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			for {
				ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
				_, err := handler.WaitForProcess(executorType, name, core.WAITING, "", "", ctx)
				cancel()
				if err != nil {
					return // timed out: no more signals coming
				}
				mu.Lock()
				woken[name]++
				mu.Unlock()
			}
		}(name)
		waitForListeners(t, handler, executorType, i+1)
	}

	for i := 0; i < signals; i++ {
		handler.Signal(process)
		// Let the wake be consumed and the waiter re-register, so signals are
		// not absorbed into one listener's 100-deep buffer.
		time.Sleep(150 * time.Millisecond)
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	total := 0
	for _, n := range woken {
		total += n
	}
	assert.Equal(t, signals, total,
		"each signal must wake exactly one listener — no amplification, no lost wake: %v", woken)
}
