package process_test

import (
	"testing"
	"time"

	"github.com/colonyos/colonies/pkg/client"
	"github.com/colonyos/colonies/pkg/server"
	"github.com/colonyos/colonies/pkg/utils"
	"github.com/stretchr/testify/assert"
)

// namedAssignTimeout is the long poll each executor asks for, in seconds. Long
// enough that only a wake-up can plausibly deliver the process inside it, short
// enough that the test is not slow: with the defect present the addressed
// executor waits the whole of it out and fails.
const namedAssignTimeout = 5

// listenerSettleTime is how long to allow an executor to get from "its assign
// request reached the server" to "it is parked in WaitForProcess".
const listenerSettleTime = 750 * time.Millisecond

// The polling below uses an EXECUTOR key, not the colony key.
// HandleGetExecutors calls RequireMembership, and the project's own security
// test asserts that the colony owner is refused: "Should not work, colony owner
// can't get executors". env.ExecutorPrvKey belongs to the default executor from
// SetupTestEnv2, which is a member and is not one of the two executors whose
// registration order this test controls.
//
// waitForHeartbeat blocks until the named executor's LastHeardFromTime has
// advanced past baseline, which proves its assign request reached the server:
// MarkAlive is the first statement of both Assign and DistributedAssign, before
// anything is selected.
//
// This is how the test orders two concurrent executors deterministically.
// Without it the two goroutines race, and the test is a coin flip — see the note
// on TestNamedProcessAssignedWithTwoExecutorsWaiting.
func waitForHeartbeat(t *testing.T, c *client.ColoniesClient, colonyName, prvKey, executorName string, baseline time.Time) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		executors, err := c.GetExecutors(colonyName, prvKey)
		if err != nil {
			// Report it rather than retrying blindly into a timeout: a
			// permission error here looks exactly like an executor that never
			// called in, and the two need different fixes.
			t.Fatalf("GetExecutors failed while waiting for %s: %v", executorName, err)
		}
		for _, e := range executors {
			if e.Name == executorName && e.LastHeardFromTime.After(baseline) {
				return
			}
		}
		lastErr = err
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s to contact the server (last error: %v)", executorName, lastErr)
}

func heartbeatOf(t *testing.T, c *client.ColoniesClient, colonyName, prvKey, executorName string) time.Time {
	t.Helper()
	executors, err := c.GetExecutors(colonyName, prvKey)
	if err != nil {
		t.Fatalf("GetExecutors failed: %v", err)
	}
	for _, e := range executors {
		if e.Name == executorName {
			return e.LastHeardFromTime
		}
	}
	t.Fatalf("executor %s not found among %d executors", executorName, len(executors))
	return time.Time{}
}

// TestNamedProcessAssignedWithTwoExecutorsWaiting checks that a process
// addressed to one executor by name reaches that executor while another
// executor of the same type is also waiting for work.
//
// It is the integration counterpart to TestNamedProcessWakesItsTarget in
// pkg/backends/gin, which pins the same behaviour on the event handler alone.
// Both are worth having: the unit test says which listener is woken, this one
// says the process is actually assigned, through the real client, server,
// scheduler and database.
//
// With a condition-blind wake-up the failure is not merely slow. One listener is
// woken per submission, and if it is the wrong one, nothing wakes the right one:
// the addressed executor waits out its entire long poll and its Assign returns
// an error, while the process stays WAITING with two idle executors that could
// have run it.
//
// # Two details this test depends on, both deliberate
//
// **The executors register in a fixed order, the WRONG one first.** The
// round-robin index starts at zero and cannot advance before a listener exists
// (sendSignal returns early when there are no general listeners), so the first
// wake goes to whichever listener registered first. A version of this test that
// simply started both goroutines passed with the defect present roughly half the
// time. Ordering is what makes it a regression test rather than a coin flip.
//
// **The process is submitted only once both executors are parked in
// WaitForProcess.** Submit earlier and the addressed executor picks the process
// up on its first assign attempt, before any listener exists, and the wake-up
// path is never involved. That is precisely why no existing test in this suite
// catches this: every named-executor test submits first, and all of them pass a
// timeout of -1, which returns immediately and never registers a listener at
// all.
func TestNamedProcessAssignedWithTwoExecutorsWaiting(t *testing.T) {
	env, c, coloniesServer, _, done := server.SetupTestEnv2(t)

	// Two executors of the same type: interchangeable to a type-only
	// submission, distinguishable only by name.
	executor1, executor1PrvKey, err := utils.CreateTestExecutorWithKey(env.ColonyName)
	assert.Nil(t, err)
	executor1.Name = "executor1"
	_, err = c.AddExecutor(executor1, env.ColonyPrvKey)
	assert.Nil(t, err)
	err = c.ApproveExecutor(env.ColonyName, executor1.Name, env.ColonyPrvKey)
	assert.Nil(t, err)

	executor2, executor2PrvKey, err := utils.CreateTestExecutorWithKey(env.ColonyName)
	assert.Nil(t, err)
	executor2.Name = "executor2"
	_, err = c.AddExecutor(executor2, env.ColonyPrvKey)
	assert.Nil(t, err)
	err = c.ApproveExecutor(env.ColonyName, executor2.Name, env.ColonyPrvKey)
	assert.Nil(t, err)

	type assignResult struct {
		name      string
		processID string
		waited    time.Duration
		err       error
	}
	results := make(chan assignResult, 2)

	assignIn := func(name, prvKey string) {
		go func() {
			start := time.Now()
			process, err := c.Assign(env.ColonyName, namedAssignTimeout, "", "", prvKey)
			r := assignResult{name: name, waited: time.Since(start), err: err}
			if process != nil {
				r.processID = process.ID
			}
			results <- r
		}()
	}

	// executor2 first: it is the one that cannot take the work, so with a
	// condition-blind wake-up it is the one that gets woken.
	baseline2 := heartbeatOf(t, c, env.ColonyName, env.ExecutorPrvKey, "executor2")
	assignIn("executor2", executor2PrvKey)
	waitForHeartbeat(t, c, env.ColonyName, env.ExecutorPrvKey, "executor2", baseline2)
	time.Sleep(listenerSettleTime)

	baseline1 := heartbeatOf(t, c, env.ColonyName, env.ExecutorPrvKey, "executor1")
	assignIn("executor1", executor1PrvKey)
	waitForHeartbeat(t, c, env.ColonyName, env.ExecutorPrvKey, "executor1", baseline1)
	time.Sleep(listenerSettleTime)

	funcSpec := utils.CreateTestFunctionSpec(env.ColonyName)
	funcSpec.Conditions.ExecutorNames = []string{"executor1"}
	addedProcess, err := c.Submit(funcSpec, env.ExecutorPrvKey)
	assert.Nil(t, err)
	assert.NotNil(t, addedProcess)

	byName := map[string]assignResult{}
	for i := 0; i < 2; i++ {
		r := <-results
		byName[r.name] = r
	}
	got := byName["executor1"]
	other := byName["executor2"]

	assert.Nil(t, got.err,
		"the executor the process is addressed to must be assigned it, not time out "+
			"(waited %s of a %ds timeout)", got.waited, namedAssignTimeout)
	if got.err == nil {
		assert.Equal(t, addedProcess.ID, got.processID,
			"executor1 must be assigned the process addressed to it")
	}
	assert.Less(t, got.waited, (namedAssignTimeout-1)*time.Second,
		"executor1 should have been woken on submission, not left to wait out its long poll")

	// executor2 cannot satisfy the condition, so it must come back empty. Were
	// it ever assigned this process, the scheduler would be ignoring the name
	// condition, which is a different and far worse bug.
	assert.Empty(t, other.processID,
		"an executor the process is not addressed to must not be assigned it")

	coloniesServer.Shutdown()
	<-done
}
