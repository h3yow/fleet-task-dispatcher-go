package scheduler_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/fleet-task-dispatcher-go/internal/domain"
	"github.com/h3yow/fleet-task-dispatcher-go/internal/scheduler"
	"github.com/h3yow/fleet-task-dispatcher-go/internal/store"
)

// newHarness 构造一个使用临时持久化文件的调度器（每个用例独立文件，不预置机器人）。
func newHarness(t *testing.T) (*store.Store, *scheduler.Scheduler) {
	t.Helper()
	path := filepathJoin(t.TempDir(), "state.json")
	st := store.New(path)
	if err := st.Load(); err != nil {
		t.Fatalf("load empty store: %v", err)
	}
	return st, scheduler.New(st)
}

// addRobot 注册机器人并在失败时终止用例。
func addRobot(t *testing.T, sc *scheduler.Scheduler, r domain.Robot) {
	t.Helper()
	if err := sc.AddRobot(r); err != nil {
		t.Fatalf("add robot %s: %v", r.ID, err)
	}
}

func filepathJoin(dir, name string) string {
	return dir + "/" + name
}

func mustSubmit(t *testing.T, sc *scheduler.Scheduler, task domain.Task) {
	t.Helper()
	t.Logf("INPUT Submit task=%q pri=%d deps=%v demand=%+v", task.ID, task.Priority, task.Dependencies, task.Demand)
	if err := sc.Submit(task); err != nil {
		t.Fatalf("submit %s: %v", task.ID, err)
	}
}

func logStates(t *testing.T, st *store.Store, phase string) {
	t.Helper()
	snap := st.Snapshot()
	t.Logf("--- states @ %s ---", phase)
	for _, ts := range snap.Tasks {
		t.Logf("  task=%-8s status=%-9s assigned=%-2s reason=%-22s msg=%s",
			ts.Task.ID, ts.Status, ts.Assigned, ts.Reason, ts.Message)
	}
}

// expectReason 断言错误携带预期的可区分失败原因。
func expectReason(t *testing.T, err error, want domain.FailureReason) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error with reason %s, got nil", want)
	}
	te, ok := err.(*domain.TaskError)
	if !ok {
		t.Fatalf("expected *domain.TaskError, got %T: %v", err, err)
	}
	t.Logf("JUDGE rejection reason=%q message=%q", te.Reason, te.Message)
	if te.Reason != want {
		t.Fatalf("reason = %s, want %s (%v)", te.Reason, want, err)
	}
}

// TestCycleDependency 提交期自环必须被 CYCLE_DEPENDENCY 拒绝。
func TestCycleDependency(t *testing.T) {
	st, sc := newHarness(t)
	addRobot(t, sc, domain.Robot{ID: "r1", Capacity: domain.Resources{CPU: 4}})
	mustSubmit(t, sc, domain.Task{ID: "a", Demand: domain.Resources{CPU: 1}})

	loop := domain.Task{ID: "loop", Demand: domain.Resources{CPU: 1}, Dependencies: []string{"loop"}}
	t.Logf("INPUT submit self-dependency task=%q deps=%v (expect explicit refusal)", loop.ID, loop.Dependencies)
	err := sc.Submit(loop)
	expectReason(t, err, domain.ReasonCycleDependency)

	// 被拒绝的任务不得进入存储，更不会被静默等待。
	for _, ts := range st.Snapshot().Tasks {
		if ts.Task.ID == "loop" {
			t.Fatalf("rejected cyclic task must not be stored")
		}
	}
}

// TestExistingCycleFailsExplicitly 存量图（如被篡改的快照）中存在多节点环时，
// Tick 必须把环上任务明确判为 CYCLE_DEPENDENCY，而不是让它们一直 PENDING。
func TestExistingCycleFailsExplicitly(t *testing.T) {
	st, sc := newHarness(t)
	addRobot(t, sc, domain.Robot{ID: "r1", Capacity: domain.Resources{CPU: 4}})
	// 手工构造一个绕过 Submit 校验的环 x->y->x。
	st.Commit(func(tx *store.Tx) error {
		now := time.Now().UTC()
		tx.PutTask(domain.TaskState{
			Task:   domain.Task{ID: "x", Demand: domain.Resources{CPU: 1}, Dependencies: []string{"y"}},
			Status: domain.StatusPending, UpdatedAt: now,
		})
		tx.PutTask(domain.TaskState{
			Task:   domain.Task{ID: "y", Demand: domain.Resources{CPU: 1}, Dependencies: []string{"x"}},
			Status: domain.StatusPending, UpdatedAt: now,
		})
		return nil
	})
	t.Log("INPUT tampered snapshot containing cycle x->y->x; running Tick")
	if err := sc.Tick(); err != nil {
		t.Fatal(err)
	}
	logStates(t, st, "after tick")
	for _, id := range []string{"x", "y"} {
		var found domain.TaskState
		for _, ts := range st.Snapshot().Tasks {
			if ts.Task.ID == id {
				found = ts
			}
		}
		if found.Status != domain.StatusFailed || found.Reason != domain.ReasonCycleDependency {
			t.Fatalf("task %s = %s/%s, want FAILED/CYCLE_DEPENDENCY", id, found.Status, found.Reason)
		}
	}
}

// TestAdmissionReasons 覆盖重复 ID、未知依赖、容量不足三类可区分拒绝。
func TestAdmissionReasons(t *testing.T) {
	_, sc := newHarness(t)
	if err := sc.AddRobot(domain.Robot{ID: "r1", Capacity: domain.Resources{CPU: 2}}); err != nil {
		t.Fatal(err)
	}
	mustSubmit(t, sc, domain.Task{ID: "a", Demand: domain.Resources{CPU: 1}})

	dup := domain.Task{ID: "a", Demand: domain.Resources{CPU: 1}}
	t.Log("INPUT duplicate task id a")
	expectReason(t, sc.Submit(dup), domain.ReasonDuplicateTask)

	ghost := domain.Task{ID: "g", Demand: domain.Resources{CPU: 1}, Dependencies: []string{"missing"}}
	t.Log("INPUT task g depends on missing task")
	expectReason(t, sc.Submit(ghost), domain.ReasonTaskNotFound)

	big := domain.Task{ID: "big", Demand: domain.Resources{CPU: 3}}
	t.Logf("INPUT task big demand %+v exceeds robot capacity {CPU:2}", big.Demand)
	expectReason(t, sc.Submit(big), domain.ReasonInsufficientCapacity)
}

// TestDependencyFailureCascades 依赖失败/取消时，下游必须明确 FAILED 并带
// DEPENDENCY_FAILED 原因，不能静默等待；再下游同样级联失败。
func TestDependencyFailureCascades(t *testing.T) {
	st, sc := newHarness(t)
	if err := sc.AddRobot(domain.Robot{ID: "r1", Capacity: domain.Resources{CPU: 4}}); err != nil {
		t.Fatal(err)
	}
	mustSubmit(t, sc, domain.Task{ID: "a", Demand: domain.Resources{CPU: 1}})
	mustSubmit(t, sc, domain.Task{ID: "b", Demand: domain.Resources{CPU: 1}, Dependencies: []string{"a"}})
	mustSubmit(t, sc, domain.Task{ID: "c", Demand: domain.Resources{CPU: 1}, Dependencies: []string{"b"}})

	t.Log("INPUT cancel task a; b and c must fail with DEPENDENCY_FAILED")
	if err := sc.Cancel("a"); err != nil {
		t.Fatal(err)
	}
	if err := sc.Tick(); err != nil {
		t.Fatal(err)
	}
	logStates(t, st, "after cancellation + tick")
	byID := map[string]domain.TaskState{}
	for _, ts := range st.Snapshot().Tasks {
		byID[ts.Task.ID] = ts
	}
	if byID["a"].Status != domain.StatusCanceled {
		t.Fatalf("a = %s, want CANCELED", byID["a"].Status)
	}
	for _, id := range []string{"b", "c"} {
		if byID[id].Status != domain.StatusFailed || byID[id].Reason != domain.ReasonDependencyFailed {
			t.Fatalf("%s = %s/%s, want FAILED/DEPENDENCY_FAILED", id, byID[id].Status, byID[id].Reason)
		}
		t.Logf("JUDGE task %s FAILED because dependency chain broken: %q", id, byID[id].Message)
	}
}

// TestConcurrentDispatchNoDuplicate 多入口并发 Tick：同一批任务只能被派发一次，
// 资源占用不得重复计入。
func TestConcurrentDispatchNoDuplicate(t *testing.T) {
	st, sc := newHarness(t)
	if err := sc.AddRobot(domain.Robot{ID: "r1", Capacity: domain.Resources{CPU: 8}}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		mustSubmit(t, sc, domain.Task{
			ID:     fmt.Sprintf("t%d", i),
			Demand: domain.Resources{CPU: 2},
		})
	}

	t.Log("INPUT 50 goroutines concurrently calling Tick (contending dispatch entries)")
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = sc.Tick() }()
	}
	wg.Wait()

	snap := st.Snapshot()
	assigned := 0
	used := domain.Resources{}
	for _, ts := range snap.Tasks {
		if ts.Status == domain.StatusRunning {
			assigned++
			if ts.Assigned != "r1" {
				t.Fatalf("task %s assigned to unexpected robot %q", ts.Task.ID, ts.Assigned)
			}
			used = used.Add(ts.Task.Demand)
		}
	}
	t.Logf("JUDGE running=%d used=%+v robot-capacity={CPU:8}", assigned, used)
	if assigned != 4 {
		t.Fatalf("assigned count = %d, want exactly 4 (no duplicate dispatch)", assigned)
	}
	if used != (domain.Resources{CPU: 8}) {
		t.Fatalf("used = %+v, want {CPU:8} (no double counting)", used)
	}
}

// TestPreemptionKeepsVictimExplainable 高优先级任务抢占：低优先级任务保留为
// PREEMPTED 并释放占用；高优先级任务完成后低优先级可重新派发。
func TestPreemptionKeepsVictimExplainable(t *testing.T) {
	st, sc := newHarness(t)
	if err := sc.AddRobot(domain.Robot{ID: "r1", Capacity: domain.Resources{CPU: 4}}); err != nil {
		t.Fatal(err)
	}
	mustSubmit(t, sc, domain.Task{ID: "low", Priority: 1, Demand: domain.Resources{CPU: 4}})
	if err := sc.Tick(); err != nil {
		t.Fatal(err)
	}
	logStates(t, st, "low assigned")

	mustSubmit(t, sc, domain.Task{ID: "high", Priority: 9, Demand: domain.Resources{CPU: 4}})
	t.Log("INPUT high-priority task arrives while low occupies the robot fully")
	if err := sc.Tick(); err != nil {
		t.Fatal(err)
	}
	logStates(t, st, "after preemption tick")

	snap := st.Snapshot()
	byID := map[string]domain.TaskState{}
	for _, ts := range snap.Tasks {
		byID[ts.Task.ID] = ts
	}
	if byID["high"].Status != domain.StatusRunning || byID["high"].Assigned != "r1" {
		t.Fatalf("high = %s/%s, want RUNNING/r1", byID["high"].Status, byID["high"].Assigned)
	}
	if byID["low"].Status != domain.StatusPreempted {
		t.Fatalf("low = %s, want PREEMPTED (preserved, not silently lost)", byID["low"].Status)
	}
	if byID["low"].Assigned != "" {
		t.Fatalf("preempted low still holds robot %q", byID["low"].Assigned)
	}
	t.Logf("JUDGE low preserved as PREEMPTED: %q", byID["low"].Message)

	// 再 Tick：被抢占任务重新 READY 但因容量被 high 占用而等待，不静默失败。
	if err := sc.Tick(); err != nil {
		t.Fatal(err)
	}
	st.View(func(tx *store.Tx) {
		low, _ := tx.Task("low")
		if low.Status != domain.StatusReady {
			t.Fatalf("low = %s, want READY waiting for resources", low.Status)
		}
		avail := tx.Available("r1")
		t.Logf("JUDGE r1 available while high runs: %+v (must be non-negative)", avail)
	})

	// high 完成（模拟执行器回写），释放后 Tick 可重新派发 low。
	st.Commit(func(tx *store.Tx) error {
		h, _ := tx.Task("high")
		h.Status = domain.StatusSucceeded
		h.Assigned = ""
		h.UpdatedAt = time.Now().UTC()
		tx.PutTask(h)
		tx.Emit(domain.EventSucceeded, "high", "r1", "succeeded in test", "")
		return nil
	})
	if err := sc.Tick(); err != nil {
		t.Fatal(err)
	}
	st.View(func(tx *store.Tx) {
		low, _ := tx.Task("low")
		if low.Status != domain.StatusRunning || low.Assigned != "r1" {
			t.Fatalf("low = %s/%s, want re-dispatched RUNNING/r1", low.Status, low.Assigned)
		}
	})
	logStates(t, st, "after high completed and low re-dispatched")
}

// TestNoPreemptionAcrossSameOrHigherPriority 不能抢占同优先级或更高优先级任务。
func TestNoPreemptionAcrossSameOrHigherPriority(t *testing.T) {
	st, sc := newHarness(t)
	if err := sc.AddRobot(domain.Robot{ID: "r1", Capacity: domain.Resources{CPU: 4}}); err != nil {
		t.Fatal(err)
	}
	mustSubmit(t, sc, domain.Task{ID: "running", Priority: 5, Demand: domain.Resources{CPU: 4}})
	if err := sc.Tick(); err != nil {
		t.Fatal(err)
	}
	mustSubmit(t, sc, domain.Task{ID: "incoming", Priority: 5, Demand: domain.Resources{CPU: 4}})
	t.Log("INPUT same-priority incoming task: preemption must not happen")
	if err := sc.Tick(); err != nil {
		t.Fatal(err)
	}
	logStates(t, st, "after tick")
	st.View(func(tx *store.Tx) {
		running, _ := tx.Task("running")
		incoming, _ := tx.Task("incoming")
		if running.Status != domain.StatusRunning {
			t.Fatalf("running = %s, must stay RUNNING", running.Status)
		}
		if incoming.Status != domain.StatusReady {
			t.Fatalf("incoming = %s, must stay READY (no forced dispatch)", incoming.Status)
		}
	})
}

// TestCancelReleasesResources 取消排队中和运行中的任务后，占用完整释放、可用量非负。
func TestCancelReleasesResources(t *testing.T) {
	st, sc := newHarness(t)
	if err := sc.AddRobot(domain.Robot{ID: "r1", Capacity: domain.Resources{CPU: 4}}); err != nil {
		t.Fatal(err)
	}
	mustSubmit(t, sc, domain.Task{ID: "hold", Priority: 2, Demand: domain.Resources{CPU: 3}})
	mustSubmit(t, sc, domain.Task{ID: "wait", Priority: 1, Demand: domain.Resources{CPU: 2}})
	if err := sc.Tick(); err != nil {
		t.Fatal(err)
	}
	st.View(func(tx *store.Tx) {
		t.Logf("JUDGE before cancel: available=%+v", tx.Available("r1"))
	})
	t.Log("INPUT cancel hold; wait must become dispatchable afterwards")
	if err := sc.Cancel("hold"); err != nil {
		t.Fatal(err)
	}
	if err := sc.Tick(); err != nil {
		t.Fatal(err)
	}
	st.View(func(tx *store.Tx) {
		hold, _ := tx.Task("hold")
		wait, _ := tx.Task("wait")
		avail := tx.Available("r1")
		t.Logf("JUDGE after cancel: hold=%s wait=%s available=%+v", hold.Status, wait.Status, avail)
		if hold.Status != domain.StatusCanceled {
			t.Fatalf("hold = %s, want CANCELED", hold.Status)
		}
		if wait.Status != domain.StatusRunning {
			t.Fatalf("wait = %s, want RUNNING after resources released", wait.Status)
		}
		if avail != (domain.Resources{CPU: 2}) {
			t.Fatalf("available = %+v, want {CPU:2}", avail)
		}
		if avail.AnyNegative() {
			t.Fatal("available resources must never be negative")
		}
	})
}
