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

// reservedRobot 注册一台带预留区的机器人，失败即终止用例。
func reservedRobot(t *testing.T, sc *scheduler.Scheduler, id string, cap, res domain.Resources, minPri int) {
	t.Helper()
	r := domain.Robot{ID: id, Capacity: cap, Reserved: res, ReserveMinPriority: minPri}
	t.Logf("INPUT AddRobot id=%s capacity=%+v reserved=%+v reserve-min-priority=%d", id, cap, res, minPri)
	if err := sc.AddRobot(r); err != nil {
		t.Fatalf("add robot %s: %v", id, err)
	}
}

func taskByID(t *testing.T, st *store.Store, id string) domain.TaskState {
	t.Helper()
	var found domain.TaskState
	ok := false
	st.View(func(tx *store.Tx) {
		found, ok = tx.Task(id)
	})
	if !ok {
		t.Fatalf("task %s missing", id)
	}
	return found
}

// TestReservedPoolOnlyForEligiblePriority 预留区只对达到门槛的任务开放：
// 公共池被低优先级任务占满后，未达门槛的任务只能 READY 等待，
// 而达门槛的高优先级任务直接吃进预留区，且不打断任何在途任务。
func TestReservedPoolOnlyForEligiblePriority(t *testing.T) {
	st, sc := newHarness(t)
	// 总 4 CPU，预留 2，门槛 5：公共池只有 2。
	reservedRobot(t, sc, "r1", domain.Resources{CPU: 4}, domain.Resources{CPU: 2}, 5)

	mustSubmit(t, sc, domain.Task{ID: "low", Priority: 2, Demand: domain.Resources{CPU: 2}})
	if err := sc.Tick(); err != nil {
		t.Fatal(err)
	}
	t.Log("INPUT low(pri=2,CPU=2) dispatched, common pool now exhausted; reserved {CPU:2} untouched")

	// mid 与 low 同优先级（都未达门槛）：mid 不能抢占 low，只能证明它确实够不到预留区。
	mustSubmit(t, sc, domain.Task{ID: "mid", Priority: 2, Demand: domain.Resources{CPU: 1}})
	mustSubmit(t, sc, domain.Task{ID: "high", Priority: 9, Demand: domain.Resources{CPU: 2}})
	t.Log("INPUT mid(pri=2,CPU=1) below threshold, high(pri=9,CPU=2) above threshold; one Tick")
	if err := sc.Tick(); err != nil {
		t.Fatal(err)
	}
	logStates(t, st, "after reserved-aware dispatch")

	low, mid, high := taskByID(t, st, "low"), taskByID(t, st, "mid"), taskByID(t, st, "high")
	if high.Status != domain.StatusRunning || high.Assigned != "r1" {
		t.Fatalf("high = %s/%s, want RUNNING/r1 (reserved pool must serve it)", high.Status, high.Assigned)
	}
	if mid.Status != domain.StatusReady {
		t.Fatalf("mid = %s, want READY (must not touch reserved pool)", mid.Status)
	}
	if low.Status != domain.StatusRunning {
		t.Fatalf("low = %s, want RUNNING: high must eat reserved pool first, not preempt", low.Status)
	}
	st.View(func(tx *store.Tx) {
		used := tx.Used("r1")
		availLow := tx.AvailableFor("r1", 1)
		availHigh := tx.AvailableFor("r1", 9)
		t.Logf("JUDGE used=%+v avail(pri=1)=%+v avail(pri=9)=%+v", used, availLow, availHigh)
		if used != (domain.Resources{CPU: 4}) {
			t.Fatalf("used = %+v, want {CPU:4}", used)
		}
		if availLow != (domain.Resources{}) {
			t.Fatalf("avail for low-priority = %+v, want zero (reserved off-limits)", availLow)
		}
		if availHigh.AnyNegative() {
			t.Fatalf("avail for high-priority negative: %+v", availHigh)
		}
	})
	// 低优先级在途任务不得出现任何被抢占的痕迹。
	for _, e := range st.Snapshot().Events {
		if e.TaskID == "low" && e.Kind == domain.EventPreempted {
			t.Fatalf("low must not be preempted while reserved pool can serve high")
		}
	}
	t.Log("JUDGE high served from reserved pool; low untouched; mid waits in READY")
}

// TestCommonTaskCannotTouchReserved 普通任务两头都够不到预留区：
// 提交期需求超过公共容量直接被拒；运行期只剩预留容量时只能 READY 等待，
// 预留容量始终保持完整。
func TestCommonTaskCannotTouchReserved(t *testing.T) {
	st, sc := newHarness(t)
	reservedRobot(t, sc, "r1", domain.Resources{CPU: 4}, domain.Resources{CPU: 2}, 5)

	// 公共容量只有 2：需求 3 的普通任务永远排不上，提交期必须明确拒绝。
	big := domain.Task{ID: "too-big-for-common", Priority: 1, Demand: domain.Resources{CPU: 3}}
	t.Logf("INPUT submit low-priority task demand=%+v exceeds common capacity {CPU:2}", big.Demand)
	expectReason(t, sc.Submit(big), domain.ReasonInsufficientCapacity)
	// 同样的需求，达门槛的任务则可以被接纳（可动用预留区）。
	t.Log("INPUT submit same demand with priority=9 (above threshold)")
	mustSubmit(t, sc, domain.Task{ID: "eligible", Priority: 9, Demand: domain.Resources{CPU: 3}})
	// 准入结论已验证；取消 eligible，让后续运行期场景只留普通任务。
	if err := sc.Cancel("eligible"); err != nil {
		t.Fatal(err)
	}

	// low-1 占满公共池；low-2 需求 1 CPU，而空闲容量恰好只剩预留区 2 CPU。
	mustSubmit(t, sc, domain.Task{ID: "low-1", Priority: 1, Demand: domain.Resources{CPU: 2}})
	mustSubmit(t, sc, domain.Task{ID: "low-2", Priority: 1, Demand: domain.Resources{CPU: 1}})
	t.Log("INPUT low-1(pri=1,CPU=2) fills common pool; low-2(pri=1,CPU=1) can only wait; tick x5")
	for i := 0; i < 5; i++ {
		if err := sc.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	logStates(t, st, "after repeated ticks")
	if ts := taskByID(t, st, "low-1"); ts.Status != domain.StatusRunning {
		t.Fatalf("low-1 = %s, want RUNNING", ts.Status)
	}
	if ts := taskByID(t, st, "low-2"); ts.Status != domain.StatusReady {
		t.Fatalf("low-2 = %s, want READY (reserved pool must stay out of reach)", ts.Status)
	}
	st.View(func(tx *store.Tx) {
		used, avail := tx.Used("r1"), tx.Available("r1")
		availLow := tx.AvailableFor("r1", 1)
		t.Logf("JUDGE used=%+v total-avail=%+v (reserved {CPU:2} intact) avail(pri=1)=%+v", used, avail, availLow)
		if used != (domain.Resources{CPU: 2}) {
			t.Fatalf("used = %+v, want {CPU:2} (only common pool consumed)", used)
		}
		if avail != (domain.Resources{CPU: 2}) {
			t.Fatalf("total avail = %+v, want {CPU:2}: reserved capacity must stay intact", avail)
		}
		if availLow != (domain.Resources{}) {
			t.Fatalf("avail for low-priority = %+v, want zero (reserved off-limits)", availLow)
		}
	})
	t.Log("JUDGE oversized common task rejected at admission; reserved capacity never leaked to low-priority tasks")
}

// TestMultiDimReservationNoOvercommit 预留与公共容量按 CPU/Power/Payload 逐维核算：
// 每一维都不得超配，结算后的剩余量任何一维都不能为负。
func TestMultiDimReservationNoOvercommit(t *testing.T) {
	st, sc := newHarness(t)
	cap := domain.Resources{CPU: 4, Power: 4, Payload: 4}
	res := domain.Resources{CPU: 1, Power: 2, Payload: 3}
	// 公共池 = {CPU:3 Power:2 Payload:1}
	reservedRobot(t, sc, "r1", cap, res, 5)

	mustSubmit(t, sc, domain.Task{ID: "la", Priority: 1, Demand: domain.Resources{CPU: 3, Power: 2}})
	mustSubmit(t, sc, domain.Task{ID: "lb", Priority: 1, Demand: domain.Resources{Payload: 1}})
	mustSubmit(t, sc, domain.Task{ID: "lc", Priority: 1, Demand: domain.Resources{CPU: 1}})
	mustSubmit(t, sc, domain.Task{ID: "hi", Priority: 9, Demand: domain.Resources{CPU: 1, Power: 2, Payload: 3}})
	t.Log("INPUT la/lb exactly fill the common pool per-dim; lc needs 1 more common CPU; hi needs exactly the reserved pool")
	if err := sc.Tick(); err != nil {
		t.Fatal(err)
	}
	logStates(t, st, "after multi-dim dispatch")

	if ts := taskByID(t, st, "la"); ts.Status != domain.StatusRunning {
		t.Fatalf("la = %s, want RUNNING", ts.Status)
	}
	if ts := taskByID(t, st, "lb"); ts.Status != domain.StatusRunning {
		t.Fatalf("lb = %s, want RUNNING", ts.Status)
	}
	if ts := taskByID(t, st, "hi"); ts.Status != domain.StatusRunning {
		t.Fatalf("hi = %s, want RUNNING (reserved pool fits it exactly)", ts.Status)
	}
	if ts := taskByID(t, st, "lc"); ts.Status != domain.StatusReady {
		t.Fatalf("lc = %s, want READY (no common CPU left; reserved CPU=1 is off-limits)", ts.Status)
	}
	st.View(func(tx *store.Tx) {
		used := tx.Used("r1")
		avail := tx.Available("r1")
		availLow := tx.AvailableFor("r1", 1)
		t.Logf("JUDGE used=%+v avail=%+v avail(pri=1)=%+v (every dim must be exactly 0, none negative)",
			used, avail, availLow)
		if used != cap {
			t.Fatalf("used = %+v, want exactly capacity %+v (no dim over-committed, none wasted)", used, cap)
		}
		if avail.AnyNegative() || availLow.AnyNegative() {
			t.Fatalf("negative remainder: avail=%+v availLow=%+v", avail, availLow)
		}
		if avail != (domain.Resources{}) || availLow != (domain.Resources{}) {
			t.Fatalf("avail=%+v availLow=%+v, want both zero", avail, availLow)
		}
	})
}

// TestConcurrentDispatchReservedNotBreached 多入口并发 Tick：预留区不被突破、
// 同一任务不被重复派发、任何一维占用不超总容量。
func TestConcurrentDispatchReservedNotBreached(t *testing.T) {
	st, sc := newHarness(t)
	// 总 8 CPU，预留 3：公共池 5。
	reservedRobot(t, sc, "r1", domain.Resources{CPU: 8}, domain.Resources{CPU: 3}, 5)
	for i := 0; i < 5; i++ {
		mustSubmit(t, sc, domain.Task{ID: fmt.Sprintf("low-%d", i), Priority: 1, Demand: domain.Resources{CPU: 1}})
	}
	for i := 0; i < 3; i++ {
		mustSubmit(t, sc, domain.Task{ID: fmt.Sprintf("high-%d", i), Priority: 9, Demand: domain.Resources{CPU: 1}})
	}

	t.Log("INPUT 50 goroutines concurrently calling Tick with 5 common-pool tasks + 3 reserved-eligible tasks")
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = sc.Tick() }()
	}
	wg.Wait()

	snap := st.Snapshot()
	runningLow, runningHigh := 0, 0
	used := domain.Resources{}
	for _, ts := range snap.Tasks {
		if ts.Status != domain.StatusRunning {
			t.Fatalf("task %s = %s, want RUNNING (capacity exactly covers all 8 tasks)", ts.Task.ID, ts.Status)
		}
		used = used.Add(ts.Task.Demand)
		if ts.Task.Priority < 5 {
			runningLow++
		} else {
			runningHigh++
		}
	}
	t.Logf("JUDGE running-low=%d running-high=%d used=%+v (common cap {CPU:5}, total cap {CPU:8})",
		runningLow, runningHigh, used)
	if runningLow != 5 || runningHigh != 3 {
		t.Fatalf("running low/high = %d/%d, want 5/3 (no duplicate dispatch, none dropped)", runningLow, runningHigh)
	}
	if used != (domain.Resources{CPU: 8}) {
		t.Fatalf("used = %+v, want {CPU:8}", used)
	}
	st.View(func(tx *store.Tx) {
		if avail := tx.Available("r1"); avail.AnyNegative() {
			t.Fatalf("available went negative under concurrency: %+v", avail)
		}
	})
}

// TestReservedFallsBackToPreemption 关键任务先吃预留区；预留也不够时才按
// 现有规则抢占，被抢占任务保留为可解释、可恢复的 PREEMPTED。
func TestReservedFallsBackToPreemption(t *testing.T) {
	st, sc := newHarness(t)
	// 总 4 CPU，预留 1，门槛 5：公共池 3。
	reservedRobot(t, sc, "r1", domain.Resources{CPU: 4}, domain.Resources{CPU: 1}, 5)

	mustSubmit(t, sc, domain.Task{ID: "low", Priority: 1, Demand: domain.Resources{CPU: 3}})
	if err := sc.Tick(); err != nil {
		t.Fatal(err)
	}
	t.Log("INPUT low(pri=1,CPU=3) fills the common pool; reserved {CPU:1} free")

	// 第一波：高优先级任务需求恰好等于预留区 —— 必须不打断 low。
	mustSubmit(t, sc, domain.Task{ID: "high-1", Priority: 9, Demand: domain.Resources{CPU: 1}})
	if err := sc.Tick(); err != nil {
		t.Fatal(err)
	}
	if ts := taskByID(t, st, "low"); ts.Status != domain.StatusRunning {
		t.Fatalf("low = %s, want RUNNING: reserved pool must absorb high-1 without preemption", ts.Status)
	}
	if ts := taskByID(t, st, "high-1"); ts.Status != domain.StatusRunning {
		t.Fatalf("high-1 = %s, want RUNNING from reserved pool", ts.Status)
	}
	t.Log("JUDGE high-1 consumed the reserved pool; low never interrupted")

	// 第二波：再来一个高优先级任务，预留区已空 —— 只能抢占 low。
	mustSubmit(t, sc, domain.Task{ID: "high-2", Priority: 9, Demand: domain.Resources{CPU: 3}})
	t.Log("INPUT high-2(pri=9,CPU=3): reserved pool exhausted, preemption is the only way")
	if err := sc.Tick(); err != nil {
		t.Fatal(err)
	}
	logStates(t, st, "after fallback preemption")
	low, high2 := taskByID(t, st, "low"), taskByID(t, st, "high-2")
	if high2.Status != domain.StatusRunning || high2.Assigned != "r1" {
		t.Fatalf("high-2 = %s/%s, want RUNNING/r1", high2.Status, high2.Assigned)
	}
	if low.Status != domain.StatusPreempted || low.Assigned != "" {
		t.Fatalf("low = %s/%q, want PREEMPTED with released assignment", low.Status, low.Assigned)
	}
	t.Logf("JUDGE low preserved explainably: %q", low.Message)

	// 高优先级任务全部完成后，low 自动恢复派发，占用完整准确。
	st.Commit(func(tx *store.Tx) error {
		for _, id := range []string{"high-1", "high-2"} {
			h, _ := tx.Task(id)
			h.Status = domain.StatusSucceeded
			h.Assigned = ""
			h.UpdatedAt = time.Now().UTC()
			tx.PutTask(h)
			tx.Emit(domain.EventSucceeded, id, "r1", "succeeded in test", "")
		}
		return nil
	})
	if err := sc.Tick(); err != nil {
		t.Fatal(err)
	}
	if ts := taskByID(t, st, "low"); ts.Status != domain.StatusRunning || ts.Assigned != "r1" {
		t.Fatalf("low = %s/%s, want re-dispatched RUNNING/r1 after high tasks completed", ts.Status, ts.Assigned)
	}
	st.View(func(tx *store.Tx) {
		used, avail := tx.Used("r1"), tx.Available("r1")
		t.Logf("JUDGE after recovery used=%+v avail=%+v", used, avail)
		if used != (domain.Resources{CPU: 3}) || avail != (domain.Resources{CPU: 1}) {
			t.Fatalf("used=%+v avail=%+v, want {CPU:3}/{CPU:1} (released exactly once, none leaked)", used, avail)
		}
	})
}

// TestRecoveryKeepsReservationConsistent 重启后：预留配置随快照恢复，
// 中断那一刻的占用（RUNNING 任务）继续计入，已结束的任务不再占回来，
// 预留区依旧对普通任务封闭。
func TestRecoveryKeepsReservationConsistent(t *testing.T) {
	path := filepathJoin(t.TempDir(), "state.json")
	st := store.New(path)
	if err := st.Load(); err != nil {
		t.Fatal(err)
	}
	sc := scheduler.New(st)
	res := domain.Resources{CPU: 2, Power: 1}
	reservedRobot(t, sc, "r1", domain.Resources{CPU: 8, Power: 2}, res, 5)

	mustSubmit(t, sc, domain.Task{ID: "done", Priority: 1, Demand: domain.Resources{CPU: 1}})
	mustSubmit(t, sc, domain.Task{ID: "low", Priority: 1, Demand: domain.Resources{CPU: 4}})
	mustSubmit(t, sc, domain.Task{ID: "high", Priority: 9, Demand: domain.Resources{CPU: 2, Power: 1}})
	if err := sc.Tick(); err != nil {
		t.Fatal(err)
	}
	// done 模拟执行完成（终态），low/high 在中断瞬间仍处于 RUNNING。
	st.Commit(func(tx *store.Tx) error {
		d, _ := tx.Task("done")
		d.Status = domain.StatusSucceeded
		d.Assigned = ""
		d.UpdatedAt = time.Now().UTC()
		tx.PutTask(d)
		tx.Emit(domain.EventSucceeded, "done", "r1", "succeeded before crash", "")
		return nil
	})
	t.Log("INPUT crash+restart: low(pri=1,CPU=4) and high(pri=9,CPU=2,Power=1) RUNNING at crash; done SUCCEEDED")

	// 重启：新 Store/Scheduler 从同一快照恢复。
	st2 := store.New(path)
	if err := st2.Load(); err != nil {
		t.Fatal(err)
	}
	sc2 := scheduler.New(st2)
	snap := st2.Snapshot()
	if len(snap.Robots) != 1 || snap.Robots[0].Reserved != res || snap.Robots[0].ReserveMinPriority != 5 {
		t.Fatalf("reservation config not restored: %+v", snap.Robots)
	}
	t.Logf("JUDGE reservation config restored from snapshot: reserved=%+v min-priority=%d",
		snap.Robots[0].Reserved, snap.Robots[0].ReserveMinPriority)
	st2.View(func(tx *store.Tx) {
		used := tx.Used("r1")
		availLow := tx.AvailableFor("r1", 1)
		t.Logf("JUDGE after restart used=%+v (done must not occupy) avail(pri=1)=%+v", used, availLow)
		if used != (domain.Resources{CPU: 6, Power: 1}) {
			t.Fatalf("used = %+v, want {CPU:6 Power:1} (only the two interrupted RUNNING tasks)", used)
		}
		if availLow != (domain.Resources{CPU: 2, Power: 1}) {
			t.Fatalf("avail for low-priority = %+v, want {CPU:2 Power:1} (common pool minus low's usage)", availLow)
		}
	})

	// 普通任务需求超过公共池剩余：重启后依旧派不进预留区。
	mustSubmit(t, sc2, domain.Task{ID: "low-2", Priority: 1, Demand: domain.Resources{CPU: 3}})
	// 达门槛任务可以继续动用剩余预留。
	mustSubmit(t, sc2, domain.Task{ID: "high-2", Priority: 9, Demand: domain.Resources{CPU: 2}})
	if err := sc2.Tick(); err != nil {
		t.Fatal(err)
	}
	if ts := taskByID(t, st2, "low-2"); ts.Status != domain.StatusReady {
		t.Fatalf("low-2 = %s, want READY (reserved pool still closed after restart)", ts.Status)
	}
	if ts := taskByID(t, st2, "high-2"); ts.Status != domain.StatusRunning {
		t.Fatalf("high-2 = %s, want RUNNING (remaining reserved capacity still usable)", ts.Status)
	}
	st2.View(func(tx *store.Tx) {
		used, avail := tx.Used("r1"), tx.Available("r1")
		t.Logf("JUDGE final used=%+v avail=%+v", used, avail)
		if used != (domain.Resources{CPU: 8, Power: 1}) {
			t.Fatalf("used = %+v, want {CPU:8 Power:1}", used)
		}
		if avail.AnyNegative() {
			t.Fatalf("available negative after restart: %+v", avail)
		}
	})
}

// TestInvalidReservationRejected 预留配置本身非法（为负或超过总容量）时
// 注册机器人必须明确失败，绝不允许公共容量被悄悄算成负数。
func TestInvalidReservationRejected(t *testing.T) {
	_, sc := newHarness(t)
	neg := domain.Robot{ID: "r-neg", Capacity: domain.Resources{CPU: 4}, Reserved: domain.Resources{CPU: -1}}
	t.Logf("INPUT AddRobot with negative reserved %+v", neg.Reserved)
	expectReason(t, sc.AddRobot(neg), domain.ReasonInsufficientCapacity)

	over := domain.Robot{ID: "r-over", Capacity: domain.Resources{CPU: 4, Power: 1}, Reserved: domain.Resources{CPU: 2, Power: 2}}
	t.Logf("INPUT AddRobot reserved %+v exceeds capacity %+v", over.Reserved, over.Capacity)
	expectReason(t, sc.AddRobot(over), domain.ReasonInsufficientCapacity)
	t.Log("JUDGE invalid reservation configs rejected explicitly")
}
