package scheduler_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/h3yow/fleet-task-dispatcher-go/internal/domain"
	"github.com/h3yow/fleet-task-dispatcher-go/internal/scheduler"
	"github.com/h3yow/fleet-task-dispatcher-go/internal/store"
)

// reservedRobot 注册一台带预留区的机器人：公共区 = Capacity - Reserved，
// 优先级 >= minPri 的任务才能动用预留区。
func reservedRobot(t *testing.T, sc *scheduler.Scheduler, id string, cap, reserved domain.Resources, minPri int) {
	t.Helper()
	r := domain.Robot{ID: id, Capacity: cap, Reserved: reserved, ReserveMinPriority: minPri}
	t.Logf("INPUT AddRobot id=%s capacity=%+v reserved=%+v reserve-min-priority=%d", id, cap, reserved, minPri)
	if err := sc.AddRobot(r); err != nil {
		t.Fatalf("add reserved robot %s: %v", id, err)
	}
}

// taskByID 从快照取任务状态。
func taskByID(snap domain.Snapshot, id string) domain.TaskState {
	for _, ts := range snap.Tasks {
		if ts.Task.ID == id {
			return ts
		}
	}
	return domain.TaskState{}
}

// countEvents 统计快照中某类事件数量。
func countEvents(snap domain.Snapshot, kind domain.EventKind) int {
	n := 0
	for _, e := range snap.Events {
		if e.Kind == kind {
			n++
		}
	}
	return n
}

// assertReservationInvariant 校验每台机器人的预留不变量（可在 View 内调用）：
//  1. 总占用逐维不超过 Capacity（Available 任一维为负会 panic）；
//  2. 未达门槛的 RUNNING 任务占用逐维不超过公共区 Capacity - Reserved。
func assertReservationInvariant(t *testing.T, tx *store.Tx, phase string) {
	t.Helper()
	for _, r := range tx.Robots() {
		avail := tx.Available(r.ID) // 负值会 panic，不静默吞掉
		lowUsed := domain.Resources{}
		for _, ts := range tx.Tasks() {
			if ts.Status == domain.StatusRunning && ts.Assigned == r.ID &&
				ts.Task.Priority < r.ReserveMinPriority {
				lowUsed = lowUsed.Add(ts.Task.Demand)
			}
		}
		pub := r.PublicCapacity()
		t.Logf("JUDGE @%s robot=%s total-used=%+v available=%+v low-priority-used=%+v public=%+v reserved=%+v",
			phase, r.ID, tx.Used(r.ID), avail, lowUsed, pub, r.Reserved)
		if !pub.Fits(lowUsed) {
			t.Fatalf("reservation breached @%s robot=%s: low-priority used %+v exceeds public capacity %+v",
				phase, r.ID, lowUsed, pub)
		}
	}
}

// TestReservedOnlyForEligibleTasks 预留区只对达到门槛的任务开放：
//   - 低优先级任务需求超过公共区（但不超过全容量）→ 准入即拒绝 INSUFFICIENT_CAPACITY；
//   - 同需求的高优先级任务 → 准入通过并直接 RUNNING（吃预留区，不抢占任何人）；
//   - 公共区被占满后，低优先级任务即使全容量还有剩余也只能等 READY。
func TestReservedOnlyForEligibleTasks(t *testing.T) {
	st, sc := newHarness(t)
	// 容量 CPU=8，预留 CPU=3（门槛 pri>=10）→ 公共区 CPU=5。
	reservedRobot(t, sc, "r1", domain.Resources{CPU: 8}, domain.Resources{CPU: 3}, 10)

	// 低优先级任务需求 CPU=6：公共区只有 5，永远放不下 → 准入拒绝。
	lowBig := domain.Task{ID: "low-big", Priority: 1, Demand: domain.Resources{CPU: 6}}
	t.Logf("INPUT Submit low-priority task=%q pri=%d demand=%+v (public capacity is only CPU=5)", lowBig.ID, lowBig.Priority, lowBig.Demand)
	expectReason(t, sc.Submit(lowBig), domain.ReasonInsufficientCapacity)

	// 同需求的高优先级任务：可以用预留区，准入通过。
	mustSubmit(t, sc, domain.Task{ID: "high-big", Priority: 10, Demand: domain.Resources{CPU: 6}})
	if err := sc.Tick(); err != nil {
		t.Fatal(err)
	}
	snap := st.Snapshot()
	if ts := taskByID(snap, "high-big"); ts.Status != domain.StatusRunning || ts.Assigned != "r1" {
		t.Fatalf("high-big = %s/%s, want RUNNING/r1 (reserved zone must serve eligible tasks)", ts.Status, ts.Assigned)
	}
	t.Logf("JUDGE high-big RUNNING on r1 via reserved zone; preempted-events=%d (must be 0)",
		countEvents(snap, domain.EventPreempted))
	if n := countEvents(snap, domain.EventPreempted); n != 0 {
		t.Fatalf("reserved zone should avoid preemption, got %d PREEMPTED events", n)
	}

	// 低优先级任务需求 CPU=2：公共区剩余 5-6<0？不——high-big 吃了 6（公共 5 + 预留 1），
	// 公共区视角已透支，低优先级任务只能 READY 等待，绝不能去碰预留区剩余。
	mustSubmit(t, sc, domain.Task{ID: "low-small", Priority: 1, Demand: domain.Resources{CPU: 2}})
	if err := sc.Tick(); err != nil {
		t.Fatal(err)
	}
	snap = st.Snapshot()
	if ts := taskByID(snap, "low-small"); ts.Status != domain.StatusReady {
		t.Fatalf("low-small = %s, want READY (reserved zone is off-limits for low priority)", ts.Status)
	}
	t.Log("JUDGE low-small stays READY: remaining capacity is reserved, not for pri=1")
	st.View(func(tx *store.Tx) { assertReservationInvariant(t, tx, "reserved-off-limits") })
}

// TestHighPriorityPrefersReservationOverPreemption 关键任务到来时优先吃预留区：
// 公共区被低优先级任务占满时，高优先级任务先动用预留区（无人被抢占）；
// 预留区也不够时才按现有规则抢占，被抢占者保留为可解释的 PREEMPTED。
func TestHighPriorityPrefersReservationOverPreemption(t *testing.T) {
	st, sc := newHarness(t)
	// 容量 CPU=8，预留 CPU=4（门槛 pri>=10）→ 公共区 CPU=4。
	reservedRobot(t, sc, "r1", domain.Resources{CPU: 8}, domain.Resources{CPU: 4}, 10)

	// 低优先级任务占满公共区。
	mustSubmit(t, sc, domain.Task{ID: "low", Priority: 1, Demand: domain.Resources{CPU: 4}})
	if err := sc.Tick(); err != nil {
		t.Fatal(err)
	}
	logStates(t, st, "low fills public zone")

	// 高优先级任务需求 CPU=3：预留区够用 → 直接 RUNNING，low 不被抢占。
	mustSubmit(t, sc, domain.Task{ID: "high-1", Priority: 10, Demand: domain.Resources{CPU: 3}})
	t.Log("INPUT high-priority task arrives; reserved zone CPU=4 can hold it")
	if err := sc.Tick(); err != nil {
		t.Fatal(err)
	}
	snap := st.Snapshot()
	if ts := taskByID(snap, "high-1"); ts.Status != domain.StatusRunning {
		t.Fatalf("high-1 = %s, want RUNNING via reserved zone", ts.Status)
	}
	if ts := taskByID(snap, "low"); ts.Status != domain.StatusRunning {
		t.Fatalf("low = %s, want still RUNNING (reservation must avoid preemption)", ts.Status)
	}
	t.Logf("JUDGE high-1 served by reserved zone, low untouched; preempted-events=%d",
		countEvents(snap, domain.EventPreempted))

	// 再来一个高优先级任务需求 CPU=2：剩余只有 CPU=1，预留区也不够 → 抢占 low。
	mustSubmit(t, sc, domain.Task{ID: "high-2", Priority: 10, Demand: domain.Resources{CPU: 2}})
	t.Log("INPUT second high-priority task; reserved zone exhausted, preemption is the only way")
	if err := sc.Tick(); err != nil {
		t.Fatal(err)
	}
	snap = st.Snapshot()
	if ts := taskByID(snap, "high-2"); ts.Status != domain.StatusRunning {
		t.Fatalf("high-2 = %s, want RUNNING after preempting low", ts.Status)
	}
	low := taskByID(snap, "low")
	if low.Status != domain.StatusPreempted || low.Assigned != "" {
		t.Fatalf("low = %s assigned=%q, want PREEMPTED with released assignment", low.Status, low.Assigned)
	}
	t.Logf("JUDGE low preserved explainably: %q", low.Message)
	st.View(func(tx *store.Tx) { assertReservationInvariant(t, tx, "after-preemption") })
}

// TestMultiDimReservationAccounting 多维容量按维度分别核算，任何一维不超配、
// 结算后剩余量不为负：CPU 维先被公共区占满时，即使 Power/Payload 还有公共余量，
// 低优先级任务也不能借预留区的 CPU 维。
func TestMultiDimReservationAccounting(t *testing.T) {
	st, sc := newHarness(t)
	reservedRobot(t, sc, "r1",
		domain.Resources{CPU: 8, Power: 8, Payload: 8},
		domain.Resources{CPU: 2, Power: 3, Payload: 4}, 10)

	// 低优先级任务：CPU 维正好占满公共区（6），其他维留有余量。
	mustSubmit(t, sc, domain.Task{ID: "low", Priority: 1, Demand: domain.Resources{CPU: 6, Power: 1, Payload: 1}})
	if err := sc.Tick(); err != nil {
		t.Fatal(err)
	}

	// 第二个低优先级任务：CPU 公共区已满，虽然 Power/Payload 公共余量充足，
	// 但 CPU 维不能碰预留区 → 必须 READY 等待。
	mustSubmit(t, sc, domain.Task{ID: "low-2", Priority: 1, Demand: domain.Resources{CPU: 1, Power: 1, Payload: 1}})
	// 高优先级任务：CPU 维可动用预留的 2。
	mustSubmit(t, sc, domain.Task{ID: "high", Priority: 10, Demand: domain.Resources{CPU: 2, Power: 3, Payload: 4}})
	if err := sc.Tick(); err != nil {
		t.Fatal(err)
	}
	snap := st.Snapshot()
	if ts := taskByID(snap, "low-2"); ts.Status != domain.StatusReady {
		t.Fatalf("low-2 = %s, want READY (reserved CPU dimension is off-limits)", ts.Status)
	}
	if ts := taskByID(snap, "high"); ts.Status != domain.StatusRunning {
		t.Fatalf("high = %s, want RUNNING (eligible for reserved dimensions)", ts.Status)
	}
	st.View(func(tx *store.Tx) {
		assertReservationInvariant(t, tx, "multi-dim")
		avail := tx.Available("r1")
		want := domain.Resources{CPU: 0, Power: 4, Payload: 3}
		t.Logf("JUDGE per-dimension remainder=%+v want=%+v (no dimension negative, none silently consumed)", avail, want)
		if avail != want {
			t.Fatalf("available = %+v, want %+v", avail, want)
		}
	})
}

// TestConcurrentReservationNotBreached 多个调度入口并发 Submit/Tick/Cancel 时，
// 预留区不被突破、总占用不超容量、同一任务不被重复计入。
func TestConcurrentReservationNotBreached(t *testing.T) {
	st, sc := newHarness(t)
	reservedRobot(t, sc, "r1", domain.Resources{CPU: 8, Power: 8}, domain.Resources{CPU: 3, Power: 3}, 10)
	reservedRobot(t, sc, "r2", domain.Resources{CPU: 8, Power: 8}, domain.Resources{CPU: 3, Power: 3}, 10)

	const n = 40
	var wg sync.WaitGroup
	t.Logf("INPUT %d goroutines concurrently Submit (half pri=1 half pri=10) + 8 goroutines Tick", n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			pri := 1
			if i%2 == 0 {
				pri = 10
			}
			_ = sc.Submit(domain.Task{
				ID:       fmt.Sprintf("t-%02d", i),
				Priority: pri,
				Demand:   domain.Resources{CPU: 2, Power: 2},
			})
		}(i)
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				if err := sc.Tick(); err != nil {
					t.Errorf("tick: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if err := sc.Tick(); err != nil {
		t.Fatal(err)
	}

	snap := st.Snapshot()
	running := map[string]int{} // robot -> running count，检测重复派发
	for _, ts := range snap.Tasks {
		if ts.Status == domain.StatusRunning {
			running[ts.Assigned]++
		}
	}
	t.Logf("JUDGE after storm: running-per-robot=%v total-tasks=%d", running, len(snap.Tasks))
	st.View(func(tx *store.Tx) { assertReservationInvariant(t, tx, "concurrent-storm") })
}

// TestReservationRecoveryConsistency 重启后预留配置与中断时的占用对得上：
// 快照重载后机器人预留配置不变、RUNNING 占用原样保留、终态任务不占资源，
// 后续调度依旧受预留规则约束。
func TestReservationRecoveryConsistency(t *testing.T) {
	path := filepathJoin(t.TempDir(), "state.json")
	st := store.New(path)
	if err := st.Load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	sc := scheduler.New(st)
	reservedRobot(t, sc, "r1", domain.Resources{CPU: 8}, domain.Resources{CPU: 3}, 10)
	// 低优先级任务先占满公共区（CPU=5），随后高优先级任务动用预留区（CPU=3）。
	mustSubmit(t, sc, domain.Task{ID: "low", Priority: 1, Demand: domain.Resources{CPU: 5}})
	if err := sc.Tick(); err != nil {
		t.Fatal(err)
	}
	mustSubmit(t, sc, domain.Task{ID: "high", Priority: 10, Demand: domain.Resources{CPU: 3}})
	if err := sc.Tick(); err != nil {
		t.Fatal(err)
	}
	if ts := taskByID(st.Snapshot(), "low"); ts.Status != domain.StatusRunning {
		t.Fatalf("low = %s, want RUNNING before crash", ts.Status)
	}
	if ts := taskByID(st.Snapshot(), "high"); ts.Status != domain.StatusRunning {
		t.Fatalf("high = %s, want RUNNING via reserved zone before crash", ts.Status)
	}
	// high 完成（终态），low 仍在 RUNNING（模拟中断瞬间）。
	st.Commit(func(tx *store.Tx) error {
		h, _ := tx.Task("high")
		h.Status = domain.StatusSucceeded
		h.Assigned = ""
		tx.PutTask(h)
		tx.Emit(domain.EventSucceeded, "high", "r1", "succeeded before crash", "")
		return nil
	})
	before := st.Snapshot()
	t.Logf("INPUT crash & restart at snapshot v%d (low RUNNING, high SUCCEEDED)", before.Version)

	// 模拟重启：同一文件新建 Store 并加载。
	st2 := store.New(path)
	if err := st2.Load(); err != nil {
		t.Fatalf("reload: %v", err)
	}
	after := st2.Snapshot()

	var robot domain.Robot
	for _, r := range after.Robots {
		if r.ID == "r1" {
			robot = r
		}
	}
	if robot.Reserved != (domain.Resources{CPU: 3}) || robot.ReserveMinPriority != 10 {
		t.Fatalf("reserved config lost after restart: %+v min-pri=%d", robot.Reserved, robot.ReserveMinPriority)
	}
	t.Logf("JUDGE reserved config survives restart: reserved=%+v min-priority=%d", robot.Reserved, robot.ReserveMinPriority)

	st2.View(func(tx *store.Tx) {
		used := tx.Used("r1")
		t.Logf("JUDGE used-after-restart=%+v (only RUNNING low counts; SUCCEEDED high must not occupy)", used)
		if used != (domain.Resources{CPU: 5}) {
			t.Fatalf("used after restart = %+v, want {CPU:5} (terminal tasks must not occupy)", used)
		}
		assertReservationInvariant(t, tx, "after-restart")
	})

	// 重启后继续调度：低优先级新任务仍不能碰预留区。
	sc2 := scheduler.New(st2)
	mustSubmit(t, sc2, domain.Task{ID: "low-2", Priority: 1, Demand: domain.Resources{CPU: 1}})
	if err := sc2.Tick(); err != nil {
		t.Fatal(err)
	}
	if ts := taskByID(st2.Snapshot(), "low-2"); ts.Status != domain.StatusReady {
		t.Fatalf("low-2 = %s after restart, want READY (public zone exhausted, reserved off-limits)", ts.Status)
	}
	t.Log("JUDGE reservation rules still enforced after restart")
}
