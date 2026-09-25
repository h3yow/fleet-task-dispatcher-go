package executor_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/fleet-task-dispatcher-go/internal/domain"
	"github.com/h3yow/fleet-task-dispatcher-go/internal/executor"
	"github.com/h3yow/fleet-task-dispatcher-go/internal/scheduler"
	"github.com/h3yow/fleet-task-dispatcher-go/internal/store"
)

func testLogger(t *testing.T) *log.Logger {
	t.Helper()
	return log.New(testWriter{t}, "[test] ", log.Ltime|log.Lmicroseconds)
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", p)
	return len(p), nil
}

func newSetup(t *testing.T, path string, run executor.RunFunc) (*store.Store, *scheduler.Scheduler, *executor.Executor) {
	t.Helper()
	st := store.New(path)
	if err := st.Load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	ex := executor.New(st, run, executor.WithPollInterval(5*time.Millisecond), executor.WithLogger(testLogger(t)))
	return st, scheduler.New(st), ex
}

func stateOf(t *testing.T, st *store.Store, id string) domain.TaskState {
	t.Helper()
	var found domain.TaskState
	st.View(func(tx *store.Tx) {
		ts, ok := tx.Task(id)
		if !ok {
			t.Fatalf("task %s missing", id)
		}
		found = ts
	})
	return found
}

func waitStatus(t *testing.T, st *store.Store, id string, want domain.TaskStatus, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if got := stateOf(t, st, id).Status; got == want {
			t.Logf("JUDGE task=%s reached %s", id, want)
			return
		}
		time.Sleep(3 * time.Millisecond)
	}
	got := stateOf(t, st, id)
	t.Fatalf("task %s status=%s reason=%s msg=%q, want %s within %s",
		id, got.Status, got.Reason, got.Message, want, within)
}

// TestSuccessFailureTimeout 三类执行结论必须落为可区分的状态/原因。
func TestSuccessFailureTimeout(t *testing.T) {
	dir := t.TempDir() + "/state.json"
	run := func(ctx context.Context, task domain.Task) error {
		switch task.ID {
		case "ok":
			return nil
		case "boom":
			return errors.New("robot hardware fault")
		case "slow":
			select {
			case <-time.After(500 * time.Millisecond):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	st, sc, ex := newSetup(t, dir, run)
	if err := sc.AddRobot(domain.Robot{ID: "r1", Capacity: domain.Resources{CPU: 6}}); err != nil {
		t.Fatal(err)
	}
	t.Log("INPUT tasks: ok(success), boom(error), slow(timeout 40ms)")
	mustSubmitExec(t, sc, domain.Task{ID: "ok", Demand: domain.Resources{CPU: 2}})
	mustSubmitExec(t, sc, domain.Task{ID: "boom", Demand: domain.Resources{CPU: 2}})
	mustSubmitExec(t, sc, domain.Task{ID: "slow", Demand: domain.Resources{CPU: 2}, Timeout: 40 * time.Millisecond})

	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	go ex.Start(ctx)
	if err := sc.Tick(); err != nil {
		t.Fatal(err)
	}

	waitStatus(t, st, "ok", domain.StatusSucceeded, time.Second)
	waitStatus(t, st, "boom", domain.StatusFailed, time.Second)
	waitStatus(t, st, "slow", domain.StatusFailed, time.Second)

	if ts := stateOf(t, st, "boom"); ts.Reason != domain.ReasonExecutionFailed {
		t.Fatalf("boom reason=%s, want EXECUTION_FAILED", ts.Reason)
	} else {
		t.Logf("JUDGE boom reason=%s msg=%q", ts.Reason, ts.Message)
	}
	if ts := stateOf(t, st, "slow"); ts.Reason != domain.ReasonTimeout {
		t.Fatalf("slow reason=%s, want TIMEOUT", ts.Reason)
	} else {
		t.Logf("JUDGE slow reason=%s msg=%q", ts.Reason, ts.Message)
	}
}

// TestIdempotentDispatch 同一 RUNNING 任务在执行器视角只执行一次。
func TestIdempotentDispatch(t *testing.T) {
	dir := t.TempDir() + "/state.json"
	var started int64
	release := make(chan struct{})
	run := func(ctx context.Context, task domain.Task) error {
		atomic.AddInt64(&started, 1)
		t.Logf("RUN start task=%s (count=%d)", task.ID, atomic.LoadInt64(&started))
		<-release
		return nil
	}
	st, sc, ex := newSetup(t, dir, run)
	if err := sc.AddRobot(domain.Robot{ID: "r1", Capacity: domain.Resources{CPU: 4}}); err != nil {
		t.Fatal(err)
	}
	mustSubmitExec(t, sc, domain.Task{ID: "solo", Demand: domain.Resources{CPU: 1}})

	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	go ex.Start(ctx)
	if err := sc.Tick(); err != nil {
		t.Fatal(err)
	}

	// 等待执行器拾取后，再多跑若干轮 Tick：不能重复派发。
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&started) == 1 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	for i := 0; i < 20; i++ {
		if err := sc.Tick(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(3 * time.Millisecond)
	}
	if n := atomic.LoadInt64(&started); n != 1 {
		t.Fatalf("dispatch count=%d, want exactly 1", n)
	}
	t.Log("JUDGE dispatch count=1 despite repeated ticks")
	close(release)
	waitStatus(t, st, "solo", domain.StatusSucceeded, time.Second)
}

// TestCancelReleasesHoldings 取消运行中任务后执行协程停止、占用完整释放。
func TestCancelReleasesHoldings(t *testing.T) {
	dir := t.TempDir() + "/state.json"
	var entered, canceled int64
	run := func(ctx context.Context, task domain.Task) error {
		atomic.AddInt64(&entered, 1)
		<-ctx.Done()
		atomic.AddInt64(&canceled, 1)
		return ctx.Err()
	}
	st, sc, ex := newSetup(t, dir, run)
	if err := sc.AddRobot(domain.Robot{ID: "r1", Capacity: domain.Resources{CPU: 4}}); err != nil {
		t.Fatal(err)
	}
	mustSubmitExec(t, sc, domain.Task{ID: "victim", Demand: domain.Resources{CPU: 4}})

	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	go ex.Start(ctx)
	if err := sc.Tick(); err != nil {
		t.Fatal(err)
	}
	waitRunning(t, st, "victim")
	waitDispatch(t, &entered, 1) // 确认执行器已实际派发并进入 run 函数

	t.Log("INPUT Cancel(victim) while running")
	if err := sc.Cancel("victim"); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, st, "victim", domain.StatusCanceled, time.Second)

	// 取消后执行协程必须观察到 ctx.Done 并退出，占用同步释放。
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&canceled) == 1 {
			break
		}
		time.Sleep(3 * time.Millisecond)
	}
	if atomic.LoadInt64(&canceled) != 1 {
		t.Fatal("execution goroutine did not observe cancellation")
	}
	st.View(func(tx *store.Tx) {
		used := tx.Used("r1")
		avail := tx.Available("r1")
		t.Logf("JUDGE after cancel used=%+v available=%+v (must fully release, non-negative)", used, avail)
		if !used.IsZero() {
			t.Fatalf("used=%+v, want zero after cancel", used)
		}
		if avail != (domain.Resources{CPU: 4}) {
			t.Fatalf("available=%+v, want full capacity", avail)
		}
	})
}

// TestPreemptionInterruptsExecution 被抢占任务的执行协程被取消，
// 且协程迟到的结果不得把 PREEMPTED/READY 覆盖成 SUCCEEDED。
func TestPreemptionInterruptsExecution(t *testing.T) {
	dir := t.TempDir() + "/state.json"
	run := func(ctx context.Context, task domain.Task) error {
		<-ctx.Done()
		return ctx.Err()
	}
	st, sc, ex := newSetup(t, dir, run)
	if err := sc.AddRobot(domain.Robot{ID: "r1", Capacity: domain.Resources{CPU: 4}}); err != nil {
		t.Fatal(err)
	}
	mustSubmitExec(t, sc, domain.Task{ID: "low", Priority: 1, Demand: domain.Resources{CPU: 4}})

	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	go ex.Start(ctx)
	if err := sc.Tick(); err != nil {
		t.Fatal(err)
	}
	waitRunning(t, st, "low")

	// 低优先级任务执行中，高优先级任务到达并触发抢占。
	mustSubmitExec(t, sc, domain.Task{ID: "high", Priority: 9, Demand: domain.Resources{CPU: 4}})
	t.Log("INPUT tick that preempts low in favor of high")
	if err := sc.Tick(); err != nil {
		t.Fatal(err)
	}

	waitStatus(t, st, "high", domain.StatusRunning, time.Second)
	ts := stateOf(t, st, "low")
	if ts.Status != domain.StatusPreempted && ts.Status != domain.StatusReady {
		t.Fatalf("low status=%s, want PREEMPTED or READY", ts.Status)
	}
	t.Logf("JUDGE low immediately after preemption: %s (%q)", ts.Status, ts.Message)

	// 持续 Tick：low 经历 PREEMPTED->READY，等 high 完成后才能再运行。
	for i := 0; i < 10; i++ {
		if err := sc.Tick(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
		low := stateOf(t, st, "low")
		if low.Status == domain.StatusReady || low.Status == domain.StatusPreempted {
			continue
		}
		if low.Status == domain.StatusRunning {
			t.Fatalf("low must not run while high holds the robot")
		}
	}
}

// TestRecoveryAfterRestart 模拟进程中断重启：从持久化文件继续，
// 终态任务绝不重新派发，中断的 RUNNING 任务重新派发执行。
func TestRecoveryAfterRestart(t *testing.T) {
	dir := t.TempDir() + "/state.json"

	// 第一次进程：done/pending 完成；running 被 gate 卡住并在 RUNNING 时"断电"。
	var entered1 int64
	gate := make(chan struct{})
	run1 := func(ctx context.Context, task domain.Task) error {
		atomic.AddInt64(&entered1, 1)
		if task.ID == "running" {
			select {
			case <-gate:
				return nil
			case <-ctx.Done(): // 进程被杀死
				return ctx.Err()
			}
		}
		return nil
	}
	st1, sc1, ex1 := newSetup(t, dir, run1)
	if err := sc1.AddRobot(domain.Robot{ID: "r1", Capacity: domain.Resources{CPU: 10}}); err != nil {
		t.Fatal(err)
	}
	mustSubmitExec(t, sc1, domain.Task{ID: "done", Demand: domain.Resources{CPU: 2}})
	mustSubmitExec(t, sc1, domain.Task{ID: "running", Demand: domain.Resources{CPU: 2}})
	mustSubmitExec(t, sc1, domain.Task{ID: "pending", Demand: domain.Resources{CPU: 2}, Dependencies: []string{"done"}})

	ctx1, stop1 := context.WithCancel(context.Background())
	go ex1.Start(ctx1)
	if err := sc1.Tick(); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, st1, "done", domain.StatusSucceeded, time.Second)
	waitRunning(t, st1, "running")
	// done 完成后再 tick，使 pending 走完；running 始终被 gate 卡住。
	if err := sc1.Tick(); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, st1, "pending", domain.StatusSucceeded, time.Second)
	waitDispatch(t, &entered1, 2)
	runningBefore := stateOf(t, st1, "running")
	if runningBefore.Status != domain.StatusRunning {
		t.Fatalf("running=%s, want RUNNING at crash point", runningBefore.Status)
	}
	// "断电"：取消执行器上下文（模拟进程中断），状态已逐次原子落盘。
	stop1()
	time.Sleep(30 * time.Millisecond)
	t.Log("INPUT simulated crash while [running] is RUNNING; [done] and [pending] are SUCCEEDED on disk")

	// 第二次进程：重新 Load 同一文件；run2 记录重启后的派发情况。
	var entered2 int64
	run2 := func(ctx context.Context, task domain.Task) error {
		atomic.AddInt64(&entered2, 1)
		t.Logf("RUN(after restart) task=%s", task.ID)
		return nil
	}
	st2, sc2, ex2 := newSetup(t, dir, run2)
	resumed, err := ex2.Recover()
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if fmt.Sprint(resumed) != "[running]" {
		t.Fatalf("resumed=%v, want [running]", resumed)
	}
	t.Logf("JUDGE recovery resumed=%v (terminal done/pending excluded)", resumed)

	ctx2, stop2 := context.WithCancel(context.Background())
	defer stop2()
	go ex2.Start(ctx2)
	if err := sc2.Tick(); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, st2, "running", domain.StatusSucceeded, time.Second)

	// 终态任务在重启后绝不能重新执行：run2 应只被 running 调用一次。
	if got := atomic.LoadInt64(&entered2); got != 1 {
		t.Fatalf("post-restart run invocations=%d, want exactly 1 (only the interrupted RUNNING task)", got)
	}
	for _, id := range []string{"done", "pending"} {
		var successEvents int
		for _, e := range st2.Snapshot().Events {
			if e.TaskID == id && e.Kind == domain.EventSucceeded {
				successEvents++
			}
		}
		if successEvents != 1 {
			t.Fatalf("task %s SUCCEEDED events=%d after restart, want exactly 1 (no redispatch)", id, successEvents)
		}
	}
	t.Log("JUDGE done/pending kept terminal state with single success events; running resumed once and succeeded")
}

// TestConcurrentEntriesConsistent 多入口同时 Tick + 执行器并发工作，
// 结论一致：成功任务数等于提交数，无重复结果事件，可用资源永不为负。
func TestConcurrentEntriesConsistent(t *testing.T) {
	dir := t.TempDir() + "/state.json"
	var runs int64
	run := func(ctx context.Context, task domain.Task) error {
		atomic.AddInt64(&runs, 1)
		select {
		case <-time.After(5 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	st, sc, ex := newSetup(t, dir, run)
	if err := sc.AddRobot(domain.Robot{ID: "r1", Capacity: domain.Resources{CPU: 10}}); err != nil {
		t.Fatal(err)
	}
	const n = 10
	for i := 0; i < n; i++ {
		mustSubmitExec(t, sc, domain.Task{
			ID: fmt.Sprintf("j%d", i), Priority: i % 3, Demand: domain.Resources{CPU: 2},
		})
	}
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	go ex.Start(ctx)

	t.Log("INPUT 8 entries calling Tick concurrently for 200ms")
	var wg sync.WaitGroup
	for e := 0; e < 8; e++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			deadline := time.Now().Add(200 * time.Millisecond)
			for time.Now().Before(deadline) {
				if err := sc.Tick(); err != nil {
					t.Errorf("tick: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		succeeded := 0
		allDone := true
		st.View(func(tx *store.Tx) {
			if tx.Available("r1").AnyNegative() {
				t.Errorf("available went negative")
			}
			for _, ts := range tx.Tasks() {
				if ts.Status == domain.StatusSucceeded {
					succeeded++
				}
				if !ts.Status.IsTerminal() {
					allDone = false
				}
			}
		})
		if allDone && succeeded == n {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	snap := st.Snapshot()
	counts := map[domain.TaskStatus]int{}
	successEvents := map[string]int{}
	for _, ts := range snap.Tasks {
		counts[ts.Status]++
	}
	for _, e := range snap.Events {
		if e.Kind == domain.EventSucceeded {
			successEvents[e.TaskID]++
		}
	}
	t.Logf("JUDGE status counts=%+v total-run-invocations=%d", counts, atomic.LoadInt64(&runs))
	for id, c := range successEvents {
		if c != 1 {
			t.Fatalf("task %s has %d SUCCEEDED events, want 1", id, c)
		}
	}
	if counts[domain.StatusSucceeded] != n {
		t.Fatalf("succeeded=%d, want %d", counts[domain.StatusSucceeded], n)
	}
}

func mustSubmitExec(t *testing.T, sc *scheduler.Scheduler, task domain.Task) {
	t.Helper()
	t.Logf("INPUT Submit task=%q pri=%d deps=%v demand=%+v timeout=%s",
		task.ID, task.Priority, task.Dependencies, task.Demand, task.Timeout)
	if err := sc.Submit(task); err != nil {
		t.Fatalf("submit %s: %v", task.ID, err)
	}
}

func waitRunning(t *testing.T, st *store.Store, id string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if stateOf(t, st, id).Status == domain.StatusRunning {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("task %s did not reach RUNNING", id)
}

// waitDispatch 等待 run 函数被实际进入 n 次（确认执行器已派发）。
func waitDispatch(t *testing.T, counter *int64, n int64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(counter) >= n {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("run function entered %d times, want >= %d", atomic.LoadInt64(counter), n)
}
