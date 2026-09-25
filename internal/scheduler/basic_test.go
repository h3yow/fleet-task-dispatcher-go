package scheduler

import (
	"bytes"
	"strings"
	"testing"

	"github.com/h3yow/fleet-task-dispatcher-go/internal/domain"
	"github.com/h3yow/fleet-task-dispatcher-go/internal/storage"
)

// newTestScheduler 构造内存调度器并把判定流水同时打印到测试日志。
func newTestScheduler(t *testing.T) (*Scheduler, *bytes.Buffer) {
	t.Helper()
	var trace bytes.Buffer
	s, err := New(storage.NewFileStore("", ""), WithTraceWriter(&trace))
	if err != nil {
		t.Fatalf("new scheduler: %v", err)
	}
	t.Cleanup(func() {
		t.Logf("判定流水:\n%s", trace.String())
	})
	return s, &trace
}

func TestDispatch_RespectsDependenciesAndCapacity(t *testing.T) {
	s, _ := newTestScheduler(t)
	mustAddRobot(t, s, domain.Robot{ID: "r1", Name: "rover", Capacity: 5})

	// 输入：t2 依赖 t1；t1 demand=2，t2 demand=4（r1 容量 5）。
	mustSubmit(t, s, domain.Task{ID: "t1", Name: "unload", Priority: 1, Demand: 2})
	mustSubmit(t, s, domain.Task{ID: "t2", Name: "haul", Priority: 1, Demand: 4, Dependencies: []string{"t1"}})

	got, err := s.DispatchOnce()
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	t.Logf("输入: tasks=t1(demand=2),t2(demand=4,deps=[t1]); robot=r1(cap=5); 第一轮派发=%v", got)
	if len(got) != 1 || got[0] != "t1" {
		t.Fatalf("依赖未完成的 t2 不得派发, 期望仅派发 [t1], 实际 %v", got)
	}
	if tk, _ := s.GetTask("t2"); tk.Status != domain.StatusPending {
		t.Fatalf("t2 应保持 PENDING 等待依赖, 实际 %s", tk.Status)
	}

	// t1 完成释放 2 单位后，t2 应可派发（4 <= 5）。
	if err := s.Complete("t1"); err != nil {
		t.Fatalf("complete t1: %v", err)
	}
	got, _ = s.DispatchOnce()
	t.Logf("输入: t1 已 SUCCEEDED 并释放占用; 第二轮派发=%v", got)
	if len(got) != 1 || got[0] != "t2" {
		t.Fatalf("依赖满足后 t2 应派发, 实际 %v", got)
	}
	if r, _ := s.GetRobot("r1"); r.Used != 4 || r.Free() != 1 {
		t.Fatalf("占用核算错误: %+v", r)
	}
}

func TestDispatch_InsufficientCapacityRejected(t *testing.T) {
	s, _ := newTestScheduler(t)
	mustAddRobot(t, s, domain.Robot{ID: "r1", Capacity: 3})
	mustSubmit(t, s, domain.Task{ID: "big", Priority: 1, Demand: 5})

	got, _ := s.DispatchOnce()
	t.Logf("输入: task=big(demand=5), robot=r1(cap=3); 派发结果=%v", got)
	if len(got) != 0 {
		t.Fatalf("物理容量不足不应派发, 实际 %v", got)
	}
	tk, _ := s.GetTask("big")
	if tk.Status != domain.StatusFailed || tk.FailReason != domain.ReasonInsufficientCap {
		t.Fatalf("期望 FAILED/INSUFFICIENT_CAPACITY, 实际 %s/%s (%s)", tk.Status, tk.FailReason, tk.FailDetail)
	}
}

func TestCancelAndTimeout_ReleaseCapacity(t *testing.T) {
	s, _ := newTestScheduler(t)
	mustAddRobot(t, s, domain.Robot{ID: "r1", Capacity: 5})
	mustSubmit(t, s, domain.Task{ID: "t1", Priority: 1, Demand: 5})
	if _, err := s.DispatchOnce(); err != nil {
		t.Fatal(err)
	}
	if err := s.Cancel("t1"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	r, _ := s.GetRobot("r1")
	t.Logf("输入: t1(demand=5) RUNNING 后 CANCEL; r1.used=%d", r.Used)
	if r.Used != 0 {
		t.Fatalf("取消后占用必须完整释放, 实际 used=%d", r.Used)
	}
	if tk, _ := s.GetTask("t1"); tk.Status != domain.StatusCanceled || tk.AssignedTo != "" {
		t.Fatalf("取消后任务状态/占用字段错误: %+v", tk)
	}
	// 已取消任务再调度也不会被派发。
	got, _ := s.DispatchOnce()
	if len(got) != 0 {
		t.Fatalf("CANCELED 任务不得重新派发, 实际 %v", got)
	}

	// 超时路径。
	mustSubmit(t, s, domain.Task{ID: "t2", Priority: 1, Demand: 3})
	_, _ = s.DispatchOnce()
	if err := s.MarkTimeout("t2"); err != nil {
		t.Fatalf("timeout: %v", err)
	}
	r, _ = s.GetRobot("r1")
	if r.Used != 0 {
		t.Fatalf("超时后占用必须归零, 实际 %d", r.Used)
	}
	tk, _ := s.GetTask("t2")
	if tk.FailReason != domain.FailReason("TIMEOUT") {
		t.Fatalf("超时原因不可区分: %s", tk.FailReason)
	}
}

func TestCompletion_IdempotentAndNonNegative(t *testing.T) {
	s, trace := newTestScheduler(t)
	mustAddRobot(t, s, domain.Robot{ID: "r1", Capacity: 5})
	mustSubmit(t, s, domain.Task{ID: "t1", Priority: 1, Demand: 2})
	_, _ = s.DispatchOnce()

	if err := s.Complete("t1"); err != nil {
		t.Fatalf("first complete: %v", err)
	}
	// 重复回传成功/失败/取消都必须是幂等 no-op，不得重复释放。
	if err := s.Complete("t1"); err != nil {
		t.Fatalf("duplicate complete should be no-op, got %v", err)
	}
	if err := s.Fail("t1", domain.FailReason("RUNTIME_ERROR"), "late report"); err != nil {
		t.Fatalf("late fail should be no-op, got %v", err)
	}
	if err := s.Cancel("t1"); err != nil {
		t.Fatalf("late cancel should be no-op, got %v", err)
	}
	r, _ := s.GetRobot("r1")
	t.Logf("输入: 成功后重复回传 COMPLETE/FAIL/CANCEL; r1.used=%d free=%d", r.Used, r.Free())
	if r.Used != 0 {
		t.Fatalf("重复回传导致重复释放或占用异常: used=%d", r.Used)
	}
	tk, _ := s.GetTask("t1")
	if tk.Status != domain.StatusSucceeded {
		t.Fatalf("终态结论不得被后续重复回传改变: %s", tk.Status)
	}
	if !strings.Contains(trace.String(), "IDEMPOTENT_SKIP") {
		t.Fatalf("判定依据中缺少幂等跳过记录")
	}
}

func TestDependencyFailure_Propagates(t *testing.T) {
	s, _ := newTestScheduler(t)
	mustAddRobot(t, s, domain.Robot{ID: "r1", Capacity: 10})
	mustSubmit(t, s, domain.Task{ID: "a", Priority: 1, Demand: 8})
	mustSubmit(t, s, domain.Task{ID: "b", Priority: 1, Demand: 1, Dependencies: []string{"a"}})
	_, _ = s.DispatchOnce()
	if err := s.Fail("a", domain.FailReason("RUNTIME_ERROR"), "actuator broken"); err != nil {
		t.Fatal(err)
	}
	got, _ := s.DispatchOnce()
	t.Logf("输入: a RUNNING 后 FAIL, b 依赖 a; 本轮派发=%v", got)
	tb, _ := s.GetTask("b")
	if tb.Status != domain.StatusFailed || tb.FailReason != domain.ReasonDependencyFailed {
		t.Fatalf("依赖失败必须以 DEPENDENCY_FAILED 明确拒绝 b, 实际 %s/%s", tb.Status, tb.FailReason)
	}
}

func mustAddRobot(t *testing.T, s *Scheduler, r domain.Robot) {
	t.Helper()
	if err := s.AddRobot(r); err != nil {
		t.Fatalf("add robot %s: %v", r.ID, err)
	}
}

func mustSubmit(t *testing.T, s *Scheduler, task domain.Task) {
	t.Helper()
	if err := s.SubmitTask(task); err != nil {
		t.Fatalf("submit task %s: %v", task.ID, err)
	}
}
