package scheduler

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/h3yow/fleet-task-dispatcher-go/internal/domain"
	"github.com/h3yow/fleet-task-dispatcher-go/internal/storage"
)

func reopenScheduler(t *testing.T, dir string) (*Scheduler, *bytes.Buffer) {
	t.Helper()
	var trace bytes.Buffer
	s, err := New(storage.NewFileStore(filepath.Join(dir, "state.json"), filepath.Join(dir, "decisions.jsonl")),
		WithTraceWriter(&trace))
	if err != nil {
		t.Fatalf("reopen scheduler: %v", err)
	}
	return s, &trace
}

func TestRecovery_AfterCrash(t *testing.T) {
	dir := t.TempDir()

	// ---- 第一次生命周期：登记、派发、部分完成 ----
	s1, err := New(storage.NewFileStore(filepath.Join(dir, "state.json"), filepath.Join(dir, "decisions.jsonl")))
	if err != nil {
		t.Fatal(err)
	}
	mustAddRobot(t, s1, domain.Robot{ID: "r1", Capacity: 5})
	mustAddRobot(t, s1, domain.Robot{ID: "r2", Capacity: 3})
	// done：会在崩溃前 SUCCEEDED；gone：会在崩溃前 CANCELED；
	// live1/live2：崩溃时 RUNNING；late：依赖 live1，崩溃时 PENDING。
	mustSubmit(t, s1, domain.Task{ID: "done", Demand: 2})
	mustSubmit(t, s1, domain.Task{ID: "gone", Demand: 3})
	mustSubmit(t, s1, domain.Task{ID: "live1", Demand: 3})
	mustSubmit(t, s1, domain.Task{ID: "live2", Demand: 2, Dependencies: []string{"done"}})
	mustSubmit(t, s1, domain.Task{ID: "late", Demand: 1, Dependencies: []string{"live1"}})

	dispatched, err := s1.DispatchOnce()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("崩溃前派发=%v", dispatched)
	if err := s1.Complete("done"); err != nil {
		t.Fatal(err)
	}
	if err := s1.Cancel("gone"); err != nil {
		t.Fatal(err)
	}
	// done 完成后再触发一轮，让依赖它的 live2 派发。
	_, _ = s1.DispatchOnce()

	beforeCrash := map[string]domain.TaskStatus{}
	for _, tk := range s1.ListTasks() {
		beforeCrash[tk.ID] = tk.Status
	}
	t.Logf("输入(崩溃瞬间快照): %v; r1/r2 used=%d/%d",
		beforeCrash, usedOf(s1, "r1"), usedOf(s1, "r2"))
	if beforeCrash["live1"] != domain.StatusRunning || beforeCrash["live2"] != domain.StatusRunning {
		t.Fatalf("前置状态错误: live1=%s live2=%s", beforeCrash["live1"], beforeCrash["live2"])
	}

	// ---- 模拟进程中断：丢弃 s1，用新实例重新打开同一路径 ----
	s2, trace2 := reopenScheduler(t, dir)
	t.Cleanup(func() { t.Logf("恢复判定流水:\n%s", trace2.String()) })

	t.Logf("恢复任务数=%d", s2.RecoveredTaskCount())
	if s2.RecoveredTaskCount() != 2 {
		t.Fatalf("live1/live2 共 2 个 RUNNING 应被恢复, 实际 %d", s2.RecoveredTaskCount())
	}

	// 终态任务保持终态，绝不重新派发。
	if tk, _ := s2.GetTask("done"); tk.Status != domain.StatusSucceeded {
		t.Fatalf("已完成任务恢复后必须仍为 SUCCEEDED, 实际 %s", tk.Status)
	}
	if tk, _ := s2.GetTask("gone"); tk.Status != domain.StatusCanceled {
		t.Fatalf("已取消任务恢复后必须仍为 CANCELED, 实际 %s", tk.Status)
	}
	// 崩溃时 RUNNING 的任务转为 PREEMPTED（可解释，不丢失），占用名义释放。
	for _, id := range []string{"live1", "live2"} {
		tk, _ := s2.GetTask(id)
		if tk.Status != domain.StatusPreempted {
			t.Fatalf("%s 应恢复为 PREEMPTED, 实际 %s", id, tk.Status)
		}
		if tk.FailReason != domain.FailReason("RECOVERED_AFTER_RESTART") || tk.AssignedTo != "" {
			t.Fatalf("%s 恢复原因/占用字段错误: %+v", id, tk)
		}
	}
	// 占用按 RUNNING 重算：恢复后没有任何 RUNNING，占用必须归零，不得为负。
	if r, _ := s2.GetRobot("r1"); r.Used != 0 || r.Free() != r.Capacity {
		t.Fatalf("恢复后 r1 占用应归零: %+v", r)
	}
	if r, _ := s2.GetRobot("r2"); r.Used != 0 {
		t.Fatalf("恢复后 r2 占用应归零: %+v", r)
	}
	if problems := s2.CheckInvariants(); len(problems) > 0 {
		t.Fatalf("恢复后不变量被破坏: %v", problems)
	}

	// ---- 恢复后继续执行：PREEMPTED 任务重新派发；late 等 live1 成功后派发 ----
	again, err := s2.DispatchOnce()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("恢复后第一轮派发=%v (PREEMPTED 任务重新进入调度)", again)
	if !contains(again, "live1") || !contains(again, "live2") {
		t.Fatalf("两个 PREEMPTED 任务都应被重新派发, 实际 %v", again)
	}
	if contains(again, "done") || contains(again, "gone") {
		t.Fatalf("终态任务不得重新派发: %v", again)
	}
	if err := s2.Complete("live1"); err != nil {
		t.Fatal(err)
	}
	last, _ := s2.DispatchOnce()
	t.Logf("live1 成功后派发=%v (late 依赖已满足)", last)
	if !contains(last, "late") {
		t.Fatalf("late 应在 live1 成功后派发, 实际 %v", last)
	}
	if err := s2.Complete("live2"); err != nil {
		t.Fatal(err)
	}
	if err := s2.Complete("late"); err != nil {
		t.Fatal(err)
	}

	// ---- 再次重启：结论必须幂等且稳定，不会二次恢复已完成任务 ----
	s3, _ := reopenScheduler(t, dir)
	if s3.RecoveredTaskCount() != 0 {
		t.Fatalf("全部任务终态后重启不应有恢复任务, 实际 %d", s3.RecoveredTaskCount())
	}
	got, _ := s3.DispatchOnce()
	if len(got) != 0 {
		t.Fatalf("全部终态后重启调度必须为空, 实际 %v", got)
	}
	for _, id := range []string{"done", "live1", "live2", "late"} {
		if tk, _ := s3.GetTask(id); tk.Status != domain.StatusSucceeded {
			t.Fatalf("二次重启后 %s 应稳定为 SUCCEEDED, 实际 %s", id, tk.Status)
		}
	}
}

func usedOf(s *Scheduler, id string) int {
	r, _ := s.GetRobot(id)
	return r.Used
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
