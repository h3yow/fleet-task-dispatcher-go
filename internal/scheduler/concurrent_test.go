package scheduler

import (
	"fmt"
	"sync"
	"testing"

	"github.com/h3yow/fleet-task-dispatcher-go/internal/domain"
)

func TestConcurrentDispatch_NoDuplicateAssignment(t *testing.T) {
	s, _ := newTestScheduler(t)
	// 输入：3 台机器人（容量各 5），20 个任务各需 2 容量 => 最多同时 6 个在跑，
	// 其余排队。多个调度入口并发竞争。
	for i := 0; i < 3; i++ {
		mustAddRobot(t, s, domain.Robot{ID: fmt.Sprintf("r%d", i), Capacity: 5})
	}
	const taskN = 20
	for i := 0; i < taskN; i++ {
		mustSubmit(t, s, domain.Task{ID: fmt.Sprintf("t%02d", i), Priority: domain.Priority(i % 3), Demand: 2})
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	allDispatched := map[string]int{}
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := 0; k < 5; k++ {
				got, err := s.DispatchOnce()
				if err != nil {
					t.Errorf("dispatch: %v", err)
					return
				}
				mu.Lock()
				for _, id := range got {
					allDispatched[id]++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	t.Logf("输入: 3*cap5 机器人, %d 个 demand=2 任务, 8 个并发调度入口", taskN)

	// 同一任务在同一时刻只能被派发一次（无重复派发计数）。
	for id, n := range allDispatched {
		if n != 1 {
			t.Fatalf("任务 %s 被派发了 %d 次（必须恰好 1 次）", id, n)
		}
	}

	// 容量核算：每个在跑任务恰好落在一台机器人上；任何机器人不得超配/负值。
	running := map[string]string{}
	used := map[string]int{}
	for _, tk := range s.ListTasks() {
		if tk.Status == domain.StatusRunning {
			if other, dup := running[tk.ID]; dup {
				t.Fatalf("任务 %s 同时出现在 %s 与 %s 上", tk.ID, other, tk.AssignedTo)
			}
			running[tk.ID] = tk.AssignedTo
			used[tk.AssignedTo] += tk.Demand
		}
	}
	for _, r := range s.ListRobots() {
		if used[r.ID] != r.Used {
			t.Fatalf("机器人 %s 派发占用合计 %d != 记账 used %d", r.ID, used[r.ID], r.Used)
		}
		if r.Used < 0 || r.Used > r.Capacity {
			t.Fatalf("机器人 %s 可用容量出现非法值 used=%d cap=%d", r.ID, r.Used, r.Capacity)
		}
		t.Logf("机器人 %s: used=%d/%d free=%d, 在跑任务数=%d", r.ID, r.Used, r.Capacity, r.Free(), countRunningOn(s, r.ID))
	}
	if len(running) > 6 { // floor(5/2)*3
		t.Fatalf("容量约束被突破: 同时在跑 %d 个 demand=2 任务", len(running))
	}
	if len(running) != 6 {
		t.Fatalf("三台 cap5 机器应恰好容纳 6 个 demand=2 任务, 实际 %d", len(running))
	}
	if problems := s.CheckInvariants(); len(problems) > 0 {
		t.Fatalf("不变量被破坏: %v", problems)
	}
}

func TestConcurrentDispatchMixedWithCompletion(t *testing.T) {
	s, _ := newTestScheduler(t)
	mustAddRobot(t, s, domain.Robot{ID: "r1", Capacity: 4})
	const n = 12
	for i := 0; i < n; i++ {
		mustSubmit(t, s, domain.Task{ID: fmt.Sprintf("t%02d", i), Demand: 2})
	}

	var wg sync.WaitGroup
	// 调度入口们
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := 0; k < 20; k++ {
				if _, err := s.DispatchOnce(); err != nil {
					t.Errorf("dispatch: %v", err)
				}
			}
		}()
	}
	// 完成入口们：扫描 RUNNING 任务并回传成功，重复回传必须幂等。
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := 0; k < 30; k++ {
				for _, tk := range s.ListTasks() {
					if tk.Status == domain.StatusRunning {
						if err := s.Complete(tk.ID); err != nil {
							t.Errorf("complete %s: %v", tk.ID, err)
						}
						// 故意立刻再报一次，验证幂等。
						if err := s.Complete(tk.ID); err != nil {
							t.Errorf("dup complete %s: %v", tk.ID, err)
						}
					}
				}
			}
		}()
	}
	wg.Wait()

	t.Logf("输入: cap4 机器人, %d 个 demand2 任务, 4 调度 + 4 完成(双重回传) 并发", n)
	succeeded, running, pending, failed := 0, 0, 0, 0
	for _, tk := range s.ListTasks() {
		switch tk.Status {
		case domain.StatusSucceeded:
			succeeded++
		case domain.StatusRunning:
			running++
		case domain.StatusPending, domain.StatusPreempted:
			pending++
		case domain.StatusFailed, domain.StatusCanceled:
			failed++
		}
	}
	t.Logf("结论: succeeded=%d running=%d queued=%d failed/canceled=%d, used=%d/4",
		succeeded, running, pending, failed, s.UsedCapacity())
	if failed != 0 {
		t.Fatalf("纯容量竞争不得产生任何终态失败, 实际 failed/canceled=%d", failed)
	}
	if s.UsedCapacity() < 0 || s.UsedCapacity() > 4 {
		t.Fatalf("总占用越界: %d", s.UsedCapacity())
	}
	if s.UsedCapacity() != running*2 {
		t.Fatalf("在跑任务占用合计 %d 与记账 used=%d 不一致", running*2, s.UsedCapacity())
	}
	if problems := s.CheckInvariants(); len(problems) > 0 {
		t.Fatalf("不变量被破坏: %v", problems)
	}

	// 再补若干轮调度+完成，验证排队任务最终全部排空，结论收敛一致。
	for k := 0; k < 50 && pending+running > 0; k++ {
		if _, err := s.DispatchOnce(); err != nil {
			t.Fatalf("drain dispatch: %v", err)
		}
		for _, tk := range s.ListTasks() {
			if tk.Status == domain.StatusRunning {
				if err := s.Complete(tk.ID); err != nil {
					t.Fatalf("drain complete: %v", err)
				}
			}
		}
	}
	for _, tk := range s.ListTasks() {
		if tk.Status != domain.StatusSucceeded {
			t.Fatalf("补调度后任务 %s 应收敛为 SUCCEEDED, 实际 %s/%s", tk.ID, tk.Status, tk.FailReason)
		}
	}
	if s.UsedCapacity() != 0 {
		t.Fatalf("全部成功后占用必须归零, 实际 %d", s.UsedCapacity())
	}
	t.Logf("补调度后全部 %d 个任务 SUCCEEDED, used=0", n)
}

func TestConcurrentCancelReleasesFully(t *testing.T) {
	s, _ := newTestScheduler(t)
	mustAddRobot(t, s, domain.Robot{ID: "r1", Capacity: 10})
	for i := 0; i < 5; i++ {
		mustSubmit(t, s, domain.Task{ID: fmt.Sprintf("t%d", i), Demand: 2})
	}
	_, _ = s.DispatchOnce()
	if s.UsedCapacity() != 10 {
		t.Fatalf("前置: 应全部派发占满 10, 实际 %d", s.UsedCapacity())
	}
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("t%d", i)
		wg.Add(2)
		go func() { defer wg.Done(); _ = s.Cancel(id) }()
		go func() { defer wg.Done(); _ = s.Cancel(id) }() // 重复取消
	}
	wg.Wait()
	t.Logf("输入: 5 个在跑任务占满 cap10, 各自被两个入口并发 CANCEL; used=%d", s.UsedCapacity())
	if s.UsedCapacity() != 0 {
		t.Fatalf("并发重复取消后占用必须完整归零, 实际 %d", s.UsedCapacity())
	}
	for _, tk := range s.ListTasks() {
		if tk.Status != domain.StatusCanceled || tk.AssignedTo != "" {
			t.Fatalf("任务 %s 取消结论不一致: %+v", tk.ID, tk)
		}
	}
}

func countRunningOn(s *Scheduler, robotID string) int {
	n := 0
	for _, tk := range s.ListTasks() {
		if tk.Status == domain.StatusRunning && tk.AssignedTo == robotID {
			n++
		}
	}
	return n
}
