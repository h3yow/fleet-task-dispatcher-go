package scheduler

import (
	"testing"

	"github.com/h3yow/fleet-task-dispatcher-go/internal/domain"
)

func TestPreempt_HigherPriorityEvictsLower(t *testing.T) {
	s, trace := newTestScheduler(t)
	mustAddRobot(t, s, domain.Robot{ID: "r1", Capacity: 5})

	// 输入：低优先级 low 占满 r1（demand=5, p=1）正在运行。
	mustSubmit(t, s, domain.Task{ID: "low", Priority: 1, Demand: 5})
	_, _ = s.DispatchOnce()

	// 高优先级 high 需求 5：物理上只有 r1 装得下，必须抢占 low。
	mustSubmit(t, s, domain.Task{ID: "high", Priority: 10, Demand: 5})
	got, err := s.DispatchOnce()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("输入: low(p=1,demand=5) 占满 r1; high(p=10,demand=5) 到来; 派发=%v", got)

	h, _ := s.GetTask("high")
	l, _ := s.GetTask("low")
	r, _ := s.GetRobot("r1")
	if h.Status != domain.StatusRunning || h.AssignedTo != "r1" {
		t.Fatalf("high 应抢占成功并在 r1 上运行, 实际 %s/%s", h.Status, h.AssignedTo)
	}
	if l.Status != domain.StatusPreempted {
		t.Fatalf("low 必须被保留为 PREEMPTED（可解释状态）, 实际 %s/%s", l.Status, l.FailReason)
	}
	if l.FailReason != domain.FailReason("PREEMPTED_BY_HIGHER_PRIORITY") || l.AssignedTo != "" {
		t.Fatalf("low 的抢占原因/释放字段错误: %+v", l)
	}
	if r.Used != 5 || r.Free() != 0 {
		t.Fatalf("抢占后占用应等于 high 的 5, 实际 used=%d", r.Used)
	}
	t.Logf("判定流水:\n%s", trace.String())

	// high 完成释放后，low 重新派发，不丢失。
	if err := s.Complete("high"); err != nil {
		t.Fatal(err)
	}
	again, _ := s.DispatchOnce()
	t.Logf("high 完成后第二轮派发=%v (被抢占的 low 重新执行)", again)
	if len(again) != 1 || again[0] != "low" {
		t.Fatalf("PREEMPTED 的 low 必须能重新派发, 实际 %v", again)
	}
	if err := s.Complete("low"); err != nil {
		t.Fatal(err)
	}
	for _, tk := range s.ListTasks() {
		if tk.Status != domain.StatusSucceeded {
			t.Fatalf("最终所有任务都应成功, %s=%s", tk.ID, tk.Status)
		}
	}
}

func TestPreempt_EqualPriorityDoesNotEvict(t *testing.T) {
	s, _ := newTestScheduler(t)
	mustAddRobot(t, s, domain.Robot{ID: "r1", Capacity: 5})
	mustSubmit(t, s, domain.Task{ID: "a", Priority: 5, Demand: 5})
	_, _ = s.DispatchOnce()
	mustSubmit(t, s, domain.Task{ID: "b", Priority: 5, Demand: 5}) // 同优先级
	got, _ := s.DispatchOnce()
	t.Logf("输入: a(p=5) 占满 r1; b(p=5, 同优先级) 到来; 派发=%v", got)
	if len(got) != 0 {
		t.Fatalf("同优先级不得抢占, b 必须排队, 实际 %v", got)
	}
	a, _ := s.GetTask("a")
	b, _ := s.GetTask("b")
	if a.Status != domain.StatusRunning || b.Status != domain.StatusPending {
		t.Fatalf("同优先级时 a 应继续运行 b 应排队, 实际 a=%s b=%s", a.Status, b.Status)
	}
	r, _ := s.GetRobot("r1")
	if r.Used != 5 {
		t.Fatalf("未发生抢占, 占用应保持 5, 实际 %d", r.Used)
	}
}

func TestPreempt_PartialEvictAndMultipleRobots(t *testing.T) {
	s, _ := newTestScheduler(t)
	// 输入：r1 cap6 上跑着两个低优先级任务 l1(demand=3,p=1)、l2(demand=3,p=2)；
	// 高优先级 big(p=10,demand=4) 到来：只需牺牲最弱的 l1(3) 即腾出 3，
	// 3+free0=3 <4，还需要再腾 1 -> l2(3) 也被牺牲；选择牺牲者数最少的机器。
	mustAddRobot(t, s, domain.Robot{ID: "r1", Capacity: 6})
	mustSubmit(t, s, domain.Task{ID: "l1", Priority: 1, Demand: 3})
	mustSubmit(t, s, domain.Task{ID: "l2", Priority: 2, Demand: 3})
	_, _ = s.DispatchOnce()
	r, _ := s.GetRobot("r1")
	if r.Used != 6 {
		t.Fatalf("前置: r1 应被占满, used=%d", r.Used)
	}

	mustSubmit(t, s, domain.Task{ID: "big", Priority: 10, Demand: 4})
	got, _ := s.DispatchOnce()
	t.Logf("输入: l1(p1,d3)+l2(p2,d3) 占满 r1(cap6); big(p10,d4); 派发=%v", got)
	if len(got) != 1 || got[0] != "big" {
		t.Fatalf("big 应在牺牲低优先级任务后派发, 实际 %v", got)
	}
	for _, id := range []string{"l1", "l2"} {
		tk, _ := s.GetTask(id)
		if tk.Status != domain.StatusPreempted {
			t.Fatalf("%s 应被抢占保留为 PREEMPTED, 实际 %s", id, tk.Status)
		}
	}
	r, _ = s.GetRobot("r1")
	if r.Used != 4 || r.Free() != 2 {
		t.Fatalf("抢占后 r1 只应计入 big 的 4, 实际 used=%d free=%d", r.Used, r.Free())
	}
	if problems := s.CheckInvariants(); len(problems) > 0 {
		t.Fatalf("抢占后不变量被破坏: %v", problems)
	}

	// big 完成后，两个 PREEMPTED 任务重新调度执行，最终都成功。
	if err := s.Complete("big"); err != nil {
		t.Fatal(err)
	}
	// 两轮：每轮容量只够一个 demand=3（另一台机器不存在），实际 cap6 一次可放两个。
	again, _ := s.DispatchOnce()
	t.Logf("big 完成后派发=%v", again)
	if len(again) != 2 {
		t.Fatalf("容量释放后 l1/l2 都应重新派发, 实际 %v", again)
	}
	_ = s.Complete("l1")
	_ = s.Complete("l2")
	for _, id := range []string{"l1", "l2", "big"} {
		tk, _ := s.GetTask(id)
		if tk.Status != domain.StatusSucceeded {
			t.Fatalf("%s 最终应 SUCCEEDED, 实际 %s", id, tk.Status)
		}
	}
}

func TestPreempt_NotPossibleWhenOnlyHigherRunning(t *testing.T) {
	s, _ := newTestScheduler(t)
	mustAddRobot(t, s, domain.Robot{ID: "r1", Capacity: 5})
	mustSubmit(t, s, domain.Task{ID: "hi", Priority: 9, Demand: 5})
	_, _ = s.DispatchOnce()
	mustSubmit(t, s, domain.Task{ID: "mid", Priority: 5, Demand: 5})
	got, _ := s.DispatchOnce()
	t.Logf("输入: hi(p9) 占满; mid(p5) 无法抢占更高优先级; 派发=%v", got)
	if len(got) != 0 {
		t.Fatalf("不能抢占更高优先级任务, 实际 %v", got)
	}
	mid, _ := s.GetTask("mid")
	if mid.Status != domain.StatusPending {
		t.Fatalf("mid 应保持排队而非被拒绝为终态, 实际 %s/%s", mid.Status, mid.FailReason)
	}
}
