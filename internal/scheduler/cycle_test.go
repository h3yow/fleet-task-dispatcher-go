package scheduler

import (
	"errors"
	"testing"

	"github.com/h3yow/fleet-task-dispatcher-go/internal/domain"
)

func TestCycle_SelfDependencyRejectedAtSubmit(t *testing.T) {
	s, _ := newTestScheduler(t)
	err := s.SubmitTask(domain.Task{ID: "x", Demand: 1, Dependencies: []string{"x"}})
	t.Logf("输入: x 依赖自身; 提交错误=%v", err)
	if !errors.Is(err, ErrCycleDependency) {
		t.Fatalf("自依赖必须返回 ErrCycleDependency, 实际 %v", err)
	}
	if _, ok := s.GetTask("x"); ok {
		t.Fatalf("成环任务不得入库")
	}
}

func TestCycle_MultiNodeCycleMarkedAtDispatch(t *testing.T) {
	s, trace := newTestScheduler(t)
	mustAddRobot(t, s, domain.Robot{ID: "r1", Capacity: 10})

	// 输入（允许前向引用，乱序登记）：a->b->c->a 成环；
	// d 依赖 c，因此依赖链可到达环，也必须被拒绝；
	// e 独立无环，应正常派发 —— 用于证明环判定不会误伤无关任务。
	mustSubmit(t, s, domain.Task{ID: "a", Demand: 1, Dependencies: []string{"b"}})
	mustSubmit(t, s, domain.Task{ID: "b", Demand: 1, Dependencies: []string{"c"}})
	mustSubmit(t, s, domain.Task{ID: "c", Demand: 1, Dependencies: []string{"a"}})
	mustSubmit(t, s, domain.Task{ID: "d", Demand: 1, Dependencies: []string{"c"}})
	mustSubmit(t, s, domain.Task{ID: "e", Demand: 1})

	got, err := s.DispatchOnce()
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	t.Logf("输入: 环 a->b->c->a, d 依赖 c, e 独立; 派发结果=%v", got)

	for _, id := range []string{"a", "b", "c", "d"} {
		tk, _ := s.GetTask(id)
		if tk.Status != domain.StatusFailed || tk.FailReason != domain.ReasonCycleDependency {
			t.Fatalf("任务 %s 必须是 FAILED/CYCLE_DEPENDENCY, 实际 %s/%s (%s)",
				id, tk.Status, tk.FailReason, tk.FailDetail)
		}
	}
	e, _ := s.GetTask("e")
	if e.Status != domain.StatusRunning {
		t.Fatalf("无环节点 e 不应被误伤, 实际 %s", e.Status)
	}
	if len(got) != 1 || got[0] != "e" {
		t.Fatalf("环上/依赖环的任务不得派发, 期望仅 [e], 实际 %v", got)
	}
	t.Logf("判定流水:\n%s", trace.String())

	// 再次调度结论必须稳定：环任务永远不会重新派发。
	again, _ := s.DispatchOnce()
	if len(again) != 0 {
		t.Fatalf("环任务终态后不得再次派发, 实际 %v", again)
	}
}

func TestCycle_MissingDependencyJustWaits(t *testing.T) {
	s, _ := newTestScheduler(t)
	mustAddRobot(t, s, domain.Robot{ID: "r1", Capacity: 10})
	// a 依赖一个尚未登记的 z：属于前向引用，应等待而非判环/拒绝。
	mustSubmit(t, s, domain.Task{ID: "a", Demand: 1, Dependencies: []string{"z"}})
	got, _ := s.DispatchOnce()
	t.Logf("输入: a 依赖尚未登记的 z; 派发=%v", got)
	if len(got) != 0 {
		t.Fatalf("缺失依赖时不得派发, 实际 %v", got)
	}
	tk, _ := s.GetTask("a")
	if tk.Status != domain.StatusPending {
		t.Fatalf("缺失依赖应保持 PENDING 等待登记, 实际 %s/%s", tk.Status, tk.FailReason)
	}
	// 登记 z 后：第一轮派发 z；a 必须等 z SUCCEEDED 后才会派发。
	mustSubmit(t, s, domain.Task{ID: "z", Demand: 1})
	got, _ = s.DispatchOnce()
	t.Logf("输入: z 已登记; 第一轮派发=%v (z 先执行)", got)
	if len(got) != 1 || got[0] != "z" {
		t.Fatalf("z 应先派发, a 仍需等待 z 成功, 实际 %v", got)
	}
	if err := s.Complete("z"); err != nil {
		t.Fatalf("complete z: %v", err)
	}
	got, _ = s.DispatchOnce()
	t.Logf("输入: z 已 SUCCEEDED; 第二轮派发=%v", got)
	if len(got) != 1 || got[0] != "a" {
		t.Fatalf("依赖成功后 a 应派发, 实际 %v", got)
	}
}

func TestCycle_DistinctFromOtherFailures(t *testing.T) {
	s, _ := newTestScheduler(t)
	mustAddRobot(t, s, domain.Robot{ID: "r1", Capacity: 2})
	mustSubmit(t, s, domain.Task{ID: "tooBig", Demand: 9})
	err := s.SubmitTask(domain.Task{ID: "loopy", Demand: 1, Dependencies: []string{"loopy"}})
	if !errors.Is(err, ErrCycleDependency) {
		t.Fatalf("自环应返回 ErrCycleDependency, 实际 %v", err)
	}
	_, _ = s.DispatchOnce()

	big, _ := s.GetTask("tooBig")
	t.Logf("判定依据: tooBig -> %s/%s; loopy 提交期拒绝 err=%v", big.Status, big.FailReason, err)
	if big.FailReason != domain.ReasonInsufficientCap {
		t.Fatalf("容量不足原因被污染: %s", big.FailReason)
	}
	if _, ok := s.GetTask("loopy"); ok {
		t.Fatalf("自环任务不得被静默入库后调度")
	}
}
