package scheduler

import (
	"testing"

	"github.com/h3yow/fleet-task-dispatcher-go/internal/domain"
)

func TestTemporaryCapacityShortage_StaysQueuedButExplained(t *testing.T) {
	s, _ := newTestScheduler(t)
	mustAddRobot(t, s, domain.Robot{ID: "r1", Capacity: 5})
	mustSubmit(t, s, domain.Task{ID: "holder", Priority: 5, Demand: 5})
	mustSubmit(t, s, domain.Task{ID: "waiter", Priority: 5, Demand: 5}) // 同优先级，不能抢占

	first, _ := s.DispatchOnce()
	t.Logf("输入: holder(p5,d5) 与 waiter(p5,d5), r1 cap5; 第一轮派发=%v", first)
	if len(first) != 1 || first[0] != "holder" {
		t.Fatalf("仅 holder 应派发, 实际 %v", first)
	}

	second, _ := s.DispatchOnce()
	t.Logf("第二轮（容量满且不可抢占）派发=%v", second)
	if len(second) != 0 {
		t.Fatalf("waiter 必须等待, 实际 %v", second)
	}
	w, _ := s.GetTask("waiter")
	if w.Status != domain.StatusPending {
		t.Fatalf("暂时容量不足不得置为终态 FAILED, 实际 %s/%s", w.Status, w.FailReason)
	}
	// 但等待必须可解释：最近的判定流水中要有针对 waiter 的 REJECT 记录。
	found := false
	for _, d := range s.Decisions() {
		if d.TaskID == "waiter" && d.Action == "REJECT" && d.Reason == domain.ReasonInsufficientCap {
			found = true
		}
	}
	if !found {
		t.Fatalf("等待必须留下可解释的 REJECT 判定记录, decisions=%v", s.Decisions())
	}

	// 容量一旦释放（holder 成功），waiter 立即可派发 —— 证明是“等待”而非“拒绝”。
	if err := s.Complete("holder"); err != nil {
		t.Fatal(err)
	}
	third, _ := s.DispatchOnce()
	t.Logf("holder 完成释放容量后第三轮派发=%v", third)
	if len(third) != 1 || third[0] != "waiter" {
		t.Fatalf("waiter 应在容量释放后派发, 实际 %v", third)
	}
}
