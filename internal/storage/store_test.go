package storage

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/fleet-task-dispatcher-go/internal/domain"
)

func TestFileStore_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	st := NewFileStore(filepath.Join(dir, "state.json"), filepath.Join(dir, "decisions.jsonl"))

	want := &domain.State{
		Tasks: map[string]*domain.Task{
			"t1": {ID: "t1", Name: "patrol", Priority: 5, Demand: 2,
				Dependencies: []string{"t0"}, Status: domain.StatusRunning, AssignedTo: "r1",
				CreatedAt: time.Unix(1, 0), Version: 3},
		},
		Robots: map[string]*domain.Robot{
			"r1": {ID: "r1", Name: "rover", Capacity: 4, Used: 2, Version: 2},
		},
	}
	if err := st.Save(want); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := st.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.Tasks["t1"].Demand != 2 || got.Tasks["t1"].AssignedTo != "r1" {
		t.Fatalf("task round-trip mismatch: %+v", got.Tasks["t1"])
	}
	if got.Robots["r1"].Used != 2 || got.Robots["r1"].Free() != 2 {
		t.Fatalf("robot round-trip mismatch: %+v", got.Robots["r1"])
	}

	// 判定日志追加与读取。
	d1 := domain.Decision{Time: time.Unix(2, 0), Action: "DISPATCH", TaskID: "t1", RobotID: "r1"}
	d2 := domain.Decision{Time: time.Unix(3, 0), Action: "COMPLETE", TaskID: "t1", Reason: domain.ReasonNone}
	if err := st.AppendDecision(d1); err != nil {
		t.Fatalf("append d1: %v", err)
	}
	if err := st.AppendDecision(d2); err != nil {
		t.Fatalf("append d2: %v", err)
	}
	ds, err := st.LoadDecisions()
	if err != nil || len(ds) != 2 || ds[1].Action != "COMPLETE" {
		t.Fatalf("decisions round-trip mismatch: %+v err=%v", ds, err)
	}

	// 冷启动：新建同路径 store 应读回相同状态。
	st2 := NewFileStore(filepath.Join(dir, "state.json"), filepath.Join(dir, "decisions.jsonl"))
	got2, err := st2.Load()
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if got2.Tasks["t1"].Status != domain.StatusRunning {
		t.Fatalf("status lost across reopen: %+v", got2.Tasks["t1"])
	}
}

func TestFileStore_LoadMissingReturnsEmpty(t *testing.T) {
	st := NewFileStore(filepath.Join(t.TempDir(), "nope.json"), "")
	got, err := st.Load()
	if err != nil || len(got.Tasks) != 0 || len(got.Robots) != 0 {
		t.Fatalf("expected empty state, got %+v err=%v", got, err)
	}
}

func TestFileStore_ConcurrentSave(t *testing.T) {
	st := NewFileStore("", "") // 内存模式
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s := &domain.State{
				Tasks:  map[string]*domain.Task{"t": {ID: "t", Demand: i, Status: domain.StatusPending}},
				Robots: map[string]*domain.Robot{"r": {ID: "r", Capacity: 100, Used: 0}},
			}
			if err := st.Save(s); err != nil {
				t.Errorf("concurrent save: %v", err)
			}
		}(i)
	}
	wg.Wait()
	got, err := st.Load()
	if err != nil {
		t.Fatalf("load after concurrent saves: %v", err)
	}
	if got.Robots["r"].Used < 0 || got.Robots["r"].Used > got.Robots["r"].Capacity {
		t.Fatalf("capacity invariant violated: %+v", got.Robots["r"])
	}
}
