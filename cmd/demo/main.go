// Command demo 在本地用文件存储演示多机器人任务调度：
//
//	go run ./cmd/demo
//
// 它会在 ./data 下生成 state.json（全量快照）与 decisions.jsonl（判定流水）。
// 删除 ./data 后再跑即为全新启动；保留 ./data 再跑即可观察中断恢复：
// 崩溃前 RUNNING 的任务会在启动时转为 PREEMPTED 并在随后重新派发。
package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/h3yow/fleet-task-dispatcher-go/internal/domain"
	"github.com/h3yow/fleet-task-dispatcher-go/internal/scheduler"
	"github.com/h3yow/fleet-task-dispatcher-go/internal/storage"
)

func main() {
	dir := filepath.Join(".", "data")
	store := storage.NewFileStore(filepath.Join(dir, "state.json"), filepath.Join(dir, "decisions.jsonl"))

	s, err := scheduler.New(store, scheduler.WithTraceWriter(os.Stdout))
	must(err)

	fmt.Printf("== 启动完成，本次从中断 RUNNING 态恢复的任务数=%d ==\n", s.RecoveredTaskCount())

	// 仅在全新集群时播种（重启后不重复登记）。
	if len(s.ListRobots()) == 0 {
		fmt.Println("== 播种集群：r1 cap=5, r2 cap=3 ==")
		must(s.AddRobot(domain.Robot{ID: "r1", Name: "rover-1", Capacity: 5}))
		must(s.AddRobot(domain.Robot{ID: "r2", Name: "rover-2", Capacity: 3}))

		fmt.Println("== 播种：scan(r2) 与依赖它的 haul(占满 r1)，均为低优先级 ==")
		must(s.SubmitTask(domain.Task{ID: "scan", Name: "scan aisle", Priority: 2, Demand: 3}))
		must(s.SubmitTask(domain.Task{ID: "haul", Name: "haul pallet", Priority: 2, Demand: 5, Dependencies: []string{"scan"}}))
	}

	// 全新启动：先跑第一轮让 scan 在 r2 上执行。
	if t, ok := s.GetTask("scan"); ok && t.Status == domain.StatusPending {
		fmt.Println("\n== 第一轮调度：scan 在 r2 上执行 ==")
		d0, err := s.DispatchOnce()
		must(err)
		fmt.Printf("派发: %v\n", d0)
	}

	// 完成 scan，让 haul 具备派发条件。
	if t, ok := s.GetTask("scan"); ok && t.Status == domain.StatusRunning {
		fmt.Println("\n== scan 在 r2 执行完成，释放 r2 ==")
		must(s.Complete("scan"))
	}

	fmt.Println("\n== 调度：低优先级 haul 派发并占满 r1 ==")
	d1, err := s.DispatchOnce()
	must(err)
	fmt.Printf("派发: %v\n", d1)
	printCapacity(s)

	// 高优先级紧急任务到来。
	if t, ok := s.GetTask("patrol"); !ok || t.Status == domain.StatusPending || t.Status == domain.StatusPreempted {
		if _, exists := s.GetTask("patrol"); !exists {
			fmt.Println("\n== 紧急任务 patrol(p=10, demand=5) 到来 ==")
			must(s.SubmitTask(domain.Task{ID: "patrol", Name: "emergency patrol", Priority: 10, Demand: 5}))
		}
		fmt.Println("== 调度：patrol 抢占低优先级 haul（haul 保留为 PREEMPTED，不丢失） ==")
		d2, err := s.DispatchOnce()
		must(err)
		fmt.Printf("派发: %v\n", d2)
		printStatus(s)
	}

	fmt.Println("\n== patrol 执行完成，释放 r1 ==")
	if t, ok := s.GetTask("patrol"); ok && t.Status == domain.StatusRunning {
		must(s.Complete("patrol"))
	}

	fmt.Println("== 再次调度：被抢占的 haul 重新派发 ==")
	d3, err := s.DispatchOnce()
	must(err)
	fmt.Printf("派发: %v\n", d3)
	printStatus(s)

	fmt.Printf("\n不变量检查: %v（空=全部满足）\n", s.CheckInvariants())
	fmt.Println("提示：若在任务 RUNNING 时中断进程（直接删状态中 RUNNING 亦可模拟），")
	fmt.Println("      再次运行本程序可观察 RUNNING -> PREEMPTED 的恢复；删除 ./data 可重置。")
}

func printCapacity(s *scheduler.Scheduler) {
	for _, r := range s.ListRobots() {
		fmt.Printf("  %s used=%d/%d free=%d\n", r.ID, r.Used, r.Capacity, r.Free())
	}
}

func printStatus(s *scheduler.Scheduler) {
	fmt.Println("-- 任务状态 --")
	for _, t := range s.ListTasks() {
		extra := ""
		if t.FailReason != "" {
			extra = fmt.Sprintf(" reason=%s detail=%q", t.FailReason, t.FailDetail)
		}
		if t.AssignedTo != "" {
			extra += " on=" + t.AssignedTo
		}
		fmt.Printf("  %-8s p=%d demand=%d deps=%-8s %s%s\n",
			t.ID, t.Priority, t.Demand, fmt.Sprint(t.Dependencies), t.Status, extra)
	}
	printCapacity(s)
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}
