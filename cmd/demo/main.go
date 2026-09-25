// Command demo 演示本地多机器人任务调度与执行编排流程：
// 任务依赖、资源竞争、优先级抢占、取消释放与可区分的拒绝原因。
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/h3yow/fleet-task-dispatcher-go/internal/domain"
	"github.com/h3yow/fleet-task-dispatcher-go/internal/executor"
	"github.com/h3yow/fleet-task-dispatcher-go/internal/scheduler"
	"github.com/h3yow/fleet-task-dispatcher-go/internal/store"
)

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmsgprefix)
	dir, err := os.MkdirTemp("", "fleet-demo-")
	must(err)
	defer os.RemoveAll(dir)
	dbPath := filepath.Join(dir, "fleet-state.json")

	run := func(ctx context.Context, t domain.Task) error {
		d := 40 * time.Millisecond
		if t.ID == "low-long" {
			d = 400 * time.Millisecond
		}
		select {
		case <-time.After(d):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	st := store.New(dbPath)
	must(st.Load())
	sc := scheduler.New(st)
	ex := executor.New(st, run, executor.WithPollInterval(10*time.Millisecond), executor.WithLogger(logger))
	go ex.Start(ctx)

	must(sc.AddRobot(domain.Robot{ID: "R1", Capacity: domain.Resources{CPU: 4, Power: 2, Payload: 2}}))

	// 1) 拒绝场景：循环依赖与容量不足（原因必须可区分）。
	//    先提交 a/b 形成 a->b，再提交 c 让 b->c，最后让 c->a 成环。
	must(sc.Submit(domain.Task{ID: "a", Priority: 1, Demand: domain.Resources{CPU: 1}}))
	must(sc.Submit(domain.Task{ID: "b", Priority: 1, Demand: domain.Resources{CPU: 1}, Dependencies: []string{"a"}}))
	must(sc.Submit(domain.Task{ID: "c", Priority: 1, Demand: domain.Resources{CPU: 1}, Dependencies: []string{"b"}}))
	err = sc.Submit(domain.Task{ID: "a", Priority: 1, Demand: domain.Resources{CPU: 1}})
	fmt.Printf("submit duplicate -> %v\n", err)
	err = sc.Submit(domain.Task{ID: "selfloop", Priority: 1, Demand: domain.Resources{CPU: 1}, Dependencies: []string{"selfloop"}})
	fmt.Printf("submit cycle     -> %v\n", err)
	err = sc.Submit(domain.Task{ID: "ghost", Priority: 1, Demand: domain.Resources{CPU: 1}, Dependencies: []string{"nope"}})
	fmt.Printf("submit unknown   -> %v\n", err)
	err = sc.Submit(domain.Task{ID: "too-big", Priority: 1, Demand: domain.Resources{CPU: 99}})
	fmt.Printf("submit capacity  -> %v\n", err)

	// 2) 依赖链：a 成功后 b、c 才会 READY。
	must(sc.Tick())
	waitFor(st, "a", domain.StatusRunning, time.Second)
	waitFor(st, "a", domain.StatusSucceeded, time.Second)
	must(sc.Tick()) // b -> READY+RUNNING
	waitFor(st, "b", domain.StatusSucceeded, time.Second)
	must(sc.Tick())
	waitFor(st, "c", domain.StatusSucceeded, time.Second)

	// 3) 抢占：低优先级长任务先占满 R1，高优先级任务到达后抢占。
	must(sc.Submit(domain.Task{ID: "low-long", Priority: 1, Demand: domain.Resources{CPU: 4, Power: 2, Payload: 2}}))
	must(sc.Tick())
	waitFor(st, "low-long", domain.StatusRunning, time.Second)
	fmt.Println("low-long is RUNNING and occupies R1 fully")

	must(sc.Submit(domain.Task{ID: "high-urgent", Priority: 10, Demand: domain.Resources{CPU: 4, Power: 2, Payload: 2}}))
	must(sc.Tick()) // 触发抢占
	time.Sleep(60 * time.Millisecond)
	snap := st.Snapshot()
	for _, t := range snap.Tasks {
		if t.Task.ID == "low-long" {
			fmt.Printf("after preemption: low-long status=%s message=%q\n", t.Status, t.Message)
		}
	}

	// 4) 高优先级完成后持续 tick，被保留的低优先级任务重新派发并完成。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		must(sc.Tick())
		var low domain.TaskState
		for _, t := range st.Snapshot().Tasks {
			if t.Task.ID == "low-long" {
				low = t
			}
		}
		if low.Status == domain.StatusSucceeded {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	stop()
	printSummary(st, dbPath)
}

func waitFor(st *store.Store, id string, want domain.TaskStatus, within time.Duration) {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		var got domain.TaskStatus
		st.View(func(tx *store.Tx) {
			if t, ok := tx.Task(id); ok {
				got = t.Status
			}
		})
		if got == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	log.Fatalf("timeout waiting task %s -> %s", id, want)
}

func printSummary(st *store.Store, path string) {
	snap := st.Snapshot()
	fmt.Println("\n==== final task states ====")
	for _, t := range snap.Tasks {
		fmt.Printf("task=%-11s status=%-9s assigned=%-2s reason=%-22s msg=%s\n",
			t.Task.ID, t.Status, t.Assigned, t.Reason, t.Message)
	}
	fmt.Printf("\npersisted version=%d, events=%d (state file: %s)\n", snap.Version, len(snap.Events), path)
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
