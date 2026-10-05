// Package scheduler 负责任务校验、依赖解析、资源分配与优先级抢占。
//
// 所有状态读写都经过 store.Commit 串行化，因此多个调度入口并发
// 调用 Submit / Tick / Cancel 时，同一任务不会被重复派发，
// 资源占用不会被重复计入，可用资源任何时刻都不会出现负值。
package scheduler

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/h3yow/fleet-task-dispatcher-go/internal/domain"
	"github.com/h3yow/fleet-task-dispatcher-go/internal/store"
)

// Scheduler 本地多机器人任务调度器。
type Scheduler struct {
	st *store.Store
}

// New 创建调度器。
func New(st *store.Store) *Scheduler {
	return &Scheduler{st: st}
}

// AddRobot 注册机器人容量并立即持久化。
// 预留配置必须合法：Reserved 任一维不得为负、也不得超过 Capacity 对应维度，
// 否则公共容量会出现负值，属于配置错误，明确拒绝而不是悄悄截断。
func (sc *Scheduler) AddRobot(r domain.Robot) error {
	if r.Reserved.AnyNegative() {
		return domain.NewError(domain.ReasonInsufficientCapacity,
			"robot %s reserved capacity must not be negative: %+v", r.ID, r.Reserved)
	}
	if !r.Capacity.Fits(r.Reserved) {
		return domain.NewError(domain.ReasonInsufficientCapacity,
			"robot %s reserved %+v exceeds capacity %+v", r.ID, r.Reserved, r.Capacity)
	}
	return sc.st.Commit(func(tx *store.Tx) error {
		tx.AddRobot(r)
		tx.Emit(domain.EventSubmitted, "", r.ID,
			fmt.Sprintf("robot registered capacity=%+v reserved=%+v reserve-min-priority=%d",
				r.Capacity, r.Reserved, r.ReserveMinPriority), "")
		return nil
	})
}

// Submit 提交任务并做准入校验，任何拒绝都返回带 FailureReason 的 *domain.TaskError：
//   - DUPLICATE_TASK        任务 ID 已存在（幂等提交也明确拒绝，避免静默覆盖）
//   - CYCLE_DEPENDENCY      依赖图中存在环
//   - TASK_NOT_FOUND        依赖了不存在的任务
//   - INSUFFICIENT_CAPACITY 需求超过任一机器人容量（机器人群无法承载）
func (sc *Scheduler) Submit(t domain.Task) error {
	if t.ID == "" {
		return domain.NewError(domain.ReasonTaskNotFound, "task id must not be empty")
	}
	if t.Demand.AnyNegative() {
		return domain.NewError(domain.ReasonInsufficientCapacity,
			"task %s demand must not be negative: %+v", t.ID, t.Demand)
	}
	return sc.st.Commit(func(tx *store.Tx) error {
		if _, ok := tx.Task(t.ID); ok {
			return domain.NewError(domain.ReasonDuplicateTask, "task %s already submitted", t.ID)
		}
		// 循环依赖检测：把新任务纳入图后做 DFS 三色标记
		//（先于未知依赖检查，使自环能得到 CYCLE_DEPENDENCY 而非 TASK_NOT_FOUND）。
		if cycle := detectCycle(tx, t); cycle != nil {
			return domain.NewError(domain.ReasonCycleDependency,
				"task %s introduces a dependency cycle: %v", t.ID, cycle)
		}
		// 依赖必须指向已提交的任务。
		for _, dep := range t.Dependencies {
			if _, ok := tx.Task(dep); !ok {
				return domain.NewError(domain.ReasonTaskNotFound,
					"task %s depends on unknown task %s", t.ID, dep)
			}
		}
		// 容量校验：需求必须能被至少一个机器人单独承载，否则永远无法调度。
		// 注意按任务优先级取有效容量：未达预留门槛的任务只能用公共容量
		// （Capacity-Reserved），需求超过公共容量的普通任务永远排不上，必须拒绝。
		robots := tx.Robots()
		if len(robots) == 0 {
			return domain.NewError(domain.ReasonInsufficientCapacity,
				"task %s cannot be admitted: no robot registered", t.ID)
		}
		fitsAny := false
		for _, r := range robots {
			if r.EffectiveCapacity(t.Priority).Fits(t.Demand) {
				fitsAny = true
				break
			}
		}
		if !fitsAny {
			return domain.NewError(domain.ReasonInsufficientCapacity,
				"task %s demand %+v exceeds effective capacity of every robot (priority=%d)", t.ID, t.Demand, t.Priority)
		}

		now := time.Now().UTC()
		tx.PutTask(domain.TaskState{
			Task:      t,
			Status:    domain.StatusPending,
			UpdatedAt: now,
		})
		tx.Emit(domain.EventSubmitted, t.ID, "",
			fmt.Sprintf("accepted priority=%d demand=%+v deps=%v", t.Priority, t.Demand, t.Dependencies), "")
		return nil
	})
}

// detectCycle 返回环上的任务 ID 序列；无环返回 nil。
// 它以"假设 t 已入图"的方式检测，覆盖存量任务与新任务之间的环。
func detectCycle(tx *store.Tx, t domain.Task) []string {
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := map[string]int{}
	var stack []string

	var dfs func(id string) []string
	dfs = func(id string) []string {
		color[id] = gray
		stack = append(stack, id)
		var deps []string
		if id == t.ID {
			deps = t.Dependencies
		} else if st, ok := tx.Task(id); ok {
			deps = st.Task.Dependencies
		}
		for _, dep := range deps {
			if color[dep] == gray {
				// 找到回边，截取环路径。
				i := 0
				for stack[i] != dep {
					i++
				}
				return append(append([]string{}, stack[i:]...), dep)
			}
			if color[dep] == white {
				if cyc := dfs(dep); cyc != nil {
					return cyc
				}
			}
		}
		stack = stack[:len(stack)-1]
		color[id] = black
		return nil
	}
	// 从新任务出发即可发现由它引入的环；再补扫所有灰点以防存量图有环。
	if cyc := dfs(t.ID); cyc != nil {
		return cyc
	}
	for _, st := range tx.Tasks() {
		if color[st.Task.ID] == white {
			if cyc := dfs(st.Task.ID); cyc != nil {
				return cyc
			}
		}
	}
	return nil
}

// Tick 执行一轮调度。可被多个入口并发调用，Commit 保证实际串行生效。
// 一轮内做两件事：
//  1. 状态传播：PENDING/PREEMPTED 任务依据依赖结果转为 READY 或 FAILED；
//  2. 资源分配：READY 任务按优先级（高→低，同级按 ID）选择机器人派发，
//     当前容量不足时可在同机器人内抢占严格更低优先级的 RUNNING 任务。
func (sc *Scheduler) Tick() error {
	return sc.st.Commit(func(tx *store.Tx) error {
		sc.failCycles(tx)
		sc.propagate(tx)
		sc.dispatch(tx)
		return nil
	})
}

// failCycles 检测存量任务图中的环（深度防御：例如磁盘快照被外部写入了环），
// 把环上仍未终结的任务明确判为 CYCLE_DEPENDENCY，绝不静默等待。
func (sc *Scheduler) failCycles(tx *store.Tx) {
	ignore := map[string]bool{}
	for {
		cycle := findExistingCycle(tx, ignore)
		if cycle == nil {
			return
		}
		for _, id := range cycle {
			ignore[id] = true
			st, ok := tx.Task(id)
			if !ok || st.Status.IsTerminal() {
				continue
			}
			sc.failLocked(tx, st, domain.ReasonCycleDependency,
				"task participates in a dependency cycle: %v", cycle)
		}
	}
}

// findExistingCycle 在已有任务图中用 DFS 三色标记找一个环；ignore 中的节点不参与检测。
func findExistingCycle(tx *store.Tx, ignore map[string]bool) []string {
	color := map[string]int{}
	var stack []string
	var dfs func(id string) []string
	dfs = func(id string) []string {
		color[id] = 1
		stack = append(stack, id)
		if st, ok := tx.Task(id); ok {
			for _, dep := range st.Task.Dependencies {
				if ignore[dep] {
					continue
				}
				switch color[dep] {
				case 1:
					i := 0
					for stack[i] != dep {
						i++
					}
					return append(append([]string{}, stack[i:]...), dep)
				case 0:
					if cyc := dfs(dep); cyc != nil {
						return cyc
					}
				}
			}
		}
		stack = stack[:len(stack)-1]
		color[id] = 2
		return nil
	}
	for _, st := range tx.Tasks() {
		if ignore[st.Task.ID] || color[st.Task.ID] != 0 {
			continue
		}
		if cyc := dfs(st.Task.ID); cyc != nil {
			return cyc
		}
	}
	return nil
}

// propagate 依据依赖状态推进任务。
func (sc *Scheduler) propagate(tx *store.Tx) {
	// 可能级联传播（依赖在同一轮失败），循环到无变化为止。
	for {
		changed := false
		for _, st := range tx.Tasks() {
			if st.Status != domain.StatusPending && st.Status != domain.StatusPreempted {
				continue
			}
			ready := true
			for _, depID := range st.Task.Dependencies {
				dep, ok := tx.Task(depID)
				if !ok {
					ready = false
					continue
				}
				switch {
				case dep.Status == domain.StatusFailed:
					sc.failLocked(tx, st, domain.ReasonDependencyFailed,
						"dependency %s is FAILED: %s", depID, dep.Message)
					changed = true
					ready = false
				case dep.Status == domain.StatusCanceled:
					sc.failLocked(tx, st, domain.ReasonDependencyFailed,
						"dependency %s is CANCELED", depID)
					changed = true
					ready = false
				case dep.Status != domain.StatusSucceeded:
					ready = false
				}
			}
			if ready {
				sc.toReady(tx, st)
				changed = true
			}
		}
		if !changed {
			return
		}
	}
}

// dispatch 尝试把 READY 任务派发出去。
func (sc *Scheduler) dispatch(tx *store.Tx) {
	ready := readyTasksOrdered(tx)
	robots := tx.Robots()
	for _, st := range ready {
		// 状态可能在本轮被传播改变，重新确认。
		cur, ok := tx.Task(st.Task.ID)
		if !ok || cur.Status != domain.StatusReady {
			continue
		}
		robot, preempted := chooseRobot(tx, robots, cur.Task)
		if robot == "" {
			// 暂时无资源（包括无人可抢占）：保留 READY 等下一轮，不静默失败。
			// 说明中带上该优先级在各机器人上的可用池，便于核对预留是否挡住了它。
			var pools []string
			for _, r := range robots {
				pools = append(pools, fmt.Sprintf("%s:%+v", r.ID, tx.AvailableFor(r.ID, cur.Task.Priority)))
			}
			tx.Emit(domain.EventReady, cur.Task.ID, "",
				fmt.Sprintf("no capacity yet demand=%+v priority=%d pools=[%s], staying READY",
					cur.Task.Demand, cur.Task.Priority, strings.Join(pools, " ")), "")
			continue
		}
		for _, victimID := range preempted {
			sc.preempt(tx, victimID, cur.Task)
		}
		sc.assign(tx, cur, robot)
	}
}

// chooseRobot 为任务选择机器人：优先按该任务优先级对应的容量池做 best-fit
// （达门槛任务可用公共+预留，未达门槛只能用公共池）；放不下时才尝试在某
// 机器人内抢占严格更低优先级的 RUNNING 任务。返回机器人 ID 与被抢占任务 ID。
//
// 与预留的配合：关键任务先吃预留区，只有"预留+公共"都不够时才进入抢占分支，
// 因此预留充足时不会打断任何在途任务。
func chooseRobot(tx *store.Tx, robots []domain.Robot, t domain.Task) (string, []string) {
	// 1) 正常 best-fit：在"该优先级可用池能容纳需求"的机器人中，选剩余量最小者。
	best := ""
	bestScore := -1
	for _, r := range robots {
		avail := tx.AvailableFor(r.ID, t.Priority)
		if !avail.Fits(t.Demand) {
			continue
		}
		score := avail.CPU + avail.Power + avail.Payload
		if best == "" || score < bestScore {
			best, bestScore = r.ID, score
		}
	}
	if best != "" {
		return best, nil
	}
	// 2) 抢占：寻找抢占后能放下、且被抢任务全部严格低优先级的机器人。
	for _, r := range robots {
		// 该优先级在本机的有效容量上限都装不下需求时，抢占也无意义
		// （未达门槛的任务抢低优先级任务最多只能腾回公共池）。
		if !r.EffectiveCapacity(t.Priority).Fits(t.Demand) {
			continue
		}
		// 被抢任务释放的占用会回到新任务可用的池：新任务达门槛时池就是整体容量；
		// 未达门槛时被抢者优先级必然更低（同样未达门槛），其占用本就在公共池内。
		avail := tx.AvailableFor(r.ID, t.Priority)
		var victims []domain.TaskState
		release := domain.Resources{}
		ok := true
		for _, other := range tasksOnRobotOrdered(tx, r.ID) {
			if other.Task.Priority >= t.Priority {
				ok = false // 存在不低于本任务优先级的占用者，不能强行抢
				break
			}
			victims = append(victims, other)
			release = release.Add(other.Task.Demand)
			if avail.Add(release).Fits(t.Demand) {
				break
			}
		}
		if ok && avail.Add(release).Fits(t.Demand) && len(victims) > 0 {
			ids := make([]string, 0, len(victims))
			for _, v := range victims {
				ids = append(ids, v.Task.ID)
			}
			return r.ID, ids
		}
	}
	return "", nil
}

// preempt 将被抢占的任务保留为可解释的 PREEMPTED 状态并回到可重试队列。
func (sc *Scheduler) preempt(tx *store.Tx, victimID string, winner domain.Task) {
	v, ok := tx.Task(victimID)
	if !ok {
		return
	}
	prevAssigned := v.Assigned
	v.Status = domain.StatusPreempted
	v.Assigned = ""
	v.Message = fmt.Sprintf("preempted by higher-priority task %s (pri=%d > pri=%d) on robot %s; preserved for requeue",
		winner.ID, winner.Priority, v.Task.Priority, prevAssigned)
	v.UpdatedAt = time.Now().UTC()
	tx.PutTask(v)
	tx.Emit(domain.EventPreempted, v.Task.ID, prevAssigned,
		fmt.Sprintf("preempted by %s; resources released, preserved PREEMPTED until requeue", winner.ID), "")
	// 任务保留为可解释的 PREEMPTED（已落盘、已释放占用）；
	// 下一轮 Tick 的 propagate 会将其转为 READY 重新竞争，不静默丢失。
}

// Cancel 请求取消任务。终态任务取消是幂等的空操作；运行中的任务取消后
// 立即从占用集合移除（仅 RUNNING 计入 Used），执行器观察到状态后停止执行。
func (sc *Scheduler) Cancel(taskID string) error {
	return sc.st.Commit(func(tx *store.Tx) error {
		st, ok := tx.Task(taskID)
		if !ok {
			return domain.NewError(domain.ReasonTaskNotFound, "task %s not found", taskID)
		}
		if st.Status.IsTerminal() {
			return nil
		}
		robot := st.Assigned
		st.Status = domain.StatusCanceled
		st.Assigned = ""
		st.Message = "canceled by request; all resource holdings released"
		st.UpdatedAt = time.Now().UTC()
		tx.PutTask(st)
		tx.Emit(domain.EventCanceled, taskID, robot, "canceled; resources released", "")
		return nil
	})
}

func (sc *Scheduler) assign(tx *store.Tx, st domain.TaskState, robotID string) {
	st.Status = domain.StatusRunning
	st.Assigned = robotID
	st.Reason = ""
	st.Message = ""
	st.UpdatedAt = time.Now().UTC()
	tx.PutTask(st)
	tx.Emit(domain.EventAssigned, st.Task.ID, robotID,
		fmt.Sprintf("assigned demand=%+v available-after=%+v", st.Task.Demand, tx.Available(robotID)), "")
}

func (sc *Scheduler) toReady(tx *store.Tx, st domain.TaskState) {
	from := st.Status
	st.Status = domain.StatusReady
	st.Assigned = ""
	st.UpdatedAt = time.Now().UTC()
	tx.PutTask(st)
	tx.Emit(domain.EventReady, st.Task.ID, "",
		fmt.Sprintf("dependencies satisfied, %s -> READY", from), "")
}

func (sc *Scheduler) failLocked(tx *store.Tx, st domain.TaskState, reason domain.FailureReason, format string, args ...any) {
	robot := st.Assigned
	st.Status = domain.StatusFailed
	st.Assigned = ""
	st.Reason = reason
	st.Message = fmt.Sprintf(format, args...)
	st.UpdatedAt = time.Now().UTC()
	tx.PutTask(st)
	tx.Emit(domain.EventFailed, st.Task.ID, robot, st.Message, reason)
}

// readyTasksOrdered 返回按优先级降序、同级 ID 升序排列的 READY 任务副本。
func readyTasksOrdered(tx *store.Tx) []domain.TaskState {
	var out []domain.TaskState
	for _, st := range tx.Tasks() {
		if st.Status == domain.StatusReady {
			out = append(out, st)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Task.Priority != out[j].Task.Priority {
			return out[i].Task.Priority > out[j].Task.Priority
		}
		return out[i].Task.ID < out[j].Task.ID
	})
	return out
}

// tasksOnRobotOrdered 返回某机器人上全部 RUNNING 任务，按优先级升序
// （先尝试抢最低优先级的），同级按 ID 升序。
func tasksOnRobotOrdered(tx *store.Tx, robotID string) []domain.TaskState {
	var out []domain.TaskState
	for _, st := range tx.Tasks() {
		if st.Status == domain.StatusRunning && st.Assigned == robotID {
			out = append(out, st)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Task.Priority != out[j].Task.Priority {
			return out[i].Task.Priority < out[j].Task.Priority
		}
		return out[i].Task.ID < out[j].Task.ID
	})
	return out
}
