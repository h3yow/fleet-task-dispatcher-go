package scheduler

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/h3yow/fleet-task-dispatcher-go/internal/domain"
	"github.com/h3yow/fleet-task-dispatcher-go/internal/storage"
)

// ErrCycleDependency 在提交任务时如果发现依赖图成环则返回。
var ErrCycleDependency = errors.New("cyclic dependency detected")

// Scheduler 是本地多机器人任务调度器。
// 所有公开方法均以单一互斥锁串行化“判定 + 状态迁移 + 持久化”，
// 因而并发入口下不会重复派发、不会重复计入占用；每次状态变更在锁内同步落盘，
// 进程中断重启后可从快照继续，已完成/已取消任务不会被重新派发。
type Scheduler struct {
	mu        sync.Mutex
	store     storage.Store
	state     *domain.State
	decisions []domain.Decision
	trace     io.Writer
	now       func() time.Time

	recovered int // New 时从中断态恢复（RUNNING -> PREEMPTED）的任务数
}

// Option 用于装配调度器可选依赖。
type Option func(*Scheduler)

// WithClock 注入时钟（测试用）。
func WithClock(c func() time.Time) Option {
	return func(s *Scheduler) { s.now = c }
}

// WithTraceWriter 接收人类可读的判定流水（输入与判定依据），默认丢弃。
func WithTraceWriter(w io.Writer) Option {
	return func(s *Scheduler) { s.trace = w }
}

// New 创建调度器并加载持久化状态。
// 重启恢复规则：
//   - 以 RUNNING 任务为唯一依据重算每台机器人的 Used（杜绝占用凭空多计/少计）；
//   - 快照里 RUNNING 但无法继续的任务（进程已中断，执行现场丢失）一律转为
//     PREEMPTED 并注明 RECOVERED_AFTER_RESTART，等待重新调度，不会被静默丢失；
//   - 已处终态（SUCCEEDED/FAILED/CANCELED）的任务保持终态，绝不重新派发。
func New(store storage.Store, opts ...Option) (*Scheduler, error) {
	st, err := store.Load()
	if err != nil {
		return nil, err
	}
	ds, err := store.LoadDecisions()
	if err != nil {
		return nil, err
	}
	s := &Scheduler{store: store, state: st, decisions: ds, now: time.Now, trace: io.Discard}
	for _, o := range opts {
		o(s)
	}
	s.recoverLocked()
	return s, nil
}

// ---- 集群配置 ----

// AddRobot 登记一台机器人；容量为非负整数，ID 不可重复。
func (s *Scheduler) AddRobot(r domain.Robot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if r.ID == "" {
		return fmt.Errorf("%w: robot id is empty", ErrInvalidArgument)
	}
	if r.Capacity < 0 {
		return fmt.Errorf("%w: robot %q capacity must be >= 0", ErrInvalidArgument, r.ID)
	}
	if _, ok := s.state.Robots[r.ID]; ok {
		return fmt.Errorf("%w: robot %q", ErrDuplicateID, r.ID)
	}
	now := s.now()
	r.Used = 0
	r.UpdatedAt = now
	r.Version = 1
	s.state.Robots[r.ID] = &r
	return s.persistLocked()
}

// ---- 任务入口 ----

// SubmitTask 登记新任务。校验规则：
//   - demand 必须为正；
//   - 自依赖立即返回 ErrCycleDependency；
//   - 允许“前向引用”尚不存在的依赖（支持乱序登记）；缺失依赖的任务保持
//     PENDING；一旦依赖图闭合成环，会在下一次 DispatchOnce 中被明确标记为
//     FAILED/CYCLE_DEPENDENCY，绝不会静默等待。
func (s *Scheduler) SubmitTask(t domain.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if t.ID == "" {
		return fmt.Errorf("%w: task id is empty", ErrInvalidArgument)
	}
	if t.Demand <= 0 {
		return fmt.Errorf("%w: task %q demand must be > 0", ErrInvalidArgument, t.ID)
	}
	if _, ok := s.state.Tasks[t.ID]; ok {
		return fmt.Errorf("%w: task %q", ErrDuplicateID, t.ID)
	}
	seen := map[string]bool{}
	for _, dep := range t.Dependencies {
		if dep == t.ID {
			return fmt.Errorf("%w: task %q depends on itself", ErrCycleDependency, t.ID)
		}
		seen[dep] = true
	}
	t.Dependencies = keysOf(seen)
	// 注意：经前向引用闭合成的多节点环不在提交期拒绝（只拒绝新加入的闭包边会让
	// 环上其余任务永远等待一个“不存在”的依赖，构成静默等待）。所有环上节点统一
	// 由 DispatchOnce 的固定点传播标记为 FAILED/CYCLE_DEPENDENCY，结论一致、可解释。

	now := s.now()
	t.Status = domain.StatusPending
	t.AssignedTo = ""
	t.FailReason = domain.ReasonNone
	t.FailDetail = ""
	t.CreatedAt = now
	t.UpdatedAt = now
	t.Version = 1
	s.state.Tasks[t.ID] = &t
	return s.persistLocked()
}

// DispatchOnce 执行一轮（可能含多趟）调度。每趟：
//  1. 固定点传播：环上任务 -> CYCLE_DEPENDENCY；依赖了坏终态/被拒任务 -> DEPENDENCY_FAILED；
//  2. 收集“依赖全部 SUCCEEDED”的 PENDING/PREEMPTED 任务，按优先级降序、创建时间升序排序；
//  3. 逐个派发：优先 best-fit 空闲容量；不足时可抢占严格更低优先级的在跑任务；
//     物理上任何机器人都装不下 -> FAILED/INSUFFICIENT_CAPACITY（可区分、终态）；
//     暂时抢不到（全是同级/更高级占用）-> 保持等待并记录 REJECT 决策（可解释，非静默）。
//
// 并发安全：同一任务在锁内做“判定+迁移”，并发调用本方法时只会被一个调用者派发。
func (s *Scheduler) DispatchOnce() ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var dispatched []string
	maxPasses := len(s.state.Tasks) + 1
	for pass := 0; pass < maxPasses; pass++ {
		// 先传播环/依赖失败终态，再派发；二者在同一锁内串行，结论确定。
		s.propagateFailuresLocked()
		ready := s.readyTasksLocked()
		if len(ready) == 0 {
			break
		}
		progress := false
		for _, t := range ready {
			switch s.tryDispatchLocked(t) {
			case dispatchAssigned:
				dispatched = append(dispatched, t.ID)
				progress = true
			case dispatchPreemptedAndAssigned:
				dispatched = append(dispatched, t.ID)
				progress = true
			case dispatchRejectedTerminal:
				progress = true // 任务进入终态，图状态发生变化
			case dispatchWaiting:
				// 本趟无容量，保持原状态，后续趟次/下一轮再试。
			}
		}
		if !progress {
			break
		}
	}
	if err := s.persistLocked(); err != nil {
		return nil, err
	}
	return dispatched, nil
}

type dispatchOutcome int

const (
	dispatchWaiting dispatchOutcome = iota
	dispatchAssigned
	dispatchPreemptedAndAssigned
	dispatchRejectedTerminal
)

// Complete 回传执行成功。仅 RUNNING 可迁移；重复回传为幂等 no-op。
func (s *Scheduler) Complete(taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.state.Tasks[taskID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTaskNotFound, taskID)
	}
	if t.Status != domain.StatusRunning {
		s.recordIdempotentSkipLocked("COMPLETE", t)
		return nil
	}
	s.releaseLocked(t)
	t.Status = domain.StatusSucceeded
	t.FailReason = domain.ReasonNone
	t.FinishedAt = s.now()
	t.UpdatedAt = t.FinishedAt
	t.Version++
	s.decideLocked(domain.Decision{
		Action: "COMPLETE", TaskID: t.ID, RobotID: t.AssignedTo,
		Priority: t.Priority, Demand: t.Demand,
		Detail: "task finished successfully; capacity released",
	})
	t.AssignedTo = ""
	return s.persistLocked()
}

// Fail 回传执行失败（运行期故障），释放占用并把任务置为 FAILED；重复回传幂等。
func (s *Scheduler) Fail(taskID string, reason domain.FailReason, detail string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.state.Tasks[taskID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTaskNotFound, taskID)
	}
	if t.Status != domain.StatusRunning {
		s.recordIdempotentSkipLocked("FAIL", t)
		return nil
	}
	if reason == domain.ReasonNone {
		reason = domain.FailReason("RUNTIME_ERROR")
	}
	robot := t.AssignedTo
	s.releaseLocked(t)
	t.Status = domain.StatusFailed
	t.FailReason = reason
	t.FailDetail = detail
	t.FinishedAt = s.now()
	t.UpdatedAt = t.FinishedAt
	t.Version++
	t.AssignedTo = ""
	s.decideLocked(domain.Decision{
		Action: "FAIL", TaskID: t.ID, RobotID: robot,
		Priority: t.Priority, Demand: t.Demand, Reason: reason, Detail: detail,
	})
	return s.persistLocked()
}

// Cancel 取消任务。RUNNING 取消会完整释放占用；终态任务取消为幂等 no-op；
// PENDING/PREEMPTED 可直接取消。已取消任务永远不会再被派发。
func (s *Scheduler) Cancel(taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.state.Tasks[taskID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTaskNotFound, taskID)
	}
	switch t.Status {
	case domain.StatusSucceeded, domain.StatusFailed, domain.StatusCanceled:
		s.recordIdempotentSkipLocked("CANCEL", t)
		return nil
	case domain.StatusRunning:
		s.releaseLocked(t)
		t.AssignedTo = ""
		t.Status = domain.StatusCanceled
	default:
		t.Status = domain.StatusCanceled
	}
	t.FailReason = domain.ReasonNone
	t.FinishedAt = s.now()
	t.UpdatedAt = t.FinishedAt
	t.Version++
	s.decideLocked(domain.Decision{
		Action: "CANCEL", TaskID: t.ID,
		Priority: t.Priority, Demand: t.Demand,
		Detail: "task canceled; any held capacity was released in full",
	})
	return s.persistLocked()
}

// MarkTimeout 标记执行超时：等价于取消但原因显式记为 TIMEOUT，占用完整释放（幂等）。
func (s *Scheduler) MarkTimeout(taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.state.Tasks[taskID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTaskNotFound, taskID)
	}
	if t.Status != domain.StatusRunning {
		s.recordIdempotentSkipLocked("TIMEOUT", t)
		return nil
	}
	robot := t.AssignedTo
	s.releaseLocked(t)
	t.Status = domain.StatusCanceled
	t.FailReason = domain.FailReason("TIMEOUT")
	t.FailDetail = "execution timed out; capacity released"
	t.AssignedTo = ""
	t.FinishedAt = s.now()
	t.UpdatedAt = t.FinishedAt
	t.Version++
	s.decideLocked(domain.Decision{
		Action: "TIMEOUT", TaskID: t.ID, RobotID: robot,
		Priority: t.Priority, Demand: t.Demand,
		Reason: domain.FailReason("TIMEOUT"),
		Detail: "execution timed out; held capacity released in full",
	})
	return s.persistLocked()
}

// ---- 查询 ----

func (s *Scheduler) GetTask(id string) (*domain.Task, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.state.Tasks[id]
	if !ok {
		return nil, false
	}
	cp := *t
	cp.Dependencies = append([]string(nil), t.Dependencies...)
	return &cp, true
}

func (s *Scheduler) ListTasks() []*domain.Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*domain.Task, 0, len(s.state.Tasks))
	for _, t := range s.state.Tasks {
		cp := *t
		cp.Dependencies = append([]string(nil), t.Dependencies...)
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) || out[i].ID < out[j].ID })
	return out
}

func (s *Scheduler) GetRobot(id string) (*domain.Robot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.state.Robots[id]
	if !ok {
		return nil, false
	}
	cp := *r
	return &cp, true
}

func (s *Scheduler) ListRobots() []*domain.Robot {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*domain.Robot, 0, len(s.state.Robots))
	for _, r := range s.state.Robots {
		cp := *r
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (s *Scheduler) Decisions() []domain.Decision {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]domain.Decision, len(s.decisions))
	copy(out, s.decisions)
	return out
}

// UsedCapacity 返回全部机器人已占用容量之和（用于校验永不为负/不超总容量）。
func (s *Scheduler) UsedCapacity() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := 0
	for _, r := range s.state.Robots {
		total += r.Used
	}
	return total
}

// RecoveredTaskCount 返回本次启动从中断 RUNNING 态恢复为 PREEMPTED 的任务数。
func (s *Scheduler) RecoveredTaskCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.recovered
}

// CheckInvariants 返回被破坏的不变量描述列表；为空表示全部满足。
//   - 任意机器人 0 <= Used <= Capacity；
//   - RUNNING 任务恰好占用 AssignedTo 一台机器人，且其 Demand 被计入该机器人 Used；
//   - 非 RUNNING 任务 AssignedTo 必须为空；
//   - 机器人 Used 等于其上 RUNNING 任务 Demand 之和。
func (s *Scheduler) CheckInvariants() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checkInvariantsLocked()
}

// ---- 内部实现（调用方持锁） ----

func (s *Scheduler) persistLocked() error {
	if errs := s.checkInvariantsLocked(); len(errs) > 0 {
		return fmt.Errorf("refuse to persist broken invariants: %v", errs)
	}
	return s.store.Save(s.state)
}

func (s *Scheduler) checkInvariantsLocked() []string {
	var problems []string
	usedBy := map[string]int{}
	for _, t := range s.state.Tasks {
		switch t.Status {
		case domain.StatusRunning:
			if t.AssignedTo == "" {
				problems = append(problems, fmt.Sprintf("task %s RUNNING without robot", t.ID))
				continue
			}
			r, ok := s.state.Robots[t.AssignedTo]
			if !ok {
				problems = append(problems, fmt.Sprintf("task %s assigned to missing robot %s", t.ID, t.AssignedTo))
				continue
			}
			usedBy[r.ID] += t.Demand
		default:
			if t.AssignedTo != "" {
				problems = append(problems, fmt.Sprintf("task %s in status %s still holds robot %s", t.ID, t.Status, t.AssignedTo))
			}
		}
	}
	for id, r := range s.state.Robots {
		if r.Used < 0 {
			problems = append(problems, fmt.Sprintf("robot %s used %d is negative", id, r.Used))
		}
		if r.Used > r.Capacity {
			problems = append(problems, fmt.Sprintf("robot %s used %d exceeds capacity %d", id, r.Used, r.Capacity))
		}
		if r.Used != usedBy[id] {
			problems = append(problems, fmt.Sprintf("robot %s used=%d but running tasks demand=%d", id, r.Used, usedBy[id]))
		}
	}
	return problems
}

// recoverLocked 启动对账，见 New 的注释。
func (s *Scheduler) recoverLocked() {
	now := s.now()
	runningByRobot := map[string][]*domain.Task{}
	for _, t := range s.state.Tasks {
		if t.Status == domain.StatusRunning {
			runningByRobot[t.AssignedTo] = append(runningByRobot[t.AssignedTo], t)
		}
	}
	for _, ts := range runningByRobot {
		for _, t := range ts {
			robot := t.AssignedTo
			t.Status = domain.StatusPreempted
			t.AssignedTo = ""
			t.FailReason = domain.FailReason("RECOVERED_AFTER_RESTART")
			t.FailDetail = "scheduler restarted while task was running; queued for redispatch"
			t.UpdatedAt = now
			t.Version++
			s.recovered++
			s.decideLocked(domain.Decision{
				Action: "RECOVER", TaskID: t.ID, RobotID: robot,
				Priority: t.Priority, Demand: t.Demand,
				Reason: domain.FailReason("RECOVERED_AFTER_RESTART"),
				Detail: "found RUNNING in persisted snapshot at startup; moved to PREEMPTED; capacity recalculated",
			})
		}
	}
	// 所有中断前 RUNNING 的任务均已转为 PREEMPTED，按“Used = RUNNING 任务占用之和”
	// 重算后所有机器人占用归零；仅在确有恢复时才变更并落盘，保证恢复结论可重放。
	if s.recovered > 0 {
		for _, r := range s.state.Robots {
			r.Used = 0
			r.UpdatedAt = now
			r.Version++
		}
		_ = s.store.Save(s.state)
	}
}

// propagateFailuresLocked 做固定点传播：
//  1. 找出依赖图中所有能到达环的任务，标记 FAILED/CYCLE_DEPENDENCY；
//  2. 依赖了 FAILED/CANCELED（或任何已带终态失败原因）任务的，标记 FAILED/DEPENDENCY_FAILED。
//
// 返回本轮是否有新标记。PREEMPTED 不具传染性（它仍可成功）。
func (s *Scheduler) propagateFailuresLocked() bool {
	changed := false
	// 1) 环检测：对每个未决任务做 DFS，收集“在环上或依赖链能到达环”的任务。
	poisonCycle := map[string]bool{}
	color := map[string]int{} // 0 白 1 灰 2 黑
	var stack []string
	var cycleNodes []string
	var dfs func(id string) bool
	dfs = func(id string) bool {
		color[id] = 1
		stack = append(stack, id)
		t := s.state.Tasks[id]
		reachesCycle := false
		if t != nil {
			for _, dep := range t.Dependencies {
				if color[dep] == 1 {
					// 发现回边，回退栈找出环上节点。
					for i := len(stack) - 1; i >= 0; i-- {
						cycleNodes = append(cycleNodes, stack[i])
						if stack[i] == dep {
							break
						}
					}
					reachesCycle = true
					continue
				}
				if color[dep] == 0 {
					if dfs(dep) {
						reachesCycle = true
					}
				} else if poisonCycle[dep] {
					reachesCycle = true
				}
			}
		}
		stack = stack[:len(stack)-1]
		color[id] = 2
		if reachesCycle {
			poisonCycle[id] = true
		}
		return reachesCycle
	}
	for id := range s.state.Tasks {
		if color[id] == 0 {
			dfs(id)
		}
	}
	_ = cycleNodes
	for id := range poisonCycle {
		t := s.state.Tasks[id]
		if t.Status == domain.StatusPending || t.Status == domain.StatusPreempted {
			s.markRejectedLocked(t, domain.ReasonCycleDependency,
				fmt.Sprintf("dependency graph contains a cycle reachable from %s: %v", id, cycleNodes))
			changed = true
		}
	}

	// 2) 依赖失败传播，固定点迭代直到不再变化。
	for {
		prog := false
		ids := make([]string, 0, len(s.state.Tasks))
		for id := range s.state.Tasks {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			t := s.state.Tasks[id]
			if t.Status != domain.StatusPending && t.Status != domain.StatusPreempted {
				continue
			}
			var badDep string
			for _, dep := range t.Dependencies {
				d := s.state.Tasks[dep]
				if d == nil {
					continue
				}
				if d.Status == domain.StatusFailed || d.Status == domain.StatusCanceled {
					badDep = dep
					break
				}
			}
			if badDep != "" {
				s.markRejectedLocked(t, domain.ReasonDependencyFailed,
					fmt.Sprintf("dependency %q is in terminal non-success status %s", badDep, s.state.Tasks[badDep].Status))
				prog = true
				changed = true
			}
		}
		if !prog {
			break
		}
	}
	return changed
}

// markRejectedLocked 把一个永不可能成功的未决任务置为 FAILED 终态并记录 REJECT 决策。
func (s *Scheduler) markRejectedLocked(t *domain.Task, reason domain.FailReason, detail string) {
	t.Status = domain.StatusFailed
	t.FailReason = reason
	t.FailDetail = detail
	t.FinishedAt = s.now()
	t.UpdatedAt = t.FinishedAt
	t.Version++
	s.decideLocked(domain.Decision{
		Action: "REJECT", TaskID: t.ID, Priority: t.Priority, Demand: t.Demand,
		Reason: reason, Detail: detail,
	})
}

// readyTasksLocked 返回当前可派发任务（PENDING/PREEMPTED 且依赖全部 SUCCEEDED），
// 按优先级降序、创建时间升序、ID 升序排列，保证并发/多轮调度结论确定。
func (s *Scheduler) readyTasksLocked() []*domain.Task {
	var ready []*domain.Task
	for _, t := range s.state.Tasks {
		if t.Status != domain.StatusPending && t.Status != domain.StatusPreempted {
			continue
		}
		allDone := true
		for _, dep := range t.Dependencies {
			if d, ok := s.state.Tasks[dep]; !ok || d.Status != domain.StatusSucceeded {
				allDone = false
				break
			}
		}
		if allDone {
			ready = append(ready, t)
		}
	}
	sort.Slice(ready, func(i, j int) bool {
		if ready[i].Priority != ready[j].Priority {
			return ready[i].Priority > ready[j].Priority
		}
		if !ready[i].CreatedAt.Equal(ready[j].CreatedAt) {
			return ready[i].CreatedAt.Before(ready[j].CreatedAt)
		}
		return ready[i].ID < ready[j].ID
	})
	return ready
}

func (s *Scheduler) tryDispatchLocked(t *domain.Task) dispatchOutcome {
	// 物理上装不下：任何机器人容量都 < demand，永远不可能成功，明确拒绝。
	maxCap := 0
	for _, r := range s.state.Robots {
		if r.Capacity > maxCap {
			maxCap = r.Capacity
		}
	}
	if t.Demand > maxCap {
		s.markRejectedLocked(t, domain.ReasonInsufficientCap,
			fmt.Sprintf("demand %d exceeds largest robot capacity %d; can never be scheduled", t.Demand, maxCap))
		return dispatchRejectedTerminal
	}

	// 1) 直接可装下：best-fit（剩余容量最小且够用），减少碎片。
	robots := sortedRobots(s.state.Robots)
	var best *domain.Robot
	for _, r := range robots {
		if r.Free() >= t.Demand {
			if best == nil || r.Free() < best.Free() {
				best = r
			}
		}
	}
	if best != nil {
		s.assignLocked(t, best, nil)
		return dispatchAssigned
	}

	// 2) 抢占：仅可抢占严格更低优先级的在跑任务。
	//    在每台物理装得下的机器人上，按优先级升序（最弱者先牺牲）挑选牺牲者，
	//    直到释放出足够容量；选择牺牲者最少的机器人，平局按机器人 ID。
	type plan struct {
		robot   *domain.Robot
		victims []*domain.Task
	}
	var chosen *plan
	for _, r := range robots {
		if r.Capacity < t.Demand {
			continue
		}
		var victims []*domain.Task
		for _, rt := range s.runningOnLocked(r.ID) {
			if rt.Priority < t.Priority {
				victims = append(victims, rt)
			}
		}
		sort.Slice(victims, func(i, j int) bool {
			if victims[i].Priority != victims[j].Priority {
				return victims[i].Priority < victims[j].Priority
			}
			if !victims[i].StartedAt.Equal(victims[j].StartedAt) {
				return victims[i].StartedAt.Before(victims[j].StartedAt)
			}
			return victims[i].ID < victims[j].ID
		})
		need := t.Demand - r.Free()
		var picked []*domain.Task
		freed := 0
		for _, v := range victims {
			picked = append(picked, v)
			freed += v.Demand
			if freed >= need {
				break
			}
		}
		if freed < need {
			continue
		}
		if chosen == nil || len(picked) < len(chosen.victims) ||
			(len(picked) == len(chosen.victims) && r.ID < chosen.robot.ID) {
			chosen = &plan{robot: r, victims: picked}
		}
	}
	if chosen == nil {
		// 暂时抢不到：这是可恢复状态（等在跑任务完成/取消释放容量），
		// 绝不置为终态，只记录 REJECT 决策让等待可解释。
		s.decideLocked(domain.Decision{
			Action: "REJECT", TaskID: t.ID, Priority: t.Priority, Demand: t.Demand,
			Reason: domain.ReasonInsufficientCap,
			Detail: "temporarily no capacity: every fitting robot is occupied by equal-or-higher priority tasks; task stays queued",
		})
		return dispatchWaiting
	}

	victimIDs := make([]string, 0, len(chosen.victims))
	for _, v := range chosen.victims {
		victimIDs = append(victimIDs, v.ID)
		s.preemptLocked(v, chosen.robot, t)
	}
	s.assignLocked(t, chosen.robot, victimIDs)
	return dispatchPreemptedAndAssigned
}

// assignLocked 在已持有锁的前提下把任务派发到机器人，计入占用并记录 DISPATCH 决策。
func (s *Scheduler) assignLocked(t *domain.Task, r *domain.Robot, preempted []string) {
	now := s.now()
	fromStatus := t.Status
	r.Used += t.Demand
	r.UpdatedAt = now
	r.Version++
	t.Status = domain.StatusRunning
	t.AssignedTo = r.ID
	t.StartedAt = now
	t.UpdatedAt = now
	t.Version++
	// 清理上一次抢占/恢复留下的解释性痕迹，避免重新运行后误读。
	t.FailReason = domain.ReasonNone
	t.FailDetail = ""
	detail := "all dependencies succeeded and robot has enough free capacity"
	if len(preempted) > 0 {
		detail = fmt.Sprintf("dispatched after preempting lower-priority task(s) %v", preempted)
	}
	if fromStatus == domain.StatusPreempted {
		detail += "; task was in PREEMPTED (queued for redispatch)"
	}
	d := domain.Decision{
		Action: "DISPATCH", TaskID: t.ID, RobotID: r.ID,
		Priority: t.Priority, Demand: t.Demand, Detail: detail,
		DepStatus: s.snapshotDepsLocked(t),
	}
	s.decideLocked(d)
}

// preemptLocked 把低优先级在跑任务转为 PREEMPTED（保留为可解释状态，不丢失），释放其占用。
func (s *Scheduler) preemptLocked(v *domain.Task, r *domain.Robot, winner *domain.Task) {
	r.Used -= v.Demand
	r.UpdatedAt = s.now()
	r.Version++
	v.Status = domain.StatusPreempted
	v.AssignedTo = ""
	v.FailReason = domain.FailReason("PREEMPTED_BY_HIGHER_PRIORITY")
	v.FailDetail = fmt.Sprintf("preempted by higher-priority task %q (p=%d > p=%d); queued for redispatch",
		winner.ID, winner.Priority, v.Priority)
	v.UpdatedAt = s.now()
	v.Version++
	s.decideLocked(domain.Decision{
		Action: "PREEMPT", TaskID: v.ID, RobotID: r.ID,
		Priority: v.Priority, Demand: v.Demand,
		Reason: domain.FailReason("PREEMPTED_BY_HIGHER_PRIORITY"),
		Detail: fmt.Sprintf("capacity reclaimed for task %q with higher priority; task retained as PREEMPTED, not lost", winner.ID),
	})
}

// releaseLocked 释放 RUNNING 任务在其机器人上的占用；减法只按实际登记量进行，杜绝负数。
func (s *Scheduler) releaseLocked(t *domain.Task) {
	if t.AssignedTo == "" {
		return
	}
	r, ok := s.state.Robots[t.AssignedTo]
	if !ok {
		return
	}
	r.Used -= t.Demand
	if r.Used < 0 {
		r.Used = 0
	}
	r.UpdatedAt = s.now()
	r.Version++
}

func (s *Scheduler) runningOnLocked(robotID string) []*domain.Task {
	var out []*domain.Task
	for _, t := range s.state.Tasks {
		if t.Status == domain.StatusRunning && t.AssignedTo == robotID {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (s *Scheduler) snapshotDepsLocked(t *domain.Task) map[string]domain.TaskStatus {
	if len(t.Dependencies) == 0 {
		return nil
	}
	m := make(map[string]domain.TaskStatus, len(t.Dependencies))
	for _, dep := range t.Dependencies {
		if d, ok := s.state.Tasks[dep]; ok {
			m[dep] = d.Status
		} else {
			m[dep] = domain.TaskStatus("MISSING")
		}
	}
	return m
}

func (s *Scheduler) recordIdempotentSkipLocked(action string, t *domain.Task) {
	s.decideLocked(domain.Decision{
		Action: action + "_IDEMPOTENT_SKIP", TaskID: t.ID, RobotID: t.AssignedTo,
		Priority: t.Priority, Demand: t.Demand,
		Reason: domain.ReasonPreconditionNotMet,
		Detail: fmt.Sprintf("task already in terminal/status %s; duplicate report ignored, capacity untouched", t.Status),
	})
}

func (s *Scheduler) decideLocked(d domain.Decision) {
	if d.Time.IsZero() {
		d.Time = s.now()
	}
	s.decisions = append(s.decisions, d)
	// 持久化判定日志失败不改变内存判定（状态快照才是恢复依据）。
	_ = s.store.AppendDecision(d)
	if s.trace != nil {
		robot := d.RobotID
		if robot == "" {
			robot = "-"
		}
		fmt.Fprintf(s.trace, "[%s] %-22s task=%-5s robot=%-5s p=%d demand=%d reason=%-22s deps=%v :: %s\n",
			d.Time.Format("15:04:05.000"), d.Action, d.TaskID, robot, d.Priority, d.Demand, d.Reason, d.DepStatus, d.Detail)
	}
}

func sortedRobots(m map[string]*domain.Robot) []*domain.Robot {
	out := make([]*domain.Robot, 0, len(m))
	for _, r := range m {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func keysOf(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
