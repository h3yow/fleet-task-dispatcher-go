package domain

import "time"

// TaskStatus 任务在其生命周期中的状态。
type TaskStatus string

const (
	StatusPending   TaskStatus = "PENDING"   // 已登记，尚未派发
	StatusRunning   TaskStatus = "RUNNING"   // 已派发到某台机器人，正在执行
	StatusSucceeded TaskStatus = "SUCCEEDED" // 执行成功（终态）
	StatusFailed    TaskStatus = "FAILED"    // 执行失败 / 被拒绝（终态）
	StatusCanceled  TaskStatus = "CANCELED"  // 被用户取消或因超时被系统取消（终态）
	StatusPreempted TaskStatus = "PREEMPTED" // 被高优先级任务抢占，等待重新调度（可解释的中间态）
)

// FailReason 任务被拒绝/失败的可区分原因。
type FailReason string

const (
	ReasonNone               FailReason = ""                      // 无失败
	ReasonCycleDependency    FailReason = "CYCLE_DEPENDENCY"      // 依赖图存在环
	ReasonDependencyFailed   FailReason = "DEPENDENCY_FAILED"     // 前置任务已失败/取消，不可能再满足
	ReasonInsufficientCap    FailReason = "INSUFFICIENT_CAPACITY" // 全部机器人容量都不足
	ReasonRobotNotFound      FailReason = "ROBOT_NOT_FOUND"       // 目标机器人不存在
	ReasonTaskNotFound       FailReason = "TASK_NOT_FOUND"        // 任务不存在
	ReasonPreconditionNotMet FailReason = "PRECONDITION_NOT_MET"  // 当前状态不允许该操作
)

// Priority 数值越大优先级越高，抢占时高优先级优先。
type Priority int

// Task 描述一个待调度任务及其当前状态。
type Task struct {
	ID           string
	Name         string
	Priority     Priority
	Demand       int      // 需要占用的机器人容量单位
	Dependencies []string // 必须先 SUCCEEDED 的任务 ID
	Status       TaskStatus
	AssignedTo   string // 当前占用的机器人 ID（RUNNING 时非空）
	FailReason   FailReason
	FailDetail   string
	CreatedAt    time.Time
	UpdatedAt    time.Time
	StartedAt    time.Time
	FinishedAt   time.Time
	Version      int // 乐观版本，每次状态迁移 +1
}

// Robot 描述一台机器人及其容量占用情况。
type Robot struct {
	ID        string
	Name      string
	Capacity  int // 总容量
	Used      int // 已占用容量（任何时刻 0 <= Used <= Capacity）
	UpdatedAt time.Time
	Version   int
}

// Free 返回剩余可用容量，任何情况下都不会为负。
func (r *Robot) Free() int { return r.Capacity - r.Used }

// State 是被持久化的全量快照。
type State struct {
	Tasks  map[string]*Task
	Robots map[string]*Robot
}

// Decision 记录一次调度判定的输入与依据，用于可解释日志。
type Decision struct {
	Time      time.Time
	Action    string // DISPATCH / REJECT / COMPLETE / FAIL / CANCEL / TIMEOUT / PREEMPT / RECOVER
	TaskID    string
	RobotID   string
	Priority  Priority
	Demand    int
	Reason    FailReason
	Detail    string
	DepStatus map[string]TaskStatus // 判定时各依赖的状态快照
}
