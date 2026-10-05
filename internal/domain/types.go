// Package domain 定义多机器人任务调度系统的核心领域类型。
package domain

import (
	"fmt"
	"time"
)

// Resources 表示一类可竞争资源的多维容量（如 CPU、电量槽位、载荷位）。
type Resources struct {
	CPU     int
	Power   int
	Payload int
}

// TaskStatus 任务生命周期状态。
type TaskStatus string

const (
	StatusPending   TaskStatus = "PENDING"   // 已提交，尚未进入可调度队列
	StatusReady     TaskStatus = "READY"     // 依赖已满足，等待调度
	StatusRunning   TaskStatus = "RUNNING"   // 已派发并在某机器人上执行
	StatusSucceeded TaskStatus = "SUCCEEDED" // 执行成功（终态）
	StatusFailed    TaskStatus = "FAILED"    // 执行/校验失败（终态）
	StatusCanceled  TaskStatus = "CANCELED"  // 被取消（终态）
	StatusPreempted TaskStatus = "PREEMPTED" // 曾运行，被高优先级任务抢占，保留待重试
)

// FailureReason 可区分的失败/拒绝原因。
type FailureReason string

const (
	ReasonCycleDependency      FailureReason = "CYCLE_DEPENDENCY"
	ReasonDependencyFailed     FailureReason = "DEPENDENCY_FAILED"
	ReasonInsufficientCapacity FailureReason = "INSUFFICIENT_CAPACITY"
	ReasonDuplicateTask        FailureReason = "DUPLICATE_TASK"
	ReasonTaskNotFound         FailureReason = "TASK_NOT_FOUND"
	ReasonExecutionFailed      FailureReason = "EXECUTION_FAILED"
	ReasonTimeout              FailureReason = "TIMEOUT"
	ReasonInvalidReservation   FailureReason = "INVALID_RESERVATION"
)

// IsTerminal 判断任务是否处于不可再派发的终态。
func (s TaskStatus) IsTerminal() bool {
	return s == StatusSucceeded || s == StatusFailed || s == StatusCanceled
}

// IsRunnable 判断任务是否可被派发给执行器。
func (s TaskStatus) IsRunnable() bool {
	return s == StatusRunning
}

// TaskError 携带可区分失败原因的错误。
type TaskError struct {
	Reason  FailureReason
	Message string
}

func (e *TaskError) Error() string {
	return string(e.Reason) + ": " + e.Message
}

// NewError 构造带原因的任务错误。
func NewError(reason FailureReason, format string, args ...any) *TaskError {
	return &TaskError{Reason: reason, Message: fmt.Sprintf(format, args...)}
}

// Add 资源逐维相加。
func (r Resources) Add(o Resources) Resources {
	return Resources{CPU: r.CPU + o.CPU, Power: r.Power + o.Power, Payload: r.Payload + o.Payload}
}

// Sub 资源逐维相减；允许出现负值用于中间计算，最终占用由存储层校验。
func (r Resources) Sub(o Resources) Resources {
	return Resources{CPU: r.CPU - o.CPU, Power: r.Power - o.Power, Payload: r.Payload - o.Payload}
}

// Fits 判断 r 是否能容纳 need（need 的每一维均不超过 r）。
func (r Resources) Fits(need Resources) bool {
	return need.CPU <= r.CPU && need.Power <= r.Power && need.Payload <= r.Payload
}

// AnyNegative 判断资源任一维是否为负（资源不可为负的不变量检查）。
func (r Resources) AnyNegative() bool {
	return r.CPU < 0 || r.Power < 0 || r.Payload < 0
}

// IsZero 判断资源是否全部为零。
func (r Resources) IsZero() bool {
	return r.CPU == 0 && r.Power == 0 && r.Payload == 0
}

// Task 任务定义（提交后不可变）。
type Task struct {
	ID           string
	Priority     int // 数值越大优先级越高
	Dependencies []string
	Demand       Resources
	Timeout      time.Duration
}

// TaskState 任务的持久化运行时状态。
type TaskState struct {
	Task      Task
	Status    TaskStatus
	Assigned  string
	Reason    FailureReason
	Message   string
	UpdatedAt time.Time
}

// Robot 机器人资源定义。
type Robot struct {
	ID       string
	Capacity Resources
	// Reserved 预留区容量（逐维度）。只有优先级达到 ReserveMinPriority 的
	// 任务才能动用这部分容量；零值表示不启用预留，行为与此前完全一致。
	Reserved Resources
	// ReserveMinPriority 动用预留区的优先级门槛：Priority >= 门槛才可用预留区。
	ReserveMinPriority int
}

// HasReservation 判断机器人是否配置了预留区。
func (r Robot) HasReservation() bool {
	return !r.Reserved.IsZero()
}

// PublicCapacity 返回公共区容量（任何任务都可使用），逐维 = Capacity - Reserved。
// 配置经 AddRobot 校验（Reserved 每维不超过 Capacity），结果不会为负。
func (r Robot) PublicCapacity() Resources {
	return r.Capacity.Sub(r.Reserved)
}

// EffectiveCapacity 返回给定优先级任务可动用的总容量上限：
// 达到门槛的任务可用全部容量（公共区 + 预留区）；
// 未达门槛的任务只能用公共区，绝不能碰预留部分。
func (r Robot) EffectiveCapacity(priority int) Resources {
	if !r.HasReservation() || priority >= r.ReserveMinPriority {
		return r.Capacity
	}
	return r.PublicCapacity()
}

// EventKind 事件种类。
type EventKind string

const (
	EventSubmitted EventKind = "SUBMITTED"
	EventReady     EventKind = "READY"
	EventAssigned  EventKind = "ASSIGNED"
	EventSucceeded EventKind = "SUCCEEDED"
	EventFailed    EventKind = "FAILED"
	EventCanceled  EventKind = "CANCELED"
	EventPreempted EventKind = "PREEMPTED"
)

// Event 调度/执行过程中的可解释事件。
type Event struct {
	Time    time.Time
	Kind    EventKind
	TaskID  string
	RobotID string
	Detail  string
	Reason  FailureReason
}

// Snapshot 持久化快照。
type Snapshot struct {
	Robots  []Robot
	Tasks   []TaskState
	Events  []Event
	Version int
	SavedAt time.Time
}
