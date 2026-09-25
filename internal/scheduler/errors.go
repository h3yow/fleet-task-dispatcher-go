package scheduler

import "errors"

// API 级错误（输入/前置条件问题）；任务本身的调度失败原因使用 domain.FailReason 落库。
var (
	ErrNotFound        = errors.New("not found")
	ErrTaskNotFound    = errors.New("task not found")
	ErrRobotNotFound   = errors.New("robot not found")
	ErrDuplicateID     = errors.New("id already exists")
	ErrInvalidArgument = errors.New("invalid argument")
	ErrPrecondition    = errors.New("precondition not met")
)
