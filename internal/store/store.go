// Package store 提供本地持久化存储与并发安全的任务/机器人状态管理。
//
// 所有状态变更都必须在 Commit 临界区内完成：互斥锁保证多个调度入口
// （HTTP、定时器、恢复流程等）并发竞争同一批资源时结论一致，
// 临界区结束后原子落盘，模拟任务队列与执行结果的本地持久化。
package store

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/h3yow/fleet-task-dispatcher-go/internal/domain"
)

// Store 本地存储：内存状态 + 原子落盘。
type Store struct {
	mu      sync.Mutex
	path    string
	version int
	robots  map[string]domain.Robot
	tasks   map[string]*domain.TaskState
	events  []domain.Event
}

// New 创建空存储。path 为空表示仅内存模式（不落盘）。
func New(path string) *Store {
	return &Store{
		path:   path,
		robots: map[string]domain.Robot{},
		tasks:  map[string]*domain.TaskState{},
	}
}

// Load 从磁盘加载快照；文件不存在时保持空状态且不报错。
func (s *Store) Load() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.path == "" {
		return nil
	}
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var snap domain.Snapshot
	if len(data) > 0 {
		if err := json.Unmarshal(data, &snap); err != nil {
			return err
		}
		s.robots = map[string]domain.Robot{}
		for _, r := range snap.Robots {
			s.robots[r.ID] = r
		}
		s.tasks = map[string]*domain.TaskState{}
		for i := range snap.Tasks {
			st := snap.Tasks[i]
			s.tasks[st.Task.ID] = &st
		}
		s.events = append(s.events[:0], snap.Events...)
		s.version = snap.Version
	}
	return nil
}

// persistLocked 调用方必须已持锁。原子写入：先写临时文件再 rename。
func (s *Store) persistLocked() error {
	if s.path == "" {
		return nil
	}
	s.version++
	snap := domain.Snapshot{
		Robots:  s.robotsLocked(),
		Tasks:   s.tasksLocked(),
		Events:  append([]domain.Event(nil), s.events...),
		Version: s.version,
		SavedAt: time.Now().UTC(),
	}
	data, err := json.MarshalIndent(&snap, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

// Commit 在单一互斥临界区内执行变更并落盘。
// fn 返回错误时回滚本次临界区内的全部修改，且不落盘。
func (s *Store) Commit(fn func(tx *Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 拷贝用于失败回滚。
	backupRobots := make(map[string]domain.Robot, len(s.robots))
	for k, v := range s.robots {
		backupRobots[k] = v
	}
	backupTasks := make(map[string]*domain.TaskState, len(s.tasks))
	for k, v := range s.tasks {
		cp := *v
		backupTasks[k] = &cp
	}
	backupEvents := append([]domain.Event(nil), s.events...)

	tx := &Tx{store: s, robots: s.robots, tasks: s.tasks, events: &s.events}
	if err := fn(tx); err != nil {
		s.robots = backupRobots
		s.tasks = backupTasks
		s.events = backupEvents
		return err
	}
	return s.persistLocked()
}

// View 在只读临界区内读取状态。
func (s *Store) View(fn func(tx *Tx)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(&Tx{store: s, robots: s.robots, tasks: s.tasks, events: &s.events})
}

func (s *Store) robotsLocked() []domain.Robot {
	out := make([]domain.Robot, 0, len(s.robots))
	for _, r := range s.robots {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (s *Store) tasksLocked() []domain.TaskState {
	out := make([]domain.TaskState, 0, len(s.tasks))
	for _, t := range s.tasks {
		out = append(out, *t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Task.ID < out[j].Task.ID })
	return out
}

// Snapshot 返回当前状态的深拷贝（读取用）。
func (s *Store) Snapshot() domain.Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return domain.Snapshot{
		Robots:  s.robotsLocked(),
		Tasks:   s.tasksLocked(),
		Events:  append([]domain.Event(nil), s.events...),
		Version: s.version,
		SavedAt: time.Now().UTC(),
	}
}

// Tx 事务句柄，仅在 Commit/View 临界区内有效。
type Tx struct {
	store  *Store
	robots map[string]domain.Robot
	tasks  map[string]*domain.TaskState
	events *[]domain.Event
}

// AddRobot 注册或更新机器人容量。
func (tx *Tx) AddRobot(r domain.Robot) {
	tx.robots[r.ID] = r
}

// Robots 按 ID 排序列出全部机器人。
func (tx *Tx) Robots() []domain.Robot {
	out := make([]domain.Robot, 0, len(tx.robots))
	for _, r := range tx.robots {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Robot 按 ID 查询机器人。
func (tx *Tx) Robot(id string) (domain.Robot, bool) {
	r, ok := tx.robots[id]
	return r, ok
}

// PutTask 插入或覆盖任务状态。
func (tx *Tx) PutTask(st domain.TaskState) {
	cp := st
	tx.tasks[st.Task.ID] = &cp
}

// Task 按 ID 查询任务状态（返回状态副本，修改需配合 PutTask 回写）。
func (tx *Tx) Task(id string) (domain.TaskState, bool) {
	st, ok := tx.tasks[id]
	if !ok {
		return domain.TaskState{}, false
	}
	return *st, true
}

// Tasks 按任务 ID 排序列出全部任务状态副本。
func (tx *Tx) Tasks() []domain.TaskState {
	out := make([]domain.TaskState, 0, len(tx.tasks))
	for _, st := range tx.tasks {
		out = append(out, *st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Task.ID < out[j].Task.ID })
	return out
}

// Used 计算机器人当前被 RUNNING 任务占用的资源量。
// 被取消、抢占（PREEMPTED）、已完成的任务均不计入占用。
func (tx *Tx) Used(robotID string) domain.Resources {
	var used domain.Resources
	for _, st := range tx.tasks {
		if st.Status == domain.StatusRunning && st.Assigned == robotID {
			used = used.Add(st.Task.Demand)
		}
	}
	return used
}

// Available 计算机器人剩余可用资源；结果任一维不得为负。
func (tx *Tx) Available(robotID string) domain.Resources {
	r, ok := tx.robots[robotID]
	if !ok {
		return domain.Resources{}
	}
	avail := r.Capacity.Sub(tx.Used(robotID))
	if avail.AnyNegative() {
		// 不变量被破坏时不静默吞掉：交由上层通过 panic/日志发现。
		panic("store: available resources went negative on robot " + robotID)
	}
	return avail
}

// Emit 追加一条可解释事件。
func (tx *Tx) Emit(kind domain.EventKind, taskID, robotID, detail string, reason domain.FailureReason) {
	*tx.events = append(*tx.events, domain.Event{
		Time:    time.Now().UTC(),
		Kind:    kind,
		TaskID:  taskID,
		RobotID: robotID,
		Detail:  detail,
		Reason:  reason,
	})
}

// Events 返回事件副本。
func (tx *Tx) Events() []domain.Event {
	return append([]domain.Event(nil), *tx.events...)
}

// ReplaceAll 用快照内容替换全部状态（用于启动加载/恢复）。
func (tx *Tx) ReplaceAll(snap domain.Snapshot) {
	for k := range tx.robots {
		delete(tx.robots, k)
	}
	for _, r := range snap.Robots {
		tx.robots[r.ID] = r
	}
	for k := range tx.tasks {
		delete(tx.tasks, k)
	}
	for i := range snap.Tasks {
		st := snap.Tasks[i]
		cp := st
		tx.tasks[st.Task.ID] = &cp
	}
	*tx.events = append((*tx.events)[:0], snap.Events...)
}
