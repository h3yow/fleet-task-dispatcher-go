// Package executor 负责任务的本地模拟执行、超时/取消处理与状态回写。
//
// 执行器通过状态存储与调度器解耦：它只派发处于 RUNNING 且当前未在执行的
// 任务，因此即使多个入口并发触发，同一任务在单个执行器进程内也只会被
// 派发一次；取消/超时/抢占都在单一 Commit 临界区内释放占用并落盘。
package executor

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/h3yow/fleet-task-dispatcher-go/internal/domain"
	"github.com/h3yow/fleet-task-dispatcher-go/internal/store"
)

// RunFunc 用户提供的模拟执行函数，需及时响应 ctx 的取消/超时。
type RunFunc func(ctx context.Context, task domain.Task) error

// Executor 轮询 RUNNING 任务并本地模拟执行。
type Executor struct {
	st       *store.Store
	run      RunFunc
	interval time.Duration
	logger   *log.Logger

	mu       sync.Mutex
	inflight map[string]context.CancelFunc // taskID -> 取消函数
	stopping atomic.Bool                   // 进程关闭中：在途结果不再回写（模拟崩溃来不及落盘）
}

// Option 可选配置。
type Option func(*Executor)

// WithPollInterval 设置轮询间隔。
func WithPollInterval(d time.Duration) Option {
	return func(ex *Executor) { ex.interval = d }
}

// WithLogger 设置日志器。
func WithLogger(l *log.Logger) Option {
	return func(ex *Executor) { ex.logger = l }
}

// New 创建执行器。run 为 nil 时使用立即成功的默认模拟。
func New(st *store.Store, run RunFunc, opts ...Option) *Executor {
	if run == nil {
		run = func(ctx context.Context, task domain.Task) error { return nil }
	}
	ex := &Executor{
		st:       st,
		run:      run,
		interval: 50 * time.Millisecond,
		logger:   log.Default(),
		inflight: map[string]context.CancelFunc{},
	}
	for _, o := range opts {
		o(ex)
	}
	return ex
}

// Recover 重启恢复：核对持久化快照。
//   - 终态任务（SUCCEEDED/FAILED/CANCELED）绝不重新派发；
//   - RUNNING 任务属于中断点之前的在途任务，重新派发以继续执行；
//   - READY/PENDING/PREEMPTED 任务等待后续调度。
func (ex *Executor) Recover() (resumed []string, err error) {
	snap := ex.st.Snapshot()
	var terminal, waiting int
	for _, st := range snap.Tasks {
		switch {
		case st.Status.IsTerminal():
			terminal++
		case st.Status == domain.StatusRunning:
			resumed = append(resumed, st.Task.ID)
		default:
			waiting++
		}
	}
	ex.logger.Printf("[recover] snapshot v%d: tasks=%d terminal(never-redispatched)=%d running(resumed)=%d waiting=%d events=%d",
		snap.Version, len(snap.Tasks), terminal, len(resumed), waiting, len(snap.Events))
	return resumed, nil
}

// Start 启动后台轮询循环，直到 ctx 被取消。返回前会等待所有执行协程退出。
func (ex *Executor) Start(ctx context.Context) {
	ex.stopping.Store(false)
	resumed, err := ex.Recover()
	if err != nil {
		ex.logger.Printf("[recover] error: %v", err)
	}
	if len(resumed) > 0 {
		ex.logger.Printf("[recover] interrupted RUNNING tasks will be re-dispatched: %v", resumed)
	}

	ticker := time.NewTicker(ex.interval)
	defer ticker.Stop()

	var wg sync.WaitGroup
	for {
		ex.scan(&wg)
		select {
		case <-ctx.Done():
			// 进程关闭/中断：在途任务结果一律不回写，保留其 RUNNING 持久化状态，
			// 与真实"断电"语义一致——重启后 Recover 会把它们识别为待续任务。
			ex.stopping.Store(true)
			ex.cancelAll()
			wg.Wait()
			return
		case <-ticker.C:
		}
	}
}

// scan 启动新出现的 RUNNING 任务，并回收已取消/被抢占任务的执行协程。
func (ex *Executor) scan(wg *sync.WaitGroup) {
	var toStart []domain.TaskState
	ex.st.View(func(tx *store.Tx) {
		for _, st := range tx.Tasks() {
			if st.Status != domain.StatusRunning {
				continue
			}
			if ex.isInflight(st.Task.ID) {
				continue // 幂等：同一任务不重复派发、不重复计入资源
			}
			toStart = append(toStart, st)
		}
	})
	for _, st := range toStart {
		ex.launch(wg, st)
	}
	ex.reapStale()
}

// launch 启动单个任务的执行协程。
// 任务 ctx 独立于执行器进程 ctx：只有任务自身超时、被取消或被抢占才会取消它，
// 从而保证"进程关闭"不会被误记为"任务取消/失败"。
func (ex *Executor) launch(wg *sync.WaitGroup, st domain.TaskState) {
	ctx, cancel := context.WithCancel(context.Background())
	if st.Task.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, st.Task.Timeout)
	}
	if !ex.tryRegister(st.Task.ID, cancel) {
		cancel() // 竞态失败：其他入口已派发，放弃本次
		return
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()
		defer ex.unregister(st.Task.ID)

		ex.logger.Printf("[exec] dispatch task=%s robot=%s priority=%d demand=%+v timeout=%s",
			st.Task.ID, st.Assigned, st.Task.Priority, st.Task.Demand, timeoutText(st.Task.Timeout))
		err := ex.run(ctx, st.Task)
		if ex.stopping.Load() {
			ex.logger.Printf("[exec] task=%s result discarded: executor shutting down (state remains RUNNING for recovery)", st.Task.ID)
			return
		}
		ex.finish(st.Task.ID, st.Assigned, err, ctx.Err())
	}()
}

// finish 在单一临界区内回写执行结果并释放占用。
// 取消/抢占导致的停止不写 FAILED；超时与执行错误给出可区分原因。
func (ex *Executor) finish(taskID, robotID string, runErr, ctxErr error) {
	commitErr := ex.st.Commit(func(tx *store.Tx) error {
		// 临界区内复查关闭标志：关闭瞬间被取消的在途任务结果不落盘，
		// 保留 RUNNING 供重启恢复，避免被误记为失败。
		if ex.stopping.Load() {
			return nil
		}
		cur, ok := tx.Task(taskID)
		if !ok {
			return nil // 任务已不存在，无需处理
		}
		// 任务已被调度器取消或抢占：执行结果一律不覆盖，占用此前已释放。
		if cur.Status != domain.StatusRunning || cur.Assigned != robotID {
			tx.Emit(domain.EventReady, taskID, robotID,
				fmt.Sprintf("execution stopped because task is now %s (resources already released)", cur.Status), "")
			return nil
		}
		now := time.Now().UTC()
		switch {
		case errors.Is(ctxErr, context.DeadlineExceeded):
			cur.Status = domain.StatusFailed
			cur.Assigned = ""
			cur.Reason = domain.ReasonTimeout
			cur.Message = fmt.Sprintf("execution timed out: %v", runErr)
			cur.UpdatedAt = now
			tx.PutTask(cur)
			tx.Emit(domain.EventFailed, taskID, robotID, cur.Message, domain.ReasonTimeout)
		case ctxErr != nil || runErr != nil:
			// 主动取消（context.Canceled）只有在任务仍 RUNNING 时才记失败，
			// 而正常取消/抢占已在上面分支被拦截；此处表示执行被异常中断。
			cur.Status = domain.StatusFailed
			cur.Assigned = ""
			cur.Reason = domain.ReasonExecutionFailed
			cur.Message = fmt.Sprintf("execution failed: runErr=%v ctxErr=%v", runErr, ctxErr)
			cur.UpdatedAt = now
			tx.PutTask(cur)
			tx.Emit(domain.EventFailed, taskID, robotID, cur.Message, domain.ReasonExecutionFailed)
		default:
			cur.Status = domain.StatusSucceeded
			cur.Assigned = ""
			cur.UpdatedAt = now
			tx.PutTask(cur)
			tx.Emit(domain.EventSucceeded, taskID, robotID, "execution succeeded; resources released", "")
		}
		return nil
	})
	if commitErr != nil {
		ex.logger.Printf("[exec] finish task=%s commit error: %v", taskID, commitErr)
	}
}

// reapStale 清理已不在 RUNNING（被取消/抢占）任务的执行协程。
func (ex *Executor) reapStale() {
	ex.mu.Lock()
	ids := make([]string, 0, len(ex.inflight))
	for id := range ex.inflight {
		ids = append(ids, id)
	}
	ex.mu.Unlock()

	for _, id := range ids {
		var status domain.TaskStatus
		var assigned string
		ex.st.View(func(tx *store.Tx) {
			if st, ok := tx.Task(id); ok {
				status, assigned = st.Status, st.Assigned
			}
		})
		if status == domain.StatusRunning && ex.isInflight(id) {
			continue
		}
		ex.logger.Printf("[exec] task=%s left RUNNING (status=%s assigned=%q), canceling execution goroutine", id, status, assigned)
		ex.cancelInflight(id)
	}
}

func (ex *Executor) tryRegister(id string, cancel context.CancelFunc) bool {
	ex.mu.Lock()
	defer ex.mu.Unlock()
	if _, ok := ex.inflight[id]; ok {
		return false
	}
	ex.inflight[id] = cancel
	return true
}

func (ex *Executor) unregister(id string) {
	ex.mu.Lock()
	defer ex.mu.Unlock()
	delete(ex.inflight, id)
}

func (ex *Executor) isInflight(id string) bool {
	ex.mu.Lock()
	defer ex.mu.Unlock()
	_, ok := ex.inflight[id]
	return ok
}

func (ex *Executor) cancelInflight(id string) {
	ex.mu.Lock()
	cancel := ex.inflight[id]
	ex.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (ex *Executor) cancelAll() {
	ex.mu.Lock()
	defer ex.mu.Unlock()
	for _, cancel := range ex.inflight {
		cancel()
	}
}

func timeoutText(d time.Duration) string {
	if d <= 0 {
		return "none"
	}
	return d.String()
}
