# 本地多机器人任务调度与执行编排（Go）

一套纯本地（无外部中间件）的多机器人任务调度与执行编排原型。用 **JSON 文件原子落盘**
模拟任务队列、机器人与执行结果的持久化；用 **单把互斥锁的事务临界区** 保证多入口
并发竞争资源时结论一致；用 **可区分的失败原因 + 事件流** 让每一次拒绝、抢占、
取消、恢复都可解释。

## 目录结构

```
internal/domain/    核心领域类型：任务、机器人、资源、状态、失败原因、事件
internal/store/     本地存储：互斥事务、资源核算、快照原子落盘（tmp+rename）
internal/scheduler/ 调度器：准入校验、循环依赖检测、依赖传播、best-fit 分配、优先级抢占
internal/executor/  执行器：幂等轮询派发、超时/取消/抢占感知、重启恢复
cmd/demo/           可运行演示：拒绝原因、依赖链、抢占与重排
```

## 运行与验证

```bash
# 编译
go build ./...

# 运行端到端演示（自动输出各类拒绝原因、抢占过程与最终状态）
go run ./cmd/demo

# 全量单测（含竞态检测），日志会打印每个用例的输入与判定依据
go test ./... -race -count=1 -v
```

演示与单测的关键日志形如：

```
INPUT Submit task="selfloop" deps=[selfloop] ...
JUDGE rejection reason="CYCLE_DEPENDENCY" message="task selfloop introduces a dependency cycle: [selfloop selfloop]"
INPUT 50 goroutines concurrently calling Tick ...
JUDGE running=4 used={CPU:8 ...} robot-capacity={CPU:8}
INPUT tick that preempts low in favor of high
JUDGE low preserved as PREEMPTED: "preempted by higher-priority task high ..."
```

## 调度规则

### 1. 准入校验（`Scheduler.Submit`，失败立即返回 `*domain.TaskError`）

| 检查 | 失败原因 `FailureReason` | 说明 |
| --- | --- | --- |
| 任务 ID 重复 | `DUPLICATE_TASK` | 幂等重提也明确拒绝，避免静默覆盖定义 |
| 引入依赖环 | `CYCLE_DEPENDENCY` | DFS 三色标记，错误信息带环路径（支持自环） |
| 依赖任务不存在 | `TASK_NOT_FOUND` | 依赖必须指向已提交任务 |
| 需求超过所有机器人容量 / 无机器人 / 需求为负 | `INSUFFICIENT_CAPACITY` | 永远无法满足的需求，不得排队静默等待 |

被拒绝的任务 **不会写入存储**，也不会出现在后续任何调度轮次中。

### 2. 依赖传播（每轮 `Tick`）

- 所有依赖 `SUCCEEDED` → `PENDING`/`PREEMPTED` 转为 `READY`；
- 任一依赖 `FAILED` 或 `CANCELED` → 本任务立即 `FAILED`，原因 `DEPENDENCY_FAILED`，
  并携带是哪个依赖导致；级联向下游传播，直到图稳定；
- 其余情况保持等待（依赖仍在途），这是 **唯一合法的"等待"**。

此外每轮 `Tick` 会对存量任务图做一次环检测作为深度防御：即使持久化快照被外部
篡改成含环（多节点环无法通过 `Submit` 产生），环上任务也会被明确判为
`CYCLE_DEPENDENCY`，而不是永远 `PENDING`。

### 3. 资源分配

- `READY` 队列排序：**优先级数值大者优先；同级按任务 ID 升序**（确定性行为，可解释）。
- 机器人选择：**best-fit**——在"剩余资源能容纳需求"的机器人中，选剩余总量最小者，
  尽量为后来的大任务保留整块容量。
- 多维资源（`CPU / Power / Payload`）逐维比较，任一维不足即放不下。
- 暂时无容量的任务保留 `READY` 等下一轮，**不会静默失败，也不会被强行调度**；
  事件说明中会带上该任务优先级在各机器人上的可用池，便于核对是否被预留挡住。
- 资源占用只统计 `RUNNING` 任务；`Used`/`Available` 在事务临界区内计算，
  一旦出现负的可用量会直接 panic（不变量破坏不静默吞掉）。

### 3.1 预留容量（按资源维度）

每台机器人可以按维度划出一部分容量作为 **预留区**，只供高优先级任务使用：

```go
sc.AddRobot(domain.Robot{
    ID:                 "R2",
    Capacity:           domain.Resources{CPU: 4, Power: 2}, // 总容量
    Reserved:           domain.Resources{CPU: 2},           // 预留区（逐维度）
    ReserveMinPriority: 5,                                  // 动用预留区的优先级门槛（含）
})
```

- **公共池 = Capacity − Reserved（逐维）**。优先级低于 `ReserveMinPriority` 的任务
  只能使用公共池：判定口径为 `min(整体剩余, 公共池容量 − 未达门槛任务的占用)`，
  每一维独立核算，任何一维都算不出负数。
- 达到门槛的任务可以使用 **公共池 + 预留区**（即整体容量）。
- 预留配置本身会被校验：`Reserved` 任一维为负、或超过 `Capacity` 对应维度时，
  `AddRobot` 明确返回 `INSUFFICIENT_CAPACITY` 错误，绝不让公共容量被悄悄算成负数。
- **准入校验同步生效**：未达门槛、且需求超过公共容量的任务在 `Submit` 时就被
  `INSUFFICIENT_CAPACITY` 拒绝（它永远排不上队）；同样的需求若优先级达到门槛
  则可正常接纳。
- **与抢占的配合**：关键任务到来时先在"公共+预留"里做 best-fit，预留区够就
  **直接吃预留区、不打断任何在途任务**；只有连预留区也不够时，才走第 4 节的
  抢占规则（被抢占者仍保留为可解释、可恢复的 `PREEMPTED`）。未达门槛的任务之间
  抢占规则不变，但释放出来的也只是公共池容量，抢不出预留区。
- **兼容表现**：不配置预留（`Reserved` 为零值）时，所有判定路径与旧版本完全一致。
- **持久化与恢复**：预留配置随 `Robot` 一起进快照；占用量永远从 `RUNNING` 任务
  现算，因此重启后"预留配置 + 中断瞬间的占用"自然对得上，终态任务不会占回来，
  预留区也不会因为并发调度或快照恢复被突破。

### 4. 优先级抢占

当某任务在所有机器人上都放不下时，允许在 **单台机器人内部** 抢占：

- 只有当该机器人上的占用者优先级 **严格低于** 新任务时才可抢占，同级/更高一律不抢；
- 按最低优先级依次选取被抢占任务，直到释放量足以容纳新任务；
- 被抢占任务 **不会静默丢失**：落为 `PREEMPTED`（带"被谁抢占、在哪台机器人、
  资源已释放"的说明消息与 `PREEMPTED` 事件），下一轮 `Tick` 自动回到 `READY`
  重新参与竞争；
- 抢占在同一个事务内完成（释放旧占用 + 建立新占用 + 事件），因此不会出现
  资源重复计入或瞬时超卖；执行器轮询发现任务已离开 `RUNNING` 会取消其执行协程，
  协程迟到的执行结果不会覆盖抢占/取消状态。

### 5. 并发与幂等

- 所有状态读写都经过 `Store.Commit`（单把 `sync.Mutex` 串行化）+ 原子落盘；
  因此任意数量的调度入口并发调用 `Submit/Tick/Cancel`，结论等价于某种串行顺序。
- 同一任务在一个执行器进程内由 `inflight` 登记表保护，**只派发一次**；
  `RUNNING` 状态 + 事务分配双重保证同一任务不会被重复计入资源占用。
- `Commit` 回调返回错误时整体回滚（内存改动与文件落盘都不发生）。
- `Cancel` 对终态任务是幂等空操作；对未知任务返回 `TASK_NOT_FOUND`。

### 6. 取消与超时的资源释放

- 取消：任务立即 `CANCELED`、清空 `Assigned`，占用在同一事务内完整释放；
  执行器在下一次轮询（默认 5ms）取消其执行协程。
- 超时：`Task.Timeout` 通过每任务独立的 `context.WithTimeout` 生效，
  结束状态为 `FAILED` + 原因 `TIMEOUT`，与普通执行错误 `EXECUTION_FAILED` 可区分。
- 执行协程观察到取消后回写时会重新读取状态：若任务已是 `CANCELED`/`PREEMPTED`，
  执行结果被丢弃，绝不把已释放的资源"加回来"造成负值。

### 7. 故障恢复

- 每次成功的事务都把全量快照以 **写临时文件 + rename** 的方式原子落盘（JSON）。
- 执行器的任务 ctx 与进程生命周期 ctx **相互独立**：进程关闭时在途任务的结果
  一律不回写（等价于真实断电来不及落盘），它们在磁盘上保留为 `RUNNING`。
- 重启后 `Store.Load` 读取快照，`Executor.Recover` 给出分类结论：
  - `SUCCEEDED / FAILED / CANCELED`：终态，**绝不重新派发**；
  - `RUNNING`：中断时在途，作为待续任务重新派发（假设执行函数自身幂等，
    语义为 at-least-once）；
  - `PENDING / READY / PREEMPTED`：等待后续 `Tick` 正常调度。

## 任务状态迁移

```
                Submit(通过校验)
  (不存在) ───────────────────────▶ PENDING
                                      │  全部依赖 SUCCEEDED
                                      ▼
        ┌──────────── 抢占后下一轮 ── READY ◀──────────────┐
        │                            │  Tick 分配到机器人   │
        │                            ▼                      │
        └──────────────────────── RUNNING ───── 被高优先级抢占 ─▶ PREEMPTED
          执行成功                │     │  │
                                  │     │  └─ 超时 Timeout ─▶ FAILED (TIMEOUT)
                                  │     ├─ run 返回错误 ────▶ FAILED (EXECUTION_FAILED)
                                  │     └─ Cancel 请求 ─────▶ CANCELED
                                  ▼
                              SUCCEEDED

  PENDING/READY/PREEMPTED ──依赖 FAILED/CANCELED──▶ FAILED (DEPENDENCY_FAILED)
  任意非终态 ────────────────────────────────────▶ CANCELED（Cancel 请求）
  任意非终态（含存量环）─────────────────────────▶ FAILED (CYCLE_DEPENDENCY)
```

终态：`SUCCEEDED / FAILED / CANCELED`（不会再被派发；`PREEMPTED` 不是终态）。

## 失败原因速查

| Reason | 触发位置 | 含义 |
| --- | --- | --- |
| `CYCLE_DEPENDENCY` | Submit / Tick | 依赖图存在环 |
| `DEPENDENCY_FAILED` | Tick 传播 | 依赖失败或被取消 |
| `INSUFFICIENT_CAPACITY` | Submit | 容量为负、无机器人或超出全部机器人容量 |
| `DUPLICATE_TASK` | Submit | 任务 ID 已存在 |
| `TASK_NOT_FOUND` | Submit / Cancel | 依赖或取消目标不存在 |
| `EXECUTION_FAILED` | Executor | run 函数返回错误 / 被异常中断 |
| `TIMEOUT` | Executor | 执行超过任务 Timeout |

## 测试覆盖

| 场景 | 用例 |
| --- | --- |
| 循环依赖（提交期自环 + 存量多节点环） | `TestCycleDependency`、`TestExistingCycleFailsExplicitly` |
| 重复 ID / 未知依赖 / 容量不足原因可区分 | `TestAdmissionReasons` |
| 依赖失败级联、不得静默等待 | `TestDependencyFailureCascades` |
| 并发多入口不重复派发、不重复计占用 | `TestConcurrentDispatchNoDuplicate`、`TestConcurrentEntriesConsistent` |
| 抢占保留可解释状态、同级不抢占、完成后重排 | `TestPreemptionKeepsVictimExplainable`、`TestNoPreemptionAcrossSameOrHigherPriority`、`TestPreemptionInterruptsExecution` |
| 取消/超时释放完整、可用资源非负 | `TestCancelReleasesResources`、`TestSuccessFailureTimeout` |
| 中断重启恢复、终态不重派 | `TestRecoveryAfterRestart` |
| 预留区只对达门槛任务开放、普通任务够不到 | `TestReservedPoolOnlyForEligiblePriority`、`TestCommonTaskCannotTouchReserved` |
| 预留按维度核算、多维不超配、剩余不为负 | `TestMultiDimReservationNoOvercommit` |
| 并发调度下预留不被突破、不重复派发 | `TestConcurrentDispatchReservedNotBreached` |
| 预留不足才回退抢占、被抢任务可恢复 | `TestReservedFallsBackToPreemption` |
| 重启后预留配置与占用一致、终态不占回 | `TestRecoveryKeepsReservationConsistent` |
| 非法预留配置（为负/超容量）明确拒绝 | `TestInvalidReservationRejected` |
| 持久化原子写、回滚、资源核算 | `internal/store` 下全部用例 |
