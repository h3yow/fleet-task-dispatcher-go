# Fleet Task Dispatcher（本地多机器人任务调度与执行编排）

用本地文件存储模拟任务队列、机器人容量与执行结果，演示一套在**多入口并发**、
**中断重启**、**优先级抢占**下结论一致且可解释的任务调度内核。无外部依赖，
仅用 Go 标准库实现。

## 目录结构

| 路径 | 职责 |
| --- | --- |
| `internal/domain/types.go` | 任务/机器人/状态/失败原因/判定记录等核心领域模型 |
| `internal/storage/store.go` | `Store` 接口与 `FileStore`：JSON 全量快照（原子 rename）+ JSONL 判定流水 |
| `internal/scheduler/scheduler.go` | 调度内核：环检测、依赖传播、容量分配、抢占、幂等回传、重启恢复、不变量守卫 |
| `internal/scheduler/*_test.go` | 覆盖环依赖、并发重复派发、中断恢复、抢占等场景 |
| `cmd/demo/main.go` | 可运行的端到端演示 |

## 核心模型

- **任务 `Task`**：`Priority`（数值越大优先级越高）、`Demand`（需要占用的容量单位）、
  `Dependencies`（必须先 `SUCCEEDED` 的任务 ID 列表）、`Status`、`AssignedTo`、
  `FailReason/FailDetail`、`Version`（每次迁移 +1）。
- **机器人 `Robot`**：`Capacity`（总容量）、`Used`（已占用），任何时刻保证
  `0 <= Used <= Capacity`，`Free() = Capacity - Used` 永不为负。
- 允许“前向引用”：提交任务时其依赖可以尚未登记；该任务保持 `PENDING` 等待。

## 调度规则

`DispatchOnce()` 在**单一互斥锁**内串行执行“判定 → 状态迁移 → 同步落盘”，多趟推进直到无进展：

1. **固定点传播拒绝（不静默等待、不强行调度）**
   - 依赖图中位于环上、或依赖链可达环的任务 → `FAILED / CYCLE_DEPENDENCY`，
     判定详情附环路径；自依赖在提交期直接拒绝（`ErrCycleDependency`）。
   - 依赖了 `FAILED`/`CANCELED` 任务的任务 → `FAILED / DEPENDENCY_FAILED`，
     固定点迭代向下游传播。`PREEMPTED` 不具传染性（它仍可成功）。
2. **就绪排序**：所有依赖均为 `SUCCEEDED` 的 `PENDING/PREEMPTED` 任务，按
   优先级降序、创建时间升序、ID 升序排序（结论确定、可复现）。
3. **容量分配**
   - 有机器人空闲容量足够时，按 best-fit（够用且剩余最小）派发，计入占用。
   - `Demand` 大于任何一台机器人容量 → `FAILED / INSUFFICIENT_CAPACITY`
     （物理上永不可行，终态拒绝）。
   - 暂时无容量但未来可行（等在跑任务释放）→ 任务**保持排队**，并写一条
     `REJECT / INSUFFICIENT_CAPACITY` 判定记录说明“暂时等待”，可解释但不误杀。
4. **优先级抢占**
   - 仅可抢占**严格更低优先级**的在跑任务；同优先级/更高优先级占用不可抢占。
   - 在每台装得下的机器人上，按“优先级升序、启动时间升序”挑选最少数量的牺牲者，
     直至腾出足够容量；平局按机器人 ID 确定。
   - 被抢占任务转为 `PREEMPTED`（`FailReason=PREEMPTED_BY_HIGHER_PRIORITY`，
     附抢占者 ID），**保留而不是静默丢失**，容量释放后在后续轮次重新派发。

## 任务状态迁移

```
                 提交
                  │
                  ▼
              ┌────────┐  依赖全部成功且有容量   ┌─────────┐
              │PENDING │ ─────────────────────▶ │ RUNNING │
              └────────┘                        └─────────┘
                  │ 环/依赖失败传播               │  │  │  │
                  ▼                               │  │  │  └─超时(MarkTimeout)─┐
              ┌────────┐                          │  │  └─取消(Cancel)────────┤
              │ FAILED │◀ 物理容量不足             │  └─失败(Fail)────────────┤
              └────────┘                          ▼                          ▼
                                             ┌───────────┐             ┌──────────┐
        ┌─────────────────────────────────── │PREEMPTED  │             │ CANCELED │
        │ 高优先级抢占 / 重启时发现RUNNING     └───────────┘             └──────────┘
        │                                     容量释放后重新进入调度
        ▼
   （下一轮 DispatchOnce 重新尝试，可能再次 RUNNING / 被抢占 / 成功）

 RUNNING ──成功(Complete)──────────────▶ SUCCEEDED
```

- 终态：`SUCCEEDED`、`FAILED`、`CANCELED`，不再参与调度，**重启后绝不重新派发**。
- `PREEMPTED` 是可解释的中间态：被高优先级抢占，或重启时发现任务还在 `RUNNING`
  （执行现场随进程消失），等待重新调度。

## 失败原因（可区分）

| `domain.FailReason` | 含义 |
| --- | --- |
| `CYCLE_DEPENDENCY` | 依赖图成环（含自依赖）或依赖链可达环 |
| `DEPENDENCY_FAILED` | 前置任务已 `FAILED`/`CANCELED`，永不可能满足 |
| `INSUFFICIENT_CAPACITY` | 物理上任何机器人都装不下（终态）；同一条目也用于记录“暂时排队”的判定（任务不进终态，看 `TaskStatus` 区分） |
| `PRECONDITION_NOT_MET` | 对非 RUNNING 任务回传结果，被幂等忽略 |
| `TIMEOUT`（取消任务的 FailReason） | 执行超时，占用完整释放 |
| `PREEMPTED_BY_HIGHER_PRIORITY` | 被高优先级任务抢占，保留待重派 |
| `RECOVERED_AFTER_RESTART` | 重启时发现任务处于 RUNNING，转为 PREEMPTED |

## 并发与幂等语义

- 全部公开方法持同一把互斥锁；“判定 + 迁移 + 持久化”在锁内一次完成，因此：
  - 并发调度入口下，**同一任务不会被重复派发**，占用不会重复计入；
  - 机器人占用恒等于其上 RUNNING 任务 `Demand` 之和（`CheckInvariants()` 可随时校验）；
  - 取消/超时/失败的占用按实际登记量释放，**可用容量永不出现负值**。
- `Complete/Fail/Cancel/MarkTimeout` 均**幂等**：任务已非 RUNNING 时重复回传为
  no-op，写一条 `*_IDEMPOTENT_SKIP` 判定记录，不动容量、不改终态结论。
- 持久化：状态用“写临时文件 + rename”原子替换；判定流水为 JSONL 追加。
  `persistLocked()` 在落盘前强制做不变量校验，拒绝写入破坏约束的状态。

## 中断恢复语义

重新 `New(store)` 打开同一存储时：

1. 以 RUNNING 任务为唯一依据**重算**每台机器人 `Used`（杜绝占用凭空多计/少计）；
2. 快照中的 RUNNING 任务（执行现场已丢失）一律转为 `PREEMPTED`，原因
   `RECOVERED_AFTER_RESTART`，释放名义占用，等待重新调度；
3. `SUCCEEDED/FAILED/CANCELED` 保持终态，绝不重新派发；
4. 恢复动作本身写入 `RECOVER` 判定流水，`RecoveredTaskCount()` 暴露本次恢复数量。

## 验证方法

```bash
# 全量单元测试（建议开竞态检测并多跑几轮）
go test ./... -race -count=3

# 仅看四类关键场景
go test ./internal/scheduler/ -run 'TestCycle' -v          # 循环依赖
go test ./internal/scheduler/ -run 'TestConcurrent' -race -v  # 并发重复派发/重复回传
go test ./internal/scheduler/ -run 'TestRecovery' -v       # 中断恢复（真实文件存储）
go test ./internal/scheduler/ -run 'TestPreempt' -v        # 优先级抢占

# 端到端演示
go run ./cmd/demo            # 首轮：播种 -> 抢占 -> 完成 -> 被抢占任务重派
go run ./cmd/demo            # 不删 data 再跑：观察 RUNNING -> PREEMPTED 恢复
rm -rf data                  # 重置演示数据
```

测试均在日志中打印**输入**与**判定依据**（`t.Logf` + 内存判定流水），
判定流水格式示例：

```
[15:04:05.000] DISPATCH task=t1 robot=r1 p=1 demand=2 deps=map[] :: all dependencies succeeded and robot has enough free capacity
[15:04:05.010] REJECT   task=c  robot=-  p=0 demand=1 reason=CYCLE_DEPENDENCY :: dependency graph contains a cycle reachable from c: [c b a]
[15:04:05.020] PREEMPT  task=low robot=r1 p=1 demand=5 reason=PREEMPTED_BY_HIGHER_PRIORITY :: capacity reclaimed for task "high" ...
```

## 设计边界（有意的简化）

- 单进程、单互斥锁串行化判定；这是“本地模拟”，不涉及分布式锁/网络协调。
- 容量为单维度整数；不建模任务执行时长（超时由外部调用 `MarkTimeout` 驱动）。
- 调度是“轮次触发”模型：完成/取消后需要再次调用 `DispatchOnce()`（或由调用方循环驱动）。
