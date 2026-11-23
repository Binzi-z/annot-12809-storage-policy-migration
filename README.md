# 多租户存储访问策略迁移工具（本机模拟）

一个只依赖 Go 标准库、完全运行在本地进程内的策略迁移工具。旧策略（V1）按
**路径前缀 + 角色作用域**授权；新策略（V2）在此之上增加**资源版本**与
**撤销世代**。工具支持两个写入端长期并行、旧端不停机的灰度迁移，并把
“不扩大权限、撤销不可恢复、故障可区分、过程可审计”做成可测试的硬不变量。

## 运行方式

```bash
# 全部单元测试 + 差分测试 + 端到端场景
go test ./... -count=1

# 带竞态检测（需要 CGO 与 C 工具链：CGO_ENABLED=1 且 PATH 中有 gcc）
go test ./... -race -count=1

# 只跑端到端演示（六个场景，带详细中文输出）
go test ./internal/demo/ -run TestAllScenariosPass -v

# 编译并运行 CLI 演示（数据落在 ./.migrator-data，可重复执行）
go run ./cmd/migrator
go run ./cmd/migrator --data-dir ./tmp/demo --workers 4 --max-attempts 7
```

不访问网络、不依赖外部数据库、不需要 CGO。所有租户与数据均为进程内模拟。

## 目录结构

```
internal/storage/   嵌入式持久化：CRC 分帧 WAL + 原子快照 + 文件锁 + 故障注入
internal/policy/    V1/V2 策略模型、交集闸门、三态错误分类
internal/migrate/   迁移状态机、事件溯源协调器、哈希链审计、有界重试调度器、
                    纯函数参考模型（差分测试的 oracle）
internal/tenantsim/ 模拟租户、双写入端、过期副本追赶
internal/demo/      六个端到端场景（同时是 go test 用例）
cmd/migrator/       薄 CLI 封装
```

## 状态机参考模型

每个资源一条独立状态机，转移表是唯一事实来源（`internal/migrate/state.go`），
真实协调器与纯函数参考模型共用同一张表：

```
LEGACY ──► DUAL_WRITING ──► CHECK ──► V2_PRIMARY ──► MIGRATED
              │  ▲             │  ▲         │  ▲
              └──┘             └──┘         └──┘
              （回滚边：V2_PRIMARY→CHECK→DUAL_WRITING→LEGACY；
                MIGRATED 可退 V2_PRIMARY；FAILED 可人工恢复到 DUAL_WRITING）
   任意状态 ──► FAILED（重试预算耗尽，写冻结、读按 V1）
   任意状态 ──► QUARANTINED（数据损坏，终态，全部访问 fail-closed）
```

## 迁移期安全规则（明确、无隐式默认）

| 资源状态 | 读 | 写 |
|---|---|---|
| LEGACY（未迁移） | 仅 V1 判定 | 仅 V1 判定 |
| DUAL_WRITING / CHECK / V2_PRIMARY | **交集闸门**：V1 与 V2 同时允许才放行；水位未追平报 NotConverged | 同左，另加 V2 版本检查 |
| MIGRATED | 仅 V2（含撤销世代/版本） | 仅 V2 |
| FAILED | 按 V1 读；V1/V2 结论冲突时报 NotConverged | 一律 NotConverged（冻结） |
| QUARANTINED | 一律 Corrupted | 一律 Corrupted |

三类结果是不同的错误类型，调用方用 `policy.IsDenied / IsNotConverged /
IsCorrupted` 区分，不允许合并成一个“拒绝”：

- **普通拒绝 ErrDenied**：状态一致，策略不允许（含乐观版本冲突、规则已撤销）；
- **状态未收敛 ErrNotConverged**：双写水位落后等暂时态，fail-closed 但可重试；
- **数据损坏 ErrCorrupted**：WAL/快照/哈希链校验失败，资源进入隔离。

## 关键不变量及其实现位置

1. **任何时刻不扩大有效权限** —— 迁移期决策取 V1∩V2
   （`internal/policy/decide.go` 的 `decideIntersection`），单侧拒绝即拒绝。
2. **撤销后不能经回滚恢复** —— 撤销世代是只增不减的“世代地板”；回滚事件
   必须携带当前地板，投影 apply 拒绝任何移动地板的回滚
   （`internal/migrate/projection.go` 的 `evRollback` 分支）。
3. **撤销后不能经副本追赶恢复** —— 快照导入只接受更高的地板/水位
   （`evSnapshotImported`）；模拟 V2 写入端直接拒绝低于地板的规则
   （`tenantsim.Environment.V2Write`）。
4. **强杀不丢已提交数据、不应用未提交数据** —— WAL 批量以 commit 位原子
   提交并 fsync；撕裂尾部在 Open 时隔离到 `*.torn` 并截断
   （`storage/store.go` 的 `recover`）。
5. **损坏可检测、可区分、可抢救** —— 帧级 CRC + 事件级 SHA-256 哈希链；
   已提交区域中间损坏拒绝普通启动，`WithSalvage()` 抢救损坏点之前的前缀，
   之后状态不明的资源显式隔离而不是静默丢弃。
6. **热点重试有界且不阻塞他人** —— 每资源串行 + 固定 worker 池；退避在
   独立定时器 goroutine 中等待，worker 立即归还池
   （`internal/migrate/runner.go`）。重试次数硬上界、退避指数增长并封顶，
   预算耗尽原子转入 FAILED。

## 差分测试方法

`internal/migrate/diff_test.go` 向**纯函数参考模型**与**真实 WAL 协调器**
喂同一条操作序列（双写、镜像、排空、快照导入、世代轮换、回滚、有界重试），
每一步比较：

1. 操作成败是否一致；
2. 全部资源的六个字段（状态、尝试数、世代地板、待镜像数、两条水位）是否逐字段相同。

此外在固定步骤注入“强杀 + 从磁盘重建”，以及在每轮注入撕裂尾部，
要求重建后与参考模型仍完全一致、哈希链仍可验证。

## 六个演示场景

1. 热点资源与正常资源并行迁移：热点恰好尝试 `maxAttempts` 次后 FAILED，
   正常资源首次尝试即成功，二者互不阻塞；
2. 交集闸门：未收敛报 NotConverged；V2 单方面收窄后 V1 仍允许也必须拒绝；
3. 撤销世代：回滚与过期副本追赶两条路径都无法恢复访问；
4. 三态区分：同一进程内依次演示 Denied / NotConverged / Corrupted；
5. 强杀重放：kill 后仅凭 WAL + 快照重建出逐字段相同的状态；
6. 哈希链审计与快照导入：链校验通过，低地板导入被忽略、高地板生效。

## 持久化文件布局

```
<data-dir>/
  wal.log                 CRC 分帧追加日志（帧头 20B：magic/flags/seq/len/crc）
  wal.log.torn*           崩溃隔离出来的撕裂尾部（审计凭证，不参与重放）
  lock                    跨进程独占锁（Unix flock / Windows LockFileEx）
  snapshots/latest.bin    原子发布的带 CRC 快照（tmp+fsync+rename）
```
