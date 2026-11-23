package migrate

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"multitenantmigration/internal/storage"
)

// 默认重试参数：预算与退避都有硬上界，热点资源不可能无限重试，
// 也不可能让单个资源长期霸占调度器（调度隔离见 runner.go）。
const (
	DefaultMaxAttempts = 5
	DefaultBaseBackoff = 10 // 毫秒
	DefaultMaxBackoff  = 1000
)

// Coordinator 是迁移工具的核心：事件溯源 + 投影 + 审计链。
// 所有变更都通过 commit 走 WAL 原子批量；进程重启后仅靠 WAL/快照即可重建。
type Coordinator struct {
	mu       sync.Mutex
	store    *storage.Store
	proj     *Projection
	readOnly bool // salvage 抢救模式
	maxRetry int
}

// Open 打开或恢复一个协调器。恢复顺序：先快照，再续放快照之后的 WAL；
// 哈希链在续放处必须严丝合缝，否则按损坏处理。
func Open(store *storage.Store) (*Coordinator, error) {
	c := &Coordinator{store: store, proj: newProjection(), maxRetry: DefaultMaxAttempts}
	if salv, _ := store.Salvage(); salv {
		c.readOnly = true
	}

	// 1) 快照（损坏则忽略并全量重放 WAL——快照只是加速器，不是唯一真相）。
	var afterSeq uint64
	if walSeq, payload, err := store.LoadSnapshot(); err == nil {
		var doc SnapshotDocument
		if jerr := json.Unmarshal(payload, &doc); jerr != nil {
			return nil, fmt.Errorf("%w: snapshot decode: %v", storage.ErrCorruption, jerr)
		}
		if doc.ChainTip != "" {
			c.proj.loadSnapshot(doc)
			afterSeq = doc.LastSeq
			_ = walSeq
		}
	} else if storage.IsCorruption(err) {
		// 快照损坏：丢弃快照，全量重放 WAL。
		c.proj = newProjection()
	} else if !isEOFMissing(err) {
		return nil, err
	} else {
		// 快照不存在：空投影起步。
	}

	// 2) 续放 WAL。抢救模式只取损坏点之前的前缀。
	if c.readOnly {
		if _, err := store.ReplayPrefix(func(_ uint64, batch [][]byte) error {
			return c.applyBatch(batch, afterSeq)
		}); err != nil && !storage.IsCorruption(err) {
			return nil, err
		}
	} else {
		if err := store.Replay(func(_ uint64, batch [][]byte) error {
			return c.applyBatch(batch, afterSeq)
		}); err != nil {
			return nil, err
		}
	}
	return c, nil
}

func isEOFMissing(err error) bool {
	// storage.LoadSnapshot 在快照缺失时返回 io.EOF。
	return errors.Is(err, io.EOF)
}

// applyBatch 在恢复路径上校验并折叠一个 WAL 批量。
func (c *Coordinator) applyBatch(batch [][]byte, afterSeq uint64) error {
	for _, raw := range batch {
		env, err := decodeEnvelope(raw)
		if err != nil {
			return fmt.Errorf("%w: envelope decode: %v", storage.ErrCorruption, err)
		}
		if env.Seq <= afterSeq {
			continue
		}
		c.proj.mu.RLock()
		tip := c.proj.chainTip
		wantSeq := c.proj.lastSeq + 1
		c.proj.mu.RUnlock()
		if env.PrevHash != tip {
			return fmt.Errorf("%w: hash chain discontinuity at seq %d", storage.ErrCorruption, env.Seq)
		}
		if sha256Sum(env) != env.Hash {
			return fmt.Errorf("%w: envelope hash mismatch at seq %d", storage.ErrCorruption, env.Seq)
		}
		if env.Seq != wantSeq {
			return fmt.Errorf("%w: event sequence gap want %d got %d", storage.ErrCorruption, wantSeq, env.Seq)
		}
		if err := c.proj.Apply(env); err != nil {
			return fmt.Errorf("%w: apply: %v", storage.ErrCorruption, err)
		}
	}
	return nil
}

// commit 在一把大锁内封签事件、原子写 WAL 并折叠投影。WAL 成功而进程
// 在折叠前被杀也没关系：重启时重放会得到完全相同的状态。
func (c *Coordinator) commit(kind string, payload any) (Envelope, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.readOnly {
		return Envelope{}, errors.New("migrate: coordinator is read-only (salvage mode); writes rejected")
	}
	seq := c.proj.lastSeq + 1
	env, err := seal(seq, kind, payload, c.proj.chainTip)
	if err != nil {
		return Envelope{}, err
	}
	frames, err := encodeBatch([]Envelope{env})
	if err != nil {
		return Envelope{}, err
	}
	if _, err := c.store.Append(frames); err != nil {
		return Envelope{}, err
	}
	if err := c.proj.Apply(env); err != nil {
		// 理论不可达：同一 apply 逻辑会在重放时再次拒绝；明确 fail-loud。
		return Envelope{}, fmt.Errorf("migrate: post-commit apply failed, restart required: %w", err)
	}
	return env, nil
}

// commit1 是只关心成败的提交便捷封装。
func (c *Coordinator) commit1(kind string, payload any) error {
	_, err := c.commit(kind, payload)
	return err
}

// commitBatch 原子提交多个事件（用于“状态转移+水位”这类必须同时生效的组合）。
func (c *Coordinator) commitBatch(items []batchItem) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.readOnly {
		return errors.New("migrate: coordinator is read-only (salvage mode); writes rejected")
	}
	envs := make([]Envelope, 0, len(items))
	prev := c.proj.chainTip
	seq := c.proj.lastSeq
	for _, it := range items {
		seq++
		env, err := seal(seq, it.kind, it.payload, prev)
		if err != nil {
			return err
		}
		envs = append(envs, env)
		prev = env.Hash
	}
	frames, err := encodeBatch(envs)
	if err != nil {
		return err
	}
	if _, err := c.store.Append(frames); err != nil {
		return err
	}
	for _, env := range envs {
		if err := c.proj.Apply(env); err != nil {
			return fmt.Errorf("migrate: post-commit apply failed, restart required: %w", err)
		}
	}
	return nil
}

type batchItem struct {
	kind    string
	payload any
}

// Close 关闭底层存储句柄。
func (c *Coordinator) Close() error { return c.store.Close() }

// View 暴露投影供策略引擎挂载（实现 policy.MigrationView）。
func (c *Coordinator) View() *Projection { return c.proj }

// ReadOnly 报告是否处于抢救模式。
func (c *Coordinator) ReadOnly() bool { return c.readOnly }

// ---- 迁移工作流操作 ----

// Register 注册一个待迁移资源，初始状态 LEGACY。
func (c *Coordinator) Register(k ResourceKey) error {
	if _, err := c.commit(evRegister, keyed(k)); err != nil {
		return err
	}
	return nil
}

// BeginMigration 走 LEGACY -> DUAL_WRITING，双版本写入端从此并行。
func (c *Coordinator) BeginMigration(k ResourceKey, reason string) error {
	r, ok := c.proj.Resource(k)
	if !ok {
		return fmt.Errorf("migrate: unknown resource %s", k)
	}
	if !CanTransition(r.State, StateDualWriting) {
		return fmt.Errorf("migrate: cannot begin from %s", r.State)
	}
	return c.commit1(evTransition, transitionPayload{
		resourceKeyed: keyed(k), From: r.State, To: StateDualWriting, Reason: reason,
	})
}

// advance 是 CHECK/V2_PRIMARY/MIGRATED 正向推进的统一入口。
func (c *Coordinator) advance(k ResourceKey, to State, reason string) error {
	r, ok := c.proj.Resource(k)
	if !ok {
		return fmt.Errorf("migrate: unknown resource %s", k)
	}
	if !CanTransition(r.State, to) {
		return fmt.Errorf("migrate: illegal advance %s->%s", r.State, to)
	}
	return c.commit1(evTransition, transitionPayload{
		resourceKeyed: keyed(k), From: r.State, To: to, Reason: reason,
	})
}

// BeginCheck 进入双写一致性校验阶段。
func (c *Coordinator) BeginCheck(k ResourceKey) error {
	return c.advance(k, StateCheck, "dual-write consistency check")
}

// PromoteV2 让 V2 成为写主端（仍保留交集闸门，支持回滚）。
func (c *Coordinator) PromoteV2(k ResourceKey) error {
	return c.advance(k, StateV2Primary, "v2 promoted to write primary")
}

// Complete 完成迁移，此后仅 V2 授权。
func (c *Coordinator) Complete(k ResourceKey) error {
	return c.advance(k, StateMigrated, "migration completed")
}

// LegacyWrite 记录旧写入端的一次写入。迁移开始后它会推高待镜像计数，
// 使该资源在 V2 追上之前显式处于“未收敛”。
func (c *Coordinator) LegacyWrite(k ResourceKey, detail string) error {
	r, ok := c.proj.Resource(k)
	if !ok {
		return fmt.Errorf("migrate: unknown resource %s", k)
	}
	return c.commit1(evLegacyWrite, legacyWritePayload{
		resourceKeyed: keyed(k),
		LegacySeq:     r.LastLegacySeq + 1,
		Detail:        detail,
	})
}

// MirrorApplied 记录 V2 端完成一次镜像追赶。
func (c *Coordinator) MirrorApplied(k ResourceKey) error {
	r, ok := c.proj.Resource(k)
	if !ok {
		return fmt.Errorf("migrate: unknown resource %s", k)
	}
	return c.commit1(evMirrorApplied, mirrorPayload{
		resourceKeyed: keyed(k),
		LegacySeq:     r.LastLegacySeq,
		MirrorSeq:     r.LastMirrorSeq + 1,
	})
}

// Drain 标记双写积压清空（CHECK 的前置条件之一）。
func (c *Coordinator) Drain(k ResourceKey) error {
	r, ok := c.proj.Resource(k)
	if !ok {
		return fmt.Errorf("migrate: unknown resource %s", k)
	}
	return c.commit1(evDualDrainComplete, drainPayload{
		resourceKeyed: keyed(k), Watermark: r.LastLegacySeq,
	})
}

// RotateRevocation 抬高撤销世代地板。地板只增不减，事件载荷同时记录
// 旧值与新值，任何试图通过回滚/副本追赶恢复旧世代的路径都会在重放时
// 被 apply 的严格递增检查拒绝。
func (c *Coordinator) RotateRevocation(k ResourceKey, newFloor uint64, reason string) error {
	r, ok := c.proj.Resource(k)
	if !ok {
		return fmt.Errorf("migrate: unknown resource %s", k)
	}
	if newFloor <= r.GenFloor {
		return fmt.Errorf("migrate: new generation floor %d must exceed current %d", newFloor, r.GenFloor)
	}
	return c.commit1(evRotate, rotatePayload{
		resourceKeyed: keyed(k),
		OldGenFloor:   r.GenFloor,
		NewGenFloor:   newFloor,
		Reason:        reason,
	})
}

// Rollback 沿状态机允许的边回退（如 V2_PRIMARY -> CHECK），但显式携带
// 当前世代地板；apply 会拒绝任何移动地板的回滚，撤销因此无法被回滚恢复。
func (c *Coordinator) Rollback(k ResourceKey, to State, reason string) error {
	r, ok := c.proj.Resource(k)
	if !ok {
		return fmt.Errorf("migrate: unknown resource %s", k)
	}
	if !CanTransition(r.State, to) {
		return fmt.Errorf("migrate: illegal rollback %s->%s", r.State, to)
	}
	return c.commit1(evRollback, rollbackPayload{
		resourceKeyed: keyed(k),
		From:          r.State,
		To:            to,
		GenFloor:      r.GenFloor,
		Reason:        reason,
	})
}

// ImportSnapshot 导入外部快照（例如另一套环境的基线）。导入不改变状态机
// 位置，只取“更高”的世代地板与水位——较低值被忽略，防止旧副本反向追赶。
func (c *Coordinator) ImportSnapshot(k ResourceKey, source string, genFloor, watermark uint64) error {
	if _, ok := c.proj.Resource(k); !ok {
		if err := c.Register(k); err != nil {
			return err
		}
	}
	return c.commit1(evSnapshotImported, snapshotPayload{
		resourceKeyed: keyed(k), Source: source,
		GenFloor: genFloor, Watermark: watermark,
	})
}

// RecordRetry 记录一次有界重试；attempt 达到预算时原子转入 FAILED。
// 返回 true 表示预算已耗尽。attempt 从 1 开始计数，永不超过 maxRetry；
// 资源已处于 FAILED 时短路返回，不再追加事件、不增长计数。
func (c *Coordinator) RecordRetry(k ResourceKey, attempt, backoffMs int, reason string) (bool, error) {
	r, ok := c.proj.Resource(k)
	if !ok {
		return false, fmt.Errorf("migrate: unknown resource %s", k)
	}
	if r.State == StateFailed {
		return true, nil
	}
	if attempt > c.maxRetry {
		attempt = c.maxRetry
	}
	items := []batchItem{{
		kind: evRetryScheduled,
		payload: retryPayload{
			resourceKeyed: keyed(k), Attempt: attempt, BackoffMs: backoffMs, Reason: reason,
		},
	}}
	if attempt >= c.maxRetry && CanTransition(r.State, StateFailed) {
		items = append(items, batchItem{
			kind: evRetryExhausted,
			payload: retryPayload{
				resourceKeyed: keyed(k), Attempt: attempt, BackoffMs: backoffMs,
				Reason: "retry budget exhausted: " + reason,
			},
		})
	}
	if err := c.commitBatch(items); err != nil {
		return false, err
	}
	last, _ := c.proj.Resource(k)
	return last.State == StateFailed, nil
}

// ResetAttempts 在某个迁移步骤成功后清零重试计数（留下审计事件）。
func (c *Coordinator) ResetAttempts(k ResourceKey) error {
	if r, ok := c.proj.Resource(k); ok && r.Attempts == 0 {
		return nil
	}
	return c.commit1(evRetryReset, keyed(k))
}

// MaxAttempts 返回该协调器的重试预算硬上界。
func (c *Coordinator) MaxAttempts() int { return c.maxRetry }

// SetMaxAttempts 调整重试预算硬上界（必须在提交任何重试之前调用）。
func (c *Coordinator) SetMaxAttempts(n int) {
	if n > 0 {
		c.maxRetry = n
	}
}

// ResetAfterIntervention 是人工处置后的显式恢复：FAILED -> DUAL_WRITING，
// 同时清零重试预算。两步在同一个 WAL 批量内原子生效，且必须留下审计事件，
// 不允许静默清零。与参考模型语义保持一致。
func (c *Coordinator) ResetAfterIntervention(k ResourceKey, reason string) error {
	r, ok := c.proj.Resource(k)
	if !ok {
		return fmt.Errorf("migrate: unknown resource %s", k)
	}
	if !CanTransition(r.State, StateDualWriting) {
		return fmt.Errorf("migrate: cannot reset from %s", r.State)
	}
	items := []batchItem{{
		kind: evTransition,
		payload: transitionPayload{
			resourceKeyed: keyed(k), From: r.State, To: StateDualWriting, Reason: reason,
		},
	}}
	if r.Attempts != 0 {
		items = append(items, batchItem{kind: evRetryReset, payload: keyed(k)})
	}
	return c.commitBatch(items)
}

// Quarantine 因检出损坏而隔离资源（终态，全部访问报 Corrupted）。
// 抢救模式下此操作仅在内存投影生效（WAL 不可写），同样 fail-closed。
func (c *Coordinator) Quarantine(k ResourceKey, reason string) error {
	if _, ok := c.proj.Resource(k); !ok {
		return fmt.Errorf("migrate: unknown resource %s", k)
	}
	if c.readOnly {
		c.proj.mu.Lock()
		defer c.proj.mu.Unlock()
		if r, ok := c.proj.resources[k]; ok {
			r.State = StateQuarantined
			r.LastReason = "salvage quarantine: " + reason
		}
		return nil
	}
	return c.commit1(evQuarantine, quarantinePayload{
		resourceKeyed: keyed(k), Reason: reason,
	})
}

// Checkpoint 把当前投影写成带 CRC 的原子快照，供重启加速。
func (c *Coordinator) Checkpoint() (uint64, error) {
	if c.readOnly {
		return 0, errors.New("migrate: checkpoint rejected in salvage mode")
	}
	c.mu.Lock()
	doc := c.proj.Snapshot()
	seq := c.proj.lastSeq
	c.mu.Unlock()
	payload, err := json.Marshal(doc)
	if err != nil {
		return 0, err
	}
	if err := c.store.SaveSnapshot(seq, payload); err != nil {
		return 0, err
	}
	return seq, nil
}

// Resource / Resources 转发只读查询。
func (c *Coordinator) Resource(k ResourceKey) (ResourceState, bool) { return c.proj.Resource(k) }
func (c *Coordinator) Resources() []ResourceState                   { return c.proj.Resources() }

func sha256Sum(env Envelope) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d|%s|%s|%s", env.Seq, env.Kind, env.Payload, env.PrevHash)))
	return hex.EncodeToString(sum[:])
}
