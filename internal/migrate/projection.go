package migrate

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"multitenantmigration/internal/policy"
)

// Projection 是 WAL 事件的纯函数折叠结果。它本身不做 I/O，因此可以被
// 参考模型直接复用，也可以在强杀后完整重建。
type Projection struct {
	mu        sync.RWMutex
	resources map[ResourceKey]*ResourceState
	// chainTip 是最近一条已提交事件的哈希，用于续写哈希链。
	chainTip string
	// lastSeq 是最近一条已提交事件的逻辑序号（与 WAL 帧序号独立计数，
	// 因为一个 WAL 批量可含多个事件）。
	lastSeq uint64
}

func newProjection() *Projection {
	return &Projection{resources: map[ResourceKey]*ResourceState{}}
}

// SnapshotDocument 是快照的 JSON 载荷。
type SnapshotDocument struct {
	LastSeq   uint64          `json:"last_seq"`
	ChainTip  string          `json:"chain_tip"`
	Resources []ResourceState `json:"resources"`
}

// Apply 把一条已验证的信封折叠进投影。它是状态机语义的唯一事实来源：
// 真实协调器、重放、快照恢复与差分参考模型都经过这里。
func (p *Projection) Apply(env Envelope) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 序号与哈希链在调用方（replay/commit）已校验，这里只做业务折叠。
	p.chainTip = env.Hash
	if env.Seq > p.lastSeq {
		p.lastSeq = env.Seq
	}

	switch env.Kind {
	case evRegister:
		var pl resourceKeyed
		if err := json.Unmarshal(env.Payload, &pl); err != nil {
			return err
		}
		k := ResourceKey{pl.TenantID, pl.Prefix}
		if _, exists := p.resources[k]; exists {
			return fmt.Errorf("migrate: duplicate registration %s", k)
		}
		p.resources[k] = &ResourceState{Key: k, State: StateLegacy}

	case evTransition:
		var pl transitionPayload
		if err := json.Unmarshal(env.Payload, &pl); err != nil {
			return err
		}
		r, err := p.require(pl.resourceKeyed)
		if err != nil {
			return err
		}
		if !CanTransition(r.State, pl.To) {
			return fmt.Errorf("migrate: illegal transition %s->%s for %s", r.State, pl.To, r.Key)
		}
		if r.State != pl.From {
			return fmt.Errorf("migrate: transition source mismatch want %s got %s", r.State, pl.From)
		}
		r.State = pl.To
		r.LastReason = pl.Reason

	case evLegacyWrite:
		var pl legacyWritePayload
		if err := json.Unmarshal(env.Payload, &pl); err != nil {
			return err
		}
		r, err := p.require(pl.resourceKeyed)
		if err != nil {
			return err
		}
		r.LastLegacySeq = pl.LegacySeq
		if r.State != StateLegacy {
			// 迁移开始后的旧端写入必须进镜像队列，保持双版本水位可见。
			r.PendingMirrors++
		}

	case evMirrorApplied:
		var pl mirrorPayload
		if err := json.Unmarshal(env.Payload, &pl); err != nil {
			return err
		}
		r, err := p.require(pl.resourceKeyed)
		if err != nil {
			return err
		}
		if r.PendingMirrors > 0 {
			r.PendingMirrors--
		}
		r.LastMirrorSeq = pl.MirrorSeq

	case evDualDrainComplete:
		var pl drainPayload
		if err := json.Unmarshal(env.Payload, &pl); err != nil {
			return err
		}
		r, err := p.require(pl.resourceKeyed)
		if err != nil {
			return err
		}
		r.PendingMirrors = 0
		r.LastMirrorSeq = pl.Watermark

	case evRotate:
		var pl rotatePayload
		if err := json.Unmarshal(env.Payload, &pl); err != nil {
			return err
		}
		r, err := p.require(pl.resourceKeyed)
		if err != nil {
			return err
		}
		if pl.NewGenFloor <= pl.OldGenFloor {
			return fmt.Errorf("migrate: generation floor must strictly increase: %d->%d",
				pl.OldGenFloor, pl.NewGenFloor)
		}
		if r.GenFloor != pl.OldGenFloor {
			return fmt.Errorf("migrate: rotation base mismatch want %d got %d", r.GenFloor, pl.OldGenFloor)
		}
		r.GenFloor = pl.NewGenFloor

	case evRollback:
		var pl rollbackPayload
		if err := json.Unmarshal(env.Payload, &pl); err != nil {
			return err
		}
		r, err := p.require(pl.resourceKeyed)
		if err != nil {
			return err
		}
		if !CanTransition(pl.From, pl.To) || r.State != pl.From {
			return fmt.Errorf("migrate: illegal rollback %s->%s (current %s)", pl.From, pl.To, r.State)
		}
		// 关键不变量：回滚只退状态，世代地板保持不动，撤销不可被回滚恢复。
		if r.GenFloor != pl.GenFloor {
			return fmt.Errorf("migrate: rollback must not move generation floor: held=%d current=%d",
				pl.GenFloor, r.GenFloor)
		}
		r.State = pl.To
		r.LastReason = pl.Reason

	case evSnapshotImported:
		var pl snapshotPayload
		if err := json.Unmarshal(env.Payload, &pl); err != nil {
			return err
		}
		k := ResourceKey{pl.TenantID, pl.Prefix}
		r, exists := p.resources[k]
		if !exists {
			r = &ResourceState{Key: k, State: StateLegacy}
			p.resources[k] = r
		}
		// 导入快照同样受世代地板约束：任何来源都不能把地板拉低。
		if pl.GenFloor > r.GenFloor {
			r.GenFloor = pl.GenFloor
		}
		if pl.Watermark > r.LastMirrorSeq {
			r.LastMirrorSeq = pl.Watermark
		}
		r.LastReason = "snapshot imported from " + pl.Source

	case evRetryScheduled:
		var pl retryPayload
		if err := json.Unmarshal(env.Payload, &pl); err != nil {
			return err
		}
		r, err := p.require(pl.resourceKeyed)
		if err != nil {
			return err
		}
		r.Attempts = pl.Attempt

	case evRetryReset:
		var pl resourceKeyed
		if err := json.Unmarshal(env.Payload, &pl); err != nil {
			return err
		}
		r, err := p.require(pl)
		if err != nil {
			return err
		}
		r.Attempts = 0

	case evRetryExhausted:
		var rp retryPayload
		if err := json.Unmarshal(env.Payload, &rp); err != nil {
			return err
		}
		r, err := p.require(rp.resourceKeyed)
		if err != nil {
			return err
		}
		if !CanTransition(r.State, StateFailed) {
			return fmt.Errorf("migrate: cannot mark FAILED from %s for %s", r.State, r.Key)
		}
		r.State = StateFailed
		r.LastReason = rp.Reason

	case evQuarantine:
		var pl quarantinePayload
		if err := json.Unmarshal(env.Payload, &pl); err != nil {
			return err
		}
		r, err := p.require(pl.resourceKeyed)
		if err != nil {
			return err
		}
		r.State = StateQuarantined
		r.LastReason = pl.Reason

	default:
		return fmt.Errorf("migrate: unknown event kind %q", env.Kind)
	}
	return nil
}

func (p *Projection) require(k resourceKeyed) (*ResourceState, error) {
	r, ok := p.resources[ResourceKey{k.TenantID, k.Prefix}]
	if !ok {
		return nil, fmt.Errorf("migrate: unknown resource %s|%s", k.TenantID, k.Prefix)
	}
	return r, nil
}

// ---- 只读查询（实现 policy.MigrationView）----

// lookup 按最长前缀匹配定位资源；未注册返回 nil。
func (p *Projection) lookup(tenantID, path string) *ResourceState {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var best *ResourceState
	bestLen := -1
	for k, r := range p.resources {
		if k.TenantID != tenantID {
			continue
		}
		if len(k.Prefix) <= len(path) && path[:len(k.Prefix)] == k.Prefix && len(k.Prefix) > bestLen {
			best, bestLen = r, len(k.Prefix)
		}
	}
	return best
}

// MigrationState 实现 policy.MigrationView。
func (p *Projection) MigrationState(tenantID, path string) (policy.MigrationState, error) {
	r := p.lookup(tenantID, path)
	if r == nil {
		return policy.StateLegacy, nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return r.State.PolicyState(), nil
}

// DualWriteConverged 实现 policy.MigrationView。
func (p *Projection) DualWriteConverged(tenantID, path string) (bool, error) {
	r := p.lookup(tenantID, path)
	if r == nil {
		return false, nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return r.Converged(), nil
}

// Snapshot 序列化当前投影（事件折叠的纯结果）。
func (p *Projection) Snapshot() SnapshotDocument {
	p.mu.RLock()
	defer p.mu.RUnlock()
	doc := SnapshotDocument{LastSeq: p.lastSeq, ChainTip: p.chainTip}
	for _, r := range p.resources {
		doc.Resources = append(doc.Resources, *r)
	}
	sort.Slice(doc.Resources, func(i, j int) bool {
		if doc.Resources[i].Key.TenantID != doc.Resources[j].Key.TenantID {
			return doc.Resources[i].Key.TenantID < doc.Resources[j].Key.TenantID
		}
		return doc.Resources[i].Key.Prefix < doc.Resources[j].Key.Prefix
	})
	return doc
}

func (p *Projection) loadSnapshot(doc SnapshotDocument) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastSeq = doc.LastSeq
	p.chainTip = doc.ChainTip
	for i := range doc.Resources {
		r := doc.Resources[i]
		p.resources[r.Key] = &r
	}
}

// Resource 返回资源状态副本与是否存在。
func (p *Projection) Resource(k ResourceKey) (ResourceState, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	r, ok := p.resources[k]
	if !ok {
		return ResourceState{}, false
	}
	return *r, true
}

// Resources 返回所有资源的排序副本，供测试与 CLI 展示。
func (p *Projection) Resources() []ResourceState {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]ResourceState, 0, len(p.resources))
	for _, r := range p.resources {
		out = append(out, *r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key.String() < out[j].Key.String() })
	return out
}
