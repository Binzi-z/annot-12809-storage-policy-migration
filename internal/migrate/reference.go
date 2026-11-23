package migrate

import (
	"fmt"
	"sort"
)

// 本文件是状态机“参考模型”：一份不依赖 WAL、事件封签或锁的纯函数实现，
// 用与协调器相同的转移表和安全规则直接维护内存状态。它与事件溯源实现
// 刻意分开编码——差分测试向两边喂同一条操作序列，任何实现偏差都会表现为
// 两边 ResourceState 不一致。参考模型本身也作为状态机语义的可读规格。

// refResource 是参考模型中的资源记录，字段与 ResourceState 对齐
// （LastReason 等仅展示用字段不参与差分比较）。
type refResource struct {
	state          State
	attempts       int
	genFloor       uint64
	pendingMirrors int
	lastLegacySeq  uint64
	lastMirrorSeq  uint64
}

// ReferenceModel 是纯内存 oracle，所有方法在非法操作时返回错误，
// 合法操作时按状态机参考语义直接改写。
type ReferenceModel struct {
	r        map[ResourceKey]*refResource
	maxRetry int
}

// NewReferenceModel 创建参考模型。
func NewReferenceModel() *ReferenceModel {
	return &ReferenceModel{r: map[ResourceKey]*refResource{}, maxRetry: DefaultMaxAttempts}
}

func (m *ReferenceModel) get(k ResourceKey) (*refResource, error) {
	r, ok := m.r[k]
	if !ok {
		return nil, fmt.Errorf("reference: unknown resource %s", k)
	}
	return r, nil
}

// Register 对应 LEGACY 初始注册。
func (m *ReferenceModel) Register(k ResourceKey) error {
	if _, exists := m.r[k]; exists {
		return fmt.Errorf("reference: duplicate registration %s", k)
	}
	m.r[k] = &refResource{state: StateLegacy}
	return nil
}

func (m *ReferenceModel) transition(k ResourceKey, to State) error {
	r, err := m.get(k)
	if err != nil {
		return err
	}
	if !CanTransition(r.state, to) {
		return fmt.Errorf("reference: illegal transition %s->%s", r.state, to)
	}
	r.state = to
	return nil
}

// BeginMigration / BeginCheck / PromoteV2 / Complete 是正向工作流。
func (m *ReferenceModel) BeginMigration(k ResourceKey) error {
	return m.transition(k, StateDualWriting)
}
func (m *ReferenceModel) BeginCheck(k ResourceKey) error { return m.transition(k, StateCheck) }
func (m *ReferenceModel) PromoteV2(k ResourceKey) error  { return m.transition(k, StateV2Primary) }
func (m *ReferenceModel) Complete(k ResourceKey) error   { return m.transition(k, StateMigrated) }

// LegacyWrite：LEGACY 下不积压；迁移开始后每次写入推高待镜像计数。
func (m *ReferenceModel) LegacyWrite(k ResourceKey) error {
	r, err := m.get(k)
	if err != nil {
		return err
	}
	r.lastLegacySeq++
	if r.state != StateLegacy {
		r.pendingMirrors++
	}
	return nil
}

// MirrorApplied：V2 追赶一次，积压递减、水位推进。
func (m *ReferenceModel) MirrorApplied(k ResourceKey) error {
	r, err := m.get(k)
	if err != nil {
		return err
	}
	if r.pendingMirrors > 0 {
		r.pendingMirrors--
	}
	r.lastMirrorSeq++
	return nil
}

// Drain：水位强制对齐，积压清零。
func (m *ReferenceModel) Drain(k ResourceKey) error {
	r, err := m.get(k)
	if err != nil {
		return err
	}
	r.pendingMirrors = 0
	r.lastMirrorSeq = r.lastLegacySeq
	return nil
}

// RotateRevocation：世代地板严格递增——撤销的核心不变量。
func (m *ReferenceModel) RotateRevocation(k ResourceKey, newFloor uint64) error {
	r, err := m.get(k)
	if err != nil {
		return err
	}
	if newFloor <= r.genFloor {
		return fmt.Errorf("reference: generation floor must strictly increase: %d->%d", r.genFloor, newFloor)
	}
	r.genFloor = newFloor
	return nil
}

// Rollback：只退状态，世代地板原样持有（撤销不可经回滚恢复）。
func (m *ReferenceModel) Rollback(k ResourceKey, to State) error {
	r, err := m.get(k)
	if err != nil {
		return err
	}
	if !CanTransition(r.state, to) {
		return fmt.Errorf("reference: illegal rollback %s->%s", r.state, to)
	}
	held := r.genFloor // 显式表达：回滚路径上地板不参与赋值
	r.state = to
	r.genFloor = held
	return nil
}

// ImportSnapshot：只接受“更高”的世代地板与水位，旧副本无法反向追赶。
func (m *ReferenceModel) ImportSnapshot(k ResourceKey, genFloor, watermark uint64) error {
	r, ok := m.r[k]
	if !ok {
		r = &refResource{state: StateLegacy}
		m.r[k] = r
	}
	if genFloor > r.genFloor {
		r.genFloor = genFloor
	}
	if watermark > r.lastMirrorSeq {
		r.lastMirrorSeq = watermark
	}
	return nil
}

// RecordRetry：参考模型复刻有界重试语义，预算耗尽即转 FAILED；
// 已在 FAILED 时短路，计数不允许超过预算。
func (m *ReferenceModel) RecordRetry(k ResourceKey, attempt int) (bool, error) {
	r, err := m.get(k)
	if err != nil {
		return false, err
	}
	if r.state == StateFailed {
		return true, nil
	}
	if attempt > m.maxRetry {
		attempt = m.maxRetry
	}
	r.attempts = attempt
	exhausted := attempt >= m.maxRetry && CanTransition(r.state, StateFailed)
	if exhausted {
		r.state = StateFailed
	}
	return exhausted, nil
}

// ResetAttempts / ResetAfterIntervention 对应人工恢复路径。
func (m *ReferenceModel) ResetAttempts(k ResourceKey) error {
	r, err := m.get(k)
	if err != nil {
		return err
	}
	r.attempts = 0
	return nil
}

func (m *ReferenceModel) ResetAfterIntervention(k ResourceKey) error {
	r, err := m.get(k)
	if err != nil {
		return err
	}
	if !CanTransition(r.state, StateDualWriting) {
		return fmt.Errorf("reference: cannot reset from %s", r.state)
	}
	r.state = StateDualWriting
	r.attempts = 0
	return nil
}

// DiffEntry 是单资源的差分比较结果。
type DiffEntry struct {
	Key      ResourceKey
	Field    string
	Expected string
	Got      string
}

// DiffTo 把参考模型与协调器投影做全字段比较，返回所有不一致项。
func (m *ReferenceModel) DiffTo(c *Coordinator) []DiffEntry {
	var diffs []DiffEntry
	keys := make(map[ResourceKey]struct{}, len(m.r))
	for k := range m.r {
		keys[k] = struct{}{}
	}
	for _, rs := range c.Resources() {
		keys[rs.Key] = struct{}{}
	}
	for k := range keys {
		ref, ok := m.r[k]
		if !ok {
			diffs = append(diffs, DiffEntry{Key: k, Field: "*", Expected: "absent", Got: "present"})
			continue
		}
		got, present := c.Resource(k)
		if !present {
			diffs = append(diffs, DiffEntry{Key: k, Field: "*", Expected: "present", Got: "absent"})
			continue
		}
		check := func(field string, want, got any) {
			if fmt.Sprint(want) != fmt.Sprint(got) {
				diffs = append(diffs, DiffEntry{Key: k, Field: field,
					Expected: fmt.Sprint(want), Got: fmt.Sprint(got)})
			}
		}
		check("State", ref.state, got.State)
		check("Attempts", ref.attempts, got.Attempts)
		check("GenFloor", ref.genFloor, got.GenFloor)
		check("PendingMirrors", ref.pendingMirrors, got.PendingMirrors)
		check("LastLegacySeq", ref.lastLegacySeq, got.LastLegacySeq)
		check("LastMirrorSeq", ref.lastMirrorSeq, got.LastMirrorSeq)
	}
	sort.Slice(diffs, func(i, j int) bool {
		if diffs[i].Key.String() != diffs[j].Key.String() {
			return diffs[i].Key.String() < diffs[j].Key.String()
		}
		return diffs[i].Field < diffs[j].Field
	})
	return diffs
}
