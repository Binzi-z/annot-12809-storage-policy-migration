// Package migrate 实现每资源的迁移状态机、双写协调、撤销世代地板、
// 哈希链审计与有界重试调度，并通过事件溯源落到嵌入式 WAL。
package migrate

import (
	"fmt"

	"multitenantmigration/internal/policy"
)

// State 与 policy 包的状态一一对应，单独定义以避免持久化层反向依赖。
type State int

const (
	StateLegacy State = iota
	StateDualWriting
	StateCheck
	StateV2Primary
	StateMigrated
	StateFailed
	StateQuarantined
)

func (s State) String() string {
	return s.PolicyState().String()
}

// PolicyState 转换为策略引擎使用的状态枚举。
func (s State) PolicyState() policy.MigrationState {
	switch s {
	case StateLegacy:
		return policy.StateLegacy
	case StateDualWriting:
		return policy.StateDualWriting
	case StateCheck:
		return policy.StateCheck
	case StateV2Primary:
		return policy.StateV2Primary
	case StateMigrated:
		return policy.StateMigrated
	case StateFailed:
		return policy.StateFailed
	case StateQuarantined:
		return policy.StateQuarantined
	default:
		panic(fmt.Sprintf("unknown migration state %d", int(s)))
	}
}

// ParseState 用于快照/事件的字符串反序列化。
func ParseState(s string) (State, error) {
	for i := StateLegacy; i <= StateQuarantined; i++ {
		if i.String() == s {
			return i, nil
		}
	}
	return 0, fmt.Errorf("migrate: unknown state %q", s)
}

// allowedTransitions 是状态机参考模型的唯一转移表，真实协调器与纯函数
// 参考模型共用它，保证差分测试比对的是同一份状态机语义。
var allowedTransitions = map[State]map[State]bool{
	StateLegacy:      {StateDualWriting: true, StateQuarantined: true},
	StateDualWriting: {StateCheck: true, StateLegacy: true, StateFailed: true, StateQuarantined: true},
	StateCheck:       {StateV2Primary: true, StateDualWriting: true, StateFailed: true, StateQuarantined: true},
	StateV2Primary:   {StateMigrated: true, StateCheck: true, StateFailed: true, StateQuarantined: true},
	StateMigrated:    {StateV2Primary: true, StateQuarantined: true},
	StateFailed:      {StateDualWriting: true, StateQuarantined: true},
	StateQuarantined: {}, // 终态：只能人工处置后重建数据目录
}

// CanTransition 判断转移是否合法。
func CanTransition(from, to State) bool { return allowedTransitions[from][to] }

// ResourceKey 标识一个迁移单元（租户 + 资源前缀）。
type ResourceKey struct {
	TenantID string `json:"t"`
	Prefix   string `json:"p"`
}

func (k ResourceKey) String() string { return k.TenantID + "|" + k.Prefix }

// ResourceState 是单个资源的投影状态，全部由 WAL 事件重放得到，
// 内存中不允许存在无法重建的“隐藏状态”。
type ResourceState struct {
	Key      ResourceKey `json:"key"`
	State    State       `json:"state"`
	Attempts int         `json:"attempts"`
	// GenFloor 是撤销世代地板：只增不减，回滚与副本追赶都不能降低它。
	GenFloor uint64 `json:"gen_floor"`
	// PendingMirrors 是旧端写入后尚未镜像到 V2 的事件数；>0 即未收敛。
	PendingMirrors int `json:"pending_mirrors"`
	// LastLegacySeq / LastMirrorSeq 是双写水位的两端。
	LastLegacySeq uint64 `json:"last_legacy_seq"`
	LastMirrorSeq uint64 `json:"last_mirror_seq"`
	LastReason    string `json:"last_reason,omitempty"`
}

// Converged 表示双写水位追平、V2 视图可采信。
func (r *ResourceState) Converged() bool {
	return r.PendingMirrors == 0 && r.LastMirrorSeq >= r.LastLegacySeq
}
