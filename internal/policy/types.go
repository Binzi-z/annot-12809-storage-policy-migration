// Package policy 定义多租户存储访问策略的两个版本及其迁移期组合判定。
//
// 旧版（V1）按“路径前缀 + 角色作用域”授权；新版（V2）在同样的主体/前缀
// 匹配之外，额外携带资源版本（ResourceVersion）与撤销世代（Revocation
// Generation）。迁移期两个写入端并行运行，读取端通过交集闸门取两个版本
// 的共同许可，保证任何时刻都不会扩大相对 V1 的有效权限。
package policy

import (
	"errors"
	"strings"
)

// Action 是存储操作类型。
type Action string

const (
	ActionRead  Action = "read"
	ActionWrite Action = "write"
)

// V1Rule 是旧版规则：租户内，某角色对某个路径前缀的读/写许可。
type V1Rule struct {
	TenantID string
	Role     string
	Prefix   string
	CanRead  bool
	CanWrite bool
}

// V2Rule 是新版规则，在 V1 的匹配维度上增加：
//   - ResourceVersion：规则所基于的资源版本，版本过旧的规则不能授权写。
//   - RevocationGen：规则签发时的撤销世代，低于资源当前世代即已失效。
type V2Rule struct {
	V1Rule
	ResourceVersion uint64
	RevocationGen   uint64
}

// V1Store 是旧写入端维护的规则视图（接口化以便差分测试打桩）。
type V1Store interface {
	V1Rules(tenantID string) []V1Rule
}

// V2Store 是新写入端维护的规则视图，额外暴露资源级版本与撤销世代。
type V2Store interface {
	V2Rules(tenantID string) []V2Rule
	// ResourceState 返回资源当前版本与撤销世代；资源不存在时 ok=false。
	ResourceState(tenantID, path string) (version, revocationGen uint64, ok bool)
}

// 三类可区分的访问结果错误。调用方用 errors.Is 判别，禁止把它们混为一个
// “拒绝”，因为未收敛与损坏都要求 fail-closed 但处置路径不同。
var (
	// ErrDenied 是普通拒绝：状态一致且完整，只是策略不允许该操作。
	ErrDenied = errors.New("policy: access denied")
	// ErrNotConverged 是状态未收敛：双版本视图尚不一致或迁移未完成，
	// 此刻无法安全地给出许可。按 fail-closed 拒绝，但这是暂时态。
	ErrNotConverged = errors.New("policy: state not converged")
	// ErrCorrupted 是数据损坏：持久化的策略记录未通过完整性校验。
	ErrCorrupted = errors.New("policy: policy data corrupted")
)

// DecisionError 携带判别所需的上下文，便于审计日志解释拒绝原因。
type DecisionError struct {
	Kind   error // ErrDenied / ErrNotConverged / ErrCorrupted
	Reason string
}

func (e *DecisionError) Error() string { return e.Kind.Error() + ": " + e.Reason }
func (e *DecisionError) Unwrap() error { return e.Kind }

func denied(reason string) *DecisionError {
	return &DecisionError{Kind: ErrDenied, Reason: reason}
}

func notConverged(reason string) *DecisionError {
	return &DecisionError{Kind: ErrNotConverged, Reason: reason}
}

func corrupted(reason string) *DecisionError {
	return &DecisionError{Kind: ErrCorrupted, Reason: reason}
}

// IsDenied / IsNotConverged / IsCorrupted 是给调用方的语义化判别器。
func IsDenied(err error) bool       { return errors.Is(err, ErrDenied) }
func IsNotConverged(err error) bool { return errors.Is(err, ErrNotConverged) }
func IsCorrupted(err error) bool    { return errors.Is(err, ErrCorrupted) }

// AccessRequest 描述一次访问尝试。
type AccessRequest struct {
	TenantID string
	Role     string
	Path     string
	Action   Action
	// ObservedVersion 是客户端持有的资源版本（写操作的乐观并发依据）；
	// 只读场景可留 0。
	ObservedVersion uint64
}

func prefixMatches(prefix, path string) bool {
	return strings.HasPrefix(path, prefix)
}
