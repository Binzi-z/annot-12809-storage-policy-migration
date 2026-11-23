package policy

import "fmt"

// MigrationState 是资源级迁移状态机在策略引擎视角下的状态。
type MigrationState int

const (
	// StateLegacy：尚未开始迁移，仅 V1 授权（未迁移资源的明确规则）。
	StateLegacy MigrationState = iota
	// StateDualWriting：双写进行中，交集闸门生效。
	StateDualWriting
	// StateCheck：双写一致性校验中，权限语义与双写期相同。
	StateCheck
	// StateV2Primary：V2 成为写主端并回映 V1，仍保留交集闸门以便回滚。
	StateV2Primary
	// StateMigrated：迁移完成，仅 V2 授权。
	StateMigrated
	// StateFailed：重试预算耗尽的中间态：读走 V1，写一律 NotConverged。
	StateFailed
	// StateQuarantined：检出数据损坏，全部访问 fail-closed 报 Corrupted。
	StateQuarantined
)

func (s MigrationState) String() string {
	switch s {
	case StateLegacy:
		return "LEGACY"
	case StateDualWriting:
		return "DUAL_WRITING"
	case StateCheck:
		return "CHECK"
	case StateV2Primary:
		return "V2_PRIMARY"
	case StateMigrated:
		return "MIGRATED"
	case StateFailed:
		return "FAILED"
	case StateQuarantined:
		return "QUARANTINED"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", int(s))
	}
}

// MigrationView 是协调器向策略引擎暴露的资源状态视图。
type MigrationView interface {
	// MigrationState 返回资源迁移状态；未知资源按 StateLegacy 处理。
	// 持久化损坏时返回的错误必须 wrap policy.ErrCorrupted。
	MigrationState(tenantID, path string) (MigrationState, error)
	// DualWriteConverged 表示该资源的双写水位是否已追平，V2 视图可采信。
	DualWriteConverged(tenantID, path string) (bool, error)
}

// Engine 组合两个版本的规则视图与迁移视图做访问判定。
type Engine struct {
	V1  V1Store
	V2  V2Store
	Mig MigrationView
}

// 单侧评估结论。优先级 corrupted > notConverged > denied > allow。
type sideVerdict int

const (
	verdictAllow sideVerdict = iota
	verdictDenied
	verdictNotConverged
	verdictCorrupted
)

type sideResult struct {
	verdict sideVerdict
	reason  string
}

// Decide 判定一次访问。返回 nil 表示放行；否则返回的错误必可被
// IsDenied / IsNotConverged / IsCorrupted 之一判别。
func (e *Engine) Decide(req AccessRequest) error {
	if e.V1 == nil || e.V2 == nil || e.Mig == nil {
		return corrupted("engine views not fully wired")
	}
	state, err := e.Mig.MigrationState(req.TenantID, req.Path)
	if err != nil {
		if IsCorrupted(err) {
			return err
		}
		return notConverged("migration state unreadable: " + err.Error())
	}

	switch state {
	case StateQuarantined:
		return corrupted("resource quarantined due to persisted-state corruption")
	case StateLegacy:
		// 明确规则：未迁移资源完全按 V1 读写，V2 缺属正常，不视为未收敛。
		return wrapV1(e.evalV1(req), "legacy resource")
	case StateFailed:
		return e.decideFailed(req)
	case StateMigrated:
		// 明确规则：迁移完成后 V2 独立授权（含撤销世代与版本检查）。
		return wrapV2(e.evalV2(req), "migrated resource")
	default: // DUAL_WRITING / CHECK / V2_PRIMARY：交集闸门
		return e.decideIntersection(req, state)
	}
}

// decideIntersection 实现“任何时刻不扩大权限”的核心：必须同时获得 V1 与
// V2 的许可。V2 视图未追平时给 NotConverged 而非 Denied，便于调用方重试，
// 但同样 fail-closed。
func (e *Engine) decideIntersection(req AccessRequest, state MigrationState) error {
	converged, err := e.Mig.DualWriteConverged(req.TenantID, req.Path)
	if err != nil {
		if IsCorrupted(err) {
			return err
		}
		return notConverged("convergence watermark unreadable: " + err.Error())
	}

	v1 := e.evalV1(req)
	v2 := e.evalV2(req)

	// 损坏优先，其次未收敛。
	if v1.verdict == verdictCorrupted || v2.verdict == verdictCorrupted {
		return corrupted(joinReasons(v1, v2))
	}
	if !converged {
		// 双写水位未追平：即便两侧恰好都允许，也不能采信 V2 视图。
		return notConverged(fmt.Sprintf(
			"dual-write watermark behind in state %s; fail-closed pending convergence", state))
	}
	if v1.verdict == verdictNotConverged || v2.verdict == verdictNotConverged {
		return notConverged(joinReasons(v1, v2))
	}
	if v1.verdict == verdictDenied || v2.verdict == verdictDenied {
		return denied(joinReasons(v1, v2))
	}
	return nil
}

// decideFailed 是重试预算耗尽后的明确安全规则：
//   - 写：一律 NotConverged，禁止任何写，等待人工处置或下一轮调度；
//   - 读：仅按 V1 放行；若 V2 已存在且与 V1 结论冲突，报 NotConverged。
func (e *Engine) decideFailed(req AccessRequest) error {
	v1 := e.evalV1(req)
	if req.Action == ActionWrite {
		return notConverged("resource migration stuck in FAILED; writes frozen")
	}
	if v1.verdict == verdictCorrupted {
		return corrupted(v1.reason)
	}
	if v1.verdict != verdictAllow {
		return wrapV1(v1, "failed-migration read")
	}
	v2 := e.evalV2(req)
	switch v2.verdict {
	case verdictCorrupted:
		return corrupted(v2.reason)
	case verdictAllow:
		return nil
	case verdictNotConverged:
		// V2 视图滞后：读仍可按迁移前的 V1 继续，但记录为暂时态由审计可见。
		return nil
	case verdictDenied:
		// 两侧结论冲突：不能假装一致。
		return notConverged("V1 allows read but V2 denies: " + v2.reason)
	default:
		return notConverged("unexpected V2 verdict")
	}
}

func wrapV1(r sideResult, ctx string) error {
	switch r.verdict {
	case verdictAllow:
		return nil
	case verdictDenied:
		return denied(ctx + ": " + r.reason)
	case verdictNotConverged:
		return notConverged(ctx + ": " + r.reason)
	default:
		return corrupted(ctx + ": " + r.reason)
	}
}

func wrapV2(r sideResult, ctx string) error { return wrapV1(r, ctx) }

func joinReasons(a, b sideResult) string {
	return "v1[" + a.reason + "] v2[" + b.reason + "]"
}

// evalV1 评估旧版规则：租户+角色+前缀匹配，任一匹配规则授予该动作即允许。
func (e *Engine) evalV1(req AccessRequest) sideResult {
	var matched bool
	for _, r := range e.V1.V1Rules(req.TenantID) {
		if r.Role != req.Role || !prefixMatches(r.Prefix, req.Path) {
			continue
		}
		matched = true
		grants := r.CanRead
		if req.Action == ActionWrite {
			grants = r.CanWrite
		}
		if grants {
			return sideResult{verdictAllow, fmt.Sprintf("rule prefix=%q grants %s", r.Prefix, req.Action)}
		}
	}
	if !matched {
		return sideResult{verdictDenied, "no V1 rule matches role/prefix"}
	}
	return sideResult{verdictDenied, "matching V1 rules do not grant " + string(req.Action)}
}

// evalV2 评估新版规则：在 V1 匹配维度之上强制撤销世代与资源版本检查。
func (e *Engine) evalV2(req AccessRequest) sideResult {
	curVersion, curGen, resourceKnown := e.V2.ResourceState(req.TenantID, req.Path)
	var (
		matched        bool
		versionStale   bool
		revokedMatched bool
	)
	for _, r := range e.V2.V2Rules(req.TenantID) {
		if r.Role != req.Role || !prefixMatches(r.Prefix, req.Path) {
			continue
		}
		matched = true
		grants := r.CanRead
		if req.Action == ActionWrite {
			grants = r.CanWrite
		}
		if !grants {
			continue
		}
		// 撤销世代：规则世代低于资源当前世代 => 已被撤销，永久拒绝。
		// 回滚或过期副本都无法把当前世代降回去（协调器另有世代地板保证）。
		if resourceKnown && r.RevocationGen < curGen {
			revokedMatched = true
			continue
		}
		if req.Action == ActionWrite {
			// 写要求规则视图与资源当前版本一致，否则视为视图滞后（暂时态；
			// 客户端还须带正确的乐观版本号。
			if resourceKnown && r.ResourceVersion != curVersion {
				versionStale = true
				continue
			}
			if req.ObservedVersion != 0 && req.ObservedVersion != curVersion {
				return sideResult{verdictDenied, fmt.Sprintf(
					"optimistic version conflict: observed=%d current=%d",
					req.ObservedVersion, curVersion)}
			}
		}
		return sideResult{verdictAllow, fmt.Sprintf(
			"rule prefix=%q gen=%d ver=%d grants %s",
			r.Prefix, r.RevocationGen, r.ResourceVersion, req.Action)}
	}
	switch {
	case !matched:
		return sideResult{verdictDenied, "no V2 rule matches role/prefix"}
	case versionStale:
		return sideResult{verdictNotConverged, "V2 rule view lags resource version; awaiting dual-write catch-up"}
	case revokedMatched:
		return sideResult{verdictDenied, fmt.Sprintf(
			"matching grants revoked: rule generation below resource generation %d", curGen)}
	default:
		return sideResult{verdictDenied, "matching V2 rules do not grant " + string(req.Action)}
	}
}
