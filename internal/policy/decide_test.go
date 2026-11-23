package policy

import (
	"errors"
	"testing"
)

// ---- 内存桩视图 ----

type memV1 struct{ rules []V1Rule }

func (m *memV1) V1Rules(tenant string) []V1Rule {
	var out []V1Rule
	for _, r := range m.rules {
		if r.TenantID == tenant {
			out = append(out, r)
		}
	}
	return out
}

type memV2 struct {
	rules []V2Rule
	res   map[string]struct {
		ver, gen uint64
	}
	corrupt bool
}

func (m *memV2) V2Rules(tenant string) []V2Rule {
	var out []V2Rule
	for _, r := range m.rules {
		if r.TenantID == tenant {
			out = append(out, r)
		}
	}
	return out
}

func (m *memV2) ResourceState(tenant, path string) (uint64, uint64, bool) {
	if m.corrupt {
		return 0, 0, false
	}
	if v, ok := m.res[tenant+"|"+path]; ok {
		return v.ver, v.gen, true
	}
	return 0, 0, false
}

type memMig struct {
	states    map[string]MigrationState
	converged map[string]bool
	corrupt   map[string]bool
}

func (m *memMig) MigrationState(tenant, path string) (MigrationState, error) {
	key := tenant + "|" + path
	if m.corrupt[key] {
		return 0, corrupted("simulated persisted-state corruption")
	}
	if s, ok := m.states[key]; ok {
		return s, nil
	}
	return StateLegacy, nil
}

func (m *memMig) DualWriteConverged(tenant, path string) (bool, error) {
	key := tenant + "|" + path
	if m.corrupt[key] {
		return false, corrupted("simulated watermark corruption")
	}
	return m.converged[key], nil
}

func newEngine() (*Engine, *memV1, *memV2, *memMig) {
	v1 := &memV1{rules: []V1Rule{
		{TenantID: "t1", Role: "writer", Prefix: "/data/a", CanRead: true, CanWrite: true},
		{TenantID: "t1", Role: "reader", Prefix: "/data/a", CanRead: true, CanWrite: false},
	}}
	v2 := &memV2{
		rules: []V2Rule{
			{V1Rule: v1.rules[0], ResourceVersion: 3, RevocationGen: 1},
			{V1Rule: v1.rules[1], ResourceVersion: 3, RevocationGen: 1},
		},
		res: map[string]struct{ ver, gen uint64 }{
			"t1|/data/a/1": {ver: 3, gen: 1},
		},
	}
	mig := &memMig{states: map[string]MigrationState{}, converged: map[string]bool{}}
	return &Engine{V1: v1, V2: v2, Mig: mig}, v1, v2, mig
}

func TestThreeErrorKindsAreDistinct(t *testing.T) {
	for _, pair := range [][2]error{
		{ErrDenied, ErrNotConverged}, {ErrDenied, ErrCorrupted}, {ErrNotConverged, ErrCorrupted},
	} {
		if errors.Is(pair[0], pair[1]) {
			t.Fatalf("%v must not wrap %v", pair[0], pair[1])
		}
	}
}

func TestLegacyResourceUsesV1Only(t *testing.T) {
	e, _, _, _ := newEngine()
	// LEGACY 下即使 V2 没有任何规则，V1 仍正常授权（未迁移资源的明确规则）。
	e.V2 = &memV2{}
	if err := e.Decide(AccessRequest{TenantID: "t1", Role: "writer", Path: "/data/a/1", Action: ActionRead}); err != nil {
		t.Fatalf("legacy read: %v", err)
	}
	if err := e.Decide(AccessRequest{TenantID: "t1", Role: "reader", Path: "/data/a/1", Action: ActionWrite}); !IsDenied(err) {
		t.Fatalf("legacy write as reader err=%v want denied", err)
	}
	if err := e.Decide(AccessRequest{TenantID: "t1", Role: "ghost", Path: "/data/a/1", Action: ActionRead}); !IsDenied(err) {
		t.Fatalf("unknown role err=%v want denied", err)
	}
}

func TestIntersectionNeverExpandsPrivilege(t *testing.T) {
	e, v1, v2, mig := newEngine()
	key := "t1|/data/a/1"
	mig.states[key] = StateDualWriting
	mig.converged[key] = true

	// 两侧一致允许 => 放行。
	if err := e.Decide(AccessRequest{TenantID: "t1", Role: "writer", Path: "/data/a/1", Action: ActionWrite, ObservedVersion: 3}); err != nil {
		t.Fatalf("dual-write allow: %v", err)
	}

	// V1 允许但 V2 不允许 => 交集必须拒绝，不能借双写扩大权限。
	v2.rules[0].CanWrite = false
	if err := e.Decide(AccessRequest{TenantID: "t1", Role: "writer", Path: "/data/a/1", Action: ActionWrite, ObservedVersion: 3}); !IsDenied(err) {
		t.Fatalf("v2 denies but v1 allows: err=%v want denied", err)
	}

	// V2 允许但 V1 不允许 => 同样拒绝。
	v2.rules[0].CanWrite = true
	v1.rules[0].CanWrite = false
	if err := e.Decide(AccessRequest{TenantID: "t1", Role: "writer", Path: "/data/a/1", Action: ActionWrite, ObservedVersion: 3}); !IsDenied(err) {
		t.Fatalf("v1 denies but v2 allows: err=%v want denied", err)
	}

	// 水位未追平 => NotConverged（fail-closed 的暂时态，区别于普通拒绝）。
	v1.rules[0].CanWrite = true
	mig.converged[key] = false
	if err := e.Decide(AccessRequest{TenantID: "t1", Role: "writer", Path: "/data/a/1", Action: ActionWrite, ObservedVersion: 3}); !IsNotConverged(err) {
		t.Fatalf("unconverged: err=%v want not-converged", err)
	}
}

func TestRevocationSurvivesStaleRuleReinstatement(t *testing.T) {
	e, _, v2, mig := newEngine()
	key := "t1|/data/a/1"
	for _, s := range []MigrationState{StateDualWriting, StateCheck, StateV2Primary, StateMigrated} {
		mig.states[key] = s
		mig.converged[key] = true

		// 资源撤销世代从 1 抬到 2（轮换）。规则仍停留在世代 1——
		// 这既覆盖“回滚到旧规则”，也覆盖“过期副本把旧规则追赶回来”。
		v2.res[key] = struct{ ver, gen uint64 }{ver: 3, gen: 2}
		err := e.Decide(AccessRequest{TenantID: "t1", Role: "writer", Path: "/data/a/1", Action: ActionRead})
		if !IsDenied(err) {
			t.Fatalf("state %s: stale-gen rule err=%v want denied", s, err)
		}
		err = e.Decide(AccessRequest{TenantID: "t1", Role: "writer", Path: "/data/a/1", Action: ActionWrite, ObservedVersion: 3})
		if !IsDenied(err) {
			t.Fatalf("state %s: stale-gen write err=%v want denied", s, err)
		}
	}
}

func TestVersionConflictIsPlainDenial(t *testing.T) {
	e, _, _, mig := newEngine()
	key := "t1|/data/a/1"
	mig.states[key] = StateMigrated
	// 客户端拿着旧版本号写：普通拒绝（乐观并发），不是未收敛。
	err := e.Decide(AccessRequest{TenantID: "t1", Role: "writer", Path: "/data/a/1", Action: ActionWrite, ObservedVersion: 2})
	if !IsDenied(err) {
		t.Fatalf("stale observed version err=%v want denied", err)
	}
	// 规则视图落后于资源版本 => 未收敛（暂时态）。
	e.V2.(*memV2).res[key] = struct{ ver, gen uint64 }{ver: 4, gen: 1}
	err = e.Decide(AccessRequest{TenantID: "t1", Role: "writer", Path: "/data/a/1", Action: ActionWrite, ObservedVersion: 4})
	if !IsNotConverged(err) {
		t.Fatalf("rule view lag err=%v want not-converged", err)
	}
}

func TestFailedStateSafetyRules(t *testing.T) {
	e, _, _, mig := newEngine()
	key := "t1|/data/a/1"
	mig.states[key] = StateFailed

	// 写一律冻结为未收敛。
	if err := e.Decide(AccessRequest{TenantID: "t1", Role: "writer", Path: "/data/a/1", Action: ActionWrite, ObservedVersion: 3}); !IsNotConverged(err) {
		t.Fatalf("failed write err=%v want not-converged", err)
	}
	// 读按 V1 继续可用。
	if err := e.Decide(AccessRequest{TenantID: "t1", Role: "reader", Path: "/data/a/1", Action: ActionRead}); err != nil {
		t.Fatalf("failed read: %v", err)
	}
	// V1 允许但 V2 明确拒绝 => 不允许假装一致。
	e.V2.(*memV2).rules[1].CanRead = false
	if err := e.Decide(AccessRequest{TenantID: "t1", Role: "reader", Path: "/data/a/1", Action: ActionRead}); !IsNotConverged(err) {
		t.Fatalf("conflicting reads err=%v want not-converged", err)
	}
}

func TestQuarantineAndCorruption(t *testing.T) {
	e, _, _, mig := newEngine()
	key := "t1|/data/a/1"
	mig.states[key] = StateQuarantined
	for _, a := range []Action{ActionRead, ActionWrite} {
		if err := e.Decide(AccessRequest{TenantID: "t1", Role: "writer", Path: "/data/a/1", Action: a}); !IsCorrupted(err) {
			t.Fatalf("quarantined %s err=%v want corrupted", a, err)
		}
	}
	mig.states = map[string]MigrationState{}
	mig.corrupt = map[string]bool{key: true}
	if err := e.Decide(AccessRequest{TenantID: "t1", Role: "writer", Path: "/data/a/1", Action: ActionRead}); !IsCorrupted(err) {
		t.Fatalf("unreadable migration state err=%v want corrupted", err)
	}
}
