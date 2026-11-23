package migrate

import (
	"math/rand"
	"path/filepath"
	"testing"

	"multitenantmigration/internal/storage"
)

// diffHarness 把“同一条操作序列同时作用于参考模型与真实协调器”的样板
// 收拢到一处，并在每一步后做全字段差分。
type diffHarness struct {
	t        *testing.T
	ref      *ReferenceModel
	coord    *Coordinator
	store    *storage.Store
	dir      string
	attempts map[ResourceKey]int
	steps    int
	closed   bool
}

func newDiffHarness(t *testing.T) *diffHarness {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "diffdb")
	store, err := storage.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	coord, err := Open(store)
	if err != nil {
		t.Fatalf("coordinator: %v", err)
	}
	return &diffHarness{
		t: t, ref: NewReferenceModel(), coord: coord, store: store, dir: dir,
		attempts: map[ResourceKey]int{},
	}
}

func (h *diffHarness) close() {
	if h.closed {
		return
	}
	if err := h.coord.Close(); err != nil {
		h.t.Fatalf("close: %v", err)
	}
	h.closed = true
}

// reopen 先释放旧句柄（含底层 store 与独占锁），再从磁盘完整重建。
// 可安全地在调用方已手动关闭句柄后使用。
func (h *diffHarness) reopen() {
	if !h.closed {
		if err := h.coord.Close(); err != nil {
			h.t.Fatalf("close before reopen: %v", err)
		}
		h.closed = true
	}
	store, err := storage.Open(h.dir)
	if err != nil {
		h.t.Fatalf("reopen storage: %v", err)
	}
	coord, err := Open(store)
	if err != nil {
		h.t.Fatalf("reopen coordinator: %v", err)
	}
	h.store = store
	h.coord = coord
	h.closed = false
}

// apply 同时驱动两边并比较“成败一致性 + 全状态一致性”。
func (h *diffHarness) apply(label string, refErr, gotErr error) {
	h.t.Helper()
	h.steps++
	if (refErr == nil) != (gotErr == nil) {
		h.t.Fatalf("step %d (%s): error divergence ref=%v got=%v", h.steps, label, refErr, gotErr)
	}
	if diffs := h.ref.DiffTo(h.coord); len(diffs) != 0 {
		h.t.Fatalf("step %d (%s): state divergence: %+v", h.steps, label, diffs)
	}
}

// rollbackTarget 给出当前状态下回滚边的目标；返回 false 表示无回滚边。
func rollbackTarget(s State) (State, bool) {
	switch s {
	case StateDualWriting:
		return StateLegacy, true
	case StateCheck:
		return StateDualWriting, true
	case StateV2Primary:
		return StateCheck, true
	case StateMigrated:
		return StateV2Primary, true
	case StateFailed:
		return StateDualWriting, true
	default:
		return 0, false
	}
}

// TestDifferentialDualWriteRotationRollbackImport 是固定脚本的差分测试，
// 显式覆盖题目点名的四类操作序列。
func TestDifferentialDualWriteRotationRollbackImport(t *testing.T) {
	h := newDiffHarness(t)
	defer h.close()
	keys := []ResourceKey{
		{TenantID: "tenant-a", Prefix: "/orders"},
		{TenantID: "tenant-a", Prefix: "/inventory"},
		{TenantID: "tenant-b", Prefix: "/logs"},
	}
	register := func(k ResourceKey) {
		h.apply("register", h.ref.Register(k), h.coord.Register(k))
	}
	for _, k := range keys {
		register(k)
	}

	// --- 双写：旧端持续写入，V2 镜像追赶，水位收敛后推进 ---
	for _, k := range keys {
		h.apply("begin", h.ref.BeginMigration(k), h.coord.BeginMigration(k, "diff"))
	}
	for round := 0; round < 3; round++ {
		for _, k := range keys {
			h.apply("legacy-write", h.ref.LegacyWrite(k), h.coord.LegacyWrite(k, "diff-write"))
		}
		for _, k := range keys {
			h.apply("mirror", h.ref.MirrorApplied(k), h.coord.MirrorApplied(k))
		}
	}
	for _, k := range keys {
		h.apply("drain", h.ref.Drain(k), h.coord.Drain(k))
		h.apply("check", h.ref.BeginCheck(k), h.coord.BeginCheck(k))
	}

	// --- 轮换：世代地板严格递增，两边规则一致 ---
	for i, k := range keys {
		floor := uint64(2 + i*2)
		h.apply("rotate", h.ref.RotateRevocation(k, floor),
			h.coord.RotateRevocation(k, floor, "diff rotation"))
		// 非法的下降轮换两边都必须拒绝。
		h.apply("rotate-down", h.ref.RotateRevocation(k, floor-1),
			h.coord.RotateRevocation(k, floor-1, "should fail"))
	}

	// --- 回滚：V2_PRIMARY 推上去再沿回滚边退回来，地板保持 ---
	for _, k := range keys {
		h.apply("promote", h.ref.PromoteV2(k), h.coord.PromoteV2(k))
		target, _ := rollbackTarget(StateV2Primary)
		h.apply("rollback", h.ref.Rollback(k, target), h.coord.Rollback(k, target, "diff rollback"))
	}

	// --- 快照导入：低地板被忽略，高地板前进 ---
	for _, k := range keys {
		h.apply("import-stale", h.ref.ImportSnapshot(k, 1, 999),
			h.coord.ImportSnapshot(k, "stale-replica", 1, 999))
		h.apply("import-fresh", h.ref.ImportSnapshot(k, 7, 1234),
			h.coord.ImportSnapshot(k, "fresh-replica", 7, 1234))
	}

	// --- 有界重试：全部资源打到预算耗尽 ---
	for _, k := range keys {
		for attempt := 1; attempt <= DefaultMaxAttempts; attempt++ {
			refEx, refErr := h.ref.RecordRetry(k, attempt)
			gotEx, gotErr := h.coord.RecordRetry(k, attempt, attempt*10, "diff hotspot")
			h.apply("retry", refErr, gotErr)
			if refEx != gotEx {
				t.Fatalf("exhaustion flag divergence ref=%v got=%v", refEx, gotEx)
			}
		}
	}
	// 人工干预后恢复，继续走到 MIGRATED。
	for _, k := range keys {
		h.apply("reset", h.ref.ResetAfterIntervention(k),
			h.coord.ResetAfterIntervention(k, "diff manual repair"))
		h.apply("check2", h.ref.BeginCheck(k), h.coord.BeginCheck(k))
		h.apply("promote2", h.ref.PromoteV2(k), h.coord.PromoteV2(k))
		h.apply("complete", h.ref.Complete(k), h.coord.Complete(k))
	}
}

// TestDifferentialRandomizedWithCrashPoints 用确定性随机序列轰炸状态机，
// 并在多个步骤注入“强杀 + 重放”，重放后参考模型与重建投影必须逐字段相同。
func TestDifferentialRandomizedWithCrashPoints(t *testing.T) {
	h := newDiffHarness(t)
	defer h.close()
	rng := rand.New(rand.NewSource(20261123))
	keys := []ResourceKey{
		{TenantID: "t1", Prefix: "/k1"},
		{TenantID: "t1", Prefix: "/k2"},
		{TenantID: "t2", Prefix: "/k3"},
	}
	for _, k := range keys {
		h.apply("register", h.ref.Register(k), h.coord.Register(k))
	}
	crashPoints := map[int]bool{3: true, 7: true, 14: true, 25: true, 40: true}

	for step := 1; step <= 60; step++ {
		k := keys[rng.Intn(len(keys))]
		refState := StateLegacy
		if rr, err := h.ref.get(k); err == nil {
			refState = rr.state
		}
		var label string
		var refErr, gotErr error
		switch rng.Intn(11) {
		case 0:
			label = "begin"
			refErr, gotErr = h.ref.BeginMigration(k), h.coord.BeginMigration(k, "rand")
		case 1:
			label = "legacy-write"
			refErr, gotErr = h.ref.LegacyWrite(k), h.coord.LegacyWrite(k, "rand")
		case 2:
			label = "mirror"
			refErr, gotErr = h.ref.MirrorApplied(k), h.coord.MirrorApplied(k)
		case 3:
			label = "drain"
			refErr, gotErr = h.ref.Drain(k), h.coord.Drain(k)
		case 4:
			label = "check"
			refErr, gotErr = h.ref.BeginCheck(k), h.coord.BeginCheck(k)
		case 5:
			label = "promote"
			refErr, gotErr = h.ref.PromoteV2(k), h.coord.PromoteV2(k)
		case 6:
			label = "complete"
			refErr, gotErr = h.ref.Complete(k), h.coord.Complete(k)
		case 7:
			label = "rotate"
			floor := (uint64(rng.Intn(4)) + 1) * (uint64(step) + 1)
			refErr, gotErr = h.ref.RotateRevocation(k, floor),
				h.coord.RotateRevocation(k, floor, "rand rotate")
		case 8:
			label = "rollback"
			if target, ok := rollbackTarget(refState); ok {
				refErr, gotErr = h.ref.Rollback(k, target), h.coord.Rollback(k, target, "rand rollback")
			} else {
				refErr, gotErr = h.ref.Rollback(k, StateMigrated),
					h.coord.Rollback(k, StateMigrated, "rand illegal rollback")
			}
		case 9:
			label = "import"
			gen := uint64(rng.Intn(12))
			wm := uint64(rng.Intn(50))
			refErr, gotErr = h.ref.ImportSnapshot(k, gen, wm),
				h.coord.ImportSnapshot(k, "rand-replica", gen, wm)
		case 10:
			label = "retry"
			h.attempts[k]++
			attempt := h.attempts[k]
			if attempt > DefaultMaxAttempts {
				h.attempts[k] = DefaultMaxAttempts
				attempt = DefaultMaxAttempts
			}
			_, refErr = h.ref.RecordRetry(k, attempt)
			_, gotErr = h.coord.RecordRetry(k, attempt, attempt, "rand retry")
		}
		h.apply(label, refErr, gotErr)

		if crashPoints[step] {
			// 强杀注入：关闭句柄（丢弃内存态），从磁盘完整重建。
			h.reopen()
			if diffs := h.ref.DiffTo(h.coord); len(diffs) != 0 {
				t.Fatalf("after crash at step %d: divergence %+v", step, diffs)
			}
			// 哈希链必须仍然可验证。
			trail, err := h.coord.AuditTrail()
			if err != nil {
				t.Fatalf("audit after crash at step %d: %v", step, err)
			}
			if len(trail) == 0 {
				t.Fatalf("audit trail unexpectedly empty at step %d", step)
			}
		}
	}
}

// TestDifferentialTornWriteAtEveryPosition 在每个步骤后追加撕裂帧再重放，
// 参考模型（只含已提交语义）与重建投影必须始终一致。
func TestDifferentialTornWriteAtEveryPosition(t *testing.T) {
	h := newDiffHarness(t)
	defer h.close()
	k := ResourceKey{TenantID: "tt", Prefix: "/p"}
	h.apply("register", h.ref.Register(k), h.coord.Register(k))
	h.apply("begin", h.ref.BeginMigration(k), h.coord.BeginMigration(k, "torn"))

	for round := 1; round <= 6; round++ {
		h.apply("legacy", h.ref.LegacyWrite(k), h.coord.LegacyWrite(k, "torn"))
		h.apply("mirror", h.ref.MirrorApplied(k), h.coord.MirrorApplied(k))

		// 强杀 + 撕裂尾部：关闭句柄释放锁后，用新句柄写一帧未提交数据
		// 再关闭，模拟 kill -9 停在批量提交之前的磁盘状态。
		if err := h.coord.Close(); err != nil {
			t.Fatal(err)
		}
		h.closed = true
		store2, err := storage.Open(h.dir)
		if err != nil {
			t.Fatalf("reopen for torn injection: %v", err)
		}
		if err := store2.AppendTorn([][]byte{[]byte("torn-batch-body")}); err != nil {
			t.Fatal(err)
		}
		if err := store2.Close(); err != nil {
			t.Fatal(err)
		}
		h.reopen()
		if diffs := h.ref.DiffTo(h.coord); len(diffs) != 0 {
			t.Fatalf("round %d after torn write: %+v", round, diffs)
		}
	}
}

// TestDifferentialSnapshotThenReplay 覆盖快照导入/检查点与续放的组合：
// 检查点之后继续写事件，强杀重建后状态与参考模型一致且哈希链不断。
func TestDifferentialSnapshotThenReplay(t *testing.T) {
	h := newDiffHarness(t)
	defer h.close()
	k := ResourceKey{TenantID: "ts", Prefix: "/snap"}
	h.apply("register", h.ref.Register(k), h.coord.Register(k))
	h.apply("begin", h.ref.BeginMigration(k), h.coord.BeginMigration(k, "snap"))
	h.apply("rotate", h.ref.RotateRevocation(k, 6), h.coord.RotateRevocation(k, 6, "snap"))
	if _, err := h.coord.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	h.apply("legacy", h.ref.LegacyWrite(k), h.coord.LegacyWrite(k, "post checkpoint"))
	h.apply("mirror", h.ref.MirrorApplied(k), h.coord.MirrorApplied(k))
	h.apply("import", h.ref.ImportSnapshot(k, 8, 5), h.coord.ImportSnapshot(k, "peer", 8, 5))

	h.reopen()
	if diffs := h.ref.DiffTo(h.coord); len(diffs) != 0 {
		t.Fatalf("snapshot replay divergence: %+v", diffs)
	}
	trail, err := h.coord.AuditTrail()
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyChain(toEnvelopes(trail)); err != nil {
		t.Fatalf("chain broken after snapshot replay: %v", err)
	}
}

func toEnvelopes(in []AuditEvent) []Envelope {
	out := make([]Envelope, 0, len(in))
	for _, e := range in {
		out = append(out, Envelope{
			Seq: e.Seq, Kind: e.Kind, Payload: []byte(e.Payload),
			PrevHash: e.PrevHash, Hash: e.Hash,
		})
	}
	return out
}
