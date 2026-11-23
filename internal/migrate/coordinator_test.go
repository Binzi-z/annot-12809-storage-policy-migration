package migrate

import (
	"path/filepath"
	"testing"

	"multitenantmigration/internal/storage"
)

func openCoordinator(t *testing.T) (*Coordinator, *storage.Store, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "db")
	store, err := storage.Open(dir)
	if err != nil {
		t.Fatalf("storage open: %v", err)
	}
	c, err := Open(store)
	if err != nil {
		t.Fatalf("coordinator open: %v", err)
	}
	return c, store, dir
}

func reopenCoordinator(t *testing.T, dir string, opts ...storage.Option) *Coordinator {
	t.Helper()
	store, err := storage.Open(dir, opts...)
	if err != nil {
		t.Fatalf("storage reopen: %v", err)
	}
	c, err := Open(store)
	if err != nil {
		t.Fatalf("coordinator reopen: %v", err)
	}
	return c
}

var keyA = ResourceKey{TenantID: "tenant-acme", Prefix: "/billing"}

func TestFullMigrationLifecycle(t *testing.T) {
	c, _, _ := openCoordinator(t)
	defer c.Close()

	if err := c.Register(keyA); err != nil {
		t.Fatal(err)
	}
	if err := c.BeginMigration(keyA, "start"); err != nil {
		t.Fatal(err)
	}
	// 迁移开始后的旧端写入产生未收敛积压。
	if err := c.LegacyWrite(keyA, "invoice-1"); err != nil {
		t.Fatal(err)
	}
	r, _ := c.Resource(keyA)
	if r.PendingMirrors != 1 || r.Converged() {
		t.Fatalf("pending=%d converged=%v, want 1/false", r.PendingMirrors, r.Converged())
	}
	if err := c.MirrorApplied(keyA); err != nil {
		t.Fatal(err)
	}
	if r, _ = c.Resource(keyA); !r.Converged() {
		t.Fatalf("after mirror converged=%v want true", r.Converged())
	}
	if err := c.BeginCheck(keyA); err != nil {
		t.Fatal(err)
	}
	if err := c.PromoteV2(keyA); err != nil {
		t.Fatal(err)
	}
	if err := c.Complete(keyA); err != nil {
		t.Fatal(err)
	}
	if r, _ = c.Resource(keyA); r.State != StateMigrated {
		t.Fatalf("state=%s want MIGRATED", r.State)
	}
}

func TestGenerationFloorMonotonicAndRollbackProof(t *testing.T) {
	c, _, _ := openCoordinator(t)
	defer c.Close()
	if err := c.Register(keyA); err != nil {
		t.Fatal(err)
	}
	if err := c.BeginMigration(keyA, "start"); err != nil {
		t.Fatal(err)
	}
	if err := c.RotateRevocation(keyA, 5, "compromise response"); err != nil {
		t.Fatal(err)
	}
	// 地板只增不减。
	if err := c.RotateRevocation(keyA, 3, "forbidden decrease"); err == nil {
		t.Fatal("decreasing generation floor must be rejected")
	}
	if err := c.BeginCheck(keyA); err != nil {
		t.Fatal(err)
	}
	if err := c.PromoteV2(keyA); err != nil {
		t.Fatal(err)
	}
	// 回滚 V2_PRIMARY -> CHECK，世代地板必须原样保留。
	if err := c.Rollback(keyA, StateCheck, "v2 validation regression"); err != nil {
		t.Fatal(err)
	}
	r, _ := c.Resource(keyA)
	if r.GenFloor != 5 {
		t.Fatalf("gen floor after rollback=%d want 5", r.GenFloor)
	}
	// 非法回滚边必须拒绝。
	if err := c.Rollback(keyA, StateLegacy, "edge does not exist"); err == nil {
		t.Fatal("rollback CHECK->LEGACY must be rejected")
	}
}

func TestSnapshotImportCannotMoveFloorBackward(t *testing.T) {
	c, _, _ := openCoordinator(t)
	defer c.Close()
	if err := c.Register(keyA); err != nil {
		t.Fatal(err)
	}
	if err := c.RotateRevocation(keyA, 9, "local rotation"); err != nil {
		t.Fatal(err)
	}
	// “过期副本”带着更低的世代地板与水位追赶：地板不得被拉低。
	if err := c.ImportSnapshot(keyA, "stale-replica", 2, 100); err != nil {
		t.Fatal(err)
	}
	r, _ := c.Resource(keyA)
	if r.GenFloor != 9 {
		t.Fatalf("gen floor=%d want 9 (stale replica must not win)", r.GenFloor)
	}
	// 更高地板仍可前进。
	if err := c.ImportSnapshot(keyA, "fresh-replica", 11, 200); err != nil {
		t.Fatal(err)
	}
	if r, _ = c.Resource(keyA); r.GenFloor != 11 || r.LastMirrorSeq != 200 {
		t.Fatalf("after fresh import floor=%d watermark=%d", r.GenFloor, r.LastMirrorSeq)
	}
}

func TestRetryBudgetIsBoundedAndFailsState(t *testing.T) {
	c, _, _ := openCoordinator(t)
	defer c.Close()
	if err := c.Register(keyA); err != nil {
		t.Fatal(err)
	}
	if err := c.BeginMigration(keyA, "start"); err != nil {
		t.Fatal(err)
	}
	var exhausted bool
	for attempt := 1; attempt <= c.MaxAttempts(); attempt++ {
		var err error
		exhausted, err = c.RecordRetry(keyA, attempt, attempt*10, "hotspot conflict")
		if err != nil {
			t.Fatal(err)
		}
	}
	if !exhausted {
		t.Fatal("retry budget must report exhaustion")
	}
	r, _ := c.Resource(keyA)
	if r.State != StateFailed || r.Attempts != c.MaxAttempts() {
		t.Fatalf("state=%s attempts=%d", r.State, r.Attempts)
	}
	// FAILED 下继续记录重试不能无限增长，也不能再推进状态。
	if _, err := c.RecordRetry(keyA, c.MaxAttempts()+1, 9999, "more"); err != nil {
		// 允许因非法转移报错；关键是状态与计数不被破坏。
	}
	if r, _ = c.Resource(keyA); r.State != StateFailed || r.Attempts > c.MaxAttempts() {
		t.Fatalf("after over-budget state=%s attempts=%d", r.State, r.Attempts)
	}
	if err := c.ResetAfterIntervention(keyA, "manual repair"); err != nil {
		t.Fatal(err)
	}
	if r, _ = c.Resource(keyA); r.State != StateDualWriting {
		t.Fatalf("after reset state=%s", r.State)
	}
}

func TestCrashReplayRebuildsIdenticalState(t *testing.T) {
	c, _, dir := openCoordinator(t)
	if err := c.Register(keyA); err != nil {
		t.Fatal(err)
	}
	if err := c.BeginMigration(keyA, "start"); err != nil {
		t.Fatal(err)
	}
	if err := c.LegacyWrite(keyA, "w1"); err != nil {
		t.Fatal(err)
	}
	if err := c.MirrorApplied(keyA); err != nil {
		t.Fatal(err)
	}
	if err := c.RotateRevocation(keyA, 3, "rotation-before-crash"); err != nil {
		t.Fatal(err)
	}
	before := c.Resources()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	c2 := reopenCoordinator(t, dir)
	defer c2.Close()
	after := c2.Resources()
	if len(before) != len(after) {
		t.Fatalf("resource count %d vs %d", len(before), len(after))
	}
	for i := range before {
		if before[i] != after[i] {
			t.Fatalf("state drifted across crash:\nbefore=%+v\nafter =%+v", before[i], after[i])
		}
	}
	// 哈希链在重放后仍可独立验证。
	trail, err := c2.AuditTrail()
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	if len(trail) == 0 {
		t.Fatal("audit trail empty")
	}
}

func TestTornWriteAfterCrashIsDroppedNotApplied(t *testing.T) {
	c, _, dir := openCoordinator(t)
	if err := c.Register(keyA); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	// 模拟强杀：用新句柄只留下没有 commit 位的撕裂帧后立即关闭。
	store2, err := storage.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store2.AppendTorn([][]byte{[]byte("not-a-valid-envelope")}); err != nil {
		t.Fatal(err)
	}
	if err := store2.Close(); err != nil {
		t.Fatal(err)
	}
	c2 := reopenCoordinator(t, dir)
	defer c2.Close()
	if r, ok := c2.Resource(keyA); !ok || r.State != StateLegacy {
		t.Fatalf("registered resource lost: %+v ok=%v", r, ok)
	}
}

func TestSnapshotAcceleratesReplayAndStaysConsistent(t *testing.T) {
	c, _, dir := openCoordinator(t)
	if err := c.Register(keyA); err != nil {
		t.Fatal(err)
	}
	if err := c.BeginMigration(keyA, "start"); err != nil {
		t.Fatal(err)
	}
	if err := c.RotateRevocation(keyA, 4, "pre-snapshot"); err != nil {
		t.Fatal(err)
	}
	seq, err := c.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if seq == 0 {
		t.Fatal("checkpoint seq must be positive")
	}
	// 快照之后再写事件，重启时走“快照 + 续放”路径。
	if err := c.LegacyWrite(keyA, "post-snapshot-write"); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c2 := reopenCoordinator(t, dir)
	defer c2.Close()
	r, ok := c2.Resource(keyA)
	if !ok {
		t.Fatal("resource missing after snapshot+replay")
	}
	if r.GenFloor != 4 || r.LastLegacySeq != 1 || r.PendingMirrors != 1 {
		t.Fatalf("rebuilt state wrong: %+v", r)
	}
}

func TestTamperedCommittedRecordIsDetected(t *testing.T) {
	c, _, dir := openCoordinator(t)
	if err := c.Register(keyA); err != nil {
		t.Fatal(err)
	}
	if err := c.BeginMigration(keyA, "start"); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	// 在已提交区域制造位腐：CRC 必须检出，普通打开拒绝启动。
	store0, err := storage.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	wal := store0.WALPath()
	if err := store0.Close(); err != nil {
		t.Fatal(err)
	}
	if err := storage.FlipByte(wal, 40); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.Open(dir); !storage.IsCorruption(err) {
		t.Fatalf("plain open on corruption err=%v want ErrCorruption", err)
	}

	// salvage 模式可抢救前缀并显式只读。
	salvageStore, err := storage.Open(dir, storage.WithSalvage())
	if err != nil {
		t.Fatalf("salvage open: %v", err)
	}
	c2, err := Open(salvageStore)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	if !c2.ReadOnly() {
		t.Fatal("salvaged coordinator must be read-only")
	}
	if err := c2.Register(ResourceKey{TenantID: "x", Prefix: "/y"}); err == nil {
		t.Fatal("writes must be rejected in salvage mode")
	}
}

func TestIllegalTransitionsRejected(t *testing.T) {
	c, _, _ := openCoordinator(t)
	defer c.Close()
	if err := c.Register(keyA); err != nil {
		t.Fatal(err)
	}
	// LEGACY 不能直接跳到 MIGRATED。
	if err := c.Complete(keyA); err == nil {
		t.Fatal("LEGACY->MIGRATED must be rejected")
	}
	if err := c.BeginMigration(keyA, "start"); err != nil {
		t.Fatal(err)
	}
	if err := c.Complete(keyA); err == nil {
		t.Fatal("DUAL_WRITING->MIGRATED must be rejected")
	}
}
