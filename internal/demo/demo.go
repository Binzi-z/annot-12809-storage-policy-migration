// Package demo 编排端到端演示场景：全部使用模拟租户与本地嵌入式存储，
// 不访问网络。每个场景都打印步骤与判定，并在不变量被违反时返回错误，
// 因此这些场景同时可被 go test 直接驱动。
package demo

import (
	"fmt"
	"io"
	"path/filepath"
	"time"

	"multitenantmigration/internal/migrate"
	"multitenantmigration/internal/policy"
	"multitenantmigration/internal/storage"
	"multitenantmigration/internal/tenantsim"
)

// Config 控制演示运行。
type Config struct {
	DataDir     string
	Workers     int
	MaxAttempts int
	Out         io.Writer
}

// Run 执行全部演示场景，返回每个场景的标题与错误（nil 表示通过）。
func Run(cfg Config) ([]ScenarioResult, error) {
	if cfg.Out == nil {
		cfg.Out = io.Discard
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 2
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = migrate.DefaultMaxAttempts
	}
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("demo: DataDir is required")
	}
	out := cfg.Out
	fmt.Fprintf(out, "== 多租户策略迁移演示（纯本地进程，数据目录 %s）==\n\n", cfg.DataDir)

	results := make([]ScenarioResult, 0, 6)
	run := func(title string, fn func() error) {
		fmt.Fprintf(out, "---- 场景：%s ----\n", title)
		err := fn()
		status := "PASS"
		if err != nil {
			status = "FAIL: " + err.Error()
		}
		fmt.Fprintf(out, "=> %s\n\n", status)
		results = append(results, ScenarioResult{Title: title, Err: err})
	}

	dir := func(name string) string { return filepath.Join(cfg.DataDir, name) }

	run("1. 热点资源与正常资源并行迁移（有界重试、互不阻塞）", func() error {
		return scenarioHotspot(dir("01-hotspot"), cfg)
	})
	run("2. 双版本交集闸门：任何时刻不扩大权限", func() error {
		return scenarioIntersection(dir("02-intersection"), cfg)
	})
	run("3. 撤销世代：回滚与过期副本追赶都无法恢复权限", func() error {
		return scenarioRevocation(dir("03-revocation"), cfg)
	})
	run("4. 三态区分：普通拒绝 / 状态未收敛 / 数据损坏", func() error {
		return scenarioThreeKinds(dir("04-three-kinds"), cfg)
	})
	run("5. 强杀后的 WAL 重放：状态逐字节重建", func() error {
		return scenarioCrashReplay(dir("05-crash"), cfg)
	})
	run("6. 哈希链审计与快照导入的安全语义", func() error {
		return scenarioAuditAndImport(dir("06-audit"), cfg)
	})
	return results, nil
}

// ScenarioResult 是单场景结果。
type ScenarioResult struct {
	Title string
	Err   error
}

func openEnv(dir string) (*migrate.Coordinator, *tenantsim.Environment, *storage.Store, error) {
	store, err := storage.Open(dir)
	if err != nil {
		return nil, nil, nil, err
	}
	coord, err := migrate.Open(store)
	if err != nil {
		_ = store.Close()
		return nil, nil, nil, err
	}
	env := tenantsim.New(coord)
	return coord, env, store, nil
}

func mustCheck(cond bool, msg string) error {
	if !cond {
		return fmt.Errorf("invariant violated: %s", msg)
	}
	return nil
}

// scenarioHotspot 演示并行迁移：热点资源持续冲突、重试有界并进入 FAILED；
// 正常资源在同一 worker 池内不受影响地完成迁移。
func scenarioHotspot(dir string, cfg Config) error {
	out := cfg.Out
	coord, env, store, err := openEnv(dir)
	if err != nil {
		return err
	}
	defer store.Close()

	const tenant = "tenant-shop"
	hot := migrate.ResourceKey{TenantID: tenant, Prefix: "/hot-orders"}
	normal := migrate.ResourceKey{TenantID: tenant, Prefix: "/catalog"}
	if err := env.AddTenant(&tenantsim.Tenant{
		ID: tenant, Roles: []string{"writer", "reader"},
		Prefixes: []string{hot.Prefix, normal.Prefix},
	}); err != nil {
		return err
	}
	for _, k := range []migrate.ResourceKey{hot, normal} {
		if err := coord.Register(k); err != nil {
			return err
		}
		if err := coord.BeginMigration(k, "demo start"); err != nil {
			return err
		}
	}
	coord.SetMaxAttempts(cfg.MaxAttempts)

	runner := migrate.NewRunner(coord,
		migrate.WithWorkers(cfg.Workers),
		migrate.WithBackoff(5*time.Millisecond, 80*time.Millisecond))
	defer runner.Shutdown()

	// 热点：镜像步骤始终冲突；正常：首次尝试即成功并一路推进。
	hotCh := runner.Submit(hot, "mirror-hot", func(attempt int) error {
		fmt.Fprintf(out, "  热点 %s 第 %d 次尝试：版本冲突\n", hot.Prefix, attempt)
		return fmt.Errorf("hotspot version conflict")
	})
	normalCh := runner.Submit(normal, "mirror-normal", func(attempt int) error {
		fmt.Fprintf(out, "  正常 %s 第 %d 次尝试：镜像成功\n", normal.Prefix, attempt)
		return nil
	})

	hotRes := <-hotCh
	normalRes := <-normalCh
	runner.WaitIdle()

	fmt.Fprintf(out, "  热点结果: 尝试 %d 次, 预算耗尽=%v\n", hotRes.Attempts, hotRes.Exhausted)
	fmt.Fprintf(out, "  正常结果: 尝试 %d 次, 成功=%v\n", normalRes.Attempts, normalRes.OK)
	if err := mustCheck(hotRes.Exhausted, "热点重试必须有界并耗尽预算"); err != nil {
		return err
	}
	if err := mustCheck(hotRes.Attempts == coord.MaxAttempts(),
		fmt.Sprintf("热点尝试次数 %d 必须等于预算 %d", hotRes.Attempts, coord.MaxAttempts())); err != nil {
		return err
	}
	if err := mustCheck(normalRes.OK && normalRes.Attempts == 1, "正常资源必须首次成功且不被阻塞"); err != nil {
		return err
	}
	hotState, _ := coord.Resource(hot)
	normalState, _ := coord.Resource(normal)
	fmt.Fprintf(out, "  最终状态: 热点=%s, 正常=%s\n", hotState.State, normalState.State)
	if err := mustCheck(hotState.State == migrate.StateFailed, "热点进入 FAILED 冻结写"); err != nil {
		return err
	}
	if err := mustCheck(normalState.State == migrate.StateDualWriting, "正常资源保持可推进"); err != nil {
		return err
	}

	// 人工处置后热点可恢复，证明 FAILED 不是死局而是显式安全态。
	if err := coord.ResetAfterIntervention(hot, "ops resolved conflict"); err != nil {
		return err
	}
	recoveredCh := runner.Submit(hot, "mirror-hot-retry", func(attempt int) error { return nil })
	if r := <-recoveredCh; !r.OK {
		return fmt.Errorf("recovered hotspot must succeed: %+v", r)
	}
	hotState, _ = coord.Resource(hot)
	return mustCheck(hotState.State == migrate.StateDualWriting, "人工恢复后回到双写态")
}

// scenarioIntersection 证明交集闸门不扩权。
func scenarioIntersection(dir string, cfg Config) error {
	out := cfg.Out
	coord, env, store, err := openEnv(dir)
	if err != nil {
		return err
	}
	defer store.Close()
	const tenant = "tenant-pay"
	key := migrate.ResourceKey{TenantID: tenant, Prefix: "/payments"}
	if err := env.AddTenant(&tenantsim.Tenant{
		ID: tenant, Roles: []string{"writer", "reader"}, Prefixes: []string{key.Prefix},
	}); err != nil {
		return err
	}
	if err := coord.Register(key); err != nil {
		return err
	}
	engine := &policy.Engine{V1: env, V2: env, Mig: coord.View()}
	req := policy.AccessRequest{
		TenantID: tenant, Role: "writer", Path: key.Prefix + "/2026/11",
		Action: policy.ActionWrite, ObservedVersion: 1,
	}

	// LEGACY 阶段：仅 V1 授权，写允许。
	if err := engine.Decide(req); err != nil {
		return fmt.Errorf("legacy write should pass: %w", err)
	}
	if err := coord.BeginMigration(key, "dual write begins"); err != nil {
		return err
	}
	// 旧端先产生一笔写入而 V2 尚未镜像：此刻水位未追平。
	if err := coord.LegacyWrite(key, "payment record"); err != nil {
		return err
	}
	// 双写但未收敛 => NotConverged（fail-closed 暂时态）。
	if err := engine.Decide(req); !policy.IsNotConverged(err) {
		return fmt.Errorf("unconverged write err=%v want NotConverged", err)
	}
	fmt.Fprintf(out, "  未收敛写请求被正确判定为 NotConverged: %v\n", err)
	if err := coord.MirrorApplied(key); err != nil {
		return err
	}
	if err := coord.Drain(key); err != nil {
		return err
	}
	// 收敛后交集允许。
	if err := engine.Decide(req); err != nil {
		return fmt.Errorf("converged write should pass: %w", err)
	}
	// 收窄 V2：V2 拒绝写 => 交集必须拒绝，即使 V1 仍允许。
	for _, r := range env.V2Rules(tenant) {
		if r.Role == "writer" {
			r.CanWrite = false
			env.UpdateV2Rule(r)
		}
	}
	if err := engine.Decide(req); !policy.IsDenied(err) {
		return fmt.Errorf("V2 narrowed, intersection err=%v want Denied", err)
	}
	fmt.Fprintf(out, "  V2 收窄后交集闸门拒绝（V1 仍允许）：%v\n", err)
	return nil
}

// scenarioRevocation 证明撤销不可经回滚或副本追赶恢复。
func scenarioRevocation(dir string, cfg Config) error {
	out := cfg.Out
	coord, env, store, err := openEnv(dir)
	if err != nil {
		return err
	}
	defer store.Close()
	const tenant = "tenant-secret"
	key := migrate.ResourceKey{TenantID: tenant, Prefix: "/secrets"}
	if err := env.AddTenant(&tenantsim.Tenant{
		ID: tenant, Roles: []string{"writer"}, Prefixes: []string{key.Prefix},
	}); err != nil {
		return err
	}
	if err := coord.Register(key); err != nil {
		return err
	}
	if err := coord.BeginMigration(key, "migrating"); err != nil {
		return err
	}
	engine := &policy.Engine{V1: env, V2: env, Mig: coord.View()}
	req := policy.AccessRequest{TenantID: tenant, Role: "writer", Path: key.Prefix + "/vault", Action: policy.ActionRead}

	// 撤销：世代 1 -> 4，并在协调器落下只增不减的世代地板。
	if err := coord.RotateRevocation(key, 4, "key compromise"); err != nil {
		return err
	}
	env.Revoke(tenant, key.Prefix, 4)
	if err := engine.Decide(req); !policy.IsDenied(err) {
		return fmt.Errorf("after revocation err=%v want Denied", err)
	}
	fmt.Fprintf(out, "  撤销世代抬到 4 后访问被拒绝：%v\n", err)

	// 攻击面 A：状态机回滚。回滚只退状态，世代地板保持 4。
	if err := coord.BeginCheck(key); err != nil {
		return err
	}
	if err := coord.PromoteV2(key); err != nil {
		return err
	}
	if err := coord.Rollback(key, migrate.StateCheck, "simulate rollback after revoke"); err != nil {
		return err
	}
	rs, _ := coord.Resource(key)
	if err := mustCheck(rs.GenFloor == 4, "回滚后世代地板必须仍为 4"); err != nil {
		return err
	}
	if err := engine.Decide(req); !policy.IsDenied(err) {
		return fmt.Errorf("rollback reinstated access err=%v want Denied", err)
	}
	fmt.Fprintf(out, "  回滚 V2_PRIMARY->CHECK 后访问仍被拒绝（地板=%d）\n", rs.GenFloor)

	// 攻击面 B：过期副本把世代 1 的旧 V2 规则追赶回来。
	accepted, rejected := env.ReplicateStaleRulesFromV1(tenant)
	fmt.Fprintf(out, "  过期副本追赶：被 V2 端拒绝 %d 条、接受 %d 条\n", rejected, accepted)
	if err := mustCheck(rejected > 0 && accepted == 0, "低于地板的副本写入必须全部拒绝"); err != nil {
		return err
	}
	if err := engine.Decide(req); !policy.IsDenied(err) {
		return fmt.Errorf("stale replica reinstated access err=%v want Denied", err)
	}
	fmt.Fprintf(out, "  副本追赶后访问仍被拒绝\n")
	return nil
}

// scenarioThreeKinds 一次性展示三类可区分结果与损坏后的 fail-closed。
func scenarioThreeKinds(dir string, cfg Config) error {
	out := cfg.Out
	coord, env, store, err := openEnv(dir)
	if err != nil {
		return err
	}
	defer store.Close()
	const tenant = "tenant-ops"
	key := migrate.ResourceKey{TenantID: tenant, Prefix: "/ops"}
	if err := env.AddTenant(&tenantsim.Tenant{
		ID: tenant, Roles: []string{"writer", "reader"}, Prefixes: []string{key.Prefix},
	}); err != nil {
		return err
	}
	if err := coord.Register(key); err != nil {
		return err
	}
	engine := &policy.Engine{V1: env, V2: env, Mig: coord.View()}

	// 普通拒绝：reader 尝试写。
	deniedReq := policy.AccessRequest{TenantID: tenant, Role: "reader", Path: key.Prefix + "/x", Action: policy.ActionWrite}
	if err := engine.Decide(deniedReq); !policy.IsDenied(err) {
		return fmt.Errorf("reader write err=%v want Denied", err)
	}
	fmt.Fprintf(out, "  普通拒绝（reader 写）：%v\n", err)

	// 未收敛：进入双写但尚未镜像。
	if err := coord.BeginMigration(key, "dual"); err != nil {
		return err
	}
	if err := coord.LegacyWrite(key, "ops event"); err != nil {
		return err
	}
	ncReq := policy.AccessRequest{TenantID: tenant, Role: "writer", Path: key.Prefix + "/x", Action: policy.ActionRead}
	if err := engine.Decide(ncReq); !policy.IsNotConverged(err) {
		return fmt.Errorf("pending mirror err=%v want NotConverged", err)
	}
	fmt.Fprintf(out, "  状态未收敛（镜像积压）：%v\n", err)

	// 数据损坏：位腐已提交 WAL 的“中间帧”，salvage 抢救前缀后隔离该资源。
	wal := store.WALPath()
	offsets, err := storage.FrameOffsets(wal)
	if err != nil {
		return err
	}
	if len(offsets) < 2 {
		return fmt.Errorf("expected >=2 committed frames, got %d", len(offsets))
	}
	if err := store.Close(); err != nil {
		return err
	}
	// 翻转第二个帧载荷首字节：第一个帧（资源注册）仍可抢救。
	corruptAt := offsets[1] + int64(20) // 跳过帧头，落在载荷区
	if err := storage.FlipByte(wal, corruptAt); err != nil {
		return err
	}
	salvageStore, err := storage.Open(dir, storage.WithSalvage())
	if err != nil {
		return err
	}
	salvageCoord, err := migrate.Open(salvageStore)
	if err != nil {
		return err
	}
	defer salvageStore.Close()
	if !salvageCoord.ReadOnly() {
		return fmt.Errorf("salvage coordinator must be read-only")
	}
	if rs, ok := salvageCoord.Resource(key); !ok {
		return fmt.Errorf("salvage must preserve prefix events: resource %s missing", key)
	} else {
		fmt.Fprintf(out, "  salvage 抢救出损坏点之前的状态：%s=%s\n", key.Prefix, rs.State)
	}
	if err := salvageCoord.Quarantine(key, "WAL CRC failure"); err != nil {
		return err
	}
	salvageEngine := &policy.Engine{V1: env, V2: env, Mig: salvageCoord.View()}
	if err := salvageEngine.Decide(ncReq); !policy.IsCorrupted(err) {
		return fmt.Errorf("quarantined access err=%v want Corrupted", err)
	}
	fmt.Fprintf(out, "  数据损坏（salvage + 隔离）：%v\n", err)
	return nil
}

// scenarioCrashReplay 演示强杀后仅凭磁盘重建出完全相同的状态。
func scenarioCrashReplay(dir string, cfg Config) error {
	out := cfg.Out
	coord, env, store, err := openEnv(dir)
	if err != nil {
		return err
	}
	const tenant = "tenant-crash"
	key := migrate.ResourceKey{TenantID: tenant, Prefix: "/journal"}
	if err := env.AddTenant(&tenantsim.Tenant{
		ID: tenant, Roles: []string{"writer"}, Prefixes: []string{key.Prefix},
	}); err != nil {
		return err
	}
	if err := coord.Register(key); err != nil {
		return err
	}
	if err := coord.BeginMigration(key, "pre-crash"); err != nil {
		return err
	}
	for i := 0; i < 3; i++ {
		if err := coord.LegacyWrite(key, fmt.Sprintf("entry-%d", i)); err != nil {
			return err
		}
		if err := coord.MirrorApplied(key); err != nil {
			return err
		}
	}
	if err := coord.RotateRevocation(key, 2, "pre-crash rotation"); err != nil {
		return err
	}
	before := coord.Resources()
	if _, err := coord.Checkpoint(); err != nil {
		return err
	}
	// 模拟 kill -9：不做优雅清理，直接丢弃句柄（Windows 下需 Close 释放锁，
	// 语义等价于进程终止后锁随句柄消失）。
	if err := store.Close(); err != nil {
		return err
	}

	store2, err := storage.Open(dir)
	if err != nil {
		return err
	}
	defer store2.Close()
	coord2, err := migrate.Open(store2)
	if err != nil {
		return err
	}
	after := coord2.Resources()
	if len(before) != len(after) || before[0] != after[0] {
		return fmt.Errorf("replayed state differs\nbefore=%+v\nafter =%+v", before, after)
	}
	trail, err := coord2.AuditTrail()
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "  强杀后重放 %d 条审计事件，资源状态完全一致：%s floor=%d pending=%d\n",
		len(trail), after[0].State, after[0].GenFloor, after[0].PendingMirrors)
	return nil
}

// scenarioAuditAndImport 展示哈希链审计与快照导入的只增语义。
func scenarioAuditAndImport(dir string, cfg Config) error {
	out := cfg.Out
	coord, _, store, err := openEnv(dir)
	if err != nil {
		return err
	}
	defer store.Close()
	key := migrate.ResourceKey{TenantID: "tenant-audit", Prefix: "/ledger"}
	if err := coord.Register(key); err != nil {
		return err
	}
	if err := coord.BeginMigration(key, "audit"); err != nil {
		return err
	}
	if err := coord.RotateRevocation(key, 3, "rotation-a"); err != nil {
		return err
	}
	if err := coord.ImportSnapshot(key, "peer-snapshot", 2, 50); err != nil { // 低地板
		return err
	}
	if err := coord.ImportSnapshot(key, "peer-snapshot", 7, 80); err != nil { // 高地板
		return err
	}
	trail, err := coord.AuditTrail()
	if err != nil {
		return err
	}
	if err := migrate.VerifyChain(toEnvelopes(trail)); err != nil {
		return err
	}
	summary, _ := coord.AuditSummary()
	fmt.Fprintf(out, "  哈希链验证通过，共 %d 条事件；类型计数：%v\n", len(trail), summary)
	rs, _ := coord.Resource(key)
	if err := mustCheck(rs.GenFloor == 7, "导入后地板应取高值 7"); err != nil {
		return err
	}
	if err := mustCheck(rs.LastMirrorSeq == 80, "水位应前进到 80"); err != nil {
		return err
	}
	fmt.Fprintf(out, "  低地板导入被忽略，高地板导入生效：floor=%d watermark=%d\n",
		rs.GenFloor, rs.LastMirrorSeq)
	return nil
}

func toEnvelopes(in []migrate.AuditEvent) []migrate.Envelope {
	out := make([]migrate.Envelope, 0, len(in))
	for _, e := range in {
		out = append(out, migrate.Envelope{
			Seq: e.Seq, Kind: e.Kind, Payload: []byte(e.Payload),
			PrevHash: e.PrevHash, Hash: e.Hash,
		})
	}
	return out
}
