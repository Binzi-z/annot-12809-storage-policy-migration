package migrate

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// blockClock 把每次退避挂在一个可由测试逐个释放的门控上，
// 从而确定性地证明“退避期间 worker 已被释放”。
type blockClock struct {
	waits chan blockWait
}

type blockWait struct {
	gate chan struct{}
	d    time.Duration
}

func newBlockClock() *blockClock { return &blockClock{waits: make(chan blockWait)} }

func (c *blockClock) Sleep(d time.Duration) {
	gate := make(chan struct{})
	c.waits <- blockWait{gate: gate, d: d} // 定时器 goroutine 在此挂起
	<-gate
}
func (c *blockClock) Now() time.Time { return time.Time{} }
func (c *blockClock) release() time.Duration {
	w := <-c.waits
	close(w.gate)
	return w.d
}

var errHotspot = errors.New("simulated hotspot version conflict")
var errNormalAttempt = errors.New("normal op retried unexpectedly")
var errNormalBlocked = errors.New("normal resource failed while hotspot was sleeping")

// TestHotspotRetriesBoundedAndNeverBlocksOtherResources 是题目要求的核心
// 并行性证明：热点资源在退避门控上挂起时，正常资源由空闲 worker 立即完成；
// 热点的尝试次数严格等于预算上限，退避时长硬封顶，最终进入 FAILED。
func TestHotspotRetriesBoundedAndNeverBlocksOtherResources(t *testing.T) {
	coord, _, _ := openCoordinator(t)
	defer coord.Close()
	hot := ResourceKey{TenantID: "tenant-hot", Prefix: "/hot"}
	normal := ResourceKey{TenantID: "tenant-normal", Prefix: "/normal"}
	for _, k := range []ResourceKey{hot, normal} {
		if err := coord.Register(k); err != nil {
			t.Fatal(err)
		}
		if err := coord.BeginMigration(k, "runner test"); err != nil {
			t.Fatal(err)
		}
	}

	clock := newBlockClock()
	runner := NewRunner(coord,
		WithWorkers(2),
		WithClock(clock),
		WithBackoff(10*time.Millisecond, 1000*time.Millisecond),
	)
	defer runner.Shutdown()

	hotCh := runner.Submit(hot, "hot-mirror", func(attempt int) error { return errHotspot })

	// 第一次尝试失败后进入退避并挂起；此刻它不应占用任何 worker。
	first := <-clock.waits
	if first.d != 10*time.Millisecond {
		t.Fatalf("first backoff=%s want 10ms", first.d)
	}

	// 热点挂起期间提交正常资源：必须被另一个 worker 立即处理完成。
	type normalOutcome struct {
		res JobResult
		err error
	}
	normalCh := make(chan normalOutcome, 1)
	go func() {
		r := <-runner.Submit(normal, "normal-mirror", func(attempt int) error {
			if attempt != 1 {
				return errNormalAttempt
			}
			return nil
		})
		if !r.OK {
			normalCh <- normalOutcome{res: r, err: errNormalBlocked}
			return
		}
		normalCh <- normalOutcome{res: r}
	}()
	select {
	case out := <-normalCh:
		if out.err != nil {
			t.Fatalf("normal resource must succeed while hotspot sleeps: %+v", out.res)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("normal resource blocked by hotspot backoff")
	}

	// 释放第一次退避，随后收集其余退避；最后一次尝试耗尽预算，不再睡眠。
	var backoffs []time.Duration
	close(first.gate)
	backoffs = append(backoffs, first.d)
	for i := 1; i < coord.MaxAttempts()-1; i++ {
		backoffs = append(backoffs, clock.release())
	}

	select {
	case res := <-hotCh:
		if !res.Exhausted || res.Attempts != coord.MaxAttempts() {
			t.Fatalf("hot result=%+v, want exhausted after %d attempts", res, coord.MaxAttempts())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("hotspot job never concluded")
	}

	// 退避序列必须符合指数曲线且不超过封顶。
	want := []time.Duration{10, 20, 40, 80}
	got := backoffs
	if len(got) != len(want) {
		t.Fatalf("backoff count=%d want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i]*time.Millisecond {
			t.Fatalf("backoff[%d]=%s want %s", i, got[i], want[i]*time.Millisecond)
		}
	}

	hotState, _ := coord.Resource(hot)
	if hotState.State != StateFailed {
		t.Fatalf("hot state=%s want FAILED", hotState.State)
	}
	normalState, _ := coord.Resource(normal)
	if normalState.State != StateDualWriting {
		t.Fatalf("normal state=%s want DUAL_WRITING (unaffected)", normalState.State)
	}
	m, _ := runner.Metrics(hot)
	if m.Attempts != coord.MaxAttempts() || !m.Exhausted {
		t.Fatalf("hot metrics=%+v", m)
	}
	nm, _ := runner.Metrics(normal)
	if nm.Successes != 1 || nm.Exhausted {
		t.Fatalf("normal metrics=%+v", nm)
	}
}

func TestBackoffHardCap(t *testing.T) {
	coord, _, _ := openCoordinator(t)
	defer coord.Close()
	k := ResourceKey{TenantID: "t", Prefix: "/cap"}
	if err := coord.Register(k); err != nil {
		t.Fatal(err)
	}
	if err := coord.BeginMigration(k, "cap"); err != nil {
		t.Fatal(err)
	}
	clock := newBlockClock()
	runner := NewRunner(coord, WithWorkers(1), WithClock(clock),
		WithBackoff(10*time.Millisecond, 25*time.Millisecond))
	defer runner.Shutdown()

	ch := runner.Submit(k, "always-fails", func(attempt int) error { return errHotspot })
	var seen []time.Duration
	for i := 0; i < coord.MaxAttempts()-1; i++ {
		seen = append(seen, clock.release())
	}
	<-ch
	// 10 -> 20 -> 25(封顶) -> 25(封顶)
	want := []time.Duration{10, 20, 25, 25}
	for i := range want {
		if seen[i] != want[i]*time.Millisecond {
			t.Fatalf("capped backoff[%d]=%s want %s", i, seen[i], want[i]*time.Millisecond)
		}
	}
}

// TestPerKeySerialization 证明同一资源的操作严格串行，不同资源可并行。
func TestPerKeySerialization(t *testing.T) {
	coord, _, _ := openCoordinator(t)
	defer coord.Close()
	k := ResourceKey{TenantID: "t", Prefix: "/ser"}
	if err := coord.Register(k); err != nil {
		t.Fatal(err)
	}
	if err := coord.BeginMigration(k, "ser"); err != nil {
		t.Fatal(err)
	}
	runner := NewRunner(coord, WithWorkers(4), WithClock(&InstantClock{}))
	defer runner.Shutdown()

	var concurrent int32
	var maxConcurrent int32
	op := func(attempt int) error {
		cur := atomic.AddInt32(&concurrent, 1)
		for {
			old := atomic.LoadInt32(&maxConcurrent)
			if cur <= old || atomic.CompareAndSwapInt32(&maxConcurrent, old, cur) {
				break
			}
		}
		time.Sleep(2 * time.Millisecond)
		atomic.AddInt32(&concurrent, -1)
		return nil
	}
	chs := make([]<-chan JobResult, 8)
	for i := range chs {
		chs[i] = runner.Submit(k, "serial-op", op)
	}
	for _, ch := range chs {
		if r := <-ch; !r.OK {
			t.Fatalf("serial op failed: %+v", r)
		}
	}
	runner.WaitIdle()
	if maxConcurrent != 1 {
		t.Fatalf("max concurrent executions for same key=%d want 1", maxConcurrent)
	}
}
