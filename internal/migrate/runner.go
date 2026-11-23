package migrate

import (
	"errors"
	"sync"
	"time"
)

// ErrRunnerStopped 表示作业在退避等待期间运行器被关闭，重试不再继续。
var ErrRunnerStopped = errors.New("migrate: runner stopped before retry could run")

// Clock 抽象等待，测试中可换成零等待时钟以获得确定性。
type Clock interface {
	Sleep(d time.Duration)
	Now() time.Time
}

type realClock struct{}

func (realClock) Sleep(d time.Duration) { time.Sleep(d) }
func (realClock) Now() time.Time        { return time.Now() }

// InstantClock 是测试时钟：Sleep 立即返回，但记录被请求的退避时长。
type InstantClock struct {
	mu    sync.Mutex
	slept []time.Duration
}

func (c *InstantClock) Sleep(d time.Duration) {
	c.mu.Lock()
	c.slept = append(c.slept, d)
	c.mu.Unlock()
}

func (c *InstantClock) Now() time.Time { return time.Time{} }

// Slept 返回累计请求过的退避切片副本。
func (c *InstantClock) Slept() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.slept...)
}

// KeyMetrics 是单个资源的调度可观测数据，也是“重试有界”的证据来源。
type KeyMetrics struct {
	Attempts    int
	Retries     int
	MaxBackoff  time.Duration
	FirstSubmit time.Time
	LastFinish  time.Time
	Exhausted   bool
	Successes   int
}

// JobResult 通过每个提交自带的缓冲通道返回。
type JobResult struct {
	Key       ResourceKey
	Name      string
	OK        bool
	Attempts  int
	Exhausted bool
	Err       error
}

type job struct {
	key     ResourceKey
	name    string
	attempt int
	op      func(attempt int) error
	result  chan JobResult
}

// Runner 是有界重试的每资源串行执行器：
//   - 固定 worker 池；一个资源同一时刻最多占用一个 worker（per-key 锁）；
//   - 退避通过独立定时器 goroutine 实现，等待期间 worker 立即归还池，
//     因此热点资源的长退避不会阻塞其他资源；
//   - 重试次数硬上界 = Coordinator.MaxAttempts，指数退避封顶 maxBackoff。
type Runner struct {
	coord     *Coordinator
	workers   int
	clock     Clock
	base, max time.Duration

	queue  chan job
	stop   chan struct{}
	wg     sync.WaitGroup
	pendWg sync.WaitGroup

	mu       sync.Mutex
	keyLocks map[ResourceKey]*sync.Mutex
	metrics  map[ResourceKey]*KeyMetrics
	closed   bool
}

// RunnerOption 配置 Runner。
type RunnerOption func(*Runner)

// WithWorkers 设置 worker 池大小。
func WithWorkers(n int) RunnerOption {
	return func(r *Runner) {
		if n > 0 {
			r.workers = n
		}
	}
}

// WithClock 注入时钟（测试用）。
func WithClock(c Clock) RunnerOption { return func(r *Runner) { r.clock = c } }

// WithBackoff 设置退避基数与封顶。
func WithBackoff(base, max time.Duration) RunnerOption {
	return func(r *Runner) { r.base, r.max = base, max }
}

// NewRunner 创建并启动运行器。
func NewRunner(c *Coordinator, opts ...RunnerOption) *Runner {
	r := &Runner{
		coord:    c,
		workers:  2,
		clock:    realClock{},
		base:     DefaultBaseBackoff * time.Millisecond,
		max:      DefaultMaxBackoff * time.Millisecond,
		queue:    make(chan job, 1024),
		stop:     make(chan struct{}),
		keyLocks: map[ResourceKey]*sync.Mutex{},
		metrics:  map[ResourceKey]*KeyMetrics{},
	}
	for _, o := range opts {
		o(r)
	}
	for i := 0; i < r.workers; i++ {
		r.wg.Add(1)
		go r.workerLoop(i)
	}
	return r
}

func (r *Runner) keyLock(k ResourceKey) *sync.Mutex {
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.keyLocks[k]
	if !ok {
		m = &sync.Mutex{}
		r.keyLocks[k] = m
	}
	return m
}

func (r *Runner) metricsFor(k ResourceKey) *KeyMetrics {
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.metrics[k]
	if !ok {
		m = &KeyMetrics{}
		r.metrics[k] = m
	}
	return m
}

// Submit 异步提交一个具名操作，返回容量 1 的结果通道。
// 同一资源的多个提交按 per-key 锁串行执行。
func (r *Runner) Submit(k ResourceKey, name string, op func(attempt int) error) <-chan JobResult {
	res := make(chan JobResult, 1)
	r.pendWg.Add(1)
	m := r.metricsFor(k)
	r.mu.Lock()
	if m.FirstSubmit.IsZero() {
		m.FirstSubmit = r.clock.Now()
	}
	r.mu.Unlock()
	r.queue <- job{key: k, name: name, attempt: 1, op: op, result: res}
	return res
}

// WaitIdle 阻塞到所有已提交作业（含重试）都产生结果。
func (r *Runner) WaitIdle() { r.pendWg.Wait() }

// Shutdown 停止 worker 池（调用前应先 WaitIdle）。
func (r *Runner) Shutdown() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	r.mu.Unlock()
	close(r.stop)
	r.wg.Wait()
}

func (r *Runner) workerLoop(id int) {
	defer r.wg.Done()
	for {
		select {
		case <-r.stop:
			return
		case j := <-r.queue:
			r.execute(j)
		}
	}
}

func (r *Runner) execute(j job) {
	lock := r.keyLock(j.key)
	lock.Lock()
	err := j.op(j.attempt)
	lock.Unlock()

	m := r.metricsFor(j.key)
	r.mu.Lock()
	m.Attempts++
	m.LastFinish = r.clock.Now()
	r.mu.Unlock()

	if err == nil {
		r.mu.Lock()
		m.Successes++
		r.mu.Unlock()
		_ = r.coord.ResetAttempts(j.key) // 成功一步即清零预算，审计可见
		r.pendWg.Done()
		j.result <- JobResult{Key: j.key, Name: j.name, OK: true, Attempts: j.attempt}
		return
	}

	// 失败路径：先持久化重试事件（含退避时长），由协调器判断是否耗尽。
	backoff := r.backoffFor(j.attempt)
	exhausted, recErr := r.coord.RecordRetry(j.key, j.attempt, int(backoff/time.Millisecond), err.Error())
	if recErr != nil {
		r.pendWg.Done()
		j.result <- JobResult{Key: j.key, Name: j.name, OK: false, Attempts: j.attempt, Err: recErr}
		return
	}
	r.mu.Lock()
	m.Retries++
	if backoff > m.MaxBackoff {
		m.MaxBackoff = backoff
	}
	r.mu.Unlock()

	if exhausted || j.attempt >= r.coord.MaxAttempts() {
		r.mu.Lock()
		m.Exhausted = true
		r.mu.Unlock()
		r.pendWg.Done()
		j.result <- JobResult{
			Key: j.key, Name: j.name, OK: false, Attempts: j.attempt,
			Exhausted: true, Err: err,
		}
		return
	}

	// 关键：退避在独立 goroutine 中等待，worker 此刻已释放，可服务其他资源。
	next := job{key: j.key, name: j.name, attempt: j.attempt + 1, op: j.op, result: j.result}
	go func() {
		r.clock.Sleep(backoff)
		select {
		case r.queue <- next:
		case <-r.stop:
			// 运行器已关闭：显式回收待完成计数并回送结果，避免 goroutine 泄漏。
			r.pendWg.Done()
			next.result <- JobResult{
				Key: next.key, Name: next.name, OK: false,
				Attempts: next.attempt - 1, Err: ErrRunnerStopped,
			}
		}
	}()
}

// backoffFor 计算指数退避并硬封顶，保证重试代价有界。
func (r *Runner) backoffFor(attempt int) time.Duration {
	d := r.base
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= r.max {
			return r.max
		}
	}
	if d > r.max {
		return r.max
	}
	return d
}

// Metrics 返回某资源的指标副本。
func (r *Runner) Metrics(k ResourceKey) (KeyMetrics, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.metrics[k]
	if !ok {
		return KeyMetrics{}, false
	}
	return *m, true
}

// AllMetrics 返回所有资源指标快照。
func (r *Runner) AllMetrics() map[ResourceKey]KeyMetrics {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[ResourceKey]KeyMetrics, len(r.metrics))
	for k, m := range r.metrics {
		out[k] = *m
	}
	return out
}
