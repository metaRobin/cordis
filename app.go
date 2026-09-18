package cordis

import "sync"

// scheduler 以单 goroutine 串行执行任务，复刻 JS 事件循环语义：
// 状态转换任务入队后并不立即执行，待当前同步栈清空后才逐个 drain。
// 这使得批量操作（如一次配置变更引发的多次 epoch 变化）先全部提交，
// 转换任务执行时看到的是最终目标视图。
//
// 队列为无界 slice：任务内部可能继续投递（每次 fiber 注册都会排入
// 一次 pump），固定容量 channel 在唯一消费者忙于当前任务时会让
// 投递方永久阻塞——单个任务内的投递数超过容量即自死锁。
type scheduler struct {
	mu       sync.Mutex
	queue    []func()
	loopDone bool // 队列封闭后置位，此后投递直接丢弃
	signal   chan struct{}
	quit     chan struct{}
	done     chan struct{}
	stopped  sync.Once
}

func newScheduler() *scheduler {
	s := &scheduler{
		signal: make(chan struct{}, 1),
		quit:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	go s.loop()
	return s
}

func (s *scheduler) loop() {
	defer close(s.done)
	for {
		select {
		case <-s.quit:
			for {
				f, ok := s.pop()
				if ok {
					f()
					continue
				}
				if s.seal() {
					return
				}
			}
		case <-s.signal:
			for {
				f, ok := s.pop()
				if !ok {
					break
				}
				f()
			}
		}
	}
}

func (s *scheduler) pop() (f func(), ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.queue) == 0 {
		return nil, false
	}
	f = s.queue[0]
	s.queue[0] = nil // 释放对闭包的引用，避免底层数组滞留任务状态
	s.queue = s.queue[1:]
	return f, true
}

// seal 封闭投递口。返回 true 表示队列已排空且封闭完成；
// 检查时刻仍有迟到任务入队时返回 false，调用方需继续排空。
func (s *scheduler) seal() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loopDone {
		return true
	}
	if len(s.queue) > 0 {
		return false
	}
	s.loopDone = true
	return true
}

func (s *scheduler) post(f func()) {
	s.mu.Lock()
	if s.loopDone {
		s.mu.Unlock()
		return
	}
	s.queue = append(s.queue, f)
	s.mu.Unlock()
	select {
	case s.signal <- struct{}{}:
	default:
	}
}

func (s *scheduler) stop() {
	s.stopped.Do(func() {
		close(s.quit)
		<-s.done
	})
}

// pending 报告是否仍有排队任务（正在执行中的任务不在此列）。
func (s *scheduler) pending() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queue) > 0
}

// App 是组件系统的宿主，持有根上下文与调度器。
//
// 所有 Context/Fiber 上的 API 必须在调度器 goroutine 内调用——
// 即 Apply、Dispose 与事件回调内部（它们天然运行于调度器内）。
// 外部 goroutine 一律通过 Do / DoSync 进入。
type App struct {
	sched     *scheduler
	root      *Context
	logger    *Logger
	closeOnce sync.Once
}

// New 创建应用并启动调度器。
func New() *App {
	app := &App{sched: newScheduler(), logger: newLogger()}
	app.root = newRootContext(app)
	return app
}

// Do 在调度器上异步执行 f。
func (a *App) Do(f func(ctx *Context)) {
	a.sched.post(func() { f(a.root) })
}

// DoSync 在调度器上执行 f 并阻塞等待完成，返回 f 是否确实执行完毕：
// false 表示调度器已停止，f 可能已被排空执行、也可能未执行。
// 不得在 Apply / Dispose / 事件回调等调度器上下文中调用（会死锁）。
func (a *App) DoSync(f func(ctx *Context)) bool {
	return a.doSync(f)
}

func (a *App) doSync(f func(ctx *Context)) bool {
	done := make(chan struct{})
	a.sched.post(func() {
		defer close(done)
		f(a.root)
	})
	select {
	case <-done:
		return true
	case <-a.sched.done:
		// 调度器正在停止：任务可能已被排空执行，也可能未执行，
		// 两种情况都不再等待。
		return false
	}
}

// Root 返回根上下文。仅在调度器上下文（Apply 等）中使用。
func (a *App) Root() *Context { return a.root }

// waitRounds 是 Wait 的轮询上限，仅用于防御病态活锁（依赖永远
// 无法满足且持续有新任务投递）。
const waitRounds = 1 << 20

// Wait 阻塞直到任务队列排空且所有 Fiber 达到稳定状态，
// 返回是否真正收敛。
//
// 返回 false 有两种情形，均伴随告警日志（调度器已停止的情形除外）：
//   - 调度器已停止，无从判断；
//   - 达到轮询上限后放弃等待——此时系统状态未收敛，调用方不得
//     基于「已稳定」的假设继续断言。
//
// 注意：依赖未满足的 Fiber 是稳定态（不是「不稳定」），
// 因此存在等待外部事件的 Fiber 不会让 Wait 挂起。
func (a *App) Wait() bool {
	for i := 0; i < waitRounds; i++ {
		stable := false
		if !a.doSync(func(ctx *Context) {
			stable = ctx.app.settled()
		}) {
			return false // 调度器已停止
		}
		if stable {
			return true
		}
	}
	a.logger.Warn("wait: system still not settled after %d rounds, giving up", waitRounds)
	return false
}

// Close 注销全部组件并停止调度器。可安全地重复调用：
// 只有首次调用执行级联回收，后续调用为空操作。
func (a *App) Close() {
	a.closeOnce.Do(func() {
		a.DoSync(func(ctx *Context) {
			// 冻结根 fiber 目标视图：效果按 LIFO 全量回收，
			// 子组件的注销沿效果链级联发生。
			a.root.fiber.setEpoch(inactiveEpoch)
		})
		a.Wait()
		a.sched.stop()
	})
}

// settled 判断系统是否静止：调度队列空、无 Fiber 处于转换中。
func (a *App) settled() bool {
	if a.sched.pending() {
		return false
	}
	if !a.root.fiber.stable() {
		return false
	}
	for _, rt := range a.root.registry.order {
		for _, f := range rt.fibers {
			if !f.stable() {
				return false
			}
		}
	}
	return true
}
