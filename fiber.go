package cordis

import (
	"fmt"
	"sort"
)

// isolateKey 协效应存储的隔离域键：同名服务在不同 realm 中互不可见。
type isolateKey struct {
	name  string
	realm string
}

// epoch 是 Fiber 的目标视图标识（对应官方实现的 runner.epoch）。
//
//   - inactive：依赖未满足（或组件已注销），目标为卸载态；
//   - 非 inactive：deps 按依赖名升序记录各依赖实现所属 Fiber 的 uid，
//     依赖集合发生任何替换都会得到不同的 epoch，从而触发
//     「先卸载旧实例、再加载新实例」的惯性转换链。
type epoch struct {
	inactive bool
	deps     []int
}

func (e epoch) equal(o epoch) bool {
	if e.inactive != o.inactive || len(e.deps) != len(o.deps) {
		return false
	}
	for i, v := range e.deps {
		if v != o.deps[i] {
			return false
		}
	}
	return true
}

var inactiveEpoch = epoch{inactive: true}

type updateHook struct {
	fn func(config any) bool
}

// Fiber 是组件的运行时实例（论文 §4 的 component instance）：
// 持有效果追踪表（disposables）、依赖快照（deps）、对外可见服务表
// （store）以及由 epoch 驱动的 reload/unload 惯性状态机。
//
// 状态机模型：epoch 变化并不立即执行转换，而是将 pump 任务投递到
// 调度器。pump 执行时按「最新 epoch」决定加载或卸载，转换完成后
// 若 epoch 又变（同步代码在转换期间再次改变目标视图）则继续追逐，
// 直到现状与目标一致——这与官方实现的 inertia 链（reload/unload
// 相互链式触发）完全等价。
type Fiber struct {
	app         *App
	uid         int
	parent      *Context
	ctx         *Context
	config      any
	runtime     *Runtime
	inject      map[string]any
	deps        map[string]*impl // 当前满足的依赖（对应 _store）
	store       map[string]*impl // 对外可见服务快照；nil 表示未加载
	disposables *disposableList
	updateHooks []*updateHook
	dispose     Dispose // 注销本 fiber 的清理函数（plugin 注册时生成）

	err         error
	ep          epoch
	loadedEpoch epoch // 当前已加载实例对应的目标视图
	forceReload bool  // Update 触发的强制卸载-重载
	state       FiberState
	disposed    bool
	pumping     bool
	pumpQueued  bool
	pending     []disposeStep // 卸载中尚未执行的撤销步骤
	watchers    []func()      // 稳定性等待者
}

func newRootFiber(ctx *Context) *Fiber {
	return &Fiber{
		app:         ctx.app,
		ctx:         ctx,
		inject:      map[string]any{},
		deps:        map[string]*impl{},
		store:       map[string]*impl{},
		disposables: newDisposableList(),
		ep:          epoch{},
		state:       StateActive,
	}
}

func newFiber(parent *Context, config any, inject map[string]any, rt *Runtime) *Fiber {
	app := parent.app
	f := &Fiber{
		app:         app,
		uid:         app.root.registry.nextUID(),
		parent:      parent,
		config:      config,
		runtime:     rt,
		inject:      inject,
		deps:        map[string]*impl{},
		disposables: newDisposableList(),
		ep:          inactiveEpoch,
	}
	var intercepts map[string]any
	for name, cfg := range inject {
		if cfg == nil {
			continue
		}
		if intercepts == nil {
			intercepts = make(map[string]any)
		}
		intercepts[name] = cfg
	}
	f.ctx = parent.extend(f, nil, intercepts)
	app.root.events.Emit(f.ctx, "internal/plugin", f)
	for name := range inject {
		f.checkImpl(name)
	}
	// 依赖倒排索引在此登记（而非更早）：保证索引中只出现构造完成
	// 的 fiber，且与注册表注销路径上的 untrack 一一对应。
	// 登记时机不影响依赖解析——调用方随后会执行 refresh 推导 epoch。
	app.root.reflect.track(f)
	return f
}

// State 返回当前生命周期状态。
func (f *Fiber) State() FiberState { return f.state }

// Err 返回组件执行失败的原因（未失败为 nil）。
func (f *Fiber) Err() error { return f.err }

// Config 返回已解析的组件配置。
func (f *Fiber) Config() any { return f.config }

// UID 返回实例编号（已注销时为 0）。
func (f *Fiber) UID() int { return f.uid }

// Dispose 将本实例从父上下文注销：先冻结 epoch 目标，
// 待效果完全回收后终结生命周期。
func (f *Fiber) Dispose() {
	if f.dispose != nil {
		f.dispose()
	}
}

func (f *Fiber) name() string {
	for cur := f; cur != nil; cur = cur.parent.fiber {
		if cur.runtime != nil && cur.runtime.plugin.Name != "" {
			return cur.runtime.plugin.Name
		}
		if cur.parent == nil {
			break
		}
	}
	return "root"
}

func (f *Fiber) assertActive() error {
	if f.disposed {
		return fmt.Errorf("%w (fiber %s)", ErrInactiveEffect, f.name())
	}
	return nil
}

// ---------------------------------------------------------------------------
// 状态机
// ---------------------------------------------------------------------------

func (f *Fiber) setEpoch(next epoch) {
	if next.equal(f.ep) {
		return
	}
	// 失败的 fiber 冻结目标视图，只能通过 Update 清除错误后重启。
	if f.err != nil {
		return
	}
	f.ep = next
	if f.pumping {
		// pump 循环会看到最新 epoch 并继续追逐。
		return
	}
	f.requestPump()
}

func (f *Fiber) requestPump() {
	if f.pumping || f.pumpQueued {
		return
	}
	f.pumpQueued = true
	f.app.sched.post(func() {
		f.pumpQueued = false
		f.pump()
	})
}

// pump 驱动 fiber 向最新 epoch 收敛。可重入安全：
// 已在 pump 中时直接返回（由循环自行追逐）。
func (f *Fiber) pump() {
	if f.pumping {
		return
	}
	f.pumping = true
	defer func() {
		f.pumping = false
		f.settle()
	}()
	for f.step() {
	}
}

func (f *Fiber) step() bool {
	if f.pending != nil {
		// 卸载进行中（可能挂起等待依赖者下线），由 watcher 链驱动。
		return false
	}
	if f.ep.inactive || f.err != nil {
		if f.store == nil {
			return false // 已卸载干净，稳定
		}
		f.doUnload()
		return f.pending == nil // 同步完成则继续追逐，否则等 watcher
	}
	if f.store != nil {
		if f.ep.equal(f.loadedEpoch) && !f.forceReload {
			return false // 已加载且目标未变，稳定
		}
		// 目标视图替换（依赖实现变化）或 Update 强制重载：
		// 先按 LIFO 回收旧实例，再加载新实例。
		f.doUnload()
		return f.pending == nil
	}
	f.doLoad()
	return true // 重新评估（失败或 epoch 又变则继续卸载）
}

func (f *Fiber) doLoad() {
	f.setState(StateLoading)
	// 捕获加载时刻的目标视图：execute 期间依赖可能再次变化
	//（如组件提供的服务满足了自身依赖），加载完成后由
	// step 重新比对并追逐到最新目标。
	ep := f.ep
	f.store = make(map[string]*impl, len(f.deps)+2)
	for k, v := range f.deps {
		f.store[k] = v
	}
	if err := f.execute(); err != nil {
		f.app.logger.Error("plugin %s apply failed: %v", f.name(), err)
		f.err = err
		// 直接改写 epoch（不经 setEpoch），失败后走卸载路径。
		f.ep = inactiveEpoch
	} else {
		f.loadedEpoch = ep
		f.forceReload = false
		// 对应官方 _reload 末尾的 _updateState(() => {})：
		// 依 (disposed, err, epoch) 推导状态。LOADING → ACTIVE 的
		// 跨越会触发 notifyOwn——加载期间 provide 的服务
		// 正是在此刻向依赖者发布。
		f.setState(f.deriveState())
	}
}

func (f *Fiber) execute() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("plugin %s apply panic: %v", f.name(), r)
		}
	}()
	return f.runtime.plugin.Apply(f.ctx, f.config)
}

func (f *Fiber) doUnload() {
	f.setState(StateUnloading)
	f.pending = f.disposables.clear()
	f.drain()
}

// drain 逐个执行撤销步骤。步骤带 wait 时挂起，
// 待其回调 then 后继续。
func (f *Fiber) drain() {
	if len(f.pending) == 0 {
		f.finishUnload()
		return
	}
	st := f.pending[0]
	f.pending = f.pending[1:]
	st.invoke(f.drain)
}

func (f *Fiber) finishUnload() {
	f.pending = nil
	f.store = nil
	f.setState(f.deriveState())
	f.settle()
	f.pump() // 追逐：卸载期间 epoch 可能已再次改变
}

func (f *Fiber) deriveState() FiberState {
	if f.disposed {
		return StateDisposed
	}
	if f.err != nil {
		return StateFailed
	}
	if f.ep.inactive {
		return StatePending
	}
	if f.store != nil {
		return StateActive
	}
	// 卸载刚完成而目标视图仍为加载态：即将重载。
	return StateLoading
}

// setState 发布状态变化。跨越 ACTIVE 边界时，本 fiber
// 提供的所有服务都触发一次通知——这正是依赖者感知
// 「提供者上下线」的机制。
func (f *Fiber) setState(s FiberState) {
	old := f.state
	if old == s {
		return
	}
	f.state = s
	f.app.root.events.Emit(f.ctx, "internal/status", f, old)
	if (old == StateActive) != (s == StateActive) {
		f.app.root.reflect.notifyOwn(f)
	}
}

func (f *Fiber) stable() bool {
	return !f.pumping && !f.pumpQueued && f.pending == nil && f.matches()
}

func (f *Fiber) matches() bool {
	if f.ep.inactive || f.err != nil {
		return f.store == nil
	}
	return f.store != nil
}

func (f *Fiber) settle() {
	if !f.stable() {
		return
	}
	ws := f.watchers
	f.watchers = nil
	for _, w := range ws {
		w()
	}
}

func (f *Fiber) whenStable(cb func()) {
	if f.stable() {
		cb()
		return
	}
	f.watchers = append(f.watchers, cb)
}

// ---------------------------------------------------------------------------
// 依赖解析（协效应）
// ---------------------------------------------------------------------------

// checkImpl 依据当前隔离域重新解析依赖 name：
// 仅接受处于 ACTIVE 状态且通过 check 校验的实现。
func (f *Fiber) checkImpl(name string) {
	r := f.app.root.reflect
	im := r.getImpl(f.ctx, name, true)
	if im == nil {
		delete(f.deps, name)
		return
	}
	if im.check != nil {
		if !f.safeCheck(im) {
			delete(f.deps, name)
			return
		}
	}
	f.deps[name] = im
}

func (f *Fiber) safeCheck(im *impl) (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			f.app.logger.Error("service %q check panic: %v", im.name, r)
			ok = false
		}
	}()
	return im.check()
}

// refresh 依据依赖满足情况推导目标 epoch。
func (f *Fiber) refresh() {
	if len(f.inject) == 0 {
		f.setEpoch(epoch{})
		return
	}
	names := make([]string, 0, len(f.inject))
	for name := range f.inject {
		names = append(names, name)
	}
	sort.Strings(names)
	ep := epoch{deps: make([]int, 0, len(names))}
	for _, name := range names {
		im, ok := f.deps[name]
		if !ok {
			ep = inactiveEpoch
			break
		}
		ep.deps = append(ep.deps, im.fiber.uid)
	}
	f.setEpoch(ep)
}

// ---------------------------------------------------------------------------
// 效果（revertible effects）
// ---------------------------------------------------------------------------

// Effect 执行 execute 并把其返回的清理函数绑定到当前 fiber：
// fiber 卸载时按 LIFO 逆序自动调用，也可提前手动调用。
// execute 出错时，已产生的清理动作立即逆序执行并返回错误。
func (f *Fiber) Effect(label string, execute func() (Dispose, error)) (Dispose, error) {
	return f.effectStep(label, func() (disposeStep, error) {
		d, err := execute()
		if err != nil {
			return disposeStep{}, err
		}
		if d == nil {
			return disposeStep{}, nil
		}
		return disposeStep{run: func() { f.safeDispose(label, d) }}, nil
	})
}

// EffectIter 增量效果：iter 通过 yield 依次登记多个清理函数，
// 全部按 LIFO 逆序撤销。对应官方实现的 generator effect 形态。
func (f *Fiber) EffectIter(label string, iter func(yield func(Dispose))) Dispose {
	d, _ := f.effectStep(label, func() (disposeStep, error) {
		var disposables []Dispose
		iter(func(d Dispose) {
			if d != nil {
				disposables = append(disposables, d)
			}
		})
		if len(disposables) == 0 {
			return disposeStep{}, nil
		}
		return disposeStep{run: func() {
			for i := len(disposables) - 1; i >= 0; i-- {
				f.safeDispose(label, disposables[i])
			}
		}}, nil
	})
	return d
}

// effectStep 底层效果原语：execute 返回两阶段清理步骤
// （run 执行撤销，wait 可选地将完成时机推迟到异步条件满足）。
func (f *Fiber) effectStep(label string, execute func() (disposeStep, error)) (Dispose, error) {
	if err := f.assertActive(); err != nil {
		return nil, err
	}
	step, err := execute()
	if err != nil {
		if step.run != nil || step.wait != nil {
			step.invoke(func() {})
		}
		return nil, err
	}
	var done bool
	var remove func()
	remove = f.disposables.push(disposeStep{
		run: func() {
			if done {
				return
			}
			done = true
			if step.run != nil {
				step.run()
			}
		},
		wait: func(then func()) {
			if step.wait == nil {
				then()
				return
			}
			step.wait(then)
		},
	})
	return func() {
		if done {
			return
		}
		done = true
		remove()
		step.invoke(func() {})
	}, nil
}

// safeDispose 执行撤销且不让 panic 波及其余清理：撤销失败是
// 环境残留（端口未释放、连接未关闭）的第一现场，因此一律记日志。
// 单次失败不阻断剩余的撤销步骤。
func (f *Fiber) safeDispose(label string, d Dispose) {
	if d == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			f.app.logger.Error("effect %q dispose panic: %v", label, r)
		}
	}()
	d()
}

// ---------------------------------------------------------------------------
// 配置热更新
// ---------------------------------------------------------------------------

// OnUpdate 注册 fiber 局部的更新钩子。钩子返回 false 表示
// 已完全接管本次更新（否决默认的「替换配置并重启」行为）。
// Group 组件借此将更新转发给子 entry 集合。
func (f *Fiber) OnUpdate(hook func(config any) bool) Dispose {
	h := &updateHook{fn: hook}
	f.updateHooks = append(f.updateHooks, h)
	return func() {
		for i, cur := range f.updateHooks {
			if cur == h {
				f.updateHooks = append(f.updateHooks[:i], f.updateHooks[i+1:]...)
				return
			}
		}
	}
}

// Update 以新配置热重载组件：替换配置、清除错误、
// 走一遍完整的卸载-重载循环（论文 §5 的 HMR 机制）。
func (f *Fiber) Update(config any) error {
	if err := f.assertActive(); err != nil {
		return err
	}
	cfg, err := resolveConfig(f.runtime.plugin, config)
	if err != nil {
		return err
	}
	for _, hook := range append([]*updateHook(nil), f.updateHooks...) {
		if !hook.fn(cfg) {
			return nil // 钩子已接管
		}
	}
	f.config = cfg
	f.err = nil
	f.restart()
	return nil
}

func (f *Fiber) restart() {
	// 即使 epoch 净变化为零（依赖集合与配置均未变），
	// Update 也要求走一遍完整的卸载-重载循环。
	f.forceReload = true
	f.setEpoch(inactiveEpoch)
	f.refresh()
}
