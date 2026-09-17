package cordis

import "fmt"

// Runtime 同一 Plugin 的全部运行时实例集合。
// 注册表以 Plugin 指针为键（Go 中不可比较的函数无法作键，
// 以 Plugin 定义本身作为组件身份，等价于官方实现的
// 以插件回调函数为身份）。
type Runtime struct {
	plugin *Plugin
	fibers []*Fiber
}

func (rt *Runtime) add(f *Fiber) func() {
	rt.fibers = append(rt.fibers, f)
	return func() { rt.remove(f) }
}

func (rt *Runtime) remove(f *Fiber) {
	for i, cur := range rt.fibers {
		if cur == f {
			rt.fibers = append(rt.fibers[:i], rt.fibers[i+1:]...)
			return
		}
	}
}

// Registry 插件注册表：管理 Plugin → Runtime 映射，
// 提供 Plugin/Inject 两个实例化入口。
type Registry struct {
	ctx      *Context // 根上下文
	counter  int
	runtimes map[*Plugin]*Runtime
	order    []*Runtime // 注册序，保证通知的确定性
}

func newRegistry(ctx *Context) *Registry {
	return &Registry{ctx: ctx, runtimes: make(map[*Plugin]*Runtime)}
}

func (r *Registry) nextUID() int {
	r.counter++
	return r.counter
}

// Size 返回已注册的 Runtime 数量。
func (r *Registry) Size() int { return len(r.runtimes) }

// Has 判断插件是否已注册。
func (r *Registry) Has(p *Plugin) bool {
	_, ok := r.runtimes[p]
	return ok
}

// Get 返回插件的 Runtime。
func (r *Registry) Get(p *Plugin) *Runtime { return r.runtimes[p] }

// Runtimes 按注册序返回全部 Runtime。
func (r *Registry) Runtimes() []*Runtime {
	return append([]*Runtime(nil), r.order...)
}

func resolveConfig(p *Plugin, config any) (any, error) {
	if p.Validate == nil {
		return config, nil
	}
	return p.Validate(config)
}

// Plugin 在 ctx 下实例化插件：
//
//  1. 创建 Fiber（PENDING），合并 inject 拦截配置到其上下文；
//  2. 以父 fiber 的效果注册——注册动作（入列、解析配置、
//     首次依赖评估）立即执行，注销动作（冻结 epoch、等待
//     效果完全回收）在父 fiber 卸载或手动 Dispose 时执行；
//  3. 依赖满足时由状态机自动驱动 LOADING → ACTIVE。
//
// 返回的 Fiber 可用于 Update 热重载与 Dispose 注销。
func (r *Registry) Plugin(ctx *Context, p *Plugin, config any) (*Fiber, error) {
	return r.PluginInject(ctx, p, config, nil)
}

// PluginInject 同 Plugin，但以 inject 覆盖插件声明的依赖表。
// loader 层据此实现入口级依赖声明（EntryOptions.Inject）。
// inject 为 nil 时沿用 Plugin.Inject。
func (r *Registry) PluginInject(ctx *Context, p *Plugin, config any, inject map[string]any) (*Fiber, error) {
	if p == nil || p.Apply == nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalidPlugin, "<nil>")
	}
	if err := ctx.fiber.assertActive(); err != nil {
		return nil, err
	}
	rt := r.runtimes[p]
	if rt == nil {
		rt = &Runtime{plugin: p}
		r.runtimes[p] = rt
		r.order = append(r.order, rt)
	}
	if inject == nil {
		inject = make(map[string]any, len(p.Inject))
		for k, v := range p.Inject {
			inject[k] = v
		}
	}
	f := newFiber(ctx, config, inject, rt)
	d, err := ctx.fiber.effectStep("ctx.plugin()", func() (disposeStep, error) {
		remove := rt.add(f)
		cfg, err := resolveConfig(p, config)
		if err != nil {
			r.ctx.app.logger.Error("plugin %s config error: %v", p.Name, err)
			f.err = err
			f.setState(StateFailed)
		} else {
			f.config = cfg
			f.refresh()
		}
		return disposeStep{
			run: func() {
				f.disposed = true
				f.uid = 0
				f.ctx.Emit("internal/plugin", f)
				if _, ok := r.runtimes[p]; ok {
					remove()
					if len(rt.fibers) == 0 {
						r.deleteRuntime(p)
					}
				}
				f.setEpoch(inactiveEpoch)
				if f.store == nil && f.pending == nil {
					f.setState(StateDisposed)
				}
			},
			wait: func(then func()) { f.whenStable(then) },
		}, nil
	})
	if err != nil {
		return f, err
	}
	f.dispose = d
	return f, nil
}

// Inject 声明动态依赖：deps 满足时执行 apply 并追踪其全部效果，
// 任一依赖失满足时效果被逆序回收。等价于实例化一个匿名插件。
func (r *Registry) Inject(ctx *Context, deps map[string]any, apply func(ctx *Context) error) (*Fiber, error) {
	p := &Plugin{
		Name:   "inject",
		Inject: deps,
		Apply:  func(c *Context, _ any) error { return apply(c) },
	}
	return r.Plugin(ctx, p, nil)
}

// Delete 注销插件的全部实例。
func (r *Registry) Delete(p *Plugin) {
	rt := r.runtimes[p]
	if rt == nil {
		return
	}
	delete(r.runtimes, p)
	for i, cur := range r.order {
		if cur == rt {
			r.order = append(r.order[:i], r.order[i+1:]...)
			break
		}
	}
	for _, f := range append([]*Fiber(nil), rt.fibers...) {
		f.Dispose()
	}
}

func (r *Registry) deleteRuntime(p *Plugin) {
	delete(r.runtimes, p)
	for i, cur := range r.order {
		if cur != nil && cur.plugin == p {
			r.order = append(r.order[:i], r.order[i+1:]...)
			return
		}
	}
}
