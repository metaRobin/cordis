package cordis

import (
	"fmt"
	"sort"
)

// impl 一个服务实现：由某个 fiber 在某个隔离域中注册。
// key 记录注册时的隔离域键——域校验的依据：
// 依赖解析（checkImpl）与直接访问（Context.Get）都以
// 调用方上下文的域键比对，同名服务跨域互不可见。
type impl struct {
	name  string
	key   isolateKey
	fiber *Fiber
	value any
	check func() bool
}

// Reflect 协效应存储（对应官方实现的 ReflectService）：
// 全局唯一的 isolate 域键 → 服务实现映射，配合依赖声明
// （inject）与依赖满足检查（checkImpl）实现空间维的可组合性。
//
// 关键语义：所有键解析都以「调用方上下文」为基准——
// provider 注册时用它自己的域键写入，dependant 检查满足度
// 时用它自己的域键读取。同名服务在不同域中互不可见。
//
// index 是依赖声明的倒排索引（服务名 → 声明依赖它的 Fiber 列表，
// 按创建序排列）：服务上下线只需通知真正声明了该服务的依赖者，
// 而非全量扫描 Runtime × Fiber（原实现的 O(全部 Fiber) 每变更）。
type Reflect struct {
	ctx   *Context // 根上下文
	store map[isolateKey]*impl
	index map[string][]*Fiber
}

func newReflect(ctx *Context) *Reflect {
	return &Reflect{
		ctx:   ctx,
		store: make(map[isolateKey]*impl),
		index: make(map[string][]*Fiber),
	}
}

// track 按 f 的依赖声明登记倒排索引（Fiber 创建时调用一次）。
func (r *Reflect) track(f *Fiber) {
	for name := range f.inject {
		r.index[name] = append(r.index[name], f)
	}
}

// untrack 注销倒排索引（Fiber 注销时调用，与其创建一一对应）。
func (r *Reflect) untrack(f *Fiber) {
	for name := range f.inject {
		list := r.index[name]
		for i, cur := range list {
			if cur != f {
				continue
			}
			list = append(list[:i], list[i+1:]...)
			break
		}
		if len(list) == 0 {
			delete(r.index, name)
			continue
		}
		r.index[name] = list
	}
}

// candidates 返回声明依赖 names 中任一服务的 Fiber（按创建序去重）。
func (r *Reflect) candidates(names []string) []*Fiber {
	if len(names) == 1 {
		return append([]*Fiber(nil), r.index[names[0]]...)
	}
	var out []*Fiber
	seen := make(map[*Fiber]bool)
	for _, name := range names {
		for _, f := range r.index[name] {
			if seen[f] {
				continue
			}
			seen[f] = true
			out = append(out, f)
		}
	}
	return out
}

// getImpl 以 ctx 的隔离域解析 name。strict 为 true 时
// 仅接受处于 ACTIVE 状态的实现——这保证了「提供者正在
// 卸载」时依赖者立即视为未满足。
func (r *Reflect) getImpl(ctx *Context, name string, strict bool) *impl {
	im, ok := r.store[ctx.isolateKey(name)]
	if !ok {
		return nil
	}
	if strict && im.fiber.state != StateActive {
		return nil
	}
	return im
}

// Get 以调用方上下文解析服务值（严格模式）。
func (r *Reflect) Get(ctx *Context, name string) (any, bool) {
	im := r.getImpl(ctx, name, true)
	if im == nil {
		return nil, false
	}
	return im.value, true
}

// Set 更新当前 fiber 注册的服务值。
func (r *Reflect) Set(ctx *Context, name string, value any) error {
	im, ok := r.store[ctx.isolateKey(name)]
	if !ok {
		return fmt.Errorf("cannot set service %q without provide", name)
	}
	if im.fiber != ctx.fiber {
		return fmt.Errorf("cannot set service %q registered by another fiber", name)
	}
	im.value = value
	return nil
}

// Provide 注册服务：以调用方上下文的隔离域键写入全局存储，
// 并登记到调用方 fiber 的服务表中。返回的 Dispose 撤销注册。
//
// 撤销顺序保证（论文 §3.2 的 dependant-first 原则）：
// 先从全局存储删除并通知所有依赖者，等待它们完全下线后，
// 才清理提供者自身的服务表——依赖者绝不会访问到已销毁的服务。
func (r *Reflect) Provide(ctx *Context, name string, value any, check func() bool) (Dispose, error) {
	f := ctx.fiber
	return f.effectStep(fmt.Sprintf("ctx.provide(%q)", name), func() (disposeStep, error) {
		key := ctx.isolateKey(name)
		if old, dup := r.store[key]; dup {
			return disposeStep{}, fmt.Errorf("%w: %q at <%s>", ErrServiceDuplicate, name, old.fiber.name())
		}
		if f.store == nil {
			return disposeStep{}, fmt.Errorf("cannot provide %q on unloaded fiber", name)
		}
		im := &impl{name: name, key: key, fiber: f, value: value, check: check}
		r.store[key] = im
		f.store[name] = im
		if f.state == StateActive {
			// 加载过程中 provide 由随后的 ACTIVE 状态发布统一通知。
			r.notify(ctx, []string{name}, nil)
		}
		var affected []*Fiber
		return disposeStep{
			run: func() {
				delete(r.store, key)
				affected = r.notify(ctx, []string{name}, nil)
			},
			wait: func(then func()) {
				cleanup := func() { delete(f.store, name) }
				if len(affected) == 0 {
					cleanup()
					then()
					return
				}
				n := len(affected)
				for _, dep := range affected {
					dep.whenStable(func() {
						n--
						if n == 0 {
							cleanup()
							then()
						}
					})
				}
			},
		}, nil
	})
}

// notify 通知所有声明依赖 names 中任一服务且通过域过滤的
// fiber 重新解析依赖并刷新目标视图。返回受影响的 fiber
// （供撤销流程等待它们下线）。
//
// 默认过滤规则：依赖者的域键与 provider 的域键一致。
// 候选集取自倒排索引，代价为 O(声明依赖者数)，与 Fiber 总数无关。
func (r *Reflect) notify(provider *Context, names []string, filter func(depCtx *Context, name string) bool) []*Fiber {
	if filter == nil {
		providerKey := make(map[string]isolateKey, len(names))
		for _, name := range names {
			providerKey[name] = provider.isolateKey(name)
		}
		filter = func(depCtx *Context, name string) bool {
			return depCtx.isolateKey(name) == providerKey[name]
		}
	}
	var fibers []*Fiber
	for _, f := range r.candidates(names) {
		hasUpdate := false
		for _, name := range names {
			if _, ok := f.inject[name]; !ok {
				continue
			}
			if !filter(f.ctx, name) {
				continue
			}
			hasUpdate = true
			f.checkImpl(name)
		}
		if !hasUpdate {
			continue
		}
		f.refresh()
		fibers = append(fibers, f)
	}
	// internal/service：按域过滤分发服务变更事件，
	// 供依赖者以事件方式观察服务出现/消失。
	for _, name := range names {
		var value any
		if im := r.getImpl(provider, name, false); im != nil {
			value = im.value
		}
		nameFilter := func(hookCtx *Context) bool { return filter(hookCtx, name) }
		r.ctx.events.EmitFiltered(provider, "internal/service", nameFilter, name, value)
	}
	return fibers
}

// notifyOwn 通知 fiber 提供的全部服务（跨越 ACTIVE 边界时调用）。
func (r *Reflect) notifyOwn(f *Fiber) {
	if f.store == nil {
		return
	}
	var names []string
	for name, im := range f.store {
		if im.fiber == f {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return
	}
	sort.Strings(names)
	r.notify(f.ctx, names, nil)
}
