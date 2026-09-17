package cordis

// Context 是统一上下文（论文 §3.3 的 unified context）：
// 既承载效果追踪（通过 fiber），也承载协效应解析（通过 isolate/intercept 链）。
//
// Context 是不可变链式结构：Extend 派生子层，查找沿父链向上。
// 同名服务在不同 isolate 域中互不可见；intercept 沿链合并，
// 子层配置覆盖父层。
type Context struct {
	app        *App
	parent     *Context
	fiber      *Fiber
	entry      *Entry            // 声明式配置层：本链所属的入口（沿链继承）
	isolates   map[string]string // name → realm 标签（"" 为默认域）
	intercepts map[string]any    // name → 拦截配置

	// 以下三个核心服务仅挂载于根上下文。
	registry *Registry
	events   *Events
	reflect  *Reflect

	// eventFilter 由分发方在 Emit 时注入（对应官方实现的
	// symbols.filter），用于 isolate 域感知的事件路由。
	eventFilter func(hookCtx *Context) bool
}

func newRootContext(app *App) *Context {
	root := &Context{app: app}
	root.fiber = newRootFiber(root)
	root.registry = newRegistry(root)
	root.events = newEvents(root)
	root.reflect = newReflect(root)
	return root
}

// App 返回所属应用。
func (c *Context) App() *App { return c.app }

// Fiber 返回当前上下文所属的 Fiber。
func (c *Context) Fiber() *Fiber { return c.fiber }

// Root 返回根上下文。
func (c *Context) Root() *Context { return c.app.root }

// Registry / Events / Reflect 返回核心服务（挂载于根上下文）。
func (c *Context) Registry() *Registry { return c.app.root.registry }
func (c *Context) Events() *Events     { return c.app.root.events }
func (c *Context) Reflect() *Reflect   { return c.app.root.reflect }

// Entry 返回本链所属的声明式入口（无 loader 层时为 nil）。
func (c *Context) Entry() *Entry { return c.entry }

// extend 派生子上下文。fiber 非 nil 时子上下文挂载该 fiber
// （每个 fiber 的 ctx 都是其父 ctx 的扩展层）；isolates/intercepts
// 为该层的覆盖项，查找沿父链向上；entry 沿链继承。
func (c *Context) extend(fiber *Fiber, isolates map[string]string, intercepts map[string]any) *Context {
	if fiber == nil {
		fiber = c.fiber
	}
	return &Context{
		app:        c.app,
		parent:     c,
		fiber:      fiber,
		entry:      c.entry,
		isolates:   isolates,
		intercepts: intercepts,
	}
}

// extendWithEntry 派生携带入口引用的子上下文（loader 层专用）：
// 在该链上实例化的全部 fiber 都能通过 Context.Entry() 找到入口。
func (c *Context) extendWithEntry(e *Entry, isolates map[string]string, intercepts map[string]any) *Context {
	child := c.extend(nil, isolates, intercepts)
	child.entry = e
	return child
}

// Isolate 派生一个对 name 服务启用新隔离域的上下文。
// 同域（相同 realm 标签）的上下文共享服务实例，不同域互不可见。
func (c *Context) Isolate(name, realm string) *Context {
	return c.extend(nil, map[string]string{name: realm}, nil)
}

// Intercept 派生一个对 name 服务携带拦截配置的上下文，
// 供该服务的提供者在构造实例时读取。
func (c *Context) Intercept(name string, config any) *Context {
	return c.extend(nil, nil, map[string]any{name: config})
}

// isolateKey 解析 name 在当前链上的隔离域键。
// 沿父链向上取最近定义；均未定义时落入默认域。
func (c *Context) isolateKey(name string) isolateKey {
	for ctx := c; ctx != nil; ctx = ctx.parent {
		if realm, ok := ctx.isolates[name]; ok {
			return isolateKey{name: name, realm: realm}
		}
	}
	return isolateKey{name: name}
}

// InterceptOf 返回 name 的拦截配置（沿链最近定义优先）。
func (c *Context) InterceptOf(name string) (any, bool) {
	for ctx := c; ctx != nil; ctx = ctx.parent {
		if cfg, ok := ctx.intercepts[name]; ok {
			return cfg, true
		}
	}
	return nil, false
}

// Get 解析服务 name：从当前 Fiber 沿父链向上查找最近的可访问实现。
// 域校验：仅接受注册域键与调用方一致的服务实现；跨越 fiber 边界
// 时还要求父上下文的域键不变——同名服务在不同域中互不可见。
// 找不到时返回 (nil, false)，不区分「未注册」与「注册者未激活」——
// 两者对依赖者而言都意味着依赖未满足。
func (c *Context) Get(name string) (any, bool) {
	key := c.isolateKey(name)
	f := c.fiber
	for {
		if f.store != nil {
			if impl, ok := f.store[name]; ok {
				if impl.key == key {
					return impl.value, true
				}
				// 同名服务注册在其他域：本域视作不存在，继续沿链查找。
			}
		}
		if _, injected := f.inject[name]; injected {
			return nil, false
		}
		if f.runtime == nil {
			return nil, false
		}
		if f.parent.isolateKey(name) != key {
			return nil, false
		}
		f = f.parent.fiber
	}
}

// GetMust 同 Get，但失败时 panic（适用于已知服务存在的场景）。
func (c *Context) GetMust(name string) any {
	v, ok := c.Get(name)
	if !ok {
		panic(ErrServiceNotFound)
	}
	return v
}

// Effect 在当前 Fiber 上创建效果（见 Fiber.Effect）。
func (c *Context) Effect(label string, execute func() (Dispose, error)) (Dispose, error) {
	return c.fiber.Effect(label, execute)
}

// EffectIter 在当前 Fiber 上创建增量效果（见 Fiber.EffectIter）。
func (c *Context) EffectIter(label string, iter func(yield func(Dispose))) Dispose {
	return c.fiber.EffectIter(label, iter)
}

// On 注册事件监听器，随当前 Fiber 卸载自动回收（见 Events.On）。
func (c *Context) On(name string, listener func(ctx *Context, args ...any) any) (Dispose, error) {
	return c.app.root.events.On(c, name, listener)
}

// Once 注册一次性监听器（见 Events.Once）。
func (c *Context) Once(name string, listener func(ctx *Context, args ...any) any) (Dispose, error) {
	return c.app.root.events.Once(c, name, listener)
}

// Emit 同步分发事件。
func (c *Context) Emit(name string, args ...any) {
	c.app.root.events.Emit(c, name, args...)
}

// Plugin 在当前上下文实例化插件（见 Registry.Plugin）。
func (c *Context) Plugin(p *Plugin, config any) (*Fiber, error) {
	return c.app.root.registry.Plugin(c, p, config)
}

// Inject 声明动态依赖：deps 满足时执行 apply，任一失满足时
// 撤销 apply 创建的全部效果。等价于注册一个匿名插件。
func (c *Context) Inject(deps map[string]any, apply func(ctx *Context) error) (*Fiber, error) {
	return c.app.root.registry.Inject(c, deps, apply)
}

// Provide 注册服务（见 Reflect.Provide）。返回的 Dispose 与
// 当前 Fiber 生命周期绑定：Fiber 卸载或手动调用时撤销注册。
func (c *Context) Provide(name string, value any, check func() bool) (Dispose, error) {
	return c.app.root.reflect.Provide(c, name, value, check)
}

// Set 更新当前 Fiber 注册的服务值。
func (c *Context) Set(name string, value any) error {
	return c.app.root.reflect.Set(c, name, value)
}
