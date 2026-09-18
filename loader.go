package cordis

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
)

// ---------------------------------------------------------------------------
// 声明式配置层（对应官方 loader 包）
//
// 论文 §5 的落地形态：组件的实例化不再由过程式代码驱动，
// 而是由「入口配置」（EntryOptions）描述。配置树与运行时之间
// 通过协调（reconciliation）保持一致：
//
//   - 配置新增入口 → 解析插件并实例化 Fiber；
//   - 配置移除入口 → 注销 Fiber（效果按 LIFO 回收）；
//   - 配置修改入口 → 配置热重载 / 空间声明重建。
//
// 空间维声明（isolate/intercept/inject）发生在入口级：
// 同一插件的不同入口可拥有不同的隔离域、拦截配置与依赖集合。
// ---------------------------------------------------------------------------

// EntryOptions 声明式入口配置。
//
// ID 是**全树唯一**的入口标识：EntryTree 以短 ID 为扁平索引键
// （寻址则按 "group:child" 路径进行）。因此不同分组内不允许出现
// 同名子入口——reconcile 与 Create 都会拒绝重复 ID（跳过并记日志）。
// 留空时由 EntryTree 自动生成随机 ID。
//
// Isolate 值为 true 表示私有域（每个入口独享，键为 "#入口ID"），
// 为字符串表示共享域（相同标签的入口互通，键为 "@标签"）。
//
// Inject 中 nil 表示必选依赖（无附加配置），非 nil 值为拦截配置，
// cordis.DepRemove 显式移除插件声明的依赖。
type EntryOptions struct {
	ID        string
	Name      string
	Config    any
	Group     bool           // 分组入口：Config 为子入口列表，自身不承载业务
	Disabled  bool           // 禁用级联：禁用分组会禁用其全部后代
	Inject    map[string]any // 入口级依赖声明：新增/覆盖/移除插件依赖
	Isolate   map[string]any // 入口级隔离域声明
	Intercept map[string]any // 入口级拦截配置
}

// EntryChange 入口结构的一次变更，在变更生效后同步提交。
// Options 与 Legacy 的组合区分增/删/改：仅 Options 为创建，
// 仅 Legacy 为移除，两者皆有且 From 非 nil 表示跨组移动。
type EntryChange struct {
	ID      string
	Group   *EntryGroup
	From    *EntryGroup
	Options *EntryOptions
	Legacy  *EntryOptions
}

// EntryGroup 一组有序的子入口。根组挂载于树，分组入口的组
// 挂载于分组插件的 Fiber 之上——子入口的服务查找沿
// 「子入口 ctx → 分组 Fiber ctx → 上层组 ctx」链进行。
type EntryGroup struct {
	tree        *EntryTree
	parentEntry *Entry // 所属分组入口（根组为 nil）
	children    []*Entry
}

func (g *EntryGroup) ctx() *Context {
	if g.parentEntry == nil {
		return g.tree.ctx
	}
	if g.parentEntry.fiber != nil {
		return g.parentEntry.fiber.ctx
	}
	return g.tree.ctx
}

func (g *EntryGroup) removeChild(e *Entry) {
	for i, c := range g.children {
		if c == e {
			g.children = append(g.children[:i], g.children[i+1:]...)
			return
		}
	}
}

func (g *EntryGroup) insertChild(e *Entry, position int) {
	if position < 0 || position > len(g.children) {
		position = len(g.children)
	}
	g.children = append(g.children, nil)
	copy(g.children[position+1:], g.children[position:])
	g.children[position] = e
}

// Stop 移除全部子入口（级联注销）。
func (g *EntryGroup) Stop() {
	for _, e := range g.children {
		e.remove()
	}
	g.children = nil
}

// Children 返回按配置序排列的子入口快照。
func (g *EntryGroup) Children() []*Entry {
	return append([]*Entry(nil), g.children...)
}

// reconcile 协调组内子入口与目标配置列表：
// 按 ID 匹配，新增的创建、缺失的移除、存留的更新，顺序以配置为准。
// （对应官方实现：先处理旧表中的移除项，再处理新表中的保留/新增项。）
func (g *EntryGroup) reconcile(options []EntryOptions) {
	tree := g.tree
	oldByID := make(map[string]*Entry, len(g.children))
	for _, e := range g.children {
		oldByID[e.options.ID] = e
	}
	// 先移除配置中缺失的入口（含级联回收）。
	present := make(map[string]bool, len(options))
	for _, opt := range options {
		present[opt.ID] = true
	}
	for _, e := range g.children {
		if present[e.options.ID] {
			continue
		}
		g.removeChild(e)
		legacy := e.options
		e.remove()
		tree.commit(EntryChange{ID: legacy.ID, Group: g, Legacy: &legacy})
		delete(oldByID, e.options.ID)
	}
	// 再按配置序处理保留与新增。
	children := make([]*Entry, 0, len(options))
	seen := make(map[string]bool, len(options))
	for _, opt := range options {
		if opt.ID == "" {
			opt.ID = tree.ensureID()
		}
		// 查重与 Create 对齐：store 以短 ID 为全局键，
		// 同列表重复或跨组同名均拒绝（跳过并记日志），
		// 避免静默覆盖索引使其与树结构脱钩。
		if seen[opt.ID] {
			tree.loader.ctx.app.logger.Error("duplicate entry id %q in group config", opt.ID)
			continue
		}
		seen[opt.ID] = true
		if e, ok := oldByID[opt.ID]; ok {
			e.update(opt)
			children = append(children, e)
			continue
		}
		if _, dup := tree.store[opt.ID]; dup {
			tree.loader.ctx.app.logger.Error("duplicate entry id %q (already exists in another group)", opt.ID)
			continue
		}
		e := &Entry{loader: tree.loader, parent: g, options: opt}
		tree.store[opt.ID] = e
		e.init()
		children = append(children, e)
		created := e.options
		tree.commit(EntryChange{ID: created.ID, Group: g, Options: &created})
	}
	g.children = children
}

// ---------------------------------------------------------------------------
// Entry
// ---------------------------------------------------------------------------

// Entry 一个声明式入口：持有目标配置与其实例化的 Fiber。
type Entry struct {
	loader   *Loader
	parent   *EntryGroup
	options  EntryOptions
	ctx      *Context
	fiber    *Fiber
	subgroup *EntryGroup // options.Group 时由分组插件建立
	updating bool        // loader 发起的更新：跳过配置回写
}

// Options 返回入口的当前配置。
func (e *Entry) Options() EntryOptions { return e.options }

// Fiber 返回入口实例化的 Fiber（未加载/已禁用时为 nil）。
func (e *Entry) Fiber() *Fiber { return e.fiber }

// Subgroup 返回分组入口的子组（非分组为 nil）。
func (e *Entry) Subgroup() *EntryGroup { return e.subgroup }

// ID 返回全路径 ID：沿分组祖先链以 ":" 连接。
func (e *Entry) ID() string {
	var segs []string
	for cur := e; cur != nil; {
		segs = append([]string{cur.options.ID}, segs...)
		g := cur.parent
		if g == nil || g.parentEntry == nil {
			break
		}
		cur = g.parentEntry
	}
	return strings.Join(segs, ":")
}

// Disabled 禁用级联：自身或任一祖先分组被禁用即为禁用。
// 分组入口本身恒视为启用（禁用标志只作用于后代）。
func (e *Entry) Disabled() bool {
	if e.options.Group {
		return false
	}
	for cur := e; cur != nil; {
		if cur.options.Disabled {
			return true
		}
		g := cur.parent
		if g == nil || g.parentEntry == nil {
			return false
		}
		cur = g.parentEntry
	}
	return false
}

// realmKey 解析 name 的隔离域键：
// true → "#入口ID"（私有域），字符串 → "@标签"（共享域）。
func (e *Entry) realmKey(name string, label any) (string, bool) {
	switch v := label.(type) {
	case bool:
		if !v {
			return "", false
		}
		return "#" + e.options.ID, true
	case string:
		if v == "" {
			return "", false
		}
		return "@" + v, true
	}
	return "", false
}

// buildContext 依据空间声明（isolate/intercept）派生入口上下文。
func (e *Entry) buildContext() {
	var isolates map[string]string
	if iso := e.options.Isolate; len(iso) > 0 {
		isolates = make(map[string]string, len(iso))
		for name, label := range iso {
			if key, ok := e.realmKey(name, label); ok {
				isolates[name] = key
			}
		}
	}
	e.ctx = e.parent.ctx().extendWithEntry(e, isolates, e.options.Intercept)
}

// init 解析插件并实例化 Fiber；分组入口固定实例化内置分组插件。
func (e *Entry) init() {
	if e.Disabled() || e.fiber != nil {
		return
	}
	e.buildContext()
	p := groupPlugin
	if !e.options.Group {
		var err error
		p, err = e.loader.resolvePlugin(e.options.Name)
		if err != nil {
			e.loader.ctx.app.logger.Error("entry %s: %v", e.ID(), err)
			return
		}
	}
	f, err := e.ctx.Registry().PluginInject(e.ctx, p, e.options.Config, refineInject(p.Inject, e.options.Inject))
	if err != nil {
		if f == nil {
			// 结构性失败：Fiber 从未注册，入口留空。
			e.loader.ctx.app.logger.Error("entry %s: %v", e.ID(), err)
			return
		}
		// 配置校验失败：Fiber 已登记为 FAILED（错误已在 PluginInject
		// 记录），入口保留引用——后续配置修复经 Update 原地恢复，
		// 无需重建实例。
	}
	e.fiber = f
	e.loader.entryFibers[f] = e
	// 配置回写：插件运行期自更新配置时同步回入口配置并提交，
	// 供持久化层（commit 钩子）落盘。
	f.OnUpdate(func(config any) bool {
		if e.updating {
			return true
		}
		legacy := e.options
		e.options.Config = config
		e.parent.tree.commit(EntryChange{ID: legacy.ID, Group: e.parent, Options: &e.options, Legacy: &legacy})
		return true
	})
}

func (e *Entry) disposeFiber() {
	if e.fiber == nil {
		return
	}
	f := e.fiber
	e.fiber = nil
	delete(e.loader.entryFibers, f)
	f.Dispose()
}

// detachSubgroup 同步摘下分组子树：注销全部子入口（树索引与父组
// 立即一致），并把 subgroup 置空。
//
// 必要性：入口的树结构变更必须在**发起变更的这一刻**保持一致，
// 而旧实例的效果回收是异步的（由调度器在后续任务中驱动）。
// 上下文重建（ctxChanged）与跨组移动都会在旧子树尚未下线时
// 实例化新子树——若不先同步摘下，新旧子入口会在短 ID 索引上
// 冲突（查重拒绝新入口，或旧入口的清理误删新入口的索引）。
//
// 旧分组插件的清理回调以 entry.subgroup == g 为守卫，置空后
// 不再重复处理已摘下的子组；Stop 本身亦幂等。
func (e *Entry) detachSubgroup() {
	if e.subgroup == nil {
		return
	}
	g := e.subgroup
	e.subgroup = nil
	g.Stop()
}

// remove 从树中注销入口：先摘除 store 登记（internal/plugin
// 监听据此区分「loader 移除」与「插件自卸载」），再注销 Fiber，
// 分组的子入口递归摘下。
//
// 索引删除是幂等的：仅当 store 槽位仍指向本入口时才清除。
// 重建路径上同 ID 的继任者可能已占据该槽位，旧入口的迟到清理
// 不得误删之。
func (e *Entry) remove() {
	if cur, ok := e.parent.tree.store[e.options.ID]; ok && cur == e {
		delete(e.parent.tree.store, e.options.ID)
	}
	e.disposeFiber()
	e.detachSubgroup()
}

// update 以新配置更新入口：
//
//   - 禁用（含级联）→ 注销 Fiber 并摘下分组子树；
//   - 空间声明变化（name/inject/isolate/intercept）→ 重建上下文
//     并完整重载（先摘下旧子树与旧实例，再以新声明实例化）；
//   - 仅配置变化 → Fiber 热重载；分组入口则协调子入口。
func (e *Entry) update(options EntryOptions) {
	legacy := e.options
	options.ID = legacy.ID
	ctxChanged := legacy.Name != options.Name ||
		legacy.Group != options.Group ||
		!reflect.DeepEqual(legacy.Inject, options.Inject) ||
		!reflect.DeepEqual(legacy.Isolate, options.Isolate) ||
		!reflect.DeepEqual(legacy.Intercept, options.Intercept)
	e.options = options
	if e.Disabled() {
		e.disposeFiber()
		e.detachSubgroup()
		return
	}
	if e.fiber != nil {
		if ctxChanged {
			e.disposeFiber()
			e.detachSubgroup()
			e.init()
			return
		}
		if e.options.Group {
			// 分组入口的任何选项变化都经 Update 触发子入口协调
			//（禁用级联即在此传播）；分组插件以更新钩子否决自身重启。
			e.updating = true
			if err := e.fiber.Update(e.options.Config); err != nil {
				e.loader.ctx.app.logger.Error("entry %s: %v", e.ID(), err)
			}
			e.updating = false
			return
		}
		if !reflect.DeepEqual(legacy.Config, options.Config) {
			e.updating = true
			if err := e.fiber.Update(e.options.Config); err != nil {
				e.loader.ctx.app.logger.Error("entry %s: %v", e.ID(), err)
			}
			e.updating = false
		}
		return
	}
	e.init()
}

// depRemove 入口级依赖声明的移除哨兵类型。
type depRemove struct{}

// DepRemove 显式移除插件声明的依赖：
//
//	Inject: map[string]any{"optional": cordis.DepRemove}
//
// 与 nil（新增/保留必选依赖）区分开。
var DepRemove any = depRemove{}

// refineInject 合并入口级依赖声明到插件声明的依赖表：
// DepRemove 移除依赖，nil 声明必选依赖（无附加配置），
// 其他值覆盖该依赖的拦截配置；也可新增插件未声明的依赖。
func refineInject(declared, entryInject map[string]any) map[string]any {
	out := make(map[string]any, len(declared)+len(entryInject))
	for k, v := range declared {
		out[k] = v
	}
	for k, v := range entryInject {
		if v == DepRemove {
			delete(out, k)
			continue
		}
		out[k] = v
	}
	return out
}

// groupPlugin 内置分组插件：自身不承载业务，其 Fiber 挂载子组，
// 配置（子入口列表）变化时协调子组而非重启自身。
var groupPlugin *Plugin

func init() {
	groupPlugin = &Plugin{
		Name: "group",
		Apply: func(ctx *Context, config any) error {
			entry := ctx.Entry()
			if entry == nil {
				return errors.New("cordis: group plugin requires entry context")
			}
			g := &EntryGroup{tree: entry.parent.tree, parentEntry: entry}
			entry.subgroup = g
			if _, err := ctx.Effect("group", func() (Dispose, error) {
				return func() {
					if entry.subgroup == g {
						entry.subgroup = nil
					}
					g.Stop()
				}, nil
			}); err != nil {
				return err
			}
			// 更新钩子：子入口列表变化 → 协调子组，否决默认重启。
			// 陈旧守卫：自身重载后旧钩子仍存留，此时放行默认行为。
			ctx.Fiber().OnUpdate(func(cfg any) bool {
				if entry.subgroup != g {
					return true
				}
				g.reconcileConfig(cfg)
				return false
			})
			g.reconcileConfig(config)
			return nil
		},
	}
}

// entryOptionsOf 解析分组配置。nil 表示空列表（合法的空分组）；
// 其他非 []EntryOptions 的值表示配置类型错误——返回 ok=false，
// 由调用方决定处置，避免把类型错误静默当成空分组。
func entryOptionsOf(config any) (list []EntryOptions, ok bool) {
	if config == nil {
		return nil, true
	}
	list, ok = config.([]EntryOptions)
	return list, ok
}

// reconcileConfig 以分组配置协调子组。配置类型错误时记日志并
// **保留**现有子入口——静默清场会把一次配置笔误放大为整棵子树
// 的下线，且失败现场无从追溯。
func (g *EntryGroup) reconcileConfig(config any) {
	list, ok := entryOptionsOf(config)
	if !ok {
		id := "<root>"
		if g.parentEntry != nil {
			id = g.parentEntry.ID()
		}
		g.tree.ctx.app.logger.Error("group entry %s: config must be []EntryOptions, got %T (children kept)", id, config)
		return
	}
	g.reconcile(list)
}

// ---------------------------------------------------------------------------
// EntryTree
// ---------------------------------------------------------------------------

// ErrEntryNotFound 目标入口不存在或路径不可解析。
var ErrEntryNotFound = errors.New("cordis: cannot resolve entry")

// EntryTree 入口树：根组 + 按 ID 索引的平铺存储。
// 全路径 ID 以 ":" 分隔（如 "group-a:child"）。
//
// 两套寻址并存，约束不同：
//   - Resolve/Create/Update/Remove 接受路径式 ID（"group-a:child"）；
//   - store 索引以**短 ID** 为键，故短 ID 必须全树唯一
//     （见 EntryOptions.ID）。
type EntryTree struct {
	ctx      *Context
	loader   *Loader
	root     *EntryGroup
	store    map[string]*Entry
	onCommit func(change EntryChange)
}

func (t *EntryTree) commit(change EntryChange) {
	if t.onCommit != nil {
		t.onCommit(change)
	}
}

func (t *EntryTree) ensureID() string {
	for {
		id := fmt.Sprintf("%08x", rand.Uint32())
		if _, ok := t.store[id]; !ok {
			return id
		}
	}
}

// Root 返回根组。
func (t *EntryTree) Root() *EntryGroup { return t.root }

// OnCommit 注册提交钩子：入口结构每次变更后同步回调。
// 文件型配置树可借此将变更持久化到磁盘。
func (t *EntryTree) OnCommit(fn func(change EntryChange)) {
	t.onCommit = fn
}

// Load 以声明式配置协调根组（可重复调用，实现整体协调）。
func (t *EntryTree) Load(options []EntryOptions) {
	t.root.reconcile(options)
}

// Resolve 按全路径 ID 解析入口。
func (t *EntryTree) Resolve(id string) (*Entry, error) {
	segs := strings.Split(id, ":")
	g := t.root
	var e *Entry
	for i, seg := range segs {
		var next *Entry
		for _, c := range g.children {
			if c.options.ID == seg {
				next = c
				break
			}
		}
		if next == nil {
			return nil, fmt.Errorf("%w: %s", ErrEntryNotFound, id)
		}
		e = next
		if i < len(segs)-1 {
			if e.subgroup == nil {
				return nil, fmt.Errorf("%w: %s is not a group", ErrEntryNotFound, seg)
			}
			g = e.subgroup
		}
	}
	return e, nil
}

func (t *EntryTree) resolveGroup(parent string) (*EntryGroup, error) {
	if parent == "" {
		return t.root, nil
	}
	e, err := t.Resolve(parent)
	if err != nil {
		return nil, err
	}
	if e.subgroup == nil {
		return nil, fmt.Errorf("%w: %s is not a group", ErrEntryNotFound, parent)
	}
	return e.subgroup, nil
}

// Create 在 parent 组（"" 为根组）的 position 位置创建入口。
// position 为负或越界时追加到末尾。返回创建的入口。
func (t *EntryTree) Create(options EntryOptions, parent string, position int) (*Entry, error) {
	g, err := t.resolveGroup(parent)
	if err != nil {
		return nil, err
	}
	if options.ID == "" {
		options.ID = t.ensureID()
	}
	if _, dup := t.store[options.ID]; dup {
		return nil, fmt.Errorf("cordis: duplicate entry id %q", options.ID)
	}
	e := &Entry{loader: t.loader, parent: g, options: options}
	t.store[options.ID] = e
	g.insertChild(e, position)
	e.init()
	created := e.options
	t.commit(EntryChange{ID: created.ID, Group: g, Options: &created})
	return e, nil
}

// Remove 按全路径 ID 移除入口。
func (t *EntryTree) Remove(id string) error {
	e, err := t.Resolve(id)
	if err != nil {
		return err
	}
	g := e.parent
	legacy := e.options
	g.removeChild(e)
	e.remove()
	t.commit(EntryChange{ID: legacy.ID, Group: g, Legacy: &legacy})
	return nil
}

// Update 按全路径 ID 更新入口；parent 变化时跨组移动
// （上下文父链变化 → 完整重载）。
func (t *EntryTree) Update(id string, options EntryOptions, parent string, position int) error {
	e, err := t.Resolve(id)
	if err != nil {
		return err
	}
	source := e.parent
	legacy := e.options
	target, err := t.resolveGroup(parent)
	if err != nil {
		return err
	}
	moved := target != source
	if moved {
		source.removeChild(e)
		e.parent = target
		target.insertChild(e, position)
		// 上下文父链变化：注销 Fiber 并同步摘下分组子树，
		// update 走完整重建路径（旧实例的效果回收是异步的，
		// 子树必须立即摘净，否则重建时短 ID 索引冲突）。
		e.disposeFiber()
		e.detachSubgroup()
	}
	e.update(options)
	if moved {
		t.commit(EntryChange{ID: legacy.ID, Group: target, From: source, Options: &e.options, Legacy: &legacy})
	}
	return nil
}

// Stop 移除全部入口（级联回收全部效果）。
func (t *EntryTree) Stop() {
	t.root.Stop()
}

// ---------------------------------------------------------------------------
// Loader
// ---------------------------------------------------------------------------

// Loader 声明式配置层的宿主：持有入口树与插件解析器，
// 以 "loader" 服务挂载于根上下文，并监听 internal/plugin
// 事件以追踪插件的自行卸载（标记入口禁用，避免下次配置加载复活）。
type Loader struct {
	tree        *EntryTree
	ctx         *Context
	resolve     func(name string) (*Plugin, error)
	entryFibers map[*Fiber]*Entry
}

// NewLoader 创建 loader 并挂载到 app 的根上下文。
//
// resolve 是插件解析器：以入口配置的 Name 定位 Plugin 定义，
// 对应官方实现的动态模块导入。所有入口结构操作
// （Load/Create/Remove/Update）经 DoSync 在调度器上执行。
func NewLoader(app *App, resolve func(name string) (*Plugin, error)) *Loader {
	l := &Loader{resolve: resolve, entryFibers: make(map[*Fiber]*Entry)}
	app.DoSync(func(ctx *Context) {
		l.ctx = ctx
		l.tree = &EntryTree{ctx: ctx, loader: l, store: make(map[string]*Entry)}
		l.tree.root = &EntryGroup{tree: l.tree}
		if _, err := ctx.Provide("loader", l, nil); err != nil {
			ctx.app.logger.Error("loader provide: %v", err)
		}
		if _, err := ctx.On("internal/plugin", func(_ *Context, args ...any) any {
			for _, arg := range args {
				if f, ok := arg.(*Fiber); ok {
					l.onPluginEvent(f)
				}
			}
			return nil
		}); err != nil {
			ctx.app.logger.Error("loader listener: %v", err)
		}
	})
	app.Wait()
	return l
}

// Tree 返回入口树。
func (l *Loader) Tree() *EntryTree { return l.tree }

// Load 以声明式配置协调根组（在调度器上执行并等待稳定）。
func (l *Loader) Load(options []EntryOptions) {
	l.ctx.app.DoSync(func(ctx *Context) {
		l.tree.Load(options)
	})
	l.ctx.app.Wait()
}

// Create 在入口树中创建入口（在调度器上执行）。
func (l *Loader) Create(options EntryOptions, parent string, position int) (*Entry, error) {
	var (
		e   *Entry
		err error
	)
	l.ctx.app.DoSync(func(ctx *Context) {
		e, err = l.tree.Create(options, parent, position)
	})
	if err != nil {
		return nil, err
	}
	l.ctx.app.Wait()
	return e, nil
}

// Remove 移除入口（在调度器上执行）。
func (l *Loader) Remove(id string) error {
	var err error
	l.ctx.app.DoSync(func(ctx *Context) {
		err = l.tree.Remove(id)
	})
	if err != nil {
		return err
	}
	l.ctx.app.Wait()
	return nil
}

// Update 更新入口（在调度器上执行）。
func (l *Loader) Update(id string, options EntryOptions, parent string, position int) error {
	var err error
	l.ctx.app.DoSync(func(ctx *Context) {
		err = l.tree.Update(id, options, parent, position)
	})
	if err != nil {
		return err
	}
	l.ctx.app.Wait()
	return nil
}

// Stop 移除全部入口并停止 loader（在调度器上执行）。
func (l *Loader) Stop() {
	l.ctx.app.DoSync(func(ctx *Context) {
		l.tree.Stop()
	})
	l.ctx.app.Wait()
}

func (l *Loader) resolvePlugin(name string) (*Plugin, error) {
	if l.resolve == nil {
		return nil, fmt.Errorf("cordis: no plugin resolver")
	}
	return l.resolve(name)
}

// onPluginEvent 处理 internal/plugin 事件，追踪入口根 Fiber
// 的自行卸载：插件调用 ctx.Fiber().Dispose() 主动退出时，
// 标记入口禁用并提交——防止下次配置协调时意外复活。
// （loader 自身发起的移除/禁用路径已先行登记，在此排除。）
func (l *Loader) onPluginEvent(f *Fiber) {
	e, ok := l.entryFibers[f]
	if !ok {
		return // 非入口根 Fiber（入口内的子插件等）
	}
	if f.UID() != 0 {
		return // 创建事件
	}
	if _, inStore := l.tree.store[e.options.ID]; !inStore {
		return // loader 已移除该入口
	}
	if !l.ctx.registry.Has(f.runtime.plugin) {
		return // 插件定义被整体注销（如热替换）
	}
	if e.Disabled() {
		return // 禁用级联或入口禁用路径
	}
	// 自行卸载：标记禁用并提交。
	legacy := e.options
	e.fiber = nil
	delete(l.entryFibers, f)
	e.options.Disabled = true
	l.tree.commit(EntryChange{ID: legacy.ID, Group: e.parent, Options: &e.options, Legacy: &legacy})
}
