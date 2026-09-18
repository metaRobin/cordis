package cordis_test

import (
	"fmt"
	"testing"
	"time"

	cordis "cordis"
)

// loaderHarness loader 测试脚手架：内置插件目录 + 变更日志。
type loaderHarness struct {
	t       *testing.T
	app     *cordis.App
	loader  *cordis.Loader
	plugins map[string]*cordis.Plugin
	changes []cordis.EntryChange
}

func newLoaderHarness(t *testing.T) *loaderHarness {
	t.Helper()
	h := &loaderHarness{t: t, app: cordis.New(), plugins: map[string]*cordis.Plugin{}}
	h.loader = cordis.NewLoader(h.app, func(name string) (*cordis.Plugin, error) {
		if p, ok := h.plugins[name]; ok {
			return p, nil
		}
		return nil, fmt.Errorf("unknown plugin %q", name)
	})
	h.loader.Tree().OnCommit(func(c cordis.EntryChange) {
		h.changes = append(h.changes, c)
	})
	return h
}

func (h *loaderHarness) close() { h.app.Close() }

func TestLoaderBasicLoad(t *testing.T) {
	h := newLoaderHarness(t)
	defer h.close()

	var applied int
	h.plugins["demo"] = &cordis.Plugin{
		Name: "demo",
		Apply: func(ctx *cordis.Context, config any) error {
			applied++
			if config != "hello" {
				return fmt.Errorf("unexpected config: %v", config)
			}
			return nil
		},
	}

	h.loader.Load([]cordis.EntryOptions{
		{ID: "main", Name: "demo", Config: "hello"},
	})

	e, err := h.loader.Tree().Resolve("main")
	if err != nil {
		t.Fatal(err)
	}
	if e.Fiber() == nil || e.Fiber().State() != cordis.StateActive {
		t.Fatalf("entry fiber should be active: %v", e.Fiber())
	}
	if applied != 1 {
		t.Fatalf("apply count: %d", applied)
	}

	// 再次 Load 相同配置：无变更，不重载。
	h.loader.Load([]cordis.EntryOptions{
		{ID: "main", Name: "demo", Config: "hello"},
	})
	if applied != 1 {
		t.Fatalf("unchanged config must not reload: %d", applied)
	}
}

func TestLoaderReconcile(t *testing.T) {
	h := newLoaderHarness(t)
	defer h.close()

	var log []string
	h.plugins["a"] = &cordis.Plugin{
		Name: "a",
		Apply: func(ctx *cordis.Context, _ any) error {
			log = append(log, "a+")
			_, err := ctx.Effect("e", func() (cordis.Dispose, error) {
				return func() { log = append(log, "a-") }, nil
			})
			return err
		},
	}
	h.plugins["b"] = &cordis.Plugin{
		Name: "b",
		Apply: func(ctx *cordis.Context, _ any) error {
			log = append(log, "b+")
			return nil
		},
	}

	h.loader.Load([]cordis.EntryOptions{
		{ID: "1", Name: "a"},
		{ID: "2", Name: "b"},
	})
	if fmt.Sprint(log) != fmt.Sprint([]string{"a+", "b+"}) {
		t.Fatalf("initial load: %v", log)
	}

	// 协调：移除 1、保留 2、新增 3。
	log = nil
	h.loader.Load([]cordis.EntryOptions{
		{ID: "2", Name: "b"},
		{ID: "3", Name: "a"},
	})
	want := []string{"a-", "a+"}
	if fmt.Sprint(log) != fmt.Sprint(want) {
		t.Fatalf("reconcile: %v", log)
	}
	if _, err := h.loader.Tree().Resolve("1"); err == nil {
		t.Fatal("entry 1 should be removed")
	}
	if e, err := h.loader.Tree().Resolve("3"); err != nil || e.Fiber() == nil {
		t.Fatalf("entry 3 should exist and be loaded: %v %v", e, err)
	}
}

func TestLoaderConfigReload(t *testing.T) {
	h := newLoaderHarness(t)
	defer h.close()

	var seen []any
	h.plugins["cfg"] = &cordis.Plugin{
		Name: "cfg",
		Apply: func(ctx *cordis.Context, config any) error {
			seen = append(seen, config)
			return nil
		},
	}

	h.loader.Load([]cordis.EntryOptions{{ID: "m", Name: "cfg", Config: 1}})
	h.loader.Load([]cordis.EntryOptions{{ID: "m", Name: "cfg", Config: 2}})
	if fmt.Sprint(seen) != fmt.Sprint([]any{1, 2}) {
		t.Fatalf("config reload: %v", seen)
	}

	// 回写断言：fiber 内部 Update 的配置应同步回入口。
	e, _ := h.loader.Tree().Resolve("m")
	h.app.DoSync(func(ctx *cordis.Context) {
		e.Fiber().Update(3)
	})
	h.app.Wait()
	if e.Options().Config != 3 {
		t.Fatalf("config writeback: %v", e.Options().Config)
	}
}

func TestLoaderGroup(t *testing.T) {
	h := newLoaderHarness(t)
	defer h.close()

	var log []string
	h.plugins["leaf"] = &cordis.Plugin{
		Name: "leaf",
		Apply: func(ctx *cordis.Context, _ any) error {
			log = append(log, "leaf+")
			_, err := ctx.Effect("e", func() (cordis.Dispose, error) {
				return func() { log = append(log, "leaf-") }, nil
			})
			return err
		},
	}

	h.loader.Load([]cordis.EntryOptions{
		{ID: "g", Name: "group", Group: true, Config: []cordis.EntryOptions{
			{ID: "c1", Name: "leaf"},
			{ID: "c2", Name: "leaf"},
		}},
	})
	if fmt.Sprint(log) != fmt.Sprint([]string{"leaf+", "leaf+"}) {
		t.Fatalf("group load: %v", log)
	}

	// 分组配置更新：移除 c1，保留 c2。
	log = nil
	h.loader.Load([]cordis.EntryOptions{
		{ID: "g", Name: "group", Group: true, Config: []cordis.EntryOptions{
			{ID: "c2", Name: "leaf"},
		}},
	})
	if fmt.Sprint(log) != fmt.Sprint([]string{"leaf-"}) {
		t.Fatalf("group reconcile: %v", log)
	}

	// 全路径解析。
	if _, err := h.loader.Tree().Resolve("g:c2"); err != nil {
		t.Fatalf("resolve nested: %v", err)
	}

	// 禁用分组 → 子入口级联下线。
	log = nil
	h.loader.Load([]cordis.EntryOptions{
		{ID: "g", Name: "group", Group: true, Disabled: true, Config: []cordis.EntryOptions{
			{ID: "c2", Name: "leaf"},
		}},
	})
	if fmt.Sprint(log) != fmt.Sprint([]string{"leaf-"}) {
		t.Fatalf("disable cascade: %v", log)
	}
}

// 回归（M-1 交互 + 审查测试缺口）：分组入口的空间声明变化
// （isolate）走 ctxChanged 完整重建路径——旧子树必须被同步摘下，
// 否则重建时新子入口会与旧子入口在短 ID 索引上冲突而被查重拒绝。
func TestLoaderGroupIsolateRebuild(t *testing.T) {
	h := newLoaderHarness(t)
	defer h.close()

	var applied int
	h.plugins["leaf"] = &cordis.Plugin{
		Name: "leaf",
		Apply: func(ctx *cordis.Context, _ any) error {
			applied++
			_, err := ctx.Provide("leaf", applied, nil)
			return err
		},
	}

	child := []cordis.EntryOptions{{ID: "x", Name: "leaf"}}
	h.loader.Load([]cordis.EntryOptions{
		{ID: "g", Name: "group", Group: true, Config: child},
	})
	e, err := h.loader.Tree().Resolve("g")
	if err != nil {
		t.Fatal(err)
	}
	f0 := e.Fiber()
	if f0 == nil || applied != 1 {
		t.Fatalf("initial load: fiber=%v applied=%d", f0, applied)
	}

	// 入口级 isolate 声明变化 → 上下文重建：分组 Fiber 与整个子树
	// 以新声明重新实例化，旧实例完整下线。
	h.loader.Load([]cordis.EntryOptions{
		{ID: "g", Name: "group", Group: true, Isolate: map[string]any{"leaf": "realm-a"}, Config: child},
	})
	f1 := e.Fiber()
	if f1 == nil || f1 == f0 {
		t.Fatalf("isolate change must rebuild the group fiber: %v -> %v", f0, f1)
	}
	if applied != 2 {
		t.Fatalf("child must be re-instantiated under the new context: applied=%d", applied)
	}
	ce, err := h.loader.Tree().Resolve("g:x")
	if err != nil {
		t.Fatalf("subgroup child must survive the rebuild: %v", err)
	}
	if ce.Fiber() == nil || ce.Fiber().State() != cordis.StateActive {
		t.Fatalf("rebuilt child should be active, got %v", ce.Fiber())
	}
	// 旧子入口的索引不得残留（短 ID 索引与树结构保持一致）。
	if cur := h.loader.Tree().Root(); len(cur.Children()) != 1 {
		t.Fatalf("root should hold exactly the group entry: %d", len(cur.Children()))
	}
}

// 回归（N-3）：分组配置类型错误（非 []EntryOptions）不得被
// 静默当成空列表——那会把一次配置笔误放大成整棵子树下线。
func TestLoaderGroupConfigTypeError(t *testing.T) {
	h := newLoaderHarness(t)
	defer h.close()

	h.plugins["leaf"] = &cordis.Plugin{
		Name:  "leaf",
		Apply: func(ctx *cordis.Context, _ any) error { return nil },
	}
	h.loader.Load([]cordis.EntryOptions{
		{ID: "g", Name: "group", Group: true, Config: []cordis.EntryOptions{
			{ID: "c", Name: "leaf"},
		}},
	})
	g, err := h.loader.Tree().Resolve("g")
	if err != nil {
		t.Fatal(err)
	}
	if n := len(g.Subgroup().Children()); n != 1 {
		t.Fatalf("initial children: %d", n)
	}

	h.loader.Load([]cordis.EntryOptions{
		{ID: "g", Name: "group", Group: true, Config: "oops"},
	})
	if n := len(g.Subgroup().Children()); n != 1 {
		t.Fatalf("config type error must keep children, got %d", n)
	}
	ce, err := h.loader.Tree().Resolve("g:c")
	if err != nil {
		t.Fatalf("child entry should survive the bad config: %v", err)
	}
	if ce.Fiber() == nil || ce.Fiber().State() != cordis.StateActive {
		t.Fatalf("child should stay active, got %v", ce.Fiber())
	}
}

func TestLoaderEntryIsolate(t *testing.T) {
	h := newLoaderHarness(t)
	defer h.close()

	// 域语义：true = 私有域（入口独享，可并存同名服务），
	// 字符串 = 共享域（相同标签的提供者与依赖者互通）。
	h.plugins["provider"] = &cordis.Plugin{
		Name: "provider",
		Apply: func(ctx *cordis.Context, config any) error {
			_, err := ctx.Provide("svc", config, nil)
			return err
		},
	}
	var seen []any
	newConsumer := func(name string) *cordis.Plugin {
		return &cordis.Plugin{
			Name:   name,
			Inject: map[string]any{"svc": nil},
			Apply: func(ctx *cordis.Context, _ any) error {
				v, _ := ctx.Get("svc")
				seen = append(seen, v)
				return nil
			},
		}
	}
	h.plugins["consumer"] = newConsumer("consumer")
	h.plugins["consumer2"] = newConsumer("consumer2")

	// p1/p2 各占私有域：同名服务互不冲突；
	// c1/c2 分别消费共享域 one/two 的服务。
	h.loader.Load([]cordis.EntryOptions{
		{ID: "p1", Name: "provider", Config: 1, Isolate: map[string]any{"svc": "one"}},
		{ID: "p2", Name: "provider", Config: 2, Isolate: map[string]any{"svc": "two"}},
		{ID: "c1", Name: "consumer", Isolate: map[string]any{"svc": "one"}},
		{ID: "c2", Name: "consumer2", Isolate: map[string]any{"svc": "two"}},
	})
	if fmt.Sprint(seen) != fmt.Sprint([]any{1, 2}) {
		t.Fatalf("shared realm routing: %v", seen)
	}

	// 私有域：两个同名提供者并存，互不可见。
	seen = nil
	h.loader.Load([]cordis.EntryOptions{
		{ID: "p1", Name: "provider", Config: 1, Isolate: map[string]any{"svc": true}},
		{ID: "p2", Name: "provider", Config: 2, Isolate: map[string]any{"svc": true}},
		{ID: "c1", Name: "consumer", Isolate: map[string]any{"svc": true}},
	})
	// 消费者私有域 "#c1" 与两个提供者的私有域均不同 → 依赖不满足。
	if len(seen) != 0 {
		t.Fatalf("private realms must not leak: %v", seen)
	}
	e1, _ := h.loader.Tree().Resolve("p1")
	e2, _ := h.loader.Tree().Resolve("p2")
	if e1.Fiber().State() != cordis.StateActive || e2.Fiber().State() != cordis.StateActive {
		t.Fatalf("private-realm providers should coexist: %v %v", e1.Fiber().State(), e2.Fiber().State())
	}
}

func TestLoaderEntryInject(t *testing.T) {
	h := newLoaderHarness(t)
	defer h.close()

	var log []string
	h.plugins["db"] = &cordis.Plugin{
		Name: "db",
		Apply: func(ctx *cordis.Context, _ any) error {
			_, err := ctx.Provide("db", "conn", nil)
			return err
		},
	}
	// 插件未声明依赖；入口级 inject 补充声明。
	// Apply 记录 db 可见性（协效应严格性：未声明的服务不可访问）。
	h.plugins["app"] = &cordis.Plugin{
		Name: "app",
		Apply: func(ctx *cordis.Context, _ any) error {
			_, ok := ctx.Get("db")
			log = append(log, fmt.Sprintf("db=%v", ok))
			return nil
		},
	}

	// db 不存在时先加载 app：依赖未满足，Fiber 停留在 PENDING。
	h.loader.Load([]cordis.EntryOptions{{ID: "a", Name: "app", Inject: map[string]any{"db": nil}}})
	ea, _ := h.loader.Tree().Resolve("a")
	if ea.Fiber() == nil || ea.Fiber().State() != cordis.StatePending {
		t.Fatalf("app should stay pending without db: %v", ea.Fiber())
	}

	// 引入 db → app 自动激活（响应式协效应）。
	h.loader.Load([]cordis.EntryOptions{
		{ID: "d", Name: "db"},
		{ID: "a", Name: "app", Inject: map[string]any{"db": nil}},
	})
	if fmt.Sprint(log) != fmt.Sprint([]string{"db=true"}) {
		t.Fatalf("entry inject activation: %v", log)
	}

	// 入口移除 inject 声明：依赖解除 → 立即重载，
	// 且未声明的服务不再可见（协效应严格性）。
	log = nil
	h.loader.Load([]cordis.EntryOptions{
		{ID: "d", Name: "db"},
		{ID: "a", Name: "app", Inject: map[string]any{}},
	})
	if fmt.Sprint(log) != fmt.Sprint([]string{"db=false"}) {
		t.Fatalf("inject removal: %v", log)
	}
}

func TestLoaderTreeOperations(t *testing.T) {
	h := newLoaderHarness(t)
	defer h.close()

	h.plugins["p"] = &cordis.Plugin{
		Name:  "p",
		Apply: func(ctx *cordis.Context, _ any) error { return nil },
	}

	// Create → 存在且 active。
	e, err := h.loader.Create(cordis.EntryOptions{Name: "p"}, "", -1)
	if err != nil {
		t.Fatal(err)
	}
	id := e.ID()
	if e.Fiber() == nil || e.Fiber().State() != cordis.StateActive {
		t.Fatal("created entry should be active")
	}

	// Update 配置 → fiber 热重载。
	if err := h.loader.Update(id, cordis.EntryOptions{Name: "p", Config: "v"}, "", -1); err != nil {
		t.Fatal(err)
	}
	ee, _ := h.loader.Tree().Resolve(id)
	if ee.Options().Config != "v" {
		t.Fatalf("update config: %v", ee.Options().Config)
	}

	// Remove → 注销。
	if err := h.loader.Remove(id); err != nil {
		t.Fatal(err)
	}
	if _, err := h.loader.Tree().Resolve(id); err == nil {
		t.Fatal("entry should be removed")
	}

	// 未知 ID 的错误路径。
	if err := h.loader.Remove("nope"); err == nil {
		t.Fatal("expected error for unknown id")
	}
}

// 回归：跨组移动（EntryTree.Update 的 moved 路径）也必须同步摘下
// 分组子树——否则旧子树残留（子入口永不注销、短 ID 索引与树脱钩，
// 且重建被查重拒绝）。
func TestLoaderGroupMoveRebuild(t *testing.T) {
	h := newLoaderHarness(t)
	defer h.close()

	var applied int
	h.plugins["leaf"] = &cordis.Plugin{
		Name: "leaf",
		Apply: func(ctx *cordis.Context, _ any) error {
			applied++
			_, err := ctx.Provide("leaf", applied, nil)
			return err
		},
	}

	child := []cordis.EntryOptions{{ID: "c", Name: "leaf"}}
	h.loader.Load([]cordis.EntryOptions{
		{ID: "outer", Name: "group", Group: true},
		{ID: "inner", Name: "group", Group: true, Config: child},
	})
	if applied != 1 {
		t.Fatalf("initial load: applied=%d", applied)
	}

	// 把 inner 分组整体移入 outer。
	if err := h.loader.Update("inner", cordis.EntryOptions{Name: "group", Group: true, Config: child}, "outer", -1); err != nil {
		t.Fatal(err)
	}
	if _, err := h.loader.Tree().Resolve("outer:inner:c"); err != nil {
		t.Fatalf("moved subtree must be rebuilt under the new parent: %v", err)
	}
	if applied != 2 {
		t.Fatalf("child must be re-instantiated after the move: applied=%d", applied)
	}
	outer, err := h.loader.Tree().Resolve("outer")
	if err != nil {
		t.Fatal(err)
	}
	if n := len(outer.Subgroup().Children()); n != 1 {
		t.Fatalf("outer should hold exactly the moved group: %d", n)
	}
}

// 回归（M-1）：reconcile 的重复短 ID 查重——
// 跨组同名与同列表重复均拒绝（跳过并记日志），store 索引不被覆盖。
func TestLoaderDuplicateShortID(t *testing.T) {
	h := newLoaderHarness(t)
	defer h.close()

	h.plugins["leaf"] = &cordis.Plugin{
		Name:  "leaf",
		Apply: func(*cordis.Context, any) error { return nil },
	}

	// 两个分组各含同名子入口 "web"：后到者被拒，索引仍指向首组。
	h.loader.Load([]cordis.EntryOptions{
		{ID: "g1", Name: "group", Group: true, Config: []cordis.EntryOptions{
			{ID: "web", Name: "leaf"},
		}},
		{ID: "g2", Name: "group", Group: true, Config: []cordis.EntryOptions{
			{ID: "web", Name: "leaf"},
		}},
	})
	if _, err := h.loader.Tree().Resolve("g1:web"); err != nil {
		t.Fatalf("g1:web should exist: %v", err)
	}
	if _, err := h.loader.Tree().Resolve("g2:web"); err == nil {
		t.Fatal("cross-group duplicate short id must be rejected")
	}
	if _, err := h.loader.Create(cordis.EntryOptions{ID: "web", Name: "leaf"}, "", -1); err == nil {
		t.Fatal("create with existing short id should fail")
	}

	// 同一配置列表内的重复 ID：子入口只实例化一次。
	h.loader.Load([]cordis.EntryOptions{
		{ID: "g1", Name: "group", Group: true, Config: []cordis.EntryOptions{
			{ID: "web", Name: "leaf"},
			{ID: "web", Name: "leaf"},
		}},
	})
	g1, err := h.loader.Tree().Resolve("g1")
	if err != nil {
		t.Fatal(err)
	}
	if n := len(g1.Subgroup().Children()); n != 1 {
		t.Fatalf("duplicate ids in one config list must yield a single child: %d", n)
	}
}

// 回归（M-2）：loader 路径的配置校验失败——入口保留 FAILED fiber
// （状态可观测），配置修复后经 Update 原地恢复，不重建实例。
func TestLoaderConfigErrorRecovery(t *testing.T) {
	h := newLoaderHarness(t)
	defer h.close()

	h.plugins["strict"] = &cordis.Plugin{
		Name: "strict",
		Validate: func(config any) (any, error) {
			if s, ok := config.(string); ok {
				return s, nil
			}
			return nil, fmt.Errorf("config must be string")
		},
		Apply: func(*cordis.Context, any) error { return nil },
	}

	h.loader.Load([]cordis.EntryOptions{{ID: "s", Name: "strict", Config: 42}})
	e, _ := h.loader.Tree().Resolve("s")
	if e.Fiber() == nil || e.Fiber().State() != cordis.StateFailed {
		t.Fatalf("invalid config should keep a FAILED fiber on entry: %v", e.Fiber())
	}

	f := e.Fiber()
	h.loader.Load([]cordis.EntryOptions{{ID: "s", Name: "strict", Config: "ok"}})
	if e.Fiber() != f {
		t.Fatal("recovery should reuse the same fiber instance")
	}
	if f.State() != cordis.StateActive {
		t.Fatalf("fixed config should activate fiber: %s", f.State())
	}
}

func TestLoaderCommitHook(t *testing.T) {
	h := newLoaderHarness(t)
	defer h.close()

	h.plugins["p"] = &cordis.Plugin{
		Name:  "p",
		Apply: func(ctx *cordis.Context, _ any) error { return nil },
	}

	h.loader.Load([]cordis.EntryOptions{{ID: "x", Name: "p"}})
	h.loader.Load([]cordis.EntryOptions{{ID: "y", Name: "p"}})
	if len(h.changes) != 3 { // create x、remove x、create y
		t.Fatalf("commit count: %d", len(h.changes))
	}
	if h.changes[0].Options == nil || h.changes[0].Legacy != nil {
		t.Fatalf("first change should be create: %+v", h.changes[0])
	}
	if h.changes[1].Options != nil || h.changes[1].Legacy == nil {
		t.Fatalf("second change should be remove: %+v", h.changes[1])
	}
}

func TestLoaderSelfDispose(t *testing.T) {
	h := newLoaderHarness(t)
	defer h.close()

	var self *cordis.Fiber
	h.plugins["suicidal"] = &cordis.Plugin{
		Name: "suicidal",
		Apply: func(ctx *cordis.Context, _ any) error {
			self = ctx.Fiber()
			return nil
		},
	}

	h.loader.Load([]cordis.EntryOptions{{ID: "s", Name: "suicidal"}})
	e, _ := h.loader.Tree().Resolve("s")
	if e.Fiber() == nil {
		t.Fatal("should be loaded")
	}

	// 插件自行卸载。
	h.app.DoSync(func(ctx *cordis.Context) { self.Dispose() })
	h.app.Wait()

	if e.Fiber() != nil {
		t.Fatal("entry fiber should be detached")
	}
	if !e.Disabled() {
		t.Fatal("self-disposed entry should be marked disabled")
	}
	// 最后一次提交记录了禁用变更。
	last := h.changes[len(h.changes)-1]
	if last.Legacy == nil || last.Options == nil || !last.Options.Disabled {
		t.Fatalf("last commit should record disable: %+v", last)
	}
}

// 回归（H-1 的 loader 路径）：单次 Load 协调 1100 个入口——每个
// 入口在同一个调度任务内投递一次 pump，无界队列不得自死锁。
func TestLoaderLargeLoad(t *testing.T) {
	h := newLoaderHarness(t)
	defer h.close()

	h.plugins["leaf"] = &cordis.Plugin{
		Name:  "leaf",
		Apply: func(ctx *cordis.Context, _ any) error { return nil },
	}

	const n = 1100
	opts := make([]cordis.EntryOptions, 0, n)
	for i := 0; i < n; i++ {
		opts = append(opts, cordis.EntryOptions{ID: fmt.Sprintf("e%d", i), Name: "leaf"})
	}

	done := make(chan struct{})
	var (
		loadErr   error
		children  int
		activeCnt int
	)
	go func() {
		defer close(done)
		h.loader.Load(opts)
		children = len(h.loader.Tree().Root().Children())
		for i := 0; i < n; i++ {
			e, err := h.loader.Tree().Resolve(fmt.Sprintf("e%d", i))
			if err != nil {
				loadErr = err
				return
			}
			if e.Fiber() != nil && e.Fiber().State() == cordis.StateActive {
				activeCnt++
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("deadlock: 1100-entry Load blocked the scheduler")
	}
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if children != n {
		t.Fatalf("root children: %d, want %d", children, n)
	}
	if activeCnt != n {
		t.Fatalf("active entries: %d, want %d", activeCnt, n)
	}
}

// BenchmarkLoaderLoad 度量声明式协调的吞吐：每个入口一次
// 实例化 + 索引登记（H-1 修复后不再受固定队列容量限制）。
func BenchmarkLoaderLoad(b *testing.B) {
	b.Run("load-100", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			h := &loaderHarness{app: cordis.New(), plugins: map[string]*cordis.Plugin{}}
			h.loader = cordis.NewLoader(h.app, func(string) (*cordis.Plugin, error) {
				return &cordis.Plugin{Name: "leaf", Apply: func(*cordis.Context, any) error { return nil }}, nil
			})
			opts := make([]cordis.EntryOptions, 0, 100)
			for j := 0; j < 100; j++ {
				opts = append(opts, cordis.EntryOptions{ID: fmt.Sprintf("e%d", j), Name: "leaf"})
			}
			h.loader.Load(opts)
			h.app.Close()
		}
	})
}
