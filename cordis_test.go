package cordis_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	cordis "cordis"
)

// harness 测试脚手架：所有场景在调度器上下文中执行，
// app.Wait 保证状态机收敛后再断言。
type harness struct {
	t   *testing.T
	app *cordis.App
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	return &harness{t: t, app: cordis.New()}
}

func (h *harness) run(f func(ctx *cordis.Context)) {
	h.app.DoSync(f)
	h.app.Wait()
}

// states 记录 fiber 的状态迁移序列（internal/status 事件）。
type states struct {
	mu    sync.Mutex
	trace []string
}

func (s *states) record(f *cordis.Fiber, from cordis.FiberState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.trace = append(s.trace, fmt.Sprintf("%s>%s", from, f.State()))
}

func TestPluginLifecycle(t *testing.T) {
	h := newHarness(t)
	defer h.app.Close()

	var order []string
	p := &cordis.Plugin{
		Name: "demo",
		Apply: func(ctx *cordis.Context, config any) error {
			order = append(order, "apply")
			if _, err := ctx.Effect("e1", func() (cordis.Dispose, error) {
				return func() { order = append(order, "dispose1") }, nil
			}); err != nil {
				return err
			}
			if _, err := ctx.Effect("e2", func() (cordis.Dispose, error) {
				return func() { order = append(order, "dispose2") }, nil
			}); err != nil {
				return err
			}
			return nil
		},
	}

	var f *cordis.Fiber
	h.run(func(ctx *cordis.Context) {
		var err error
		f, err = ctx.Plugin(p, nil)
		if err != nil {
			t.Fatal(err)
		}
		if f.State() != cordis.StatePending && f.State() != cordis.StateLoading {
			// 注册在同步段内完成，转换任务入队但未执行。
			t.Fatalf("expected pre-transition state, got %s", f.State())
		}
	})
	if f.State() != cordis.StateActive {
		t.Fatalf("expected active, got %s", f.State())
	}

	h.run(func(ctx *cordis.Context) {
		f.Dispose()
	})
	if f.State() != cordis.StateDisposed {
		t.Fatalf("expected disposed, got %s", f.State())
	}
	want := []string{"apply", "dispose2", "dispose1"} // LIFO 逆序回收
	if fmt.Sprint(order) != fmt.Sprint(want) {
		t.Fatalf("unexpected order: %v", order)
	}
}

func TestReactiveCoeffects(t *testing.T) {
	h := newHarness(t)
	defer h.app.Close()

	var events []string
	provider := &cordis.Plugin{
		Name: "database",
		Apply: func(ctx *cordis.Context, _ any) error {
			events = append(events, "db+")
			_, err := ctx.Provide("database", map[string]string{"kind": "sqlite"}, nil)
			return err
		},
	}
	consumer := &cordis.Plugin{
		Name:   "cache",
		Inject: map[string]any{"database": nil},
		Apply: func(ctx *cordis.Context, _ any) error {
			db, ok := ctx.Get("database")
			if !ok {
				return fmt.Errorf("database not visible")
			}
			if db.(map[string]string)["kind"] != "sqlite" {
				return fmt.Errorf("wrong service value")
			}
			events = append(events, "cache+")
			_, err := ctx.Effect("conn", func() (cordis.Dispose, error) {
				return func() { events = append(events, "cache-") }, nil
			})
			return err
		},
	}

	var db, cache *cordis.Fiber
	h.run(func(ctx *cordis.Context) {
		db, _ = ctx.Plugin(provider, nil)
		cache, _ = ctx.Plugin(consumer, nil)
	})
	if cache.State() != cordis.StateActive {
		t.Fatalf("consumer should be active, got %s", cache.State())
	}

	// 提供者下线 → 依赖者自动卸载（效果回收），回到 PENDING。
	h.run(func(ctx *cordis.Context) {
		db.Dispose()
	})
	if cache.State() != cordis.StatePending {
		t.Fatalf("consumer should fall back to pending, got %s", cache.State())
	}
	if fmt.Sprint(events) != fmt.Sprint([]string{"db+", "cache+", "cache-"}) {
		t.Fatalf("unexpected events: %v", events)
	}

	// 提供者回归 → 依赖者自动恢复。
	events = nil
	h.run(func(ctx *cordis.Context) {
		db2, _ := ctx.Plugin(provider, nil)
		db = db2
	})
	if cache.State() != cordis.StateActive {
		t.Fatalf("consumer should reactivate, got %s", cache.State())
	}
	if fmt.Sprint(events) != fmt.Sprint([]string{"db+", "cache+"}) {
		t.Fatalf("unexpected events: %v", events)
	}
}

func TestDependantFirstDispose(t *testing.T) {
	h := newHarness(t)
	defer h.app.Close()

	var log []string
	provider := &cordis.Plugin{
		Name: "database",
		Apply: func(ctx *cordis.Context, _ any) error {
			// 先注册服务，再注册尾部效果：LIFO 下尾部先回收，
			// 服务撤销（含等待依赖者下线）最后完成。
			_, err := ctx.Provide("database", "db", nil)
			if err != nil {
				return err
			}
			_, err = ctx.Effect("tail", func() (cordis.Dispose, error) {
				return func() { log = append(log, "provider-tail-") }, nil
			})
			return err
		},
	}
	consumer := &cordis.Plugin{
		Name:   "consumer",
		Inject: map[string]any{"database": nil},
		Apply: func(ctx *cordis.Context, _ any) error {
			_, err := ctx.Effect("work", func() (cordis.Dispose, error) {
				return func() { log = append(log, "consumer-") }, nil
			})
			return err
		},
	}

	var db, app *cordis.Fiber
	h.run(func(ctx *cordis.Context) {
		db, _ = ctx.Plugin(provider, nil)
		app, _ = ctx.Plugin(consumer, nil)
	})
	_ = app
	h.run(func(ctx *cordis.Context) {
		db.Dispose()
	})
	want := []string{"provider-tail-", "consumer-"}
	if fmt.Sprint(log) != fmt.Sprint(want) {
		t.Fatalf("dependant must unload before provider service cleanup: %v", log)
	}
}

func TestIsolation(t *testing.T) {
	h := newHarness(t)
	defer h.app.Close()

	var activated int
	var seenFoo any
	newConsumer := func() *cordis.Plugin {
		return &cordis.Plugin{
			Inject: map[string]any{"foo": nil},
			Apply: func(ctx *cordis.Context, _ any) error {
				activated++
				if v, ok := ctx.Get("foo"); ok {
					seenFoo = v
				}
				return nil
			},
		}
	}

	var rootFiber, realm1, realm2 *cordis.Fiber
	h.run(func(ctx *cordis.Context) {
		rootFiber, _ = ctx.Plugin(newConsumer(), nil)
		r1, _ := ctx.Isolate("foo", "realm-1").Plugin(newConsumer(), nil)
		realm1 = r1
		r2, _ := ctx.Isolate("foo", "realm-2").Plugin(newConsumer(), nil)
		realm2 = r2
	})
	if activated != 0 {
		t.Fatalf("no service yet, no consumer should be active: %d", activated)
	}

	// 默认域提供 → 仅默认域依赖者激活，且取到本域值。
	h.run(func(ctx *cordis.Context) {
		ctx.Provide("foo", 100, nil)
	})
	if activated != 1 || rootFiber.State() != cordis.StateActive || seenFoo != 100 {
		t.Fatalf("only default-realm consumer should activate: %d seen=%v", activated, seenFoo)
	}

	// realm-1 域内的提供者插件注册服务 → 仅 realm-1 依赖者激活。
	provider := &cordis.Plugin{
		Name: "foo-provider",
		Apply: func(ctx *cordis.Context, _ any) error {
			_, err := ctx.Provide("foo", 200, nil)
			return err
		},
	}
	seenFoo = nil
	h.run(func(ctx *cordis.Context) {
		ctx.Isolate("foo", "realm-1").Plugin(provider, nil)
	})
	if activated != 2 || realm1.State() != cordis.StateActive || realm2.State() != cordis.StatePending {
		t.Fatalf("realm isolation broken: activated=%d", activated)
	}
	if seenFoo != 200 {
		t.Fatalf("realm-1 consumer should see realm-1 value, got %v", seenFoo)
	}

	// 跨域不可见：realm-2 上下文解析不到任何域的服务；
	// 默认域上下文只看到默认域的服务。
	h.run(func(ctx *cordis.Context) {
		if _, ok := ctx.Isolate("foo", "realm-2").Get("foo"); ok {
			t.Fatal("realm-2 must not see services of other realms")
		}
		if v, ok := ctx.Get("foo"); !ok || v != 100 {
			t.Fatalf("default realm should see its own service, got %v %v", v, ok)
		}
	})
}

func TestSharedRealm(t *testing.T) {
	h := newHarness(t)
	defer h.app.Close()

	var activated int
	newConsumer := func() *cordis.Plugin {
		return &cordis.Plugin{
			Inject: map[string]any{"foo": nil},
			Apply:  func(*cordis.Context, any) error { activated++; return nil },
		}
	}
	var a, b *cordis.Fiber
	h.run(func(ctx *cordis.Context) {
		ctx1 := ctx.Isolate("foo", "shared")
		a, _ = ctx1.Plugin(newConsumer(), nil)
		ctx2 := ctx.Isolate("foo", "shared")
		b, _ = ctx2.Plugin(newConsumer(), nil)
	})
	h.run(func(ctx *cordis.Context) {
		ctx.Isolate("foo", "shared").Provide("foo", 1, nil)
	})
	if activated != 2 || a.State() != cordis.StateActive || b.State() != cordis.StateActive {
		t.Fatalf("shared realm should satisfy both consumers: %d", activated)
	}
}

func TestHotReload(t *testing.T) {
	h := newHarness(t)
	defer h.app.Close()

	var applied, disposed int
	var seenConfig string
	p := &cordis.Plugin{
		Name: "server",
		Apply: func(ctx *cordis.Context, config any) error {
			applied++
			seenConfig = config.(string)
			_, err := ctx.Effect("listener", func() (cordis.Dispose, error) {
				return func() { disposed++ }, nil
			})
			return err
		},
	}
	var f *cordis.Fiber
	h.run(func(ctx *cordis.Context) {
		f, _ = ctx.Plugin(p, "v1")
	})
	if seenConfig != "v1" || applied != 1 {
		t.Fatalf("bad initial load: %v %v", seenConfig, applied)
	}

	h.run(func(ctx *cordis.Context) {
		if err := f.Update("v2"); err != nil {
			t.Fatal(err)
		}
	})
	if applied != 2 || disposed != 1 || seenConfig != "v2" {
		t.Fatalf("hot reload failed: applied=%d disposed=%d config=%s", applied, disposed, seenConfig)
	}
	if f.State() != cordis.StateActive {
		t.Fatalf("expected active after reload, got %s", f.State())
	}
}

func TestFailureAndRecovery(t *testing.T) {
	h := newHarness(t)
	defer h.app.Close()

	var applied, disposed int
	fail := true
	p := &cordis.Plugin{
		Name: "flaky",
		Apply: func(ctx *cordis.Context, _ any) error {
			applied++
			_, err := ctx.Effect("e", func() (cordis.Dispose, error) {
				return func() { disposed++ }, nil
			})
			if err != nil {
				return err
			}
			if fail {
				return fmt.Errorf("boom")
			}
			return nil
		},
	}
	var f *cordis.Fiber
	h.run(func(ctx *cordis.Context) {
		f, _ = ctx.Plugin(p, nil)
	})
	if f.State() != cordis.StateFailed || f.Err() == nil {
		t.Fatalf("expected failed fiber, got %s err=%v", f.State(), f.Err())
	}
	// 失败实例的效果同样被回收。
	if disposed != 1 {
		t.Fatalf("effects of failed load must be reverted, disposed=%d", disposed)
	}

	// Update 清除错误并重启。
	fail = false
	h.run(func(ctx *cordis.Context) {
		if err := f.Update(nil); err != nil {
			t.Fatal(err)
		}
	})
	if f.State() != cordis.StateActive || f.Err() != nil {
		t.Fatalf("expected recovery, got %s err=%v", f.State(), f.Err())
	}
	// 失败实例的效果已在失败路径回收（disposed=1），
	// 重启是一次干净的加载：不产生新的撤销。
	if applied != 2 || disposed != 1 {
		t.Fatalf("unexpected counts: applied=%d disposed=%d", applied, disposed)
	}
}

func TestConfigValidation(t *testing.T) {
	h := newHarness(t)
	defer h.app.Close()

	p := &cordis.Plugin{
		Name: "strict",
		Validate: func(config any) (any, error) {
			s, ok := config.(string)
			if !ok {
				return nil, fmt.Errorf("config must be string")
			}
			return s, nil
		},
		Apply: func(*cordis.Context, any) error { return nil },
	}
	var f *cordis.Fiber
	h.run(func(ctx *cordis.Context) {
		f, _ = ctx.Plugin(p, 42)
	})
	if f.State() != cordis.StateFailed {
		t.Fatalf("invalid config should fail the fiber, got %s", f.State())
	}
	h.run(func(ctx *cordis.Context) {
		if err := f.Update("ok"); err != nil {
			t.Fatal(err)
		}
	})
	if f.State() != cordis.StateActive {
		t.Fatalf("valid config should activate, got %s", f.State())
	}
}

func TestServiceEvents(t *testing.T) {
	h := newHarness(t)
	defer h.app.Close()

	var seen []any
	h.run(func(ctx *cordis.Context) {
		ctx.On("internal/service", func(c *cordis.Context, args ...any) any {
			seen = append(seen, args...)
			return nil
		})
	})
	var dispose cordis.Dispose
	h.run(func(ctx *cordis.Context) {
		d, err := ctx.Provide("foo", "value", nil)
		if err != nil {
			t.Fatal(err)
		}
		dispose = d
	})
	h.run(func(ctx *cordis.Context) {
		dispose()
	})
	if len(seen) != 4 { // (name, value) × (provide, dispose)
		t.Fatalf("expected 2 service events, got %v", seen)
	}
	if seen[0] != "foo" || seen[1] != "value" {
		t.Fatalf("bad provide event: %v", seen)
	}
	if seen[2] != "foo" || seen[3] != nil {
		t.Fatalf("bad dispose event: %v", seen)
	}
}

func TestDuplicateProvide(t *testing.T) {
	h := newHarness(t)
	defer h.app.Close()

	var err error
	h.run(func(ctx *cordis.Context) {
		_, err = ctx.Provide("foo", 1, nil)
		if err != nil {
			t.Fatal(err)
		}
		_, err = ctx.Provide("foo", 2, nil)
	})
	if err == nil {
		t.Fatal("expected duplicate error")
	}
}

func TestEpochChase(t *testing.T) {
	h := newHarness(t)
	defer h.app.Close()

	// 依赖者在卸载过程中目标视图再次改变：最终应收敛到最新目标。
	var events []string
	provider := &cordis.Plugin{
		Name: "db",
		Apply: func(ctx *cordis.Context, _ any) error {
			events = append(events, "db+")
			_, err := ctx.Provide("db", "v", nil)
			return err
		},
	}
	consumer := &cordis.Plugin{
		Name:   "app",
		Inject: map[string]any{"db": nil},
		Apply: func(ctx *cordis.Context, _ any) error {
			events = append(events, "app+")
			_, err := ctx.Effect("x", func() (cordis.Dispose, error) {
				return func() { events = append(events, "app-") }, nil
			})
			return err
		},
	}
	var db *cordis.Fiber
	h.run(func(ctx *cordis.Context) {
		db, _ = ctx.Plugin(provider, nil)
		ctx.Plugin(consumer, nil)
	})
	h.run(func(ctx *cordis.Context) {
		// 同步段内先注销旧提供者再注册新提供者：
		// 消费者的目标视图经历 ":1" → inactive → ":2"，
		// pump 执行时直接看到最终目标，一次收敛。
		db.Dispose()
		db2, _ := ctx.Plugin(provider, nil)
		db = db2
	})
	// 最终：新提供者 active，消费者 active（无论中间路径）。
	if db.State() != cordis.StateActive {
		t.Fatalf("provider should be active: %s", db.State())
	}
	found := false
	for _, e := range events {
		if e == "app+" {
			found = true
		}
	}
	if !found {
		t.Fatalf("consumer should have reactivated: %v", events)
	}
}

func TestEventListenerCleanup(t *testing.T) {
	h := newHarness(t)
	defer h.app.Close()

	var ticks int
	p := &cordis.Plugin{
		Name: "ticker",
		Apply: func(ctx *cordis.Context, _ any) error {
			_, err := ctx.On("tick", func(*cordis.Context, ...any) any {
				ticks++
				return nil
			})
			return err
		},
	}
	var f *cordis.Fiber
	// 注册与 Emit 必须分批：Plugin 只入队 pump 任务，
	// Apply（含监听器注册）要到本同步段结束后才执行。
	h.run(func(ctx *cordis.Context) {
		f, _ = ctx.Plugin(p, nil)
	})
	h.run(func(ctx *cordis.Context) {
		ctx.Emit("tick")
	})
	if ticks != 1 {
		t.Fatalf("listener should fire, ticks=%d", ticks)
	}
	// Dispose 同理：注销也是入队任务，效果回收在下一段完成。
	h.run(func(ctx *cordis.Context) {
		f.Dispose()
	})
	h.run(func(ctx *cordis.Context) {
		ctx.Emit("tick")
	})
	if ticks != 1 {
		t.Fatalf("listener must be auto-removed with fiber, ticks=%d", ticks)
	}
}

func TestRootCloseCascades(t *testing.T) {
	h := newHarness(t)
	defer h.app.Close()

	var log []string
	provider := &cordis.Plugin{
		Name: "db",
		Apply: func(ctx *cordis.Context, _ any) error {
			_, err := ctx.Provide("db", 1, nil)
			return err
		},
	}
	consumer := &cordis.Plugin{
		Name:   "app",
		Inject: map[string]any{"db": nil},
		Apply: func(ctx *cordis.Context, _ any) error {
			_, err := ctx.Effect("x", func() (cordis.Dispose, error) {
				return func() { log = append(log, "app-") }, nil
			})
			return err
		},
	}
	h.run(func(ctx *cordis.Context) {
		db, _ := ctx.Plugin(provider, nil)
		_ = db
		ctx.Plugin(consumer, nil)
	})
	h.app.Close()
	if fmt.Sprint(log) != fmt.Sprint([]string{"app-"}) {
		t.Fatalf("close should cascade disposal: %v", log)
	}
}

func TestCheckFunction(t *testing.T) {
	h := newHarness(t)
	defer h.app.Close()

	// 提供者以 check 声明健康度：check 失败时服务虽已注册，
	// 依赖仍视为未满足（checkImpl 在 notify 时重新评估）。
	healthy := false
	provider := &cordis.Plugin{
		Name: "db",
		Apply: func(ctx *cordis.Context, _ any) error {
			_, err := ctx.Provide("db", "conn", func() bool { return healthy })
			return err
		},
	}
	var activated int
	consumer := &cordis.Plugin{
		Inject: map[string]any{"db": nil},
		Apply:  func(*cordis.Context, any) error { activated++; return nil },
	}
	var db *cordis.Fiber
	h.run(func(ctx *cordis.Context) {
		db, _ = ctx.Plugin(provider, nil)
		ctx.Plugin(consumer, nil)
	})
	if activated != 0 {
		t.Fatalf("check=false must block activation: %d", activated)
	}

	// 健康度翻转 + 提供者热重载（notify 触发依赖重新评估）→ 依赖者激活。
	healthy = true
	h.run(func(ctx *cordis.Context) {
		if err := db.Update(nil); err != nil {
			t.Fatal(err)
		}
	})
	if activated != 1 {
		t.Fatalf("check=true should activate dependant: %d", activated)
	}
}

// 回归：调度队列必须无界。单个调度任务内部触发的投递
// （每次 fiber 注册排入一次 pump）超过旧实现的固定容量
// 1024 时，唯一消费者正忙于执行当前任务，投递方会永久
// 阻塞并死锁。此处在单个任务内注册 1100 个 fiber 复现。
func TestSchedulerUnboundedQueue(t *testing.T) {
	h := newHarness(t)
	defer h.app.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		h.app.DoSync(func(ctx *cordis.Context) {
			for i := 0; i < 1100; i++ {
				p := &cordis.Plugin{
					Name:  fmt.Sprintf("p%d", i),
					Apply: func(*cordis.Context, any) error { return nil },
				}
				if _, err := ctx.Plugin(p, nil); err != nil {
					t.Errorf("plugin %d: %v", i, err)
					return
				}
			}
		})
		h.app.Wait()
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("deadlock: >1024 posts from a single scheduler task")
	}
}
