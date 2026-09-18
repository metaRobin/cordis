package cordis

import "testing"

// 倒排索引的生命周期不变量（M-3 修复引入的内部状态）：
// track 与 untrack 必须严格配对——实例注销、未注册失败路径与
// Close 级联回收之后，索引都必须回到空，否则服务通知会随
// 运行时长逐步退化，并可能触达已注销的 fiber。
func TestReflectIndexLifecycle(t *testing.T) {
	app := New()
	root := app.Root()

	consumer := &Plugin{
		Name:   "consumer",
		Inject: map[string]any{"db": nil},
		Apply:  func(*Context, any) error { return nil },
	}
	var f *Fiber
	app.DoSync(func(ctx *Context) {
		f, _ = ctx.Plugin(consumer, nil)
	})
	app.Wait()
	if n := len(root.reflect.index["db"]); n != 1 {
		t.Fatalf("index should hold the consumer: %d", n)
	}

	// 注销 → 索引回退。
	app.DoSync(func(*Context) { f.Dispose() })
	app.Wait()
	if n := len(root.reflect.index); n != 0 {
		t.Fatalf("index must be empty after dispose: %v", root.reflect.index)
	}

	// 未声明依赖的 fiber 不进入索引。
	plain := &Plugin{Name: "plain", Apply: func(*Context, any) error { return nil }}
	app.DoSync(func(ctx *Context) {
		if _, err := ctx.Plugin(plain, nil); err != nil {
			t.Errorf("plugin: %v", err)
		}
	})
	app.Wait()
	if n := len(root.reflect.index); n != 0 {
		t.Fatalf("plugin without deps must not be indexed: %v", root.reflect.index)
	}

	// Close 级联回收后索引同样清空。
	app.DoSync(func(ctx *Context) {
		if _, err := ctx.Plugin(consumer, nil); err != nil {
			t.Errorf("plugin: %v", err)
		}
	})
	app.Wait()
	if n := len(root.reflect.index["db"]); n != 1 {
		t.Fatalf("index should hold the consumer again: %d", n)
	}
	app.Close()
	if n := len(root.reflect.index); n != 0 {
		t.Fatalf("index must be empty after close: %v", root.reflect.index)
	}
}
