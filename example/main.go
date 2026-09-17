// 示例：时空可组合组件模型（论文《Spatiotemporal Composability》Go 实现）
//
// 场景：一个由声明式配置驱动的「数据库 + 缓存 + Web 服务」组合：
//
//   - 时间维：组件配置热重载，旧实例的副作用（连接、监听端口）按 LIFO 逆序回收；
//   - 空间维：依赖以协效应声明，提供者下线时依赖者自动降级，
//     隔离域让多套服务栈并存互不干扰（如多租户）。
package main

import (
	"fmt"
	"strings"

	cordis "cordis"
)

func main() {
	app := cordis.New()
	defer app.Close()

	// ---------------------------------------------------------------------------
	// 组件定义：每个副作用都返回 Dispose（显式逆操作），
	// 对外能力以服务（Provide）形式发布。
	// ---------------------------------------------------------------------------

	database := &cordis.Plugin{
		Name: "database",
		Apply: func(ctx *cordis.Context, config any) error {
			dsn := config.(string)
			fmt.Printf("[database] connect %s\n", dsn)
			_, err := ctx.Provide("database", dsn, nil)
			if err != nil {
				return err
			}
			_, err = ctx.Effect("conn", func() (cordis.Dispose, error) {
				return func() { fmt.Printf("[database] close %s\n", dsn) }, nil
			})
			return err
		},
	}

	cache := &cordis.Plugin{
		Name:   "cache",
		Inject: map[string]any{"database": nil}, // 协效应：依赖声明
		Apply: func(ctx *cordis.Context, _ any) error {
			dsn, _ := ctx.Get("database")
			fmt.Printf("[cache] warm up on %s\n", dsn)
			if _, err := ctx.Provide("cache", "cache:"+fmt.Sprint(dsn), nil); err != nil {
				return err
			}
			_, err := ctx.Effect("entries", func() (cordis.Dispose, error) {
				return func() { fmt.Println("[cache] flush entries") }, nil
			})
			return err
		},
	}

	web := &cordis.Plugin{
		Name:   "web",
		Inject: map[string]any{"database": nil, "cache": nil},
		Apply: func(ctx *cordis.Context, config any) error {
			port := config.(int)
			dsn, _ := ctx.Get("database")
			fmt.Printf("[web] listen :%d (on %s)\n", port, dsn)
			_, err := ctx.Effect("listener", func() (cordis.Dispose, error) {
				return func() { fmt.Printf("[web] shutdown :%d\n", port) }, nil
			})
			return err
		},
	}

	plugins := map[string]*cordis.Plugin{
		"database": database,
		"cache":    cache,
		"web":      web,
	}

	loader := cordis.NewLoader(app, func(name string) (*cordis.Plugin, error) {
		if p, ok := plugins[name]; ok {
			return p, nil
		}
		return nil, fmt.Errorf("unknown plugin %q", name)
	})

	// ---------------------------------------------------------------------------
	// 声明式配置：组件实例由 Entry 描述，而非过程式代码。
	// ---------------------------------------------------------------------------

	config := []cordis.EntryOptions{
		{ID: "db", Name: "database", Config: "postgres://prod"},
		{ID: "cache", Name: "cache"},
		{ID: "web", Name: "web", Config: 8080},
	}

	fmt.Println("== 初始加载 ==")
	loader.Load(config)
	printStates(loader, []string{"db", "cache", "web"})

	// ---------------------------------------------------------------------------
	// 时间维演示：配置热重载（论文 §5 的 HMR 机制）
	// web 配置变更 → 卸载旧实例（逆序回收监听器）→ 加载新实例。
	// ---------------------------------------------------------------------------

	fmt.Println("\n== 热重载：web 8080 → 9090 ==")
	config[2].Config = 9090
	loader.Load(config)
	printStates(loader, []string{"db", "cache", "web"})

	// ---------------------------------------------------------------------------
	// 空间维演示：依赖者降级与恢复（reactive coeffects）
	// 数据库下线 → cache/web 自动回收效果回到 PENDING；
	// 数据库回归 → 依赖者自动恢复。
	// ---------------------------------------------------------------------------

	fmt.Println("\n== 提供者下线：db 禁用 ==")
	config[0].Disabled = true
	loader.Load(config)
	printStates(loader, []string{"db", "cache", "web"})

	fmt.Println("\n== 提供者回归：db 启用 ==")
	config[0].Disabled = false
	loader.Load(config)
	printStates(loader, []string{"db", "cache", "web"})

	// ---------------------------------------------------------------------------
	// 空间维演示：隔离域（isolation domain）
	// 两套完整的「数据库+缓存+Web」栈以共享域标签隔离：
	// tenant-a 栈与 tenant-b 栈互不可见，同名服务多实例并存。
	// ---------------------------------------------------------------------------

	fmt.Println("\n== 隔离域：多租户双栈 ==")
	realm := func(label string) map[string]any {
		return map[string]any{"database": label, "cache": label}
	}
	loader.Load([]cordis.EntryOptions{
		{ID: "db-a", Name: "database", Config: "postgres://tenant-a", Isolate: realm("tenant-a")},
		{ID: "db-b", Name: "database", Config: "postgres://tenant-b", Isolate: realm("tenant-b")},
		{ID: "cache-a", Name: "cache", Isolate: realm("tenant-a")},
		{ID: "cache-b", Name: "cache", Isolate: realm("tenant-b")},
		{ID: "web-a", Name: "web", Config: 9001, Isolate: realm("tenant-a")},
		{ID: "web-b", Name: "web", Config: 9002, Isolate: realm("tenant-b")},
	})
	printStates(loader, []string{"db-a", "db-b", "cache-a", "cache-b", "web-a", "web-b"})

	// ---------------------------------------------------------------------------
	// 声明式操作：运行期动态调整入口树（协调与级联回收）。
	// ---------------------------------------------------------------------------

	fmt.Println("\n== 动态操作：移除 tenant-b 的数据库（其栈整体降级）==")
	loader.Remove("db-b")
	printStates(loader, []string{"db-a", "cache-a", "web-a", "cache-b", "web-b"})

	// ---------------------------------------------------------------------------
	// 关闭：沿效果链级联回收——全部组件按依赖逆序完全还原环境。
	// ---------------------------------------------------------------------------
	fmt.Println("\n== 关闭应用 ==")
}

func printStates(loader *cordis.Loader, ids []string) {
	var parts []string
	for _, id := range ids {
		e, err := loader.Tree().Resolve(id)
		if err != nil {
			parts = append(parts, fmt.Sprintf("%s=removed", id))
			continue
		}
		if e.Fiber() == nil {
			parts = append(parts, fmt.Sprintf("%s=off", id))
			continue
		}
		parts = append(parts, fmt.Sprintf("%s=%s", id, e.Fiber().State()))
	}
	fmt.Printf("   %s\n", strings.Join(parts, "  "))
}
