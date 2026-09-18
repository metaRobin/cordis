// Package cordis 是论文《Spatiotemporal Composability》所提出的
// 时空可组合组件模型的 Go 实现。
//
// 论文的核心主张是：组件系统应同时具备两个维度的可组合性——
//
//   - 时间维（temporal）：每个副作用都携带显式的逆操作，由运行时追踪，
//     组件卸载时环境能被完全还原（revertible effects）；
//   - 空间维（spatial）：组件以协效应（coeffect）形式声明对服务的依赖，
//     依赖的满足状态变化时，运行时自动驱动组件的加载与卸载
//     （reactive coeffects）。
//
// 本实现与官方 TypeScript 实现的概念映射：
//
//	Fiber        组件的运行时实例，持有效果追踪表与生命周期状态机
//	epoch        目标视图标识：由当前依赖集合推导，驱动 reload/unload 惯性链
//	Reflect      协效应存储：isolate 域键 → 服务实现
//	Registry     插件注册表：相同 Plugin 的多次实例化共享 Runtime
//	Events       事件总线，监听器随注册它的 Fiber 生命周期自动回收
//	Loader       声明式配置层：Entry/EntryGroup/EntryTree 及协调（reconciliation）
//
// 并发模型：与 JavaScript 单线程事件循环对齐，App 内置一个调度器
// goroutine。所有状态转换任务在其中串行执行；用户回调（Apply、Dispose、
// 事件监听器）天然运行于调度器内，可直接调用 Context 上的任何 API 而
// 无需加锁。外部 goroutine 通过 App.Do 进入。
package cordis

import "errors"

// Dispose 撤销一个副作用。实现必须在语义上完全逆转其创建的操作。
// Dispose 必须幂等：重复调用不产生额外效果。
type Dispose func()

// FiberState 组件生命周期状态，与论文 §4 的生命周期规则对应。
type FiberState int

const (
	// StatePending 已注册但依赖未满足，等待激活。
	StatePending FiberState = iota
	// StateLoading 依赖已满足，正在执行组件逻辑。
	StateLoading
	// StateActive 组件逻辑执行成功且全部依赖仍然满足。
	StateActive
	// StateFailed 组件执行失败——首次加载、重载或 Update 中出错，
	// 或配置未通过 Validate；产生的效果已被回收，等待下次
	// Update 清除错误后重新启动。
	StateFailed
	// StateDisposed 已从父上下文注销，生命周期终结。
	StateDisposed
	// StateUnloading 正在按 LIFO 逆序回收效果。
	StateUnloading
)

func (s FiberState) String() string {
	switch s {
	case StatePending:
		return "pending"
	case StateLoading:
		return "loading"
	case StateActive:
		return "active"
	case StateFailed:
		return "failed"
	case StateDisposed:
		return "disposed"
	case StateUnloading:
		return "unloading"
	}
	return "unknown"
}

var (
	// ErrInactiveEffect 在已失活的上下文上创建效果时返回，
	// 对应官方实现的 CordisError('INACTIVE_EFFECT')。
	ErrInactiveEffect = errors.New("cordis: cannot create effect on inactive context")
	// ErrServiceDuplicate 同一隔离域内重复注册同名服务。
	ErrServiceDuplicate = errors.New("cordis: duplicate service registration")
	// ErrServiceNotFound 请求的服务在当前隔离域中不可见。
	ErrServiceNotFound = errors.New("cordis: service not found")
	// ErrInvalidPlugin Plugin.Apply 为 nil。
	ErrInvalidPlugin = errors.New("cordis: invalid plugin, apply must not be nil")
)

// Plugin 描述一个可组合组件（论文中的 component definition）。
//
// Inject 声明协效应：键为服务名，值为传给该服务提供者的拦截配置
// （nil 表示必选依赖、无附加配置）。声明后本插件的激活与否交由
// 运行时根据依赖满足状态自动驱动。
type Plugin struct {
	Name     string
	Inject   map[string]any
	Validate func(config any) (any, error)
	Apply    func(ctx *Context, config any) error
}
