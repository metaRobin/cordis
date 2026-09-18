package cordis

import (
	"fmt"
	"os"
	"sync"
)

// Logger 极简日志器。效果回收与组件执行中的错误一律被运行时吞掉并
// 记录于此（对应官方实现的行为：dispose 失败不阻断其余清理）。
type Logger struct {
	mu    sync.Mutex
	Error func(format string, args ...any)
	Warn  func(format string, args ...any)
	Info  func(format string, args ...any)
}

func newLogger() *Logger {
	l := &Logger{}
	logf := func(w *os.File, level string) func(string, ...any) {
		return func(format string, args ...any) {
			l.mu.Lock()
			defer l.mu.Unlock()
			fmt.Fprintf(w, "[cordis:"+level+"] "+format+"\n", args...)
		}
	}
	l.Error = logf(os.Stderr, "error")
	l.Warn = logf(os.Stderr, "warn")
	l.Info = logf(os.Stderr, "info")
	return l
}

func (a *App) Logger() *Logger { return a.logger }

// hook 事件监听器，随注册它的 Fiber 生命周期自动回收。
type hook struct {
	ctx      *Context
	callback func(ctx *Context, args ...any) any
	global   bool
}

// Events 事件总线。监听器通过 Fiber.Effect 注册，
// Fiber 卸载时自动注销。
//
// 单线程模型下所有分发模式均为串行执行；Parallel 与官方实现的
// 差别仅在于回调不并发，错误聚合语义保持一致。
type Events struct {
	ctx   *Context // 根上下文
	hooks map[string][]*hook
}

func newEvents(ctx *Context) *Events {
	return &Events{ctx: ctx, hooks: make(map[string][]*hook)}
}

// On 注册监听器（追加到注册序末尾），返回可手动注销的 Dispose；
// Fiber 卸载时监听器自动注销。
//
// 失活校验由 Effect 内部统一执行（assertActive），此处不重复。
func (e *Events) On(ctx *Context, name string, listener func(ctx *Context, args ...any) any) (Dispose, error) {
	h := &hook{ctx: ctx, callback: listener}
	return ctx.fiber.Effect("ctx.on("+name+")", func() (Dispose, error) {
		e.hooks[name] = append(e.hooks[name], h)
		return func() { e.unregister(name, h) }, nil
	})
}

// Once 注册一次性监听器，首次触发后自动注销。
func (e *Events) Once(ctx *Context, name string, listener func(ctx *Context, args ...any) any) (Dispose, error) {
	var outer Dispose
	inner, err := e.On(ctx, name, func(c *Context, args ...any) any {
		if outer != nil {
			outer()
		}
		return listener(c, args...)
	})
	if err != nil {
		return nil, err
	}
	outer = inner
	return inner, nil
}

func (e *Events) unregister(name string, h *hook) {
	hooks := e.hooks[name]
	for i, cur := range hooks {
		if cur == h {
			e.hooks[name] = append(hooks[:i], hooks[i+1:]...)
			if len(e.hooks[name]) == 0 {
				delete(e.hooks, name)
			}
			return
		}
	}
}

// hooksOf 取按注册序排列的监听器。filter 非 nil 时，
// 仅保留 global 或通过过滤的监听器（用于 isolate 域感知分发）。
func (e *Events) hooksOf(name string, filter func(hookCtx *Context) bool) []*hook {
	hooks := e.hooks[name]
	if filter == nil {
		return hooks
	}
	var out []*hook
	for _, h := range hooks {
		if h.global || filter(h.ctx) {
			out = append(out, h)
		}
	}
	return out
}

// Emit 同步分发事件，回调返回值与错误均被忽略（错误记日志）。
func (e *Events) Emit(ctx *Context, name string, args ...any) {
	for _, h := range e.hooksOf(name, ctx.eventFilter) {
		e.invoke(ctx, name, h, args)
	}
}

// EmitFiltered 以显式过滤规则分发事件。
func (e *Events) EmitFiltered(ctx *Context, name string, filter func(hookCtx *Context) bool, args ...any) {
	for _, h := range e.hooksOf(name, filter) {
		e.invoke(ctx, name, h, args)
	}
}

func (e *Events) invoke(ctx *Context, name string, h *hook, args []any) {
	defer func() {
		if r := recover(); r != nil {
			e.ctx.app.logger.Error("event %q listener panic: %v", name, r)
		}
	}()
	_ = h.callback(ctx, args...)
}

// Serial 串行分发；首个返回非 nil 结果的回调终止分发并返回该结果。
func (e *Events) Serial(ctx *Context, name string, args ...any) any {
	for _, h := range e.hooksOf(name, ctx.eventFilter) {
		if result, err := call(h, ctx, args); err != nil {
			e.ctx.app.logger.Error("event %q listener error: %v", name, err)
		} else if result != nil {
			return result
		}
	}
	return nil
}

// Bail 同 Serial，但同步执行且不吞 panic。
func (e *Events) Bail(ctx *Context, name string, args ...any) any {
	for _, h := range e.hooksOf(name, ctx.eventFilter) {
		if result := h.callback(ctx, args...); result != nil {
			return result
		}
	}
	return nil
}

// Parallel 分发并聚合全部错误（单线程下等价于串行）。
func (e *Events) Parallel(ctx *Context, name string, args ...any) error {
	var errs []error
	for _, h := range e.hooksOf(name, ctx.eventFilter) {
		if _, err := call(h, ctx, args); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) == 1 {
		return errs[0]
	}
	if len(errs) > 1 {
		msgs := ""
		for i, err := range errs {
			if i > 0 {
				msgs += "; "
			}
			msgs += err.Error()
		}
		return fmt.Errorf("%d errors: %s", len(errs), msgs)
	}
	return nil
}

func call(h *hook, ctx *Context, args []any) (result any, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("listener panic: %v", r)
		}
	}()
	return h.callback(ctx, args...), nil
}
