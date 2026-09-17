package cordis

import "sort"

// disposeStep 一步撤销操作。run 执行撤销本体；wait 非 nil 时，
// run 返回后需等待其回调 then 才能继续后续步骤——用于实现
// 「服务提供者先等全部依赖者下线，再销毁自身服务」的撤销顺序保证。
type disposeStep struct {
	run  func()
	wait func(then func())
}

func (s disposeStep) invoke(then func()) {
	if s.run != nil {
		s.run()
	}
	if s.wait == nil {
		then()
		return
	}
	s.wait(then)
}

// disposableList 保序的可撤销列表，clear 时按 LIFO 逆序返回。
// 对应官方实现的 DisposableList：push 返回移除函数，
// delete 可中途移除单个条目，clear 原子地取出全部并清空。
type disposableList struct {
	next  int
	order []int
	steps map[int]disposeStep
}

func newDisposableList() *disposableList {
	return &disposableList{steps: make(map[int]disposeStep)}
}

func (l *disposableList) push(step disposeStep) func() {
	id := l.next
	l.next++
	l.order = append(l.order, id)
	l.steps[id] = step
	return func() { l.delete(id) }
}

func (l *disposableList) delete(id int) {
	delete(l.steps, id)
	// 墓碑过多时压缩 order，摊还 O(1)。
	if len(l.order) > 8 && len(l.order) > 2*len(l.steps) {
		l.order = l.order[:0]
		for id, step := range l.steps {
			_ = step
			l.order = append(l.order, id)
		}
		// 重建插入序：id 单调递增，排序即可。
		sort.Ints(l.order)
	}
}

// clear 原子取出全部步骤，按 LIFO 逆序返回。
func (l *disposableList) clear() []disposeStep {
	steps := make([]disposeStep, 0, len(l.steps))
	for i := len(l.order) - 1; i >= 0; i-- {
		if step, ok := l.steps[l.order[i]]; ok {
			steps = append(steps, step)
		}
	}
	l.order = l.order[:0]
	clear(l.steps)
	return steps
}

func (l *disposableList) len() int { return len(l.steps) }
