package canvas

import "sync"

// Engine 管理所有页面的画布状态，由上层 WebSocket 网关调用。
// 所有方法并发安全；不同页面之间互不阻塞。
type Engine struct {
	mu    sync.Mutex
	pages map[string]*page
}

// NewEngine 创建一个空引擎。
func NewEngine() *Engine { return &Engine{pages: make(map[string]*page)} }

func (e *Engine) pageFor(id string) *page {
	e.mu.Lock()
	defer e.mu.Unlock()
	p, ok := e.pages[id]
	if !ok {
		p = newPage()
		e.pages[id] = p
	}
	return p
}

// Accept 受理一个操作：按受理顺序编号、应用到画面、推送给本页所有订阅者。
// 返回编号后的事件。擦除不存在的笔画也会被受理编号，只是对画面无效果。
func (e *Engine) Accept(pageID string, op Op) Event {
	p := e.pageFor(pageID)
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.acceptLocked(op)
}

// Enter 进入页面：返回起点画面（及其编号）与订阅，
// 之后从订阅通道接着收推送，与起点画面原子衔接。
func (e *Engine) Enter(pageID, participant string) (Snapshot, *Subscription) {
	return e.pageFor(pageID).enter()
}

// Resume 断线重连：lastSeq 是客户端收到的最后一个编号。
// 断开期间新增操作不超过 ReplayLimit 条时只补发缺口（ResumeResult.Replay），
// 否则给新的起点画面（ResumeResult.Snapshot）；两种情况都附带新订阅。
func (e *Engine) Resume(pageID, participant string, lastSeq uint64) (ResumeResult, *Subscription) {
	return e.pageFor(pageID).resume(lastSeq)
}

// Leave 退出页面，停止推送。
func (e *Engine) Leave(pageID string, sub *Subscription) {
	if sub == nil {
		return
	}
	sub.Close()
}
