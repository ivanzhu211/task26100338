package canvassync

import (
	"errors"
	"sync"
)

// ErrInvalidOp 表示提交了非法操作。
var ErrInvalidOp = errors.New("canvassync: invalid op")

// Engine 是画布同步引擎，由上层 WebSocket 网关调用。
// 所有数据只在进程内存中，方法均可并发调用。
type Engine struct {
	mu    sync.Mutex
	pages map[string]*page
}

// NewEngine 创建一个引擎实例。
func NewEngine() *Engine {
	return &Engine{pages: make(map[string]*page)}
}

func (e *Engine) page(id string) *page {
	e.mu.Lock()
	defer e.mu.Unlock()
	p := e.pages[id]
	if p == nil {
		p = newPage()
		e.pages[id] = p
	}
	return p
}

// Enter 让客户端进入一页，返回起点画面与后续推送的订阅。
// 起点画面加上之后按序收到的推送，与从头重放该页全部操作的结果一致。
func (e *Engine) Enter(pageID, clientID string) (View, *Subscription) {
	p := e.page(pageID)
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.view(), p.subscribe(clientID)
}

// Reconnect 处理断线重连，lastSeq 是客户端收到的最后一个编号。
// 断开期间新增操作不超过 ReplayWindow 条时只补发缺的一段，
// 否则下发新的起点画面。两种情况下都会返回后续推送的订阅。
func (e *Engine) Reconnect(pageID, clientID string, lastSeq uint64) (ReconnectResult, *Subscription) {
	p := e.page(pageID)
	p.mu.Lock()
	defer p.mu.Unlock()

	sub := p.subscribe(clientID)
	if missed, ok := p.replay(lastSeq); ok {
		return ReconnectResult{Kind: ReconnectReplay, Missed: missed}, sub
	}
	return ReconnectResult{Kind: ReconnectSnapshot, View: p.view()}, sub
}

// Apply 受理一个操作，返回带编号（落笔时含分配的笔画 ID）的完整操作。
// 操作会被推送给当时在这页上的每个订阅者。
func (e *Engine) Apply(pageID, actor string, in OpInput) (Op, error) {
	switch in.Kind {
	case OpAdd:
		if len(in.Stroke.Points) == 0 {
			return Op{}, ErrInvalidOp
		}
	case OpErase, OpClear:
	default:
		return Op{}, ErrInvalidOp
	}

	p := e.page(pageID)
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.apply(actor, in), nil
}
