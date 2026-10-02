package canvassync

import "sync"

const (
	// mergeThreshold 未合并操作累计到该值时合并进快照。
	mergeThreshold = 5000
	// ReplayWindow 断线重连可补发的最大操作数；日志始终保留最近这么多条。
	ReplayWindow = 5000
	// subBuffer 单个订阅者缓冲的操作条数；落后达到该值即被明确踢出。
	// 通道实际容量为 subBuffer+1，预留一个槽位给重同步信号，
	// 保证被踢出的订阅者先收完连续前缀、再收到显式信号。
	subBuffer = 2048
)

// page 是一页画布的同步状态。所有字段仅在持有 mu 时访问。
type page struct {
	mu sync.Mutex

	seq     uint64 // 最后受理的操作编号
	baseSeq uint64 // 快照已合并到的编号

	snap  []Stroke                         // baseSeq 时刻之后仍可能可见的笔画（含已被擦除的，用 alive 过滤）
	alive map[StrokeID]Stroke              // 当前可见的全部笔画（无论是否已并入快照）
	byAut map[string]map[StrokeID]struct{} // 可见笔画按作者索引，用于清除

	log []Op // 编号连续的一段历史操作，至少保留最近 ReplayWindow 条

	nextStroke StrokeID
	subs       map[string]*Subscription
}

func newPage() *page {
	return &page{
		alive:      make(map[StrokeID]Stroke),
		byAut:      make(map[string]map[StrokeID]struct{}),
		subs:       make(map[string]*Subscription),
		nextStroke: 1,
	}
}

// apply 受理一个操作：编号、改状态、入日志、按需合并、扇出推送。
// 调用方必须持有 p.mu。
func (p *page) apply(actor string, in OpInput) Op {
	p.seq++
	op := Op{Seq: p.seq, Kind: in.Kind, Actor: actor}

	switch in.Kind {
	case OpAdd:
		st := in.Stroke
		st.ID = p.nextStroke
		p.nextStroke++
		st.Author = actor
		op.Stroke = st
		p.alive[st.ID] = st
		set := p.byAut[actor]
		if set == nil {
			set = make(map[StrokeID]struct{})
			p.byAut[actor] = set
		}
		set[st.ID] = struct{}{}

	case OpErase:
		op.Target = in.Target
		if st, ok := p.alive[in.Target]; ok {
			delete(p.alive, in.Target)
			if set := p.byAut[st.Author]; set != nil {
				delete(set, in.Target)
				if len(set) == 0 {
					delete(p.byAut, st.Author)
				}
			}
		}

	case OpClear:
		op.TargetAuthor = in.TargetAuthor
		for id := range p.byAut[in.TargetAuthor] {
			delete(p.alive, id)
		}
		delete(p.byAut, in.TargetAuthor)
	}

	p.log = append(p.log, op)

	if p.seq-p.baseSeq >= mergeThreshold {
		p.merge()
	}

	p.fanout(&op)
	return op
}

// merge 把全部未合并操作并入快照，并把日志截断到最近 ReplayWindow 条。
// 调用方必须持有 p.mu。
func (p *page) merge() {
	kept := make([]Stroke, 0, len(p.alive))
	for _, st := range p.snap {
		if _, ok := p.alive[st.ID]; ok {
			kept = append(kept, st)
		}
	}
	for _, op := range p.log {
		if op.Seq > p.baseSeq && op.Kind == OpAdd {
			if _, ok := p.alive[op.Stroke.ID]; ok {
				kept = append(kept, op.Stroke)
			}
		}
	}
	p.snap = kept
	p.baseSeq = p.seq

	// 截断日志：只保留最近 ReplayWindow 条，拷贝到新切片以释放旧数组。
	cut := 0
	for cut < len(p.log) && p.log[cut].Seq+ReplayWindow <= p.seq {
		cut++
	}
	trimmed := make([]Op, len(p.log)-cut, ReplayWindow+mergeThreshold)
	copy(trimmed, p.log[cut:])
	p.log = trimmed
}

// view 构造当前画面：快照中仍可见的笔画 + 未合并的可见落笔，自下而上排列。
// 调用方必须持有 p.mu。
func (p *page) view() View {
	strokes := make([]Stroke, 0, len(p.alive))
	for _, st := range p.snap {
		if _, ok := p.alive[st.ID]; ok {
			strokes = append(strokes, st)
		}
	}
	for _, op := range p.log {
		if op.Seq > p.baseSeq && op.Kind == OpAdd {
			if _, ok := p.alive[op.Stroke.ID]; ok {
				strokes = append(strokes, op.Stroke)
			}
		}
	}
	return View{Seq: p.seq, Strokes: strokes}
}

// replay 取出编号大于 lastSeq 的全部历史操作。
// 返回 false 表示日志已覆盖不到，需要下发新快照。
// 调用方必须持有 p.mu。
func (p *page) replay(lastSeq uint64) ([]Op, bool) {
	if lastSeq >= p.seq {
		return nil, true
	}
	if p.seq-lastSeq > ReplayWindow {
		return nil, false
	}
	start := -1
	for i := len(p.log) - 1; i >= 0; i-- {
		if p.log[i].Seq <= lastSeq {
			break
		}
		start = i
	}
	if start < 0 || p.log[start].Seq != lastSeq+1 {
		return nil, false
	}
	out := make([]Op, len(p.log)-start)
	copy(out, p.log[start:])
	return out, true
}

// subscribe 注册订阅者；同一客户端重复订阅会替换旧订阅。
// 调用方必须持有 p.mu。
func (p *page) subscribe(clientID string) *Subscription {
	if old := p.subs[clientID]; old != nil {
		old.closeLocked()
	}
	sub := &Subscription{
		ch:     make(chan Envelope, subBuffer+1),
		page:   p,
		client: clientID,
	}
	p.subs[clientID] = sub
	return sub
}

// unsubscribe 移除订阅者。调用方必须持有 p.mu。
func (p *page) unsubscribe(sub *Subscription) {
	if p.subs[sub.client] == sub {
		delete(p.subs, sub.client)
	}
}

// fanout 把操作推给所有订阅者；落后达到 subBuffer 的订阅者被明确踢出。
// 调用方必须持有 p.mu。
func (p *page) fanout(op *Op) {
	for _, sub := range p.subs {
		if sub.closed {
			continue
		}
		if len(sub.ch) >= subBuffer {
			p.evictLocked(sub)
			continue
		}
		sub.ch <- Envelope{Op: op}
	}
}

// evictLocked 踢出落后太多的订阅者：保留已缓冲的连续前缀，
// 在预留槽位放入显式的重同步信号后关闭通道，绝不悄悄跳过一段。
// 调用方必须持有 p.mu。
func (p *page) evictLocked(sub *Subscription) {
	sub.ch <- Envelope{ResyncRequired: true}
	sub.closed = true
	sub.evicted = true
	close(sub.ch)
	delete(p.subs, sub.client)
}
