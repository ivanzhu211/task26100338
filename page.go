package canvas

import "sync"

const (
	// MergeThreshold 是一页未合并操作的上限，达到即合并进快照。
	MergeThreshold = 5000
	// ReplayLimit 是断线重连时只补发缺口（而非重发整页）的最大缺口长度。
	ReplayLimit = 5000
	// MaxLag 是单个订阅者允许积压的推送条数，超过即判定卡住并重置。
	// 订阅者落后的条数不会超过该值，内存占用有界。
	MaxLag = 4096
)

// ResumeResult 是重连的响应：恰有一个字段非空。
type ResumeResult struct {
	Replay   []Event   // 缺口不超过 ReplayLimit 时，只补发这一段
	Snapshot *Snapshot // 缺口过大时，给新的起点画面
}

// page 是一页的全部内存状态。页内操作串行，页与页互不阻塞。
type page struct {
	mu         sync.Mutex
	seq        uint64 // 已受理的最后一个编号
	baseSeq    uint64 // 快照已合并到的编号
	strokes    []*Stroke
	alive      map[uint64]*Stroke
	log        []Event // 未合并的事件，外加最近 ReplayLimit 条已合并事件（用于补发）
	sinceMerge int
	subs       map[*Subscription]struct{}
}

func newPage() *page {
	return &page{
		alive: make(map[uint64]*Stroke),
		subs:  make(map[*Subscription]struct{}),
	}
}

// accept 受理一个操作：编号、应用、入日志、按需合并、扇出。调用方需持有 p.mu。
func (p *page) acceptLocked(op Op) Event {
	p.seq++
	ev := Event{Seq: p.seq, Op: op}
	p.applyLocked(op)
	p.log = append(p.log, ev)
	p.sinceMerge++
	if p.sinceMerge >= MergeThreshold {
		p.mergeLocked()
	}
	for sub := range p.subs {
		select {
		case sub.c <- ev:
		default:
			// 订阅者卡住：显式重置，绝不静默跳过编号。
			delete(p.subs, sub)
			sub.finish(ErrLagged)
		}
	}
	return ev
}

// applyLocked 把操作作用到当前画面。
func (p *page) applyLocked(op Op) {
	switch op.Kind {
	case OpDraw:
		s := op.Stroke
		s.Points = append([]Point(nil), s.Points...) // 冻结调用方的切片
		stroke := &s
		p.strokes = append(p.strokes, stroke)
		p.alive[s.ID] = stroke
	case OpErase:
		if _, ok := p.alive[op.StrokeID]; !ok {
			return // 擦除不存在的笔画：无效果
		}
		delete(p.alive, op.StrokeID)
		for i, s := range p.strokes {
			if s.ID == op.StrokeID {
				p.strokes = append(p.strokes[:i], p.strokes[i+1:]...)
				break
			}
		}
	case OpClear:
		target := op.ClearOf
		if target == "" {
			target = op.Author
		}
		// 当前可见的该参与者笔画即“清除之前受理”的全部笔画。
		kept := p.strokes[:0]
		for _, s := range p.strokes {
			if s.Author == target {
				delete(p.alive, s.ID)
				continue
			}
			kept = append(kept, s)
		}
		// 置空尾部，避免已清除笔画被切片 backing array 留住。
		for i := len(kept); i < len(p.strokes); i++ {
			p.strokes[i] = nil
		}
		p.strokes = kept
	}
}

// mergeLocked 把迄今的操作并入快照：推进 baseSeq 并裁剪日志。
// 画面状态随受理即时更新，合并本身只需推进边界；
// 已合并事件中最近 ReplayLimit 条保留在日志里，供断线补发。
func (p *page) mergeLocked() {
	p.baseSeq = p.seq
	p.sinceMerge = 0
	cut := p.seq - ReplayLimit
	i := 0
	for i < len(p.log) && p.log[i].Seq <= cut {
		i++
	}
	if i > 0 {
		p.log = append([]Event(nil), p.log[i:]...)
	}
}

// snapshotLocked 生成当前画面的起点快照（拷贝切片头，笔画本身不可变）。
func (p *page) snapshotLocked() Snapshot {
	return Snapshot{
		BaseSeq: p.seq,
		Strokes: append([]*Stroke(nil), p.strokes...),
	}
}

// subscribeLocked 注册订阅；此后的事件经推送通道送达。调用方需持有 p.mu。
func (p *page) subscribeLocked() *Subscription {
	sub := &Subscription{c: make(chan Event, MaxLag)}
	sub.C = sub.c
	sub.closeFn = p.unsubscribe
	p.subs[sub] = struct{}{}
	return sub
}

// enter 进入页面：起点画面 + 订阅，与后续推送原子衔接。
func (p *page) enter() (Snapshot, *Subscription) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.snapshotLocked(), p.subscribeLocked()
}

// resume 断线重连：缺口可补则补发缺口，否则给新的起点画面。
func (p *page) resume(lastSeq uint64) (ResumeResult, *Subscription) {
	p.mu.Lock()
	defer p.mu.Unlock()
	sub := p.subscribeLocked()
	if lastSeq <= p.seq && p.seq-lastSeq <= ReplayLimit && p.logCoversLocked(lastSeq) {
		from := 0
		for from < len(p.log) && p.log[from].Seq <= lastSeq {
			from++
		}
		replay := append([]Event(nil), p.log[from:]...)
		return ResumeResult{Replay: replay}, sub
	}
	snap := p.snapshotLocked()
	return ResumeResult{Snapshot: &snap}, sub
}

// logCoversLocked 报告日志是否完整覆盖 (lastSeq, p.seq] 这一段。
func (p *page) logCoversLocked(lastSeq uint64) bool {
	if lastSeq == p.seq {
		return true
	}
	return len(p.log) > 0 && p.log[0].Seq <= lastSeq+1
}

// unsubscribe 取消订阅，幂等。
func (p *page) unsubscribe(sub *Subscription) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.subs[sub]; ok {
		delete(p.subs, sub)
		sub.finish(nil)
	}
}
