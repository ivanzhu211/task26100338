package canvassync

import (
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"
)

// goldenLog 记录测试写入端受理的全部操作，用于从头重放得到参考画面。
var authors = []string{"alice", "bob", "carol", "dave"}

type goldenLog struct {
	mu   sync.Mutex
	byPg map[string][]Op
}

func (g *goldenLog) record(page string, op Op) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.byPg[page] = append(g.byPg[page], op)
}

// view 从头重放全部操作得到画面（笔画 ID 序列，自下而上）。
func (g *goldenLog) view(page string) []StrokeID {
	g.mu.Lock()
	defer g.mu.Unlock()
	ops := g.byPg[page]
	sort.Slice(ops, func(i, j int) bool { return ops[i].Seq < ops[j].Seq })
	alive := map[StrokeID]bool{}
	var order []StrokeID
	authorOf := map[StrokeID]string{}
	for _, op := range ops {
		switch op.Kind {
		case OpAdd:
			alive[op.Stroke.ID] = true
			authorOf[op.Stroke.ID] = op.Stroke.Author
			order = append(order, op.Stroke.ID)
		case OpErase:
			delete(alive, op.Target)
		case OpClear:
			for id, a := range authorOf {
				if a == op.TargetAuthor {
					delete(alive, id)
				}
			}
		}
	}
	var out []StrokeID
	for _, id := range order {
		if alive[id] {
			out = append(out, id)
		}
	}
	return out
}

func (g *goldenLog) finalSeq(page string) uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	var max uint64
	for _, op := range g.byPg[page] {
		if op.Seq > max {
			max = op.Seq
		}
	}
	return max
}

// clientModel 是某个客户端在某页上手里画面的模拟。
type clientModel struct {
	order []StrokeID
	alive map[StrokeID]bool
	auth  map[StrokeID]string
}

func newClientModel() *clientModel {
	return &clientModel{alive: map[StrokeID]bool{}, auth: map[StrokeID]string{}}
}

func (m *clientModel) reset(v View) {
	m.order, m.alive, m.auth = nil, map[StrokeID]bool{}, map[StrokeID]string{}
	for _, st := range v.Strokes {
		m.order = append(m.order, st.ID)
		m.alive[st.ID] = true
		m.auth[st.ID] = st.Author
	}
}

func (m *clientModel) apply(op *Op) {
	switch op.Kind {
	case OpAdd:
		m.order = append(m.order, op.Stroke.ID)
		m.alive[op.Stroke.ID] = true
		m.auth[op.Stroke.ID] = op.Stroke.Author
	case OpErase:
		delete(m.alive, op.Target)
	case OpClear:
		for id, a := range m.auth {
			if a == op.TargetAuthor {
				delete(m.alive, id)
			}
		}
	}
}

func (m *clientModel) ids() []StrokeID {
	var out []StrokeID
	for _, id := range m.order {
		if m.alive[id] {
			out = append(out, id)
		}
	}
	return out
}

// TestChaos 是主验收场景：多人在多页上同时操作，合并多次发生，
// 期间不断有人进入、重连、长时不取推送；最终每人画面与从头重放一致。
func TestChaos(t *testing.T) {
	const (
		nPages   = 3
		nWriters = 6
		nClients = 12
		opsPerPg = 2*mergeThreshold + 2500 // 每页远超 5000，合并发生多次
		totalOps = nPages * opsPerPg
	)
	e := NewEngine()
	gold := &goldenLog{byPg: map[string][]Op{}}
	pages := []string{"alpha", "beta", "gamma"}

	// 记录每页已受理的笔画 ID，供擦除挑选目标。
	var idMu sync.Mutex
	strokeIDs := map[string][]StrokeID{}

	var writeWG sync.WaitGroup
	stop := make(chan struct{})

	// 写入端：多 goroutine 并发落笔/擦除/清除。
	perWriter := totalOps / nWriters
	for w := 0; w < nWriters; w++ {
		writeWG.Add(1)
		go func(seed int64) {
			defer writeWG.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < perWriter; i++ {
				pg := pages[rng.Intn(nPages)]
				actor := authors[rng.Intn(len(authors))]
				var in OpInput
				switch roll := rng.Intn(100); {
				case roll < 80:
					in = addOp(actor, 1+rng.Intn(5))
				case roll < 90:
					idMu.Lock()
					ids := strokeIDs[pg]
					var tgt StrokeID
					if len(ids) > 0 {
						tgt = ids[rng.Intn(len(ids))]
					}
					idMu.Unlock()
					in = OpInput{Kind: OpErase, Target: tgt}
				default:
					in = OpInput{Kind: OpClear, TargetAuthor: authors[rng.Intn(len(authors))]}
				}
				op, err := e.Apply(pg, actor, in)
				if err != nil {
					t.Errorf("apply: %v", err)
					return
				}
				if in.Kind == OpAdd {
					idMu.Lock()
					strokeIDs[pg] = append(strokeIDs[pg], op.Stroke.ID)
					idMu.Unlock()
				}
				gold.record(pg, op)
			}
		}(int64(w) + 1)
	}

	// 客户端：随机进入、读推送、断线重连；一个客户端永远卡住不读。
	var clientWG sync.WaitGroup
	type clientState struct {
		id     string
		models map[string]*clientModel
		next   map[string]uint64
		subs   map[string]*Subscription
	}
	states := make([]*clientState, nClients)
	for c := 0; c < nClients; c++ {
		id := authors[c%len(authors)] + "-cli"
		if c >= len(authors) {
			id = authors[c%len(authors)] + "-cli" + string(rune('0'+c))
		}
		states[c] = &clientState{
			id:     id,
			models: map[string]*clientModel{},
			next:   map[string]uint64{},
			subs:   map[string]*Subscription{},
		}
	}
	stuck := states[0] // 这个客户端进入后从不读取

	enter := func(st *clientState, pg string) {
		v, sub := e.Enter(pg, st.id)
		st.models[pg] = newClientModel()
		st.models[pg].reset(v)
		st.next[pg] = v.Seq + 1
		st.subs[pg] = sub
	}
	reconnect := func(st *clientState, pg string) {
		last := st.next[pg] - 1
		res, sub := e.Reconnect(pg, st.id, last)
		st.subs[pg] = sub
		switch res.Kind {
		case ReconnectReplay:
			for i := range res.Missed {
				op := &res.Missed[i]
				if op.Seq != st.next[pg] {
					t.Errorf("%s/%s: missed seq %d, want %d", st.id, pg, op.Seq, st.next[pg])
					return
				}
				st.models[pg].apply(op)
				st.next[pg]++
			}
		case ReconnectSnapshot:
			st.models[pg].reset(res.View)
			st.next[pg] = res.View.Seq + 1
		}
	}
	drain := func(st *clientState, pg string, block bool) {
		sub := st.subs[pg]
		if sub == nil {
			return
		}
		for {
			var env Envelope
			var ok bool
			if block {
				env, ok = <-sub.C()
			} else {
				select {
				case env, ok = <-sub.C():
				default:
					return
				}
			}
			if !ok {
				if sub.ResyncRequired() {
					enter(st, pg) // 被明确踢出后重新进入
				}
				return
			}
			if env.ResyncRequired {
				enter(st, pg)
				return
			}
			if env.Op.Seq != st.next[pg] {
				t.Errorf("%s/%s: pushed seq %d, want %d", st.id, pg, env.Op.Seq, st.next[pg])
				return
			}
			st.models[pg].apply(env.Op)
			st.next[pg]++
		}
	}

	for c, st := range states {
		clientWG.Add(1)
		go func(idx int, st *clientState) {
			defer clientWG.Done()
			rng := rand.New(rand.NewSource(int64(idx) + 100))
			for _, pg := range pages {
				enter(st, pg)
			}
			if st == stuck {
				<-stop // 永远不读推送，直到测试结束
				return
			}
			for {
				select {
				case <-stop:
					return
				default:
				}
				pg := pages[rng.Intn(nPages)]
				switch rng.Intn(10) {
				case 0:
					reconnect(st, pg)
				case 1:
					enter(st, pg)
				case 2:
					time.Sleep(time.Duration(rng.Intn(30)) * time.Millisecond) // 长时不取推送
				}
				drain(st, pg, false)
			}
		}(c, st)
	}

	writeWG.Wait()
	close(stop)
	clientWG.Wait()

	// 收尾：每个客户端追平到最终编号，再与从头重放比对。
	for _, st := range states {
		if st == stuck {
			continue
		}
		for _, pg := range pages {
			final := gold.finalSeq(pg)
			for st.next[pg]-1 < final {
				reconnect(st, pg)
			}
			want := gold.view(pg)
			got := st.models[pg].ids()
			if len(got) != len(want) {
				t.Fatalf("%s/%s: %d strokes, want %d", st.id, pg, len(got), len(want))
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("%s/%s: stroke %d = %d, want %d", st.id, pg, i, got[i], want[i])
				}
			}
		}
	}
}

// TestApplyLatencyWithStuckConsumer 验收：有人卡住不取推送时，
// 其他人的落笔受理耗时 p99 仍不超过 10ms（合并期间也一样）。
func TestApplyLatencyWithStuckConsumer(t *testing.T) {
	e := NewEngine()
	// 一个卡住的订阅者 + 二十个正常读者。
	if _, sub := e.Enter("p", "stuck"); sub != nil {
		defer sub.Close()
	}
	done := make(chan struct{})
	var readerWG sync.WaitGroup
	for i := 0; i < 20; i++ {
		_, sub := e.Enter("p", authors[i%len(authors)]+"-r")
		readerWG.Add(1)
		go func(s *Subscription) {
			defer readerWG.Done()
			for {
				select {
				case <-done:
					return
				case _, ok := <-s.C():
					if !ok {
						return
					}
				}
			}
		}(sub)
	}

	const n = 4 * mergeThreshold // 触发多次合并
	durs := make([]time.Duration, 0, n)
	for i := 0; i < n; i++ {
		start := time.Now()
		if _, err := e.Apply("p", "bob", addOp("bob", 8)); err != nil {
			t.Fatal(err)
		}
		durs = append(durs, time.Since(start))
	}
	close(done)
	readerWG.Wait()

	sort.Slice(durs, func(i, j int) bool { return durs[i] < durs[j] })
	p99 := durs[len(durs)*99/100]
	t.Logf("apply p50=%v p99=%v max=%v", durs[len(durs)/2], p99, durs[len(durs)-1])
	if p99 > 10*time.Millisecond {
		t.Fatalf("apply p99 = %v, exceeds 10ms", p99)
	}
}

// TestEnterCostIndependentOfHistory 验收：进入与重连的耗时不随累计操作数变长。
// 页面上累计 10 万条操作（可见笔画通过定期清除保持在约一千笔），
// 结束时进入与重连的耗时应与开局时同一量级。
func TestEnterCostIndependentOfHistory(t *testing.T) {
	e := NewEngine()
	const total = 100000

	measure := func() (enter, recon time.Duration) {
		start := time.Now()
		v, sub := e.Enter("p", "probe")
		enter = time.Since(start)
		sub.Close()
		start = time.Now()
		_, sub2 := e.Reconnect("p", "probe", v.Seq)
		recon = time.Since(start)
		sub2.Close()
		return
	}

	enter0, recon0 := measure()
	for i := 0; i < total; i++ {
		if _, err := e.Apply("p", "bob", addOp("bob", 4)); err != nil {
			t.Fatal(err)
		}
		if i%1000 == 999 { // 定期清除，保持可见笔画约一千笔
			if _, err := e.Apply("p", "x", OpInput{Kind: OpClear, TargetAuthor: "bob"}); err != nil {
				t.Fatal(err)
			}
		}
	}
	enter1, recon1 := measure()
	t.Logf("at 0 ops: enter=%v reconnect=%v; at 100k ops: enter=%v reconnect=%v",
		enter0, recon0, enter1, recon1)
	if enter1 > 20*time.Millisecond {
		t.Fatalf("enter after 100k ops took %v", enter1)
	}
	if recon1 > 20*time.Millisecond {
		t.Fatalf("reconnect after 100k ops took %v", recon1)
	}
}
