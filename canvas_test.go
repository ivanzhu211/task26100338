package canvas

import "testing"

func drawOp(author string, id uint64) Op {
	return Op{Kind: OpDraw, Author: author, Stroke: Stroke{
		ID: id, Author: author, Color: 0xFF0000, Alpha: 0.5, Width: 2,
		Points: []Point{{1, 2}, {3, 4}},
	}}
}

// 收集订阅通道中当前可得的全部事件（非阻塞）。
func drain(sub *Subscription) []Event {
	var out []Event
	for {
		select {
		case ev, ok := <-sub.C:
			if !ok {
				return out
			}
			out = append(out, ev)
		default:
			return out
		}
	}
}

func applyAll(strokes []*Stroke, evs []Event) []*Stroke {
	p := newPage()
	p.strokes = append([]*Stroke(nil), strokes...)
	for _, s := range p.strokes {
		p.alive[s.ID] = s
	}
	for _, ev := range evs {
		p.applyLocked(ev.Op)
	}
	return p.strokes
}

func strokeIDs(strokes []*Stroke) []uint64 {
	ids := make([]uint64, len(strokes))
	for i, s := range strokes {
		ids[i] = s.ID
	}
	return ids
}

func equalIDs(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestDrawEraseClear(t *testing.T) {
	e := NewEngine()
	e.Accept("p1", drawOp("alice", 1))
	e.Accept("p1", drawOp("bob", 2))
	e.Accept("p1", drawOp("alice", 3))
	e.Accept("p1", Op{Kind: OpErase, Author: "bob", StrokeID: 1})
	e.Accept("p1", Op{Kind: OpErase, Author: "bob", StrokeID: 99}) // 不存在：无效果
	e.Accept("p1", Op{Kind: OpClear, Author: "alice"})

	snap, _ := e.Enter("p1", "carol")
	if snap.BaseSeq != 6 {
		t.Fatalf("BaseSeq = %d, want 6", snap.BaseSeq)
	}
	if got := strokeIDs(snap.Strokes); !equalIDs(got, []uint64{2}) {
		t.Fatalf("strokes = %v, want [2]", got)
	}
}

func TestClearOnlyAffectsEarlierStrokes(t *testing.T) {
	e := NewEngine()
	e.Accept("p", drawOp("alice", 1))
	e.Accept("p", Op{Kind: OpClear, Author: "alice"})
	e.Accept("p", drawOp("alice", 2)) // 清除之后受理的笔画不受影响
	snap, _ := e.Enter("p", "bob")
	if got := strokeIDs(snap.Strokes); !equalIDs(got, []uint64{2}) {
		t.Fatalf("strokes = %v, want [2]", got)
	}
}

func TestEnterSnapshotThenEventsConsistent(t *testing.T) {
	e := NewEngine()
	var all []Event
	for i := uint64(1); i <= 100; i++ {
		all = append(all, e.Accept("p", drawOp("a", i)))
	}
	snap, sub := e.Enter("p", "b")
	for i := uint64(101); i <= 200; i++ {
		all = append(all, e.Accept("p", drawOp("a", i)))
	}
	evs := drain(sub)
	// 起点画面 + 后续推送 == 从头重放全部操作
	got := applyAll(snap.Strokes, evs)
	want := applyAll(nil, all)
	if !equalIDs(strokeIDs(got), strokeIDs(want)) {
		t.Fatal("snapshot + events != full replay")
	}
	// 编号连续、不重不漏
	for i, ev := range evs {
		if ev.Seq != snap.BaseSeq+1+uint64(i) {
			t.Fatalf("event %d seq = %d, want %d", i, ev.Seq, snap.BaseSeq+1+uint64(i))
		}
	}
}

func TestStrokeDataPreserved(t *testing.T) {
	e := NewEngine()
	in := Stroke{ID: 7, Author: "a", Color: 0x00FF00, Alpha: 0.25, Width: 3.5,
		Points: []Point{{1, 1}, {2, 2}, {3, 3}}}
	e.Accept("p", Op{Kind: OpDraw, Author: "a", Stroke: in})
	in.Points[0] = Point{99, 99} // 受理后修改调用方切片不应影响已受理数据
	snap, _ := e.Enter("p", "b")
	s := snap.Strokes[0]
	if s.Color != 0x00FF00 || s.Alpha != 0.25 || s.Width != 3.5 || s.Author != "a" {
		t.Fatalf("stroke attrs not preserved: %+v", s)
	}
	if s.Points[0] != (Point{1, 1}) {
		t.Fatalf("points not frozen: %v", s.Points[0])
	}
}

func TestLaggedSubscriberReset(t *testing.T) {
	e := NewEngine()
	_, sub := e.Enter("p", "slow") // 从不取推送
	for i := uint64(1); i <= MaxLag+10; i++ {
		e.Accept("p", drawOp("a", i))
	}
	// 通道应已关闭且 Err 为 ErrLagged
	for range sub.C {
	}
	if sub.Err() != ErrLagged {
		t.Fatalf("Err = %v, want ErrLagged", sub.Err())
	}
	// 其他订阅者不受影响
	_, sub2 := e.Enter("p", "fast")
	e.Accept("p", drawOp("a", MaxLag+11))
	evs := drain(sub2)
	if len(evs) != 1 || evs[0].Seq != MaxLag+11 {
		t.Fatalf("fast subscriber got %v", evs)
	}
}

func TestLeaveStopsDelivery(t *testing.T) {
	e := NewEngine()
	_, sub := e.Enter("p", "a")
	e.Leave("p", sub)
	e.Leave("p", sub) // 幂等
	if _, ok := <-sub.C; ok {
		t.Fatal("channel should be closed after Leave")
	}
	if sub.Err() != nil {
		t.Fatalf("Err = %v, want nil", sub.Err())
	}
}

func TestPagesIsolated(t *testing.T) {
	e := NewEngine()
	e.Accept("p1", drawOp("a", 1))
	e.Accept("p2", drawOp("a", 2))
	s1, _ := e.Enter("p1", "b")
	s2, _ := e.Enter("p2", "b")
	if s1.BaseSeq != 1 || s2.BaseSeq != 1 {
		t.Fatalf("seqs shared across pages: %d, %d", s1.BaseSeq, s2.BaseSeq)
	}
	if len(s1.Strokes) != 1 || len(s2.Strokes) != 1 {
		t.Fatal("strokes leaked across pages")
	}
}
