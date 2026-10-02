package canvassync

import "testing"

func addOp(author string, n int) OpInput {
	pts := make([]Point, n)
	for i := range pts {
		pts[i] = Point{X: float64(i), Y: float64(i)}
	}
	return OpInput{Kind: OpAdd, Stroke: Stroke{Author: author, Color: 0xff00, Alpha: 0.5, Width: 2, Points: pts}}
}

func strokeIDs(v View) []StrokeID {
	ids := make([]StrokeID, len(v.Strokes))
	for i, st := range v.Strokes {
		ids[i] = st.ID
	}
	return ids
}

func TestAddEraseClearAndViewOrder(t *testing.T) {
	e := NewEngine()
	if _, err := e.Apply("p", "alice", addOp("alice", 3)); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Apply("p", "bob", addOp("bob", 3)); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Apply("p", "alice", addOp("alice", 3)); err != nil {
		t.Fatal(err)
	}

	v, _ := e.Enter("p", "carol")
	if v.Seq != 3 {
		t.Fatalf("view seq = %d, want 3", v.Seq)
	}
	got := strokeIDs(v)
	want := []StrokeID{1, 2, 3}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("stacking order = %v, want %v", got, want)
		}
	}
	st := v.Strokes[0]
	if st.Author != "alice" || st.Color != 0xff00 || st.Alpha != 0.5 || st.Width != 2 || len(st.Points) != 3 {
		t.Fatalf("stroke attributes not preserved: %+v", st)
	}

	// 擦除不存在的一笔：无效果，但仍占一个编号。
	if _, err := e.Apply("p", "bob", OpInput{Kind: OpErase, Target: 99}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Apply("p", "bob", OpInput{Kind: OpErase, Target: 1}); err != nil {
		t.Fatal(err)
	}
	v, _ = e.Enter("p", "dave")
	if got := strokeIDs(v); len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Fatalf("after erase = %v, want [2 3]", got)
	}

	// 清除 alice：只影响清除之前受理的笔画。
	if _, err := e.Apply("p", "bob", OpInput{Kind: OpClear, TargetAuthor: "alice"}); err != nil {
		t.Fatal(err)
	}
	v, _ = e.Enter("p", "erin")
	if got := strokeIDs(v); len(got) != 1 || got[0] != 2 {
		t.Fatalf("after clear = %v, want [2]", got)
	}
	// 清除之后 alice 再画的不受影响（上一轮清除已结束）。
	if _, err := e.Apply("p", "alice", addOp("alice", 1)); err != nil {
		t.Fatal(err)
	}
	v, _ = e.Enter("p", "frank")
	if got := strokeIDs(v); len(got) != 2 || got[0] != 2 || got[1] != 4 {
		t.Fatalf("after post-clear add = %v, want [2 4]", got)
	}
}

func TestSeqsAreContiguousAndPushedInOrder(t *testing.T) {
	e := NewEngine()
	_, sub := e.Enter("p", "alice")
	const n = 10
	for i := 0; i < n; i++ {
		if _, err := e.Apply("p", "bob", addOp("bob", 1)); err != nil {
			t.Fatal(err)
		}
	}
	for want := uint64(1); want <= n; want++ {
		env := <-sub.C()
		if env.Op == nil || env.Op.Seq != want {
			t.Fatalf("got envelope %+v, want seq %d", env, want)
		}
	}
}

func TestMergeKeepsEraseAndClearOnSnapshotStrokes(t *testing.T) {
	e := NewEngine()
	// 触发一次合并：5000 条落笔。
	for i := 0; i < mergeThreshold; i++ {
		if _, err := e.Apply("p", "alice", addOp("alice", 1)); err != nil {
			t.Fatal(err)
		}
	}
	// 再画一笔，然后擦除已并入快照的第一笔、清除 alice（含快照内笔画）。
	if _, err := e.Apply("p", "bob", addOp("bob", 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Apply("p", "x", OpInput{Kind: OpErase, Target: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Apply("p", "x", OpInput{Kind: OpClear, TargetAuthor: "alice"}); err != nil {
		t.Fatal(err)
	}
	v, _ := e.Enter("p", "late")
	got := strokeIDs(v)
	if len(got) != 1 || got[0] != StrokeID(mergeThreshold+1) {
		t.Fatalf("view after merge+erase+clear = %v, want [%d]", got, mergeThreshold+1)
	}
	if v.Seq != uint64(mergeThreshold+3) {
		t.Fatalf("view seq = %d, want %d", v.Seq, mergeThreshold+3)
	}
}

func TestReconnectReplayBoundary(t *testing.T) {
	for _, gap := range []int{ReplayWindow, ReplayWindow + 1} {
		e := NewEngine()
		_, sub := e.Enter("p", "alice")
		if _, err := e.Apply("p", "bob", addOp("bob", 1)); err != nil {
			t.Fatal(err)
		}
		env := <-sub.C()
		lastSeq := env.Op.Seq // alice 只收到第 1 条就断线
		sub.Close()

		for i := 0; i < gap; i++ {
			if _, err := e.Apply("p", "bob", addOp("bob", 1)); err != nil {
				t.Fatal(err)
			}
		}
		res, sub2 := e.Reconnect("p", "alice", lastSeq)
		defer sub2.Close()
		if gap <= ReplayWindow {
			if res.Kind != ReconnectReplay {
				t.Fatalf("gap=%d: kind = %v, want replay", gap, res.Kind)
			}
			if len(res.Missed) != gap || res.Missed[0].Seq != lastSeq+1 || res.Missed[len(res.Missed)-1].Seq != lastSeq+uint64(gap) {
				t.Fatalf("gap=%d: bad missed range (len=%d)", gap, len(res.Missed))
			}
		} else {
			if res.Kind != ReconnectSnapshot {
				t.Fatalf("gap=%d: kind = %v, want snapshot", gap, res.Kind)
			}
			if res.View.Seq != lastSeq+uint64(gap) {
				t.Fatalf("gap=%d: snapshot seq = %d", gap, res.View.Seq)
			}
			if len(res.View.Strokes) != gap+1 {
				t.Fatalf("gap=%d: snapshot strokes = %d, want %d", gap, len(res.View.Strokes), gap+1)
			}
		}
	}
}

func TestSlowConsumerEvictedOthersUnaffected(t *testing.T) {
	e := NewEngine()
	_, slow := e.Enter("p", "slow")
	_, fast := e.Enter("p", "fast")

	// 写满并踢出 slow：subBuffer+1 条推送即可。
	for i := 0; i < subBuffer+1; i++ {
		if _, err := e.Apply("p", "bob", addOp("bob", 1)); err != nil {
			t.Fatal(err)
		}
		if i%97 == 0 {
			<-fast.C() // fast 正常消费
		}
	}
	// slow 收到显式的重同步信号。
	var sawResync bool
	for env := range slow.C() {
		if env.ResyncRequired {
			sawResync = true
		}
	}
	if !sawResync {
		t.Fatal("slow consumer was not told to resync")
	}
	if !slow.ResyncRequired() {
		t.Fatal("ResyncRequired() = false, want true")
	}
	// fast 继续按序接收。
	if _, err := e.Apply("p", "bob", addOp("bob", 1)); err != nil {
		t.Fatal(err)
	}
	for {
		env := <-fast.C()
		if env.Op != nil && env.Op.Seq == uint64(subBuffer+2) {
			break
		}
	}
}
