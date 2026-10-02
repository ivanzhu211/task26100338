package canvas

import "testing"

// 在客户端断线期间向页面追加 n 条操作，返回客户端重连结果。
func reconnectAfter(t *testing.T, e *Engine, page string, lastSeq uint64, n int) (ResumeResult, *Subscription) {
	t.Helper()
	base := lastSeq
	for i := 0; i < n; i++ {
		base++
		e.Accept(page, drawOp("teacher", base))
	}
	return e.Resume(page, "student", lastSeq)
}

func TestResumeGapExactlyAtLimit(t *testing.T) {
	e := NewEngine()
	snap, sub := e.Enter("p", "student")
	lastSeq := snap.BaseSeq // 0
	e.Leave("p", sub)

	res, sub2 := reconnectAfter(t, e, "p", lastSeq, ReplayLimit)
	if res.Snapshot != nil {
		t.Fatal("gap == ReplayLimit should replay, got snapshot")
	}
	if len(res.Replay) != ReplayLimit {
		t.Fatalf("replay len = %d, want %d", len(res.Replay), ReplayLimit)
	}
	for i, ev := range res.Replay {
		if ev.Seq != uint64(i+1) {
			t.Fatalf("replay[%d].Seq = %d, want %d", i, ev.Seq, i+1)
		}
	}
	// 补发段 + 后续推送 == 从头重放
	more := e.Accept("p", drawOp("teacher", ReplayLimit+1))
	evs := append(append([]Event(nil), res.Replay...), drain(sub2)...)
	if evs[len(evs)-1].Seq != more.Seq {
		t.Fatal("missing event after resume")
	}
	got := applyAll(nil, evs)
	if len(got) != ReplayLimit+1 {
		t.Fatalf("final strokes = %d, want %d", len(got), ReplayLimit+1)
	}
	e.Leave("p", sub2)
}

func TestResumeGapOverLimit(t *testing.T) {
	e := NewEngine()
	snap, sub := e.Enter("p", "student")
	lastSeq := snap.BaseSeq
	e.Leave("p", sub)

	res, sub2 := reconnectAfter(t, e, "p", lastSeq, ReplayLimit+1)
	if res.Replay != nil {
		t.Fatal("gap > ReplayLimit should give snapshot, got replay")
	}
	if res.Snapshot == nil || res.Snapshot.BaseSeq != uint64(ReplayLimit+1) {
		t.Fatalf("snapshot = %+v, want BaseSeq %d", res.Snapshot, ReplayLimit+1)
	}
	if len(res.Snapshot.Strokes) != ReplayLimit+1 {
		t.Fatalf("snapshot strokes = %d, want %d", len(res.Snapshot.Strokes), ReplayLimit+1)
	}
	// 新起点 + 后续推送 == 从头重放
	e.Accept("p", drawOp("teacher", ReplayLimit+2))
	got := applyAll(res.Snapshot.Strokes, drain(sub2))
	if len(got) != ReplayLimit+2 {
		t.Fatalf("final strokes = %d, want %d", len(got), ReplayLimit+2)
	}
	e.Leave("p", sub2)
}

// 合并发生后，补发恰好 ReplayLimit 条的缺口仍必须可用（合并不得吞掉补发段）。
func TestReplayAvailableRightAfterMerge(t *testing.T) {
	e := NewEngine()
	snap, sub := e.Enter("p", "student")
	e.Leave("p", sub)
	// 制造两次合并：共 2*MergeThreshold 条
	for i := 0; i < 2*MergeThreshold; i++ {
		e.Accept("p", drawOp("teacher", uint64(i+1)))
	}
	res, _ := e.Resume("p", "student", snap.BaseSeq)
	if res.Snapshot == nil {
		t.Fatal("gap of 2*MergeThreshold should give snapshot")
	}
	// 换一个只差 ReplayLimit 条的客户端
	res2, sub2 := e.Resume("p", "student2", uint64(2*MergeThreshold-ReplayLimit))
	if res2.Replay == nil || len(res2.Replay) != ReplayLimit {
		t.Fatalf("replay after merge = %v events, want %d", len(res2.Replay), ReplayLimit)
	}
	e.Leave("p", sub2)
}

// 合并之后，擦除和清除照样作用于已并入快照的笔画。
func TestEraseClearAfterMerge(t *testing.T) {
	e := NewEngine()
	e.Accept("p", drawOp("alice", 1))
	e.Accept("p", drawOp("bob", 2))
	// 填满一个合并周期，把上面两笔并入快照
	for i := uint64(3); i <= MergeThreshold; i++ {
		e.Accept("p", drawOp("teacher", i))
	}
	e.Accept("p", Op{Kind: OpErase, Author: "bob", StrokeID: 1})
	e.Accept("p", Op{Kind: OpClear, Author: "bob"})
	snap, _ := e.Enter("p", "carol")
	for _, s := range snap.Strokes {
		if s.ID == 1 || s.ID == 2 {
			t.Fatalf("stroke %d should be gone after merge", s.ID)
		}
	}
	if len(snap.Strokes) != MergeThreshold-2 {
		t.Fatalf("strokes = %d, want %d", len(snap.Strokes), MergeThreshold-2)
	}
}

// 进入与重连耗时不得随累计操作数增长：10 万条操作后 Enter/Resume 仍应快速返回。
func TestEnterResumeCostIndependentOfOpCount(t *testing.T) {
	e := NewEngine()
	for i := uint64(1); i <= 100000; i++ {
		e.Accept("p", drawOp("teacher", i))
	}
	// 不 benchmark 精确耗时，只验证能在宽松时限内完成（冒烟）。
	for i := 0; i < 100; i++ {
		_, sub := e.Enter("p", "s")
		e.Leave("p", sub)
		res, sub2 := e.Resume("p", "s", 100000-ReplayLimit)
		if len(res.Replay) != ReplayLimit {
			t.Fatalf("replay len = %d", len(res.Replay))
		}
		e.Leave("p", sub2)
	}
}
