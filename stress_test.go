package canvas

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// applier 在客户端侧把事件流应用到起点画面上。
type applier struct{ p *page }

func newApplier(snap Snapshot) *applier {
	p := newPage()
	p.strokes = append([]*Stroke(nil), snap.Strokes...)
	for _, s := range p.strokes {
		p.alive[s.ID] = s
	}
	return &applier{p: p}
}

func (a *applier) apply(ev Event) { a.p.applyLocked(ev.Op) }

func fullReplay(events []Event) []*Stroke {
	p := newPage()
	for _, ev := range events {
		p.applyLocked(ev.Op)
	}
	return p.strokes
}

// 验收场景：多人在多页上同时落笔/擦除/清除，每页操作远超 5000 条，
// 合并发生多次；期间不断有人进入、断线重连、长期不取推送。
// 结束后每人画面必须等于从头重放，编号连续、不重不漏。
func TestConcurrentMultiPageConsistency(t *testing.T) {
	const (
		numPages   = 3
		opsPerPage = 12000 // 远超 5000，合并会发生多次
		numWriters = 4
		numClients = 12
		numStuck   = 2
	)
	e := NewEngine()
	pages := []string{"A", "B", "C"}
	authors := []string{"alice", "bob", "carol", "dave", "erin"}

	var strokeID atomic.Uint64
	histories := make([][]Event, numPages)
	var histMu [numPages]sync.Mutex

	var writersWG sync.WaitGroup
	for pi := range pages {
		for w := 0; w < numWriters; w++ {
			writersWG.Add(1)
			go func(pi, w int) {
				defer writersWG.Done()
				rng := rand.New(rand.NewSource(int64(pi*100 + w)))
				pg := pages[pi]
				for i := 0; i < opsPerPage/numWriters; i++ {
					var op Op
					switch rng.Intn(20) {
					case 0: // 擦除：可能命中也可能不存在
						op = Op{Kind: OpErase, Author: authors[rng.Intn(len(authors))],
							StrokeID: uint64(rng.Intn(int(strokeID.Load()) + 2))}
					case 1: // 清除某参与者
						a := authors[rng.Intn(len(authors))]
						op = Op{Kind: OpClear, Author: a, ClearOf: a}
					default: // 落笔，点数 1..500
						id := strokeID.Add(1)
						n := rng.Intn(500) + 1
						pts := make([]Point, n)
						for j := range pts {
							pts[j] = Point{float32(rng.Intn(1000)), float32(rng.Intn(1000))}
						}
						a := authors[rng.Intn(len(authors))]
						op = Op{Kind: OpDraw, Author: a, Stroke: Stroke{
							ID: id, Author: a, Color: uint32(rng.Intn(1 << 24)),
							Alpha: 0.5, Width: 2, Points: pts}}
					}
					ev := e.Accept(pg, op)
					histMu[pi].Lock()
					histories[pi] = append(histories[pi], ev)
					histMu[pi].Unlock()
				}
			}(pi, w)
		}
	}

	writersDone := make(chan struct{})
	go func() { writersWG.Wait(); close(writersDone) }()

	var clientsWG sync.WaitGroup
	for pi := range pages {
		for c := 0; c < numClients+numStuck; c++ {
			clientsWG.Add(1)
			go func(pi, c int) {
				defer clientsWG.Done()
				pg := pages[pi]
				name := fmt.Sprintf("client-%s-%d", pg, c)
				stuck := c >= numClients // 后 numStuck 个长期不取推送
				rng := rand.New(rand.NewSource(int64(pi*1000 + c)))
				finalSeq := uint64(opsPerPage)

				var sub *Subscription
				var app *applier
				var nextSeq, lastSeq uint64
				inSession := false

				check := func(tag string, ev Event) bool {
					if ev.Seq != nextSeq {
						t.Errorf("%s: [%s] seq = %d, want %d, lastSeq=%d（不重不漏）", name, tag, ev.Seq, nextSeq, lastSeq)
						return false
					}
					nextSeq++
					lastSeq = ev.Seq
					app.apply(ev)
					return true
				}

				for {
					// 写入全部结束后，卡住的客户端也转为正常追平
					select {
					case <-writersDone:
						stuck = false
					default:
					}
					// 全部写完后且已追平，结束
					if lastSeq >= finalSeq {
						select {
						case <-writersDone:
							if sub != nil {
								sub.Close()
							}
							goto verify
						default:
						}
					}
					if !inSession {
						var snap Snapshot
						if lastSeq == 0 || rng.Intn(2) == 0 {
							snap, sub = e.Enter(pg, name)
						} else {
							res, s := e.Resume(pg, name, lastSeq)
							sub = s
							if res.Snapshot != nil {
								snap = *res.Snapshot
							} else {
								snap = Snapshot{} // 画面沿用本地，只补缺口
								nextSeq = lastSeq + 1
								app2 := app
								_ = app2
								for _, ev := range res.Replay {
									if !check("replay", ev) {
										return
									}
								}
								inSession = true
								continue
							}
						}
						app = newApplier(snap)
						nextSeq = snap.BaseSeq + 1
						lastSeq = snap.BaseSeq
						inSession = true
						continue
					}
					if stuck {
						// 长期不取推送：每 50ms 才看一眼是否被重置
						time.Sleep(50 * time.Millisecond)
						select {
						case ev, ok := <-sub.C:
							if !ok {
								if sub.Err() != ErrLagged {
									t.Errorf("%s: stuck sub Err = %v, want ErrLagged", name, sub.Err())
									return
								}
								inSession = false
							} else if !check("stuck-poll", ev) {
								return
							}
						default:
						}
						continue
					}
					select {
					case ev, ok := <-sub.C:
						if !ok {
							if sub.Err() != ErrLagged {
								t.Errorf("%s: sub closed, Err = %v", name, sub.Err())
								return
							}
							inSession = false // 被重置：带 lastSeq 重连
							continue
						}
						if !check("chan", ev) {
							return
						}
					case <-time.After(5 * time.Millisecond):
						if rng.Intn(40) == 0 { // 主动离开，稍后重连
							sub.Close()
							inSession = false
						}
						select {
						case <-writersDone:
							if lastSeq >= finalSeq {
								if inSession && sub != nil {
									sub.Close()
								}
								goto verify
							}
						default:
						}
					}
				}
			verify:
				// 追平到 finalSeq 后校验最终画面
				histMu[pi].Lock()
				hist := append([]Event(nil), histories[pi]...)
				histMu[pi].Unlock()
				sort.Slice(hist, func(i, j int) bool { return hist[i].Seq < hist[j].Seq })
				for i, ev := range hist {
					if ev.Seq != uint64(i+1) {
						t.Errorf("page %s: history seq %d at %d, 编号不连续", pg, ev.Seq, i)
						return
					}
				}
				want := fullReplay(hist)
				got := app.p.strokes
				if !equalIDs(strokeIDs(got), strokeIDs(want)) {
					t.Errorf("%s: final picture != full replay (got %d strokes, want %d)",
						name, len(got), len(want))
				}
			}(pi, c)
		}
	}

	done := make(chan struct{})
	go func() { clientsWG.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(180 * time.Second):
		t.Fatal("stress test timed out")
	}
}
