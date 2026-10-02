package canvas

import (
	"sort"
	"sync"
	"testing"
	"time"
)

// 有人卡着不取推送、几十人同时进入/重连、页面正在合并快照时，
// 单次落笔受理耗时的 99 分位不得超过 10ms。
func TestDrawLatencyP99UnderAdversarialLoad(t *testing.T) {
	const (
		numDraws   = 20000 // 触发多次合并
		numReaders = 20    // 正常取推送的订阅者
		numStuck   = 5     // 卡着不取的订阅者
	)
	e := NewEngine()
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// 正常订阅者：持续取推送
	for i := 0; i < numReaders; i++ {
		_, sub := e.Enter("hot", "reader")
		wg.Add(1)
		go func(s *Subscription) {
			defer wg.Done()
			for {
				select {
				case _, ok := <-s.C:
					if !ok {
						return
					}
				case <-stop:
					s.Close()
					return
				}
			}
		}(sub)
	}
	// 卡住的订阅者：从不取推送，被重置后立刻再进入，持续制造压力
	for i := 0; i < numStuck; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, sub := e.Enter("hot", "stuck")
				// 基本不取推送，只偶尔探一下是否已被重置
				for {
					select {
					case <-stop:
						sub.Close()
						return
					case <-time.After(2 * time.Millisecond):
					}
					select {
					case _, ok := <-sub.C:
						if !ok { // 已被重置，重新进入
							if sub.Err() != ErrLagged {
								t.Errorf("stuck sub Err = %v, want ErrLagged", sub.Err())
							}
							goto reenter
						}
					default:
					}
				}
			reenter:
			}
		}()
	}
	// 进入/重连风暴
	wg.Add(1)
	go func() {
		defer wg.Done()
		var lastSeq uint64
		for {
			select {
			case <-stop:
				return
			default:
			}
			snap, sub := e.Enter("hot", "joiner")
			lastSeq = snap.BaseSeq
			sub.Close()
			res, sub2 := e.Resume("hot", "joiner", lastSeq)
			_ = res
			sub2.Close()
		}
	}()

	durations := make([]time.Duration, 0, numDraws)
	for i := 0; i < numDraws; i++ {
		start := time.Now()
		e.Accept("hot", drawOp("teacher", uint64(i+1)))
		durations = append(durations, time.Since(start))
	}
	close(stop)
	wg.Wait()

	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	p99 := durations[len(durations)*99/100]
	p50 := durations[len(durations)/2]
	t.Logf("draw accept: p50=%v p99=%v max=%v", p50, p99, durations[len(durations)-1])
	if p99 > 10*time.Millisecond {
		t.Fatalf("p99 = %v, want <= 10ms", p99)
	}
}
