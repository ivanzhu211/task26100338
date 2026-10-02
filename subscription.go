package canvas

import (
	"errors"
	"sync"
)

// ErrLagged 表示订阅者落后太多被重置，必须重新进入页面。
// 推送通道会以该原因关闭，绝不静默跳过编号。
var ErrLagged = errors.New("canvas: subscription lagged, re-enter required")

// Subscription 是一个页面推送订阅。
// 事件按编号顺序从 C 送出，不漏不重。
// C 关闭后调用 Err 获取原因：ErrLagged 表示被重置，nil 表示正常退出。
type Subscription struct {
	// C 是事件推送通道，按编号升序送达。
	C <-chan Event

	c       chan Event
	err     error
	closeFn func(*Subscription)
	once    sync.Once
}

// Err 在推送通道关闭后返回关闭原因；正常关闭返回 nil。
func (s *Subscription) Err() error { return s.err }

// Close 主动取消订阅，幂等。
func (s *Subscription) Close() {
	s.once.Do(func() { s.closeFn(s) })
}

// finish 由页面在锁内调用：记录原因并关闭推送通道。
func (s *Subscription) finish(err error) {
	s.err = err
	close(s.c)
}
