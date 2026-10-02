package canvassync

// Envelope 是订阅通道上传输的消息：要么是一个操作，要么是重同步信号。
type Envelope struct {
	// Op 在需要重同步时为 nil。
	Op *Op
	// ResyncRequired 为 true 表示该订阅者落后太多被踢出，
	// 必须重新进入页面；引擎不会悄悄跳过任何一段操作。
	ResyncRequired bool
}

// Subscription 是一个客户端在某页上的订阅句柄。
// 字段仅在持有 page.mu 时访问（closed、evicted），或初始化后只读。
type Subscription struct {
	ch     chan Envelope
	page   *page
	client string

	closed  bool
	evicted bool
}

// C 返回接收推送的通道。订阅者按编号顺序收到操作，不漏不重。
// 通道关闭后可用 ResyncRequired 区分是被踢出还是主动关闭。
func (s *Subscription) C() <-chan Envelope {
	return s.ch
}

// ResyncRequired 报告该订阅是否因落后太多被引擎踢出。
// 仅在 C() 返回的通道关闭后调用才有意义。
func (s *Subscription) ResyncRequired() bool {
	s.page.mu.Lock()
	defer s.page.mu.Unlock()
	return s.evicted
}

// Close 取消订阅并释放资源；重复调用安全。
func (s *Subscription) Close() {
	s.page.mu.Lock()
	defer s.page.mu.Unlock()
	s.closeLocked()
}

// closeLocked 关闭订阅。调用方必须持有 page.mu。
func (s *Subscription) closeLocked() {
	if s.closed {
		return
	}
	s.closed = true
	close(s.ch)
	s.page.unsubscribe(s)
}
