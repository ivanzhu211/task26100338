package canvassync

// Point 是笔画中的一个采样点。
type Point struct {
	X float64
	Y float64
}

// StrokeID 由引擎在受理落笔时分配，页内唯一。
type StrokeID uint64

// OpKind 标识一页上操作的种类。
type OpKind int

const (
	// OpAdd 落笔：新增一笔。
	OpAdd OpKind = iota
	// OpErase 擦除指定的一笔；笔画不存在时无效果。
	OpErase
	// OpClear 清除某个参与者在本页、且先于本操作受理的全部笔画。
	OpClear
)

// Stroke 是一笔的完整数据，作者、颜色、透明度、粗细与点序列原样保留。
type Stroke struct {
	ID     StrokeID
	Author string
	Color  uint32
	Alpha  float64
	Width  float64
	Points []Point
}

// Op 是页上按受理顺序编号的一次操作，也是推送给订阅者的单元。
type Op struct {
	Seq   uint64
	Kind  OpKind
	Actor string

	// OpAdd 时有效：完整的笔画数据（含引擎分配的 ID）。
	Stroke Stroke
	// OpErase 时有效：要擦除的笔画 ID。
	Target StrokeID
	// OpClear 时有效：被清除笔画的参与者。
	TargetAuthor string
}

// OpInput 是上层（WebSocket 网关）提交的操作请求。
type OpInput struct {
	Kind OpKind

	// OpAdd 时有效：ID 字段忽略，由引擎分配。
	Stroke Stroke
	// OpErase 时有效。
	Target StrokeID
	// OpClear 时有效。
	TargetAuthor string
}

// View 是进入页面或需要重同步时下发的起点画面。
// Strokes 自下而上按叠放顺序排列，Seq 是该画面对应的操作编号。
type View struct {
	Seq     uint64
	Strokes []Stroke
}

// ReconnectKind 标识断线重连的处理结果。
type ReconnectKind int

const (
	// ReconnectReplay 断开期间新增操作不超过 ReplayWindow 条，只补发缺的这一段。
	ReconnectReplay ReconnectKind = iota
	// ReconnectSnapshot 断开期间新增操作过多，下发新的起点画面。
	ReconnectSnapshot
)

// ReconnectResult 是断线重连的返回结果。
type ReconnectResult struct {
	Kind ReconnectKind
	// Kind 为 ReconnectReplay 时有效：编号连续、从 lastSeq+1 开始的一段操作。
	Missed []Op
	// Kind 为 ReconnectSnapshot 时有效：新的起点画面。
	View View
}
