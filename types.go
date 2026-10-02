package canvas

// Point 是笔画轨迹中的一个采样点。
type Point struct {
	X, Y float32
}

// Stroke 是一笔的完整数据。受理后不可变。
type Stroke struct {
	ID     uint64
	Author string
	Color  uint32
	Alpha  float32
	Width  float32
	Points []Point
}

// OpKind 区分一页上的三种操作。
type OpKind uint8

const (
	OpDraw  OpKind = iota // 落笔：新增一笔
	OpErase               // 擦除指定的一笔
	OpClear               // 清除某参与者在本页此前的笔画
)

// Op 是一次待受理的操作。
type Op struct {
	Kind     OpKind
	Author   string // 操作发起人
	Stroke   Stroke // OpDraw 使用
	StrokeID uint64 // OpErase 使用
	ClearOf  string // OpClear 使用；为空表示清除 Author 自己
}

// Event 是已受理、已编号的操作。
type Event struct {
	Seq uint64
	Op  Op
}

// Snapshot 是进入页面时拿到的起点画面。
// Strokes 按受理顺序排列，后受理的在上层。
type Snapshot struct {
	BaseSeq uint64
	Strokes []*Stroke
}
