package disruptor

// Cursors should be a party of the same backing array to keep them as close together as possible:
// https://news.ycombinator.com/item?id=7800825
type (
	Wireup struct {
		capacity int64
		groups   [][]Consumer
		cursors  []*Cursor // backing array keeps cursors (with padding) in contiguous memory
	}
)

func Configure(capacity int64) Wireup {
	return Wireup{
		capacity: capacity,
		groups:   [][]Consumer{},
		cursors:  []*Cursor{NewCursor()},
	}
}

func (w Wireup) WithConsumerGroup(consumers ...Consumer) Wireup {
	if len(consumers) == 0 {
		return w
	}

	target := make([]Consumer, len(consumers))
	copy(target, consumers)

	for i := 0; i < len(consumers); i++ {
		w.cursors = append(w.cursors, NewCursor())
	}

	w.groups = append(w.groups, target)
	return w
}

func (w Wireup) Build() Disruptor {
	allReaders := []*Reader{}
	written := w.cursors[0]
	var depBarrier Barrier = w.cursors[0]
	cursorIndex := 1 // 0 index is reserved for the writer Cursor

	for groupIndex, group := range w.groups {
		groupReaders, groupBarrier := w.buildReaders(groupIndex, cursorIndex, written, depBarrier)
		for _, item := range groupReaders {
			allReaders = append(allReaders, item)
		}
		depBarrier = groupBarrier
		cursorIndex += len(group)
	}

	writer := NewWriter(written, depBarrier, w.capacity)
	return Disruptor{writer: writer, readers: allReaders}
}

func (w Wireup) BuildShared() SharedDisruptor {
	allReaders := []*Reader{}
	written := w.cursors[0]
	writerBarrier := NewSharedWriterBarrier(written, w.capacity)
	var depBarrier Barrier = writerBarrier
	cursorIndex := 1 // 0 index is reserved for the writer Cursor

	for groupIndex, group := range w.groups {
		groupReaders, groupBarrier := w.buildReaders(groupIndex, cursorIndex, written, depBarrier)
		for _, item := range groupReaders {
			allReaders = append(allReaders, item)
		}
		depBarrier = groupBarrier
		cursorIndex += len(group)
	}

	writer := NewSharedWriter(writerBarrier, depBarrier)
	return SharedDisruptor{writer: writer, readers: allReaders}
}

func (w Wireup) buildReaders(consumerIndex, cursorIndex int, written *Cursor, depBarrier Barrier) ([]*Reader, Barrier) {
	barrierCursors := []*Cursor{}
	readers := []*Reader{}

	for _, consumer := range w.groups[consumerIndex] {
		cursor := w.cursors[cursorIndex]
		barrierCursors = append(barrierCursors, cursor)
		reader := NewReader(cursor, written, depBarrier, consumer)
		readers = append(readers, reader)
		cursorIndex++
	}

	if len(w.groups[consumerIndex]) == 1 {
		return readers, barrierCursors[0]
	} else {
		return readers, NewCompositeBarrier(barrierCursors...)
	}
}
