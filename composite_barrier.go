package disruptor

type CompositeBarrier []*Cursor

func NewCompositeBarrier(barriers ...*Cursor) CompositeBarrier {
	if len(barriers) == 0 {
		panic("At least one barrier cursor is required.")
	}

	cursors := make([]*Cursor, len(barriers))
	copy(cursors, barriers)
	return CompositeBarrier(cursors)
}

func (c CompositeBarrier) Read(noop int64) int64 {
	minimum := MaxSequenceValue
	for _, item := range c {
		sequence := item.Load()
		if sequence < minimum {
			minimum = sequence
		}
	}

	return minimum
}
