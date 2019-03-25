package disruptor

import "runtime"

type Writer struct {
	written       *Cursor
	readerBarrier Barrier
	capacity      int64
	previous      int64
	gate          int64
}

func NewWriter(written *Cursor, readerBarrier Barrier, capacity int64) *Writer {
	assertPowerOfTwo(capacity)

	return &Writer{
		readerBarrier: readerBarrier,
		written:       written,
		capacity:      capacity,
		previous:      InitialSequenceValue,
		gate:          InitialSequenceValue,
	}
}

func assertPowerOfTwo(value int64) {
	if value > 0 && (value&(value-1)) != 0 {
		// Wikipedia entry: http://bit.ly/1krhaSB
		panic("The ring capacity must be a power of two, e.g. 2, 4, 8, 16, 32, 64, etc.")
	}
}

func (w *Writer) Reserve(count int64) int64 {
	w.previous += count

	for spin := int64(0); w.previous-w.capacity > w.gate; spin++ {
		if spin&SpinMask == 0 {
			runtime.Gosched() // LockSupport.parkNanos(1L); http://bit.ly/1xiDINZ
		}

		w.gate = w.readerBarrier.Read(0)
	}

	return w.previous
}

func (w *Writer) Await(next int64) {
	for next-w.capacity > w.gate {
		w.gate = w.readerBarrier.Read(0)
	}
}

const SpinMask = 1024*16 - 1 // arbitrary; we'll want to experiment with different values

func (w *Writer) Commit(lower, upper int64) {
	w.written.Store(upper)
}
