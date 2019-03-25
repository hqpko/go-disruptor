package disruptor

import (
	"runtime"
	"sync/atomic"
)

type SharedWriter struct {
	written  *Cursor
	upstream Barrier
	capacity int64
	gate     *Cursor

	writerBarrier *SharedWriterBarrier
}

func NewSharedWriter(write *SharedWriterBarrier, upstream Barrier) *SharedWriter {
	return &SharedWriter{
		written:       write.written,
		upstream:      upstream,
		capacity:      write.capacity,
		gate:          NewCursor(),
		writerBarrier: write,
	}
}

func (this *SharedWriter) Reserve(count int64) int64 {
	for {
		previous := this.written.Load()
		upper := previous + count

		for spin := int64(0); upper-this.capacity > this.gate.Load(); spin++ {
			if spin&SpinMask == 0 {
				runtime.Gosched() // LockSupport.parkNanos(1L); http://bit.ly/1xiDINZ
			}
			this.gate.Store(this.upstream.Read(0))
		}

		if atomic.CompareAndSwapInt64(&this.written.sequence, previous, upper) {
			return upper
		}
	}
}

func (this *SharedWriter) Commit(lower, upper int64) {
	this.writerBarrier.Commit(lower, upper)
}
