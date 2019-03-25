package disruptor

import "runtime"

type SharedWriter struct {
	written    *Cursor
	depBarrier Barrier // 所依赖的屏障，写屏障依赖于最后一组读屏障
	capacity   int64
	gate       *Cursor

	writerBarrier *SharedWriterBarrier
}

func NewSharedWriter(write *SharedWriterBarrier, depBarrier Barrier) *SharedWriter {
	return &SharedWriter{
		written:       write.written,
		depBarrier:    depBarrier,
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
			this.gate.Store(this.depBarrier.Read(0))
		}

		if this.written.CompareAndSwapInt64(previous, upper) {
			return upper
		}
	}
}

func (this *SharedWriter) Commit(lower, upper int64) {
	this.writerBarrier.Commit(lower, upper)
}
