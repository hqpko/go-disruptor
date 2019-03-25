package disruptor

import (
	"runtime"
	"time"
)

type Reader struct {
	read          *Cursor
	written       *Cursor
	writerBarrier Barrier
	consumer      Consumer
	ready         bool
}

func NewReader(read, written *Cursor, writerBarrier Barrier, consumer Consumer) *Reader {
	return &Reader{
		read:          read,
		written:       written,
		writerBarrier: writerBarrier,
		consumer:      consumer,
		ready:         false,
	}
}

func (r *Reader) Start() {
	r.ready = true
	go r.receive()
}
func (r *Reader) Stop() {
	r.ready = false
}

func (r *Reader) receive() {
	previous := r.read.Load()
	idling, gating := 0, 0

	for {
		lower := previous + 1
		upper := r.writerBarrier.Read(lower)

		if lower <= upper {
			r.consumer.Consume(lower, upper)
			r.read.Store(upper)
			previous = upper
		} else if upper = r.written.Load(); lower <= upper {
			time.Sleep(time.Microsecond)
			// Gating--TODO: wait strategy (provide gating count to wait strategy for phased backoff)
			gating++
			idling = 0
		} else if r.ready {
			time.Sleep(time.Millisecond)
			// Idling--TODO: wait strategy (provide idling count to wait strategy for phased backoff)
			idling++
			gating = 0
		} else {
			break
		}

		// sleeping increases the batch size which reduces number of writes required to store the sequence
		// reducing the number of writes allows the CPU to optimize the pipeline without prediction failures
		runtime.Gosched() // LockSupport.parkNanos(1L); http://bit.ly/1xiDINZ
	}
}
