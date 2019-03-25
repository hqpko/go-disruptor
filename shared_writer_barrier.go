package disruptor

import "math"

type SharedWriterBarrier struct {
	written   *Cursor
	committed []int32
	capacity  int64
	mask      int64
	shift     uint8
}

func NewSharedWriterBarrier(written *Cursor, capacity int64) *SharedWriterBarrier {
	assertPowerOfTwo(capacity)

	return &SharedWriterBarrier{
		written:   written,
		committed: prepareCommitBuffer(capacity),
		capacity:  capacity,
		mask:      capacity - 1,
		shift:     uint8(math.Log2(float64(capacity))),
	}
}
func prepareCommitBuffer(capacity int64) []int32 {
	buffer := make([]int32, capacity)
	for i := range buffer {
		buffer[i] = int32(InitialSequenceValue)
	}
	return buffer
}

func (this *SharedWriterBarrier) Read(lower int64) int64 {
	shift, mask := this.shift, this.mask
	upper := this.written.Load()

	for ; lower <= upper; lower++ {
		if this.committed[lower&mask] != int32(lower>>shift) {
			return lower - 1
		}
	}

	return upper
}

func (this *SharedWriterBarrier) Commit(lower, upper int64) {
	if lower > upper {
		panic("Attempting to commit a sequence where the lower reservation is greater than the higher reservation.")
	} else if (upper - lower) > this.capacity {
		panic("Attempting to commit a reservation larger than the size of the ring buffer. (upper-lower > this.capacity)")
	} else if lower == upper {
		this.committed[upper&this.mask] = int32(upper >> this.shift)
	} else {
		// working down the array rather than up keeps all items in the commit together
		// otherwise the reader(s) could split up the group
		for upper >= lower {
			this.committed[upper&this.mask] = int32(upper >> this.shift)
			upper--
		}
	}
}
