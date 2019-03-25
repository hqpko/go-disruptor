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

func (s *SharedWriterBarrier) Read(lower int64) int64 {
	shift, mask := s.shift, s.mask
	upper := s.written.Load()

	for ; lower <= upper; lower++ {
		if s.committed[lower&mask] != int32(lower>>shift) {
			return lower - 1
		}
	}

	return upper
}

func (s *SharedWriterBarrier) Commit(lower, upper int64) {
	if lower > upper {
		panic("Attempting to commit a sequence where the lower reservation is greater than the higher reservation.")
	} else if (upper - lower) > s.capacity {
		panic("Attempting to commit a reservation larger than the size of the ring buffer. (upper-lower > s.capacity)")
	} else if lower == upper {
		s.committed[upper&s.mask] = int32(upper >> s.shift)
	} else {
		// working down the array rather than up keeps all items in the commit together
		// otherwise the reader(s) could split up the group
		for upper >= lower {
			s.committed[upper&s.mask] = int32(upper >> s.shift)
			upper--
		}
	}
}
