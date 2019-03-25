package disruptor

type SharedDisruptor struct {
	writer  *SharedWriter
	readers []*Reader
}

func (s SharedDisruptor) Writer() *SharedWriter {
	return s.writer
}

func (s SharedDisruptor) Start() {
	for _, item := range s.readers {
		item.Start()
	}
}

func (s SharedDisruptor) Stop() {
	for _, item := range s.readers {
		item.Stop()
	}
}
