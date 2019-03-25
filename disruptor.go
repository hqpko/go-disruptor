package disruptor

type Disruptor struct {
	writer  *Writer
	readers []*Reader
}

func (d Disruptor) Writer() *Writer {
	return d.writer
}

func (d Disruptor) Start() {
	for _, item := range d.readers {
		item.Start()
	}
}

func (d Disruptor) Stop() {
	for _, item := range d.readers {
		item.Stop()
	}
}
