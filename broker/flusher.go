package broker

import (
	"time"

	"github.com/nalgnaohel/kage/storage"
)

type AppendResult struct {
	Offset int64
	Error  error
}

type BatchItem struct {
	Value      []byte
	ResultChan chan AppendResult
}

type Flusher struct {
	log           *storage.Log
	incomingQueue chan BatchItem
	batchSize     int
	lingerTime    time.Duration
	stopChan      chan struct{}
}

func NewFlusher(l *storage.Log, batchSize int, linger time.Duration) *Flusher {
	f := &Flusher{
		log:           l,
		incomingQueue: make(chan BatchItem, 4096),
		batchSize:     batchSize,
		lingerTime:    linger,
		stopChan:      make(chan struct{}),
	}

	go f.run()
	return f
}

func (f *Flusher) run() {
	var currentBatch []BatchItem

	ticker := time.NewTicker(f.lingerTime)
	defer ticker.Stop()
	for {
		select {
		case <-f.stopChan:
			f.flushBatch(currentBatch)
			return
		case item := <-f.incomingQueue:
			currentBatch = append(currentBatch, item)
			if len(currentBatch) >= f.batchSize {
				f.flushBatch(currentBatch)
				currentBatch = nil
				ticker.Reset(f.lingerTime)
			}
		case <-ticker.C:
			if len(currentBatch) > 0 {
				f.flushBatch(currentBatch)
				currentBatch = nil
			}
		}
	}
}

func (f *Flusher) flushBatch(batch []BatchItem) {
	for _, item := range batch {
		offset, err := f.log.Append(item.Value)

		// Group-commit: each waiter gets its own channel so producers sharing
		// a batch unblock independently instead of all waiting on one signal.
		item.ResultChan <- AppendResult{
			Offset: int64(offset),
			Error:  err,
		}
	}
	// TODO: Sync for safety
}

func (f *Flusher) Push(value []byte) chan AppendResult {
	resChan := make(chan AppendResult, 1)
	f.incomingQueue <- BatchItem{
		Value:      value,
		ResultChan: resChan,
	}
	return resChan
}

func (f *Flusher) Close() {
	close(f.stopChan)
}
