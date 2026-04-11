package broker

type AppendResult struct {
	Offset int64
	Error  error
}

type BatchItem struct {
	Value      []byte
	resultChan chan AppendResult
}

// Flusher manages the batching logic for a single partition.
// It ensures that data is written in batches to optimize Disk I/O.
type Flusher struct {
	log           *storage.Log
	incomingQueue chan BatchItem
	batchSize     int
	lingerTime    time.Duration
	stopChan      chan struct{}
}

// NewFlusher initializes and starts a background flusher for the given log.
func NewFlusher(l *storage.Log, batchSize int, linger time.Duration) *Flusher {
	f := &Flusher{
		log:           l,
		incomingQueue: make(chan BatchItem, 4096), // Buffer to handle request spikes
		batchSize:     batchSize,
		lingerTime:    linger,
		stopChan:      make(chan struct{}),
	}

	// Start the background worker goroutine
	go f.run()
	return f
}

// run is the main worker loop. It stays active until stopChan is closed.
func (f *Flusher) run() {
	var currentBatch []BatchItem

	ticker := time.NewTicker(f.lingerTime)
	defer ticker.Stop()
	for {
		select {
		case <-f.stopChan:
			// Flush any remaining items before exiting
			f.flushBatch(currentBatch)
			return
		case item := <-f.incomingQueue:
			currentBatch = append(currentBatch, item)
			if len(currentBatch) >= f.batchSize {
				f.flushBatch(currentBatch)
				currentBatch = nil         // Reset batch after flushing
				ticker.Reset(f.lingerTime) // Reset timer after flushing
			}
		case <-ticker.C:
			if len(currentBatch) > 0 {
				f.flushBatch(currentBatch)
				currentBatch = nil // Reset batch after flushing
			}
		}
	}
}

// executeFlush writes the accumulated batch to the storage engine and notifies callers.
func (f *Flusher) executeFlush(batch []BatchItem) {
	for _, item := range batch {
		offset, err := f.log.Append(item.Value)

		// Group-commit: Send result back to the specific producer waiting for this item.
		// The producer (gRPC handler) will only unblock once this is sent.
		item.ResultChan <- AppendResult{
			Offset: offset,
			Error:  err,
		}
	}
	// TODO: Sync for safety
}

// Push adds a new record to the flusher's queue.
// It returns a result channel that the caller must listen to.
func (f *Flusher) Push(value []byte) chan AppendResult {
	resChan := make(chan AppendResult, 1)
	f.incomingQueue <- BatchItem{
		Value:      value,
		ResultChan: resChan,
	}
	return resChan
}

// Close signals the flusher to shut down gracefully.
func (f *Flusher) Close() {
	close(f.stopChan)
}
