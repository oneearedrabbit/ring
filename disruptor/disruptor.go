package disruptor

import (
	"math"
	"runtime"
	"sync"
	"sync/atomic"
)

// Cursor ... prevent false sharing of the sequence cursor by padding the CPU cache line with 64 *bytes* of data.
type Cursor [8]int64

// NewCursor ...
func NewCursor() *Cursor {
	var cursor Cursor
	cursor[0] = defaultCursorValue
	return &cursor
}

// Store ...
func (cursor *Cursor) Store(value int64) { atomic.StoreInt64(&cursor[0], value) }

// Load ...
func (cursor *Cursor) Load() int64 { return atomic.LoadInt64(&cursor[0]) }

const defaultCursorValue = -1

// Writer ...
type Writer struct {
	written  *Cursor // the ring buffer has been written up to this sequence
	upstream Barrier // all of the readers have advanced up to this sequence
	capacity int64
	previous int64
	gate     int64
}

// GetSequence ...
func (writer *Writer) GetSequence() int64 {
	return writer.previous
}

// Reserve ...
func (writer *Writer) Reserve(count int64) int64 {
	writer.previous += count

	for spin := int64(0); writer.previous-writer.capacity > writer.gate; spin++ {
		// time.Sleep(time.Nanosecond)
		if spin&SpinMask == 0 {
			runtime.Gosched() // ; better performance if we sleep a nanosecond
		}

		writer.gate = writer.upstream.Load()
	}

	return writer.previous
}

// SpinMask ...
const SpinMask = 1024*16 - 1 // arbitrary; we'll want to experiment with different values

// Commit ...
func (writer *Writer) Commit(lower int64, upper int64) {
	writer.written.Store(upper)
}

// NewWriter ...
func NewWriter(written *Cursor, upstream Barrier, capacity int64) *Writer {
	return &Writer{
		upstream: upstream,
		written:  written,
		capacity: capacity,
		previous: defaultCursorValue,
		gate:     defaultCursorValue,
	}
}

// Reader ...
type Reader struct {
	state    int64
	current  *Cursor // this reader has processed up to this sequence
	written  *Cursor // the ring buffer has been written up to this sequence
	upstream Barrier // all of the readers have advanced up to this sequence
	waiter   WaitStrategy
	consumer Consumer
}

// NewReader ...
func NewReader(current, written *Cursor, upstream Barrier, waiter WaitStrategy, consumer Consumer) *Reader {
	return &Reader{
		state:    stateRunning,
		current:  current,
		written:  written,
		upstream: upstream,
		waiter:   waiter,
		consumer: consumer,
	}
}

func (reader *Reader) Read() {
	var gateCount, idleCount, lower, upper int64
	var current = reader.current.Load()

	for {
		lower = current + 1
		upper = reader.upstream.Load()

		if lower <= upper {
			reader.consumer.Consume(lower, upper)
			reader.current.Store(upper)
			current = upper
		} else if upper = reader.written.Load(); lower <= upper {
			// This is a known gate
			gateCount++
			idleCount = 0
			reader.waiter.Gate(gateCount)
		} else if atomic.LoadInt64(&reader.state) == stateRunning {
			// Could be an unknown gate or waiting for a new message
			// from a producer
			idleCount++
			gateCount = 0
			reader.waiter.Idle(idleCount)
		} else {
			break
		}
	}

	// if closer, ok := reader.consumer.(io.Closer); ok {
	// 	_ = closer.Close()
	// }
}

// Close ...
func (reader *Reader) Close() error {
	atomic.StoreInt64(&reader.state, stateClosed)
	return nil
}

const (
	stateRunning = iota
	stateClosed
)

// CompositeReader ...
type CompositeReader []*Reader

// NewCompositeReader ...
func NewCompositeReader(reader ...*Reader) *CompositeReader {
	var compositeReader = CompositeReader{}
	compositeReader = CompositeReader(reader)
	return &compositeReader
}

// Read ...
func (reader CompositeReader) Read() {
	var waiter sync.WaitGroup
	waiter.Add(len(reader))

	for _, item := range reader {
		go func(reader *Reader) {
			reader.Read()
			waiter.Done()
		}(item)
	}

	waiter.Wait()
}

// Close ...
func (reader CompositeReader) Close() error {
	for _, item := range reader {
		_ = item.Close()
	}

	return nil
}

// DefaultWaitStrategy ...
type DefaultWaitStrategy struct{}

// WaitStrategy ...
type WaitStrategy interface {
	Gate(int64)
	Idle(int64)
}

// NewWaitStrategy ...
func NewWaitStrategy() DefaultWaitStrategy { return DefaultWaitStrategy{} }

// Gate ...
func (waiter DefaultWaitStrategy) Gate(count int64) { runtime.Gosched() }

// Idle ...
func (waiter DefaultWaitStrategy) Idle(count int64) { runtime.Gosched() }

// Barrier ...
type Barrier interface {
	Load() int64
}

// CompositeBarrier ...
type CompositeBarrier []*Cursor

// NewCompositeBarrier ...
func NewCompositeBarrier(sequences ...*Cursor) Barrier {
	if len(sequences) == 1 {
		return sequences[0]
	}

	return CompositeBarrier(sequences)
}

// Load ...
func (barrier CompositeBarrier) Load() int64 {
	var minimum int64 = math.MaxInt64

	for _, item := range barrier {
		if sequence := item.Load(); sequence < minimum {
			minimum = sequence
		}
	}

	return minimum
}

// Consumer ...
type Consumer interface {
	Consume(lower, upper int64)
}
