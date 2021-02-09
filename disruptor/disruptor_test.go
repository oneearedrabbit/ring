package disruptor

import (
	"runtime"
	"testing"

	"kruzenshtern.org/alpaca/ring/onering"
)

const (
	// BufferSize ...
	BufferSize = 1024
	// Iterations ...
	Iterations = 1024 * 16
	// Reservations ...
	Reservations = 1
)

var ringBuffer = onering.NewRingBuffer(BufferSize)
var node *onering.Node

func BenchmarkSingleProducerSingleConsumer(b *testing.B) {
	// producer -> ring buffer <- empty reader
	pp := runtime.GOMAXPROCS(1)

	iterations := int64(b.N)

	waiter := NewWaitStrategy()

	writerCursor := NewCursor()

	readerCursor := NewCursor()
	emptyReader := NewReader(readerCursor, writerCursor, writerCursor, waiter, emptyConsumer{})

	barrier := NewCompositeBarrier(readerCursor)

	writer := NewWriter(writerCursor, barrier, BufferSize)

	reader := NewCompositeReader(emptyReader)

	go produce(b, iterations, writer, reader)

	reader.Read()

	runtime.GOMAXPROCS(pp)
}

func BenchmarkSingleProducerChainConsumers(b *testing.B) {
	// producer -> ring buffer <- empty reader <- chain reader
	pp := runtime.GOMAXPROCS(4)

	iterations := int64(b.N)

	waiter := NewWaitStrategy()

	writerCursor := NewCursor()

	readerCursor := NewCursor()
	emptyReader := NewReader(readerCursor, writerCursor, writerCursor, waiter, emptyConsumer{})

	chainCursor := NewCursor()
	chainReader := NewReader(chainCursor, writerCursor, readerCursor, waiter, emptyConsumer{})

	barrier := NewCompositeBarrier(chainCursor)
	writer := NewWriter(writerCursor, barrier, BufferSize)

	reader := NewCompositeReader(emptyReader, chainReader)

	go produce(b, iterations, writer, reader)

	reader.Read()

	runtime.GOMAXPROCS(pp)
}

func BenchmarkSingleProducerDiamondConsumers(b *testing.B) {
	// producer -> ring buffer <- funnel reader <- (fork1, fork2 readers) <- diamon reader
	pp := runtime.GOMAXPROCS(4)

	iterations := int64(b.N)

	waiter := NewWaitStrategy()

	writerCursor := NewCursor()

	funnelCursor := NewCursor()
	funnelReader := NewReader(funnelCursor, writerCursor, writerCursor, waiter, emptyConsumer{})

	fork1Cursor := NewCursor()
	fork1Reader := NewReader(fork1Cursor, writerCursor, funnelCursor, waiter, emptyConsumer{})

	fork2Cursor := NewCursor()
	fork2Reader := NewReader(fork2Cursor, writerCursor, funnelCursor, waiter, emptyConsumer{})

	forkBarrier := NewCompositeBarrier(fork1Cursor, fork2Cursor)

	diamondCursor := NewCursor()
	diamodReader := NewReader(diamondCursor, writerCursor, forkBarrier, waiter, emptyConsumer{})

	barrier := NewCompositeBarrier(diamondCursor)
	writer := NewWriter(writerCursor, barrier, BufferSize)

	reader := NewCompositeReader(funnelReader, fork1Reader, fork2Reader, diamodReader)

	go produce(b, iterations, writer, reader)

	reader.Read()

	runtime.GOMAXPROCS(pp)
}

func produce(b *testing.B, iterations int64, writer *Writer, reader *CompositeReader) {
	b.ReportAllocs()
	b.ResetTimer()

	for sequence := int64(0); sequence < iterations-1; {
		sequence = writer.Reserve(Reservations)

		for lower := sequence - Reservations + 1; lower <= sequence; lower++ {
			node = &ringBuffer.Nodes[lower&ringBuffer.BufferMask]
			node.Trade.Timestamp = lower
		}

		writer.Commit(sequence-Reservations+1, sequence)
	}

	_ = reader.Close()
}

type emptyConsumer struct{}

// Consume ...
func (consumer emptyConsumer) Consume(lower, upper int64) {
	for ; lower <= upper; lower++ {
	}
}
