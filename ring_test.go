package main_test

import (
	"fmt"
	"testing"

	"github.com/valyala/fastjson"
	"kruzenshtern.org/alpaca/ring/disruptor"
	"kruzenshtern.org/alpaca/ring/onering"
	"kruzenshtern.org/alpaca/ring/polygon"
)

// BufferSize ...
const bufferSize = 1024

var ringBuffer = onering.NewRingBuffer(bufferSize)
var writer *disruptor.Writer

var node *onering.Node
var parser fastjson.Parser
var object, value *fastjson.Value
var array []*fastjson.Value

func BenchmarkSingleProducerSingleConsumer(b *testing.B) {
	// producer -> ring buffer <- empty reader

	waiter := disruptor.NewWaitStrategy()

	writerCursor := disruptor.NewCursor()

	readerCursor := disruptor.NewCursor()
	emptyReader := disruptor.NewReader(readerCursor, writerCursor, writerCursor, waiter, emptyConsumer{})

	barrier := disruptor.NewCompositeBarrier(readerCursor)

	writer = disruptor.NewWriter(writerCursor, barrier, bufferSize)

	reader := disruptor.NewCompositeReader(emptyReader)

	go producer(b, reader, writer)

	reader.Read()
}

func producer(b *testing.B, reader *disruptor.CompositeReader, writer *disruptor.Writer) {
	b.ReportAllocs()
	b.ResetTimer()

	iterations := int64(b.N)

	var messages = []byte(`[{"ev":"Q","sym":"SPY","c":1,"bx":11,"ax":12,"bp":315.35,"ap":315.37,"bs":9,"as":3,"t":1594215625298,"z":1}]`)

	for i := int64(0); i < iterations-1; i++ {
		produce(messages)
	}

	_ = reader.Close()
}

// Merge with the main product function
func produce(messages []byte) {
	var sequence int64
	var err error

	value, err = parser.ParseBytes(messages)
	if err != nil {
		panic(fmt.Sprintf("cannot obtain object: %s\n", err))
	}

	// We always receive a list of events
	array, err = value.Array()
	if err != nil {
		panic((fmt.Sprintf("cannot obtain object: %s", err)))
	}

	for i := 0; i < len(array); i++ {
		sequence = writer.Reserve(1)
		object = array[i]
		node = &ringBuffer.Nodes[sequence&ringBuffer.BufferMask]
		switch object.GetStringBytes("ev")[0] {
		case polygon.Trades:
			node.Trade.Symbol = object.GetStringBytes("sym")
			// node.Trade.Exchange = object.GetInt("x")        // not used
			// node.Trade.TradeID = object.GetStringBytes("i") // not used
			node.Trade.Price = object.GetFloat64("p")
			node.Trade.Size = object.GetInt64("s")
			node.Trade.Timestamp = object.GetInt64("t")
			// node.Trade.Conditions = o.GetArray("c") // not used
		case polygon.Quotes:
			node.Quote.Symbol = object.GetStringBytes("sym")
			// node.Quote.Condition = object.GetInt("c")    // not used
			// node.Quote.BidExchange = object.GetInt("bx") // not used
			// node.Quote.AskExchange = object.GetInt("ax") // not used
			node.Quote.BidPrice = object.GetFloat64("bp")
			node.Quote.AskPrice = object.GetFloat64("ap")
			node.Quote.BidSize = object.GetInt64("bs")
			node.Quote.AskSize = object.GetInt64("as")
			node.Quote.Timestamp = object.GetInt64("t")
		default:
			// unrecognized message
		}
		writer.Commit(sequence, sequence)
	}
}

// emptyConsumer ...
type emptyConsumer struct{}

// Consume ...
func (consumer emptyConsumer) Consume(lower, upper int64) {
	for ; lower <= upper; lower++ {
	}
}
