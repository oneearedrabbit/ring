package onering

import (
	"kruzenshtern.org/alpaca/ring/polygon"
)

// Node ...
type Node struct {
	Message   []byte
	Trade     polygon.StreamTrade
	Quote     polygon.StreamQuote
	ReceiveAt int64 // polygon.Cursor
	JournalAt int64
	PrintAt   int64
}

// Nodes ...
type Nodes []Node

// RingBuffer ...
type RingBuffer struct {
	Nodes      Nodes
	BufferMask int64
}

// roundUp takes a int64 greater than 0 and rounds it up to the next
// power of 2.
func roundUp(v int64) int64 {
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v |= v >> 32
	v++
	return v
}

func (rb *RingBuffer) init(size int64) {
	size = roundUp(size)
	rb.Nodes = make(Nodes, size)
	for i := int64(0); i < size; i++ {
		rb.Nodes[i] = Node{Trade: polygon.StreamTrade{}, Quote: polygon.StreamQuote{}}
	}
	// so we don't have to do this with every put/get operation
	rb.BufferMask = size - 1
}

// NewRingBuffer will allocate, initialize, and return a ring buffer
// with the specified size.
func NewRingBuffer(size int64) *RingBuffer {
	rb := &RingBuffer{}
	rb.init(size)
	return rb
}

// Structures for Volume Order Imbalance calculations

// AssetNode ...
type AssetNode struct {
	TradeVolume          int64
	TradePrice           float64
	AskVolume            int64
	AskPrice             float64
	BidVolume            int64
	BidPrice             float64
	Timestamp            int64
	BucketSize           int64
	AverageTradePrice    float64
	TurnOver             float64
	VolumeOrderImbalance int64
	OrderImbalanceRatio  float64
	MidPriceBasis        float64
}

// AssetNodes ...
type AssetNodes []AssetNode

// AssetRingBuffer is a simplified RingBuffer with one producer and no consumers
type AssetRingBuffer struct {
	Nodes      AssetNodes
	BufferMask int64
}

// VolumeOrderImbalance ...
// type VolumeOrderImbalance map[string]*AssetRingBuffer

func (rb *AssetRingBuffer) init(size int64) {
	size = roundUp(size)
	rb.Nodes = make(AssetNodes, size)
	for i := int64(0); i < size; i++ {
		rb.Nodes[i] = AssetNode{}
	}
	// so we don't have to do this with every put/get operation
	rb.BufferMask = size - 1
}

// NewAssetRingBuffer will allocate, initialize, and return a ring buffer
// with the specified size.
func NewAssetRingBuffer(size int64) *AssetRingBuffer {
	rb := &AssetRingBuffer{}
	rb.init(size)
	return rb
}

// // Reserve ...
// func (rb *AssetRingBuffer) Reserve() int64 {
// 	rb.Sequence++
// 	return rb.Sequence
// }

// // Commit ...
// func (rb *AssetRingBuffer) Commit(sequence int64) {
// 	atomic.StoreInt64(&rb.Position, sequence)
// }
