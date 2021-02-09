package main

import (
	"bufio"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/VictoriaMetrics/metrics"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/valyala/fastjson"
	"kruzenshtern.org/alpaca/ring/alpaca"
	"kruzenshtern.org/alpaca/ring/disruptor"
	"kruzenshtern.org/alpaca/ring/onering"
	"kruzenshtern.org/alpaca/ring/polygon"

	_ "net/http/pprof"
)

const (
	// BufferSize ...
	BufferSize = 16384
	// VolumeOrderImbalanceSize ...
	VolumeOrderImbalanceSize = 8
)

var ringBuffer = onering.NewRingBuffer(BufferSize)

// VolumeOrderImbalance ...
type VolumeOrderImbalance map[string]*onering.AssetRingBuffer

var volumeOrderImbalance = make(VolumeOrderImbalance)
var volumeWriter, writer *disruptor.Writer

var node *onering.Node
var parser fastjson.Parser
var object, value *fastjson.Value
var array []*fastjson.Value

var count int

var (
	// Register counters
	writerCounter = metrics.NewCounter("writer_counter")
	ticksCounter  = metrics.NewCounter("ticks_counter")
)

func main() {
	// application doesn't support multi-core setup to optimize the
	// total number of allocations
	// runtime.GOMAXPROCS(1)

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMicro

	var startflag = flag.Bool("start", true, "start strategy")
	var backfillflag = flag.String("backfill", "", "run backfill from `file`")
	var assetsflag = flag.String("assets", "", "assets to subscribe to")

	flag.Parse()

	// Expose the registered metrics at `/metrics` path.
	http.HandleFunc("/metrics", func(w http.ResponseWriter, req *http.Request) {
		metrics.WritePrometheus(w, true)
	})
	go http.ListenAndServe(":8080", nil)

	start(assetsflag, startflag, backfillflag)
}

func start(assetsflag *string, startflag *bool, backfillflag *string) {
	// main ring buffer
	waiter := disruptor.NewWaitStrategy()

	writerCursor := disruptor.NewCursor()

	ticksCursor := disruptor.NewCursor()
	ticksReader := disruptor.NewReader(ticksCursor, writerCursor, writerCursor, waiter, TicksConsumer{})

	barrier := disruptor.NewCompositeBarrier(ticksCursor)
	writer = disruptor.NewWriter(writerCursor, barrier, BufferSize)

	// aggregated ring buffers
	volumeOrderImbalance["SPY"] = onering.NewAssetRingBuffer(VolumeOrderImbalanceSize)

	volumeWaiter := disruptor.NewWaitStrategy()

	volumeWriterCursor := disruptor.NewCursor()

	volumeReaderCursor := disruptor.NewCursor()
	voiReader := disruptor.NewReader(volumeReaderCursor, volumeWriterCursor, volumeWriterCursor, volumeWaiter, volumeOrderImbalanceConsumer{})

	volumeBarrier := disruptor.NewCompositeBarrier(volumeReaderCursor)

	volumeWriter = disruptor.NewWriter(volumeWriterCursor, volumeBarrier, VolumeOrderImbalanceSize)

	reader := disruptor.NewCompositeReader(ticksReader, voiReader)

	if *backfillflag != "" {
		// Review how to pass readers to multiple ring buffers in a more
		// native way. Right now, it's going to skip the very last
		// messages in volumeOrderImbalance buffer
		go backfillStreamer(reader, backfillflag, produce)
	} else if *startflag == true {
		go streamer(reader, *assetsflag, produce)
	}

	reader.Read()
}

func backfillStreamer(reader *disruptor.CompositeReader, backfillflag *string, handler func(msg []byte)) {
	var s string

	file, err := os.Open(*backfillflag)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		// read a log message
		s = scanner.Text()

		// extract a polygon message
		s, _ = strconv.Unquote(s[51 : len(s)-1])
		value, err = parser.Parse(s)
		if err != nil {
			log.Print(fmt.Sprintf("Cannot parse message: `%s'", s))
			continue
		}

		handler(value.MarshalTo(nil))

	}

	if err := scanner.Err(); err != nil {
		log.Print(err)
	}

	log.Print(fmt.Sprintf("Processed: %d", count))

	_ = reader.Close()
}

func streamer(reader *disruptor.CompositeReader, subscribeString string, handler func(msg []byte)) {
	var subscribeAssets []string

	if subscribeString == "" {
		client := alpaca.NewClient(alpaca.Credentials())
		assets, _ := client.ListAssets(nil)
		for _, asset := range assets {
			subscribeAssets = append(subscribeAssets, fmt.Sprintf("T.%s", asset.Symbol))
			subscribeAssets = append(subscribeAssets, fmt.Sprintf("Q.%s", asset.Symbol))
		}

		subscribeString = strings.Join(subscribeAssets, ",")
	}

	polygon.Register(subscribeString, handler)

	log.Print(fmt.Sprintf("Processed: %d", count))

	_ = reader.Close()
}

func produce(messages []byte) {
	var sequence int64
	var err error

	log.Print(fmt.Sprintf("%s", messages))

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
		count++

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

		if count%100000 == 0 {
			log.Print(fmt.Sprintf("Processed: %d", count))
		}
	}

	// Metrics
	writerCounter.Set(uint64(sequence))
}

// TicksConsumer ...
type TicksConsumer struct{}

// Consume ...
func (consumer TicksConsumer) Consume(lower, upper int64) {
	var prevMessage, message *onering.Node
	var previousNode, node *onering.AssetNode
	var ring *onering.AssetRingBuffer
	var previousTimestamp, timestamp int64
	var sequence int64

	ring = volumeOrderImbalance["SPY"]

	for ; lower <= upper; lower++ {
		message = &ringBuffer.Nodes[lower&ringBuffer.BufferMask]
		prevMessage = &ringBuffer.Nodes[(lower-1)&ringBuffer.BufferMask]

		// We assume that messages are sequentially ordered, but there
		// are some exceptions
		if prevMessage.Trade.Timestamp > 0 {
			previousTimestamp = prevMessage.Trade.Timestamp
		} else if prevMessage.Quote.Timestamp > 0 {
			previousTimestamp = prevMessage.Quote.Timestamp
		}

		if message.Trade.Timestamp > 0 {
			timestamp = message.Trade.Timestamp
		} else if message.Quote.Timestamp > 0 {
			timestamp = message.Quote.Timestamp
		}

		if timestamp == previousTimestamp {
			// Aggregate trades/quotes that have the same timestamp
			sequence = volumeWriter.GetSequence()
			node = &ring.Nodes[sequence&ring.BufferMask]

			if message.Trade.Timestamp > 0 {
				node.BucketSize = node.BucketSize + 1

				node.TradePrice = message.Trade.Price // TODO: Calculate mid-price
				node.TradeVolume += message.Trade.Size

				// Calculate turnover
				node.TurnOver = node.TurnOver + node.TradePrice*float64(message.Trade.Size)
			} else {
				node.AskPrice = message.Quote.AskPrice // TODO: Calculate mid-price
				node.AskVolume += message.Quote.AskSize
				node.BidPrice = message.Quote.BidPrice // TODO: Calculate mid-price
				node.BidVolume += message.Quote.BidSize
			}
		} else {
			// Notify volumeOrderImbalance buffer that we aggregated a message
			sequence = volumeWriter.GetSequence()
			volumeWriter.Commit(sequence, sequence)

			// Start collecting data for a new timestamp
			sequence = volumeWriter.Reserve(1)
			node = &ring.Nodes[sequence&ring.BufferMask]

			if message.Trade.Timestamp > 0 {
				node.BucketSize = 1

				node.Timestamp = message.Trade.Timestamp
				node.TradeVolume = message.Trade.Size
				node.TradePrice = message.Trade.Price

				// Calculate turnover
				node.TurnOver = node.TradePrice * float64(node.TradeVolume)

				// Fill Quote gaps, AskPrice and BidPrice is 0
				previousNode = &ring.Nodes[(sequence-1)&ring.BufferMask]
				node.AskPrice = previousNode.AskPrice
				node.BidPrice = previousNode.BidPrice
			} else {
				node.BucketSize = 0

				node.Timestamp = message.Quote.Timestamp
				node.AskVolume = message.Quote.AskSize
				node.BidVolume = message.Quote.BidSize
				node.AskPrice = message.Quote.AskPrice
				node.BidPrice = message.Quote.BidPrice

				// Fill Trade gaps, TradePrice is 0
				previousNode = &ring.Nodes[(sequence-1)&ring.BufferMask]
				node.TradePrice = previousNode.TradePrice
			}
		}
	}

	ticksCounter.Set(uint64(upper))
}

type volumeOrderImbalanceConsumer struct{}

// Consume ...
func (consumer volumeOrderImbalanceConsumer) Consume(lower, upper int64) {
	var node, previousNode *onering.AssetNode
	var deltaAskVolume, deltaBidVolume int64

	ring := volumeOrderImbalance["SPY"]

	for ; lower <= upper; lower++ {
		node = &ring.Nodes[lower&ring.BufferMask]

		// Volume Order Imbalance
		previousNode = &ring.Nodes[(lower-1)&ring.BufferMask]

		if node.BidPrice < previousNode.BidPrice {
			deltaBidVolume = 0
		} else if node.BidPrice == previousNode.BidPrice {
			deltaBidVolume = node.BidVolume - previousNode.BidVolume
		} else {
			deltaBidVolume = node.BidVolume
		}

		if node.AskPrice < previousNode.AskPrice {
			deltaAskVolume = node.AskVolume
		} else if node.BidPrice == previousNode.BidPrice {
			deltaAskVolume = node.AskVolume - previousNode.AskVolume
		} else {
			deltaAskVolume = 0
		}

		node.VolumeOrderImbalance = deltaBidVolume - deltaAskVolume

		// Order Imbalance Ratio
		if node.BidVolume+node.AskVolume > 0 {
			node.OrderImbalanceRatio = float64(node.BidVolume-node.AskVolume) /
				float64(node.BidVolume+node.AskVolume)
		}

		// Average Trade Price
		if node.TradeVolume != previousNode.TradeVolume {
			node.AverageTradePrice = (node.TurnOver - previousNode.TurnOver) /
				float64(node.TradeVolume-previousNode.TradeVolume)
		} else {
			node.AverageTradePrice = previousNode.AverageTradePrice
		}

		node.MidPriceBasis = node.AverageTradePrice -
			(previousNode.AverageTradePrice+node.AverageTradePrice)/2

		// e, _ := json.Marshal(node)
		// fmt.Println(string(e))
		// log.Print(node.Timestamp, node.TradePrice, node.VolumeOrderImbalance, node.OrderImbalanceRatio, node.MidPriceBasis)
	}
}
