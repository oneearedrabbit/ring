package polygon

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

const (
	// EnvAPIKeyID ...
	EnvAPIKeyID = "<your_key>"
	// EnvAPISecretKey ...
	EnvAPISecretKey = "<your_secret>"
)

// StreamTrade is the structure that defines a trade that
// polygon transmits via websocket protocol.
type StreamTrade struct {
	Symbol    []byte  // `json:"sym"`
	Exchange  int     // `json:"x"`
	TradeID   []byte  // `json:"i"`
	Price     float64 // `json:"p"`
	Size      int64   // `json:"s"`
	Timestamp int64   // `json:"t"`
	// Conditions []int   // `json:"c"`
}

// StreamQuote is the structure that defines a quote that
// polygon transmits via websocket protocol.
type StreamQuote struct {
	Symbol      []byte  // `json:"sym"`
	Condition   int     // `json:"c"`
	BidExchange int     // `json:"bx"`
	AskExchange int     // `json:"ax"`
	BidPrice    float64 // `json:"bp"`
	AskPrice    float64 // `json:"ap"`
	BidSize     int64   // `json:"bs"`
	AskSize     int64   // `json:"as"`
	Timestamp   int64   // `json:"t"`
}

const (
	// Trades ...
	Trades = 84 // T
	// Quotes ...
	Quotes = 81 // Q
)

type handler func(msg []byte)

// Stream ...
type Stream struct {
	// sync.Mutex
	// sync.Once
	conn                  net.Conn
	authenticated, closed atomic.Value
	channel               string
	handler               handler
}

// Register ...
func Register(channel string, fn handler) {
	s := &Stream{
		authenticated: atomic.Value{},
		closed:        atomic.Value{},
		channel:       channel,
		handler:       fn,
	}

	s.authenticated.Store(false)
	s.closed.Store(false)

	s.OpenSocket()
	s.Auth()
	s.Subscribe()
	s.Start()
}

// Close ...
func (s *Stream) Close() error {
	// s.Lock()
	// defer s.Unlock()

	// if err := s.conn.WriteMessage(
	// 	websocket.CloseMessage,
	// 	websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
	// ); err != nil {
	// 	return err
	// }

	// so we know it was gracefully closed
	s.closed.Store(true)

	return s.conn.Close()
}

func (s *Stream) reconnect() {
	s.authenticated.Store(false)

	time.Sleep(time.Second) // gracefully wait on reconnect

	s.conn = s.OpenSocket()
	s.Auth()
	s.Subscribe()
}

func (s *Stream) handleError(err error) {
	s.reconnect()
}

// Start ...
func (s *Stream) Start() {
	var err error
	var message []byte

	// As little logic as possible in the reader loop:
	for {
		message, _, err = wsutil.ReadServerData(s.conn)
		if err != nil {
			s.handleError(err)
			continue
		}

		// Parsing happens in a separate go routine. Any processing
		// done in this loop increases the chances of disconnects due
		// to not consuming the data fast enough.
		s.handler(message)
	}
}

// Subscribe ...
func (s *Stream) Subscribe() (err error) {
	// s.Lock()
	// defer s.Unlock()

	err = wsutil.WriteClientMessage(s.conn, ws.OpText, []byte(fmt.Sprintf("{\"action\":\"subscribe\",\"params\":\"%s\"}", s.channel)))
	if err != nil {
		// handle error
	}

	return
}

func (s *Stream) isAuthenticated() bool {
	return s.authenticated.Load().(bool)
}

// Auth ...
func (s *Stream) Auth() (err error) {
	// s.Lock()
	// defer s.Unlock()

	// if s.isAuthenticated() {
	// 	return
	// }

	err = wsutil.WriteClientMessage(s.conn, ws.OpText, []byte(fmt.Sprintf("{\"action\":\"auth\",\"params\":\"%s\"}", EnvAPIKeyID)))
	if err != nil {
		// handle error
	}

	s.authenticated.Store(true)

	return
}

// OpenSocket ...
func (s *Stream) OpenSocket() net.Conn {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel() // test me

	c, _, _, err := ws.DefaultDialer.Dial(ctx, "wss://alpaca.socket.polygon.io/stocks")
	if err != nil {
		panic(err)
	}

	s.conn = c

	return c
}
