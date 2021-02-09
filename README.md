# Volume Order Imbalance Intraday Trading

This is a port of the ![LMAX
Disruptor](https://lmax-exchange.github.io/disruptor/disruptor.html)
into the Go programming language. In the spirit of LMAX pattern, this
is zero-allocation implementation. Once a ring buffer is initialized
there's no additional overhead which means it'll process messages at a
constant rate.

I'm not using Alpaca go library as it causes unnecessary allocations
during runtime. Instead, I'm relying `gobwas/ws` which does (almost)
zero allocations.

## Install

```
go test -v
go build -v
```

## Run

Live:
```
$ ./ring -assets SPY
```

Backfill:
```
$ ./ring -assets SPY -backfill output.log
```

## Benchmark

Disruptor:
```
$ go test -bench=.
goos: darwin
goarch: amd64
pkg: kruzenshtern.org/alpaca/ring/disruptor
BenchmarkSingleProducerSingleConsumer-4     	84466063	        13.9 ns/op	       0 B/op	       0 allocs/op
BenchmarkSingleProducerChainConsumers-4     	31671645	        38.9 ns/op	       0 B/op	       0 allocs/op
BenchmarkSingleProducerDiamondConsumers-4   	26620614	        39.5 ns/op	       0 B/op	       0 allocs/op
PASS
ok  	kruzenshtern.org/alpaca/ring/disruptor	3.681s
```

VOI calculations per ![Order Imbalance Based Strategy in
High Frequency
Trading](https://www.palmislandtraders.com/econ136/hftois.pdf) thesis:
```
$ go test -bench=.
goos: darwin
goarch: amd64
pkg: kruzenshtern.org/alpaca/ring
BenchmarkSingleProducerSingleConsumer-4   	 1657720	       705 ns/op	       0 B/op	       0 allocs/op
PASS
ok  	kruzenshtern.org/alpaca/ring	2.142s
```

I've been running the application for about a week in 2020 and I
didn't notice any visible bottlenecks. When it falls behind (e.g.
volume spike), it catches up momentarily.
