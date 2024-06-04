package http

import (
	"sync/atomic"

	"github.com/olebeck/fast"
)

type Stats struct {
	RequestsSent    atomic.Int64
	Errors          atomic.Int64
	TotalConnects   atomic.Int64
	OpenConnections atomic.Int64
	Retries         atomic.Int64
	LastErr         error
	RequestTimeMS   *fast.MovingAverage
}
