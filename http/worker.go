package http

import (
	"context"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
)

func DoHTTPWork[TW any](input chan TW, client *Client, parallel int, workFunc func(work TW, hc **Conn), doneFunc func()) {
	var wg sync.WaitGroup
	sem := semaphore.NewWeighted(int64(parallel))

	start := time.Now()
	rampupDone := false

	for work := range input {
		wg.Add(1)
		sem.Acquire(context.Background(), 1)
		hc := client.GetConn()

		// throttle startup
		if hc == nil && !rampupDone && parallel > 10 {
			tt := time.Since(start)
			if tt > 30*time.Second {
				rampupDone = true
			} else {
				ttt := (30 * time.Second) / time.Duration(parallel)
				time.Sleep(ttt)
			}
		}

		go func(work TW, hc *Conn) {
			defer wg.Done()
			defer sem.Release(1)
			defer client.ReturnConn(&hc)
			workFunc(work, &hc)
		}(work, hc)
	}

	wg.Wait()
	if doneFunc != nil {
		doneFunc()
	}
}
