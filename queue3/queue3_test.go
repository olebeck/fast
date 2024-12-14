package queue3_test

import (
	"fmt"
	"os"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/olebeck/fast/queue3"
)

func BenchmarkPushClientToServer(b *testing.B) {
	// Setup server
	server := queue3.NewServer("pw")
	err := server.Listen("localhost:0")
	if err != nil {
		b.Fatal(err)
	}

	go server.Process(func(sessionID uuid.UUID, data []byte) {

	})

	// Prepare sample data to push
	data := []byte("test data")

	f, _ := os.Create("cpu.pprof")
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	defer func() {
		f, _ := os.Create("mem.pprof")
		pprof.WriteHeapProfile(f)
	}()

	// Setup client
	client, err := queue3.NewClient("ws://pw@"+server.Address(), uuid.New())
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := client.Push(data); err != nil {
			b.Errorf("failed to push data: %v", err)
		}
	}
}

func TestPushClientToServer(b *testing.T) {
	// Setup server
	server := queue3.NewServer("pw")
	err := server.Listen("localhost:0")
	if err != nil {
		b.Fatal(err)
	}

	go server.Process(func(sessionID uuid.UUID, data []byte) {

	})

	// Prepare sample data to push
	data := []byte("test data")

	client, err := queue3.NewClient("ws://pw@"+server.Address(), uuid.New())
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < 300; i++ {
		err := client.Push(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestServerThroughput(t *testing.T) {
	server := queue3.NewServer("pw")
	err := server.Listen("localhost:1345")
	if err != nil {
		t.Fatal(err)
	}

	var lastCount int
	var count int
	go func() {
		t := time.NewTicker(1 * time.Second)
		for range t.C {
			fmt.Printf("%d/s\n", count-lastCount)
			lastCount = count
		}
	}()

	server.Process(func(sessionID uuid.UUID, data []byte) {
		count++
	})
}

func TestClient(t *testing.T) {
	data := []byte("test data")

	client, err := queue3.NewClient("ws://pw@localhost:1345", uuid.New())
	if err != nil {
		t.Fatal(err)
	}

	for {
		err := client.Push(data)
		if err != nil {
			t.Fatal(err)
		}
	}
}
