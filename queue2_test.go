package fast_test

import (
	"fmt"
	"os"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/olebeck/fast"
)

func BenchmarkPushClientToServer(b *testing.B) {
	// Setup server
	server, err := fast.NewPushServer("localhost:0")
	if err != nil {
		b.Fatalf("failed to start server: %v", err)
	}
	defer server.Close() // Ensure server is closed after benchmark

	go server.Run()
	go func() {
		for range server.Push {
		}
	}()

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
	client, err := fast.NewPushClient("push://"+server.Address(), uuid.New())
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
	server, err := fast.NewPushServer("localhost:0")
	if err != nil {
		b.Fatalf("failed to start server: %v", err)
	}
	defer server.Close() // Ensure server is closed after benchmark

	go server.Run()
	go func() {
		for range server.Push {
		}
	}()

	// Prepare sample data to push
	data := []byte("test data")

	client, err := fast.NewPushClient("push://"+server.Address(), uuid.New())
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
	server, err := fast.NewPushServer("localhost:1345")
	if err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer server.Close() // Ensure server is closed after benchmark

	var lastCount int
	var count int
	go func() {
		t := time.NewTicker(1 * time.Second)
		for range t.C {
			fmt.Printf("%d/s\n", count-lastCount)
			lastCount = count
		}
	}()

	go func() {
		for range server.Push {
			count++
		}
	}()

	server.Run()
}

func TestClient(t *testing.T) {
	data := []byte("test data")

	client, err := fast.NewPushClient("push://localhost:1345", uuid.New())
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
