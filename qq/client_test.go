package qq_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/olebeck/fast/qq"
)

func TestPub(t *testing.T) {
	address := "127.0.0.1:5655"
	password := "password"

	server := qq.NewServer(address, password)
	client1 := qq.NewClient(address, password)
	client2 := qq.NewClient(address, password)

	go func() {
		err := server.Listen()
		if err != nil {
			t.Error(err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	err := client1.Connect()
	if err != nil {
		t.Fatal(err)
	}

	err = client2.Connect()
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	err = client1.Subscribe("queue", func(messageData []byte) {
		fmt.Printf("Received: %s\n", string(messageData))
		close(done)
	})
	if err != nil {
		t.Fatal(err)
	}

	err = client1.Publish("queue", []byte("testdata"))
	if err != nil {
		t.Fatal(err)
	}

	<-done
	client2.Close()
}
