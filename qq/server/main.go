package main

import (
	"net/http"

	"github.com/olebeck/fast/qq"
	"github.com/sirupsen/logrus"
)

func main() {
	go http.ListenAndServe("127.0.0.1:3223", nil)
	server := qq.NewServer("0.0.0.0:4554", "password")
	logrus.Fatal(server.Listen())
}
