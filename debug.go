package fast

import (
	"fmt"
	"net"
	"net/http"
	"runtime/debug"

	"github.com/sirupsen/logrus"
)

var Commit struct {
	Hash string
}

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" {
				Commit.Hash = setting.Value
			}
		}
	}
	if Commit.Hash == "" {
		Commit.Hash = "None"
	}
}

func StartPprof() {
	go func() {
		for port := 6060; port < 7000; port++ {
			listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
			if err != nil {
				continue
			}
			logrus.Debugf("Using Debug Port: %d", port)
			err = http.Serve(listener, nil)
			if err != nil {
				logrus.Fatal(err)
			}
		}
	}()
}
