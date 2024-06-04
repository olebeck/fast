package http

import "fmt"

type StatusError struct {
	i      int
	url    string
	status int
}

func (s StatusError) Status() int {
	return s.status
}

func (s StatusError) Error() string {
	return fmt.Sprintf("%d status: %d", s.i, s.status)
}
