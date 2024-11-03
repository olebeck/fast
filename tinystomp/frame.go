package tinystomp

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
	"unsafe"
)

// StompFrame represents a parsed STOMP frame.
type StompFrame struct {
	Command []byte
	Headers map[string]string
	Body    []byte
}

// UnsafeString converts a []byte to string without allocation.
func UnsafeString(b []byte) string {
	return unsafe.String(&b[0], len(b))
}

// ReadStompFrame reads and parses a STOMP frame from a buffered reader without unnecessary allocations.
func ReadStompFrame(reader *bufio.Reader) (*StompFrame, error) {
	frame := &StompFrame{
		Headers: make(map[string]string),
	}

	// Read command line (e.g., CONNECT, SEND, MESSAGE)
	command, err := reader.ReadSlice('\n')
	if err != nil {
		return nil, fmt.Errorf("failed to read command: %w", err)
	}
	frame.Command = bytes.TrimSpace(command)

	// Read headers
	var contentLength int = -1
	for {
		line, err := reader.ReadSlice('\n')
		if err != nil {
			return nil, fmt.Errorf("failed to read header: %w", err)
		}
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			break // Headers end with a blank line
		}

		splitIdx := bytes.IndexByte(line, ':')
		if splitIdx == -1 {
			return nil, fmt.Errorf("invalid header format: %s", line)
		}

		key := UnsafeString(line[:splitIdx])
		value := UnsafeString(line[splitIdx+1:])
		frame.Headers[key] = value

		if key == "content-length" {
			contentLength, err = strconv.Atoi(value)
			if err != nil {
				return nil, fmt.Errorf("invalid content-length value: %s", value)
			}
		}
	}

	// Read body using content-length
	if contentLength >= 0 {
		frame.Body = make([]byte, contentLength)
		_, err := io.ReadFull(reader, frame.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read body with content-length: %w", err)
		}

		// Ensure we skip the null byte (frame terminator)
		if term, _ := reader.ReadByte(); term != '\x00' {
			return nil, fmt.Errorf("expected frame terminator null byte, found: %v", term)
		}
	} else {
		// No content-length
		// read to 0x00
		var bodyBuf []byte
		for {
			b, err := reader.ReadByte()
			if err != nil {
				return nil, fmt.Errorf("failed to read body: %w", err)
			}
			if b == '\x00' {
				break
			}
			bodyBuf = append(bodyBuf, b)
		}
		frame.Body = bodyBuf
	}

	return frame, nil
}
