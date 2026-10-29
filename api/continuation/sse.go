/*
Copyright 2026 The llm-d Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package continuation

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
)

const (
	defaultSSEInitialBufSize = 4096
	defaultMaxSSEEventBytes  = 8 << 20 // 8 MiB safety cap per SSE event
)

// SSEEvent represents a single parsed Server-Sent Event frame.
type SSEEvent struct {
	Event string
	Data  []byte
	Done  bool
}

// SSEReader parses a Server-Sent Events byte stream frame by frame.
type SSEReader struct {
	br       *bufio.Reader
	maxBytes int
	eof      bool
}

// NewSSEReader returns a streaming SSEReader over r.
func NewSSEReader(r io.Reader) *SSEReader {
	return &SSEReader{
		br:       bufio.NewReaderSize(r, defaultSSEInitialBufSize),
		maxBytes: defaultMaxSSEEventBytes,
	}
}

// WithMaxEventBytes overrides the per-event payload size limit.
func (sr *SSEReader) WithMaxEventBytes(maxBytes int) *SSEReader {
	if maxBytes > 0 {
		sr.maxBytes = maxBytes
	}
	return sr
}

// Next reads and returns the next SSEEvent. It returns io.EOF when the stream
// closes cleanly with no remaining buffered event data.
func (sr *SSEReader) Next() (SSEEvent, error) {
	if sr.eof {
		return SSEEvent{}, io.EOF
	}

	var eventType string
	var dataBuf bytes.Buffer
	hasDataField := false

	for {
		line, err := sr.readLine()
		if err != nil {
			if errors.Is(err, io.EOF) {
				sr.eof = true
				if len(line) > 0 {
					if perr := sr.processLine(line, &eventType, &dataBuf, &hasDataField); perr != nil {
						return SSEEvent{}, perr
					}
				}
				if hasDataField || eventType != "" {
					return buildSSEEvent(eventType, dataBuf.Bytes()), nil
				}
				return SSEEvent{}, io.EOF
			}
			return SSEEvent{}, fmt.Errorf("read sse line: %w", err)
		}

		// Blank line dispatches the accumulated event if any fields were seen.
		if len(line) == 0 {
			if hasDataField || eventType != "" {
				return buildSSEEvent(eventType, dataBuf.Bytes()), nil
			}
			continue
		}

		if err := sr.processLine(line, &eventType, &dataBuf, &hasDataField); err != nil {
			return SSEEvent{}, err
		}
	}
}

func buildSSEEvent(eventType string, rawData []byte) SSEEvent {
	trimmed := bytes.TrimSpace(rawData)
	if bytes.Equal(trimmed, []byte("[DONE]")) {
		return SSEEvent{Event: eventType, Data: []byte("[DONE]"), Done: true}
	}
	cp := make([]byte, len(rawData))
	copy(cp, rawData)
	return SSEEvent{Event: eventType, Data: cp}
}

func (sr *SSEReader) processLine(line []byte, eventType *string, dataBuf *bytes.Buffer, hasDataField *bool) error {
	// Leading colon is an SSE comment/keepalive.
	if line[0] == ':' {
		return nil
	}

	field := line
	var value []byte
	if idx := bytes.IndexByte(line, ':'); idx >= 0 {
		field = line[:idx]
		value = line[idx+1:]
		if len(value) > 0 && value[0] == ' ' {
			value = value[1:]
		}
	}

	switch string(field) {
	case "event":
		*eventType = string(value)
	case "data":
		if *hasDataField {
			dataBuf.WriteByte('\n')
		}
		if sr.maxBytes > 0 && dataBuf.Len()+len(value) > sr.maxBytes {
			return fmt.Errorf("sse event data exceeds max size of %d bytes", sr.maxBytes)
		}
		dataBuf.Write(value)
		*hasDataField = true
	default:
		// Ignore id:, retry:, and unknown fields per SSE specification.
	}
	return nil
}

// readLine reads a single \n- or \r\n-terminated line without imposing
// bufio.Scanner's 64 KiB line limit.
func (sr *SSEReader) readLine() ([]byte, error) {
	var full []byte
	for {
		frag, isPrefix, err := sr.br.ReadLine()
		if len(frag) > 0 {
			if !isPrefix && len(full) == 0 {
				if err != nil {
					return frag, fmt.Errorf("read line: %w", err)
				}
				return frag, nil
			}
			full = append(full, frag...)
			if sr.maxBytes > 0 && len(full) > sr.maxBytes {
				return nil, fmt.Errorf("sse line exceeds max size of %d bytes", sr.maxBytes)
			}
		}
		if err != nil {
			return full, fmt.Errorf("read line: %w", err)
		}
		if !isPrefix {
			return full, nil
		}
	}
}
