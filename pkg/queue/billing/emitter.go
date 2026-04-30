/*
Copyright 2026 The Knative Authors

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

// Package billing emits compact per-invocation billing samples from the
// queue-proxy hot path to a local node daemon.
package billing

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	packetVersion = "v1"

	// DefaultPort is the UDP port where the node-local Billet daemon listens.
	DefaultPort = 15020

	// DefaultQueueDepth bounds memory and guarantees request handling drops
	// samples instead of waiting for the UDP writer.
	DefaultQueueDepth = 1024
)

// ErrDisabled indicates that billing cannot be enabled from the supplied
// configuration. Queue-proxy should continue serving normally in this case.
var ErrDisabled = errors.New("billing emitter disabled")

// Metadata identifies a function invocation stream.
type Metadata struct {
	UserID string
	AppID  string
	FuncID string
}

// Valid reports whether the metadata is complete enough to emit samples.
func (m Metadata) Valid() bool {
	return m.UserID != "" && m.AppID != "" && m.FuncID != ""
}

type invocationSample struct {
	StartUnixNano int64
	EndUnixNano   int64
}

// Emitter owns the non-blocking queue and background UDP writer.
type Emitter struct {
	metadata Metadata
	writer   io.WriteCloser
	samples  chan invocationSample
	done     chan struct{}
	closed   atomic.Bool
	once     sync.Once
}

// NewUDPEmitter creates an emitter that writes to nodeIP:port over UDP.
func NewUDPEmitter(metadata Metadata, nodeIP string, port int, queueDepth int) (*Emitter, error) {
	if !metadata.Valid() || nodeIP == "" {
		return nil, fmt.Errorf("%w: USER_ID, APP_ID, FUNC_ID, and NODE_IP are required", ErrDisabled)
	}
	if port <= 0 {
		port = DefaultPort
	}

	conn, err := net.Dial("udp", net.JoinHostPort(nodeIP, strconv.Itoa(port)))
	if err != nil {
		return nil, err
	}

	return newEmitter(metadata, conn, queueDepth), nil
}

func newEmitter(metadata Metadata, writer io.WriteCloser, queueDepth int) *Emitter {
	if queueDepth <= 0 {
		queueDepth = DefaultQueueDepth
	}

	e := &Emitter{
		metadata: metadata,
		writer:   writer,
		samples:  make(chan invocationSample, queueDepth),
		done:     make(chan struct{}),
	}
	go e.writeLoop()
	return e
}

// EmitInvocation queues one invocation timeline sample for background writing.
// It returns false when the sample was dropped because the queue is full,
// closed, or invalid.
func (e *Emitter) EmitInvocation(startUnixNano, endUnixNano int64) bool {
	if e == nil || e.closed.Load() {
		return false
	}
	if startUnixNano < 0 || endUnixNano < startUnixNano {
		return false
	}

	sample := invocationSample{
		StartUnixNano: startUnixNano,
		EndUnixNano:   endUnixNano,
	}
	select {
	case e.samples <- sample:
		return true
	default:
		return false
	}
}

// Close stops the writer loop and closes the underlying UDP connection.
func (e *Emitter) Close() error {
	if e == nil {
		return nil
	}

	var err error
	e.once.Do(func() {
		e.closed.Store(true)
		close(e.done)
		err = e.writer.Close()
	})
	return err
}

func (e *Emitter) writeLoop() {
	for {
		select {
		case <-e.done:
			return
		case sample := <-e.samples:
			if e.closed.Load() {
				continue
			}
			_ = e.write(sample)
		}
	}
}

func (e *Emitter) write(sample invocationSample) error {
	payload := e.serialize(sample)
	_, err := e.writer.Write(payload)
	return err
}

func (e *Emitter) serialize(sample invocationSample) []byte {
	// Hash IDs are injected from trace metadata and are expected not to contain
	// semicolons or newlines. Keeping the protocol unescaped keeps the queue
	// proxy side as small as possible.
	size := len(packetVersion) + 5 + len(e.metadata.UserID) + len(e.metadata.AppID) + len(e.metadata.FuncID) + 40
	payload := make([]byte, 0, size)
	payload = append(payload, packetVersion...)
	payload = append(payload, ';')
	payload = append(payload, e.metadata.UserID...)
	payload = append(payload, ';')
	payload = append(payload, e.metadata.AppID...)
	payload = append(payload, ';')
	payload = append(payload, e.metadata.FuncID...)
	payload = append(payload, ';')
	payload = strconv.AppendInt(payload, sample.StartUnixNano, 10)
	payload = append(payload, ';')
	payload = strconv.AppendInt(payload, sample.EndUnixNano, 10)
	return payload
}

// Handler wraps the proxied request and records turnaround after the inner
// handler returns. A nil emitter leaves the handler chain unchanged.
func Handler(next http.Handler, emitter *Emitter) http.Handler {
	if emitter == nil {
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		startUnixNano := start.UnixNano()
		defer func() {
			durationNano := time.Since(start).Nanoseconds()
			if durationNano < 0 {
				durationNano = 0
			}
			emitter.EmitInvocation(startUnixNano, startUnixNano+durationNano)
		}()

		next.ServeHTTP(w, r)
	})
}
