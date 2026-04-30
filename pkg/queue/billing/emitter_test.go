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

package billing

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestEmitterWritesCompactPayload(t *testing.T) {
	writer := &captureWriter{writes: make(chan []byte, 1)}
	emitter := newEmitter(Metadata{UserID: "user", AppID: "app", FuncID: "func"}, writer, 1)
	defer emitter.Close()

	if ok := emitter.EmitInvocation(1000, 1150); !ok {
		t.Fatal("EmitInvocation returned false, want true")
	}

	select {
	case got := <-writer.writes:
		want := "v1;user;app;func;1000;1150"
		if string(got) != want {
			t.Fatalf("payload = %q, want %q", got, want)
		}
	case <-timeout():
		t.Fatal("timed out waiting for billing payload")
	}
}

func TestEmitterDropsWhenQueueIsFull(t *testing.T) {
	writer := &blockingWriter{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	emitter := newEmitter(Metadata{UserID: "user", AppID: "app", FuncID: "func"}, writer, 1)
	defer emitter.Close()
	defer close(writer.release)

	if ok := emitter.EmitInvocation(1000, 1001); !ok {
		t.Fatal("first EmitInvocation returned false, want true")
	}

	select {
	case <-writer.started:
	case <-timeout():
		t.Fatal("writer did not start")
	}

	if ok := emitter.EmitInvocation(1000, 1002); !ok {
		t.Fatal("second EmitInvocation returned false, want true")
	}
	if ok := emitter.EmitInvocation(1000, 1003); ok {
		t.Fatal("third EmitInvocation returned true, want false when queue is full")
	}
}

func TestEmitterRejectsInvalidTimeline(t *testing.T) {
	writer := &captureWriter{writes: make(chan []byte, 1)}
	emitter := newEmitter(Metadata{UserID: "user", AppID: "app", FuncID: "func"}, writer, 1)
	defer emitter.Close()

	if ok := emitter.EmitInvocation(2000, 1000); ok {
		t.Fatal("EmitInvocation returned true, want false for end before start")
	}
}

func TestNewUDPEmitterDisabledWithoutMetadata(t *testing.T) {
	_, err := NewUDPEmitter(Metadata{UserID: "user", AppID: "app"}, "127.0.0.1", DefaultPort, 1)
	if !errors.Is(err, ErrDisabled) {
		t.Fatalf("NewUDPEmitter error = %v, want ErrDisabled", err)
	}
}

func TestHandlerEmitsAfterRequest(t *testing.T) {
	writer := &captureWriter{writes: make(chan []byte, 1)}
	emitter := newEmitter(Metadata{UserID: "user", AppID: "app", FuncID: "func"}, writer, 1)
	defer emitter.Close()

	handler := Handler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}), emitter)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusAccepted)
	}

	select {
	case got := <-writer.writes:
		fields := strings.Split(string(got), ";")
		if len(fields) != 6 {
			t.Fatalf("payload fields = %v, want 6 fields", fields)
		}
		if strings.Join(fields[:4], ";") != "v1;user;app;func" {
			t.Fatalf("payload identity = %q, want v1;user;app;func", strings.Join(fields[:4], ";"))
		}
		start, err := strconv.ParseInt(fields[4], 10, 64)
		if err != nil {
			t.Fatalf("start timestamp parse error: %v", err)
		}
		end, err := strconv.ParseInt(fields[5], 10, 64)
		if err != nil {
			t.Fatalf("end timestamp parse error: %v", err)
		}
		if start <= 0 || end < start {
			t.Fatalf("timestamps start=%d end=%d, want positive start and end >= start", start, end)
		}
	case <-timeout():
		t.Fatal("timed out waiting for billing payload")
	}
}

func timeout() <-chan time.Time {
	return time.After(time.Second)
}

type captureWriter struct {
	writes chan []byte
}

func (w *captureWriter) Write(p []byte) (int, error) {
	cp := append([]byte(nil), p...)
	w.writes <- cp
	return len(p), nil
}

func (*captureWriter) Close() error {
	return nil
}

type blockingWriter struct {
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	w.once.Do(func() {
		close(w.started)
	})
	<-w.release
	return len(p), nil
}

func (*blockingWriter) Close() error {
	return nil
}
