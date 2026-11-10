package tunnel

import (
	"bufio"
	"context"
	pb "github.com/adal-cloud/adal-cli/internal/tunnelpb"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestInformational103(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Link", "</style.css>; rel=preload")
		w.WriteHeader(103)
		w.WriteHeader(201)
	}))
	defer server.Close()
	s := snapshot(server.URL)
	frames, err := perform(t, s, opening(s, "GET", "/", ""), true)
	if err != nil {
		t.Fatal(err)
	}
	info, final := 0, 0
	for _, f := range frames {
		if f.GetInformational() != nil && f.GetInformational().StatusCode == 103 {
			info++
		}
		if f.GetResponseStart() != nil {
			final++
		}
	}
	if info != 1 || final != 1 {
		t.Fatal(info, final)
	}
}
func TestExpectContinue(t *testing.T) {
	got := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		got <- string(raw)
		w.WriteHeader(202)
	}))
	defer server.Close()
	s := snapshot(server.URL)
	open := opening(s, "POST", "/", "")
	n := int64(4)
	open.GetOpen().Request.ContentLength = &n
	open.GetOpen().Request.Headers = []*pb.Header{{Name: "Expect", RawValues: [][]byte{[]byte("100-continue")}}}
	x, err := NewExecutor().Start(t.Context(), s, open)
	if err != nil {
		t.Fatal(err)
	}
	defer x.Cancel()
	c := &capture{ack: make(chan struct{})}
	continued := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- x.Run(func(f *pb.ExecuteFrame) error {
			if f.GetInformational() != nil && f.GetInformational().StatusCode == 100 {
				continued <- struct{}{}
			}
			return c.emit(f)
		})
	}()
	select {
	case <-continued:
	case <-time.After(2 * time.Second):
		t.Fatal("no 100 Continue")
	}
	f := x.frame()
	f.Payload = &pb.ExecuteFrame_RequestData{RequestData: &pb.Data{SequenceNo: 1, Data: []byte("data")}}
	if err = x.Feed(f); err != nil {
		t.Fatal(err)
	}
	if err = x.Feed(input(open, &pb.End{Complete: true})); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if <-got != "data" {
		t.Fatal("body changed")
	}
}
func TestNoHiddenHTTPRetry(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var accepted, requests atomic.Int32
	stop := make(chan struct{})
	go func() {
		defer close(stop)
		for {
			c, err := listener.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			go func() {
				defer c.Close()
				r, err := http.ReadRequest(bufio.NewReader(c))
				if err != nil {
					return
				}
				_, _ = io.Copy(io.Discard, r.Body)
				requests.Add(1)
				if requests.Load() == 1 {
					_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
				}
			}()
		}
	}()
	s := snapshot("http://" + listener.Addr().String())
	open := opening(s, "GET", "/", "")
	zero := int64(0)
	open.GetOpen().Request.ContentLength = &zero
	if _, err = perform(t, s, open, true); err != nil {
		t.Fatal(err)
	}
	if _, err = perform(t, s, open, true); err == nil {
		t.Fatal("broken response succeeded")
	}
	if requests.Load() != 2 || accepted.Load() != 2 {
		t.Fatal("HTTP was retried", requests.Load(), accepted.Load())
	}
	listener.Close()
	<-stop
}
func TestHTTPForwardingAndNoRedirect(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			t.Error("redirect followed")
		}
		if r.Header.Get("X-Forwarded-For") != "192.0.2.1" || r.Header.Get("X-Secret-Hop") != "" || r.Header.Get("Authorization") != "Bearer application" || r.Header.Get("Accept-Encoding") != "" {
			t.Error("forwarding changed")
		}
		if strings.Contains(r.Header.Get("Forwarded"), "spoof") {
			t.Error("spoof retained")
		}
		w.Header().Set("Location", "/other")
		w.WriteHeader(302)
	}))
	defer server.Close()
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	s := snapshot(server.URL)
	open := opening(s, "GET", "/", "")
	open.GetOpen().Request.Headers = []*pb.Header{{Name: "Connection", RawValues: [][]byte{[]byte("X-Secret-Hop")}}, {Name: "X-Secret-Hop", RawValues: [][]byte{[]byte("private")}}, {Name: "Forwarded", RawValues: [][]byte{[]byte("spoof")}}, {Name: "Authorization", RawValues: [][]byte{[]byte("Bearer application")}}}
	frames, err := perform(t, s, open, true)
	if err != nil {
		t.Fatal(err)
	}
	m, _ := result(frames)
	if m == nil || m.StatusCode != 302 {
		t.Fatal(m)
	}
}
func TestCancellationReachesEndpoint(t *testing.T) {
	entered := make(chan struct{})
	cancelled := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-r.Context().Done(); close(cancelled) }))
	defer server.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	s := snapshot(server.URL)
	open := opening(s, "GET", "/", "")
	zero := int64(0)
	open.GetOpen().Request.ContentLength = &zero
	x, err := NewExecutor().Start(ctx, s, open)
	if err != nil {
		t.Fatal(err)
	}
	defer x.Cancel()
	done := make(chan error, 1)
	go func() { done <- x.Run(func(*pb.ExecuteFrame) error { return nil }) }()
	<-entered
	cancel()
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("endpoint not cancelled")
	}
	<-done
}
