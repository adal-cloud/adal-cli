package tunnel

import (
	"context"
	pb "github.com/adal-cloud/adal-cli/internal/tunnelpb"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestImmutableSnapshotAndAdmission(t *testing.T) {
	called := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called <- r.URL.Path; _, _ = io.WriteString(w, "old") }))
	defer server.Close()
	s := snapshot(server.URL + "/old")
	open := opening(s, "GET", "/", "")
	zero := int64(0)
	open.GetOpen().Request.ContentLength = &zero
	e := NewExecutor()
	x, err := e.Start(t.Context(), s, open)
	if err != nil {
		t.Fatal(err)
	}
	defer x.Cancel()
	if _, err = e.Start(t.Context(), s, open); err == nil {
		t.Fatal("duplicate request accepted")
	}
	s.EndpointUrl = "http://127.0.0.1:1/new"
	open.GetOpen().Request.RawPath = []byte("/changed")
	if err = x.Run(func(*pb.ExecuteFrame) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if <-called != "/old/" {
		t.Fatal("mutable snapshot used")
	}
}
func TestInvalidSnapshotAndOpenDoNotCall(t *testing.T) {
	for _, change := range []func(*pb.ConfigSnapshot, *pb.ExecuteFrame){
		func(s *pb.ConfigSnapshot, f *pb.ExecuteFrame) { s.ConfigHash[0] ^= 1 },
		func(s *pb.ConfigSnapshot, f *pb.ExecuteFrame) { f.GetOpen().OwnerId = requestID },
		func(s *pb.ConfigSnapshot, f *pb.ExecuteFrame) { f.GetOpen().Request.RawPath = []byte("/bad%xy") },
		func(s *pb.ConfigSnapshot, f *pb.ExecuteFrame) {
			f.GetOpen().Request.Headers = []*pb.Header{{Name: "X-Test", RawValues: [][]byte{[]byte("bad\r\nInjected: yes")}}}
		},
	} {
		s := snapshot("http://127.0.0.1:1")
		f := opening(s, "GET", "/", "")
		change(s, f)
		if _, err := NewExecutor().Start(t.Context(), s, f); err == nil {
			t.Fatal("invalid input admitted")
		}
	}
}
func TestHTTPDuplexTrailersAndUserAgent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil || string(body) != "upload" {
			t.Error("upload changed", err)
		}
		if r.Trailer.Get("X-End") != "finished" {
			t.Error("request trailer missing")
		}
		ua := r.Header.Values("User-Agent")
		if len(ua) != 2 || ua[0] != "first" || ua[1] != "second" {
			t.Error("User-Agent changed", ua)
		}
		w.Header().Set("Trailer", "X-End")
		_, _ = io.WriteString(w, "download")
		w.Header().Set("X-End", "response")
	}))
	defer server.Close()
	s := snapshot(server.URL)
	f := opening(s, "POST", "/", "")
	f.GetOpen().Request.Headers = []*pb.Header{{Name: "User-Agent", RawValues: [][]byte{[]byte("first"), []byte("second")}}}
	x, err := NewExecutor().Start(t.Context(), s, f)
	if err != nil {
		t.Fatal(err)
	}
	defer x.Cancel()
	capture := &capture{ack: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- x.Run(capture.emit) }()
	<-capture.ack
	data := x.frame()
	data.Payload = &pb.ExecuteFrame_RequestData{RequestData: &pb.Data{SequenceNo: 1, Data: []byte("upload")}}
	if err = x.Feed(data); err != nil {
		t.Fatal(err)
	}
	if err = x.Feed(input(f, &pb.End{Complete: true, Trailers: []*pb.Header{{Name: "X-End", RawValues: [][]byte{[]byte("finished")}}}})); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("exchange stalled")
	}
	found := false
	for _, frame := range capture.frames {
		if end := frame.GetResponseEnd(); end != nil {
			for _, h := range end.Trailers {
				if h.Name == "X-End" && string(h.RawValues[0]) == "response" {
					found = true
				}
			}
		}
	}
	if !found {
		t.Fatal("response trailer missing")
	}
}
func TestCancelledBeforeRunCannotStartHTTP(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	s := snapshot("http://127.0.0.1:1")
	x, err := NewExecutor().Start(ctx, s, opening(s, "GET", "/", ""))
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	defer x.Cancel()
	if err = x.Run(func(*pb.ExecuteFrame) error { return nil }); err == nil {
		t.Fatal("cancelled exchange succeeded")
	}
}
