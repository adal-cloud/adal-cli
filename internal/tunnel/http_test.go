package tunnel

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	pb "github.com/adal-cloud/adal-cli/internal/tunnelpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const owner = "11111111-1111-4111-8111-111111111111"
const tunnelID = "22222222-2222-4222-8222-222222222222"
const requestID = "33333333-3333-4333-8333-333333333333"
const sessionID = "44444444-4444-4444-8444-444444444444"
const controlID = "55555555-5555-4555-8555-555555555555"

func snapshot(endpoint string) *pb.ConfigSnapshot {
	s := &pb.ConfigSnapshot{EventId: requestID, TunnelId: tunnelID, OwnerId: owner, RegionId: owner, Address: "aaaaaaataaaaaaaa", Revision: 1, EndpointUrl: endpoint, TimeoutSeconds: 30, MaxConcurrentRequests: 16, Enabled: true, SaveRequest: true, SaveResponse: true}
	hash := SnapshotHash(s)
	s.ConfigHash = hash[:]
	return s
}
func opening(s *pb.ConfigSnapshot, method, path, query string) *pb.ExecuteFrame {
	return &pb.ExecuteFrame{Version: 1, RequestId: requestID, SessionId: sessionID, ControlId: controlID, Payload: &pb.ExecuteFrame_Open{Open: &pb.Open{OwnerId: s.OwnerId, TunnelId: s.TunnelId, RegionId: s.RegionId, Revision: s.Revision, ConfigHash: s.ConfigHash, RemainingDuration: durationpb.New(5 * time.Second), Request: &pb.RequestMetadata{Method: method, Authority: "aaaaaaataaaaaaaa.se.adal.cloud", RawPath: []byte(path), RawQuery: []byte(query), Protocol: pb.HTTPVersion_HTTP_VERSION_1_1, PublicScheme: "https", SenderIp: "192.0.2.1"}}}}
}
func input(open *pb.ExecuteFrame, payload *pb.End) *pb.ExecuteFrame {
	return &pb.ExecuteFrame{Version: 1, RequestId: open.RequestId, SessionId: open.SessionId, ControlId: open.ControlId, Payload: &pb.ExecuteFrame_RequestEnd{RequestEnd: payload}}
}

type capture struct {
	mu     sync.Mutex
	frames []*pb.ExecuteFrame
	ack    chan struct{}
	once   sync.Once
}

func (c *capture) emit(f *pb.ExecuteFrame) error {
	c.mu.Lock()
	c.frames = append(c.frames, proto.Clone(f).(*pb.ExecuteFrame))
	c.mu.Unlock()
	if f.GetOpenAck() != nil {
		c.once.Do(func() { close(c.ack) })
	}
	return nil
}
func perform(t *testing.T, s *pb.ConfigSnapshot, open *pb.ExecuteFrame, finish bool) ([]*pb.ExecuteFrame, error) {
	return performExecutor(t, NewExecutor(), s, open, finish)
}
func performExecutor(t *testing.T, e *Executor, s *pb.ConfigSnapshot, open *pb.ExecuteFrame, finish bool) ([]*pb.ExecuteFrame, error) {
	t.Helper()
	x, err := e.Start(t.Context(), s, open)
	if err != nil {
		return nil, err
	}
	t.Cleanup(x.Cancel)
	c := &capture{ack: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- x.Run(c.emit) }()
	select {
	case <-c.ack:
	case <-time.After(time.Second):
		t.Fatal("OpenAck missing")
	}
	if finish {
		if err := x.Feed(input(open, &pb.End{Complete: true})); err != nil && !errors.Is(err, ErrClosed) {
			t.Fatal(err)
		}
	}
	select {
	case err := <-done:
		return c.frames, err
	case <-time.After(6 * time.Second):
		t.Fatal("exchange did not finish")
		return nil, nil
	}
}
func result(frames []*pb.ExecuteFrame) (*pb.ResponseMetadata, []byte) {
	var start *pb.ResponseMetadata
	var body []byte
	for _, f := range frames {
		if f.GetResponseStart() != nil {
			start = f.GetResponseStart()
		}
		if f.GetResponseData() != nil {
			body = append(body, f.GetResponseData().Data...)
		}
	}
	return start, body
}

func TestHTTPRawPathQuery(t *testing.T) {
	got := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { got <- r.RequestURI; _, _ = io.WriteString(w, "ok") }))
	defer server.Close()
	s := snapshot(server.URL + "/base%2Fpath/")
	frames, err := perform(t, s, opening(s, "GET", "//a%2Fb/../c", "x=1&x=&z=%ff"), true)
	if err != nil {
		t.Fatal(err)
	}
	start, body := result(frames)
	if start == nil || start.StatusCode != 200 || string(body) != "ok" {
		t.Fatal(start, string(body))
	}
	if uri := <-got; uri != "/base%2Fpath//a%2Fb/../c?x=1&x=&z=%ff" {
		t.Fatal(uri)
	}
}
func TestHTTPGzipNoDecode(t *testing.T) {
	var compressed bytes.Buffer
	zip := gzip.NewWriter(&compressed)
	_, _ = zip.Write([]byte("compressible body"))
	_ = zip.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		_, _ = w.Write(compressed.Bytes())
	}))
	defer server.Close()
	s := snapshot(server.URL)
	frames, err := perform(t, s, opening(s, "GET", "/", ""), true)
	_, body := result(frames)
	if err != nil || !bytes.Equal(body, compressed.Bytes()) {
		t.Fatal("compressed bytes changed", err)
	}
}
func TestHTTPHeaderBytes(t *testing.T) {
	got := make(chan []string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- r.Header.Values("X-Bytes")
		w.Header()["X-Bytes"] = []string{string([]byte{0xff, 0x80}), "second"}
		w.WriteHeader(201)
	}))
	defer server.Close()
	s := snapshot(server.URL)
	open := opening(s, "GET", "/", "")
	open.GetOpen().Request.Headers = []*pb.Header{{Name: "X-Bytes", RawValues: [][]byte{{0xff, 0x80}, []byte("second")}}}
	frames, err := perform(t, s, open, true)
	if err != nil {
		t.Fatal(err)
	}
	values := <-got
	if len(values) != 2 || values[0] != string([]byte{0xff, 0x80}) || values[1] != "second" {
		t.Fatal("request headers changed")
	}
	start, _ := result(frames)
	found := false
	for _, h := range start.Headers {
		if h.Name == "X-Bytes" {
			found = len(h.RawValues) == 2 && bytes.Equal(h.RawValues[0], []byte{0xff, 0x80}) && string(h.RawValues[1]) == "second"
		}
	}
	if !found {
		t.Fatal("response headers changed")
	}
}
func TestEarlyResponseBeforeUploadEnd(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = http.NewResponseController(w).EnableFullDuplex()
		w.WriteHeader(413)
		_, _ = io.WriteString(w, "early")
	}))
	defer server.Close()
	s := snapshot(server.URL)
	frames, err := perform(t, s, opening(s, "POST", "/", ""), false)
	start, body := result(frames)
	if err != nil || start == nil || start.StatusCode != 413 || string(body) != "early" {
		t.Fatal(start, string(body), err)
	}
}
func TestOpenAckPrecedesHTTP(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	defer server.Close()
	s := snapshot(server.URL)
	x, err := NewExecutor().Start(t.Context(), s, opening(s, "GET", "/", ""))
	if err != nil {
		t.Fatal(err)
	}
	defer x.Cancel()
	if calls.Load() != 0 {
		t.Fatal("HTTP started before OpenAck")
	}
	err = x.Run(func(*pb.ExecuteFrame) error { return context.Canceled })
	if err == nil || calls.Load() != 0 {
		t.Fatal("failed OpenAck started HTTP", err)
	}
}
