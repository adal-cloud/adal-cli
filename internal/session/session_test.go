package session

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/adal-cloud/adal-cli/internal/deliver"
	"github.com/adal-cloud/adal-cli/internal/journal"
)

func TestHelloAndPing(t *testing.T) {
	const token = "s3cr3t-value"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pinged := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != sessionPath {
			t.Errorf("path = %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		defer conn.CloseNow()
		msg := readHello(t, conn)
		if msg.Type != "hello" || msg.Version != 1 || msg.Token != token {
			t.Errorf("hello = %+v", msg)
		}
		readDone := make(chan struct{})
		go func() {
			defer close(readDone)
			ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
			defer stop()
			_, _, _ = conn.Read(ctx)
		}()
		pingCtx, stopPing := context.WithTimeout(context.Background(), 2*time.Second)
		defer stopPing()
		if err := conn.Ping(pingCtx); err != nil {
			t.Errorf("ping: %v", err)
			return
		}
		close(pinged)
		<-readDone
	}))
	defer srv.Close()

	done := make(chan error, 1)
	go func() {
		done <- run(ctx, hostOf(srv), token, frameLimit, time.Second, nil, t.TempDir())
	}()

	select {
	case <-pinged:
	case <-time.After(2 * time.Second):
		t.Fatal("ping was not answered")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("session did not stop")
	}
}

func TestFatalCloseStopsWithoutReconnect(t *testing.T) {
	const token = "s3cr3t-value"
	cases := []struct {
		code websocket.StatusCode
		text string
	}{
		{code: 4001, text: "authentication failed"},
		{code: 4003, text: "access revoked"},
		{code: 4004, text: "token expired"},
		{code: 4008, text: "session replaced"},
	}
	for _, tc := range cases {
		t.Run(tc.text, func(t *testing.T) {
			var accepts atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				accepts.Add(1)
				conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
				if err != nil {
					return
				}
				defer conn.CloseNow()
				readHello(t, conn)
				_ = conn.Close(tc.code, "")
			}))
			defer srv.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			err := run(ctx, hostOf(srv), token, frameLimit, 0, nil, t.TempDir())
			if err == nil || err.Error() != tc.text {
				t.Fatalf("error = %v", err)
			}
			if strings.Contains(err.Error(), token) {
				t.Fatalf("token leaked into %q", err.Error())
			}
			if accepts.Load() != 1 {
				t.Fatalf("connections = %d, want 1", accepts.Load())
			}
		})
	}
}

func TestRetryableCloseReconnects(t *testing.T) {
	cases := []struct {
		name  string
		code  websocket.StatusCode
		pause time.Duration
	}{
		{name: "going away", code: websocket.StatusGoingAway, pause: time.Second},
		{name: "verification timeout", code: 4002, pause: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			const token = "s3cr3t-value"
			var hellos atomic.Int32
			var first time.Time
			second := make(chan time.Time, 1)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
				if err != nil {
					return
				}
				defer conn.CloseNow()
				readHello(t, conn)
				n := hellos.Add(1)
				if n == 1 {
					first = time.Now()
					_ = conn.Close(tc.code, "")
					return
				}
				second <- time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				_, _, _ = conn.Read(ctx)
			}))
			defer srv.Close()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				done <- run(ctx, hostOf(srv), token, frameLimit, tc.pause, nil, t.TempDir())
			}()

			select {
			case at := <-second:
				if tc.pause > 0 && at.Sub(first) < tc.pause {
					t.Fatalf("reconnect after %s, want at least %s", at.Sub(first), tc.pause)
				}
			case <-time.After(4 * time.Second):
				t.Fatal("no reconnect")
			}
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("session did not stop")
			}
		})
	}
}

func TestDialFailureReconnectsAfterOneSecond(t *testing.T) {
	const token = "s3cr3t-value"
	var hits atomic.Int32
	var first time.Time
	second := make(chan time.Time, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		if n == 1 {
			first = time.Now()
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		defer conn.CloseNow()
		readHello(t, conn)
		second <- time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _, _ = conn.Read(ctx)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, hostOf(srv), token, frameLimit, time.Second, nil, t.TempDir())
	}()

	select {
	case at := <-second:
		if at.Sub(first) < time.Second {
			t.Fatalf("reconnect after %s, want at least 1s", at.Sub(first))
		}
	case <-time.After(4 * time.Second):
		t.Fatal("no reconnect")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("session did not stop")
	}
}

func TestOversizedFrameReconnects(t *testing.T) {
	const token = "s3cr3t-value"
	second := make(chan struct{}, 1)
	var hellos atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		defer conn.CloseNow()
		readHello(t, conn)
		n := hellos.Add(1)
		if n == 1 {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = conn.Write(ctx, websocket.MessageText, bytesOf(200))
			return
		}
		second <- struct{}{}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _, _ = conn.Read(ctx)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, hostOf(srv), token, 32, 0, nil, t.TempDir())
	}()

	select {
	case <-second:
	case <-time.After(2 * time.Second):
		t.Fatal("oversized frame did not reconnect")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("session did not stop")
	}
}

func TestRaisedLimitAccepts40KiB(t *testing.T) {
	const token = "s3cr3t-value"
	var accepts atomic.Int32
	held := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if accepts.Add(1) > 1 {
			t.Errorf("reconnect on a frame inside the limit")
			http.Error(w, "unexpected", http.StatusTooManyRequests)
			return
		}
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		defer conn.CloseNow()
		readHello(t, conn)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := conn.Write(ctx, websocket.MessageText, bytesOf(40<<10)); err != nil {
			t.Errorf("write: %v", err)
			return
		}
		close(held)
		_, _, _ = conn.Read(context.Background())
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, hostOf(srv), token, frameLimit, time.Second, nil, t.TempDir())
	}()

	select {
	case <-held:
	case <-time.After(2 * time.Second):
		t.Fatal("frame was not accepted")
	}
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("session did not stop")
	}
	if accepts.Load() != 1 {
		t.Fatalf("connections = %d, want 1", accepts.Load())
	}
}

type helloFrame struct {
	Type    string `json:"type"`
	Version int    `json:"version"`
	Token   string `json:"token"`
}

func readHello(t *testing.T, conn *websocket.Conn) helloFrame {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	kind, data, err := conn.Read(ctx)
	if err != nil || kind != websocket.MessageText {
		t.Fatalf("hello read: kind %v err %v", kind, err)
	}
	var msg helloFrame
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("hello json: %v", err)
	}
	return msg
}

func hostOf(srv *httptest.Server) string {
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

func bytesOf(n int) []byte {
	body := make([]byte, n)
	for i := range body {
		body[i] = 'a'
	}
	return body
}

func TestAssignmentStartsWithoutAManagementFrame(t *testing.T) {
	const token = "s3cr3t-value"
	const requestID = "018f3c2e-7b1a-7c3d-8e4f-123456789abc"
	buf := &safeBuf{}
	logger := log.New(buf, "", log.LstdFlags)
	quiet := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		defer conn.CloseNow()
		msg := readHello(t, conn)
		if msg.Version != 1 || msg.Token != token {
			t.Errorf("hello = %+v", msg)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := conn.Write(ctx, websocket.MessageText, []byte(disabledAssignment(requestID))); err != nil {
			t.Errorf("assignment: %v", err)
			return
		}
		readCtx, stop := context.WithTimeout(context.Background(), 150*time.Millisecond)
		defer stop()
		_, data, err := conn.Read(readCtx)
		if err != nil || !strings.Contains(string(data), "assignment_ack") {
			t.Errorf("assignment acknowledgement = %s, error %v", data, err)
		}
		close(quiet)
		_, _, _ = conn.Read(context.Background())
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, hostOf(srv), token, frameLimit, time.Second, logger, t.TempDir())
	}()
	select {
	case <-quiet:
	case <-time.After(2 * time.Second):
		t.Fatal("assignment was not delivered")
	}
	text := waitLog(t, buf, "Assignment has no enabled endpoint\n"+logField("request", requestID))
	if !strings.Contains(text, "Assignment received\n"+logField("request", requestID)) {
		t.Fatalf("log = %q", text)
	}
	for _, line := range []string{"sync finished", "session ready", "assignment acknowledged", "ack resent", "sync_end", `"type":"ack"`} {
		if strings.Contains(text, line) {
			t.Fatalf("log = %q", text)
		}
	}
	if strings.Contains(text, token) || strings.Contains(text, "http://127.0.0.1:3000") || strings.Contains(text, "YQ==") {
		t.Fatalf("frame leaked into %q", text)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("session did not stop")
	}
}

func TestUnknownAssignmentIsDropped(t *testing.T) {
	const requestID = "018f3c2e-7b1a-7c3d-8e4f-123456789abc"
	book := journal.New()
	link := newPipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = serve(ctx, link, book, nil, func(string) {})
	}()
	link.in <- []byte(enabledAssignment(requestID))
	assertAssignmentACK(t, link)
	waitUntil(t, func() bool { return book.Contains(requestID) })
	link.in <- []byte(`{"type":"error","version":3,"code":"unknown_assignment","request_id":"` + requestID + `"}`)
	waitUntil(t, func() bool { return !book.Contains(requestID) })
	if _, ok := book.Arm(requestID); ok {
		t.Fatal("unknown assignment was armed")
	}
	select {
	case data := <-link.out:
		t.Fatalf("frame after drop = %s", data)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
}

func TestIgnoredAssignmentIsNotAnswered(t *testing.T) {
	const token = "s3cr3t-value"
	const requestID = "018f3c2e-7b1a-7c3d-8e4f-123456789abc"
	book := journal.New()
	link := newPipe()
	var mu sync.Mutex
	var logs []string
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = serve(ctx, link, book, nil, func(message string) {
			mu.Lock()
			logs = append(logs, message)
			mu.Unlock()
		})
	}()
	link.in <- []byte(`{"type":"assignment","token":"` + token + `","url":"http://127.0.0.1:9/hook"}`)
	link.in <- []byte("{")
	link.in <- []byte(`{"type":"note"}`)
	link.in <- []byte(`{"type":"sync","version":3,"operation_id":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}`)
	link.in <- []byte(`{"type":"ready","version":3}`)
	link.in <- []byte(`{"type":"region_ready","version":3}`)
	waitUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		text := strings.Join(logs, "\n")
		return strings.Contains(text, "incomplete assignment") &&
			strings.Contains(text, "invalid json") &&
			strings.Contains(text, "not an assignment")
	})
	mu.Lock()
	text := strings.Join(logs, "\n")
	mu.Unlock()
	if book.Contains(requestID) {
		t.Fatal("incomplete assignment was stored")
	}
	if strings.Contains(text, token) || strings.Contains(text, "127.0.0.1:9") {
		t.Fatalf("frame contents leaked into log %q", text)
	}
	select {
	case data := <-link.out:
		t.Fatalf("frame for ignored input = %s", data)
	default:
	}

	link.in <- []byte(enabledAssignment(requestID))
	assertAssignmentACK(t, link)
	waitUntil(t, func() bool { return book.Contains(requestID) })
	select {
	case data := <-link.out:
		t.Fatalf("frame for assignment = %s", data)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
}

func TestAssignmentStartsWhenItArrives(t *testing.T) {
	const requestID = "018f3c2e-7b1a-7c3d-8e4f-123456789abc"
	book := journal.New()
	work := &fakeWork{}
	link := newPipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = serve(ctx, link, book, work, func(string) {})
	}()
	link.in <- []byte(enabledAssignment(requestID))
	assertAssignmentACK(t, link)
	waitUntil(t, func() bool {
		ids := work.submittedIDs()
		return book.Contains(requestID) && len(ids) == 1 && ids[0] == requestID
	})
	link.in <- []byte(enabledAssignment(requestID))
	assertAssignmentACK(t, link)
	time.Sleep(30 * time.Millisecond)
	if ids := work.submittedIDs(); len(ids) != 1 {
		t.Fatalf("submitted = %v", ids)
	}
	select {
	case data := <-link.out:
		t.Fatalf("management frame = %s", data)
	default:
	}
	cancel()
}

type pipe struct {
	in   chan []byte
	out  chan []byte
	fail atomic.Bool
}

func newPipe() *pipe {
	return &pipe{in: make(chan []byte, 8), out: make(chan []byte, 8)}
}

func (p *pipe) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	select {
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	case data := <-p.in:
		return websocket.MessageText, data, nil
	}
}

func (p *pipe) Write(ctx context.Context, kind websocket.MessageType, data []byte) error {
	if p.fail.CompareAndSwap(true, false) {
		return errors.New("write failed")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.out <- append([]byte(nil), data...):
		return nil
	}
}

func waitUntil(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition was not met")
}

type fakeWork struct {
	mu        sync.Mutex
	submitted []string
	cancelled []string
	paused    []string
}

func (f *fakeWork) Submit(assignment journal.Assignment) {
	f.mu.Lock()
	f.submitted = append(f.submitted, assignment.RequestID)
	f.mu.Unlock()
}

func (f *fakeWork) Cancel(requestID string) {
	f.mu.Lock()
	f.cancelled = append(f.cancelled, requestID)
	f.mu.Unlock()
}

func (f *fakeWork) Resume() {}

func (f *fakeWork) submittedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.submitted...)
}

func (f *fakeWork) cancelledIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.cancelled...)
}

func TestDisabledAssignmentDoesNotStart(t *testing.T) {
	const requestID = "018f3c2e-7b1a-7c3d-8e4f-123456789abc"
	book := journal.New()
	work := &fakeWork{}
	link := newPipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = serve(ctx, link, book, work, func(string) {})
	}()
	link.in <- []byte(disabledAssignment(requestID))
	assertAssignmentACK(t, link)
	waitUntil(t, func() bool { return book.Contains(requestID) })
	time.Sleep(30 * time.Millisecond)
	if ids := work.submittedIDs(); len(ids) != 0 {
		t.Fatalf("submitted = %v", ids)
	}
	select {
	case data := <-link.out:
		t.Fatalf("frame = %s", data)
	default:
	}
	cancel()
}

func TestUnknownAssignmentCancelsDelivery(t *testing.T) {
	const requestID = "018f3c2e-7b1a-7c3d-8e4f-123456789abc"
	book := journal.New()
	work := &fakeWork{}
	link := newPipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = serve(ctx, link, book, work, func(string) {})
	}()
	link.in <- []byte(enabledAssignment(requestID))
	assertAssignmentACK(t, link)
	waitUntil(t, func() bool { return len(work.submittedIDs()) == 1 })
	link.in <- []byte(`{"type":"error","version":3,"code":"unknown_assignment","request_id":"` + requestID + `"}`)
	waitUntil(t, func() bool { return len(work.cancelledIDs()) == 1 })
	if ids := work.cancelledIDs(); ids[0] != requestID {
		t.Fatalf("cancelled = %v", ids)
	}
	if book.Contains(requestID) {
		t.Fatal("unknown assignment was kept")
	}
	cancel()
}

func TestStoredWorkResumesWithoutAManagementFrame(t *testing.T) {
	const requestID = "018f3c2e-7b1a-7c3d-8e4f-123456789abc"
	book := journal.New()
	assignment, err := journal.Parse([]byte(enabledAssignment(requestID)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := book.Put(assignment); err != nil {
		t.Fatal(err)
	}
	started := make(chan string, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	work := deliver.NewScheduler(ctx, deliver.WallClock{}, func(_ context.Context, item journal.Assignment, _ journal.Endpoint) journal.Result {
		started <- item.RequestID
		return journal.Result{Outcome: journal.OutcomeSuccess, Status: http.StatusNoContent}
	}, book, nil, nil)
	link := newPipe()
	go func() {
		_ = serve(ctx, link, book, work, func(string) {})
	}()
	select {
	case id := <-started:
		if id != requestID {
			t.Fatalf("started = %s", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stored assignment did not start")
	}
	select {
	case data := <-link.out:
		t.Fatalf("management frame = %s", data)
	default:
	}
}

func TestReadingDoesNotWaitForHTTP(t *testing.T) {
	const firstID = "018f3c2e-7b1a-7c3d-8e4f-123456789abc"
	const secondID = "018f3c2e-7b1a-7c3d-8e4f-abcdefabcdef"
	const endpointA = "11111111-1111-4111-8111-111111111111"
	const endpointB = "22222222-2222-4222-8222-222222222222"
	entered := make(chan struct{})
	release := make(chan struct{})
	var started sync.Once
	var closeRelease sync.Once
	defer closeRelease.Do(func() { close(release) })
	var mu sync.Mutex
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.RequestURI())
		mu.Unlock()
		started.Do(func() { close(entered) })
		<-release
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	book := journal.New()
	client := deliver.NewClient()
	work := deliver.NewScheduler(context.Background(), deliver.WallClock{}, func(ctx context.Context, assignment journal.Assignment, endpoint journal.Endpoint) journal.Result {
		return deliver.Call(ctx, client, assignment, endpoint)
	}, book, nil, nil)
	link := newPipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = serve(ctx, link, book, work, func(string) {})
	}()
	link.in <- []byte(callAssignment(firstID, endpointA, srv.URL))
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("http did not start")
	}
	link.in <- []byte(callAssignment(secondID, endpointB, srv.URL))
	waitUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(paths) == 2
	})
	closeRelease.Do(func() { close(release) })
	waitUntil(t, func() bool {
		return len(book.Results(firstID)) == 1 && len(book.Results(secondID)) == 1
	})
	for _, id := range []string{firstID, secondID} {
		got := book.Results(id)
		if got[0].Outcome != journal.OutcomeSuccess || got[0].Status != http.StatusNoContent || got[0].Duration < 0 {
			t.Fatalf("result %s = %+v", id, got[0])
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(paths) != 2 || paths[0] != "/hook/orders?a=1" || paths[1] != "/hook/orders?a=1" {
		t.Fatalf("paths = %v", paths)
	}
}

func callAssignment(requestID, endpointID, base string) string {
	return `{
		"type":"assignment","version":3,
		"request_id":"` + requestID + `",
		"received_at":"2026-01-02T03:04:05.000000Z",
		"method":"POST","path":"/orders","query":"a=1",
		"headers":{"Content-Type":["text/plain"]},"trailers":{},
		"body":"YQ==",
		"endpoints":[{"id":"` + endpointID + `","url":"` + base + `/hook","delivery_interval_ms":0,"idempotency_key":true,"enabled":true,"attempt_count":0,"state":"pending"}]
	}`
}

func enabledAssignment(requestID string) string {
	return assignmentFrame(requestID, true)
}

func disabledAssignment(requestID string) string {
	return assignmentFrame(requestID, false)
}

func TestSessionLogsConnection(t *testing.T) {
	const token = "s3cr3t-value"
	buf := &safeBuf{}
	logger := log.New(buf, "", log.LstdFlags)
	ready := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		defer conn.CloseNow()
		readHello(t, conn)
		close(ready)
		_, _, _ = conn.Read(context.Background())
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, hostOf(srv), token, frameLimit, time.Second, logger, t.TempDir())
	}()
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("session was not accepted")
	}
	text := waitLog(t, buf, "Connected")
	host := hostOf(srv)
	for _, line := range []string{"Connecting", logField("host", host), "Connected"} {
		if !strings.Contains(text, line) {
			t.Fatalf("log = %q, missing %q", text, line)
		}
	}
	if strings.Contains(text, "sync finished") || strings.Contains(text, "session ready") {
		t.Fatalf("log = %q", text)
	}
	matched, err := regexp.MatchString(`(?m)^[0-9]{4}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} Connected$`, text)
	if err != nil || !matched {
		t.Fatalf("log = %q", text)
	}
	if strings.Contains(text, token) || strings.Contains(text, "Connection failed") || strings.Contains(text, "Reconnecting") {
		t.Fatalf("log = %q", text)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("session did not stop")
	}
	text = buf.String()
	if !strings.Contains(text, "Stopped") || strings.Contains(text, "Connection failed") || strings.Contains(text, token) {
		t.Fatalf("log = %q", text)
	}
}

func TestCancelledSessionLogsStopped(t *testing.T) {
	const token = "s3cr3t-value"
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var buf safeBuf
	logger := log.New(&buf, "", log.LstdFlags)
	if err := run(ctx, "ws://127.0.0.1:9", token, frameLimit, time.Second, logger, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	text := buf.String()
	if !strings.Contains(text, "Stopped") || strings.Contains(text, "Connecting") || strings.Contains(text, "Connection failed") || strings.Contains(text, token) {
		t.Fatalf("log = %q", text)
	}
}

func TestFatalCloseIsLogged(t *testing.T) {
	const token = "s3cr3t-value"
	cases := []struct {
		code websocket.StatusCode
		text string
	}{
		{code: 4001, text: "authentication failed"},
		{code: 4003, text: "access revoked"},
		{code: 4004, text: "token expired"},
		{code: 4008, text: "session replaced"},
	}
	for _, tc := range cases {
		t.Run(tc.text, func(t *testing.T) {
			buf := &safeBuf{}
			logger := log.New(buf, "", log.LstdFlags)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
				if err != nil {
					return
				}
				defer conn.CloseNow()
				readHello(t, conn)
				_ = conn.Close(tc.code, "")
			}))
			defer srv.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			err := run(ctx, hostOf(srv), token, frameLimit, 0, logger, t.TempDir())
			if err == nil || err.Error() != tc.text {
				t.Fatalf("error = %v", err)
			}
			text := buf.String()
			if !strings.Contains(text, "Connection failed") || !strings.Contains(text, logField("reason", tc.text)) {
				t.Fatalf("log = %q", text)
			}
			if strings.Contains(text, "Reconnecting") || strings.Contains(text, "Stopped") || strings.Contains(text, token) {
				t.Fatalf("log = %q", text)
			}
		})
	}
}

func TestDialFailureIsLogged(t *testing.T) {
	const token = "s3cr3t-value"
	var hits atomic.Int32
	buf := &safeBuf{}
	logger := log.New(buf, "", log.LstdFlags)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		defer conn.CloseNow()
		readHello(t, conn)
		_, _, _ = conn.Read(context.Background())
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, hostOf(srv), token, frameLimit, 0, logger, t.TempDir())
	}()
	text := waitLog(t, buf, "Connected")
	host := hostOf(srv)
	if strings.Count(text, "Connecting") < 2 || !strings.Contains(text, logField("host", host)) || !strings.Contains(text, "Connection failed") || !strings.Contains(text, "Reconnecting") || !strings.Contains(text, "Connected") {
		t.Fatalf("log = %q", text)
	}
	if strings.Contains(text, token) {
		t.Fatalf("token leaked into %q", text)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("session did not stop")
	}
}

func TestAssignmentReceiptIsLogged(t *testing.T) {
	const requestID = "018f3c2e-7b1a-7c3d-8e4f-123456789abc"
	book := journal.New()
	work := &fakeWork{}
	link, buf, cancel := startLogged(t, book, work)
	defer cancel()
	link.in <- []byte(enabledAssignment(requestID))
	assertAssignmentACK(t, link)
	waitUntil(t, func() bool { return len(work.submittedIDs()) == 1 })
	link.in <- []byte(enabledAssignment(requestID))
	assertAssignmentACK(t, link)
	waitUntil(t, func() bool {
		return strings.Count(buf.String(), "Assignment received\n"+logField("request", requestID)) == 2
	})
	text := buf.String()
	if strings.Contains(text, "assignment acknowledged") || strings.Contains(text, "already acknowledged") {
		t.Fatalf("log = %q", text)
	}
	if len(work.submittedIDs()) != 1 {
		t.Fatalf("submitted = %v", work.submittedIDs())
	}
	if strings.Contains(text, "http://127.0.0.1:3000") || strings.Contains(text, "YQ==") || strings.Contains(text, "text/plain") {
		t.Fatalf("frame leaked into %q", text)
	}
}

func TestDisabledAssignmentIsLogged(t *testing.T) {
	const requestID = "018f3c2e-7b1a-7c3d-8e4f-123456789abc"
	book := journal.New()
	work := &fakeWork{}
	link, buf, cancel := startLogged(t, book, work)
	defer cancel()
	link.in <- []byte(disabledAssignment(requestID))
	assertAssignmentACK(t, link)
	text := waitLog(t, buf, "Assignment has no enabled endpoint\n"+logField("request", requestID))
	if !strings.Contains(text, "Assignment received\n"+logField("request", requestID)) || strings.Contains(text, "assignment acknowledged") {
		t.Fatalf("log = %q", text)
	}
	if strings.Contains(text, "http://127.0.0.1:3000") || strings.Contains(text, "YQ==") || strings.Contains(text, "text/plain") {
		t.Fatalf("frame leaked into %q", text)
	}
	if ids := work.submittedIDs(); len(ids) != 0 {
		t.Fatalf("submitted = %v", ids)
	}
}

func TestDroppedAssignmentIsLogged(t *testing.T) {
	const requestID = "018f3c2e-7b1a-7c3d-8e4f-123456789abc"
	book := journal.New()
	link, buf, cancel := startLogged(t, book, &fakeWork{})
	defer cancel()
	link.in <- []byte(enabledAssignment(requestID))
	assertAssignmentACK(t, link)
	waitLog(t, buf, "Assignment received\n"+logField("request", requestID))
	link.in <- []byte(`{"type":"error","version":3,"code":"unknown_assignment","request_id":"` + requestID + `"}`)
	waitLog(t, buf, "Assignment dropped\n"+logField("request", requestID))
	waitUntil(t, func() bool { return !book.Contains(requestID) })
}

func startLogged(t *testing.T, book *journal.Journal, work delivery) (*pipe, *safeBuf, context.CancelFunc) {
	t.Helper()
	buf := &safeBuf{}
	logger := log.New(buf, "", log.LstdFlags)
	link := newPipe()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = serve(ctx, link, book, work, func(message string) { logger.Print(message) })
	}()
	return link, buf, cancel
}

type safeBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func waitLog(t *testing.T, buf *safeBuf, want string) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		text := buf.String()
		if strings.Contains(text, want) {
			return text
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("log = %q, missing %q", buf.String(), want)
	return ""
}

func assignmentFrame(requestID string, enabled bool) string {
	flag := "false"
	if enabled {
		flag = "true"
	}
	return `{
		"type":"assignment","version":3,
		"request_id":"` + requestID + `",
		"received_at":"2026-01-02T03:04:05.000000Z",
		"method":"POST","path":"/orders","query":"a=1",
		"headers":{"Content-Type":["text/plain"]},"trailers":{},
		"body":"YQ==",
		"endpoints":[{"id":"11111111-1111-4111-8111-111111111111","url":"http://127.0.0.1:3000/hook","delivery_interval_ms":0,"idempotency_key":false,"enabled":` + flag + `,"attempt_count":0,"state":"pending"}]
	}`
}

func assertAssignmentACK(t *testing.T, link *pipe) {
	t.Helper()
	select {
	case raw := <-link.out:
		if !strings.Contains(string(raw), `"type":"assignment_ack"`) {
			t.Fatalf("expected assignment ack, got %s", raw)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("assignment was not acknowledged")
	}
}
