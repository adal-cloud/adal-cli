package session

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/adal-cloud/adal-cli/internal/deliver"
	"github.com/adal-cloud/adal-cli/internal/journal"
)

const (
	reportRequest  = "018f3c2e-7b1a-7c3d-8e4f-123456789abc"
	reportEndpoint = "11111111-1111-4111-8111-111111111111"
)

func TestSuccessReportIsNotSentAgainAfterAck(t *testing.T) {
	session := startReportSession(t, http.StatusNoContent)
	defer session.stop()
	frame := session.exchange(t)
	payload := decodeFrame(t, frame)
	if payload["type"] != "report" || payload["version"] != float64(3) || payload["request_id"] != reportRequest || payload["endpoint_id"] != reportEndpoint {
		t.Fatalf("report = %s", frame)
	}
	if payload["outcome"] != journal.OutcomeSuccess || payload["http_status"] != float64(http.StatusNoContent) {
		t.Fatalf("report = %s", frame)
	}
	if _, ok := payload["error"]; ok {
		t.Fatalf("success report included error %s", frame)
	}
	if duration, _ := payload["duration_ms"].(float64); duration < 0 {
		t.Fatalf("duration_ms = %v", payload["duration_ms"])
	}
	if wait, ok := payload["queue_wait_ms"].(float64); !ok || wait < 0 {
		t.Fatalf("queue_wait_ms = %v", payload["queue_wait_ms"])
	}
	if len(frame) > 8192 {
		t.Fatalf("frame length = %d", len(frame))
	}
	attemptID, _ := payload["attempt_id"].(string)
	session.link.in <- []byte(`{"type":"report_ack","version":3,"request_id":"` + reportRequest + `","attempt_id":"` + attemptID + `"}`)
	waitUntil(t, func() bool { return len(session.book.Pending()) == 0 })
	session.stop()
	assertNoReplay(t, session.book)
}

func TestFailureReportKeepsStatusAndError(t *testing.T) {
	session := startReportSession(t, http.StatusBadGateway)
	defer session.stop()
	frame := session.exchange(t)
	payload := decodeFrame(t, frame)
	if payload["outcome"] != journal.OutcomeFailure || payload["http_status"] != float64(http.StatusBadGateway) || payload["error"] != "unexpected status" {
		t.Fatalf("report = %s", frame)
	}
	if duration, _ := payload["duration_ms"].(float64); duration < 0 {
		t.Fatalf("duration_ms = %v", payload["duration_ms"])
	}
	if wait, ok := payload["queue_wait_ms"].(float64); !ok || wait < 0 {
		t.Fatalf("queue_wait_ms = %v", payload["queue_wait_ms"])
	}
	if strings.Contains(string(frame), strings.TrimPrefix(session.base, "http://")) {
		t.Fatalf("url leaked into %s", frame)
	}
}

func TestReportIsResentUnchanged(t *testing.T) {
	session := startReportSession(t, http.StatusOK)
	original := session.exchange(t)
	session.stop()
	assertSameReport(t, replayReport(t, session.book), original)
}

func TestReportDeliveryIsLogged(t *testing.T) {
	session := startReportSession(t, http.StatusNoContent)
	defer session.stop()
	frame := session.exchange(t)
	attemptID, _ := decodeFrame(t, frame)["attempt_id"].(string)
	text := session.log.String()
	address := session.base + "/hook/orders?a=1"
	if !strings.Contains(text, "Call queued\n"+logField("request", reportRequest)) ||
		!strings.Contains(text, "Call starting\n"+logField("request", reportRequest)) ||
		!strings.Contains(text, "Call finished\n"+logField("request", reportRequest)) ||
		!strings.Contains(text, "Report sending\n"+logField("request", reportRequest)) ||
		!strings.Contains(text, logField("endpoint", address)) ||
		!strings.Contains(text, logField("outcome", "success")) ||
		!strings.Contains(text, logField("status", "HTTP 204")) ||
		!strings.Contains(text, logField("attempt", attemptID)) ||
		!strings.Contains(text, "\n    duration    ") ||
		!strings.Contains(text, "\n    queue ") {
		t.Fatalf("log = %q", text)
	}
	if strings.Contains(text, "YQ==") || strings.Contains(text, "text/plain") {
		t.Fatalf("request leaked into %q", text)
	}
	session.link.in <- []byte(`{"type":"report_ack","version":3,"request_id":"` + reportRequest + `","attempt_id":"` + attemptID + `"}`)
	waitLog(t, session.log, "Report accepted\n"+logField("request", reportRequest))
	if !strings.Contains(session.log.String(), logField("attempt", attemptID)) {
		t.Fatalf("log = %q", session.log.String())
	}
}

func TestReportKeptAndRejectedIsLogged(t *testing.T) {
	var kept safeBuf
	out := &socket{log: log.New(&kept, "", log.LstdFlags)}
	book, frame := storedReport(t, journal.Result{
		EndpointID: reportEndpoint,
		Outcome:    journal.OutcomeFailure,
		Error:      "timeout",
		Duration:   30 * time.Millisecond,
	})
	out.sendFrame(book, reportRequest, journal.Result{EndpointID: reportEndpoint, Outcome: journal.OutcomeFailure}, frame)
	attemptID, _ := decodeFrame(t, frame)["attempt_id"].(string)
	if !strings.Contains(kept.String(), "Report kept") ||
		!strings.Contains(kept.String(), logField("request", reportRequest)) ||
		!strings.Contains(kept.String(), logField("attempt", attemptID)) ||
		!strings.Contains(kept.String(), logField("endpoint", "http://127.0.0.1:9/hook/orders?a=1")) ||
		strings.Contains(kept.String(), "Report sending") {
		t.Fatalf("log = %q", kept.String())
	}
	if len(book.Pending()) != 1 {
		t.Fatal("report was not kept")
	}

	var failed safeBuf
	writer := &socket{log: log.New(&failed, "", log.LstdFlags)}
	link := newPipe()
	link.fail.Store(true)
	writer.bind(context.Background(), link)
	stored, storedFrame := storedReport(t, journal.Result{
		EndpointID: reportEndpoint,
		Outcome:    journal.OutcomeSuccess,
		Status:     http.StatusNoContent,
		Duration:   time.Millisecond,
	})
	writer.sendFrame(stored, reportRequest, journal.Result{EndpointID: reportEndpoint, Outcome: journal.OutcomeSuccess}, storedFrame)
	storedAttempt, _ := decodeFrame(t, storedFrame)["attempt_id"].(string)
	if !strings.Contains(failed.String(), "Report kept") ||
		!strings.Contains(failed.String(), logField("request", reportRequest)) ||
		!strings.Contains(failed.String(), logField("attempt", storedAttempt)) ||
		strings.Contains(failed.String(), "Report sending") {
		t.Fatalf("log = %q", failed.String())
	}
	if len(stored.Pending()) != 1 {
		t.Fatal("failed write dropped the report")
	}

	var rejected safeBuf
	reject := &socket{log: log.New(&rejected, "", log.LstdFlags)}
	reject.sendFrame(nil, reportRequest, journal.Result{Outcome: journal.OutcomeSuccess}, nil)
	if !strings.Contains(rejected.String(), "Report was not stored") {
		t.Fatalf("log = %q", rejected.String())
	}
}

func storedReport(t *testing.T, result journal.Result) (*journal.Journal, []byte) {
	t.Helper()
	book := journal.New()
	assignment, err := journal.Parse([]byte(callAssignment(reportRequest, reportEndpoint, "http://127.0.0.1:9")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := book.Put(assignment); err != nil {
		t.Fatal(err)
	}
	id, ok, err := book.BeginAttempt(reportRequest, reportEndpoint)
	if err != nil || !ok {
		t.Fatalf("begin: ok=%v err=%v", ok, err)
	}
	decision, err := book.FinishAttempt(reportRequest, reportEndpoint, id, result, time.Unix(0, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	return book, decision.Frame
}

func TestReportStoppedIsLogged(t *testing.T) {
	session := startReportSession(t, http.StatusOK)
	defer session.stop()
	frame := session.exchange(t)
	attemptID, _ := decodeFrame(t, frame)["attempt_id"].(string)
	session.link.in <- []byte(`{"type":"error","version":3,"code":"invalid_report","request_id":"` + reportRequest + `","attempt_id":"` + attemptID + `"}`)
	waitLog(t, session.log, "Report stopped\n"+logField("request", reportRequest))
	if !strings.Contains(session.log.String(), logField("attempt", attemptID)) || !strings.Contains(session.log.String(), logField("code", "invalid_report")) {
		t.Fatalf("log = %q", session.log.String())
	}
	waitUntil(t, func() bool { return len(session.book.Pending()) == 0 })
}

func TestReportResentIsLogged(t *testing.T) {
	session := startReportSession(t, http.StatusOK)
	original := session.exchange(t)
	attemptID, _ := decodeFrame(t, original)["attempt_id"].(string)
	session.stop()

	var buf safeBuf
	logger := log.New(&buf, "", log.LstdFlags)
	link := newPipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = serve(ctx, link, session.book, nil, func(message string) { logger.Print(message) })
	}()
	frame := readOut(t, link)
	assertSameReport(t, frame, original)
	text := waitLog(t, &buf, "Report resent\n"+logField("request", reportRequest))
	if !strings.Contains(text, logField("attempt", attemptID)) || strings.Contains(text, session.base) || strings.Contains(text, "YQ==") {
		t.Fatalf("request leaked into %q", text)
	}
}

func TestInFlightReportIsSentOnTheNextConnection(t *testing.T) {
	book := journal.New()
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	out := &socket{}
	work := deliver.NewScheduler(context.Background(), deliver.WallClock{}, func(context.Context, journal.Assignment, journal.Endpoint) journal.Result {
		once.Do(func() { close(started) })
		<-release
		return journal.Result{Outcome: journal.OutcomeSuccess, Status: http.StatusNoContent}
	}, book, func(requestID string, result journal.Result, frame []byte) {
		out.sendFrame(book, requestID, result, frame)
	}, nil)

	link := newPipe()
	connCtx, cancelConn := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		out.bind(connCtx, link)
		done <- serve(connCtx, link, book, work, func(string) {})
		out.unbind()
	}()
	link.in <- []byte(enabledAssignment(reportRequest))
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("call did not start")
	}
	cancelConn()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("connection did not stop")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	next := newPipe()
	out.bind(ctx, next)
	go func() {
		_ = serve(ctx, next, book, work, func(string) {})
	}()
	close(release)
	frame := readOut(t, next)
	payload := decodeFrame(t, frame)
	if payload["type"] != "report" || payload["attempt_number"] != float64(1) || payload["request_id"] != reportRequest {
		t.Fatalf("report = %s", frame)
	}
	if strings.Contains(string(frame), `"reports"`) || strings.Contains(string(frame), `"sync_end"`) {
		t.Fatalf("report = %s", frame)
	}
	if len(book.Results(reportRequest)) != 1 {
		t.Fatal("finished call was dropped")
	}
}

func TestPermanentReportErrorStopsResend(t *testing.T) {
	codes := []string{"attempt_conflict", "unknown_assignment", "unknown_endpoint", "invalid_report"}
	for _, code := range codes {
		t.Run(code, func(t *testing.T) {
			session := startReportSession(t, http.StatusOK)
			frame := session.exchange(t)
			attemptID, _ := decodeFrame(t, frame)["attempt_id"].(string)
			session.link.in <- []byte(`{"type":"error","version":3,"code":"` + code + `","request_id":"` + reportRequest + `","attempt_id":"` + attemptID + `"}`)
			waitUntil(t, func() bool { return len(session.book.Pending()) == 0 })
			if !session.book.Contains(reportRequest) {
				t.Fatal("assignment was dropped with the report")
			}
			session.stop()
			assertNoReplay(t, session.book)
		})
	}
}

type reportRun struct {
	book *journal.Journal
	link *pipe
	base string
	log  *safeBuf
	stop func()
}

func (r *reportRun) exchange(t *testing.T) []byte {
	t.Helper()
	r.link.in <- []byte(callAssignment(reportRequest, reportEndpoint, r.base))
	return readOut(t, r.link)
}

func startReportSession(t *testing.T, status int) *reportRun {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
	}))
	buf := &safeBuf{}
	logger := log.New(buf, "", log.LstdFlags)
	book := journal.New()
	out := &socket{log: logger}
	client := deliver.NewClient()
	ctx, cancel := context.WithCancel(context.Background())
	work := deliver.NewScheduler(ctx, deliver.WallClock{}, func(ctx context.Context, assignment journal.Assignment, endpoint journal.Endpoint) journal.Result {
		return deliver.Call(ctx, client, assignment, endpoint)
	}, book, func(requestID string, result journal.Result, frame []byte) {
		out.sendFrame(book, requestID, result, frame)
	}, logger)
	link := newPipe()
	done := make(chan struct{})
	go func() {
		out.bind(ctx, link)
		defer out.unbind()
		_ = serve(ctx, link, book, work, func(message string) { logger.Print(message) })
		close(done)
	}()
	var once sync.Once
	return &reportRun{
		book: book,
		link: link,
		base: srv.URL,
		log:  buf,
		stop: func() {
			once.Do(func() {
				cancel()
				srv.Close()
				select {
				case <-done:
				case <-time.After(2 * time.Second):
				}
			})
		},
	}
}

func replayReport(t *testing.T, book *journal.Journal) []byte {
	t.Helper()
	link := newPipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = serve(ctx, link, book, nil, func(string) {})
	}()
	return readOut(t, link)
}

func assertNoReplay(t *testing.T, book *journal.Journal) {
	t.Helper()
	link := newPipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = serve(ctx, link, book, nil, func(string) {})
	}()
	select {
	case data := <-link.out:
		t.Fatalf("unexpected frame = %s", data)
	case <-time.After(50 * time.Millisecond):
	}
}

func assertSameReport(t *testing.T, replayed, original []byte) {
	t.Helper()
	if string(replayed) != string(original) {
		t.Fatalf("replayed = %s, want %s", replayed, original)
	}
	if strings.Contains(string(replayed), `"type":"reports"`) || strings.Contains(string(replayed), `"sync_end"`) || strings.Contains(string(replayed), `"ack"`) {
		t.Fatalf("management frame = %s", replayed)
	}
}

func readOut(t *testing.T, link *pipe) []byte {
	t.Helper()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for {
		select {
		case raw := <-link.out:
			var f struct {
				Type string `json:"type"`
			}
			_ = json.Unmarshal(raw, &f)
			if f.Type == "assignment_ack" {
				continue
			}
			return raw
		case <-timer.C:
			t.Fatal("missing frame")
			return nil
		}
	}
}

func logField(label, value string) string {
	return fmt.Sprintf("    %-*s  %s", 10, label, value)
}

func decodeFrame(t *testing.T, frame []byte) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(frame, &payload); err != nil {
		t.Fatal(err)
	}
	return payload
}
