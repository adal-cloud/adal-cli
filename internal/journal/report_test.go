package journal

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

const endpointC = "33333333-3333-4333-8333-333333333333"

var attemptPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestReportSuccessOmitsError(t *testing.T) {
	book := New()
	putEndpoints(t, book, endpointA)
	frame := finishAttempt(t, book, endpointA, Result{
		Outcome:  OutcomeSuccess,
		Status:   204,
		Error:    "secret",
		Duration: 1500 * time.Microsecond,
	})
	payload := decodeReport(t, frame)
	if payload["outcome"] != OutcomeSuccess || payload["http_status"] != float64(204) || payload["duration_ms"] != float64(1) {
		t.Fatalf("report = %s", frame)
	}
	if _, ok := payload["error"]; ok {
		t.Fatalf("success report included error %s", frame)
	}
	if _, ok := payload["queue_wait_ms"]; ok {
		t.Fatalf("unknown queue wait was sent in %s", frame)
	}
	if !attemptPattern.MatchString(payload["attempt_id"].(string)) {
		t.Fatalf("attempt_id = %v", payload["attempt_id"])
	}
	if len(frame) > reportLimit {
		t.Fatalf("frame length = %d", len(frame))
	}
	if pending := book.Pending(); len(pending) != 1 || string(pending[0]) != string(frame) {
		t.Fatal("pending report changed")
	}
}

func TestReportFailureWithoutStatus(t *testing.T) {
	book := New()
	putEndpoints(t, book, endpointA)
	frame := finishAttempt(t, book, endpointA, Result{
		Outcome:  OutcomeFailure,
		Error:    "timeout",
		Duration: -5 * time.Millisecond,
	})
	payload := decodeReport(t, frame)
	if payload["outcome"] != OutcomeFailure || payload["error"] != "timeout" || payload["duration_ms"] != float64(0) {
		t.Fatalf("report = %s", frame)
	}
	if _, ok := payload["http_status"]; ok {
		t.Fatalf("status was included in %s", frame)
	}
	if _, ok := payload["queue_wait_ms"]; ok {
		t.Fatalf("unknown queue wait was sent in %s", frame)
	}
}

func TestReportSendsAKnownQueueWait(t *testing.T) {
	book := New()
	putEndpoints(t, book, endpointA, endpointB, endpointC)
	zero := finishAttempt(t, book, endpointA, Result{
		Outcome:        OutcomeSuccess,
		Status:         204,
		Duration:       time.Millisecond,
		QueueWait:      0,
		QueueWaitKnown: true,
	})
	if got := decodeReport(t, zero)["queue_wait_ms"]; got != float64(0) {
		t.Fatalf("queue_wait_ms = %v", got)
	}
	wait := finishAttempt(t, book, endpointB, Result{
		Outcome:        OutcomeFailure,
		Error:          "timeout",
		QueueWait:      1500 * time.Millisecond,
		QueueWaitKnown: true,
	})
	if got := decodeReport(t, wait)["queue_wait_ms"]; got != float64(1500) {
		t.Fatalf("queue_wait_ms = %v", got)
	}
	rounded := finishAttempt(t, book, endpointC, Result{
		Outcome:        OutcomeSuccess,
		Status:         204,
		QueueWait:      1500 * time.Microsecond,
		QueueWaitKnown: true,
	})
	if got := decodeReport(t, rounded)["queue_wait_ms"]; got != float64(1) {
		t.Fatalf("queue_wait_ms = %v", got)
	}
	pending := book.Pending()
	if len(pending) != 3 || string(pending[0]) != string(zero) || string(pending[1]) != string(wait) || string(pending[2]) != string(rounded) {
		t.Fatal("stored report changed")
	}
}

func TestReportOmitsANegativeQueueWait(t *testing.T) {
	book := New()
	putEndpoints(t, book, endpointA)
	frame := finishAttempt(t, book, endpointA, Result{
		Outcome:        OutcomeFailure,
		Error:          "timeout",
		QueueWait:      -time.Millisecond,
		QueueWaitKnown: true,
	})
	if _, ok := decodeReport(t, frame)["queue_wait_ms"]; ok {
		t.Fatalf("negative queue wait was sent in %s", frame)
	}
}

func TestReportClipsError(t *testing.T) {
	book := New()
	putEndpoints(t, book, endpointA)
	frame := finishAttempt(t, book, endpointA, Result{
		Outcome:  OutcomeFailure,
		Status:   500,
		Error:    strings.Repeat("\u00e9", 1025),
		Duration: time.Millisecond,
	})
	if len(frame) > reportLimit {
		t.Fatalf("frame length = %d", len(frame))
	}
	payload := decodeReport(t, frame)
	text, _ := payload["error"].(string)
	if utf8.RuneCountInString(text) != errorLimit || payload["http_status"] != float64(500) {
		t.Fatalf("report = %s", frame)
	}
}

func TestReportIsKeptUntilForgotten(t *testing.T) {
	book := New()
	putEndpoints(t, book, endpointA)
	first := finishAttempt(t, book, endpointA, Result{Outcome: OutcomeFailure, Error: "timeout"})
	second := finishAttempt(t, book, endpointA, Result{Outcome: OutcomeFailure, Error: "request failed"})
	one := decodeReport(t, first)["attempt_id"].(string)
	two := decodeReport(t, second)["attempt_id"].(string)
	if one == two || !attemptPattern.MatchString(two) {
		t.Fatalf("attempt ids = %s %s", one, two)
	}
	if err := book.ForgetReport(requestID, "018f3c2e-7b1a-4000-8000-000000000000"); err != nil {
		t.Fatal(err)
	}
	if len(book.Pending()) != 2 {
		t.Fatal("unrelated attempt was dropped")
	}
	if err := book.ForgetReport(requestID, one); err != nil {
		t.Fatal(err)
	}
	pending := book.Pending()
	if len(pending) != 1 || string(pending[0]) != string(second) {
		t.Fatal("the remaining report changed")
	}
	if err := book.Drop(requestID); err != nil {
		t.Fatal(err)
	}
	if len(book.Pending()) != 0 {
		t.Fatal("reports were kept after the assignment was dropped")
	}
}

func TestReportRejectsSuccessWithoutStatus(t *testing.T) {
	book := New()
	putEndpoints(t, book, endpointA)
	id, ok, err := book.BeginAttempt(requestID, endpointA)
	if err != nil || !ok {
		t.Fatalf("begin: ok=%v err=%v", ok, err)
	}
	if _, err := book.FinishAttempt(requestID, endpointA, id, Result{Outcome: OutcomeSuccess}, time.Unix(0, 0).UTC()); err == nil {
		t.Fatal("success without status was stored")
	}
	if len(book.Pending()) != 0 {
		t.Fatal("invalid report was stored")
	}
}

func putEndpoints(t *testing.T, book *Journal, ids ...string) {
	t.Helper()
	assignment := Assignment{
		RequestID: requestID,
		Method:    "POST",
		Path:      "/orders",
		Headers:   map[string][]string{},
		Endpoints: make([]Endpoint, len(ids)),
	}
	for i, id := range ids {
		assignment.Endpoints[i] = Endpoint{ID: id, URL: "http://127.0.0.1:9/hook", Enabled: true}
	}
	if _, err := book.Put(assignment); err != nil {
		t.Fatal(err)
	}
}

func finishAttempt(t *testing.T, book *Journal, endpointID string, result Result) []byte {
	t.Helper()
	id, ok, err := book.BeginAttempt(requestID, endpointID)
	if err != nil || !ok {
		t.Fatalf("begin %s: ok=%v err=%v", endpointID, ok, err)
	}
	decision, err := book.FinishAttempt(requestID, endpointID, id, result, time.Unix(0, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	return decision.Frame
}

func decodeReport(t *testing.T, frame []byte) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(frame, &payload); err != nil {
		t.Fatal(err)
	}
	return payload
}
