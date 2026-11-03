package journal

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

const (
	requestID = "018f3c2e-7b1a-7c3d-8e4f-123456789abc"
	endpointA = "11111111-1111-4111-8111-111111111111"
	endpointB = "22222222-2222-4222-8222-222222222222"
)

func TestParseAssignment(t *testing.T) {
	assignment, err := Parse([]byte(frame(true, false)))
	if err != nil {
		t.Fatal(err)
	}
	if assignment.RequestID != requestID || assignment.Method != "POST" || assignment.Path != "/orders" || assignment.Query != "a=1" {
		t.Fatalf("assignment = %+v", assignment)
	}
	if assignment.Body != "YQ==" || len(assignment.Endpoints) != 2 || !assignment.Endpoints[0].Enabled || assignment.Endpoints[1].Enabled {
		t.Fatalf("assignment = %+v", assignment)
	}
	if assignment.Endpoints[0].DeliveryIntervalMS != 1000 || assignment.Headers["Content-Type"][0] != "text/plain" {
		t.Fatalf("assignment = %+v", assignment)
	}

	if _, err := Parse([]byte("{")); !errorIs(err, ErrInvalidJSON) {
		t.Fatalf("error = %v", err)
	}
	if _, err := Parse([]byte(`{"type":"ready","version":3}`)); !errorIs(err, ErrNotAssignment) {
		t.Fatalf("error = %v", err)
	}
	incomplete := []string{
		`{"type":"assignment"}`,
		strings.ReplaceAll(frame(true, false), `"method":"POST"`, `"method":""`),
		strings.ReplaceAll(frame(true, false), `"body":"YQ=="`, `"body":"%%%"`),
		strings.ReplaceAll(frame(true, false), requestID, "NOT-A-UUID"),
		strings.ReplaceAll(frame(true, false), `"trailers":{},`, ""),
		strings.ReplaceAll(frame(true, false), `"received_at":"2026-01-02T03:04:05.000000Z",`, ""),
		strings.ReplaceAll(frame(true, false), `"received_at":"2026-01-02T03:04:05.000000Z"`, `"received_at":"2026-01-02T03:04:05Z"`),
		strings.ReplaceAll(frame(true, false), `"attempt_count":0`, `"attempt_count":-1`),
		strings.ReplaceAll(frame(true, false), `"attempt_count":0`, `"attempt_count":11`),
	}
	if _, err := Parse([]byte(strings.ReplaceAll(frame(true, false), `,"deliver_backlog":false`, ""))); err != nil {
		t.Fatal(err)
	}
	for _, item := range incomplete {
		if _, err := Parse([]byte(item)); !errorIs(err, ErrIncomplete) {
			t.Fatalf("Parse(%s) error = %v", item, err)
		}
	}
}

func TestPutStoresAndArmsOnce(t *testing.T) {
	j := New()
	assignment, err := Parse([]byte(frame(true, false)))
	if err != nil {
		t.Fatal(err)
	}
	created, err := j.Put(assignment)
	if err != nil || !created {
		t.Fatal(err)
	}
	if !j.Contains(requestID) {
		t.Fatal("assignment was not stored")
	}
	if !j.HasEnabled(requestID) {
		t.Fatal("enabled assignment reported no endpoint")
	}
	if _, ok := j.Arm(requestID); !ok {
		t.Fatal("enabled assignment did not arm")
	}
	if _, ok := j.Arm(requestID); ok {
		t.Fatal("armed assignment started twice")
	}
	again, err := j.Put(assignment)
	if err != nil || again {
		t.Fatalf("repeat created=%v err=%v", again, err)
	}
	if stored := j.Stored(); len(stored) != 1 || stored[0].RequestID != requestID {
		t.Fatalf("stored = %+v", stored)
	}
}

func TestDisabledEndpointDoesNotArm(t *testing.T) {
	j := New()
	assignment, err := Parse([]byte(frame(false, false)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.Put(assignment); err != nil {
		t.Fatal(err)
	}
	if j.HasEnabled(requestID) {
		t.Fatal("disabled assignment reported an enabled endpoint")
	}
	if _, ok := j.Arm(requestID); ok {
		t.Fatal("disabled endpoints were armed")
	}
	if !j.Contains(requestID) {
		t.Fatal("disabled assignment was dropped")
	}
}

func TestDropRemovesUnknownAssignment(t *testing.T) {
	j := New()
	assignment, err := Parse([]byte(frame(true, false)))
	if err != nil {
		t.Fatal(err)
	}
	j.Put(assignment)
	j.Drop(requestID)
	if j.Contains(requestID) {
		t.Fatal("dropped assignment is still stored")
	}
	if _, ok := j.Arm(requestID); ok {
		t.Fatal("dropped assignment was armed")
	}
}

func TestStoredKeepsArrivalOrder(t *testing.T) {
	j := New()
	first, err := Parse([]byte(frame(true, false)))
	if err != nil {
		t.Fatal(err)
	}
	secondID := "018f3c2e-7b1a-7c3d-8e4f-abcdefabcdef"
	second, err := Parse([]byte(strings.ReplaceAll(frame(true, false), requestID, secondID)))
	if err != nil {
		t.Fatal(err)
	}
	j.Put(first)
	j.Put(second)
	stored := j.Stored()
	if len(stored) != 2 || stored[0].RequestID != requestID || stored[1].RequestID != secondID {
		t.Fatalf("stored = %+v", stored)
	}
}

func frame(firstEnabled, secondEnabled bool) string {
	return `{
		"type":"assignment","version":3,
		"request_id":"` + requestID + `",
		"received_at":"2026-01-02T03:04:05.000000Z",
		"method":"POST","path":"/orders","query":"a=1",
		"headers":{"Content-Type":["text/plain"]},"trailers":{},
		"body":"YQ==",
		"endpoints":[
			{"id":"` + endpointA + `","url":"http://127.0.0.1:3001/hook","delivery_interval_ms":1000,"idempotency_key":true,"enabled":` + boolText(firstEnabled) + `,"attempt_count":0,"state":"pending"},
			{"id":"` + endpointB + `","url":"http://127.0.0.1:3000/hook","delivery_interval_ms":0,"idempotency_key":false,"enabled":` + boolText(secondEnabled) + `,"attempt_count":0,"state":"pending"}
		]
	}`
}

func boolText(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func unmarshal(body []byte, dest any) error {
	if err := json.Unmarshal(body, dest); err != nil {
		return err
	}
	return nil
}

func errorIs(err, target error) bool {
	return err == target
}

func TestEndpointAttemptCountsRemainIndependent(t *testing.T) {
	book := New()
	a := mustParse(t, frame(true, true))
	a.Endpoints[0].AttemptCount = 9
	a.Endpoints[1].AttemptCount = 0
	if _, err := book.Put(a); err != nil {
		t.Fatal(err)
	}
	for i, ep := range a.Endpoints {
		id, ok, err := book.BeginAttempt(requestID, ep.ID)
		if err != nil || !ok {
			t.Fatal(err)
		}
		decision, err := book.FinishAttempt(requestID, ep.ID, id, Result{Outcome: OutcomeFailure, Error: "timeout"}, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		var report struct {
			Number int `json:"attempt_number"`
		}
		if json.Unmarshal(decision.Frame, &report) != nil {
			t.Fatal("invalid report")
		}
		expected := 10
		if i == 1 {
			expected = 1
		}
		if report.Number != expected {
			t.Fatal("endpoint count leaked", report.Number, expected)
		}
	}
}
