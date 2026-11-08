package journal

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func losslessFrame() string {
	return strings.Replace(strings.Replace(frame(true, false), `"version":3`, `"version":4`, 1), `"headers":{"Content-Type":["text/plain"]}`, `"headers":{"X-Bytes":[{"base64":"/4A="},"/4A=","", "Привет", "{\"base64\":\"/4A=\"}"]}`, 1)
}

func TestParseLosslessAssignment(t *testing.T) {
	raw := strings.Replace(losslessFrame(), `"trailers":{}`, `"trailers":{"X-End":[{"base64":"/4A="},"tail",""]}`, 1)
	a, err := Parse([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a.Headers["X-Bytes"], []string{string([]byte{255, 128}), "/4A=", "", "Привет", `{"base64":"/4A="}`}) {
		t.Fatalf("headers=%q", a.Headers)
	}
	if !reflect.DeepEqual(a.Trailers, map[string][]string{"X-End": {string([]byte{255, 128}), "tail", ""}}) {
		t.Fatal("trailers were discarded")
	}
	for _, bad := range []string{`{"base64":"%%%"}`, `{"base64":"/4A"}`, `{"base64":"/4A=","extra":true}`, `{"base64":null}`, `{"bytes":"/4A="}`, `null`} {
		if _, err := Parse([]byte(strings.Replace(raw, `{"base64":"/4A="}`, bad, 1))); err == nil {
			t.Fatalf("accepted %s", bad)
		}
	}
	for _, bad := range []string{`{"base64":"%%%"}`, `null`, `7`} {
		invalid := strings.Replace(raw, `"trailers":{"X-End":[{"base64":"/4A="},"tail",""]}`, `"trailers":{"X-End":[`+bad+`]}`, 1)
		if _, err := Parse([]byte(invalid)); err == nil {
			t.Fatal("accepted malformed trailer")
		}
	}
	if _, err := Parse([]byte(strings.Replace(raw, `"attempt_count":0`, `"attempt_count":11`, 1))); err == nil {
		t.Fatal("accepted invalid count")
	}
	if _, err := Parse([]byte(frame(true, false))); err != nil {
		t.Fatal(err)
	}
	if _, err := Parse([]byte(strings.Replace(raw, `"version":4`, `"version":3`, 1))); err == nil {
		t.Fatal("legacy accepted byte tags")
	}
}

func TestLosslessJournalRestartAndSnapshot(t *testing.T) {
	raw := strings.Replace(losslessFrame(), `"trailers":{}`, `"trailers":{"X-End":[{"base64":"/4A="},"tail",""]}`, 1)
	original, err := Parse([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	book := openBook(t, dir)
	if _, err := book.Put(original); err != nil {
		t.Fatal(err)
	}
	original.Headers["X-Bytes"][0] = "changed"
	original.Trailers["X-End"][0] = "changed"
	attempt, ok, err := book.BeginAttempt(requestID, endpointA)
	if err != nil || !ok {
		t.Fatalf("begin: %v", err)
	}
	decision, err := book.FinishAttempt(requestID, endpointA, attempt, Result{Outcome: OutcomeFailure, Error: "request failed"}, time.Now())
	if err != nil || !decision.Retry {
		t.Fatalf("finish: %v", err)
	}
	if err := book.Close(); err != nil {
		t.Fatal(err)
	}
	book = openBook(t, dir)
	defer book.Close()
	expected, err := Parse([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, ok := book.Arm(requestID)
	if !ok {
		t.Fatal("not armed")
	}
	if !reflect.DeepEqual(snapshot.Headers, expected.Headers) || !reflect.DeepEqual(snapshot.Trailers, expected.Trailers) {
		t.Fatal("bytes changed after restart")
	}
	snapshot.Headers["X-Bytes"][0] = "changed"
	snapshot.Trailers["X-End"][0] = "changed"
	stored := book.Stored()[0]
	if !reflect.DeepEqual(stored.Headers, expected.Headers) || !reflect.DeepEqual(stored.Trailers, expected.Trailers) {
		t.Fatal("snapshot mutated stored bytes")
	}
	// Legacy journals have no trailers field.
	legacyDir := t.TempDir()
	legacy := openBook(t, legacyDir)
	if _, err := legacy.Put(mustParse(t, frame(true, false))); err != nil {
		t.Fatal(err)
	}
	legacy.Close()
	bytes, err := os.ReadFile(filepath.Join(legacyDir, stateName))
	if err != nil {
		t.Fatal(err)
	}
	var file map[string]json.RawMessage
	if err := json.Unmarshal(bytes, &file); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(bytes), `"trailers"`) {
		t.Fatal("empty trailers must remain optional in the journal")
	}
	legacy = openBook(t, legacyDir)
	if len(legacy.Stored()) != 1 || len(legacy.Stored()[0].Trailers) != 0 {
		t.Fatal("legacy journal did not load")
	}
	legacy.Close()
}
