package journal

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestDirSeparatesChannelsWithoutTheToken(t *testing.T) {
	const token = "alpha-token"
	one, err := Dir("127.0.0.1:8009", token)
	if err != nil {
		t.Fatal(err)
	}
	two, err := Dir("127.0.0.1:8009", "beta-token")
	if err != nil {
		t.Fatal(err)
	}
	three, err := Dir("10.0.0.1:8009", token)
	if err != nil {
		t.Fatal(err)
	}
	if one == two || one == three || two == three {
		t.Fatalf("dirs = %s %s %s", one, two, three)
	}
	if strings.Contains(one, token) || strings.Contains(filepath.Base(one), token) {
		t.Fatalf("token leaked into %s", one)
	}
}

func TestJournalSurvivesRestart(t *testing.T) {
	const token = "not-stored"
	dir := t.TempDir()
	book := openBook(t, dir)
	assignment := mustParse(t, frame(true, false))
	if _, err := book.Put(assignment); err != nil {
		t.Fatal(err)
	}
	if err := book.Close(); err != nil {
		t.Fatal(err)
	}

	reloaded := openBook(t, dir)
	defer reloaded.Close()
	if !reloaded.Contains(requestID) {
		t.Fatal("assignment was lost")
	}
	body, ok := reloaded.Body(requestID)
	if !ok || body != "YQ==" {
		t.Fatalf("body = %q ok=%v", body, ok)
	}
	raw, err := os.ReadFile(filepath.Join(dir, stateName))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), token) {
		t.Fatal("token leaked into the journal")
	}
	if strings.Contains(string(raw), "trailers") || strings.Contains(string(raw), "deliver_backlog") || strings.Contains(string(raw), "received_at") || strings.Contains(string(raw), "attempt_count") || strings.Contains(string(raw), "recipients") {
		t.Fatal("unused assignment fields were stored")
	}
	if _, err := os.Stat(filepath.Join(dir, tempName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary file remained: %v", err)
	}
	if runtime.GOOS != "windows" {
		info, err := os.Stat(dir)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o700 {
			t.Fatalf("dir mode = %o", info.Mode().Perm())
		}
		info, err = os.Stat(filepath.Join(dir, stateName))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm()&0o077 != 0 {
			t.Fatalf("file mode = %o", info.Mode().Perm())
		}
	}
}

func TestTemporaryFileIsIgnored(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, tempName), []byte(`{"assignments":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	book := openBook(t, dir)
	if book.Contains(requestID) {
		t.Fatal("temporary file was loaded")
	}
	book.Close()

	book = openBook(t, dir)
	assignment := mustParse(t, frame(true, false))
	if _, err := book.Put(assignment); err != nil {
		t.Fatal(err)
	}
	book.Close()
	if err := os.WriteFile(filepath.Join(dir, tempName), []byte("{not-the-journal"), 0o600); err != nil {
		t.Fatal(err)
	}
	book = openBook(t, dir)
	defer book.Close()
	if !book.Contains(requestID) {
		t.Fatal("state was replaced by the temporary file")
	}
}

func TestCorruptJournalIsRejected(t *testing.T) {
	for _, body := range []string{"", "{", "null"} {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, stateName), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := Open(dir)
		if !errors.Is(err, ErrCorrupt) {
			t.Fatalf("Open(%q) error = %v", body, err)
		}
	}
}

func TestSecondOpenerIsBusy(t *testing.T) {
	dir := t.TempDir()
	first := openBook(t, dir)
	_, err := Open(dir)
	if !errors.Is(err, ErrBusy) {
		t.Fatalf("error = %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second := openBook(t, dir)
	second.Close()
}

func TestChannelsDoNotShareState(t *testing.T) {
	left := openBook(t, t.TempDir())
	right := openBook(t, t.TempDir())
	defer left.Close()
	defer right.Close()
	if _, err := left.Put(mustParse(t, frame(true, false))); err != nil {
		t.Fatal(err)
	}
	if right.Contains(requestID) || !left.Contains(requestID) {
		t.Fatal("journals shared an assignment")
	}
}

func TestSaveFailureKeepsThePreviousState(t *testing.T) {
	dir := t.TempDir()
	book := openBook(t, dir)
	if _, err := book.Put(mustParse(t, frame(true, false))); err != nil {
		t.Fatal(err)
	}
	book.saver = func([]byte) error { return errors.New("disk full") }
	second := mustParse(t, strings.ReplaceAll(frame(true, false), requestID, "018f3c2e-7b1a-7c3d-8e4f-abcdefabcdef"))
	if _, err := book.Put(second); err == nil {
		t.Fatal("save failure was ignored")
	}
	if book.Contains(second.RequestID) || !book.Contains(requestID) {
		t.Fatal("failed save changed the journal")
	}
	if _, _, err := book.BeginAttempt(requestID, endpointA); err == nil {
		t.Fatal("failed attempt reservation was accepted")
	}
	book.Close()

	reloaded := openBook(t, dir)
	defer reloaded.Close()
	if reloaded.Contains(second.RequestID) || !reloaded.Contains(requestID) {
		t.Fatal("disk kept a failed write")
	}
	if mode, _ := reloaded.Plan(requestID, endpointA, time.Unix(0, 0)); mode != PlanNow {
		t.Fatalf("plan = %s", mode)
	}
}

func TestRepeatedAssignmentKeepsTheFirstSnapshot(t *testing.T) {
	book := New()
	first := mustParse(t, frame(true, false))
	if _, err := book.Put(first); err != nil {
		t.Fatal(err)
	}
	changed := first
	changed.Endpoints = append([]Endpoint(nil), first.Endpoints...)
	changed.Endpoints[0].URL = "http://127.0.0.1:9/other"
	if created, err := book.Put(changed); err != nil || created {
		t.Fatalf("created = %v err %v", created, err)
	}
	stored := book.Stored()
	if len(stored) != 1 || stored[0].Endpoints[0].URL != first.Endpoints[0].URL {
		t.Fatalf("snapshot = %+v", stored)
	}
}

func TestUncertainAttemptConsumesBudgetAndReportsInterruption(t *testing.T) {
	dir := t.TempDir()
	now := time.Unix(1_700_000_000, 0).UTC()
	book := openBook(t, dir)
	if _, err := book.Put(mustParse(t, frame(true, false))); err != nil {
		t.Fatal(err)
	}
	for n := 1; n <= attemptBudget; n++ {
		id, ok, err := book.BeginAttempt(requestID, endpointA)
		if err != nil || !ok || id == "" {
			t.Fatalf("begin %d: ok=%v err=%v", n, ok, err)
		}
		if err := book.Close(); err != nil {
			t.Fatal(err)
		}
		book = openBook(t, dir)
		if len(book.Pending()) != n-1 {
			t.Fatal("previous interruption reports were lost")
		}
		if err := book.Recover(now); err != nil {
			t.Fatal(err)
		}
		pending := book.Pending()
		var report struct {
			AttemptID string `json:"attempt_id"`
			Number    int    `json:"attempt_number"`
			Error     string `json:"error"`
		}
		if len(pending) != n || json.Unmarshal(pending[n-1], &report) != nil || report.AttemptID != id || report.Number != n || report.Error != "attempt interrupted" {
			t.Fatal("missing interruption report", report)
		}
		if err := book.Recover(now); err != nil || len(book.Pending()) != n {
			t.Fatal("recovery duplicated reports", err)
		}
		mode, _ := book.Plan(requestID, endpointA, now)
		if n < attemptBudget && mode != PlanNow {
			t.Fatalf("after %d uncertain attempts plan = %s", n, mode)
		}
		if n == attemptBudget && mode != PlanSkip {
			t.Fatalf("exhausted plan = %s", mode)
		}
	}
	book.Close()
}

func TestReportAckKeepsDeliveryProgress(t *testing.T) {
	book := New()
	if _, err := book.Put(mustParse(t, frame(true, false))); err != nil {
		t.Fatal(err)
	}
	at := time.Unix(1_700_000_000, 0).UTC()
	id, ok, err := book.BeginAttempt(requestID, endpointA)
	if err != nil || !ok {
		t.Fatal(err)
	}
	decision, err := book.FinishAttempt(requestID, endpointA, id, Result{
		Outcome: OutcomeFailure, Status: 500, Error: "unexpected status",
	}, at)
	if err != nil || !decision.Retry {
		t.Fatalf("decision = %+v err %v", decision, err)
	}
	before := append([]byte(nil), book.Pending()[0]...)
	if err := book.ForgetReport(requestID, id); err != nil {
		t.Fatal(err)
	}
	if len(book.Pending()) != 0 {
		t.Fatal("acknowledged report was kept")
	}
	results := book.Results(requestID)
	if len(results) != 1 || results[0].Status != 500 {
		t.Fatalf("results = %+v", results)
	}
	if mode, due := book.Plan(requestID, endpointA, at); mode != PlanLater || !due.Equal(decision.Due) {
		t.Fatalf("plan = %s due %s", mode, due)
	}
	if !bytesEqual(before, decision.Frame) {
		t.Fatal("stored report differed from the returned frame")
	}
}

func TestReportBytesStayStableAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	book := openBook(t, dir)
	if _, err := book.Put(mustParse(t, frame(true, false))); err != nil {
		t.Fatal(err)
	}
	id, ok, err := book.BeginAttempt(requestID, endpointA)
	if err != nil || !ok {
		t.Fatal(err)
	}
	decision, err := book.FinishAttempt(requestID, endpointA, id, Result{
		Outcome: OutcomeSuccess, Status: 204,
	}, time.Unix(0, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	book.Close()

	reloaded := openBook(t, dir)
	defer reloaded.Close()
	pending := reloaded.Pending()
	if len(pending) != 1 || string(pending[0]) != string(decision.Frame) {
		t.Fatalf("pending = %s", pending)
	}
	if !strings.Contains(string(pending[0]), id) || !strings.Contains(string(pending[0]), `"attempt_number":1`) {
		t.Fatalf("frame = %s", pending[0])
	}
	if mode, _ := reloaded.Plan(requestID, endpointA, time.Unix(0, 0)); mode != PlanSkip {
		t.Fatalf("successful endpoint plan = %s", mode)
	}
}

func TestAttemptCountDoesNotRegressLocalProgress(t *testing.T) {
	book := New()
	if _, err := book.Put(mustParse(t, frame(true, false))); err != nil {
		t.Fatal(err)
	}
	at := time.Unix(1_700_000_000, 0).UTC()
	id, ok, err := book.BeginAttempt(requestID, endpointA)
	if err != nil || !ok {
		t.Fatal(err)
	}
	decision, err := book.FinishAttempt(requestID, endpointA, id, Result{
		Outcome: OutcomeFailure, Status: 500, Error: "unexpected status",
	}, at)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := book.Put(mustParse(t, frame(true, false))); err != nil {
		t.Fatal(err)
	}
	if mode, due := book.Plan(requestID, endpointA, at); mode != PlanLater || !due.Equal(decision.Due) {
		t.Fatalf("plan = %s due %s, want %s", mode, due, decision.Due)
	}
	if len(book.Results(requestID)) != 1 {
		t.Fatal("server count removed a local attempt")
	}

	ahead := strings.ReplaceAll(frame(true, false), `"attempt_count":0`, `"attempt_count":3`)
	if _, err := book.Put(mustParse(t, ahead)); err != nil {
		t.Fatal(err)
	}
	if mode, due := book.Plan(requestID, endpointA, at); mode != PlanLater || !due.Equal(decision.Due) {
		t.Fatalf("plan = %s due %s, want %s", mode, due, decision.Due)
	}
	nextID, ok, err := book.BeginAttempt(requestID, endpointA)
	if err != nil || !ok {
		t.Fatal(err)
	}
	next, err := book.FinishAttempt(requestID, endpointA, nextID, Result{
		Outcome: OutcomeFailure, Status: 502, Error: "unexpected status",
	}, at)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(next.Frame), `"attempt_number":4`) {
		t.Fatalf("frame = %s", next.Frame)
	}
	otherID, ok, err := book.BeginAttempt(requestID, endpointB)
	if err != nil || !ok {
		t.Fatal(err)
	}
	other, err := book.FinishAttempt(requestID, endpointB, otherID, Result{
		Outcome: OutcomeFailure, Error: "timeout",
	}, at)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(other.Frame), `"attempt_number":4`) {
		t.Fatalf("frame = %s", other.Frame)
	}

	exhausted := strings.ReplaceAll(frame(true, false), `"attempt_count":0`, `"attempt_count":10`)
	if _, err := book.Put(mustParse(t, exhausted)); err != nil {
		t.Fatal(err)
	}
	if mode, _ := book.Plan(requestID, endpointA, at.Add(24*time.Hour)); mode != PlanSkip {
		t.Fatalf("exhausted plan = %s", mode)
	}
	if _, ok, err := book.BeginAttempt(requestID, endpointA); err != nil || ok {
		t.Fatalf("begin after exhaustion ok=%v err=%v", ok, err)
	}
}

func TestRepeatedAssignmentKeepsTheOpenAttempt(t *testing.T) {
	book := New()
	if _, err := book.Put(mustParse(t, frame(true, false))); err != nil {
		t.Fatal(err)
	}
	id, ok, err := book.BeginAttempt(requestID, endpointA)
	if err != nil || !ok {
		t.Fatal(err, ok)
	}
	ahead := strings.ReplaceAll(frame(true, false), `"attempt_count":0`, `"attempt_count":10`)
	if _, err := book.Put(mustParse(t, ahead)); err != nil {
		t.Fatal(err)
	}
	decision, err := book.FinishAttempt(requestID, endpointA, id, Result{Outcome: OutcomeFailure, Error: "timeout"}, time.Unix(0, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(decision.Frame), `"attempt_number":1`) || !strings.Contains(string(decision.Frame), `"error":"timeout"`) {
		t.Fatalf("frame = %s", decision.Frame)
	}
}

func TestModeChangeFailureStillPublishesTheJournal(t *testing.T) {
	previous := chmodFile
	chmodFile = func(string, os.FileMode) error { return errors.New("mode") }
	t.Cleanup(func() { chmodFile = previous })
	dir := t.TempDir()
	book := openBook(t, dir)
	defer book.Close()
	if _, err := book.Put(mustParse(t, frame(true, false))); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, stateName)); err != nil {
		t.Fatal(err)
	}
}

func TestFreshAttemptCountSetsTheNextReportNumber(t *testing.T) {
	book := New()
	body := strings.ReplaceAll(frame(true, false), `"attempt_count":0`, `"attempt_count":2`)
	if _, err := book.Put(mustParse(t, body)); err != nil {
		t.Fatal(err)
	}
	id, ok, err := book.BeginAttempt(requestID, endpointA)
	if err != nil || !ok {
		t.Fatal(err)
	}
	decision, err := book.FinishAttempt(requestID, endpointA, id, Result{
		Outcome: OutcomeSuccess, Status: 204,
	}, time.Unix(0, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(decision.Frame), `"attempt_number":3`) {
		t.Fatalf("frame = %s", decision.Frame)
	}
}

func openBook(t *testing.T, dir string) *Journal {
	t.Helper()
	book, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	return book
}

func mustParse(t *testing.T, body string) Assignment {
	t.Helper()
	assignment, err := Parse([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	return assignment
}

func bytesEqual(left, right []byte) bool {
	return string(left) == string(right)
}
