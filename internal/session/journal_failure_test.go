package session

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/adal-cloud/adal-cli/internal/journal"
)

func TestJournalRemovalFailureWaitsForPersistence(t *testing.T) {
	for _, kind := range []string{"ack", "stopped", "drop"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			book, err := journal.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer book.Close()
			a := journal.Assignment{RequestID: reportRequest, Method: "POST", Path: "/", Endpoints: []journal.Endpoint{{ID: reportEndpoint, URL: "http://example.test", Enabled: true}}}
			if _, err := book.Put(a); err != nil {
				t.Fatal(err)
			}
			id, ok, err := book.BeginAttempt(reportRequest, reportEndpoint)
			if err != nil || !ok {
				t.Fatal(err)
			}
			if _, err := book.FinishAttempt(reportRequest, reportEndpoint, id, journal.Result{Outcome: journal.OutcomeSuccess, Status: 204}, time.Now()); err != nil {
				t.Fatal(err)
			}
			blocker := filepath.Join(dir, "state.json.tmp")
			if err := os.Mkdir(blocker, 0700); err != nil {
				t.Fatal(err)
			}
			frame := fmt.Sprintf(`{"type":"report_ack","version":3,"request_id":%q,"attempt_id":%q}`, reportRequest, id)
			success := "Report accepted"
			if kind == "stopped" {
				frame = fmt.Sprintf(`{"type":"error","version":3,"code":"invalid_report","request_id":%q,"attempt_id":%q}`, reportRequest, id)
				success = "Report stopped"
			}
			if kind == "drop" {
				frame = fmt.Sprintf(`{"type":"error","version":3,"code":"unknown_assignment","request_id":%q}`, reportRequest)
				success = "Assignment dropped"
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			logs := &safeBuf{}
			work := &fakeWork{}
			done := make(chan error, 1)
			go func() {
				done <- handleConnected(ctx, nil, book, work, func(line string) { logs.Write([]byte(line + "\n")) }, []byte(frame))
			}()
			select {
			case err := <-done:
				t.Fatalf("removal returned before persistence: %v; log=%s", err, logs.String())
			case <-time.After(40 * time.Millisecond):
			}
			if strings.Contains(logs.String(), success) {
				t.Fatal("logged removal before disk commit")
			}
			if kind == "drop" && len(work.cancelledIDs()) != 1 {
				t.Fatal("rejected assignment was not cancelled immediately")
			}
			if err := os.Remove(blocker); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("removal did not recover")
			}
			if len(book.Pending()) != 0 || !strings.Contains(logs.String(), success) {
				t.Fatalf("pending=%d log=%s", len(book.Pending()), logs.String())
			}
			if kind == "drop" && book.Contains(reportRequest) {
				t.Fatal("rejected assignment remains stored")
			}
		})
	}
}

func TestSnapshotUnavailablePersistsPauseAcrossRestart(t *testing.T) {
	for _, reportError := range []bool{false, true} {
		t.Run(fmt.Sprint(reportError), func(t *testing.T) {
			dir := t.TempDir()
			book, err := journal.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { book.Close() }()
			a := journal.Assignment{RequestID: reportRequest, Method: "POST", Path: "/", Endpoints: []journal.Endpoint{{ID: reportEndpoint, URL: "http://example.test", Enabled: true}}}
			if _, err := book.Put(a); err != nil {
				t.Fatal(err)
			}
			id, ok, err := book.BeginAttempt(reportRequest, reportEndpoint)
			if err != nil || !ok {
				t.Fatal(err)
			}
			if _, err := book.FinishAttempt(reportRequest, reportEndpoint, id, journal.Result{Outcome: journal.OutcomeFailure, Error: "timeout"}, time.Now()); err != nil {
				t.Fatal(err)
			}
			original := book.Pending()[0]
			attempt := ""
			if reportError {
				attempt = id
			}
			frame := fmt.Sprintf(`{"type":"error","version":3,"code":"snapshot_unavailable","request_id":%q,"attempt_id":%q}`, reportRequest, attempt)
			logs := &safeBuf{}
			work := &fakeWork{}
			if err := handle(book, work, func(line string) { logs.Write([]byte(line + "\n")) }, []byte(frame)); err != nil {
				t.Fatal(err)
			}
			if len(work.pausedIDs()) != 1 || len(work.cancelledIDs()) != 0 {
				t.Fatal("snapshot error must pause queued work without destructive cancellation")
			}
			if !strings.Contains(logs.String(), "Assignment unavailable") ||
				!strings.Contains(logs.String(), logField("request", reportRequest)) ||
				!strings.Contains(logs.String(), logField("code", "snapshot_unavailable")) {
				t.Fatalf("log=%s", logs.String())
			}
			if err := book.Close(); err != nil {
				t.Fatal(err)
			}
			book, err = journal.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := book.Recover(time.Now()); err != nil {
				t.Fatal(err)
			}
			if !book.Contains(reportRequest) || len(book.Pending()) != 1 || string(book.Pending()[0]) != string(original) {
				t.Fatal("pause lost assignment or diagnostic report")
			}
			if mode, _ := book.Plan(reportRequest, reportEndpoint, time.Now().Add(24*time.Hour)); mode != journal.PlanSkip {
				t.Fatalf("paused assignment resumed: %s", mode)
			}
			conn := newPipe()
			if err := resend(context.Background(), conn, book, func(string) {}); err != nil {
				t.Fatal(err)
			}
			select {
			case <-conn.out:
				t.Fatal("paused diagnostic report was resent")
			default:
			}
			if _, ok := book.Arm(reportRequest); ok {
				t.Fatal("paused assignment armed after restart")
			}
			if _, ok, err := book.BeginAttempt(reportRequest, reportEndpoint); err != nil || ok {
				t.Fatalf("paused assignment began: %t %v", ok, err)
			}
		})
	}
}

func (f *fakeWork) Pause(requestID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.paused = append(f.paused, requestID)
}

func (f *fakeWork) pausedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.paused...)
}
