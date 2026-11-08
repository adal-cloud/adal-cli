package session

import (
	"context"
	"encoding/json"
	"github.com/adal-cloud/adal-cli/internal/journal"
	"strings"
	"testing"
	"time"
)

func TestAssignment4KeepsVersion3Acknowledgement(t *testing.T) {
	book := journal.New()
	work := &fakeWork{}
	link := newPipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = serve(ctx, link, book, work, func(string) {}) }()
	raw := strings.Replace(assignmentFrame(reportRequest, true), `"version":3`, `"version":4`, 1)
	for i := 0; i < 2; i++ {
		link.in <- []byte(raw)
		select {
		case bytes := <-link.out:
			var ack struct {
				Type      string `json:"type"`
				Version   int    `json:"version"`
				RequestID string `json:"request_id"`
			}
			if err := json.Unmarshal(bytes, &ack); err != nil || ack.Type != "assignment_ack" || ack.Version != 3 || ack.RequestID != reportRequest {
				t.Fatalf("ack=%s err=%v", bytes, err)
			}
			if !book.Contains(reportRequest) {
				t.Fatal("ack preceded storage")
			}
		case <-time.After(time.Second):
			t.Fatal("assignment4 was not acknowledged")
		}
	}
	waitUntil(t, func() bool { return len(work.submittedIDs()) == 1 })
}
