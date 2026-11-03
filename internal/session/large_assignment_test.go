package session

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/adal-cloud/adal-cli/internal/journal"
	"github.com/coder/websocket"
)

func TestLargeAssignmentIsDeliveredAndAcknowledged(t *testing.T) {
	const bodySize = 24 << 20
	wantHash := sha256.Sum256(bytes.Repeat([]byte{'a'}, bodySize))
	called := make(chan error, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hash := sha256.New()
		n, err := io.Copy(hash, r.Body)
		if err == nil && (n != bodySize || !bytes.Equal(hash.Sum(nil), wantHash[:])) {
			err = fmt.Errorf("HTTP body changed: received %d bytes", n)
		}
		called <- err
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()
	var assignment map[string]any
	if err := json.Unmarshal([]byte(callAssignment(reportRequest, reportEndpoint, target.URL)), &assignment); err != nil {
		t.Fatal(err)
	}
	assignment["body"] = strings.Repeat("YWFh", bodySize/3)
	frame, err := json.Marshal(assignment)
	if err != nil {
		t.Fatal(err)
	}
	if len(frame) <= 32<<20 {
		t.Fatal("regression fixture must exceed the former receive limit")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	exchanged := make(chan error, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			exchanged <- err
			return
		}
		defer conn.CloseNow()
		err = func() error {
			if _, _, err := conn.Read(ctx); err != nil {
				return err
			}
			if err := conn.Write(ctx, websocket.MessageText, frame); err != nil {
				return err
			}
			_, raw, err := conn.Read(ctx)
			if err != nil {
				return fmt.Errorf("assignment acknowledgement: %w", err)
			}
			ack := decodeFrame(t, raw)
			if ack["type"] != "assignment_ack" || ack["version"] != float64(3) || ack["request_id"] != reportRequest {
				return fmt.Errorf("invalid assignment acknowledgement")
			}
			_, raw, err = conn.Read(ctx)
			if err != nil {
				return fmt.Errorf("delivery report: %w", err)
			}
			report := decodeFrame(t, raw)
			if report["type"] != "report" || report["version"] != float64(3) || report["request_id"] != reportRequest || report["endpoint_id"] != reportEndpoint || report["outcome"] != "success" || report["http_status"] != float64(204) {
				return fmt.Errorf("invalid delivery report")
			}
			ackFrame, _ := json.Marshal(map[string]any{"type": "report_ack", "version": 3, "request_id": reportRequest, "attempt_id": report["attempt_id"]})
			return conn.Write(ctx, websocket.MessageText, ackFrame)
		}()
		exchanged <- err
		if err != nil {
			cancel()
		}
		_ = conn.Close(4001, "")
	}))
	defer srv.Close()
	dir := t.TempDir()
	err = run(ctx, hostOf(srv), "secret", frameLimit, time.Hour, nil, dir)
	if err := <-exchanged; err != nil {
		t.Fatal(err)
	}
	if err == nil || err.Error() != "authentication failed" {
		t.Fatalf("session did not finish the exchange: %v", err)
	}
	select {
	case err := <-called:
		if err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatal("HTTP call was not made")
	}
	book, err := journal.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer book.Close()
	if !book.Contains(reportRequest) || len(book.Pending()) != 0 {
		t.Fatal("assignment or report acknowledgement was not persisted")
	}
}
