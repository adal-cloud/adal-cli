// Package session keeps the WebSocket connection to CLI Server.
package session

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/adal-cloud/adal-cli/internal/console"
	"github.com/adal-cloud/adal-cli/internal/deliver"
	"github.com/adal-cloud/adal-cli/internal/journal"
	"github.com/adal-cloud/adal-cli/internal/tunnel"
)

// socket writes a report to the connection that is open now.
type socket struct {
	mu   sync.Mutex
	ctx  context.Context
	conn link
	log  *log.Logger
}

// delivery starts and cancels endpoint calls.
// A nil delivery stores assignments and does not call endpoints.
type delivery interface {
	Submit(journal.Assignment)
	Cancel(string)
	Pause(string)
	Resume()
}

const (
	frameLimit   = 256 << 20
	sessionPath  = "/v1/session"
	helloVersion = 1
	frameVersion = 3
)

// Run dials the host's /v1/session and keeps the session until ctx is cancelled.
// Operational events are written to stdout.
// A nil stdout discards them.
// A fatal close returns an English error.
// That error does not include the token.
// A dial failure waits one second and tries again.
// A dropped connection waits one second and tries again.
// Close codes 1001 and 4002 wait one second and try again.
// Close 4001 does not connect again.
func Run(ctx context.Context, host, token string, stdout io.Writer) error {
	if len(token) == 32 && token[7] == 't' {
		address, err := URL(host)
		if err != nil {
			return err
		}
		return tunnel.Run(ctx, address, token, stdout)
	}
	if stdout == nil {
		stdout = io.Discard
	}
	dir, err := journal.Dir(host, token)
	if err != nil {
		return err
	}
	return run(ctx, host, token, frameLimit, time.Second, log.New(stdout, "", log.LstdFlags), dir)
}

func run(ctx context.Context, host, token string, limit int64, pause time.Duration, logger *log.Logger, dir string) error {
	url, err := URL(host)
	if err != nil {
		return err
	}
	if logger == nil {
		logger = log.New(io.Discard, "", log.LstdFlags)
	}
	logf := func(message string) { logger.Print(message) }
	book, err := journal.Open(dir)
	if err != nil {
		return err
	}
	defer book.Close()
	if err := book.Recover(time.Now().UTC()); err != nil {
		return err
	}
	client := deliver.NewClient()
	out := &socket{log: logger}
	work := deliver.NewScheduler(ctx, deliver.WallClock{}, func(ctx context.Context, assignment journal.Assignment, endpoint journal.Endpoint) journal.Result {
		return deliver.Call(ctx, client, assignment, endpoint)
	}, book, func(requestID string, result journal.Result, frame []byte) {
		out.sendFrame(book, requestID, result, frame)
	}, logger)
	for {
		if ctx.Err() != nil {
			logger.Print(console.Format(console.Event{Title: "Stopped"}))
			return nil
		}
		logger.Print(console.Format(console.Event{Title: "Connecting", Host: host}))
		err := connect(ctx, url, token, limit, book, work, out, logf)
		if fatal := fatalClose(err); fatal != nil {
			logger.Print(console.Format(console.Event{Title: "Connection failed", Reason: fatal.Error()}))
			return fatal
		}
		if ctx.Err() != nil {
			logger.Print(console.Format(console.Event{Title: "Stopped"}))
			return nil
		}
		if err != nil {
			logger.Print(console.Format(console.Event{Title: "Connection failed", Reason: err.Error()}))
		}
		logger.Print(console.Format(console.Event{Title: "Reconnecting"}))
		if err := wait(ctx, pause); err != nil {
			logger.Print(console.Format(console.Event{Title: "Stopped"}))
			return nil
		}
	}
}

type link interface {
	Read(context.Context) (websocket.MessageType, []byte, error)
	Write(context.Context, websocket.MessageType, []byte) error
}

func (s *socket) bind(ctx context.Context, conn link) {
	s.mu.Lock()
	s.ctx = ctx
	s.conn = conn
	s.mu.Unlock()
}

func (s *socket) unbind() {
	s.mu.Lock()
	s.ctx = nil
	s.conn = nil
	s.mu.Unlock()
}

func (s *socket) sendFrame(book *journal.Journal, requestID string, result journal.Result, frame []byte) {
	if len(frame) == 0 {
		s.print(console.Event{Title: "Report was not stored"})
		return
	}
	_, _, attemptID, _ := reportFields(frame)
	event := console.Event{
		Title:    "Report kept",
		Request:  requestID,
		Endpoint: callAddress(book, requestID, result.EndpointID),
		Attempt:  attemptID,
	}
	s.mu.Lock()
	ctx, conn := s.ctx, s.conn
	s.mu.Unlock()
	if conn == nil {
		s.print(event)
		return
	}
	if err := conn.Write(ctx, websocket.MessageText, frame); err != nil {
		s.print(event)
		return
	}
	event.Title = "Report sending"
	event.Outcome = result.Outcome
	s.print(event)
}

func (s *socket) print(e console.Event) {
	if s == nil || s.log == nil {
		return
	}
	s.log.Print(console.Format(e))
}

func emit(logf func(string), e console.Event) {
	if logf == nil {
		return
	}
	logf(console.Format(e))
}

func callAddress(book *journal.Journal, requestID, endpointID string) string {
	if book == nil || requestID == "" || endpointID == "" {
		return ""
	}
	for _, assignment := range book.Stored() {
		if assignment.RequestID != requestID {
			continue
		}
		for _, endpoint := range assignment.Endpoints {
			if endpoint.ID != endpointID {
				continue
			}
			address, ok := deliver.Address(endpoint.URL, assignment.Path, assignment.Query)
			if !ok {
				return ""
			}
			return address
		}
	}
	return ""
}

func connect(ctx context.Context, url, token string, limit int64, book *journal.Journal, work delivery, out *socket, logf func(string)) error {
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return err
	}
	defer conn.CloseNow()
	conn.SetReadLimit(limit)

	hello, err := json.Marshal(struct {
		Type    string `json:"type"`
		Version int    `json:"version"`
		Token   string `json:"token"`
	}{Type: "hello", Version: helloVersion, Token: token})
	if err != nil {
		return err
	}
	if err := conn.Write(ctx, websocket.MessageText, hello); err != nil {
		return err
	}
	emit(logf, console.Event{Title: "Connected"})
	out.bind(ctx, conn)
	defer out.unbind()
	return serve(ctx, conn, book, work, logf)
}

func serve(ctx context.Context, conn link, book *journal.Journal, work delivery, logf func(string)) error {
	if err := resend(ctx, conn, book, logf); err != nil {
		return err
	}
	if work != nil {
		work.Resume()
	}
	for {
		kind, data, err := conn.Read(ctx)
		if err != nil {
			return err
		}
		if kind != websocket.MessageText {
			return errors.New("binary frame")
		}
		if err := handleConnected(ctx, conn, book, work, logf, data); err != nil {
			return err
		}
	}
}

func resend(ctx context.Context, conn link, book *journal.Journal, logf func(string)) error {
	for _, frame := range book.PendingSendable() {
		requestID, endpointID, attemptID, _ := reportFields(frame)
		if err := conn.Write(ctx, websocket.MessageText, frame); err != nil {
			emit(logf, console.Event{
				Title:    "Report kept",
				Request:  requestID,
				Endpoint: callAddress(book, requestID, endpointID),
				Attempt:  attemptID,
			})
			return err
		}
		emit(logf, console.Event{Title: "Report resent", Request: requestID, Attempt: attemptID})
	}
	return nil
}

func handle(book *journal.Journal, work delivery, logf func(string), data []byte) error {
	return handleConnected(context.Background(), nil, book, work, logf, data)
}

func handleConnected(ctx context.Context, conn link, book *journal.Journal, work delivery, logf func(string), data []byte) error {
	switch frameType(data) {
	case "assignment":
		assignment, err := journal.Parse(data)
		if err != nil {
			emit(logf, console.Event{Title: "Ignored frame", Reason: err.Error()})
			return nil
		}
		emit(logf, console.Event{Title: "Assignment received", Request: assignment.RequestID})
		created, err := book.Put(assignment)
		if err != nil {
			emit(logf, console.Event{Title: "Assignment was not stored"})
			return nil
		}
		if (assignment.Version == 3 || assignment.Version == 4) && conn != nil {
			ack, _ := json.Marshal(map[string]any{"type": "assignment_ack", "version": 3, "request_id": assignment.RequestID})
			if err := conn.Write(ctx, websocket.MessageText, ack); err != nil {
				return err
			}
		}
		if armed, ok := book.Arm(assignment.RequestID); ok {
			if work != nil {
				work.Submit(armed)
			}
			return nil
		}
		if created && book.Contains(assignment.RequestID) && !book.HasEnabled(assignment.RequestID) {
			emit(logf, console.Event{Title: "Assignment has no enabled endpoint", Request: assignment.RequestID})
		}
		return nil
	case "error":
		if requestID, attemptID, code, ok := errorNotice(data); ok {
			if code == "snapshot_unavailable" && requestID != "" {
				if work != nil {
					work.Pause(requestID)
				}
				if err := persist(ctx, logf, func() error { return book.Pause(requestID, code) }); err != nil {
					return err
				}
				emit(logf, console.Event{Title: "Assignment unavailable", Request: requestID, Code: "snapshot_unavailable"})
				return nil
			}
			if attemptID != "" && permanentReport(code) {
				if err := persist(ctx, logf, func() error { return book.ForgetReport(requestID, attemptID) }); err != nil {
					return err
				}
				emit(logf, console.Event{Title: "Report stopped", Request: requestID, Attempt: attemptID, Code: code})
				return nil
			}
			if code == "unknown_assignment" && requestID != "" {
				if work != nil {
					work.Cancel(requestID)
				}
				if err := persist(ctx, logf, func() error { return book.Drop(requestID) }); err != nil {
					return err
				}
				emit(logf, console.Event{Title: "Assignment dropped", Request: requestID})
			}
		}
		return nil
	case "report_ack":
		if requestID, attemptID, ok := reportAck(data); ok {
			if err := persist(ctx, logf, func() error { return book.ForgetReport(requestID, attemptID) }); err != nil {
				return err
			}
			emit(logf, console.Event{Title: "Report accepted", Request: requestID, Attempt: attemptID})
		}
		return nil
	case "":
		if json.Valid(data) {
			emit(logf, console.Event{Title: "Ignored frame", Reason: "not an assignment"})
			return nil
		}
		emit(logf, console.Event{Title: "Ignored frame", Reason: "invalid json"})
		return nil
	default:
		emit(logf, console.Event{Title: "Ignored frame", Reason: "not an assignment"})
		return nil
	}
}

// Persist a server decision before the session advances.
// A transient disk failure does not acknowledge removal.
// A transient disk failure does not let stale work resume.
func persist(ctx context.Context, logf func(string), save func() error) error {
	for {
		if err := save(); err == nil {
			return nil
		}
		emit(logf, console.Event{Title: "Journal update was not stored"})
		if err := wait(ctx, time.Second); err != nil {
			return err
		}
	}
}

func reportFields(frame []byte) (requestID, endpointID, attemptID, outcome string) {
	var payload struct {
		RequestID  string `json:"request_id"`
		EndpointID string `json:"endpoint_id"`
		AttemptID  string `json:"attempt_id"`
		Outcome    string `json:"outcome"`
	}
	if json.Unmarshal(frame, &payload) != nil {
		return "", "", "", ""
	}
	return payload.RequestID, payload.EndpointID, payload.AttemptID, payload.Outcome
}

func frameType(data []byte) string {
	var frame struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(data, &frame) != nil {
		return ""
	}
	return frame.Type
}

func permanentReport(code string) bool {
	switch code {
	case "attempt_conflict", "unknown_assignment", "unknown_endpoint", "invalid_report":
		return true
	default:
		return false
	}
}

func errorNotice(data []byte) (requestID, attemptID, code string, ok bool) {
	var frame struct {
		Type      string `json:"type"`
		Version   int    `json:"version"`
		Code      string `json:"code"`
		RequestID string `json:"request_id"`
		AttemptID string `json:"attempt_id"`
	}
	if json.Unmarshal(data, &frame) != nil {
		return "", "", "", false
	}
	if frame.Type != "error" || frame.Version != frameVersion || frame.Code == "" {
		return "", "", "", false
	}
	return frame.RequestID, frame.AttemptID, frame.Code, true
}

func reportAck(data []byte) (string, string, bool) {
	var frame struct {
		Type      string `json:"type"`
		Version   int    `json:"version"`
		RequestID string `json:"request_id"`
		AttemptID string `json:"attempt_id"`
	}
	if json.Unmarshal(data, &frame) != nil {
		return "", "", false
	}
	if frame.Type != "report_ack" || frame.Version != frameVersion || frame.RequestID == "" || frame.AttemptID == "" {
		return "", "", false
	}
	return frame.RequestID, frame.AttemptID, true
}

func fatalClose(err error) error {
	switch websocket.CloseStatus(err) {
	case 4001:
		return errors.New("authentication failed")
	case 4003:
		return errors.New("access revoked")
	case 4004:
		return errors.New("token expired")
	case 4008:
		return errors.New("session replaced")
	default:
		return nil
	}
}

func wait(ctx context.Context, pause time.Duration) error {
	timer := time.NewTimer(pause)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
