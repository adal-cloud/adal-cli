package tunnel

import (
	"context"
	"encoding/json"
	pb "github.com/adal-cloud/adal-cli/internal/tunnelpb"
	"github.com/adal-cloud/adal-cli/internal/tunnelwire"
	"github.com/coder/websocket"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type sessionOutput struct {
	mu   sync.Mutex
	text strings.Builder
}

func (b *sessionOutput) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.text.Write(p)
}

func (b *sessionOutput) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.text.String()
}

func TestSessionExecutesWithoutJournal(t *testing.T) {
	const token = "aaaaaaataaaaaaaaaaaaaaaaaaaaaaaa"
	const requestBody = "private request body"
	const privateHeader = "private header value"
	const privateQuery = "private-query-value"
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/unavailable" {
			conn, _, err := http.NewResponseController(w).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = conn.Close()
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil || string(body) != requestBody || r.Header.Get("X-Private") != privateHeader {
			t.Error("request changed", err)
		}
		_, _ = io.WriteString(w, "live bytes")
	}))
	defer endpoint.Close()
	cfg := snapshot(endpoint.URL)
	var stdout sessionOutput
	done := make(chan error, 1)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			done <- err
			return
		}
		defer c.CloseNow()
		ctx, cancel := context.WithTimeout(r.Context(), 4*time.Second)
		defer cancel()
		_, raw, err := c.Read(ctx)
		if err != nil {
			done <- err
			return
		}
		var hello struct {
			Type         string
			Capabilities []string
		}
		if json.Unmarshal(raw, &hello) != nil || hello.Type != "hello" || len(hello.Capabilities) != 1 || hello.Capabilities[0] != "http_tunnel_v1" {
			t.Error("missing capability")
		}
		ctl := &pb.ControlFrame{Version: 1, ControlId: controlID, SessionId: sessionID, TunnelId: tunnelID, OwnerId: owner, RegionId: owner, Payload: &pb.ControlFrame_Prepare{Prepare: cfg}}
		writeControl := func() error {
			raw, err := tunnelwire.MarshalControl(ctl)
			if err != nil {
				return err
			}
			return c.Write(ctx, websocket.MessageBinary, raw)
		}
		if err = writeControl(); err != nil {
			done <- err
			return
		}
		_, raw, err = c.Read(ctx)
		if err != nil {
			done <- err
			return
		}
		reply, err := tunnelwire.UnmarshalControl(raw)
		if err != nil || reply.GetPrepared() == nil {
			t.Error("missing prepared")
			done <- err
			return
		}
		if got := stdout.String(); strings.Contains(got, " Connected\n") {
			t.Errorf("stdout before activation = %q", got)
		}
		ctl.Payload = &pb.ControlFrame_Activate{Activate: &pb.ActivateRevision{Next: &pb.RevisionState{Revision: cfg.Revision, ConfigHash: cfg.ConfigHash}}}
		for range 2 {
			if err = writeControl(); err != nil {
				done <- err
				return
			}
			_, raw, err = c.Read(ctx)
			if err != nil {
				done <- err
				return
			}
			reply, err = tunnelwire.UnmarshalControl(raw)
			if err != nil || reply.GetActive() == nil {
				t.Error("missing active")
				done <- err
				return
			}
		}
		open := opening(cfg, "POST", "/", "key="+privateQuery)
		open.GetOpen().Request.Headers = []*pb.Header{{Name: "X-Private", RawValues: [][]byte{[]byte(privateHeader)}}}
		raw, err = tunnelwire.MarshalExecute(open)
		if err == nil {
			err = c.Write(ctx, websocket.MessageBinary, raw)
		}
		if err != nil {
			done <- err
			return
		}
		_, raw, err = c.Read(ctx)
		if err != nil {
			done <- err
			return
		}
		ack, err := tunnelwire.UnmarshalExecute(raw)
		if err != nil || ack.GetOpenAck().GetResult() != pb.AdmissionResult_ADMISSION_RESULT_ACCEPTED {
			t.Error("missing accepted acknowledgement")
			done <- err
			return
		}
		data := &pb.ExecuteFrame{Version: 1, RequestId: open.RequestId, SessionId: sessionID, ControlId: controlID, Payload: &pb.ExecuteFrame_RequestData{RequestData: &pb.Data{SequenceNo: 1, Data: []byte(requestBody)}}}
		for _, frame := range []*pb.ExecuteFrame{data, input(open, &pb.End{Complete: true})} {
			raw, err = tunnelwire.MarshalExecute(frame)
			if err == nil {
				err = c.Write(ctx, websocket.MessageBinary, raw)
			}
			if err != nil {
				done <- err
				return
			}
		}
		var body []byte
		for {
			_, raw, err = c.Read(ctx)
			if err != nil {
				done <- err
				return
			}
			f, err := tunnelwire.UnmarshalExecute(raw)
			if err != nil {
				done <- err
				return
			}
			if f.GetResponseData() != nil {
				body = append(body, f.GetResponseData().Data...)
			}
			if f.GetResponseEnd() != nil {
				break
			}
		}
		if string(body) != "live bytes" {
			t.Error("response changed")
		}
		failed := opening(cfg, "GET", "/unavailable", "")
		failed.RequestId = "44444444-4444-4444-8444-444444444444"
		zero := int64(0)
		failed.GetOpen().Request.ContentLength = &zero
		raw, err = tunnelwire.MarshalExecute(failed)
		if err == nil {
			err = c.Write(ctx, websocket.MessageBinary, raw)
		}
		if err != nil {
			done <- err
			return
		}
		for {
			_, raw, err = c.Read(ctx)
			if err != nil {
				done <- err
				return
			}
			frame, err := tunnelwire.UnmarshalExecute(raw)
			if err != nil {
				done <- err
				return
			}
			if termination := frame.GetError(); termination != nil {
				if frame.RequestId != failed.RequestId || termination.Code != pb.ErrorCode_ERROR_CODE_ENDPOINT_UNAVAILABLE {
					t.Error("unexpected endpoint failure", termination.Code)
				}
				break
			}
		}
		late := &pb.ExecuteFrame{Version: 1, RequestId: open.RequestId, SessionId: sessionID, ControlId: controlID, Payload: &pb.ExecuteFrame_Cancel{Cancel: &pb.Termination{Direction: pb.Direction_DIRECTION_BOTH, Code: pb.ErrorCode_ERROR_CODE_CLIENT_DISCONNECTED}}}
		raw, err = tunnelwire.MarshalExecute(late)
		if err == nil {
			err = c.Write(ctx, websocket.MessageBinary, raw)
		}
		if err != nil {
			done <- err
			return
		}
		ctl.Payload = &pb.ControlFrame_Reconcile{Reconcile: &pb.Reconcile{}}
		if err = writeControl(); err != nil {
			done <- err
			return
		}
		_, raw, err = c.Read(ctx)
		if err != nil {
			done <- err
			return
		}
		reply, err = tunnelwire.UnmarshalControl(raw)
		if err != nil || reply.GetState() == nil || !reply.GetState().Ready {
			t.Error("late Cancel closed the session")
			done <- err
			return
		}
		_ = c.Close(websocket.StatusCode(4001), "Token is invalid")
		done <- nil
	}))
	defer peer.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Second)
	defer cancel()
	err := Run(ctx, "ws"+strings.TrimPrefix(peer.URL, "http"), token, &stdout)
	if err == nil || err.Error() != "Token is invalid" {
		t.Fatal("fatal close", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	got := stdout.String()
	for _, want := range []string{
		" Connecting\n", "host        ws", " WebSocket connected\n", " Tunnel configuration prepared\n",
		" Tunnel configuration activated\n", " Tunnel state reconciled\n",
		" Tunnel request received\n", requestID, " Tunnel request accepted\n", " Tunnel request data received\n",
		"bytes       20", " Tunnel request body received\n", " Tunnel response started\n", "HTTP 200",
		" Tunnel response data sent\n", "bytes       10", " Tunnel request finished\n",
		" Tunnel request failed\n", "ERROR_CODE_ENDPOINT_UNAVAILABLE",
		" Connection failed\n", "Token is invalid",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("stdout missing %q", want)
		}
	}
	if strings.Count(got, " Connected\n") != 1 {
		t.Errorf("Connected must appear once per activated session")
	}
	for _, secret := range []string{token, requestBody, privateHeader, privateQuery, "live bytes"} {
		if strings.Contains(got, secret) {
			t.Error("stdout contains private data")
		}
	}
}

func TestConnectionFailureLogsRetryAndShutdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var attempts atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			http.Error(w, "private handshake response", http.StatusServiceUnavailable)
			return
		}
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer c.CloseNow()
		if _, _, err := c.Read(r.Context()); err != nil {
			t.Error(err)
			return
		}
		cancel()
	}))
	defer peer.Close()

	var stdout strings.Builder
	err := Run(ctx, "ws"+strings.TrimPrefix(peer.URL, "http"), "aaaaaaataaaaaaaaaaaaaaaaaaaaaaaa", &stdout)
	if err != nil {
		t.Fatal(err)
	}
	got := stdout.String()
	if attempts.Load() != 2 || strings.Count(got, " Connecting\n") != 2 || strings.Count(got, " Connection failed\n") != 1 {
		t.Fatalf("unexpected retry sequence: %d attempts", attempts.Load())
	}
	for _, want := range []string{"WebSocket handshake failed", " Reconnecting\n", "delay       ", " WebSocket connected\n", " Stopped\n"} {
		if !strings.Contains(got, want) {
			t.Errorf("stdout missing %q", want)
		}
	}
	if strings.Contains(got, "private handshake response") {
		t.Error("stdout contains private handshake data")
	}
}
