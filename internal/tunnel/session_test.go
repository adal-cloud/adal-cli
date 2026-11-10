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
	"testing"
	"time"
)

func TestSessionExecutesWithoutJournal(t *testing.T) {
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "live bytes") }))
	defer endpoint.Close()
	cfg := snapshot(endpoint.URL)
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
		ctl.Payload = &pb.ControlFrame_Activate{Activate: &pb.ActivateRevision{Next: &pb.RevisionState{Revision: cfg.Revision, ConfigHash: cfg.ConfigHash}}}
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
		open := opening(cfg, "GET", "/", "")
		zero := int64(0)
		open.GetOpen().Request.ContentLength = &zero
		raw, err = tunnelwire.MarshalExecute(open)
		if err == nil {
			err = c.Write(ctx, websocket.MessageBinary, raw)
		}
		if err != nil {
			done <- err
			return
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
	err := Run(ctx, "ws"+strings.TrimPrefix(peer.URL, "http"), "aaaaaaataaaaaaaaaaaaaaaaaaaaaaaa", io.Discard)
	if err == nil || err.Error() != "Token is invalid" {
		t.Fatal("fatal close", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
