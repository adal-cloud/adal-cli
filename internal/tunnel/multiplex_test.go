package tunnel

import (
	"context"
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

func configurePeer(ctx context.Context, c *websocket.Conn, cfg *pb.ConfigSnapshot) error {
	if _, _, err := c.Read(ctx); err != nil {
		return err
	}
	f := &pb.ControlFrame{Version: 1, ControlId: controlID, SessionId: sessionID, TunnelId: tunnelID, OwnerId: owner, RegionId: owner, Payload: &pb.ControlFrame_Prepare{Prepare: cfg}}
	for _, activate := range []bool{false, true} {
		if activate {
			f.Payload = &pb.ControlFrame_Activate{Activate: &pb.ActivateRevision{Next: &pb.RevisionState{Revision: cfg.Revision, ConfigHash: cfg.ConfigHash}}}
		}
		raw, err := tunnelwire.MarshalControl(f)
		if err != nil {
			return err
		}
		if err = c.Write(ctx, websocket.MessageBinary, raw); err != nil {
			return err
		}
		_, raw, err = c.Read(ctx)
		if err != nil {
			return err
		}
		if _, err = tunnelwire.UnmarshalControl(raw); err != nil {
			return err
		}
	}
	return nil
}
func TestSlowRequestDoesNotBlockFast(t *testing.T) {
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			_, _ = io.WriteString(w, strings.Repeat("x", 512<<10))
			return
		}
		_, _ = io.WriteString(w, "fast")
	}))
	defer endpoint.Close()
	cfg := snapshot(endpoint.URL)
	result := make(chan error, 1)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			result <- err
			return
		}
		defer c.CloseNow()
		c.SetReadLimit(tunnelwire.MaxFrameBytes)
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if err = configurePeer(ctx, c, cfg); err != nil {
			result <- err
			return
		}
		frames := make(chan *pb.ExecuteFrame, 64)
		readErr := make(chan error, 1)
		go func() {
			for {
				_, raw, err := c.Read(ctx)
				if err != nil {
					readErr <- err
					return
				}
				f, err := tunnelwire.UnmarshalExecute(raw)
				if err != nil {
					readErr <- err
					return
				}
				select {
				case frames <- f:
				case <-ctx.Done():
					return
				}
			}
		}()
		write := func(f *pb.ExecuteFrame) error {
			raw, err := tunnelwire.MarshalExecute(f)
			if err != nil {
				return err
			}
			return c.Write(ctx, websocket.MessageBinary, raw)
		}
		next := func() (*pb.ExecuteFrame, error) {
			select {
			case f := <-frames:
				return f, nil
			case err := <-readErr:
				return nil, err
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		slow := opening(cfg, "GET", "/slow", "")
		zero := int64(0)
		slow.GetOpen().Request.ContentLength = &zero
		if err = write(slow); err != nil {
			result <- err
			return
		}
		received := 0
		for received < 256<<10 {
			f, err := next()
			if err != nil {
				result <- err
				return
			}
			if f.GetError() != nil {
				t.Error("slow request failed before consuming its window")
				result <- errProtocol
				return
			}
			if f.GetResponseData() != nil {
				received += len(f.GetResponseData().Data)
			}
		}
		fast := opening(cfg, "GET", "/fast", "")
		fast.RequestId = owner
		fast.GetOpen().Request.ContentLength = &zero
		if err = write(fast); err != nil {
			result <- err
			return
		}
		var body strings.Builder
		for {
			f, err := next()
			if err != nil {
				result <- err
				return
			}
			if f.RequestId == slow.RequestId {
				if f.GetResponseData() != nil {
					t.Error("response exceeded its credit")
					result <- errProtocol
					return
				}
				continue
			}
			if f.GetResponseData() != nil {
				body.Write(f.GetResponseData().Data)
			}
			if f.GetResponseEnd() != nil {
				break
			}
		}
		if body.String() != "fast" {
			t.Error("fast response blocked or changed")
		}
		pong := make(chan error, 1)
		go func() { pong <- c.Ping(ctx) }()
		select {
		case err = <-pong:
			if err != nil {
				result <- err
				return
			}
		case <-time.After(time.Second):
			t.Error("Pong blocked by full window")
			result <- errProtocol
			return
		}
		slow.Payload = &pb.ExecuteFrame_Cancel{Cancel: &pb.Termination{Direction: pb.Direction_DIRECTION_BOTH, Code: pb.ErrorCode_ERROR_CODE_CLIENT_DISCONNECTED}}
		if err = write(slow); err != nil {
			result <- err
			return
		}
		_ = c.Close(websocket.StatusCode(4001), "Token is invalid")
		result <- nil
	}))
	defer peer.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 7*time.Second)
	defer cancel()
	err := Run(ctx, "ws"+strings.TrimPrefix(peer.URL, "http"), "aaaaaaataaaaaaaaaaaaaaaaaaaaaaaa", io.Discard)
	if err == nil || err.Error() != "Token is invalid" {
		select {
		case e := <-result:
			t.Log("peer result", e)
		default:
		}
		t.Fatal("session failed", err)
	}
	if err = <-result; err != nil {
		t.Fatal(err)
	}
}
