package tunnel

import (
	"bytes"
	"context"
	pb "github.com/adal-cloud/adal-cli/internal/tunnelpb"
	"github.com/adal-cloud/adal-cli/internal/tunnelwire"
	"github.com/coder/websocket"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDisabledOrDeletedSessionDoesNotReconnect(t *testing.T) {
	for _, tt := range []struct {
		reason  pb.SessionCloseReason
		message string
	}{{pb.SessionCloseReason_SESSION_CLOSE_REASON_TUNNEL_DISABLED, "Tunnel is disabled"}, {pb.SessionCloseReason_SESSION_CLOSE_REASON_TUNNEL_DELETED, "Tunnel is deleted"}} {
		t.Run(tt.message, func(t *testing.T) {
			for _, typed := range []bool{false, true} {
				t.Run(map[bool]string{false: "websocket", true: "typed_frame"}[typed], func(t *testing.T) {
					var connections atomic.Int32
					peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						connections.Add(1)
						c, err := websocket.Accept(w, r, nil)
						if err != nil {
							return
						}
						defer c.CloseNow()
						if _, _, err = c.Read(r.Context()); err != nil {
							return
						}
						if !typed {
							reason := "tunnel_disabled"
							if tt.reason == pb.SessionCloseReason_SESSION_CLOSE_REASON_TUNNEL_DELETED {
								reason = "tunnel_deleted"
							}
							_ = c.Close(websocket.StatusCode(4003), reason)
							return
						}
						reconcile := &pb.ControlFrame{Version: 1, ControlId: controlID, SessionId: sessionID, TunnelId: tunnelID, OwnerId: owner, RegionId: owner, Payload: &pb.ControlFrame_Reconcile{Reconcile: &pb.Reconcile{}}}
						raw, err := tunnelwire.MarshalControl(reconcile)
						if err != nil {
							t.Error(err)
							return
						}
						if err = c.Write(r.Context(), websocket.MessageBinary, raw); err != nil {
							return
						}
						if _, _, err = c.Read(r.Context()); err != nil {
							return
						}
						frame := &pb.ControlFrame{Version: 1, ControlId: controlID, SessionId: sessionID, TunnelId: tunnelID, OwnerId: owner, RegionId: owner, Payload: &pb.ControlFrame_SessionClosed{SessionClosed: &pb.SessionClosed{Reason: tt.reason}}}
						raw, err = tunnelwire.MarshalControl(frame)
						if err != nil {
							t.Error(err)
							return
						}
						_ = c.Write(r.Context(), websocket.MessageBinary, raw)
						_, _, _ = c.Read(r.Context())
					}))
					defer peer.Close()
					ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
					defer cancel()
					var output bytes.Buffer
					err := Run(ctx, "ws"+strings.TrimPrefix(peer.URL, "http"), "aaaaaaataaaaaaaaaaaaaaaaaaaaaaaa", &output)
					if err == nil || err.Error() != tt.message || connections.Load() != 1 || strings.Contains(output.String(), "Reconnecting") {
						t.Fatal("terminal close lost reason or retried", err, connections.Load(), output.String())
					}
				})
			}
		})
	}
}
