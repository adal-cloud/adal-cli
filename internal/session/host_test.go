package session

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestSessionWithWebSocketOrigin(t *testing.T) {
	for _, secure := range []bool{false, true} {
		name := "ws"
		if secure {
			name = "wss"
		}
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/session" {
					t.Errorf("path = %q", r.URL.Path)
					http.NotFound(w, r)
					return
				}
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					t.Error(err)
					return
				}
				defer conn.CloseNow()
				readHello(t, conn)
				_ = conn.Close(4001, "")
			}))
			if secure {
				srv.StartTLS()
				previous := http.DefaultClient
				http.DefaultClient = srv.Client()
				t.Cleanup(func() { http.DefaultClient = previous })
			} else {
				srv.Start()
			}
			defer srv.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := run(ctx, "ws"+srv.URL[4:]+"/", "secret", frameLimit, time.Second, nil, t.TempDir()); err == nil || err.Error() != "authentication failed" {
				t.Fatalf("session error = %v, want authentication failed after connecting", err)
			}
		})
	}
}
