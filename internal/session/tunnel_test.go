package session

import (
	"context"
	"encoding/json"
	"github.com/coder/websocket"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestTunnelSkipsInboundJournal(t *testing.T) {
	// An unusable configuration directory makes an accidental journal open fail.
	unusable := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(unusable, []byte("file"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("APPDATA", unusable)
	t.Setenv("XDG_CONFIG_HOME", unusable)
	t.Setenv("HOME", unusable)
	hello := make(chan bool, 1)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		_, raw, err := c.Read(ctx)
		if err != nil {
			return
		}
		var h struct{ Capabilities []string }
		_ = json.Unmarshal(raw, &h)
		hello <- len(h.Capabilities) == 1 && h.Capabilities[0] == "http_tunnel_v1"
		_ = c.Close(websocket.StatusCode(4001), "Token is invalid")
	}))
	defer peer.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	err := Run(ctx, "ws"+strings.TrimPrefix(peer.URL, "http"), "aaaaaaataaaaaaaaaaaaaaaaaaaaaaaa", io.Discard)
	if err == nil || err.Error() != "Token is invalid" {
		t.Fatal("Tunnel opened the Inbound journal", err)
	}
	select {
	case ok := <-hello:
		if !ok {
			t.Fatal("missing capability")
		}
	default:
		t.Fatal("no Tunnel connection")
	}
}
