package tunnel

import (
	"errors"
	pb "github.com/adal-cloud/adal-cli/internal/tunnelpb"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestOptionsAsteriskPreservesServerTarget(t *testing.T) {
	got := make(chan string, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { got <- r.RequestURI; w.WriteHeader(204) }))
	server.Config.DisableGeneralOptionsHandler = true
	server.Start()
	defer server.Close()
	cfg := snapshot(server.URL + "/base")
	f := opening(cfg, "OPTIONS", "*", "")
	zero := int64(0)
	f.GetOpen().Request.ContentLength = &zero
	_, err := perform(t, cfg, f, true)
	if err != nil {
		t.Fatal(err)
	}
	if target := <-got; target != "*" {
		t.Fatal("asterisk became a base path", target)
	}
}
func TestCLIMethodGuards(t *testing.T) {
	for _, test := range []struct {
		method, name, value string
		code                pb.ErrorCode
	}{
		{"CONNECT", "", "", pb.ErrorCode_ERROR_CODE_CONNECT_NOT_SUPPORTED},
		{"GET", "Upgrade", "websocket", pb.ErrorCode_ERROR_CODE_UPGRADE_NOT_SUPPORTED},
		{"POST", "Expect", "magic", pb.ErrorCode_ERROR_CODE_EXPECTATION_FAILED},
	} {
		cfg := snapshot("http://localhost")
		f := opening(cfg, test.method, "/", "")
		if test.name != "" {
			f.GetOpen().Request.Headers = []*pb.Header{{Name: test.name, RawValues: [][]byte{[]byte(test.value)}}}
		}
		x, err := NewExecutor().Start(t.Context(), cfg, f)
		if x != nil {
			x.Cancel()
		}
		var typed *Error
		if !errors.As(err, &typed) || typed.Code != test.code {
			t.Fatal(test.method, test.name, err)
		}
	}
}
func TestMaxForwardsDecrement(t *testing.T) {
	got := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { got <- r.Header.Get("Max-Forwards"); w.WriteHeader(200) }))
	defer server.Close()
	cfg := snapshot(server.URL)
	f := opening(cfg, "OPTIONS", "/", "")
	f.GetOpen().Request.Headers = []*pb.Header{{Name: "Max-Forwards", RawValues: [][]byte{[]byte("0010")}}}
	if _, err := perform(t, cfg, f, true); err != nil {
		t.Fatal(err)
	}
	if <-got != "0009" {
		t.Fatal("Max-Forwards not decremented")
	}
}
func TestUnrequested101IsInvalidResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, b, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer c.Close()
		_, _ = b.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
		_ = b.Flush()
	}))
	defer server.Close()
	cfg := snapshot(server.URL)
	_, err := perform(t, cfg, opening(cfg, "GET", "/", ""), true)
	var typed *Error
	if !errors.As(err, &typed) || typed.Code != pb.ErrorCode_ERROR_CODE_INVALID_ENDPOINT_RESPONSE {
		t.Fatal("wrong unsolicited upgrade error", err)
	}
}
func TestRepeatedContinueNormalizesControlField(t *testing.T) {
	f := opening(snapshot("http://localhost"), "POST", "/", "")
	f.GetOpen().Request.Headers = []*pb.Header{{Name: "Expect", RawValues: [][]byte{[]byte("100-continue"), []byte("100-continue")}}}
	headers, err := requestHeaders(f.GetOpen().Request)
	if err != nil || len(headers.Values("Expect")) != 1 || headers.Get("Expect") != "100-continue" {
		t.Fatal(headers, err)
	}
}
