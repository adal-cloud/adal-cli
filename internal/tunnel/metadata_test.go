package tunnel

import (
	"errors"
	pb "github.com/adal-cloud/adal-cli/internal/tunnelpb"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func TestResponseMetadataBeforeProxyTransforms(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Connection", "X-Hop")
		w.Header().Set("X-Hop", "raw endpoint value")
		w.Header().Add("Via", "1.0 prior")
		_, _ = io.WriteString(w, "ok")
	}))
	defer server.Close()
	cfg := snapshot(server.URL)
	frames, err := perform(t, cfg, opening(cfg, "GET", "/", ""), true)
	if err != nil {
		t.Fatal(err)
	}
	m, _ := result(frames)
	values := map[string][]string{}
	for _, h := range m.Headers {
		for _, v := range h.RawValues {
			values[h.Name] = append(values[h.Name], string(v))
		}
	}
	if len(values["X-Hop"]) != 1 || values["X-Hop"][0] != "raw endpoint value" || len(values["Via"]) != 1 || values["Via"][0] != "1.0 prior" {
		t.Fatal("endpoint metadata changed before history", values)
	}
}

func TestConnectionTokensSurviveHTTPParser(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, b, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer c.Close()
		_, _ = b.WriteString("HTTP/1.1 200 OK\r\nConnection: X-Hop, close\r\nContent-Length: 2\r\nX-Hop: private\r\n\r\nok")
		_ = b.Flush()
	}))
	defer server.Close()
	cfg := snapshot(server.URL)
	frames, err := perform(t, cfg, opening(cfg, "GET", "/", ""), true)
	if err != nil {
		t.Fatal(err)
	}
	metadata, _ := result(frames)
	found := false
	for _, h := range metadata.Headers {
		if h.Name == "Connection" && len(h.RawValues) == 1 && string(h.RawValues[0]) == "X-Hop, close" {
			found = true
		}
	}
	if !found {
		t.Fatal("HTTP parser discarded connection tokens")
	}
}

func TestInformationalSemanticBudget(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Info", strings.Repeat("x", (64<<10)-len("X-Info")))
		w.WriteHeader(103)
		w.Header().Del("X-Info")
		w.WriteHeader(200)
	}))
	defer server.Close()
	cfg := snapshot(server.URL)
	frames, err := perform(t, cfg, opening(cfg, "GET", "/", ""), true)
	if err != nil {
		t.Fatal("semantic header budget included protobuf overhead", err)
	}
	found := false
	for _, f := range frames {
		if f.GetInformational() != nil && f.GetInformational().StatusCode == 103 {
			found = true
		}
	}
	if !found {
		t.Fatal("informational response lost")
	}
}
func TestResponseHeadersTooLargeTyped(t *testing.T) {
	for _, size := range []int{70 << 10, 140 << 10} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Large", strings.Repeat("x", size))
			w.WriteHeader(200)
		}))
		defer server.Close()
		cfg := snapshot(server.URL)
		_, err := perform(t, cfg, opening(cfg, "GET", "/", ""), true)
		var typed *Error
		if !errors.As(err, &typed) || typed.Code != pb.ErrorCode_ERROR_CODE_RESPONSE_HEADERS_TOO_LARGE {
			t.Fatal("wrong header limit error", size, err)
		}
	}
}

func TestInformationalOverflowIsInvalidResponse(t *testing.T) {
	for _, test := range []struct {
		name        string
		count, size int
	}{{"single", 1, 70 << 10}, {"wire_limit", 1, 140 << 10}, {"aggregate", 2, 32 << 10}, {"count", 17, 0}} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if test.size > 0 {
					w.Header().Set("X-Info", strings.Repeat("x", test.size))
				}
				for range test.count {
					w.WriteHeader(103)
				}
				w.Header().Del("X-Info")
				w.WriteHeader(200)
			}))
			defer server.Close()
			cfg := snapshot(server.URL)
			_, err := perform(t, cfg, opening(cfg, "GET", "/", ""), true)
			var typed *Error
			if !errors.As(err, &typed) || typed.Code != pb.ErrorCode_ERROR_CODE_INVALID_ENDPOINT_RESPONSE {
				t.Fatal(err)
			}
		})
	}
}
func TestHEADAndNoBodyRepresentation(t *testing.T) {
	const representation = int64(256 << 20)
	for _, test := range []struct {
		method string
		status int
	}{{"HEAD", 200}, {"GET", 204}, {"GET", 304}} {
		t.Run(test.method+strconv.Itoa(test.status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				c, b, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				defer c.Close()
				_, _ = b.WriteString("HTTP/1.1 " + strconv.Itoa(test.status) + " " + http.StatusText(test.status) + "\r\nContent-Length: " + strconv.FormatInt(representation, 10) + "\r\n\r\n")
				_ = b.Flush()
			}))
			defer server.Close()
			cfg := snapshot(server.URL)
			f := opening(cfg, test.method, "/", "")
			zero := int64(0)
			f.GetOpen().Request.ContentLength = &zero
			frames, err := perform(t, cfg, f, true)
			m, body := result(frames)
			if err != nil || m == nil || len(body) != 0 {
				t.Fatal(m, len(body), err)
			}
			if test.status == 204 {
				if m.ContentLength != nil {
					t.Fatal("204 has representation length")
				}
			} else if m.ContentLength == nil || *m.ContentLength != representation {
				t.Fatal("representation length lost", m.ContentLength)
			}
		})
	}
}
