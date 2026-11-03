package deliver

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/adal-cloud/adal-cli/internal/journal"
)

const (
	endpointA = "11111111-1111-4111-8111-111111111111"
	endpointB = "22222222-2222-4222-8222-222222222222"
)

func TestClientTimeoutAndNoRedirect(t *testing.T) {
	client := NewClient()
	if client.Timeout != 30*time.Second {
		t.Fatalf("timeout = %s", client.Timeout)
	}
	req, err := http.NewRequest(http.MethodGet, "https://example.test", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.CheckRedirect(req, nil); err != http.ErrUseLastResponse {
		t.Fatalf("redirect = %v", err)
	}
}

func TestCallForwardsHeadersAndReplacesIdempotencyKey(t *testing.T) {
	var got http.Request
	var body string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = *r.Clone(r.Context())
		payload, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("body: %v", err)
		}
		body = string(payload)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	assignment := sampleAssignment()
	assignment.Headers = map[string][]string{
		"Content-Type":    {"text/plain"},
		"X-Trace":         {"one", "two"},
		"Idempotency-Key": {"incoming"},
		"Host":            {"evil.test"},
		"Content-Length":  {"999"},
	}
	endpoint := journal.Endpoint{ID: endpointA, URL: srv.URL + "/hook", IdempotencyKey: true, Enabled: true}

	result := Call(context.Background(), NewClient(), assignment, endpoint)
	if result.Outcome != journal.OutcomeSuccess || result.Status != http.StatusNoContent || result.Error != "" {
		t.Fatalf("result = %+v", result)
	}
	if result.Duration < 0 {
		t.Fatalf("duration = %s", result.Duration)
	}
	if got.URL.Path != "/hook/orders" || got.URL.RawQuery != "a=1" {
		t.Fatalf("url = %s", got.URL)
	}
	if got.Host != strings.TrimPrefix(srv.URL, "http://") || strings.Contains(got.Host, "evil.test") {
		t.Fatalf("host = %q", got.Host)
	}
	if got.Header.Get("Content-Length") != "1" || body != "a" {
		t.Fatalf("body %q length %s", body, got.Header.Get("Content-Length"))
	}
	if values := got.Header.Values("X-Trace"); len(values) != 2 || values[0] != "one" || values[1] != "two" {
		t.Fatalf("X-Trace = %v", values)
	}
	if got.Header.Get("Idempotency-Key") != assignment.RequestID {
		t.Fatalf("Idempotency-Key = %q", got.Header.Get("Idempotency-Key"))
	}
}

func TestCallKeepsIdempotencyKeyWhenDisabled(t *testing.T) {
	var key string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key = r.Header.Get("Idempotency-Key")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	assignment := sampleAssignment()
	assignment.Headers = map[string][]string{"Idempotency-Key": {"incoming"}}
	endpoint := journal.Endpoint{ID: endpointA, URL: srv.URL + "/hook", Enabled: true}
	result := Call(context.Background(), NewClient(), assignment, endpoint)
	if result.Outcome != journal.OutcomeSuccess || result.Status != http.StatusOK {
		t.Fatalf("result = %+v", result)
	}
	if key != "incoming" {
		t.Fatalf("Idempotency-Key = %q", key)
	}
}

func TestCallEmptyBodyHasZeroLength(t *testing.T) {
	var length string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		length = r.Header.Get("Content-Length")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	assignment := sampleAssignment()
	assignment.Body = ""
	endpoint := journal.Endpoint{ID: endpointA, URL: srv.URL, Enabled: true}
	result := Call(context.Background(), NewClient(), assignment, endpoint)
	if result.Outcome != journal.OutcomeSuccess {
		t.Fatalf("result = %+v", result)
	}
	if length != "0" {
		t.Fatalf("Content-Length = %q", length)
	}
}

func TestCallUnexpectedStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	result := Call(context.Background(), NewClient(), sampleAssignment(), journal.Endpoint{ID: endpointA, URL: srv.URL, Enabled: true})
	if result.Outcome != journal.OutcomeFailure || result.Status != http.StatusBadGateway || result.Error != errStatus {
		t.Fatalf("result = %+v", result)
	}
}

func TestCallDoesNotFollowRedirect(t *testing.T) {
	next := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("redirect was followed")
		w.WriteHeader(http.StatusOK)
	}))
	defer next.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, next.URL, http.StatusFound)
	}))
	defer srv.Close()

	result := Call(context.Background(), NewClient(), sampleAssignment(), journal.Endpoint{ID: endpointA, URL: srv.URL, Enabled: true})
	if result.Outcome != journal.OutcomeFailure || result.Status != http.StatusFound || result.Error != errStatus {
		t.Fatalf("result = %+v", result)
	}
}

func TestSlowBodyUsesTheCallTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		time.Sleep(300 * time.Millisecond)
		_, _ = w.Write([]byte("late"))
	}))
	defer srv.Close()

	client := NewClient()
	client.Timeout = 50 * time.Millisecond
	result := Call(context.Background(), client, sampleAssignment(), journal.Endpoint{ID: endpointA, URL: srv.URL, Enabled: true})
	if result.Outcome != journal.OutcomeFailure || result.Status != 0 || result.Error != errTimeout {
		t.Fatalf("result = %+v", result)
	}
	if strings.Contains(result.Error, srv.URL) {
		t.Fatalf("url leaked into %q", result.Error)
	}
}

func TestReloadedBodyIsDelivered(t *testing.T) {
	dir := t.TempDir()
	book, err := journal.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	assignment := sampleAssignment()
	assignment.Body = "a2VwdC1ib2R5"
	if _, err := book.Put(assignment); err != nil {
		t.Fatal(err)
	}
	if err := book.Close(); err != nil {
		t.Fatal(err)
	}
	book, err = journal.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer book.Close()
	stored := book.Stored()
	if len(stored) != 1 {
		t.Fatal("assignment was not reloaded")
	}

	var body string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		body = string(payload)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	result := Call(context.Background(), NewClient(), stored[0], journal.Endpoint{ID: endpointA, URL: srv.URL, Enabled: true})
	if result.Outcome != journal.OutcomeSuccess || result.Status != http.StatusNoContent || body != "kept-body" {
		t.Fatalf("result = %+v body = %q", result, body)
	}
}

func TestCallTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := NewClient()
	client.Timeout = 50 * time.Millisecond
	result := Call(context.Background(), client, sampleAssignment(), journal.Endpoint{ID: endpointA, URL: srv.URL, Enabled: true})
	if result.Outcome != journal.OutcomeFailure || result.Status != 0 || result.Error != errTimeout {
		t.Fatalf("result = %+v", result)
	}
	if result.Duration < 20*time.Millisecond {
		t.Fatalf("duration = %s", result.Duration)
	}
	if strings.Contains(result.Error, srv.URL) {
		t.Fatalf("url leaked into %q", result.Error)
	}
}

func TestCallNetworkErrorOmitsURL(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	result := Call(context.Background(), NewClient(), sampleAssignment(), journal.Endpoint{
		ID: endpointA, URL: "http://" + addr + "/hook", Enabled: true,
	})
	if result.Outcome != journal.OutcomeFailure || result.Status != 0 || result.Error != errRequest {
		t.Fatalf("result = %+v", result)
	}
	if strings.Contains(result.Error, addr) {
		t.Fatalf("url leaked into %q", result.Error)
	}
}

func TestCallInvalidBodyDoesNotDial(t *testing.T) {
	hit := false
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		hit = true
	}))
	defer srv.Close()

	assignment := sampleAssignment()
	assignment.Body = "???"
	result := Call(context.Background(), NewClient(), assignment, journal.Endpoint{ID: endpointA, URL: srv.URL, Enabled: true})
	if result.Error != errBadBody || result.Status != 0 || hit {
		t.Fatalf("result = %+v hit=%v", result, hit)
	}
}

func TestCallInvalidURLDoesNotDial(t *testing.T) {
	hit := false
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		hit = true
	}))
	defer srv.Close()

	result := Call(context.Background(), NewClient(), sampleAssignment(), journal.Endpoint{ID: endpointA, URL: "ftp://example.test", Enabled: true})
	if result.Error != errBadURL || result.Status != 0 || hit {
		t.Fatalf("result = %+v hit=%v", result, hit)
	}
}

func sampleAssignment() journal.Assignment {
	return journal.Assignment{
		RequestID: "018f3c2e-7b1a-7c3d-8e4f-123456789abc",
		Method:    http.MethodPost,
		Path:      "/orders",
		Query:     "a=1",
		Headers:   map[string][]string{},
		Body:      "YQ==",
	}
}
