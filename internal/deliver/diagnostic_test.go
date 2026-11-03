package deliver

import (
	"context"
	"errors"
	"github.com/adal-cloud/adal-cli/internal/console"
	"github.com/adal-cloud/adal-cli/internal/journal"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type diagnosticTransport func(*http.Request) (*http.Response, error)

func (f diagnosticTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type brokenBody struct{}

func (brokenBody) Read([]byte) (int, error) { return 0, errors.New("secret response") }
func (brokenBody) Close() error             { return nil }

func TestSafeTransportDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		body io.ReadCloser
		want string
	}{
		{name: "dns", err: &net.DNSError{Err: "secret resolver", Name: "secret.example"}, want: "dns"},
		{name: "connect", err: &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("secret address")}, want: "connect"},
		{name: "read", body: brokenBody{}, want: "read"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := NewClient()
			client.Transport = diagnosticTransport(func(*http.Request) (*http.Response, error) {
				if tc.err != nil {
					return nil, tc.err
				}
				return &http.Response{StatusCode: 200, Body: tc.body, Header: http.Header{}}, nil
			})
			got := Call(context.Background(), client, sampleAssignment(), journal.Endpoint{ID: endpointA, URL: "https://secret.example"})
			if got.Diagnostic != tc.want || got.Error != errRequest || got.Status != 0 {
				t.Fatalf("result=%+v", got)
			}
			address, ok := Address("https://secret.example", sampleAssignment().Path, sampleAssignment().Query)
			if !ok {
				t.Fatal("address")
			}
			line := console.Format(console.Event{
				Title:       "Call finished",
				Request:     sampleAssignment().RequestID,
				Endpoint:    address,
				Outcome:     got.Outcome,
				Error:       got.Error,
				Diagnostic:  got.Diagnostic,
				Status:      got.Status,
				HasDuration: true,
			})
			if !strings.Contains(line, logField("diagnostic", tc.want)) ||
				strings.Contains(line, "secret resolver") ||
				strings.Contains(line, "secret address") ||
				strings.Contains(line, "secret response") {
				t.Fatalf("unsafe or missing diagnostic: %s", line)
			}
		})
	}
}

func TestSafeTLSHandshakeDiagnostic(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()
	got := Call(context.Background(), NewClient(), sampleAssignment(), journal.Endpoint{ID: endpointA, URL: srv.URL})
	if got.Diagnostic != "tls" || got.Error != errRequest || got.Status != 0 {
		t.Fatalf("result=%+v", got)
	}
}
