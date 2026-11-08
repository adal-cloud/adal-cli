package deliver

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/adal-cloud/adal-cli/internal/journal"
)

func TestCallLosslessHeadersAndTrailers(t *testing.T) {
	for _, method := range []string{"POST", "GET", "HEAD"} {
		for _, body := range []string{"payload", ""} {
			t.Run(method+"/"+body, func(t *testing.T) {
				values := []string{string([]byte{255, 128}), "Привет", "/4A=", ""}
				var header, trailer []string
				var received string
				var length int64
				var transfer []string
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					data, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
					}
					received = string(data)
					header = r.Header.Values("X-Bytes")
					trailer = r.Trailer.Values("X-End")
					length = r.ContentLength
					transfer = r.TransferEncoding
					w.WriteHeader(http.StatusNoContent)
				}))
				defer srv.Close()
				a := sampleAssignment()
				a.Method = method
				a.Body = base64.StdEncoding.EncodeToString([]byte(body))
				a.Headers = map[string][]string{"X-Bytes": values}
				a.Trailers = map[string][]string{"X-End": values}
				result := Call(context.Background(), NewClient(), a, journal.Endpoint{ID: endpointA, URL: srv.URL, Enabled: true})
				if result.Outcome != journal.OutcomeSuccess {
					t.Fatalf("result=%+v", result)
				}
				if received != body || !reflect.DeepEqual(header, values) || !reflect.DeepEqual(trailer, values) {
					t.Fatalf("body=%q headers=%q trailers=%q", received, header, trailer)
				}
				if length != -1 || !reflect.DeepEqual(transfer, []string{"chunked"}) {
					t.Fatalf("length=%d transfer=%v", length, transfer)
				}
			})
		}
	}

}

func TestCallIdempotencyHeaderCannotBeOverriddenByTrailer(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			var header, trailer string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				header = r.Header.Get("Idempotency-Key")
				trailer = r.Trailer.Get("Idempotency-Key")
				w.WriteHeader(204)
			}))
			defer srv.Close()
			a := sampleAssignment()
			a.Trailers = map[string][]string{"idempotency-key": {"incoming"}}
			result := Call(context.Background(), NewClient(), a, journal.Endpoint{ID: endpointA, URL: srv.URL, Enabled: true, IdempotencyKey: enabled})
			if result.Outcome != journal.OutcomeSuccess {
				t.Fatalf("result=%+v", result)
			}
			if enabled && (header != a.RequestID || trailer != "") {
				t.Fatalf("header=%q trailer=%q", header, trailer)
			}
			if !enabled && trailer != "incoming" {
				t.Fatalf("trailer=%q", trailer)
			}
		})
	}
}

func TestCallLosslessHTTP2Trailers(t *testing.T) {
	for _, tc := range []struct{ method, body string }{{"POST", "payload"}, {"GET", ""}, {"HEAD", ""}} {
		t.Run(tc.method, func(t *testing.T) {
			values := []string{string([]byte{255, 128}), "Привет", "/4A=", ""}
			var header, trailer []string
			var received string
			var protocol int
			srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				data, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
				}
				protocol = r.ProtoMajor
				received = string(data)
				header = r.Header.Values("X-Bytes")
				trailer = r.Trailer.Values("X-End")
				w.WriteHeader(http.StatusNoContent)
			}))
			srv.EnableHTTP2 = true
			srv.StartTLS()
			defer srv.Close()
			a := sampleAssignment()
			a.Method = tc.method
			a.Body = base64.StdEncoding.EncodeToString([]byte(tc.body))
			a.Headers = map[string][]string{"X-Bytes": values}
			a.Trailers = map[string][]string{"X-End": values}
			result := Call(context.Background(), srv.Client(), a, journal.Endpoint{ID: endpointA, URL: srv.URL, Enabled: true})
			if result.Outcome != journal.OutcomeSuccess {
				t.Fatalf("result=%+v", result)
			}
			if protocol != 2 || received != tc.body || !reflect.DeepEqual(header, values) || !reflect.DeepEqual(trailer, values) {
				t.Fatalf("protocol=%d body=%q headers=%q trailers=%q", protocol, received, header, trailer)
			}
		})
	}
}
