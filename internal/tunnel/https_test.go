package tunnel

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	pb "github.com/adal-cloud/adal-cli/internal/tunnelpb"
	"golang.org/x/net/http2"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func trustedExecutor(server *httptest.Server) *Executor {
	pool := x509.NewCertPool()
	pool.AddCert(server.Certificate())
	e := NewExecutor()
	e.transport = func() *http.Transport { tr := freshTransport(); tr.TLSClientConfig.RootCAs = pool; return tr }
	return e
}
func TestHTTPSNegotiatesHTTP2(t *testing.T) {
	var protocol atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		protocol.Store(int32(r.ProtoMajor))
		if r.Header.Get("Accept-Encoding") != "" {
			t.Error("automatic compression enabled")
		}
		w.WriteHeader(103)
		_, _ = io.WriteString(w, "secure")
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()
	cfg := snapshot(server.URL)
	f := opening(cfg, "GET", "/", "")
	zero := int64(0)
	f.GetOpen().Request.ContentLength = &zero
	frames, err := performExecutor(t, trustedExecutor(server), cfg, f, true)
	m, body := result(frames)
	if err != nil || protocol.Load() != 2 || m == nil || m.Protocol != pb.HTTPVersion_HTTP_VERSION_2 || string(body) != "secure" {
		t.Fatal("HTTP/2 not negotiated", protocol.Load(), m, string(body), err)
	}
	for _, f := range frames {
		if f.GetInformational() != nil && f.GetInformational().Protocol != pb.HTTPVersion_HTTP_VERSION_2 {
			t.Fatal("wrong informational protocol")
		}
	}
}
func TestHTTPSTrustAndSNI(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.TLS.ServerName != "example.com" || !strings.HasPrefix(r.Host, "example.com:") {
			t.Error("Host/SNI changed")
		}
		_, _ = io.WriteString(w, "trusted")
	}))
	defer server.Close()
	cfg := snapshot(server.URL)
	if _, err := perform(t, cfg, opening(cfg, "GET", "/", ""), true); err == nil || calls.Load() != 0 {
		t.Fatal("untrusted endpoint executed")
	}
	e := trustedExecutor(server)
	original := e.transport
	e.transport = func() *http.Transport {
		tr := original()
		tr.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, strings.TrimPrefix(server.URL, "https://"))
		}
		return tr
	}
	cfg = snapshot(strings.Replace(server.URL, "127.0.0.1", "example.com", 1))
	if _, err := performExecutor(t, e, cfg, opening(cfg, "GET", "/", ""), true); err != nil || calls.Load() != 1 {
		t.Fatal("verified endpoint failed", calls.Load(), err)
	}
}
func TestNoHiddenHTTP2Retry(t *testing.T) {
	fixture := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	fixture.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfgTLS := fixture.TLS.Clone()
	cfgTLS.NextProtos = []string{"h2"}
	secure := tls.NewListener(listener, cfgTLS)
	defer secure.Close()
	var requests, connections atomic.Int32
	var workers sync.WaitGroup
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			c, err := secure.Accept()
			if err != nil {
				return
			}
			connections.Add(1)
			workers.Go(func() {
				defer c.Close()
				_ = c.SetDeadline(time.Now().Add(6 * time.Second))
				preface := make([]byte, len(http2.ClientPreface))
				if _, err := io.ReadFull(c, preface); err != nil || string(preface) != http2.ClientPreface {
					return
				}
				framer := http2.NewFramer(c, c)
				if err := framer.WriteSettings(); err != nil {
					return
				}
				for {
					f, err := framer.ReadFrame()
					if err != nil {
						return
					}
					switch f := f.(type) {
					case *http2.SettingsFrame:
						if !f.IsAck() {
							_ = framer.WriteSettingsAck()
						}
					case *http2.HeadersFrame:
						requests.Add(1)
						_ = framer.WriteRSTStream(f.StreamID, http2.ErrCodeRefusedStream)
					}
				}
			})
		}
	}()
	s := snapshot("https://" + listener.Addr().String())
	open := opening(s, "GET", "/", "")
	zero := int64(0)
	open.GetOpen().Request.ContentLength = &zero
	if _, err = performExecutor(t, trustedExecutor(fixture), s, open, true); err == nil {
		t.Fatal("refused stream succeeded")
	}
	secure.Close()
	<-stopped
	workers.Wait()
	if requests.Load() != 1 || connections.Load() != 1 {
		t.Fatal("refused stream was repeated", requests.Load(), connections.Load())
	}
}

func TestHTTPSPreservesSpecialHeaderBytes(t *testing.T) {
	for _, test := range []struct {
		name   string
		values []string
	}{{"Cookie", []string{"a=1;b=2"}}, {"User-Agent", []string{"first", "second"}}, {"User-Agent", []string{"", "second"}}} {
		t.Run(test.name+strings.Join(test.values, "/"), func(t *testing.T) {
			got := make(chan []string, 1)
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.ProtoMajor != 1 {
					t.Error("special fields require HTTP/1.1")
				}
				got <- append([]string(nil), r.Header.Values(test.name)...)
				_, _ = io.WriteString(w, "ok")
			}))
			server.EnableHTTP2 = true
			server.StartTLS()
			defer server.Close()
			cfg := snapshot(server.URL)
			open := opening(cfg, "GET", "/", "")
			zero := int64(0)
			open.GetOpen().Request.ContentLength = &zero
			header := &pb.Header{Name: test.name}
			for _, v := range test.values {
				header.RawValues = append(header.RawValues, []byte(v))
			}
			open.GetOpen().Request.Headers = []*pb.Header{header}
			if _, err := performExecutor(t, trustedExecutor(server), cfg, open, true); err != nil {
				t.Fatal(err)
			}
			values := <-got
			if len(values) != len(test.values) {
				t.Fatal("special field occurrences changed", values, test.values)
			}
			for i := range values {
				if values[i] != test.values[i] {
					t.Fatal("special field bytes changed", values, test.values)
				}
			}
		})
	}
}
