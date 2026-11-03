package deliver

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/adal-cloud/adal-cli/internal/journal"
)

const (
	// Timeout is the limit for one endpoint call.
	Timeout = 30 * time.Second

	errTimeout = "timeout"
	errRequest = "request failed"
	errStatus  = "unexpected status"
	errBadURL  = "invalid url"
	errBadBody = "invalid body"
)

var errInvalidURL = errors.New(errBadURL)

// NewClient returns the HTTP client used for endpoint calls.
// Redirects are not followed.
func NewClient() *http.Client {
	return &http.Client{
		Timeout:       Timeout,
		CheckRedirect: refuseRedirect,
	}
}

func refuseRedirect(*http.Request, []*http.Request) error {
	return http.ErrUseLastResponse
}

// Call performs one HTTP call and returns its outcome.
// The error text does not include the URL.
// The error text does not include the body.
// The error text does not include the headers.
func Call(ctx context.Context, client *http.Client, assignment journal.Assignment, endpoint journal.Endpoint) (result journal.Result) {
	started := time.Now()
	result = journal.Result{EndpointID: endpoint.ID, Outcome: journal.OutcomeFailure}
	defer func() {
		result.Duration = time.Since(started)
	}()

	target, err := Join(endpoint.URL, assignment.Path, assignment.Query)
	if err != nil {
		result.Error = errBadURL
		return result
	}
	body, err := decodeBody(assignment.Body)
	if err != nil {
		result.Error = errBadBody
		return result
	}
	req, err := http.NewRequestWithContext(ctx, assignment.Method, target, reader(body))
	if err != nil {
		result.Error = errRequest
		return result
	}
	for name, values := range assignment.Headers {
		for _, value := range values {
			req.Header.Add(name, value)
		}
	}
	req.Header.Del("Content-Length")
	req.Header.Set("Content-Length", strconv.Itoa(len(body)))
	req.ContentLength = int64(len(body))
	req.Host = req.URL.Host
	if endpoint.IdempotencyKey {
		req.Header.Set("Idempotency-Key", assignment.RequestID)
	}

	var tlsFailed atomic.Bool
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{
		TLSHandshakeDone: func(_ tls.ConnectionState, err error) { tlsFailed.Store(err != nil) },
	}))
	resp, err := client.Do(req)
	if err != nil {
		result.Diagnostic = transportCategory(err, tlsFailed.Load())
		if timedOut(err) {
			result.Error = errTimeout
			return result
		}
		result.Error = errRequest
		return result
	}
	defer resp.Body.Close()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		result.Diagnostic = "read"
		result.Outcome = journal.OutcomeFailure
		result.Status = 0
		if timedOut(err) {
			result.Error = errTimeout
			return result
		}
		result.Error = errRequest
		return result
	}
	result.Status = resp.StatusCode
	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		result.Outcome = journal.OutcomeSuccess
		return result
	}
	result.Error = errStatus
	return result
}

func decodeBody(encoded string) ([]byte, error) {
	if encoded == "" {
		return nil, nil
	}
	return base64.StdEncoding.DecodeString(encoded)
}

func reader(body []byte) io.Reader {
	if len(body) == 0 {
		return http.NoBody
	}
	return bytes.NewReader(body)
}

func timedOut(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func transportCategory(err error, tlsFailed bool) string {
	var dns *net.DNSError
	if errors.As(err, &dns) {
		return "dns"
	}
	if tlsFailed {
		return "tls"
	}
	var operation *net.OpError
	if errors.As(err, &operation) && operation.Op == "dial" {
		return "connect"
	}
	if timedOut(err) {
		return "timeout"
	}
	return "request"
}
