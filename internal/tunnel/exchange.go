package tunnel

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	pb "github.com/adal-cloud/adal-cli/internal/tunnelpb"
	"github.com/adal-cloud/adal-cli/internal/tunnelwire"
	"google.golang.org/protobuf/proto"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/textproto"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type Executor struct {
	transport func() *http.Transport
	mu        sync.Mutex
	active    map[string]*Exchange
	closed    map[string]closedExchange
}
type closedExchange struct {
	until                time.Time
	sessionID, controlID string
}

func NewExecutor() *Executor {
	return &Executor{active: make(map[string]*Exchange), closed: make(map[string]closedExchange), transport: freshTransport}
}

type Exchange struct {
	transport func() *http.Transport
	ctx       context.Context
	cancel    context.CancelFunc
	executor  *Executor
	snapshot  *pb.ConfigSnapshot
	open      *pb.ExecuteFrame
	body      *body
	flow      *tunnelwire.Stream
	once      sync.Once
	run       atomic.Bool
	emitMu    sync.Mutex
	emit      func(*pb.ExecuteFrame) error
}

func (e *Executor) Start(ctx context.Context, s *pb.ConfigSnapshot, f *pb.ExecuteFrame) (*Exchange, error) {
	if err := validateSnapshot(s); err != nil {
		return nil, err
	}
	if tunnelwire.ValidateExecute(f) != nil || f.GetOpen() == nil || len(f.ProtoReflect().GetUnknown()) != 0 {
		return nil, fail(pb.ErrorCode_ERROR_CODE_INVALID_REQUEST)
	}
	o := f.GetOpen()
	if o.Request.Method == "CONNECT" {
		return nil, fail(pb.ErrorCode_ERROR_CODE_CONNECT_NOT_SUPPORTED)
	}
	if o.OwnerId != s.OwnerId || o.TunnelId != s.TunnelId || o.RegionId != s.RegionId || o.Revision != s.Revision || !bytes.Equal(o.ConfigHash, s.ConfigHash) {
		return nil, fail(pb.ErrorCode_ERROR_CODE_TUNNEL_NOT_READY)
	}
	if _, err := target(s.EndpointUrl, o.Request); err != nil {
		return nil, err
	}
	if _, err := requestHeaders(o.Request); err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for id, closed := range e.closed {
		if time.Now().After(closed.until) {
			delete(e.closed, id)
		}
	}
	if e.active[f.RequestId] != nil || !e.closed[f.RequestId].until.IsZero() {
		return nil, fail(pb.ErrorCode_ERROR_CODE_INVALID_REQUEST)
	}
	maxRequests := s.MaxConcurrentRequests
	if maxRequests == 0 {
		maxRequests = 16
	}
	if len(e.active) >= int(maxRequests) || len(e.active) >= 64 || len(e.closed) >= 1024 {
		return nil, fail(pb.ErrorCode_ERROR_CODE_CONCURRENCY_LIMIT_EXCEEDED)
	}
	ctx, cancel := context.WithTimeout(ctx, min(o.RemainingDuration.AsDuration(), time.Duration(s.TimeoutSeconds)*time.Second))
	flow, err := tunnelwire.NewStream(f)
	if err != nil {
		cancel()
		return nil, err
	}
	x := &Exchange{ctx: ctx, cancel: cancel, executor: e, snapshot: proto.Clone(s).(*pb.ConfigSnapshot), open: proto.Clone(f).(*pb.ExecuteFrame), flow: flow}
	x.transport = e.transport
	x.body = newBody(ctx, func(n int) error { return x.sendWindow(n) })
	e.active[f.RequestId] = x
	context.AfterFunc(ctx, func() {
		if !x.run.Load() {
			x.Cancel()
		}
	})
	return x, nil
}
func (x *Exchange) Cancel() {
	x.once.Do(func() {
		x.cancel()
		_ = x.body.Close()
		x.executor.mu.Lock()
		delete(x.executor.active, x.open.RequestId)
		x.executor.closed[x.open.RequestId] = closedExchange{time.Now().Add(125 * time.Second), x.open.SessionId, x.open.ControlId}
		x.executor.mu.Unlock()
	})
}
func (x *Exchange) frame() *pb.ExecuteFrame {
	return &pb.ExecuteFrame{Version: 1, RequestId: x.open.RequestId, SessionId: x.open.SessionId, ControlId: x.open.ControlId}
}
func (x *Exchange) send(f *pb.ExecuteFrame) error {
	x.emitMu.Lock()
	defer x.emitMu.Unlock()
	if x.emit == nil {
		return ErrClosed
	}
	if err := x.flow.FromCLI(f); err != nil {
		return err
	}
	return x.emit(f)
}
func (x *Exchange) sendWindow(n int) error {
	f := x.frame()
	f.Payload = &pb.ExecuteFrame_WindowUpdate{WindowUpdate: &pb.WindowUpdate{Direction: pb.Direction_DIRECTION_REQUEST, CreditBytes: uint32(n)}}
	return x.send(f)
}
func (x *Exchange) Feed(f *pb.ExecuteFrame) error {
	if x.ctx.Err() != nil {
		return ErrClosed
	}
	if tunnelwire.ValidateExecute(f) != nil || f.RequestId != x.open.RequestId || f.SessionId != x.open.SessionId || f.ControlId != x.open.ControlId || len(f.ProtoReflect().GetUnknown()) != 0 {
		return fail(pb.ErrorCode_ERROR_CODE_INVALID_REQUEST)
	}
	if err := x.flow.FromRegion(f); err != nil {
		if errors.Is(err, tunnelwire.ErrStreamClosed) {
			return ErrClosed
		}
		return err
	}
	switch p := f.Payload.(type) {
	case *pb.ExecuteFrame_RequestData:
		return x.body.push(p.RequestData)
	case *pb.ExecuteFrame_RequestEnd:
		return x.body.end(p.RequestEnd)
	case *pb.ExecuteFrame_WindowUpdate:
		if p.WindowUpdate.Direction == pb.Direction_DIRECTION_RESPONSE {
			return nil
		}
	case *pb.ExecuteFrame_Cancel:
		x.Cancel()
		return nil
	case *pb.ExecuteFrame_Error:
		x.Cancel()
		return nil
	}
	return fail(pb.ErrorCode_ERROR_CODE_INVALID_REQUEST)
}
func (x *Exchange) Run(emit func(*pb.ExecuteFrame) error) (err error) {
	if !x.run.CompareAndSwap(false, true) {
		return ErrClosed
	}
	defer x.Cancel()
	x.emitMu.Lock()
	x.emit = emit
	x.emitMu.Unlock()
	f := x.frame()
	f.Payload = &pb.ExecuteFrame_OpenAck{OpenAck: &pb.OpenAck{Result: pb.AdmissionResult_ADMISSION_RESULT_ACCEPTED}}
	if err = x.send(f); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			code := pb.ErrorCode_ERROR_CODE_ENDPOINT_UNAVAILABLE
			var typed *Error
			if errors.As(err, &typed) {
				code = typed.Code
			}
			var protocol *tunnelwire.ProtocolError
			if errors.As(err, &protocol) {
				code = protocol.Code
			}
			if errors.Is(err, tunnelwire.ErrWriterFull) {
				code = pb.ErrorCode_ERROR_CODE_SERVICE_OVERLOADED
			}
			if errors.Is(x.ctx.Err(), context.DeadlineExceeded) {
				code = pb.ErrorCode_ERROR_CODE_REQUEST_TIMEOUT
			}
			f := x.frame()
			f.Payload = &pb.ExecuteFrame_Error{Error: &pb.Termination{Direction: pb.Direction_DIRECTION_RESPONSE, Code: code}}
			_ = x.send(f)
			err = fail(code)
		}
	}()
	return x.execute()
}
func (x *Exchange) execute() error {
	meta := x.open.GetOpen().Request
	u, err := target(x.snapshot.EndpointUrl, meta)
	if err != nil {
		return err
	}
	headers, err := requestHeaders(meta)
	if err != nil {
		return err
	}
	req := &http.Request{Method: meta.Method, URL: u, Host: u.Host, Header: headers, Body: x.body, ContentLength: -1, Trailer: make(http.Header)}
	if meta.ContentLength != nil {
		req.ContentLength = *meta.ContentLength
		if *meta.ContentLength == 0 {
			req.Body = http.NoBody
		}
	}
	x.body.onEOF = func(trailers []*pb.Header) error {
		h, err := trailerHeaders(trailers)
		if err != nil {
			return err
		}
		for k, v := range h {
			req.Trailer[k] = v
		}
		return nil
	}
	var gotResponse atomic.Bool
	transport := x.transport()
	if requiresHTTP1(meta.Headers) {
		transport.Protocols = new(http.Protocols)
		transport.Protocols.SetHTTP1(true)
	}
	capture := captureFraming(transport, u)
	defer transport.CloseIdleConnections()
	var protocol atomic.Int32
	protocol.Store(int32(pb.HTTPVersion_HTTP_VERSION_1_1))
	trace := &httptrace.ClientTrace{TLSHandshakeDone: func(state tls.ConnectionState, _ error) {
		if state.NegotiatedProtocol == "h2" {
			protocol.Store(int32(pb.HTTPVersion_HTTP_VERSION_2))
		}
	}, GotFirstResponseByte: func() { gotResponse.Store(true) }, Got1xxResponse: func(code int, h textproto.MIMEHeader) error {
		if err := capture.restore(code, http.Header(h)); err != nil {
			return err
		}
		m, err := responseMetadata(code, http.Header(h), nil, pb.HTTPVersion(protocol.Load()))
		if err != nil {
			return fail(pb.ErrorCode_ERROR_CODE_INVALID_ENDPOINT_RESPONSE)
		}
		f := x.frame()
		f.Payload = &pb.ExecuteFrame_Informational{Informational: m}
		return x.send(f)
	}}
	req = req.WithContext(httptrace.WithClientTrace(x.ctx, trace))
	port := u.Port()
	if port == "" {
		port = "80"
		if u.Scheme == "https" {
			port = "443"
		}
	}
	connectCtx, stopConnect := context.WithTimeout(req.Context(), 5*time.Second)
	connection, err := transport.NewClientConn(connectCtx, u.Scheme, net.JoinHostPort(u.Hostname(), port))
	stopConnect()
	if err != nil {
		return err
	}
	defer connection.Close()
	stopConnection := context.AfterFunc(x.ctx, func() { _ = connection.Close() })
	defer stopConnection()
	// A single ClientConn.RoundTrip bypasses Transport's automatic retry loop.
	response, err := connection.RoundTrip(req)
	if err != nil {
		var typed *Error
		if errors.As(err, &typed) {
			return typed
		}
		var protocol *tunnelwire.ProtocolError
		if errors.As(err, &protocol) {
			return fail(protocol.Code)
		}
		if gotResponse.Load() {
			return fail(pb.ErrorCode_ERROR_CODE_INVALID_ENDPOINT_RESPONSE)
		}
		return err
	}
	defer response.Body.Close()
	if err := capture.restore(response.StatusCode, response.Header); err != nil {
		return err
	}
	if response.StatusCode == 101 {
		return fail(pb.ErrorCode_ERROR_CODE_INVALID_ENDPOINT_RESPONSE)
	}
	length := responseLength(response, meta.Method)
	version := pb.HTTPVersion_HTTP_VERSION_1_1
	if response.ProtoMajor == 2 {
		version = pb.HTTPVersion_HTTP_VERSION_2
	}
	metadata, err := responseMetadata(response.StatusCode, response.Header, length, version)
	if err != nil {
		return err
	}
	f := x.frame()
	f.Payload = &pb.ExecuteFrame_ResponseStart{ResponseStart: metadata}
	if err = x.send(f); err != nil {
		return err
	}
	buffer := make([]byte, chunk)
	var total int64
	var seq uint64
	for {
		n, readErr := response.Body.Read(buffer)
		if n > 0 {
			total += int64(n)
			if total > bodyLimit {
				return fail(pb.ErrorCode_ERROR_CODE_RESPONSE_BODY_TOO_LARGE)
			}
			for pos := 0; pos < n; {
				allowed, err := x.flow.WaitCredit(x.ctx, pb.Direction_DIRECTION_RESPONSE, n-pos)
				if err != nil {
					return err
				}
				seq++
				f := x.frame()
				f.Payload = &pb.ExecuteFrame_ResponseData{ResponseData: &pb.Data{SequenceNo: seq, Data: buffer[pos : pos+allowed]}}
				if err = x.send(f); err != nil {
					return err
				}
				pos += allowed
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return fail(pb.ErrorCode_ERROR_CODE_INVALID_ENDPOINT_RESPONSE)
		}
	}
	h, err := trailerHeadersFromHTTP(response.Trailer)
	if err != nil {
		return err
	}
	f = x.frame()
	f.Payload = &pb.ExecuteFrame_ResponseEnd{ResponseEnd: &pb.End{Complete: true, Trailers: h}}
	return x.send(f)
}
func trailerHeadersFromHTTP(h http.Header) ([]*pb.Header, error) {
	return fields(h, 16<<10, pb.ErrorCode_ERROR_CODE_RESPONSE_TRAILERS_TOO_LARGE)
}
func responseLength(r *http.Response, method string) *int64 {
	if r.StatusCode == 204 {
		return nil
	}
	if method == "HEAD" || r.StatusCode == 304 {
		if raw := r.Header.Get("Content-Length"); raw != "" {
			if n, err := strconv.ParseInt(raw, 10, 64); err == nil && n >= 0 {
				return &n
			}
		}
	}
	if r.ContentLength >= 0 {
		n := r.ContentLength
		return &n
	}
	return nil
}
