package tunnel

import (
	"bytes"
	"context"
	"crypto/tls"
	pb "github.com/adal-cloud/adal-cli/internal/tunnelpb"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"sync"
)

const headerWireLimit = 128 << 10

type capturedHeader struct {
	code   int
	fields http.Header
}
type headerCapture struct {
	mu                sync.Mutex
	buffer            []byte
	queue             []capturedHeader
	enabled, finished bool
}
type headerConn struct {
	net.Conn
	capture *headerCapture
}

func (c *headerConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		if captureErr := c.capture.observe(p[:n]); captureErr != nil {
			return 0, captureErr
		}
	}
	return n, err
}
func (c *headerCapture) wrap(conn net.Conn) net.Conn {
	c.mu.Lock()
	c.enabled = true
	c.mu.Unlock()
	return &headerConn{Conn: conn, capture: c}
}
func (c *headerCapture) observe(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finished {
		return nil
	}
	if c.buffer == nil {
		c.buffer = make([]byte, 0, headerWireLimit+1)
	}
	for {
		if end := bytes.Index(c.buffer, []byte("\r\n\r\n")); end >= 0 {
			if end+4 > headerWireLimit {
				return c.overflow()
			}
			header, err := specialHeader(c.buffer[:end+4])
			if err != nil {
				return err
			}
			if len(c.queue) >= 18 {
				return fail(pb.ErrorCode_ERROR_CODE_INVALID_ENDPOINT_RESPONSE)
			}
			c.queue = append(c.queue, header)
			if header.code < 100 || header.code >= 200 || header.code == 101 {
				c.finished = true
				c.buffer = nil
				return nil
			}
			c.buffer = c.buffer[end+4:]
			continue
		}
		if len(c.buffer) > headerWireLimit {
			return c.overflow()
		}
		if len(data) == 0 {
			return nil
		}
		n := min(len(data), headerWireLimit+1-len(c.buffer))
		c.buffer = append(c.buffer, data[:n]...)
		data = data[n:]
	}
}

func (c *headerCapture) overflow() error {
	line, _, _ := bytes.Cut(c.buffer, []byte("\r\n"))
	status := bytes.Fields(line)
	if len(status) >= 2 {
		code, _ := strconv.Atoi(string(status[1]))
		if code >= 100 && code < 200 {
			return fail(pb.ErrorCode_ERROR_CODE_INVALID_ENDPOINT_RESPONSE)
		}
	}
	return fail(pb.ErrorCode_ERROR_CODE_RESPONSE_HEADERS_TOO_LARGE)
}
func specialHeader(raw []byte) (capturedHeader, error) {
	line, rest, _ := bytes.Cut(raw, []byte("\r\n"))
	status := bytes.Fields(line)
	if len(status) < 2 {
		return capturedHeader{}, fail(pb.ErrorCode_ERROR_CODE_INVALID_ENDPOINT_RESPONSE)
	}
	code, err := strconv.Atoi(string(status[1]))
	if err != nil {
		return capturedHeader{}, fail(pb.ErrorCode_ERROR_CODE_INVALID_ENDPOINT_RESPONSE)
	}
	fields := make(http.Header)
	current := ""
	for len(rest) > 0 {
		line, rest, _ = bytes.Cut(rest, []byte("\r\n"))
		if len(line) == 0 {
			break
		}
		if line[0] == ' ' || line[0] == '\t' {
			if current != "" {
				values := fields[current]
				values[len(values)-1] += " " + textproto.TrimString(string(line))
			}
			continue
		}
		current = ""
		name, value, ok := bytes.Cut(line, []byte(":"))
		if !ok {
			continue
		}
		key := textproto.CanonicalMIMEHeaderKey(string(name))
		switch key {
		case "Connection", "Transfer-Encoding", "Trailer":
			current = key
			fields.Add(key, textproto.TrimString(string(value)))
		}
	}
	return capturedHeader{code, fields}, nil
}
func (c *headerCapture) restore(code int, h http.Header) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.enabled {
		return nil
	}
	if len(c.queue) == 0 || c.queue[0].code != code {
		return fail(pb.ErrorCode_ERROR_CODE_INVALID_ENDPOINT_RESPONSE)
	}
	fields := c.queue[0].fields
	c.queue[0] = capturedHeader{}
	c.queue = c.queue[1:]
	for name, values := range fields {
		h[name] = values
	}
	return nil
}

// The parser removes Connection when it contains close. Retain only framing
// fields before parsing, never a body. HTTP/2 rejects connection-specific fields.
func captureFraming(tr *http.Transport, target *url.URL) *headerCapture {
	capture := new(headerCapture)
	dial := tr.DialContext
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	tr.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := dial(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return capture.wrap(conn), nil
	}
	tr.DialTLSContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		raw, err := dial(ctx, network, address)
		if err != nil {
			return nil, err
		}
		cfg := tr.TLSClientConfig.Clone()
		if cfg.ServerName == "" {
			cfg.ServerName = target.Hostname()
		}
		secured := tls.Client(raw, cfg)
		if err = secured.HandshakeContext(ctx); err != nil {
			raw.Close()
			return nil, err
		}
		if strings.EqualFold(secured.ConnectionState().NegotiatedProtocol, "h2") {
			return secured, nil
		}
		return capture.wrap(secured), nil
	}
	return capture
}
