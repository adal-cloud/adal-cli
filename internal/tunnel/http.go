package tunnel

import (
	"crypto/tls"
	pb "github.com/adal-cloud/adal-cli/internal/tunnelpb"
	"github.com/adal-cloud/adal-cli/internal/tunnelwire"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

func target(base string, m *pb.RequestMetadata) (*url.URL, error) {
	u, err := url.Parse(base)
	if err != nil {
		return nil, fail(pb.ErrorCode_ERROR_CODE_INVALID_REQUEST)
	}
	path := string(m.RawPath)
	if path == "*" && m.Method == "OPTIONS" && len(m.RawQuery) == 0 && !m.ForceQuery {
		u.Path = "*"
		u.RawPath = ""
		return u, nil
	}
	if !strings.HasPrefix(path, "/") || strings.ContainsAny(path, "?#\r\n") {
		return nil, fail(pb.ErrorCode_ERROR_CODE_INVALID_REQUEST)
	}
	prefix := u.EscapedPath()
	if strings.HasSuffix(prefix, "/") && strings.HasPrefix(path, "/") {
		prefix = strings.TrimSuffix(prefix, "/")
	}
	raw := prefix + path
	decoded, err := url.PathUnescape(raw)
	if err != nil {
		return nil, fail(pb.ErrorCode_ERROR_CODE_INVALID_REQUEST)
	}
	u.Path = decoded
	u.RawPath = raw
	u.RawQuery = string(m.RawQuery)
	u.ForceQuery = m.ForceQuery
	if u.EscapedPath() != raw || strings.ContainsAny(u.RawQuery, "#\r\n") {
		return nil, fail(pb.ErrorCode_ERROR_CODE_INVALID_REQUEST)
	}
	return u, nil
}
func freshTransport() *http.Transport {
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetHTTP2(true)
	return &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}, TLSHandshakeTimeout: 5 * time.Second,
		DisableKeepAlives: true, DisableCompression: true, Protocols: protocols,
		ExpectContinueTimeout: time.Second, MaxResponseHeaderBytes: (128 << 10) + 4096}
}

// The HTTP/2 encoder normalizes Cookie and keeps only the first User-Agent.
func requiresHTTP1(fields []*pb.Header) bool {
	agents := 0
	for _, field := range fields {
		if strings.EqualFold(field.Name, "Cookie") {
			return true
		}
		if strings.EqualFold(field.Name, "User-Agent") {
			for _, value := range field.RawValues {
				agents++
				if len(value) == 0 || agents > 1 {
					return true
				}
			}
		}
	}
	return false
}
func incomingHeaders(fields []*pb.Header) (http.Header, error) {
	h := make(http.Header)
	for _, f := range fields {
		for _, v := range f.RawValues {
			for _, c := range v {
				if c == 127 || (c < 32 && c != '\t') {
					return nil, fail(pb.ErrorCode_ERROR_CODE_INVALID_REQUEST)
				}
			}
			h.Add(f.Name, string(v))
		}
	}
	stripHop(h)
	return h, nil
}
func stripHop(h http.Header) {
	for _, v := range h.Values("Connection") {
		for _, name := range strings.Split(v, ",") {
			h.Del(strings.TrimSpace(name))
		}
	}
	for _, name := range []string{"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "TE", "Trailer", "Transfer-Encoding", "Upgrade"} {
		h.Del(name)
	}
}
func fields(h http.Header, limit int, code pb.ErrorCode) ([]*pb.Header, error) {
	out := make([]*pb.Header, 0, len(h))
	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		values := h[k]
		if len(values) == 0 {
			continue
		}
		f := &pb.Header{Name: k}
		for _, v := range values {
			limit -= len(k) + len(v)
			if limit < 0 {
				return nil, fail(code)
			}
			f.RawValues = append(f.RawValues, []byte(v))
		}
		out = append(out, f)
	}
	return out, nil
}
func requestHeaders(m *pb.RequestMetadata) (http.Header, error) {
	for _, field := range m.Headers {
		if strings.EqualFold(field.Name, "Upgrade") {
			return nil, fail(pb.ErrorCode_ERROR_CODE_UPGRADE_NOT_SUPPORTED)
		}
		if strings.EqualFold(field.Name, "Connection") {
			for _, value := range field.RawValues {
				for _, token := range strings.Split(string(value), ",") {
					if strings.EqualFold(strings.TrimSpace(token), "upgrade") {
						return nil, fail(pb.ErrorCode_ERROR_CODE_UPGRADE_NOT_SUPPORTED)
					}
				}
			}
		}
	}
	h, err := incomingHeaders(m.Headers)
	if err != nil {
		return nil, err
	}
	continued, err := tunnelwire.ExpectContinue(h.Values("Expect"))
	if err != nil {
		return nil, fail(pb.ErrorCode_ERROR_CODE_EXPECTATION_FAILED)
	}
	if continued {
		h.Set("Expect", "100-continue")
	}
	if m.Method == "TRACE" && (m.ContentLength == nil || *m.ContentLength != 0) {
		return nil, fail(pb.ErrorCode_ERROR_CODE_INVALID_REQUEST)
	}
	if m.Method == "OPTIONS" || m.Method == "TRACE" {
		values := h.Values("Max-Forwards")
		if len(values) > 0 {
			if len(values) != 1 {
				return nil, fail(pb.ErrorCode_ERROR_CODE_INVALID_REQUEST)
			}
			next, zero, err := tunnelwire.DecrementMaxForwards(values[0])
			if err != nil || zero {
				return nil, fail(pb.ErrorCode_ERROR_CODE_INVALID_REQUEST)
			}
			h.Set("Max-Forwards", next)
		}
	}
	h.Del("Host")
	h.Del("Content-Length")
	for k := range h {
		if strings.EqualFold(k, "Forwarded") || strings.HasPrefix(strings.ToLower(k), "x-forwarded-") {
			delete(h, k)
		}
	}
	sender := m.SenderIp
	if strings.Contains(sender, ":") {
		sender = "[" + sender + "]"
	}
	h.Set("Forwarded", "for="+strconv.Quote(sender)+";host="+strconv.Quote(m.Authority)+";proto="+m.PublicScheme)
	h.Set("X-Forwarded-For", m.SenderIp)
	h.Set("X-Forwarded-Host", m.Authority)
	h.Set("X-Forwarded-Proto", m.PublicScheme)
	via := "1.1 adal-tunnel"
	if m.Protocol == pb.HTTPVersion_HTTP_VERSION_2 {
		via = "2 adal-tunnel"
	}
	h.Add("Via", via)
	// Request.Write special-cases the canonical key and keeps only its first value.
	values, exists := h["User-Agent"]
	h["User-Agent"] = nil
	if exists {
		h["user-agent"] = values
	}
	return h, nil
}
func trailerHeaders(h []*pb.Header) (http.Header, error) {
	out, err := incomingHeaders(h)
	if err != nil {
		return nil, err
	}
	for k := range out {
		switch strings.ToLower(k) {
		case "host", "content-length", "authorization", "cookie", "set-cookie", "expect", "forwarded":
			delete(out, k)
		default:
			if strings.HasPrefix(strings.ToLower(k), "x-forwarded-") {
				delete(out, k)
			}
		}
	}
	return out, nil
}
func responseMetadata(status int, h http.Header, length *int64, version pb.HTTPVersion) (*pb.ResponseMetadata, error) {
	headers, err := fields(h, 64<<10, pb.ErrorCode_ERROR_CODE_RESPONSE_HEADERS_TOO_LARGE)
	if err != nil {
		return nil, err
	}
	return &pb.ResponseMetadata{StatusCode: uint32(status), Headers: headers, Protocol: version, ContentLength: length}, nil
}
