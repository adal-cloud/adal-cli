package session

import (
	"errors"
	"net/url"
	"strings"
)

var ErrInvalidHost = errors.New("host must be a ws:// or wss:// origin")

// URL builds the session address without accepting credentials or URL parameters.
func URL(host string) (string, error) {
	u, err := url.Parse(host)
	if err != nil || (u.Scheme != "ws" && u.Scheme != "wss") || u.Hostname() == "" ||
		u.User != nil || (u.Path != "" && u.Path != "/") || u.RawPath != "" ||
		strings.ContainsAny(host, "?#") {
		return "", ErrInvalidHost
	}
	u.Path = sessionPath
	return u.String(), nil
}
