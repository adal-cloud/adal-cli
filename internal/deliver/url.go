package deliver

import (
	"net/url"
	"strings"
)

// Join appends the assignment path and query to an endpoint URL.
// An empty path leaves the base path unchanged.
// An empty query leaves the base query unchanged.
func Join(base, path, query string) (string, error) {
	parsed, err := url.Parse(base)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return "", errInvalidURL
	}
	extra := strings.Trim(path, "/")
	if extra != "" {
		basePath := strings.TrimRight(parsed.Path, "/")
		if basePath == "" {
			parsed.Path = "/" + extra
		} else {
			parsed.Path = basePath + "/" + extra
		}
		parsed.RawPath = ""
	}
	if query != "" {
		if parsed.RawQuery == "" {
			parsed.RawQuery = query
		} else {
			parsed.RawQuery += "&" + query
		}
	}
	return parsed.String(), nil
}

// Address joins a call URL for a log line.
// Userinfo is removed.
// The query is kept.
// A false result means the raw URL is not logged.
func Address(base, path, query string) (string, bool) {
	joined, err := Join(base, path, query)
	if err != nil {
		return "", false
	}
	parsed, err := url.Parse(joined)
	if err != nil || parsed.Host == "" {
		return "", false
	}
	parsed.User = nil
	return parsed.String(), true
}
