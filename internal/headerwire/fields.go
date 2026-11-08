package headerwire

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"
)

type Fields map[string][]string

func (h Fields) MarshalJSON() ([]byte, error) {
	out := make(map[string][]any, len(h))
	for name, values := range h {
		out[name] = make([]any, 0, len(values))
		for _, value := range values {
			if utf8.ValidString(value) && !strings.ContainsRune(value, 0) {
				out[name] = append(out[name], value)
			} else {
				out[name] = append(out[name], struct {
					Base64 string `json:"base64"`
				}{base64.StdEncoding.EncodeToString([]byte(value))})
			}
		}
	}
	return json.Marshal(out)
}

func (h *Fields) UnmarshalJSON(raw []byte) error {
	if !utf8.Valid(raw) {
		return errors.New("invalid header JSON encoding")
	}
	var fields map[string][]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return errors.New("invalid header object")
	}
	if fields == nil {
		*h = nil
		return nil
	}
	out := make(Fields, len(fields))
	for name, values := range fields {
		out[name] = make([]string, 0, len(values))
		for _, rawValue := range values {
			rawValue = bytes.TrimSpace(rawValue)
			var value string
			if len(rawValue) > 0 && rawValue[0] == '"' {
				if err := json.Unmarshal(rawValue, &value); err != nil {
					return errors.New("invalid header text")
				}
			} else {
				var tagged map[string]json.RawMessage
				if err := json.Unmarshal(rawValue, &tagged); err != nil || len(tagged) != 1 {
					return errors.New("invalid header value")
				}
				encoded, ok := tagged["base64"]
				if !ok {
					return errors.New("invalid header encoding")
				}
				var text string
				if json.Unmarshal(encoded, &text) != nil || bytes.Equal(bytes.TrimSpace(encoded), []byte("null")) {
					return errors.New("invalid header bytes")
				}
				decoded, err := base64.StdEncoding.Strict().DecodeString(text)
				if err != nil || base64.StdEncoding.EncodeToString(decoded) != text {
					return errors.New("invalid header bytes")
				}
				value = string(decoded)
			}
			out[name] = append(out[name], value)
		}
	}
	*h = out
	return nil
}
