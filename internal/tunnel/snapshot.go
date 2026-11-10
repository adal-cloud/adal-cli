package tunnel

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	pb "github.com/adal-cloud/adal-cli/internal/tunnelpb"
	"github.com/adal-cloud/adal-cli/internal/tunnelwire"
	"net/url"
	"strconv"
	"strings"
	"unicode/utf8"
)

func SnapshotHash(s *pb.ConfigSnapshot) [32]byte {
	if s == nil {
		return [32]byte{}
	}
	raw, _ := json.Marshal(struct {
		EndpointURL           string `json:"endpoint_url"`
		TimeoutSeconds        uint32 `json:"timeout_seconds"`
		MaxConcurrentRequests uint32 `json:"max_concurrent_requests"`
		SaveRequest           bool   `json:"save_request"`
		SaveResponse          bool   `json:"save_response"`
		Enabled               bool   `json:"enabled"`
		Deleted               bool   `json:"deleted"`
	}{s.EndpointUrl, s.TimeoutSeconds, s.MaxConcurrentRequests, s.SaveRequest, s.SaveResponse, s.Enabled, s.Deleted})
	return sha256.Sum256(raw)
}
func validateSnapshot(s *pb.ConfigSnapshot) error {
	if tunnelwire.ValidateSnapshot(s) != nil || len(s.ProtoReflect().GetUnknown()) != 0 || !utf8.ValidString(s.EndpointUrl) {
		return fail(pb.ErrorCode_ERROR_CODE_TUNNEL_NOT_READY)
	}
	u, err := url.Parse(s.EndpointUrl)
	if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.ForceQuery || strings.Contains(s.EndpointUrl, "#") || u.Opaque != "" || strings.HasSuffix(u.Host, ":") {
		return fail(pb.ErrorCode_ERROR_CODE_INVALID_REQUEST)
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return fail(pb.ErrorCode_ERROR_CODE_INVALID_REQUEST)
		}
	}
	hash := SnapshotHash(s)
	if !bytes.Equal(hash[:], s.ConfigHash) {
		return fail(pb.ErrorCode_ERROR_CODE_TUNNEL_NOT_READY)
	}
	if s.Deleted {
		return fail(pb.ErrorCode_ERROR_CODE_TUNNEL_DELETED)
	}
	if !s.Enabled {
		return fail(pb.ErrorCode_ERROR_CODE_TUNNEL_DISABLED)
	}
	return nil
}
