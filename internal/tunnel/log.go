package tunnel

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"strings"

	"github.com/adal-cloud/adal-cli/internal/console"
	pb "github.com/adal-cloud/adal-cli/internal/tunnelpb"
	"github.com/adal-cloud/adal-cli/internal/tunnelwire"
	"github.com/coder/websocket"
)

var errHandshake = errors.New("WebSocket handshake failed")

func printEvent(logger *log.Logger, event console.Event) {
	logger.Print(console.Format(event))
}

func logHost(address, token string) string {
	u, err := url.Parse(address)
	if err != nil || (u.Scheme != "ws" && u.Scheme != "wss") || u.Host == "" {
		return ""
	}
	host := u.Scheme + "://" + u.Host
	if token != "" && strings.Contains(host, token) {
		return ""
	}
	return host
}

// Session and endpoint errors can contain URLs or arbitrary peer text.
func sessionFailure(err error) string {
	var dns *net.DNSError
	switch {
	case errors.Is(err, errProtocol):
		return "Tunnel protocol error"
	case errors.As(err, &dns):
		return "WebSocket host lookup failed"
	case errors.Is(err, context.DeadlineExceeded):
		return "WebSocket operation timed out"
	case errors.Is(err, errHandshake):
		return errHandshake.Error()
	}
	if code := errorCode(err); code != "" {
		return code
	}
	var closed *sessionClosedError
	if errors.As(err, &closed) {
		return closed.reason.String()
	}
	if status := websocket.CloseStatus(err); status != -1 {
		return fmt.Sprintf("WebSocket closed with code %d", status)
	}
	return "WebSocket connection lost"
}

func errorCode(err error) string {
	var endpoint *Error
	var protocol *tunnelwire.ProtocolError
	switch {
	case errors.As(err, &endpoint):
		return endpoint.Code.String()
	case errors.As(err, &protocol):
		return protocol.Code.String()
	default:
		return ""
	}
}

func (s *connection) logControl(f *pb.ControlFrame, previous *pb.ConfigSnapshot) {
	var event console.Event
	switch f.Payload.(type) {
	case *pb.ControlFrame_Prepare:
		event = console.Event{Title: "Tunnel configuration prepared", Revision: s.pending.GetRevision()}
	case *pb.ControlFrame_Activate:
		if s.active == previous {
			return
		}
		event = console.Event{Title: "Tunnel configuration activated", Revision: s.active.GetRevision()}
	case *pb.ControlFrame_Reconcile:
		event = console.Event{Title: "Tunnel state reconciled", Revision: s.active.GetRevision()}
	default:
		return
	}
	printEvent(s.logger, event)
}

func (s *connection) logFrame(f *pb.ExecuteFrame) {
	event := console.Event{Request: f.RequestId}
	switch p := f.Payload.(type) {
	case *pb.ExecuteFrame_Open:
		event.Title = "Tunnel request received"
	case *pb.ExecuteFrame_OpenAck:
		event.Title = "Tunnel request accepted"
		if p.OpenAck.Result == pb.AdmissionResult_ADMISSION_RESULT_REJECTED {
			event.Title = "Tunnel request rejected"
			event.Code = p.OpenAck.Error.String()
		}
	case *pb.ExecuteFrame_RequestData:
		event.Title = "Tunnel request data received"
		event.Bytes = len(p.RequestData.Data)
		event.Sequence = p.RequestData.SequenceNo
	case *pb.ExecuteFrame_RequestEnd:
		event.Title = "Tunnel request body received"
		if !p.RequestEnd.Complete {
			event.Outcome = "incomplete"
		}
	case *pb.ExecuteFrame_Informational:
		event.Title = "Tunnel informational response sent"
		event.Status = int(p.Informational.StatusCode)
	case *pb.ExecuteFrame_ResponseStart:
		event.Title = "Tunnel response started"
		event.Status = int(p.ResponseStart.StatusCode)
	case *pb.ExecuteFrame_ResponseData:
		event.Title = "Tunnel response data sent"
		event.Bytes = len(p.ResponseData.Data)
		event.Sequence = p.ResponseData.SequenceNo
	case *pb.ExecuteFrame_Cancel:
		event.Title = "Tunnel request cancelled"
		event.Code = p.Cancel.Code.String()
	case *pb.ExecuteFrame_Error:
		event.Title = "Tunnel stream error"
		event.Code = p.Error.Code.String()
	default:
		return
	}
	printEvent(s.logger, event)
}
