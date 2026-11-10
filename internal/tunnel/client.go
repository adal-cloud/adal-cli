package tunnel

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	pb "github.com/adal-cloud/adal-cli/internal/tunnelpb"
	"github.com/adal-cloud/adal-cli/internal/tunnelwire"
	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"
	"io"
	"math/rand/v2"
	"sync"
	"time"
)

var errProtocol = errors.New("tunnel protocol error")

// Run keeps a live session. Disconnects cancel all HTTP calls; nothing is replayed.
func Run(ctx context.Context, address, token string, stdout io.Writer) error {
	if stdout == nil {
		stdout = io.Discard
	}
	delay := time.Second
	for {
		if ctx.Err() != nil {
			return nil
		}
		started := time.Now()
		_, _ = io.WriteString(stdout, "Connecting\n")
		err := connect(ctx, address, token)
		if ctx.Err() != nil {
			return nil
		}
		if fatal := fatalClose(err); fatal != nil {
			return fatal
		}
		if time.Since(started) >= 60*time.Second {
			delay = time.Second
		}
		_, _ = io.WriteString(stdout, "Reconnecting\n")
		pause := min(10*time.Second, time.Duration(float64(delay)*(0.8+rand.Float64()*0.4)))
		timer := time.NewTimer(pause)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
		delay = min(10*time.Second, delay*2)
	}
}
func fatalClose(err error) error {
	switch websocket.CloseStatus(err) {
	case 4001:
		return errors.New("Token is invalid")
	case 4003:
		return errors.New("Tunnel access is disabled")
	case 4005:
		return errors.New("CLI upgrade is required")
	}
	var fatal *sessionClosedError
	if errors.As(err, &fatal) {
		switch fatal.reason {
		case pb.SessionCloseReason_SESSION_CLOSE_REASON_INVALID_TOKEN:
			return errors.New("Token is invalid")
		case pb.SessionCloseReason_SESSION_CLOSE_REASON_TUNNEL_DISABLED:
			return errors.New("Tunnel is disabled")
		case pb.SessionCloseReason_SESSION_CLOSE_REASON_TUNNEL_DELETED:
			return errors.New("Tunnel is deleted")
		case pb.SessionCloseReason_SESSION_CLOSE_REASON_ACCESS_DISABLED:
			return errors.New("Access is disabled")
		}
	}
	return nil
}

type sessionClosedError struct{ reason pb.SessionCloseReason }

func (*sessionClosedError) Error() string { return "tunnel session closed" }

type connection struct {
	ctx                            context.Context
	cancel                         context.CancelFunc
	socket                         *websocket.Conn
	writeMu                        sync.Mutex
	executor                       *Executor
	pending, active                *pb.ConfigSnapshot
	identity                       *pb.ControlFrame
	preparedControl, activeControl string
	workers                        sync.WaitGroup
}

func connect(parent context.Context, address, token string) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	c, _, err := websocket.Dial(ctx, address, nil)
	if err != nil {
		return err
	}
	defer c.CloseNow()
	c.SetReadLimit(tunnelwire.MaxFrameBytes)
	s := &connection{ctx: ctx, cancel: cancel, socket: c, executor: NewExecutor()}
	defer func() {
		cancel()
		s.executor.mu.Lock()
		active := make([]*Exchange, 0, len(s.executor.active))
		for _, x := range s.executor.active {
			active = append(active, x)
		}
		s.executor.mu.Unlock()
		for _, x := range active {
			x.Cancel()
		}
		s.workers.Wait()
	}()
	hello, _ := json.Marshal(struct {
		Type         string   `json:"type"`
		Version      int      `json:"version"`
		Token        string   `json:"token"`
		Capabilities []string `json:"capabilities"`
	}{"hello", 1, token, []string{"http_tunnel_v1"}})
	if err = s.write(websocket.MessageText, hello); err != nil {
		return err
	}
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				pingCtx, stop := context.WithTimeout(ctx, 10*time.Second)
				err := c.Ping(pingCtx)
				stop()
				if err != nil {
					cancel()
					_ = c.CloseNow()
					return
				}
			}
		}
	}()
	for {
		typ, data, err := c.Read(ctx)
		if err != nil {
			return err
		}
		if typ != websocket.MessageBinary {
			return errProtocol
		}
		if control, err := tunnelwire.UnmarshalControl(data); err == nil && len(control.ProtoReflect().GetUnknown()) == 0 {
			if err = s.control(control); err != nil {
				return err
			}
			continue
		}
		frame, err := tunnelwire.UnmarshalExecute(data)
		if err != nil || len(frame.ProtoReflect().GetUnknown()) != 0 {
			return errProtocol
		}
		if frame.GetOpen() != nil {
			if s.identity == nil || s.active == nil || frame.SessionId != s.identity.SessionId || frame.ControlId != s.activeControl {
				return errProtocol
			}
			x, err := s.executor.Start(ctx, s.active, frame)
			if err != nil {
				code := pb.ErrorCode_ERROR_CODE_TUNNEL_NOT_READY
				var typed *Error
				if errors.As(err, &typed) {
					code = typed.Code
				}
				reply := &pb.ExecuteFrame{Version: 1, RequestId: frame.RequestId, SessionId: frame.SessionId, ControlId: frame.ControlId, Payload: &pb.ExecuteFrame_OpenAck{OpenAck: &pb.OpenAck{Result: pb.AdmissionResult_ADMISSION_RESULT_REJECTED, Error: code}}}
				if err = s.executeWrite(reply); err != nil {
					return err
				}
				continue
			}
			s.workers.Add(1)
			go func() { defer s.workers.Done(); _ = x.Run(s.executeWrite) }()
		} else {
			s.executor.mu.Lock()
			x := s.executor.active[frame.RequestId]
			closed := s.executor.closed[frame.RequestId]
			s.executor.mu.Unlock()
			if x == nil {
				if time.Now().Before(closed.until) && frame.SessionId == closed.sessionID && frame.ControlId == closed.controlID {
					continue
				}
				return errProtocol
			}
			if err = x.Feed(frame); err != nil {
				return err
			}
		}
	}
}
func (s *connection) write(typ websocket.MessageType, data []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()
	err := s.socket.Write(ctx, typ, data)
	if err != nil {
		s.cancel()
	}
	return err
}
func (s *connection) executeWrite(f *pb.ExecuteFrame) error {
	raw, err := tunnelwire.MarshalExecute(f)
	if err != nil {
		return err
	}
	return s.write(websocket.MessageBinary, raw)
}
func (s *connection) control(f *pb.ControlFrame) error {
	if s.identity == nil {
		if f.GetPrepare() == nil {
			return errProtocol
		}
		s.identity = proto.Clone(f).(*pb.ControlFrame)
	}
	id := s.identity
	if f.SessionId != id.SessionId || f.TunnelId != id.TunnelId || f.OwnerId != id.OwnerId || f.RegionId != id.RegionId {
		return errProtocol
	}
	reply := &pb.ControlFrame{Version: 1, SessionId: id.SessionId, TunnelId: id.TunnelId, OwnerId: id.OwnerId, RegionId: id.RegionId, ControlId: f.ControlId}
	switch p := f.Payload.(type) {
	case *pb.ControlFrame_Prepare:
		if err := validateSnapshot(p.Prepare); err != nil {
			return err
		}
		if s.active != nil && p.Prepare.Revision <= s.active.Revision {
			return errProtocol
		}
		s.pending = proto.Clone(p.Prepare).(*pb.ConfigSnapshot)
		s.preparedControl = f.ControlId
		reply.Payload = &pb.ControlFrame_Prepared{Prepared: &pb.RevisionState{Revision: s.pending.Revision, ConfigHash: s.pending.ConfigHash}}
	case *pb.ControlFrame_Activate:
		a := p.Activate
		old := int64(0)
		if s.active != nil {
			old = s.active.Revision
		}
		if s.active != nil && f.ControlId == s.activeControl && a.Next.Revision == s.active.Revision && bytes.Equal(a.Next.ConfigHash, s.active.ConfigHash) {
			reply.Payload = &pb.ControlFrame_Active{Active: &pb.RevisionState{Revision: s.active.Revision, ConfigHash: s.active.ConfigHash}}
			break
		}
		if s.pending == nil || f.ControlId != s.preparedControl || a.ExpectedOldRevision != old || a.Next.Revision != s.pending.Revision || !bytes.Equal(a.Next.ConfigHash, s.pending.ConfigHash) {
			return errProtocol
		}
		s.active = s.pending
		s.pending = nil
		s.activeControl = f.ControlId
		reply.Payload = &pb.ControlFrame_Active{Active: &pb.RevisionState{Revision: s.active.Revision, ConfigHash: s.active.ConfigHash}}
	case *pb.ControlFrame_Reconcile:
		state := &pb.ControlState{Ready: s.active != nil}
		if s.active != nil {
			state.Active = &pb.RevisionState{Revision: s.active.Revision, ConfigHash: s.active.ConfigHash}
		}
		reply.Payload = &pb.ControlFrame_State{State: state}
	case *pb.ControlFrame_SessionClosed:
		return &sessionClosedError{reason: p.SessionClosed.Reason}
	default:
		return errProtocol
	}
	raw, err := tunnelwire.MarshalControl(reply)
	if err != nil {
		return err
	}
	return s.write(websocket.MessageBinary, raw)
}
