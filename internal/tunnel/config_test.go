package tunnel

import (
	"context"
	pb "github.com/adal-cloud/adal-cli/internal/tunnelpb"
	"github.com/adal-cloud/adal-cli/internal/tunnelwire"
	"testing"
)

func TestReconcileInitialAndNewControl(t *testing.T) {
	frames := make(chan *pb.ControlFrame, 4)
	writer := tunnelwire.NewWriter(t.Context(), func(_ context.Context, raw []byte) error {
		f, err := tunnelwire.UnmarshalControl(raw)
		if err == nil {
			frames <- f
		}
		return err
	})
	defer writer.Close()
	s := &connection{ctx: t.Context(), writer: writer}
	f := &pb.ControlFrame{Version: 1, ControlId: controlID, SessionId: sessionID, TunnelId: tunnelID, OwnerId: owner, RegionId: owner, Payload: &pb.ControlFrame_Reconcile{Reconcile: &pb.Reconcile{}}}
	if err := s.control(f); err != nil {
		t.Fatal("initial Reconcile rejected", err)
	}
	if reply := <-frames; reply.GetState() == nil || reply.GetState().Ready {
		t.Fatal("unconfigured session ready")
	}
	cfg := snapshot("http://localhost:3000")
	f.Payload = &pb.ControlFrame_Prepare{Prepare: cfg}
	if err := s.control(f); err != nil {
		t.Fatal(err)
	}
	<-frames
	f.Payload = &pb.ControlFrame_Activate{Activate: &pb.ActivateRevision{Next: &pb.RevisionState{Revision: cfg.Revision, ConfigHash: cfg.ConfigHash}}}
	if err := s.control(f); err != nil {
		t.Fatal(err)
	}
	<-frames
	f.ControlId = owner
	f.Payload = &pb.ControlFrame_Reconcile{Reconcile: &pb.Reconcile{}}
	if err := s.control(f); err != nil {
		t.Fatal(err)
	}
	if reply := <-frames; reply.GetState() == nil || !reply.GetState().Ready || s.activeControl != owner || reply.GetState().Active.Revision != 1 {
		t.Fatal("new Control did not bind active revision")
	}
}
