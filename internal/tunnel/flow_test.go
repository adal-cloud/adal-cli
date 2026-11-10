package tunnel

import (
	"bytes"
	"context"
	pb "github.com/adal-cloud/adal-cli/internal/tunnelpb"
	"github.com/adal-cloud/adal-cli/internal/tunnelwire"
	"testing"
	"time"
)

func TestRequestWindowAndCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	credits := 0
	b := newBody(ctx, func(n int) error { credits += n; return nil })
	for i := uint64(1); i <= 4; i++ {
		if err := b.push(&pb.Data{SequenceNo: i, Data: bytes.Repeat([]byte{'x'}, 64<<10)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.push(&pb.Data{SequenceNo: 5, Data: []byte{1}}); err == nil {
		t.Fatal("credit overrun accepted")
	}
	buf := make([]byte, 32<<10)
	if n, err := b.Read(buf); err != nil || n != len(buf) || credits != len(buf) {
		t.Fatal(n, err, credits)
	}
	if err := b.push(&pb.Data{SequenceNo: 5, Data: []byte{1}}); err != nil {
		t.Fatal(err)
	}
	if err := b.end(&pb.End{Complete: true}); err != nil {
		t.Fatal(err)
	}
	if err := b.push(&pb.Data{SequenceNo: 6, Data: []byte{1}}); err == nil {
		t.Fatal("data after End accepted")
	}
	empty := newBody(ctx, func(int) error { return nil })
	done := make(chan error, 1)
	go func() { _, err := empty.Read(buf); done <- err }()
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancel ignored")
		}
	case <-time.After(time.Second):
		t.Fatal("reader blocked after cancellation")
	}
}
func TestResponseCreditIsBounded(t *testing.T) {
	f := opening(snapshot("http://localhost"), "GET", "/", "")
	c, err := tunnelwire.NewStream(f)
	if err != nil {
		t.Fatal(err)
	}
	f.Payload = &pb.ExecuteFrame_OpenAck{OpenAck: &pb.OpenAck{Result: pb.AdmissionResult_ADMISSION_RESULT_ACCEPTED}}
	if err = c.FromCLI(f); err != nil {
		t.Fatal(err)
	}
	f.Payload = &pb.ExecuteFrame_ResponseStart{ResponseStart: &pb.ResponseMetadata{StatusCode: 200, Protocol: pb.HTTPVersion_HTTP_VERSION_1_1}}
	if err = c.FromCLI(f); err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 4; i++ {
		f.Payload = &pb.ExecuteFrame_ResponseData{ResponseData: &pb.Data{SequenceNo: i, Data: make([]byte, 64<<10)}}
		if err := c.FromCLI(f); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { _, err := c.WaitCredit(ctx, pb.Direction_DIRECTION_RESPONSE, 1); done <- err }()
	cancel()
	if err := <-done; err == nil {
		t.Fatal("empty credit ignored cancellation")
	}
	f.Payload = &pb.ExecuteFrame_WindowUpdate{WindowUpdate: &pb.WindowUpdate{Direction: pb.Direction_DIRECTION_RESPONSE, CreditBytes: 256 << 10}}
	if err := c.FromRegion(f); err != nil {
		t.Fatal(err)
	}
	f.GetWindowUpdate().CreditBytes = 1
	if err := c.FromRegion(f); err == nil {
		t.Fatal("excessive credit accepted")
	}
}
