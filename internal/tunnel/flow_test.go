package tunnel

import (
	"bytes"
	"context"
	pb "github.com/adal-cloud/adal-cli/internal/tunnelpb"
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
	c := newCredit()
	for range 4 {
		if err := c.take(t.Context(), 64<<10); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- c.take(ctx, 1) }()
	cancel()
	if err := <-done; err == nil {
		t.Fatal("empty credit ignored cancellation")
	}
	if err := c.grant(256 << 10); err != nil {
		t.Fatal(err)
	}
	if err := c.grant(1); err == nil {
		t.Fatal("excessive credit accepted")
	}
}
