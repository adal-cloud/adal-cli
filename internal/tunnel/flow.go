package tunnel

import (
	"context"
	pb "github.com/adal-cloud/adal-cli/internal/tunnelpb"
	"io"
	"sync"
)

const window = 256 << 10
const chunk = 64 << 10
const bodyLimit = 128 << 20

// A byte ring bounds memory even when a peer sends many tiny frames.
type body struct {
	ctx           context.Context
	mu            sync.Mutex
	wake          chan struct{}
	data          [window]byte
	head, size    int
	total         int64
	sequence      uint64
	ended, closed bool
	trailers      []*pb.Header
	onEOF         func([]*pb.Header) error
	consumed      func(int) error
}

func newBody(ctx context.Context, consumed func(int) error) *body {
	return &body{ctx: ctx, wake: make(chan struct{}, 1), consumed: consumed}
}
func signal(wake chan struct{}) {
	select {
	case wake <- struct{}{}:
	default:
	}
}
func (b *body) push(d *pb.Data) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	if d == nil || b.ended || d.SequenceNo != b.sequence+1 || len(d.Data) > chunk || len(d.Data) > window-b.size || b.total+int64(len(d.Data)) > bodyLimit {
		return fail(pb.ErrorCode_ERROR_CODE_INVALID_REQUEST)
	}
	pos := (b.head + b.size) % window
	n := copy(b.data[pos:], d.Data)
	copy(b.data[:], d.Data[n:])
	b.size += len(d.Data)
	b.total += int64(len(d.Data))
	b.sequence = d.SequenceNo
	signal(b.wake)
	return nil
}
func (b *body) end(e *pb.End) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	if e == nil || b.ended || !e.Complete {
		return fail(pb.ErrorCode_ERROR_CODE_INVALID_REQUEST)
	}
	b.ended = true
	b.trailers = e.Trailers
	signal(b.wake)
	return nil
}
func (b *body) Read(p []byte) (int, error) {
	for {
		if err := b.ctx.Err(); err != nil {
			return 0, err
		}
		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			return 0, io.ErrClosedPipe
		}
		if b.size > 0 {
			n := min(len(p), b.size, window-b.head)
			copy(p, b.data[b.head:b.head+n])
			b.head = (b.head + n) % window
			b.size -= n
			b.mu.Unlock()
			if err := b.consumed(n); err != nil {
				return n, err
			}
			return n, nil
		}
		ended, trailers := b.ended, b.trailers
		b.mu.Unlock()
		if ended {
			if b.onEOF != nil {
				if err := b.onEOF(trailers); err != nil {
					return 0, err
				}
			}
			return 0, io.EOF
		}
		select {
		case <-b.ctx.Done():
			return 0, b.ctx.Err()
		case <-b.wake:
		}
	}
}
func (b *body) Close() error { b.mu.Lock(); b.closed = true; b.mu.Unlock(); signal(b.wake); return nil }

type credit struct {
	mu        sync.Mutex
	available int
	wake      chan struct{}
}

func newCredit() *credit { return &credit{available: window, wake: make(chan struct{}, 1)} }
func (c *credit) grant(n int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n <= 0 || n > window-c.available {
		return fail(pb.ErrorCode_ERROR_CODE_INVALID_REQUEST)
	}
	c.available += n
	signal(c.wake)
	return nil
}
func (c *credit) take(ctx context.Context, n int) error {
	if n < 0 || n > chunk {
		return fail(pb.ErrorCode_ERROR_CODE_INVALID_REQUEST)
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		c.mu.Lock()
		if c.available >= n {
			c.available -= n
			c.mu.Unlock()
			return nil
		}
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.wake:
		}
	}
}
