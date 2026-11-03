package deliver

import (
	"sync"
	"time"
)

type waiter struct {
	at time.Time
	ch chan struct{}
}

// ManualClock is a clock that tests advance by hand.
type ManualClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []waiter
}

// NewManualClock starts at the Unix epoch.
func NewManualClock() *ManualClock {
	return &ManualClock{now: time.Unix(0, 0).UTC()}
}

// Now returns the clock's current time.
func (c *ManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Until waits until Advance reaches the deadline.
// A deadline that is already due is reported immediately.
func (c *ManualClock) Until(deadline time.Time) <-chan struct{} {
	ch := make(chan struct{})
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.now.Before(deadline) {
		close(ch)
		return ch
	}
	c.waiters = append(c.waiters, waiter{at: deadline, ch: ch})
	return ch
}

// Advance moves the clock forward.
// Waiters whose deadline has been reached are released.
func (c *ManualClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	var keep []waiter
	var fire []chan struct{}
	for _, item := range c.waiters {
		if !c.now.Before(item.at) {
			fire = append(fire, item.ch)
		} else {
			keep = append(keep, item)
		}
	}
	c.waiters = keep
	c.mu.Unlock()
	for _, ch := range fire {
		close(ch)
	}
}
