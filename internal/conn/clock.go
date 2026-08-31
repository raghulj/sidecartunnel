package conn

import "time"

// Alarm is one scheduled notification obtained from a Clock: either a one-shot timer or a
// repeating ticker, depending on which constructor produced it.
//
// The two share an interface because every consumer in this package treats them the same
// way — it selects on C() and calls Stop() when it is done — and a second interface whose
// only difference is the return type of Stop would buy nothing.
//
// An Alarm is owned by the goroutine that created it. C() may be received on from any
// goroutine, but Stop must be called by the owner, exactly as with time.Timer.
type Alarm interface {
	// C returns the channel the alarm fires on. It never returns nil.
	C() <-chan time.Time

	// Stop releases the alarm. It is idempotent and never blocks. Stop does not drain
	// C, so a value already delivered stays readable — callers that care set their
	// local copy of the channel to nil instead of relying on Stop to silence it.
	Stop()
}

// Clock is this package's entire view of time.
//
// It exists as an interface for one reason: FR-7's ping timeout, FR-4's handshake timeout
// and FR-22's expiry are all deadline behaviour, and a test that drives them with real
// time is a test built on time.Sleep. docs/14-coding-standards.md §2 rules that out —
// a sleep-based concurrency test is a race waiting to be flaky on a loaded CI box. With
// this seam the tests advance a fake clock and synchronise on channels, so they are
// deterministic and take microseconds.
//
// Every implementation must be safe for concurrent use: a Conn's reader and writer
// goroutines both consult it.
type Clock interface {
	// Now returns the current time. It is used for socket write deadlines, so a fake
	// implementation's Now must move with its alarms or the deadlines it produces will
	// be meaningless.
	Now() time.Time

	// NewTimer returns an Alarm that fires once, d from now.
	NewTimer(d time.Duration) Alarm

	// NewTicker returns an Alarm that fires every d until it is stopped. d must be
	// positive, as with time.NewTicker.
	NewTicker(d time.Duration) Alarm
}

// SystemClock returns the Clock backed by the time package.
//
// It is a function rather than an exported variable because docs/14-coding-standards.md
// §7 forbids package-level state that anything could write to; the returned value is an
// empty struct, so calling it allocates nothing.
func SystemClock() Clock { return systemClock{} }

// systemClock is the real implementation of Clock.
type systemClock struct{}

// Now returns time.Now.
func (systemClock) Now() time.Time { return time.Now() }

// NewTimer returns a one-shot alarm backed by time.Timer.
func (systemClock) NewTimer(d time.Duration) Alarm { return systemTimer{t: time.NewTimer(d)} }

// NewTicker returns a repeating alarm backed by time.Ticker.
func (systemClock) NewTicker(d time.Duration) Alarm { return systemTicker{t: time.NewTicker(d)} }

// systemTimer adapts time.Timer to Alarm.
type systemTimer struct{ t *time.Timer }

// C returns the timer's channel.
func (a systemTimer) C() <-chan time.Time { return a.t.C }

// Stop stops the timer, discarding whether it had already fired: no caller in this
// package acts on that answer, and returning it would put a bool on Alarm that the
// ticker half cannot honestly implement.
func (a systemTimer) Stop() { a.t.Stop() }

// systemTicker adapts time.Ticker to Alarm.
type systemTicker struct{ t *time.Ticker }

// C returns the ticker's channel.
func (a systemTicker) C() <-chan time.Time { return a.t.C }

// Stop stops the ticker.
func (a systemTicker) Stop() { a.t.Stop() }
