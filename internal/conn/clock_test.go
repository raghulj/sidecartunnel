package conn

import (
	"testing"
	"time"
)

// failAfter is the generous timeout every test in this package waits on instead of
// sleeping. docs/14-coding-standards.md §2: the happy path takes microseconds, so the
// deadline only fires when the test is about to fail anyway, and a hung test with a clear
// message beats a hung test.
const failAfter = 5 * time.Second

func TestSystemClock_NowAdvances(t *testing.T) {
	clk := SystemClock()
	before := time.Now()
	got := clk.Now()
	after := time.Now()
	if got.Before(before) || got.After(after) {
		t.Fatalf("Now() = %v, want within [%v, %v]", got, before, after)
	}
}

func TestSystemClock_TimerFires(t *testing.T) {
	clk := SystemClock()
	timer := clk.NewTimer(time.Millisecond)
	defer timer.Stop()

	select {
	case <-timer.C():
	case <-time.After(failAfter):
		t.Fatal("timer did not fire within the deadline")
	}
}

func TestSystemClock_TimerStopSilencesIt(t *testing.T) {
	clk := SystemClock()
	timer := clk.NewTimer(time.Hour)
	timer.Stop()

	select {
	case <-timer.C():
		t.Fatal("a stopped timer fired")
	default:
	}
}

func TestSystemClock_TickerFiresRepeatedly(t *testing.T) {
	clk := SystemClock()
	ticker := clk.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for i := range 2 {
		select {
		case <-ticker.C():
		case <-time.After(failAfter):
			t.Fatalf("tick %d did not arrive within the deadline", i)
		}
	}
}

func TestSystemClock_TickerStop(t *testing.T) {
	clk := SystemClock()
	ticker := clk.NewTicker(time.Hour)
	ticker.Stop()

	select {
	case <-ticker.C():
		t.Fatal("a stopped ticker fired")
	default:
	}
}
