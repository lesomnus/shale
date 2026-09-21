package producer

import (
	"testing"
	"time"
)

const blackframeLine = "[Parsed_blackframe_2 @ 0xaaaa] frame:12 pblack:100 pts:12 t:12.000000 type:I last_keyframe:12"

func TestDarkTracker(t *testing.T) {
	now := time.Date(2026, 9, 21, 22, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	var changes []bool
	d := newDarkTracker(10*time.Second, clock)
	d.onChange = func(v bool) { changes = append(changes, v) }

	if d.observe("[swscaler @ 0x1] deprecated pixel format used") {
		t.Fatal("an ordinary line is not consumed")
	}
	// Dark lines once a second: suppressed once ten seconds have passed.
	for i := 0; i < 9; i++ {
		if !d.observe(blackframeLine) {
			t.Fatal("blackframe's line is consumed")
		}
		if d.Suppressed() {
			t.Fatalf("suppressed after %d s", i+1)
		}
		now = now.Add(time.Second)
		d.tick(now)
	}
	if !d.Dark() {
		t.Fatal("dark")
	}
	d.observe(blackframeLine)
	now = now.Add(time.Second)
	d.tick(now)
	if !d.Suppressed() {
		t.Fatal("suppressed after dark_after")
	}
	if len(changes) != 1 || !changes[0] {
		t.Fatalf("changes = %v", changes)
	}
	// Still dark: nothing changes.
	for i := 0; i < 5; i++ {
		d.observe(blackframeLine)
		now = now.Add(time.Second)
		d.tick(now)
	}
	if len(changes) != 1 {
		t.Fatalf("changes = %v", changes)
	}
	// The lines stop: lit once litAfter passes, and storing resumes. The
	// loop above left the clock a second past the last line.
	now = now.Add(time.Second)
	d.tick(now)
	if !d.Suppressed() {
		t.Fatal("two seconds of silence is not lit yet")
	}
	now = now.Add(time.Second)
	d.tick(now)
	if d.Suppressed() || d.Dark() {
		t.Fatal("lit after the lines stopped")
	}
	if len(changes) != 2 || changes[1] {
		t.Fatalf("changes = %v", changes)
	}
	// Dark again: another dark_after before suppression.
	for i := 0; i < 9; i++ {
		d.observe(blackframeLine)
		now = now.Add(time.Second)
		d.tick(now)
	}
	if d.Suppressed() {
		t.Fatal("a fresh dark spell starts the clock again")
	}
}

func TestIdleFilter(t *testing.T) {
	var c *IdleConfig
	if got := c.filter(); got != "split[v][m];[m]fps=1,blackframe=amount=98:threshold=26,nullsink;[v]null" {
		t.Fatalf("default filter = %q", got)
	}
	if got := (&IdleConfig{Threshold: 0.5}).filter(); got != "split[v][m];[m]fps=1,blackframe=amount=98:threshold=128,nullsink;[v]null" {
		t.Fatalf("filter = %q", got)
	}
	if got := (&IdleConfig{}).darkAfter(); got != DefaultDarkAfter {
		t.Fatalf("darkAfter = %v", got)
	}
	if !darkLine(blackframeLine) || darkLine("frame= 12 fps=30 pblack:1") {
		t.Fatal("darkLine")
	}
}
