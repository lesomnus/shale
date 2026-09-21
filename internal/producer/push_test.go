package producer

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFrameRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	at := time.Date(2026, 9, 21, 7, 0, 0, 123, time.UTC)
	in := []Frame{
		{Kind: FramePrefix, Payload: []byte("HDR")},
		{Kind: FrameData, Time: at, Payload: bytes.Repeat([]byte{7}, 1000)},
		{Kind: FrameData, Payload: nil},
	}
	for i := range in {
		require.NoError(t, WriteFrame(&buf, &in[i]))
	}
	r := NewFrameReader(bytes.NewReader(buf.Bytes()))
	var f Frame
	for i := range in {
		require.NoError(t, r.Next(&f), i)
		require.Equal(t, in[i].Kind, f.Kind)
		require.True(t, in[i].Time.Equal(f.Time), "time of %d", i)
		require.Equal(t, len(in[i].Payload), len(f.Payload))
		require.Equal(t, in[i].Payload, append([]byte(nil), f.Payload...))
	}
	require.ErrorIs(t, r.Next(&f), io.EOF)

	// A torn frame is not a clean end.
	torn := buf.Bytes()[:FrameHeader+3+FrameHeader+10]
	r = NewFrameReader(bytes.NewReader(torn))
	require.NoError(t, r.Next(&f))
	require.ErrorIs(t, r.Next(&f), io.ErrUnexpectedEOF)

	// A kind nobody defined is a stream that lost its framing.
	bad := append([]byte{9}, make([]byte, FrameHeader-1)...)
	require.ErrorIs(t, NewFrameReader(bytes.NewReader(bad)).Next(&f), errFrameKind)
}

// The cutter in raw mode (§38.9): laminae are cut at frame boundaries on
// the schedule, each starts with the last prefix frame, and their times
// are the frames' own.
func TestCutterFrames(t *testing.T) {
	t0 := time.Date(2026, 9, 21, 7, 0, 0, 0, time.UTC)
	var prefix []byte
	var out []*Segment
	c := &Cutter{
		Schedule: func() Schedule { return Schedule{Duration: time.Second, Ceiling: 1 << 30} },
		Now:      func() time.Time { return t0.Add(time.Hour) },
		Prefix:   func() []byte { return prefix },
		OnPrefix: func(b []byte) { prefix = b },
		Out:      func(s *Segment) { out = append(out, s) },
	}
	c.FeedFrame(&Frame{Kind: FramePrefix, Payload: []byte("HDR")})
	c.FeedFrame(&Frame{Kind: FrameData, Time: t0, Payload: []byte("aaaa")})
	c.FeedFrame(&Frame{Kind: FrameData, Time: t0.Add(500 * time.Millisecond), Payload: []byte("bbbb")})
	c.FeedFrame(&Frame{Kind: FrameData, Time: t0.Add(1200 * time.Millisecond), Payload: []byte("cccc")})
	c.FeedFrame(&Frame{Kind: FramePrefix, Payload: []byte("HDR2")})
	c.FeedFrame(&Frame{Kind: FrameData, Time: t0.Add(2100 * time.Millisecond), Payload: []byte("dddd")})
	c.Stop()

	require.Len(t, out, 3)
	require.Equal(t, "HDRaaaabbbb", string(out[0].Bytes()))
	require.True(t, out[0].Started.Equal(t0))
	require.True(t, out[0].Ended.Equal(t0.Add(1200*time.Millisecond)), "ends where the next lamina starts")
	require.Equal(t, "HDRcccc", string(out[1].Bytes()))
	require.Equal(t, "HDR2dddd", string(out[2].Bytes()), "the new prefix from the lamina after it")
	require.True(t, out[2].Stopped)
	require.True(t, out[2].Ended.Equal(t0.Add(2100*time.Millisecond)), "a stopped raw source ends at its last frame")
}

// streamsOf runs a push source and collects what each stream carried.
type streamsOf struct {
	mu      sync.Mutex
	streams [][]byte
	idles   int
	got     chan struct{}
}

func (c *streamsOf) run(ctx context.Context, s *pushSource) {
	s.Run(ctx, func(st *pushStream) {
		st.OnIdle = func() {
			c.mu.Lock()
			c.idles++
			c.mu.Unlock()
		}
		var b []byte
		buf := make([]byte, 1024)
		for {
			n, err := st.Read(buf)
			b = append(b, buf[:n]...)
			c.mu.Lock()
			select {
			case c.got <- struct{}{}:
			default:
			}
			c.mu.Unlock()
			if err != nil {
				break
			}
		}
		c.mu.Lock()
		c.streams = append(c.streams, b)
		c.mu.Unlock()
	})
}

func newPush(t *testing.T, idle time.Duration) (*Push, *pushSource, *streamsOf, context.CancelFunc) {
	t.Helper()
	p := &Push{Addr: "unix:" + filepath.Join(t.TempDir(), "push.sock"), Idle: idle}
	s := p.Add("a")
	require.NoError(t, p.Listen())
	ctx, cancel := context.WithCancel(context.Background())
	go p.Serve(ctx)
	c := &streamsOf{got: make(chan struct{}, 1)}
	go c.run(ctx, s)

	return p, s, c, cancel
}

// A stream goes in parts, each answered with the offset taken, resumes
// from HEAD, and completes; the next stream starts at zero (§38.9).
func TestPushStream(t *testing.T) {
	p, s, c, cancel := newPush(t, 5*time.Second)
	defer cancel()
	ctx := context.Background()
	w := &Pusher{Addr: p.Addr, Alias: "a", Part: 5}

	off, open, err := w.Offset(ctx)
	require.NoError(t, err)
	require.Zero(t, off)
	require.False(t, open)

	end, err := w.Push(ctx, bytes.NewReader([]byte("hello, world")), 0, true)
	require.NoError(t, err)
	require.EqualValues(t, 12, end)
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.streams) == 1
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, "hello, world", string(c.streams[0]))
	require.Zero(t, s.Offset(), "a completed stream leaves the offset at zero")

	// A stream left open: the offset is where it stopped, and a wrong
	// offset is refused with the right one.
	end, err = w.Push(ctx, bytes.NewReader([]byte("abcdefgh")), 0, false)
	require.NoError(t, err)
	require.EqualValues(t, 8, end)
	off, open, err = w.Offset(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 8, off)
	require.True(t, open)
	resp := putRaw(t, p, "a", 3, "?0", "xyz")
	require.Equal(t, http.StatusConflict, resp.StatusCode)
	require.Equal(t, "8", resp.Header.Get("Upload-Offset"))
	// Resuming from there completes it.
	end, err = w.Push(ctx, bytes.NewReader([]byte("ij")), 8, true)
	require.NoError(t, err)
	require.EqualValues(t, 10, end)
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.streams) == 2
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, "abcdefghij", string(c.streams[1]))

	// Something that is not a source.
	resp = putRaw(t, p, "b", 0, "?0", "x")
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// A silent stream closes its segment after the idle window and goes on
// with the next bytes on the same stream.
func TestPushIdle(t *testing.T) {
	p, _, c, cancel := newPush(t, 150*time.Millisecond)
	defer cancel()
	ctx := context.Background()
	w := &Pusher{Addr: p.Addr, Alias: "a"}
	_, err := w.Push(ctx, bytes.NewReader([]byte("first")), 0, false)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.idles == 1
	}, 5*time.Second, 10*time.Millisecond, "the idle window closes the segment once")
	_, err = w.Push(ctx, bytes.NewReader([]byte("second")), 5, true)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.streams) == 1
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, "firstsecond", string(c.streams[0]), "one stream across the idle gap")
}

func putRaw(t *testing.T, p *Push, alias string, off int64, complete, body string) *http.Response {
	t.Helper()
	_, addr, err := pushAddr(p.Addr)
	require.NoError(t, err)
	cl := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return net.Dial("unix", addr)
	}}}
	req, err := http.NewRequest(http.MethodPut, "http://producer/sources/"+alias+"/frames", bytes.NewReader([]byte(body)))
	require.NoError(t, err)
	req.Header.Set("Upload-Offset", itoa(off))
	req.Header.Set("Upload-Complete", complete)
	resp, err := cl.Do(req)
	require.NoError(t, err)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	return resp
}

func itoa(v int64) string { return strconv.FormatInt(v, 10) }
