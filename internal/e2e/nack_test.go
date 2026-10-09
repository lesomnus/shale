package e2e_test

import (
	"context"
	"encoding/binary"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"
)

// TestLiveNack is a viewer on a lossy link (§39.4): one video packet in
// every 25 never arrives the first time it is sent, the keyframe's and the
// group of pictures' included. The viewer's NACKs, which pion sends as a
// browser does, name what it missed, the relay sends it again, and every
// frame from the first on comes whole. Between local sockets nothing is
// lost otherwise, so this is what shows the relay answers NACKs at all.
func TestLiveNack(t *testing.T) {
	c := start(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	admin := c.dial("@acme/admin")
	_, live := liveSource(t, ctx, c, admin, "av.mp4")
	// A keyframe has come, so the viewer gets a group of pictures.
	time.Sleep(2500 * time.Millisecond)

	drop := &lossy{every: 25}
	me := &webrtc.MediaEngine{}
	require.NoError(t, me.RegisterDefaultCodecs())
	ir := &interceptor.Registry{}
	// First, so the NACK generator the defaults add sees the loss.
	ir.Add(drop)
	require.NoError(t, webrtc.RegisterDefaultInterceptors(me, ir))
	pc, err := webrtc.NewAPI(webrtc.WithMediaEngine(me), webrtc.WithInterceptorRegistry(ir)).NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	defer pc.Close()

	var mu sync.Mutex
	var first time.Time
	var frames, broken, lost int64
	onVideo(pc, func(p *rtp.Packet, at time.Time, asm *assembler) {
		asm.push(p, at, func(u accessUnit) {
			mu.Lock()
			defer mu.Unlock()
			if first.IsZero() {
				if u.decodable() {
					first = u.at
				}
				return
			}
			frames++
			lost += u.lost
			if !u.whole {
				broken++
			}
		})
	})
	_, _, err = offerWhep(pc, live)
	require.NoError(t, err)
	time.Sleep(8 * time.Second)

	mu.Lock()
	defer mu.Unlock()
	t.Logf("dropped on arrival %d video packets; then %d frames after the first, %d not whole, %d packets lost for good",
		drop.dropped.Load(), frames, broken, lost)
	require.False(t, first.IsZero(), "the picture came up")
	require.Greater(t, frames, int64(150), "and went on")
	require.Greater(t, drop.dropped.Load(), int64(20), "packets were lost")
	require.Zero(t, broken, "and sent again: every frame after the first came whole")
}

// lossy is an interceptor that drops every `every`th packet of each video
// stream the first time it comes, before the interceptors after it see
// it; a packet sent again, on the RTX stream or the same one, gets
// through.
type lossy struct {
	interceptor.NoOp
	every   uint16
	dropped atomic.Int64
}

func (l *lossy) NewInterceptor(string) (interceptor.Interceptor, error) { return l, nil }

func (l *lossy) BindRemoteStream(info *interceptor.StreamInfo, reader interceptor.RTPReader) interceptor.RTPReader {
	if !strings.HasPrefix(info.MimeType, "video/") || strings.EqualFold(info.MimeType, webrtc.MimeTypeRTX) {
		return reader
	}
	var mu sync.Mutex
	seen := map[uint16]bool{}

	return interceptor.RTPReaderFunc(func(b []byte, a interceptor.Attributes) (int, interceptor.Attributes, error) {
		for {
			n, attr, err := reader.Read(b, a)
			if err != nil || n < 4 {
				return n, attr, err
			}
			seq := binary.BigEndian.Uint16(b[2:4])
			mu.Lock()
			again := seen[seq]
			seen[seq] = true
			mu.Unlock()
			if again || seq%l.every != 0 {
				return n, attr, nil
			}
			l.dropped.Add(1)
		}
	})
}
