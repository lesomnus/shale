package e2e_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/fmp4"
	"github.com/lesomnus/shale/internal/producer"
)

// TestLiveWall is E6's first line: a browser watches a set of eight
// cameras through one relay, and each picture comes up within a second.
// A browser does what this viewer does: one `Live` for the set, then a
// WHEP session per camera, all at once (§39.4). What is timed is from the
// `Live` call to the last packet of the first frame that decodes on its
// own, an IDR with the parameter sets in front of it, received whole; the
// time from the WHEP offer alone is logged beside it. The cameras are on
// the default policy, `always`, so the relay holds each one's group of
// pictures before anyone asks (§39.3) and a joining viewer gets it at
// once instead of waiting for the next keyframe.
//
// The wall is opened three times over, each time anew. What follows the
// first frame is checked too: every frame after it that came whole is the
// recording's, as many frames on as the RTP clock says, never one already
// sent and never one from further back, so the picture that came up goes
// on, and every frame after the first comes whole: the group of pictures
// goes out faster than real time after the keyframe rather than at once,
// and a packet lost all the same is asked for again with a NACK, which the
// viewer's pion sends as a browser does and the relay answers (§39.4). How
// long each viewer took to reach the live edge is logged.
func TestLiveWall(t *testing.T) {
	const cameras = 8
	c := start(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	admin := c.dial("@acme/admin")
	sample, err := filepath.Abs(filepath.Join("..", "producer", "testdata", "av.mp4"))
	require.NoError(t, err)
	index := indexFrames(t, sample)
	srcs := make([]producer.SourceConfig, cameras)
	for i := range srcs {
		srcs[i] = producer.SourceConfig{Alias: fmt.Sprintf("cam-%d", i+1), Input: "raw:" + sample, Fps: 30, MaxBitrate: 2_000_000, Format: "h264"}
	}
	set, _, _ := liveProducer(t, ctx, c, admin, srcs...)
	relays := api.NewRelayServiceClient(c.dialCluster("@cluster/ops"))
	awaitActive(t, ctx, relays, cameras, "always: every camera of the set is on the relay")
	// A keyframe of each camera has come since it started, so the relay
	// has a group of pictures to hand over: one keyframe interval.
	time.Sleep(2500 * time.Millisecond)

	sets := api.NewSetServiceClient(admin)
	var fromLive, fromOffer, edges []time.Duration
	broken := 0
	for round := range 3 {
		asked := time.Now()
		resp, err := sets.Live(ctx, api.SetLiveRequest_builder{Ref: api.SetRef_builder{Id: set.GetId()}.Build()}.Build())
		require.NoError(t, err)
		require.Len(t, resp.GetSources(), cameras)
		relay := resp.GetSources()[0].GetRelayId()

		var wg sync.WaitGroup
		got := make([]wallTile, cameras)
		for i, live := range resp.GetSources() {
			require.Equal(t, relay, live.GetRelayId(), "a set is on one relay")
			wg.Add(1)
			go func() {
				defer wg.Done()
				got[i] = watchTile(t, live, index, 3*time.Second)
			}()
		}
		wg.Wait()
		for i, g := range got {
			require.NoError(t, g.err, "camera %d", i+1)
			fromLive = append(fromLive, g.first.Sub(asked))
			fromOffer = append(fromOffer, g.first.Sub(g.offered))
			broken += g.broken
			edges = append(edges, g.edge)
			t.Logf("round %d camera %d: first frame %4d ms after Live, %4d ms after the offer, live edge %4d ms after that; then %d frames whole and in order, %d not whole (%d packets lost)",
				round+1, i+1, g.first.Sub(asked).Milliseconds(), g.first.Sub(g.offered).Milliseconds(), g.edge.Milliseconds(), g.after, g.broken, g.lost)
		}
	}
	t.Logf("first frame after Live, %d sessions: %s", len(fromLive), spread(fromLive))
	t.Logf("first frame after the offer, %d sessions: %s", len(fromOffer), spread(fromOffer))
	t.Logf("first frame to the live edge, %d sessions: %s", len(edges), spread(edges))
	t.Logf("frames after the first that did not come whole, all sessions: %d", broken)
	for _, d := range fromLive {
		require.Less(t, d, time.Second, "every camera of the wall comes up within a second of Live")
	}
	require.Zero(t, broken, "every frame after the first comes whole: the group of pictures paced, and what was lost sent again")
}

// spread is the least, the median and the most of some durations.
func spread(ds []time.Duration) string {
	s := slices.Clone(ds)
	slices.Sort(s)

	return fmt.Sprintf("min %d ms, median %d ms, max %d ms", s[0].Milliseconds(), s[len(s)/2].Milliseconds(), s[len(s)-1].Milliseconds())
}

// wallTile is what one viewer of the wall saw: when it offered, when its
// first decodable frame was in, how many frames after it came whole and
// in the recording's order, and how many did not come whole for packets
// that never arrived, and how many packets those were.
type wallTile struct {
	offered time.Time
	first   time.Time
	after   int
	broken  int
	lost    int64
	// edge is how long after the first frame the viewer was at the live
	// edge (liveEdge).
	edge time.Duration
	err  error
}

// watchTile opens a WHEP session for one camera and watches it until the
// first frame that decodes on its own is in, then for `then` more,
// checking the frames run on as the recording has them: each whole frame
// is the one after the last, or after those that did not come whole,
// never one already shown and never one from further back. It reports
// rather than fails, since it runs beside the other tiles.
func watchTile(t *testing.T, live *api.LiveSource, index frameIndex, then time.Duration) (out wallTile) {
	units := make(chan accessUnit, 1024)
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		return wallTile{err: err}
	}
	defer pc.Close()
	onVideo(pc, func(p *rtp.Packet, at time.Time, asm *assembler) {
		asm.push(p, at, func(u accessUnit) {
			select {
			case units <- u:
			default:
			}
		})
	})
	out.offered = time.Now()
	if _, _, err := offerWhep(pc, live); err != nil {
		return wallTile{err: err}
	}

	deadline := time.After(10 * time.Second)
	var prev []int
	var prevTs uint32
	skipped := 0
	var end <-chan time.Time
	var frames []arrival
	defer func() { out.edge = liveEdge(frames) }()
	for {
		select {
		case u := <-units:
			if out.first.IsZero() {
				if !u.decodable() {
					continue
				}
				out.first = u.at
				end = time.After(then)
			}
			frames = append(frames, arrival{u.at, u.ts})
			if len(frames) > 1 {
				out.lost += u.lost
				if !u.whole {
					out.broken++
					skipped++
					continue
				}
			}
			at, ok := index.at[u.hash()]
			if !ok {
				out.err = fmt.Errorf("frame %d after the first is none of the recording's (NAL types %v)", out.after, u.types())
				return out
			}
			// How far on it should be: by the RTP clock, which the relay
			// runs on by the frames' durations, so frames none of whose
			// packets came are counted too.
			step := int((u.ts - prevTs + index.ticks/2) / index.ticks)
			if prev != nil && !follows(prev, at, index.n, step) {
				out.err = fmt.Errorf("frame %d after the first is the recording's %v, %d frames by the clock after %v and %d not whole: out of order or twice", out.after, at, step, prev, skipped)
				return out
			}
			prev, prevTs, skipped = at, u.ts, 0
			out.after++
		case <-end:
			if out.after < 30 {
				out.err = fmt.Errorf("only %d frames after the first", out.after)
			}
			return out
		case <-deadline:
			if out.first.IsZero() {
				out.err = fmt.Errorf("no decodable frame in 10 s")
			}
			return out
		}
	}
}

// liveEdge is how long after the first of some frames, each with when it
// came and its RTP timestamp, a viewer was at the live edge: from the
// frame on that came as far ahead of the first by its timestamp as the
// frames from then on settled at, within 50 ms. A viewer handed the group
// of pictures gets frames faster than real time until it has caught up,
// and from then at the camera's pace.
func liveEdge(frames []arrival) time.Duration {
	if len(frames) < 4 {
		return 0
	}
	lead := func(f arrival) time.Duration {
		return time.Duration(float64(f.ts-frames[0].ts)/90000*float64(time.Second)) - f.at.Sub(frames[0].at)
	}
	var tail []time.Duration
	for _, f := range frames[len(frames)/2:] {
		tail = append(tail, lead(f))
	}
	slices.Sort(tail)
	settled := tail[len(tail)/2]
	for _, f := range frames {
		if lead(f) >= settled-50*time.Millisecond {
			return f.at.Sub(frames[0].at)
		}
	}

	return 0
}

// follows says a frame at one of `next` comes `step` frames after one at
// one of `prev` in a recording of n frames that loops, give or take one
// for the rounding of the clock, and never at or before it.
func follows(prev, next []int, n, step int) bool {
	for _, a := range prev {
		for _, b := range next {
			if d := ((b-a)%n + n) % n; d >= 1 && d >= step-1 && d <= step+1 {
				return true
			}
		}
	}

	return false
}

// onVideo hands every video RTP packet a peer connection receives, with
// when it came, to `f`, with an assembler of its own per track; audio is
// read and dropped. The connection offers to receive video and audio, as
// a browser does.
func onVideo(pc *webrtc.PeerConnection, f func(p *rtp.Packet, at time.Time, asm *assembler)) {
	pc.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly})
	pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly})
	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		video := track.Kind() == webrtc.RTPCodecTypeVideo
		asm := &assembler{}
		jb := &jitter{wait: jitterWait}
		for {
			p, _, err := track.ReadRTP()
			if err != nil {
				return
			}
			if video {
				jb.push(p, time.Now(), func(p *rtp.Packet, at time.Time) { f(p, at, asm) })
			}
		}
	})
}

// jitterWait is how long a packet that came out of order waits for those
// before it. Packets between local sockets come out of order now and
// then, by tens of milliseconds, when the kernel delivers a burst from two
// CPUs' queues; a browser's jitter buffer, which holds what it plays by
// about this much, puts them back in order the same way.
const jitterWait = 250 * time.Millisecond

// jitter hands on RTP packets in the order of their sequence numbers: a
// packet ahead of the next one due waits, up to `wait`, and then those
// still missing are given up as lost; one that comes after it was given
// up, or twice, is dropped.
type jitter struct {
	wait    time.Duration
	started bool
	next    uint16
	held    map[uint16]heldPacket
}

type heldPacket struct {
	p  *rtp.Packet
	at time.Time
}

func (j *jitter) push(p *rtp.Packet, at time.Time, out func(*rtp.Packet, time.Time)) {
	if !j.started {
		j.started, j.next, j.held = true, p.SequenceNumber, map[uint16]heldPacket{}
	}
	if d := int16(p.SequenceNumber - j.next); d < 0 {
		return
	}
	j.held[p.SequenceNumber] = heldPacket{p, at}
	for {
		for {
			h, ok := j.held[j.next]
			if !ok {
				break
			}
			delete(j.held, j.next)
			out(h.p, at)
			j.next++
		}
		if len(j.held) == 0 {
			return
		}
		// Something is missing: give it up once the oldest held packet
		// has waited long enough, and go on from the first one held.
		var first uint16
		var since time.Time
		found := false
		for seq, h := range j.held {
			if !found || int16(seq-first) < 0 {
				first, found = seq, true
			}
			if since.IsZero() || h.at.Before(since) {
				since = h.at
			}
		}
		if at.Sub(since) < j.wait {
			return
		}
		j.next = first
	}
}

// accessUnit is one video frame as a viewer reassembles it from RTP: its
// NAL units, when its last packet came, and whether every packet of it
// did.
type accessUnit struct {
	ts    uint32
	at    time.Time
	nals  [][]byte
	whole bool
	// marked says the packet that ends it came: the RTP marker bit.
	marked bool
	// lost is how many packets were missing just before or within it.
	lost int64
}

// decodable says the frame decodes on its own: received whole, an IDR
// slice with both parameter sets in front of it.
func (u accessUnit) decodable() bool {
	ts := u.types()

	return u.whole && slices.Contains(ts, 5) && slices.Contains(ts, 7) && slices.Contains(ts, 8)
}

func (u accessUnit) types() []byte {
	var out []byte
	for _, n := range u.nals {
		out = append(out, n[0]&0x1f)
	}

	return out
}

// hash identifies the frame by its slices: the NAL units other than the
// parameter sets, which the producer and the relay put in front of a
// keyframe (§39.3), and those the RTP packetizer drops (AUD, filler).
func (u accessUnit) hash() uint64 { return hashNALs(u.nals) }

func hashNALs(nals [][]byte) uint64 {
	h := fnv.New64a()
	var l [4]byte
	for _, n := range nals {
		switch n[0] & 0x1f {
		case 7, 8, 9, 12:
			continue
		}
		binary.BigEndian.PutUint32(l[:], uint32(len(n)))
		h.Write(l[:])
		h.Write(n)
	}

	return h.Sum64()
}

// assembler puts H.264 RTP packets back into access units (RFC 6184):
// single NAL units, STAP-A and FU-A, a frame ending at a new timestamp. A
// lost packet, a gap in the sequence numbers, leaves the frames it may
// have belonged to not whole.
type assembler struct {
	started bool
	seq     uint16
	cur     accessUnit
	frag    []byte
	// Packets and bytes taken, and packets that never came.
	packets, bytes, lost int64
}

func (a *assembler) push(p *rtp.Packet, at time.Time, out func(accessUnit)) {
	gap := a.started && p.SequenceNumber != a.seq+1
	var missing int64
	if gap {
		missing = int64(uint16(p.SequenceNumber - a.seq - 1))
		a.lost += missing
	}
	a.packets++
	a.bytes += int64(len(p.Payload))
	if !a.started || p.Timestamp != a.cur.ts {
		if a.started && len(a.cur.nals) > 0 {
			a.cur.whole = a.cur.whole && a.cur.marked
			out(a.cur)
		}
		a.cur = accessUnit{ts: p.Timestamp, whole: !gap, lost: missing}
		a.frag = nil
	} else if gap {
		a.cur.whole = false
		a.cur.lost += missing
		a.frag = nil
	}
	a.started, a.seq = true, p.SequenceNumber
	a.cur.at = at
	a.cur.marked = p.Marker

	pl := p.Payload
	if len(pl) == 0 {
		return
	}
	switch typ := pl[0] & 0x1f; {
	case typ >= 1 && typ <= 23:
		a.cur.nals = append(a.cur.nals, bytes.Clone(pl))
	case typ == 24: // STAP-A: sizes and NAL units
		for off := 1; off+2 <= len(pl); {
			n := int(binary.BigEndian.Uint16(pl[off:]))
			if off+2+n > len(pl) {
				a.cur.whole = false
				break
			}
			a.cur.nals = append(a.cur.nals, bytes.Clone(pl[off+2:off+2+n]))
			off += 2 + n
		}
	case typ == 28: // FU-A: a NAL unit in pieces
		if len(pl) < 2 {
			a.cur.whole = false
			return
		}
		if pl[1]&0x80 != 0 {
			a.frag = append([]byte{pl[0]&0xe0 | pl[1]&0x1f}, pl[2:]...)
		} else if a.frag != nil {
			a.frag = append(a.frag, pl[2:]...)
		} else {
			a.cur.whole = false
		}
		if pl[1]&0x40 != 0 && a.frag != nil {
			a.cur.nals = append(a.cur.nals, a.frag)
			a.frag = nil
		}
	}
}

// frameIndex is where each video frame of a recording is in it, by the
// hash of what a viewer reassembles of it; a frame that recurs is at more
// than one place. n is how many frames the recording has.
type frameIndex struct {
	at map[uint64][]int
	n  int
	// ticks is a frame's duration on the RTP clock, 90 kHz.
	ticks uint32
	// bytes is what the frames weigh together.
	bytes int64
}

// bitrate is the recording's video in bits per second.
func (x frameIndex) bitrate() float64 {
	return float64(x.bytes*8) / (float64(x.n) * float64(x.ticks) / 90000)
}

// indexFrames reads a fragmented MP4 and indexes its video frames.
func indexFrames(t *testing.T, path string) frameIndex {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	r := fmp4.NewReader(bytes.NewReader(b))
	out := frameIndex{at: map[uint64][]int{}}
	for {
		u, err := r.Next()
		if err != nil {
			break
		}
		if u.Frag == nil {
			continue
		}
		v := r.Init().Video()
		tr := u.Frag.Video(r.Init())
		if tr == nil {
			continue
		}
		size := v.LengthSize()
		for _, sm := range tr.Samples {
			if out.ticks == 0 && sm.Duration > 0 && v.Timescale > 0 {
				out.ticks = uint32(uint64(sm.Duration) * 90000 / uint64(v.Timescale))
			}
			raw := u.Frag.Bytes[sm.Off : sm.Off+sm.Size]
			out.bytes += int64(len(raw))
			var nals [][]byte
			for off := 0; off+size <= len(raw); {
				n := 0
				for _, c := range raw[off : off+size] {
					n = n<<8 | int(c)
				}
				off += size
				if n == 0 || off+n > len(raw) {
					break
				}
				nals = append(nals, raw[off:off+n])
				off += n
			}
			h := hashNALs(nals)
			out.at[h] = append(out.at[h], out.n)
			out.n++
		}
	}
	require.Greater(t, out.n, 30, "the recording has frames")
	require.NotZero(t, out.ticks, "and says how long they last")

	return out
}
