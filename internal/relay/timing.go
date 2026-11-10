package relay

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/metric"
)

// Timed frames (§39.4). A viewer that asks for them gets, inside each video
// access unit, an SEI message (user data unregistered) saying which frame it
// is and when it passed each point on its way: t1 and t2 on the producer's
// clock, t3 and t4 on the relay's. A decoder skips it. The viewer reads it
// from the encoded frame, notes when the frame arrived (t5) and when it was
// shown (t6), and reports the three spans only it can see back to the
// session (POST /whep/{session}/timing), whose answer also gives the relay's
// clock for the viewer to measure its own against.

// timingUUID marks Shale's timing SEI among others an encoder may write.
var timingUUID = [16]byte{0x8f, 0x0a, 0x5b, 0x63, 0x4e, 0x1c, 0x4d, 0x2f, 0x9a, 0x6e, 0x53, 0x68, 0x61, 0x6c, 0x65, 0x54}

// timingVersion is the payload's layout: the version, then six big-endian
// int64s: the frame's decode time on the stream's clock in nanoseconds,
// which names it, and t1 to t4 in Unix nanoseconds, zero where unknown.
const (
	timingVersion = 1
	timingSize    = 1 + 5*8
)

// frameTiming is what a timing SEI says.
type frameTiming struct {
	media               time.Duration
	read, sent, arrived time.Time
	relayed             time.Time
}

func unixNano(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func fromUnixNano(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}

// timingSEI is the SEI NAL unit, with its start code, for a frame sent at
// t4 = relayed.
func timingSEI(h265 bool, ft frameTiming) []byte {
	payload := make([]byte, 0, len(timingUUID)+timingSize)
	payload = append(payload, timingUUID[:]...)
	payload = append(payload, timingVersion)
	for _, v := range []int64{int64(ft.media), unixNano(ft.read), unixNano(ft.sent), unixNano(ft.arrived), unixNano(ft.relayed)} {
		payload = binary.BigEndian.AppendUint64(payload, uint64(v))
	}
	// One SEI message: user_data_unregistered (5), its size, the payload,
	// and the RBSP's stop bit.
	rbsp := []byte{5}
	for n := len(payload); ; n -= 255 {
		if n < 255 {
			rbsp = append(rbsp, byte(n))
			break
		}
		rbsp = append(rbsp, 0xff)
	}
	rbsp = append(rbsp, payload...)
	rbsp = append(rbsp, 0x80)

	nal := []byte{0, 0, 0, 1}
	if h265 {
		nal = append(nal, 39<<1, 1) // PREFIX_SEI_NUT, layer 0, temporal id 0
	} else {
		nal = append(nal, 6) // SEI
	}
	return append(nal, escape(rbsp)...)
}

// escape inserts the emulation prevention bytes a NAL unit's payload needs
// so that no start code appears inside it.
func escape(rbsp []byte) []byte {
	out := make([]byte, 0, len(rbsp)+4)
	zeros := 0
	for _, b := range rbsp {
		if zeros >= 2 && b <= 3 {
			out = append(out, 3)
			zeros = 0
		}
		out = append(out, b)
		if b == 0 {
			zeros++
		} else {
			zeros = 0
		}
	}
	return out
}

// unescape takes them out again.
func unescape(b []byte) []byte {
	out := make([]byte, 0, len(b))
	zeros := 0
	for _, c := range b {
		if zeros >= 2 && c == 3 {
			zeros = 0
			continue
		}
		out = append(out, c)
		if c == 0 {
			zeros++
		} else {
			zeros = 0
		}
	}
	return out
}

// nalUnits are the offsets of an Annex B access unit's start codes and of
// the NAL units after them.
func nalUnits(au []byte) (starts, units []int) {
	for i := 0; i+2 < len(au); {
		if au[i] == 0 && au[i+1] == 0 && au[i+2] == 1 {
			start := i
			if i > 0 && au[i-1] == 0 {
				start = i - 1
			}
			starts = append(starts, start)
			units = append(units, i+3)
			i += 3
			continue
		}
		i++
	}
	return starts, units
}

// vcl says whether a NAL unit is coded picture data.
func vcl(h265 bool, header byte) bool {
	if h265 {
		return (header>>1)&0x3f < 32
	}
	t := header & 0x1f
	return t >= 1 && t <= 5
}

// withSEI puts an SEI NAL unit before an access unit's first slice: after
// any delimiter and parameter sets, where an SEI belongs.
func withSEI(au, sei []byte, h265 bool) []byte {
	at := 0
	starts, units := nalUnits(au)
	for i, u := range units {
		if u < len(au) && vcl(h265, au[u]) {
			at = starts[i]
			break
		}
	}
	out := make([]byte, 0, len(au)+len(sei))
	out = append(out, au[:at]...)
	out = append(out, sei...)
	return append(out, au[at:]...)
}

// readTiming finds a timing SEI in an access unit, as a viewer would.
func readTiming(h265 bool, au []byte) (frameTiming, bool) {
	starts, units := nalUnits(au)
	for i, u := range units {
		end := len(au)
		if i+1 < len(units) {
			end = starts[i+1]
		}
		if u >= end {
			continue
		}
		header := 1
		if h265 {
			header = 2
			if (au[u]>>1)&0x3f != 39 {
				continue
			}
		} else if au[u]&0x1f != 6 {
			continue
		}
		rbsp := unescape(bytes.TrimRight(au[u+header:end], "\x00"))
		if len(rbsp) < 2 || rbsp[0] != 5 {
			continue
		}
		size, p := 0, 1
		for p < len(rbsp) && rbsp[p] == 0xff {
			size += 255
			p++
		}
		if p >= len(rbsp) {
			continue
		}
		size += int(rbsp[p])
		p++
		body := rbsp[p:]
		if size != len(timingUUID)+timingSize || len(body) < size || !bytes.Equal(body[:16], timingUUID[:]) || body[16] != timingVersion {
			continue
		}
		v := func(k int) int64 { return int64(binary.BigEndian.Uint64(body[17+8*k:])) }
		return frameTiming{media: time.Duration(v(0)), read: fromUnixNano(v(1)), sent: fromUnixNano(v(2)), arrived: fromUnixNano(v(3)), relayed: fromUnixNano(v(4))}, true
	}
	return frameTiming{}, false
}

// timingReport is what a viewer sends back: the spans of frames it saw,
// in milliseconds, each measured as it can be. Network is t5 - t4 and
// total t6 - t1, both read across clocks with the offset the viewer
// measured against the relay; playout is t6 - t5, on the viewer's own.
type timingReport struct {
	Frames []struct {
		Network *float64 `json:"network"`
		Playout *float64 `json:"playout"`
		Total   *float64 `json:"total"`
	} `json:"frames"`
}

// The most a report may carry: a minute of frames at 60 fps, and a span
// past a minute is not live.
const (
	reportFrames = 3600
	reportSpan   = 60_000.0
)

// timing takes a viewer's report on its session (§39.4) and answers with
// when the relay received it and when it replied, on its own clock in Unix
// milliseconds, for the viewer's next measure of the offset between the
// clocks.
func (w *whepServer) timing(rw http.ResponseWriter, req *http.Request, key string) {
	received := time.Now()
	rw.Header().Set("Access-Control-Allow-Origin", "*")
	rw.Header().Set("Cache-Control", "no-store")
	if _, _, ok := w.session(rw, req, key); !ok {
		return
	}
	var report timingReport
	if err := json.NewDecoder(io.LimitReader(req.Body, 1<<20)).Decode(&report); err != nil && err != io.EOF {
		http.Error(rw, "a timing report is JSON", http.StatusBadRequest)
		return
	}
	if len(report.Frames) > reportFrames {
		http.Error(rw, "too many frames in one report", http.StatusRequestEntityTooLarge)
		return
	}
	ctx := context.Background()
	record := func(h metric.Float64Histogram, v *float64) {
		if v == nil || math.IsNaN(*v) || math.IsInf(*v, 0) || *v > reportSpan || *v < -reportSpan {
			return
		}
		// A span read across clocks may come out a little below zero, by
		// the error of the offset: none of it was spent.
		h.Record(ctx, max(0, *v))
	}
	for _, f := range report.Frames {
		record(w.r.m.viewerNetwork, f.Network)
		record(w.r.m.viewerPlayout, f.Playout)
		record(w.r.m.viewerTotal, f.Total)
	}
	rw.Header().Set("Content-Type", "application/json")
	json.NewEncoder(rw).Encode(map[string]float64{
		"received": float64(received.UnixMicro()) / 1000,
		"replied":  float64(time.Now().UnixMicro()) / 1000,
	})
}
