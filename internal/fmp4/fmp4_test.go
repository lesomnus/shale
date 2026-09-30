package fmp4

import (
	"bytes"
	"errors"
	"io"
	"os"
	"testing"
)

// readAll hands a recording to a Reader and answers what came out.
func readAll(t *testing.T, b []byte) (*Init, []*Fragment) {
	t.Helper()
	r := NewReader(bytes.NewReader(b))
	var init *Init
	var frags []*Fragment
	for {
		u, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if u.Init != nil {
			init = u.Init
		} else {
			frags = append(frags, u.Frag)
		}
	}

	return init, frags
}

// av.mp4 is what a capture writes (§38.3): 6 s of 640x360 at 30 fps with
// a keyframe every 2 s, AAC and Opus beside it, fragments of 500 ms, and
// the mfra ffmpeg appends when it exits.
func TestReaderAV(t *testing.T) {
	b, err := os.ReadFile("testdata/av.mp4")
	if err != nil {
		t.Fatal(err)
	}
	init, frags := readAll(t, b)
	if init == nil {
		t.Fatal("no init segment")
	}
	if len(init.Tracks) != 3 {
		t.Fatalf("%d tracks", len(init.Tracks))
	}
	v := init.Video()
	if v == nil || v.Entry != "avc1" || v.Timescale != 15360 || len(v.Config) < 7 || v.Config[0] != 1 {
		t.Fatalf("video track %+v", v)
	}
	audio := init.Audio()
	if len(audio) != 2 || audio[0].Entry != "mp4a" || audio[1].Entry != "Opus" || audio[1].SampleRate != 48000 || audio[1].Channels != 1 {
		t.Fatalf("audio tracks %+v %+v", audio[0], audio[1])
	}
	if init.Opus() != audio[1] {
		t.Fatal("Opus is the second audio track")
	}
	if !bytes.HasPrefix(init.Bytes, []byte{0, 0, 0, 28, 'f', 't', 'y', 'p'}) {
		t.Fatalf("the init segment starts %q", init.Bytes[:8])
	}
	// 6 s in 500 ms fragments, numbered from 1, a keyframe every 2 s, and
	// the samples add up to the frames. The thirteenth is the audio's
	// tail, a fragment with no video in it.
	if len(frags) != 13 {
		t.Fatalf("%d fragments", len(frags))
	}
	if last := frags[12]; last.Video(init) != nil || last.Key(init) || len(last.Trafs) == 0 {
		t.Fatalf("the last fragment: %+v", last.Trafs)
	}
	frames, keys := 0, 0
	for i, f := range frags[:12] {
		if f.Seq != uint32(i+1) {
			t.Fatalf("fragment %d numbered %d", i, f.Seq)
		}
		vt := f.Video(init)
		if vt == nil || len(vt.Samples) != 15 {
			t.Fatalf("fragment %d: video %+v", i, vt)
		}
		if vt.Time != int64(i)*7680 {
			t.Fatalf("fragment %d at %d", i, vt.Time)
		}
		if vt.Duration() != 7680 {
			t.Fatalf("fragment %d lasts %d", i, vt.Duration())
		}
		frames += len(vt.Samples)
		if f.Key(init) {
			keys++
			if i%4 != 0 {
				t.Fatalf("fragment %d is a key", i)
			}
		}
		if len(f.Trafs) != 3 {
			t.Fatalf("fragment %d: %d trafs", i, len(f.Trafs))
		}
		// Every sample lies inside the mdat.
		for _, tr := range f.Trafs {
			for _, s := range tr.Samples {
				if s.Off < 0 || s.Off+s.Size > len(f.Bytes) || s.Size == 0 {
					t.Fatalf("fragment %d track %d: sample at %d+%d of %d", i, tr.TrackID, s.Off, s.Size, len(f.Bytes))
				}
			}
		}
		if !bytes.HasPrefix(f.Bytes[4:], []byte("moof")) {
			t.Fatalf("fragment %d starts %q", i, f.Bytes[4:8])
		}
	}
	if frames != 180 || keys != 3 {
		t.Fatalf("%d frames, %d keys", frames, keys)
	}
	// The first video sample of a key fragment is an I-frame that depends
	// on nothing, and that of any other is not a sync sample.
	if s := frags[0].Video(init).Samples[0]; !s.Sync() || s.Flags>>24&3 != 2 {
		t.Fatalf("first sample flags %08x", s.Flags)
	}
	if s := frags[1].Video(init).Samples[0]; s.Sync() {
		t.Fatalf("a P-frame marked sync: %08x", s.Flags)
	}
}

// A recording cut inside a fragment ends with io.ErrUnexpectedEOF; cut
// between boxes, with io.EOF.
func TestReaderTorn(t *testing.T) {
	b, err := os.ReadFile("testdata/sample.mp4")
	if err != nil {
		t.Fatal(err)
	}
	_, frags := readAll(t, b)
	if len(frags) != 30 {
		t.Fatalf("%d fragments", len(frags))
	}
	// Cut 100 bytes into the third fragment's mdat.
	start := bytes.Index(b, frags[2].Bytes)
	torn := b[:start+len(frags[2].Bytes)-100]
	r := NewReader(bytes.NewReader(torn))
	n := 0
	for {
		u, err := r.Next()
		if err != nil {
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("torn: %v after %d units", err, n)
			}
			break
		}
		if u.Frag != nil {
			n++
		}
	}
	if n != 2 {
		t.Fatalf("%d whole fragments before the tear", n)
	}
	clean := b[:start]
	r = NewReader(bytes.NewReader(clean))
	for {
		if _, err := r.Next(); err != nil {
			if !errors.Is(err, io.EOF) {
				t.Fatalf("clean: %v", err)
			}
			break
		}
	}
}

// The index a lamina ends with: written from the key fragments, found
// from the last 16 bytes, read back the same.
func TestMfra(t *testing.T) {
	keys := []KeyAt{{Time: 0, Offset: 1700}, {Time: 30720, Offset: 717121}, {Time: 61440, Offset: 1433000}}
	idx := Mfra(1, keys)
	if n := MfraSize(idx); n != len(idx) {
		t.Fatalf("mfro says %d, the box is %d", n, len(idx))
	}
	lamina := append(bytes.Repeat([]byte{0}, 4096), idx...)
	n := MfraSize(lamina[len(lamina)-16:])
	track, got, err := ParseMfra(lamina[len(lamina)-n:])
	if err != nil || track != 1 || len(got) != 3 {
		t.Fatalf("parse: %v track=%d keys=%v", err, track, got)
	}
	for i := range keys {
		if got[i] != keys[i] {
			t.Fatalf("entry %d: %v, want %v", i, got[i], keys[i])
		}
	}
	if MfraSize([]byte("not an index at all here")) != 0 {
		t.Fatal("a tail that is not an mfro says a size")
	}
	// ffmpeg's own mfra, appended when it exits, reads too.
	b, err := os.ReadFile("testdata/av.mp4")
	if err != nil {
		t.Fatal(err)
	}
	m := MfraSize(b[len(b)-16:])
	if m == 0 {
		t.Fatal("av.mp4 has no mfra at its end")
	}
	track, got, err = ParseMfra(b[len(b)-m:])
	if err != nil || track != 1 || len(got) < 3 {
		t.Fatalf("ffmpeg's mfra: %v track=%d keys=%v", err, track, got)
	}
	for i := 1; i < len(got); i++ {
		if got[i].Time <= got[i-1].Time || got[i].Offset <= got[i-1].Offset {
			t.Fatalf("ffmpeg's entries out of order at %d: %v", i, got)
		}
	}
}

// Fuzz the stream reader with whatever bytes: it must never panic.
func FuzzReader(f *testing.F) {
	b, _ := os.ReadFile("testdata/aac.mp4")
	f.Add(b)
	f.Add(b[:1000])
	f.Add([]byte("\x00\x00\x00\x08free"))
	f.Fuzz(func(t *testing.T, in []byte) {
		r := NewReader(bytes.NewReader(in))
		for i := 0; i < 1000; i++ {
			if _, err := r.Next(); err != nil {
				return
			}
		}
	})
}

// A keyframe is what the slice says, not what the flag says: the sample's
// NAL units decide, and only a sample with no slice falls back to the
// flag (#84).
func TestKeyByNAL(t *testing.T) {
	b, err := os.ReadFile("testdata/av.mp4")
	if err != nil {
		t.Fatal(err)
	}
	init, frags := readAll(t, b)
	v := init.Video()
	for i, f := range frags[:12] {
		s := f.Traf(v.ID).Samples[0]
		got := KeyByNAL(f.Bytes[s.Off:s.Off+s.Size], v.LengthSize(), false)
		if want := map[bool]int{true: 1, false: -1}[i%4 == 0]; got != want {
			t.Fatalf("fragment %d: KeyByNAL %d, want %d", i, got, want)
		}
		// A flag that lies is overruled.
		lied := s
		lied.Flags ^= 0x10000
		if v.SampleKey(f.Bytes[s.Off:s.Off+s.Size], lied) != (i%4 == 0) {
			t.Fatalf("fragment %d: the flag was believed over the slice", i)
		}
	}
	if KeyByNAL([]byte{0, 0, 0, 2, 0x06, 0}, 4, false) != 0 {
		t.Fatal("an SEI alone says nothing")
	}
	if KeyByNAL([]byte{0, 0, 0, 2, 0x26, 0}, 4, true) != 1 {
		t.Fatal("an H.265 IDR (type 19) is a key")
	}
}
