package producer

import (
	"io"
	"time"

	"github.com/lesomnus/shale/internal/fmp4"
)

// A capture's stream is fragmented MP4 (§38.2): an init segment, then
// fragments. The reader turns it into the frames a raw source sends
// (§38.9), so the cutter, the uploader and the tee know one shape: the
// init segment is the prefix of every lamina, a fragment is a data frame,
// and a fragment whose video begins at a sync sample is one a lamina may
// be cut before. The container is known here and nowhere after.

// MP4Reader reads a fragmented MP4 stream as frames.
type MP4Reader struct {
	r    *fmp4.Reader
	init *fmp4.Init
}

// NewMP4Reader reads from r.
func NewMP4Reader(r io.Reader) *MP4Reader {
	return &MP4Reader{r: fmp4.NewReader(r)}
}

// Init is the last init segment seen, or nil.
func (r *MP4Reader) Init() *fmp4.Init { return r.init }

// Next reads the next frame into f. It answers io.EOF at a clean end and
// io.ErrUnexpectedEOF inside a box.
func (r *MP4Reader) Next(f *Frame) error {
	u, err := r.r.Next()
	if err != nil {
		return err
	}
	*f = Frame{}
	if u.Init != nil {
		r.init = u.Init
		f.Kind = FramePrefix
		f.Payload = u.Init.Bytes
		if v := u.Init.Video(); v != nil {
			f.Track = v.ID
		}

		return nil
	}
	f.Kind = FrameData
	f.Payload = u.Frag.Bytes
	f.Seq = u.Frag.Seq
	if v := r.init.Video(); v != nil {
		// A fragment with no video in it (the audio's tail) is data of
		// the same track, at which no lamina starts.
		f.Track = v.ID
		if tr := u.Frag.Traf(v.ID); tr != nil {
			f.Key = u.Frag.Key(r.init)
			f.Frames = len(tr.Samples)
			f.Ticks = tr.Time
			if v.Timescale > 0 {
				var longest uint32
				for _, s := range tr.Samples {
					longest = max(longest, s.Duration)
				}
				f.Longest = time.Duration(longest) * time.Second / time.Duration(v.Timescale)
			}
		}
	}

	return nil
}

// needsOpus says whether a stream's audio has to be transcoded for the
// relay (§38.7): there is audio, and none of it is Opus.
func needsOpus(init *fmp4.Init) bool {
	return init != nil && len(init.Audio()) > 0 && init.Opus() == nil
}
