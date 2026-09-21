package producer

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"
)

// A raw source's stream is a sequence of frames (§38.9): TS with the video
// taken out. A prefix frame is what PAT and PMT are to TS, kept and
// prepended to every lamina that starts after it; a data frame is a place
// the lamina may be cut before, as a keyframe is; the timestamp is the PCR,
// the data time the writer knows and the producer does not.
//
//	frame = kind (1) | timestamp (8, unix ns, big-endian, 0 = none) | length (4, big-endian) | bytes
const FrameHeader = 1 + 8 + 4

// FrameKind is what a frame is to the cutter.
type FrameKind byte

const (
	// FrameData is bytes of the stream; a lamina may be cut before it.
	FrameData FrameKind = 0
	// FramePrefix is kept and prepended to every lamina that starts after
	// it, replacing the prefix before it.
	FramePrefix FrameKind = 1
)

// MaxFrame is the largest frame accepted; a length above it is a stream
// that lost its framing, not a frame.
const MaxFrame = 64 << 20

// Frame is one frame of a raw stream.
type Frame struct {
	Kind FrameKind
	// Time is the writer's timestamp, zero when it gave none.
	Time    time.Time
	Payload []byte
}

var (
	errFrameKind = errors.New("frame: unknown kind")
	errFrameSize = errors.New("frame: length above the maximum")
)

// FrameReader reads frames from a stream.
type FrameReader struct {
	r *bufio.Reader
}

// NewFrameReader reads frames from r.
func NewFrameReader(r io.Reader) *FrameReader {
	return &FrameReader{r: bufio.NewReaderSize(r, 256<<10)}
}

// Next reads one frame into f, reusing its payload buffer. It answers
// io.EOF at a clean end and io.ErrUnexpectedEOF for a torn frame.
func (r *FrameReader) Next(f *Frame) error {
	var h [FrameHeader]byte
	if _, err := io.ReadFull(r.r, h[:]); err != nil {
		return err
	}
	kind := FrameKind(h[0])
	if kind != FrameData && kind != FramePrefix {
		return fmt.Errorf("%w: %d", errFrameKind, h[0])
	}
	ns := int64(binary.BigEndian.Uint64(h[1:9]))
	n := binary.BigEndian.Uint32(h[9:13])
	if n > MaxFrame {
		return fmt.Errorf("%w: %d", errFrameSize, n)
	}
	if cap(f.Payload) < int(n) {
		f.Payload = make([]byte, n)
	}
	f.Payload = f.Payload[:n]
	if _, err := io.ReadFull(r.r, f.Payload); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}

		return err
	}
	f.Kind = kind
	f.Time = time.Time{}
	if ns != 0 {
		f.Time = time.Unix(0, ns).UTC()
	}

	return nil
}

// WriteFrame writes one frame.
func WriteFrame(w io.Writer, f *Frame) error {
	if len(f.Payload) > MaxFrame {
		return errFrameSize
	}
	var h [FrameHeader]byte
	h[0] = byte(f.Kind)
	if !f.Time.IsZero() {
		binary.BigEndian.PutUint64(h[1:9], uint64(f.Time.UnixNano()))
	}
	binary.BigEndian.PutUint32(h[9:13], uint32(len(f.Payload)))
	if _, err := w.Write(h[:]); err != nil {
		return err
	}
	_, err := w.Write(f.Payload)

	return err
}
