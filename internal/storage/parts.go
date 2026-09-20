package storage

import (
	"context"
	"os"
	"sync"
	"syscall"
	"unsafe"
)

func unsafePointer(b []byte) unsafe.Pointer { return unsafe.Pointer(unsafe.SliceData(b)) }

// Staging in parts (§12.2, §22.4): incoming bytes fill a RAM part buffer
// that grows on demand up to part_size; a full buffer is one write at its
// offset, aligned to 4 KiB so the device can take it with O_DIRECT, and
// the object stays one contiguous extent. The end of a request flushes
// the aligned prefix; what does not fill a block is dropped and re-sent.
// A node-wide pool bounds what every upload holds at once: when it is
// spent, the sockets stop being read and TCP flow control slows the
// senders.

const (
	// Align is the alignment O_DIRECT needs: offsets, lengths, and buffers.
	Align = 4096
	// growStep is how much a part buffer grows at a time.
	growStep = 1 << 20
)

// pool is the node's part buffer budget.
type pool struct {
	mu     sync.Mutex
	cond   *sync.Cond
	budget int64
	used   int64
}

func newPool(budget int64) *pool {
	p := &pool{budget: budget}
	p.cond = sync.NewCond(&p.mu)

	return p
}

// acquire takes n bytes of the budget, waiting while it is spent; the
// context ends the wait.
func (p *pool) acquire(ctx context.Context, n int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.used+n > p.budget && p.used > 0 {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// A waiter wakes when something is released, or when the context
		// is done: a ticker of the latter costs nothing in the common case.
		done := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				p.cond.Broadcast()
			case <-done:
			}
		}()
		p.cond.Wait()
		close(done)
	}
	p.used += n

	return nil
}

func (p *pool) release(n int64) {
	p.mu.Lock()
	p.used -= n
	if p.used < 0 {
		p.used = 0
	}
	p.mu.Unlock()
	p.cond.Broadcast()
}

// Used is what uploads hold now.
func (p *pool) Used() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.used
}

// part is one upload's staging buffer: aligned in memory, growing by
// growStep up to its cap, holding what has not reached the device.
type part struct {
	pool *pool
	raw  []byte
	buf  []byte // the aligned window into raw
	n    int    // bytes held
	cap  int
}

func newPart(pool *pool, cap int) *part {
	return &part{pool: pool, cap: cap}
}

// room makes sure at least `want` more bytes fit, growing within the cap.
func (p *part) room(ctx context.Context, want int) error {
	need := p.n + want
	if need > p.cap {
		need = p.cap
	}
	if need <= len(p.buf) {
		return nil
	}
	size := ((need + growStep - 1) / growStep) * growStep
	if size > p.cap {
		size = p.cap
	}
	if err := p.pool.acquire(ctx, int64(size-len(p.buf))); err != nil {
		return err
	}
	raw := make([]byte, size+Align)
	off := Align - int(uintptr(unsafePointer(raw))%Align)
	if off == Align {
		off = 0
	}
	buf := raw[off : off+size]
	copy(buf, p.buf[:p.n])
	p.raw, p.buf = raw, buf

	return nil
}

// free returns the buffer to the pool.
func (p *part) free() {
	if p.buf != nil {
		p.pool.release(int64(len(p.buf)))
	}
	p.raw, p.buf, p.n = nil, nil, 0
}

// space is what the buffer can take before it is full.
func (p *part) space() []byte { return p.buf[p.n:] }

// aligned is the length of the held bytes' aligned prefix.
func (p *part) aligned() int { return p.n &^ (Align - 1) }

// flush writes the aligned prefix at `off` and keeps the remainder at the
// front, answering how much reached the file.
func (p *part) flush(f *os.File, off int64) (int, error) {
	n := p.aligned()
	if n == 0 {
		return 0, nil
	}
	if _, err := f.WriteAt(p.buf[:n], off); err != nil {
		return 0, err
	}
	rest := copy(p.buf, p.buf[n:p.n])
	p.n = rest

	return n, nil
}

// flushAll writes everything held at `off`, padded to alignment with
// zeros the caller truncates away; for the last part of an upload.
func (p *part) flushAll(f *os.File, off int64) (int, error) {
	if p.n == 0 {
		return 0, nil
	}
	held := p.n
	padded := (held + Align - 1) &^ (Align - 1)
	if padded > len(p.buf) {
		// No room for the padding within the buffer: write the aligned
		// prefix, then the remainder in a fresh aligned block.
		n, err := p.flush(f, off)
		if err != nil {
			return 0, err
		}
		off += int64(n)
		held = p.n
		padded = (held + Align - 1) &^ (Align - 1)
		if padded > len(p.buf) {
			blk := alignedBlock(padded)
			copy(blk, p.buf[:held])
			if _, err := f.WriteAt(blk, off); err != nil {
				return 0, err
			}
			p.n = 0

			return n + held, nil
		}
		for i := held; i < padded; i++ {
			p.buf[i] = 0
		}
		if _, err := f.WriteAt(p.buf[:padded], off); err != nil {
			return 0, err
		}
		p.n = 0

		return n + held, nil
	}
	for i := held; i < padded; i++ {
		p.buf[i] = 0
	}
	if _, err := f.WriteAt(p.buf[:padded], off); err != nil {
		return 0, err
	}
	p.n = 0

	return held, nil
}

// alignedBlock is a zeroed buffer of n bytes aligned to Align.
func alignedBlock(n int) []byte {
	raw := make([]byte, n+Align)
	off := Align - int(uintptr(unsafePointer(raw))%Align)
	if off == Align {
		off = 0
	}

	return raw[off : off+n]
}

// openData opens the file for the data writes: with O_DIRECT where the
// sink can, so the page cache stays out of the way (§22.4).
func openData(path string, direct bool) (*os.File, error) {
	flag := os.O_RDWR
	if direct {
		flag |= syscall.O_DIRECT
	}
	f, err := os.OpenFile(path, flag, 0)
	if err != nil && direct {
		// A filesystem that refused O_DIRECT after the probe said yes:
		// buffered, rather than no upload.
		return os.OpenFile(path, os.O_RDWR, 0)
	}

	return f, err
}
