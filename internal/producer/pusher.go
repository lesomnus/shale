package producer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"
)

// Pusher writes a stream to a producer's push endpoint (§38.9) in requests
// of about a part each, keeping only the part in flight until the producer
// answered for it, and resuming from HEAD after a refusal or a broken
// connection. It is `shale producer push` and what a robot's first
// integration can copy.
type Pusher struct {
	// Addr is the producer's listener, `unix:/path` or `tcp://host:port`.
	Addr  string
	Alias string
	// Part is how many bytes go in one request; 4 MiB when zero.
	Part int
	Log  *slog.Logger

	client *http.Client
	url    string
}

// ErrBehind is a stream the producer holds more of than the pusher can
// resend: a stdin pusher cannot rewind.
var ErrBehind = errors.New("push: the producer is ahead of this stream; give a file to resume from its offset")

func (p *Pusher) init() error {
	if p.client != nil {
		return nil
	}
	network, addr, err := pushAddr(p.Addr)
	if err != nil {
		return err
	}
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer

		return d.DialContext(ctx, network, addr)
	}}
	p.client = &http.Client{Transport: tr}
	host := "producer"
	if network == "tcp" {
		host = addr
	}
	p.url = "http://" + host + "/sources/" + p.Alias + "/frames"
	if p.Part <= 0 {
		p.Part = 4 << 20
	}

	return nil
}

// Offset asks the producer where the stream is.
func (p *Pusher) Offset(ctx context.Context) (int64, bool, error) {
	if err := p.init(); err != nil {
		return 0, false, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, p.url, nil)
	if err != nil {
		return 0, false, err
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return 0, false, err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return 0, false, fmt.Errorf("push: HEAD: %s", resp.Status)
	}
	off, err := strconv.ParseInt(resp.Header.Get("Upload-Offset"), 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("push: HEAD: no offset")
	}

	return off, resp.Header.Get("Upload-Complete") == "?0", nil
}

// Push sends r from `from`, the offset its first byte has in the stream,
// and completes the stream at EOF when `complete` says so. It answers the
// offset reached.
func (p *Pusher) Push(ctx context.Context, r io.Reader, from int64, complete bool) (int64, error) {
	if err := p.init(); err != nil {
		return from, err
	}
	off := from
	buf := make([]byte, p.Part)
	for {
		n, rerr := io.ReadFull(r, buf)
		if rerr != nil && !errors.Is(rerr, io.EOF) && !errors.Is(rerr, io.ErrUnexpectedEOF) {
			return off, rerr
		}
		last := errors.Is(rerr, io.EOF) || errors.Is(rerr, io.ErrUnexpectedEOF)
		part := buf[:n]
		// One part, until the producer has all of it.
		for len(part) > 0 || (last && complete) {
			cur, err := p.send(ctx, off, part, last && complete)
			if err != nil {
				return off, err
			}
			if cur > off+int64(len(part)) || cur < off {
				return off, ErrBehind
			}
			part = part[cur-off:]
			off = cur
			if last && complete && len(part) == 0 {
				return off, nil
			}
		}
		if last {
			return off, nil
		}
	}
}

// send is one request, retried from the producer's offset while the
// producer refuses or the connection breaks; it answers the offset the
// producer is at, which may be short of the part when it was refused for
// being open elsewhere.
func (p *Pusher) send(ctx context.Context, off int64, part []byte, complete bool) (int64, error) {
	backoff := 200 * time.Millisecond
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, p.url, bytes.NewReader(part))
		if err != nil {
			return off, err
		}
		req.ContentLength = int64(len(part))
		req.Header.Set("Upload-Offset", strconv.FormatInt(off, 10))
		if complete {
			req.Header.Set("Upload-Complete", "?1")
		} else {
			req.Header.Set("Upload-Complete", "?0")
		}
		resp, err := p.client.Do(req)
		if err == nil {
			cur, perr := strconv.ParseInt(resp.Header.Get("Upload-Offset"), 10, 64)
			resp.Body.Close()
			switch {
			case resp.StatusCode == http.StatusNoContent && perr == nil:
				return cur, nil
			case resp.StatusCode == http.StatusConflict && perr == nil:
				if cur != off {
					// The producer is elsewhere in the stream: say so
					// and let the caller resend from there.
					return cur, nil
				}
				err = errors.New("push: a request is open on the source")
			case resp.StatusCode == http.StatusNotFound:
				return off, fmt.Errorf("push: %s: no such source at the producer", p.Alias)
			default:
				err = fmt.Errorf("push: %s", resp.Status)
			}
		}
		if p.Log != nil {
			p.Log.Warn("push: retrying", "source", p.Alias, "offset", off, "err", err.Error(), "in", backoff.String())
		}
		select {
		case <-ctx.Done():
			return off, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 5*time.Second {
			backoff *= 2
		}
		// Where is the producer now?
		if cur, _, herr := p.Offset(ctx); herr == nil && cur != off {
			return cur, nil
		}
	}
}
