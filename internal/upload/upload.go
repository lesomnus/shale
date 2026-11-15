// Package upload is the upload client of §12.2 and §13: one segment sent
// to the candidates of its allocation, resumable from the offset a node
// reports, moving to the next candidate after a failure it reports, and
// asking for more when the candidates run out. A producer runs it for its
// own segments; a satellite runs it for the segments it took under
// `retain: accepted` (§41.9), so both fail over alike.
package upload

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lesomnus/payday/pdid"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/storage"
	"github.com/lesomnus/shale/internal/token"
	"github.com/lesomnus/shale/server/core"
)

// Config is how an upload behaves (§36.1).
type Config struct {
	ResumeTimeout    time.Duration
	RetryAfterCap    time.Duration
	PlacementRetries int
	Client           *http.Client
	// Part is how many bytes one request carries under `retain: written`
	// before it ends and the node reports what is durable (§12.2).
	Part int64
}

// Defaults fills in what is left out.
func (c *Config) Defaults() {
	if c.ResumeTimeout == 0 {
		c.ResumeTimeout = 2 * time.Minute
	}
	if c.Part == 0 {
		c.Part = 16 << 20
	}
	if c.RetryAfterCap == 0 {
		c.RetryAfterCap = 2 * time.Minute
	}
	if c.PlacementRetries == 0 {
		c.PlacementRetries = 3
	}
	if c.Client == nil {
		c.Client = &http.Client{Transport: &http.Transport{
			MaxIdleConnsPerHost: 8,
			IdleConnTimeout:     90 * time.Second,
			ForceAttemptHTTP2:   true,
		}}
	}
}

// Segment is what an upload sends: bytes that may still be growing while
// a live upload reads them, closed when the segment is cut (§38.2).
type Segment interface {
	// ReaderFrom reads from an offset, blocking for bytes not there yet
	// and ending when the segment is closed.
	ReaderFrom(offset int64) io.Reader
	// Len is the bytes so far, released ones included.
	Len() int64
	Closed() bool
	// Released is the offset below which the bytes were let go, and
	// Release lets them go (§12.2).
	Released() int64
	Release(upTo int64)
	// Span is when the segment's data starts and, once closed, ends.
	Span() (started, ended time.Time)
	// Epoch is the capture incarnation the segment belongs to.
	Epoch() string
}

// Router is where an upload's requests go: for a producer, the node a
// candidate names or the satellite in front of it (§41.5); for a
// satellite, the node at the address the CP gives it (§41.3).
type Router interface {
	// Route is the base URL a request for the candidate goes to, and
	// whether it goes through a satellite.
	Route(cand *api.Candidate) (base string, viaSatellite bool, err error)
	// Prepare marks a request before it goes; put says what a PUT is of,
	// nil for a HEAD.
	Prepare(req *http.Request, viaSatellite bool, put *Request)
	// Sent counts a request on its path.
	Sent(viaSatellite bool)
	// Failed is the satellite failing a request: not the node's failure.
	Failed(err error)
	// Succeeded is an answer through the satellite.
	Succeeded()
	// DirectFailed is a request sent directly that reached no node.
	DirectFailed()
}

// Request is what a PUT is of, for a Router to mark it.
type Request struct {
	Allocation *api.Allocation
	Segment    Segment
	// Offset is where its body begins.
	Offset int64
	// Live says the segment was still growing when the request began.
	Live bool
}

// Hooks are told what the upload finds out, for metrics; each may be nil.
type Hooks struct {
	// Stored is a segment stored, and how long it took.
	Stored func(time.Duration)
	// Retry is a retry: `same_target` after a 503, `placement` after the
	// candidates ran out (§13).
	Retry func(kind string)
	// Request is one PUT, on its path.
	Request func(viaSatellite bool)
}

// Uploader uploads segments with the allocations it is given.
type Uploader struct {
	Cfg     Config
	Laminae api.LaminaServiceClient
	Log     *slog.Logger
	// Written is `retain: written` (§12.2): a live segment goes as a series
	// of requests, the bytes a node reported durable are released, and a
	// target that fails after that ends the segment where it has it.
	Written bool
	// Router says where requests go; nil is the first endpoint of each
	// candidate.
	Router Router
	Hooks  Hooks
}

// Result is what became of a segment.
type Result struct {
	Stored     bool
	Incomplete bool
	Attempts   int
	Err        error
	// Cut says the segment ended where its node had it: bytes below the
	// node's offset had been released under `retain: written`, so no other
	// target could take it from the start (§12.2). The node finalizes what
	// it holds as an incomplete lamina by the abandon rule (§15).
	Cut bool
	// HandedOff says a satellite holds the whole segment and delivers it
	// itself (§41.9): the producer let its bytes go before any node had
	// them.
	HandedOff bool
	// Passed says a satellite passed the segment through to the node
	// rather than took it, and the segment was uploaded as under
	// `committed` (§41.9).
	Passed bool
	// Allocation is the last allocation the upload had: Reallocate and
	// Renew replace the one it was given.
	Allocation *api.Allocation
}

var (
	errResume  = errors.New("no progress within resume_timeout")
	errBusy    = errors.New("the node stayed busy past resume_timeout")
	errTooLong = errors.New("413: the upload exceeds max_length")
	errRefused = errors.New("the node refused the upload")
	// errNoCompletion is a 200 or 201 without Upload-Complete: ?1, which a
	// node never means; the upload resumes from the offset it holds.
	errNoCompletion = errors.New("the node answered success without completion")
	// errForeign is a key that holds bytes this producer never sent: an
	// earlier incarnation's upload of the same slot. They are not this
	// segment's, so the attempt is given up and the lamina gets another
	// (§12.5, §15).
	errForeign = errors.New(core.ForeignBytesReason)
)

// Sent is how far an earlier request for each key went, by key: bytes the
// node may hold at that key that the upload did not send itself, and that
// are its own all the same (a request a satellite passed through, §41.9).
type Sent map[string]int64

// Upload sends one segment: live while it grows, buffered once closed. It
// answers when the segment is durable somewhere, or lost.
func (u *Uploader) Upload(ctx context.Context, al *api.Allocation, seg Segment) Result {
	return u.UploadSent(ctx, al, seg, nil)
}

// UploadSent is Upload, knowing what was sent for each key before.
func (u *Uploader) UploadSent(ctx context.Context, al *api.Allocation, seg Segment, sent Sent) Result {
	began := time.Now()
	res := Result{Allocation: al}
	tried := map[string]bool{}

	for round := 0; round <= u.Cfg.PlacementRetries; round++ {
		if al.GetCaptureEpoch() != seg.Epoch() {
			res.Err = errors.New("allocation capture incarnation differs from queued segment")
			return res
		}
		for _, cand := range al.GetCandidates() {
			key := string(cand.GetAttemptId())
			if tried[key] {
				continue
			}
			tried[key] = true
			res.Attempts++

			err := u.attempt(ctx, al, cand, seg, sent[keyOf(al, cand)])
			if err == nil {
				res.Stored = true
				if u.Hooks.Stored != nil {
					u.Hooks.Stored(time.Since(began))
				}
				return res
			}
			if ctx.Err() != nil {
				res.Err = ctx.Err()
				return res
			}
			if seg.Released() > 0 {
				// Bytes below the node's offset are gone from here: the
				// segment cannot start over elsewhere. It ends where that
				// node has it (§12.2), and the node's abandon rule makes
				// an incomplete lamina of that (§15).
				u.Log.Warn("segment cut short at the node's offset", "key", al.GetLaminaKey(), "node", MustId(cand.GetNodeId()).String(), "offset", seg.Released(), "err", err.Error())
				res.Cut, res.Err = true, err

				return res
			}
			if errors.Is(err, ErrExpired) {
				// The attempt outlived its TTL unrenewed, and the CP will
				// not renew it now: neither the candidates issued with it
				// nor a reallocation help. The segment is kept, and asking
				// for its slot again answers the same lamina with a new
				// attempt (§12.1, §12.2).
				res.Err = err

				return res
			}
			u.Log.Warn("attempt failed", "key", al.GetLaminaKey(), "node", MustId(cand.GetNodeId()).String(), "err", err.Error())
			// The failed attempt is reported before moving on (§13).
			rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			u.Laminae.ReportAttempt(rctx, api.LaminaReportAttemptRequest_builder{
				Ref:           api.LaminaRef_builder{Id: al.GetLaminaId()}.Build(),
				Attempt:       api.AttemptRef_builder{Id: cand.GetAttemptId()}.Build(),
				FailureReason: err.Error(),
			}.Build())
			cancel()
		}

		// Every candidate tried: ask for the next one.
		rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		if u.Hooks.Retry != nil {
			u.Hooks.Retry("placement")
		}
		next, err := u.Laminae.Reallocate(rctx, api.LaminaReallocateRequest_builder{Ref: api.LaminaRef_builder{Id: al.GetLaminaId()}.Build()}.Build())
		cancel()
		if err != nil {
			u.Log.Warn("reallocate", "key", al.GetLaminaKey(), "err", err.Error())
			break
		}
		al = next
		res.Allocation = al
	}

	// Every candidate and every reallocation failed: not stored anywhere.
	// The caller keeps the segment and tries again later, or gives it up
	// when its budget says so (§16).
	res.Err = errors.New("every candidate failed")

	return res
}

// GiveUp reports a lamina the uploader will not upload: it becomes LOST
// (§13). The CP leaves a lamina that is no longer PENDING as it is, so a
// segment stored after all is not undone. key is for the log.
func (u *Uploader) GiveUp(ctx context.Context, lamina []byte, key, reason string) {
	rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, err := u.Laminae.ReportFailure(rctx, api.LaminaReportFailureRequest_builder{
		Ref: api.LaminaRef_builder{Id: lamina}.Build(), Reason: reason,
	}.Build()); err != nil {
		u.Log.Warn("report failure", "key", key, "err", err.Error())
	}
}

// Skip tells the CP nothing was stored for a segment on purpose (§38.10):
// its lamina becomes SKIPPED with the span and the reason.
func (u *Uploader) Skip(ctx context.Context, al *api.Allocation, ended time.Time, reason api.LaminaSkipReason) error {
	rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err := u.Laminae.Skip(rctx, api.LaminaSkipRequest_builder{
		Ref: api.LaminaRef_builder{Id: al.GetLaminaId()}.Build(), DateEnded: timestamppb.New(ended), Reason: reason,
	}.Build())

	return err
}

// MustId is an identifier's bytes as one, or Nil.
func MustId(b []byte) pdid.Id {
	id, _ := pdid.From(b)

	return id
}

// keyOf is the key a candidate's token names: it names the attempt, and
// the token names the key (§23.2).
func keyOf(al *api.Allocation, cand *api.Candidate) string {
	if k := cand.GetLaminaKey(); k != "" {
		return k
	}

	return al.GetLaminaKey()
}

// route is where a request for the candidate goes.
func (u *Uploader) route(cand *api.Candidate) (string, bool, error) {
	if u.Router != nil {
		return u.Router.Route(cand)
	}
	eps := cand.GetEndpoints()
	if len(eps) == 0 {
		return "", false, errors.New("no endpoint")
	}

	return EndpointURL(eps[0]), false, nil
}

// EndpointURL is scheme://host:port of an endpoint.
func EndpointURL(ep *api.Endpoint) string {
	return ep.GetScheme() + "://" + net.JoinHostPort(ep.GetHost(), strconv.Itoa(int(ep.GetPort())))
}

func (u *Uploader) prepare(req *http.Request, via bool, put *Request) {
	if u.Router != nil {
		u.Router.Prepare(req, via, put)
	}
}

func (u *Uploader) failed(err error) {
	if u.Router != nil {
		u.Router.Failed(err)
	}
}

// attempt is one target: same-target retries with resume until
// resume_timeout passes without progress (§13). sentBefore is how far
// requests sent before this upload went at the candidate's key.
func (u *Uploader) attempt(ctx context.Context, al *api.Allocation, cand *api.Candidate, seg Segment, sentBefore int64) error {
	if _, _, err := u.route(cand); err != nil {
		return err
	}
	key := keyOf(al, cand)

	// The token is renewed while the attempt runs, before its TTL, so an
	// upload that outlives it carries on at the same key (§12.2).
	ls := NewLease(u.Laminae, u.Log, al, cand)
	lctx, stop := context.WithCancel(ctx)
	defer stop()
	go ls.Keep(lctx)

	var offset int64
	// sentMax is the furthest byte this process sent for the key: a node
	// offset beyond it is somebody else's bytes.
	sentMax := sentBefore
	// Under `retain: written` a live segment goes in parts: each request
	// ends after `part` bytes without completing, the node answers with
	// what is durable, and those bytes are released (§12.2).
	var part int64
	if u.Written {
		part = u.Cfg.Part
	}
	lastProgress := time.Now()
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		tok, err := ls.Token(ctx)
		if err != nil {
			return err
		}
		// Where this request goes: the node, or the satellite in front of
		// it (§41.5), asked at every request, so a satellite that fails or
		// is replaced is left at the next.
		base, viaSatellite, err := u.route(cand)
		if err != nil {
			return err
		}
		url := base + "/" + key
		sentBefore := sentMax
		r := u.Put(ctx, url, viaSatellite, tok, al, seg, offset, part, nil)
		status, newOffset, sent, complete, err := r.Status, r.Offset, r.Sent, r.Complete, r.Err
		if offset+sent > sentMax {
			sentMax = offset + sent
		}
		if ctx.Err() == nil && u.Router != nil {
			switch {
			case SatelliteFailure(viaSatellite, r.Resp, err):
				// The satellite's failure, not the node's: it does not count
				// against this candidate (§41.5). The offset is asked for
				// where requests go now, and the upload resumes from it.
				u.failed(r.Failure())
				lastProgress = time.Now()
				status, err = 0, r.Failure()
			case viaSatellite:
				u.Router.Succeeded()
			case status == 0 && err != nil:
				u.Router.DirectFailed()
			}
		}
		if (status == http.StatusCreated || status == http.StatusOK) && !complete {
			// A success status that does not say the lamina is complete is
			// no commit: an answer the node did not mean (§12.5). The
			// offset is asked for and the upload resumes.
			status, err = 0, errNoCompletion
		}
		switch {
		case status == http.StatusCreated || status == http.StatusOK:
			return nil
		case status == http.StatusNoContent && part > 0:
			// A part is on the device up to the offset the node reports
			// (aligned, so at most a few KiB short of what was sent): the
			// bytes below it are released and the next part follows, or
			// the completing request once the segment has closed.
			if newOffset < 0 || newOffset > sentMax {
				return errForeign
			}
			if newOffset > offset {
				lastProgress = time.Now()
			}
			offset = newOffset
			seg.Release(offset)
			continue
		case status == http.StatusRequestEntityTooLarge:
			return errTooLong
		case status == http.StatusConflict:
			// The node's offset differs: carry on from there (§12.5), unless
			// it is past both what we told it and what our earlier requests
			// sent, which is a file we did not write. A request the node
			// answers 409 it refuses whole, so only an earlier one can have
			// put our bytes there: one cut short after the node had them,
			// whose offset a HEAD that failed, or a 503, did not bring back.
			if newOffset > max(offset, sentBefore) {
				return errForeign
			}
			if newOffset >= 0 {
				offset = newOffset
				lastProgress = time.Now()
				continue
			}
			return errRefused
		case status == http.StatusServiceUnavailable:
			// Busy: wait Retry-After on the same target (§13), but not
			// forever: a target that stays busy past resume_timeout with
			// no progress is given up like one that stays silent, so a
			// node that keeps restarting cannot hold a segment.
			if time.Since(lastProgress) > u.Cfg.ResumeTimeout {
				return errBusy
			}
			wait := u.retryAfter(newOffset)
			if u.Hooks.Retry != nil {
				u.Hooks.Retry("same_target")
			}
			u.Log.Info("node busy", "key", al.GetLaminaKey(), "wait", wait.String())
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
			continue
		case status == http.StatusForbidden || status == http.StatusUnauthorized:
			return fmt.Errorf("refused: %d", status)
		case status >= 400 && status < 500:
			return fmt.Errorf("refused: %d", status)
		}

		// A transport error or a 5xx: HEAD for the offset and resume.
		if err != nil {
			u.Log.Warn("upload interrupted", "key", al.GetLaminaKey(), "url", token.RedactURL(url), "err", err.Error())
		}
		cur, herr := u.head(ctx, cand, key, tok, &lastProgress)
		if herr == nil {
			if cur > sentMax {
				return errForeign
			}
			if cur > offset {
				offset = cur
				lastProgress = time.Now()
			}
			if cur < 0 {
				// Complete already.
				return nil
			}
		}
		if time.Since(lastProgress) > u.Cfg.ResumeTimeout {
			return errResume
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func (u *Uploader) retryAfter(v int64) time.Duration {
	d := time.Duration(v) * time.Second
	if d <= 0 {
		d = 10 * time.Second
	}
	if d > u.Cfg.RetryAfterCap {
		d = u.Cfg.RetryAfterCap
	}

	return d
}

// Reply is what one request came back with: the status, the offset a 409
// or a 204 or the Retry-After a 503 carried, the bytes sent, whether the
// answer said the lamina is complete, and the answer itself, without its
// body, for whose answer it was (§41.5).
type Reply struct {
	Status   int
	Offset   int64
	Sent     int64
	Complete bool
	Err      error
	Resp     *http.Response
}

// Failure is what the log says of a satellite's failure.
func (r Reply) Failure() error {
	if r.Err != nil {
		return r.Err
	}
	if r.Resp != nil {
		return fmt.Errorf("the satellite refused: %d %s", r.Status, r.Resp.Header.Get(HdrSatellite))
	}

	return errors.New("the satellite did not answer")
}

// Put is one request from `offset`: the rest of the segment as a body that
// grows while the capture runs (live) or a known length (buffered), or,
// with `part` set on a live segment, at most that many bytes that do not
// complete the upload (§12.2). edit, when given, sets what else the
// request carries.
func (u *Uploader) Put(ctx context.Context, url string, viaSatellite bool, tok string, al *api.Allocation, seg Segment, offset, part int64, edit func(*http.Request)) Reply {
	live := !seg.Closed()
	// A live segment in parts: this request carries at most `part` bytes,
	// or what arrives before the segment closes, and does not complete
	// the upload. The completing request comes once the segment is closed
	// and everything is on the node: a closed segment, so the buffered
	// branch, with nothing left to send.
	partial := live && part > 0
	var rd io.Reader = seg.ReaderFrom(offset)
	if partial {
		rd = io.LimitReader(rd, part)
	}
	counted := &countingReader{r: rd}
	var body io.Reader = counted
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, body)
	if err != nil {
		return Reply{Offset: -1, Err: err}
	}
	started, ended := seg.Span()
	req.Header.Set("Authorization", token.Scheme+" "+tok)
	u.prepare(req, viaSatellite, &Request{Allocation: al, Segment: seg, Offset: offset, Live: live})
	req.Header.Set(storage.HdrUploadOffset, strconv.FormatInt(offset, 10))
	req.Header.Set(storage.HdrUploadComplete, "?1")
	req.Header.Set(storage.HdrDateStarted, started.UTC().Format(time.RFC3339Nano))
	if partial {
		req.Header.Set(storage.HdrUploadComplete, "?0")
		req.ContentLength = -1
		if h := al.GetSizeHint(); h > 0 {
			req.Header.Set(storage.HdrSizeHint, strconv.FormatInt(h, 10))
		}
	} else if live {
		// Chunked, with the size hint and the end time as a trailer (§12.2).
		req.ContentLength = -1
		if h := al.GetSizeHint(); h > 0 {
			req.Header.Set(storage.HdrSizeHint, strconv.FormatInt(h, 10))
		}
		req.Trailer = http.Header{storage.HdrDateEnded: nil}
		req.Body = &trailerBody{ReadCloser: io.NopCloser(body), seg: seg, req: req}
	} else {
		n := seg.Len() - offset
		req.ContentLength = n
		req.Header.Set(storage.HdrUploadLength, strconv.FormatInt(seg.Len(), 10))
		if !ended.IsZero() {
			req.Header.Set(storage.HdrDateEnded, ended.UTC().Format(time.RFC3339Nano))
		}
	}
	if edit != nil {
		edit(req)
	}

	if u.Router != nil {
		u.Router.Sent(viaSatellite)
	}
	if u.Hooks.Request != nil {
		u.Hooks.Request(viaSatellite)
	}
	resp, err := u.Cfg.Client.Do(req)
	if err != nil {
		return Reply{Offset: -1, Sent: counted.n.Load(), Err: err}
	}
	defer resp.Body.Close()
	// The body of a refusal says why; the log carries it.
	var reason string
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 200))
		reason = strings.TrimSpace(string(b))
	}
	io.Copy(io.Discard, resp.Body)
	complete := strings.TrimSpace(resp.Header.Get(storage.HdrUploadComplete)) == "?1"
	if resp.StatusCode == http.StatusServiceUnavailable && reason != "" {
		u.Log.Info("node refused", "key", al.GetLaminaKey(), "why", reason)
	}

	var v int64 = -1
	switch resp.StatusCode {
	case http.StatusConflict, http.StatusNoContent:
		if s := resp.Header.Get(storage.HdrUploadOffset); s != "" {
			v, _ = strconv.ParseInt(s, 10, 64)
		}
	case http.StatusServiceUnavailable:
		if s := resp.Header.Get(storage.HdrRetryAfter); s != "" {
			v, _ = strconv.ParseInt(s, 10, 64)
		}
	}
	r := Reply{Status: resp.StatusCode, Offset: v, Sent: counted.n.Load(), Complete: complete, Resp: resp}
	if resp.StatusCode >= 500 {
		r.Err = fmt.Errorf("server error %d", resp.StatusCode)
	}

	return r
}

// countingReader counts what a request body handed over. The transport
// reads a body on a goroutine of its own, which can still be at it when Do
// has answered -- with an error, or a node's answer that came first -- so
// the count is atomic.
type countingReader struct {
	r io.Reader
	n atomic.Int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n.Add(int64(n))

	return n, err
}

// trailerBody fills the end-time trailer once the segment is closed, which
// is when the body ends.
type trailerBody struct {
	io.ReadCloser
	seg Segment
	req *http.Request
}

func (b *trailerBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err == io.EOF {
		if _, ended := b.seg.Span(); !ended.IsZero() {
			b.req.Trailer.Set(storage.HdrDateEnded, ended.UTC().Format(time.RFC3339Nano))
		}
	}

	return n, err
}

// head asks the node where the upload stands: the offset, or -1 when the
// upload is already complete. It goes where a request for the candidate
// goes now, and a satellite that fails it is the satellite's failure, as
// a PUT's is (§41.5).
func (u *Uploader) head(ctx context.Context, cand *api.Candidate, key, tok string, lastProgress *time.Time) (int64, error) {
	base, viaSatellite, err := u.route(cand)
	if err != nil {
		return 0, err
	}
	hctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(hctx, http.MethodHead, base+"/"+key, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", token.Scheme+" "+tok)
	u.prepare(req, viaSatellite, nil)
	resp, err := u.Cfg.Client.Do(req)
	if ctx.Err() == nil && SatelliteFailure(viaSatellite, resp, err) {
		f := Reply{Err: err, Resp: resp}
		if resp != nil {
			f.Status = resp.StatusCode
			resp.Body.Close()
		}
		u.failed(f.Failure())
		*lastProgress = time.Now()

		return 0, f.Failure()
	}
	if err != nil {
		if !viaSatellite && ctx.Err() == nil && u.Router != nil {
			u.Router.DirectFailed()
		}
		return 0, err
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return 0, nil
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("HEAD %d", resp.StatusCode)
	}
	if strings.TrimSpace(resp.Header.Get(storage.HdrUploadComplete)) == "?1" {
		return -1, nil
	}
	v, err := strconv.ParseInt(resp.Header.Get(storage.HdrUploadOffset), 10, 64)
	if err != nil {
		return 0, err
	}

	return v, nil
}
