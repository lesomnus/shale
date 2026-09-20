package producer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/storage"
	"github.com/lesomnus/shale/internal/token"
	"github.com/lesomnus/shale/server/core"
)

// The upload client (§12.2, §13): live or buffered, resumable from the
// offset HEAD reports, moving to the next candidate after a failure it
// reports, asking for more when the candidates run out.

// UploadConfig is the producer's own upload settings (§36.1).
type UploadConfig struct {
	ResumeTimeout    time.Duration
	RetryAfterCap    time.Duration
	PlacementRetries int
	Client           *http.Client
}

func (c *UploadConfig) defaults() {
	if c.ResumeTimeout == 0 {
		c.ResumeTimeout = 2 * time.Minute
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

// Uploader uploads segments with the allocations it is given.
type Uploader struct {
	Cfg     UploadConfig
	Objects api.ObjectServiceClient
	Log     *slog.Logger
	Mode    api.UploadMode
}

// Result is what became of a segment.
type Result struct {
	Stored     bool
	Incomplete bool
	Attempts   int
	Err        error
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
	// segment's, so the attempt is given up and the object gets another
	// (§12.5, §15).
	errForeign = errors.New(core.ForeignBytesReason)
)

// Upload sends one segment: live while it grows, buffered once closed. It
// answers when the segment is durable somewhere, or lost.
func (u *Uploader) Upload(ctx context.Context, al *api.Allocation, seg *Segment) Result {
	u.Cfg.defaults()
	res := Result{}
	tried := map[string]bool{}

	for round := 0; round <= u.Cfg.PlacementRetries; round++ {
		for _, cand := range al.GetCandidates() {
			key := string(cand.GetAttemptId())
			if tried[key] {
				continue
			}
			tried[key] = true
			res.Attempts++

			err := u.attempt(ctx, al, cand, seg)
			if err == nil {
				res.Stored = true
				return res
			}
			if ctx.Err() != nil {
				res.Err = ctx.Err()
				return res
			}
			u.Log.Warn("attempt failed", "key", al.GetObjectKey(), "node", pdid.Id(mustId(cand.GetNodeId())).String(), "err", err.Error())
			// The failed attempt is reported before moving on (§13).
			rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			u.Objects.ReportAttempt(rctx, api.ObjectReportAttemptRequest_builder{
				Ref:           api.ObjectRef_builder{Id: al.GetObjectId()}.Build(),
				Attempt:       api.AttemptRef_builder{Id: cand.GetAttemptId()}.Build(),
				FailureReason: err.Error(),
			}.Build())
			cancel()
		}

		// Every candidate tried: ask for the next one.
		rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		next, err := u.Objects.Reallocate(rctx, api.ObjectReallocateRequest_builder{Ref: api.ObjectRef_builder{Id: al.GetObjectId()}.Build()}.Build())
		cancel()
		if err != nil {
			u.Log.Warn("reallocate", "key", al.GetObjectKey(), "err", err.Error())
			break
		}
		al = next
	}

	// Every candidate and every reallocation failed: not stored anywhere.
	// The producer keeps the segment and tries again later, or gives it up
	// when its RAM budget says so (§16).
	res.Err = errors.New("every candidate failed")

	return res
}

// GiveUp reports an object the producer will not upload: it becomes LOST
// (§13).
func (u *Uploader) GiveUp(ctx context.Context, al *api.Allocation, reason string) {
	rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, err := u.Objects.ReportFailure(rctx, api.ObjectReportFailureRequest_builder{
		Ref: api.ObjectRef_builder{Id: al.GetObjectId()}.Build(), Reason: reason,
	}.Build()); err != nil {
		u.Log.Warn("report failure", "key", al.GetObjectKey(), "err", err.Error())
	}
}

func mustId(b []byte) pdid.Id {
	id, _ := pdid.From(b)

	return id
}

// attempt is one target: same-target retries with resume until
// resume_timeout passes without progress (§13).
func (u *Uploader) attempt(ctx context.Context, al *api.Allocation, cand *api.Candidate, seg *Segment) error {
	if len(cand.GetEndpoints()) == 0 {
		return errors.New("no endpoint")
	}
	ep := cand.GetEndpoints()[0]
	// The key is the candidate's: it names the attempt, and the token
	// names the key (§23.2).
	key := cand.GetObjectKey()
	if key == "" {
		key = al.GetObjectKey()
	}
	url := fmt.Sprintf("%s://%s:%d/%s", ep.GetScheme(), ep.GetHost(), ep.GetPort(), key)

	var offset int64
	// sentMax is the furthest byte this process sent for the key: a node
	// offset beyond it is somebody else's bytes.
	var sentMax int64
	lastProgress := time.Now()
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		status, newOffset, sent, complete, err := u.put(ctx, url, cand.GetToken(), al, seg, offset)
		if offset+sent > sentMax {
			sentMax = offset + sent
		}
		if (status == http.StatusCreated || status == http.StatusOK) && !complete {
			// A success status that does not say the object is complete is
			// no commit: an answer the node did not mean (§12.5). The
			// offset is asked for and the upload resumes.
			status, err = 0, errNoCompletion
		}
		switch {
		case status == http.StatusCreated || status == http.StatusOK:
			return nil
		case status == http.StatusRequestEntityTooLarge:
			return errTooLong
		case status == http.StatusConflict:
			// The node's offset differs: carry on from there (§12.5), unless
			// it is past what we told it, which is a file we did not write:
			// our own bytes only ever leave the node behind what we said.
			if newOffset > offset {
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
			u.Log.Info("node busy", "key", al.GetObjectKey(), "wait", wait.String())
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
			u.Log.Warn("upload interrupted", "key", al.GetObjectKey(), "url", url, "err", err.Error())
		}
		cur, herr := u.head(ctx, url, cand.GetToken())
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

// put is one request from `offset`: the rest of the segment as a body that
// grows while the capture runs (live) or a known length (buffered). It
// answers the status, and for 409/503 the offset or Retry-After.
// put is one request: the status, the offset a 409 or the Retry-After a
// 503 carried, the bytes sent, and whether the answer said the object is
// complete.
func (u *Uploader) put(ctx context.Context, url, tok string, al *api.Allocation, seg *Segment, offset int64) (int, int64, int64, bool, error) {
	live := !seg.Closed()
	counted := &countingReader{r: seg.ReaderFrom(offset)}
	var body io.Reader = counted
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, body)
	if err != nil {
		return 0, -1, 0, false, err
	}
	req.Header.Set("Authorization", token.Scheme+" "+tok)
	req.Header.Set(storage.HdrUploadOffset, strconv.FormatInt(offset, 10))
	req.Header.Set(storage.HdrUploadComplete, "?1")
	req.Header.Set(storage.HdrDateStarted, seg.Started.UTC().Format(time.RFC3339Nano))
	if live {
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
		if !seg.Ended.IsZero() {
			req.Header.Set(storage.HdrDateEnded, seg.Ended.UTC().Format(time.RFC3339Nano))
		}
	}

	resp, err := u.Cfg.Client.Do(req)
	if err != nil {
		return 0, -1, counted.n, false, err
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
		u.Log.Info("node refused", "key", al.GetObjectKey(), "why", reason)
	}

	var v int64 = -1
	switch resp.StatusCode {
	case http.StatusConflict:
		if s := resp.Header.Get(storage.HdrUploadOffset); s != "" {
			v, _ = strconv.ParseInt(s, 10, 64)
		}
	case http.StatusServiceUnavailable:
		if s := resp.Header.Get(storage.HdrRetryAfter); s != "" {
			v, _ = strconv.ParseInt(s, 10, 64)
		}
	}
	if resp.StatusCode >= 500 {
		return resp.StatusCode, v, counted.n, complete, fmt.Errorf("server error %d", resp.StatusCode)
	}

	return resp.StatusCode, v, counted.n, complete, nil
}

// countingReader counts what a request body handed over.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)

	return n, err
}

// trailerBody fills the end-time trailer once the segment is closed, which
// is when the body ends.
type trailerBody struct {
	io.ReadCloser
	seg *Segment
	req *http.Request
}

func (b *trailerBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err == io.EOF && !b.seg.Ended.IsZero() {
		b.req.Trailer.Set(storage.HdrDateEnded, b.seg.Ended.UTC().Format(time.RFC3339Nano))
	}

	return n, err
}

// head asks the node where the upload stands: the offset, or -1 when the
// upload is already complete.
func (u *Uploader) head(ctx context.Context, url, tok string) (int64, error) {
	hctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(hctx, http.MethodHead, url, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", token.Scheme+" "+tok)
	resp, err := u.Cfg.Client.Do(req)
	if err != nil {
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
