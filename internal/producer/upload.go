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
	errTooLong = errors.New("413: the upload exceeds max_length")
	errRefused = errors.New("the node refused the upload")
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

	// Retries exhausted: the object is LOST and the segment dropped (§13).
	rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	u.Objects.ReportFailure(rctx, api.ObjectReportFailureRequest_builder{
		Ref: api.ObjectRef_builder{Id: al.GetObjectId()}.Build(), Reason: "every candidate failed",
	}.Build())
	cancel()
	res.Err = errors.New("every candidate failed")

	return res
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
	url := fmt.Sprintf("%s://%s:%d/%s", ep.GetScheme(), ep.GetHost(), ep.GetPort(), al.GetObjectKey())

	var offset int64
	lastProgress := time.Now()
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		status, newOffset, err := u.put(ctx, url, cand.GetToken(), al, seg, offset)
		switch {
		case status == http.StatusCreated || status == http.StatusOK:
			return nil
		case status == http.StatusRequestEntityTooLarge:
			return errTooLong
		case status == http.StatusConflict:
			// The node's offset differs: carry on from there (§12.5).
			if newOffset >= 0 {
				offset = newOffset
				lastProgress = time.Now()
				continue
			}
			return errRefused
		case status == http.StatusServiceUnavailable:
			// Busy: wait Retry-After on the same target (§13).
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
			u.Log.Debug("upload interrupted", "key", al.GetObjectKey(), "err", err.Error())
		}
		cur, herr := u.head(ctx, url, cand.GetToken())
		if herr == nil {
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
func (u *Uploader) put(ctx context.Context, url, tok string, al *api.Allocation, seg *Segment, offset int64) (int, int64, error) {
	live := !seg.Closed()
	var body io.Reader = seg.ReaderFrom(offset)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, body)
	if err != nil {
		return 0, -1, err
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
		return 0, -1, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

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
		return resp.StatusCode, v, fmt.Errorf("server error %d", resp.StatusCode)
	}

	return resp.StatusCode, v, nil
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
