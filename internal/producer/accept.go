package producer

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/storage"
	"github.com/lesomnus/shale/internal/token"
	"github.com/lesomnus/shale/internal/upload"
)

// `retain: accepted` (§41.9): a segment is handed to the satellite, which
// takes its bytes into its own RAM and delivers them to the allocated node
// itself, failing over as the producer would. The producer sends the
// segment in parts, as under `written`; the satellite answers each with
// the offset below which it holds the bytes (Shale-Accepted), and the
// producer lets those go. Once the satellite holds the whole segment it
// answers 202, and the segment is handed off: neither stored nor lost
// here, the satellite's from then on.
//
// What the satellite will not take it passes through: the node's answer,
// marked Shale-Pass, and the producer goes on as under `committed`.
// Without a satellite, `accepted` is `committed`.

var (
	// errAcceptedLess is a satellite that holds less of a segment than
	// the producer already let go: what it lost cannot be sent again.
	errAcceptedLess = errors.New("the satellite holds less of the segment than was let go")
	// errHolderGone is a satellite that holds a segment's start and could
	// not be reached for resume_timeout.
	errHolderGone = errors.New("the satellite holding the segment's start could not be reached")
)

// handoff hands a segment to the satellite. ok false: the satellite did
// not take it, or there is none, and the segment goes as under
// `committed`, Passed set when the satellite passed it through; sent is
// how far requests the satellite passed on may have reached the node at
// the first candidate's key.
func (u *Uploader) handoff(ctx context.Context, al *api.Allocation, seg *Segment) (Result, upload.Sent, bool) {
	res := Result{Allocation: al}
	cands := al.GetCandidates()
	if len(cands) == 0 || len(cands[0].GetEndpoints()) == 0 || seg.wasPassed() {
		return res, nil, false
	}
	if al.GetCaptureEpoch() != seg.CaptureEpoch {
		return res, nil, false
	}
	cand := cands[0]
	if _, via := u.Satellite.route(cand.GetEndpoints()[0]); (!via || !u.Satellite.accepts()) && seg.Released() == 0 {
		if !u.plain.Swap(true) {
			u.Log.Info("retain: accepted takes a satellite that takes it; without one the segments go as under committed")
		}
		return res, nil, false
	}
	u.plain.Store(false)
	key := cand.GetLaminaKey()
	if key == "" {
		key = al.GetLaminaKey()
	}
	abandon := time.Duration(al.GetLink().GetAbandonTimeoutSeconds()) * time.Second
	if abandon <= 0 {
		abandon = 5 * time.Minute
	}
	handoff, err := upload.EncodeHandoff(api.Handoff_builder{
		Allocation:       al,
		ResumeTimeoutMs:  u.Cfg.ResumeTimeout.Milliseconds(),
		AbandonTimeoutMs: abandon.Milliseconds(),
		RetryAfterCapMs:  u.Cfg.RetryAfterCap.Milliseconds(),
		PlacementRetries: int32(u.Cfg.PlacementRetries),
	}.Build())
	if err != nil {
		return res, nil, false
	}

	// The token is the first candidate's, renewed as an attempt's is: the
	// satellite checks it on every request (§41.3).
	ls := upload.NewLease(u.Laminae, u.Log, al, cand)
	lctx, stop := context.WithCancel(ctx)
	defer stop()
	go ls.Keep(lctx)

	res.Attempts = 1
	offset := seg.Released()
	// sentMax is the furthest byte sent at the key: the satellite may have
	// passed some of it on to the node before it failed.
	var sentMax int64
	sent := func() upload.Sent { return upload.Sent{key: sentMax} }
	// cut ends a segment whose start only the satellite has (§41.9): it
	// delivers what it holds, if it is there to.
	cut := func(err error) (Result, upload.Sent, bool) {
		u.Log.Warn("handed-off segment cut short at what the satellite holds", "key", al.GetLaminaKey(), "offset", seg.Released(), "err", err.Error())
		res.Cut, res.Err = true, err

		return res, nil, true
	}
	lastProgress := time.Now()
	for {
		if ctx.Err() != nil {
			res.Err = ctx.Err()
			return res, nil, true
		}
		tok, err := ls.Token(ctx)
		if err != nil {
			if seg.Released() > 0 {
				return cut(err)
			}
			return res, sent(), false
		}
		base, ok := u.Satellite.holder()
		if !ok {
			if seg.Released() > 0 {
				return cut(errHolderGone)
			}
			return res, sent(), false
		}
		edit := func(req *http.Request) {
			req.Header.Set(upload.HdrRetain, upload.RetainAccepted)
			if offset == 0 {
				req.Header.Set(upload.HdrHandoff, handoff)
			}
		}
		r := u.engine.Put(ctx, base+"/"+key, true, tok, al, seg, offset, u.Cfg.AcceptPart, edit)
		if offset+r.Sent > sentMax {
			sentMax = offset + r.Sent
		}
		if ctx.Err() != nil {
			res.Err = ctx.Err()
			return res, nil, true
		}
		accepted, holds := acceptedOf(r.Resp)
		switch {
		case r.Resp != nil && r.Resp.Header.Get(upload.HdrPass) != "":
			// Passed through: the node's answer. The producer keeps the
			// bytes, as under `committed`, and the segment goes on that way.
			u.Satellite.succeeded()
			u.countPassed(r.Resp.Header.Get(upload.HdrPass))
			if seg.Released() > 0 {
				return cut(errAcceptedLess)
			}
			seg.pass()
			res.Passed = true
			if (r.Status == http.StatusCreated || r.Status == http.StatusOK) && r.Complete {
				res.Stored = true
				return res, nil, true
			}

			return res, sent(), false
		case r.Status == http.StatusAccepted && holds:
			u.Satellite.succeeded()
			if !seg.Closed() || accepted != seg.Len() {
				// Not what was sent: ask where it stands.
				break
			}
			seg.Release(accepted)
			res.HandedOff = true

			return res, nil, true
		case (r.Status == http.StatusNoContent || r.Status == http.StatusConflict) && holds:
			u.Satellite.succeeded()
			if accepted < seg.Released() {
				return cut(errAcceptedLess)
			}
			if accepted > max(sentMax, offset) {
				return cut(fmt.Errorf("the satellite holds %d bytes of a segment %d of which were sent", accepted, sentMax))
			}
			moved := accepted > offset
			if moved {
				lastProgress = time.Now()
			}
			offset = accepted
			seg.Release(accepted)
			if !moved && r.Status == http.StatusNoContent {
				// It took nothing of this request: no room for more yet.
				if time.Since(lastProgress) > u.Cfg.ResumeTimeout {
					return cut(errors.New("the satellite stayed full past resume_timeout"))
				}
				if !sleep(ctx, u.retryAfterOf(r.Resp)) {
					res.Err = ctx.Err()
					return res, nil, true
				}
			}
			continue
		case r.Status == http.StatusServiceUnavailable:
			// No room at the satellite for more: wait, as for a busy node.
			u.Satellite.succeeded()
			if time.Since(lastProgress) > u.Cfg.ResumeTimeout {
				if seg.Released() > 0 {
					return cut(errors.New("the satellite stayed full past resume_timeout"))
				}
				return res, sent(), false
			}
			wait := u.engine.Cfg.RetryAfterCap
			if r.Offset > 0 {
				wait = min(time.Duration(r.Offset)*time.Second, wait)
			}
			if !sleep(ctx, wait) {
				res.Err = ctx.Err()
				return res, nil, true
			}
			continue
		case r.Resp != nil && r.Err == nil && !upload.SatelliteFailure(true, r.Resp, nil):
			// A refusal of the satellite's own, a token it would not take
			// say: as under `committed`, while it holds nothing.
			u.Satellite.succeeded()
			if seg.Released() == 0 {
				u.Log.Info("the satellite did not take the segment; as under committed", "key", al.GetLaminaKey(), "status", r.Status, "why", r.Resp.Header.Get(upload.HdrSatellite))
				return res, sent(), false
			}
			return cut(fmt.Errorf("the satellite refused: %d", r.Status))
		default:
			u.satelliteFailed(r.Failure())
		}

		// No answer, or one that says nothing of what it holds: ask the
		// satellite where the segment stands, and resume from there.
		held, whole, herr := u.heldAt(ctx, base, key, tok)
		switch {
		case herr == nil && whole && seg.Closed() && (held == seg.Len() || held < 0):
			// Whole at the satellite, or at a node it delivered to already:
			// the answer that said so did not arrive.
			if held < 0 {
				held = seg.Len()
			}
			seg.Release(held)
			res.HandedOff = true
			return res, nil, true
		case herr == nil:
			if held < seg.Released() {
				return cut(errAcceptedLess)
			}
			if held == 0 {
				// It holds nothing: nothing ties the segment to it.
				return res, sent(), false
			}
			if held > max(sentMax, offset) {
				return cut(fmt.Errorf("the satellite holds %d bytes of a segment %d of which were sent", held, sentMax))
			}
			if held > offset {
				lastProgress = time.Now()
			}
			offset = held
			seg.Release(held)
		case seg.Released() == 0:
			// Unreachable, and it holds nothing of ours: as under
			// `committed`, wherever requests go now.
			return res, sent(), false
		}
		if time.Since(lastProgress) > u.Cfg.ResumeTimeout {
			return cut(errHolderGone)
		}
		if !sleep(ctx, 2*time.Second) {
			res.Err = ctx.Err()
			return res, nil, true
		}
	}
}

// acceptedOf is the offset an answer says the satellite holds below.
func acceptedOf(resp *http.Response) (int64, bool) {
	if resp == nil {
		return 0, false
	}
	s := strings.TrimSpace(resp.Header.Get(upload.HdrAccepted))
	if s == "" {
		return 0, false
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v < 0 {
		return 0, false
	}

	return v, true
}

// heldAt asks the satellite how much of the segment it holds: the offset,
// and whether that is the whole of it; nothing it holds is 0, and -1 with
// whole is a segment a node has complete, which the satellite delivered.
func (u *Uploader) heldAt(ctx context.Context, base, key, tok string) (int64, bool, error) {
	hctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(hctx, http.MethodHead, base+"/"+key, nil)
	if err != nil {
		return 0, false, err
	}
	req.Header.Set("Authorization", token.Scheme+" "+tok)
	req.Header.Set(upload.HdrRetain, upload.RetainAccepted)
	u.Satellite.prepare(req, true)
	resp, err := u.Cfg.Client.Do(req)
	if err != nil {
		return 0, false, err
	}
	resp.Body.Close()
	if upload.SatelliteFailure(true, resp, nil) {
		return 0, false, fmt.Errorf("the satellite refused: %d", resp.StatusCode)
	}
	v, ok := acceptedOf(resp)
	if !ok {
		// The node's answer, passed on: the satellite holds nothing of it,
		// and the node may have it whole.
		if resp.StatusCode == http.StatusOK && strings.TrimSpace(resp.Header.Get(storage.HdrUploadComplete)) == "?1" {
			return -1, true, nil
		}
		return 0, false, nil
	}

	return v, strings.TrimSpace(resp.Header.Get(storage.HdrUploadComplete)) == "?1", nil
}

// retryAfterOf is how long an answer says to wait, within retry_after_cap.
func (u *Uploader) retryAfterOf(resp *http.Response) time.Duration {
	wait := 5 * time.Second
	if resp != nil {
		if v, err := strconv.Atoi(strings.TrimSpace(resp.Header.Get(storage.HdrRetryAfter))); err == nil && v > 0 {
			wait = time.Duration(v) * time.Second
		}
	}

	return min(wait, u.engine.Cfg.RetryAfterCap)
}

// countPassed counts a request the satellite passed through.
func (u *Uploader) countPassed(why string) {
	u.passed.Add(1)
	if u.Satellite != nil {
		u.Satellite.passed.Add(1)
		u.Satellite.passedTotal.Add(1)
	}
	if u.m != nil {
		u.m.acceptPassed.Add(context.Background(), 1, reasonAttr(why))
	}
}
