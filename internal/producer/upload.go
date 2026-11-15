package producer

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/h3"
	"github.com/lesomnus/shale/internal/upload"
)

// The upload client (§12.2, §13) is internal/upload's, which a satellite
// runs too (§41.9): live or buffered, resumable from the offset HEAD
// reports, moving to the next candidate after a failure it reports, asking
// for more when the candidates run out. What is the producer's own is the
// path its requests take (§41.5), and handing a segment to a satellite
// under `retain: accepted` (§41.9).

// UploadConfig is the producer's own upload settings (§36.1).
type UploadConfig struct {
	ResumeTimeout    time.Duration
	RetryAfterCap    time.Duration
	PlacementRetries int
	Client           *http.Client
	// HTTP3 is when a node's HTTP/3 is taken (§35.6, `producer.http3`):
	// off by default, so a producer updated without a word in its
	// configuration uploads over TCP as it did. Read where the producer
	// makes Client; a Client given is used as it is.
	HTTP3 h3.Mode
	// Part is how many bytes one request carries under `retain: written`
	// before it ends and the node reports what is durable (§12.2); about
	// the node's part size keeps the device's writes large.
	Part int64
	// AcceptPart is how many bytes one request carries under `retain:
	// accepted` before the satellite answers what it holds and the
	// producer lets it go (§41.9): no device waits on it, so it is small,
	// and the producer holds about this much of a live segment.
	AcceptPart int64
	// Via is the path uploads take (§41.5, `producer.upload_via`):
	// directly to the nodes by default, so a producer updated without a
	// word in its configuration uploads as it did.
	Via Via
}

func (c *UploadConfig) defaults() {
	e := c.engine()
	e.Defaults()
	c.ResumeTimeout, c.RetryAfterCap, c.PlacementRetries, c.Client, c.Part = e.ResumeTimeout, e.RetryAfterCap, e.PlacementRetries, e.Client, e.Part
	if c.AcceptPart == 0 {
		c.AcceptPart = 1 << 20
	}
}

func (c *UploadConfig) engine() upload.Config {
	return upload.Config{ResumeTimeout: c.ResumeTimeout, RetryAfterCap: c.RetryAfterCap, PlacementRetries: c.PlacementRetries, Client: c.Client, Part: c.Part}
}

// Uploader uploads segments with the allocations it is given.
type Uploader struct {
	// m counts what the upload path finds out (§31); nil in tests.
	m       *metrics
	Cfg     UploadConfig
	Laminae api.LaminaServiceClient
	Log     *slog.Logger
	Mode    api.UploadMode
	// Written is `retain: written` (§12.2): a live segment goes as a series
	// of requests, the bytes a node reported durable are released, and a
	// target that fails after that ends the segment where it has it.
	Written bool
	// Accepted is `retain: accepted` (§41.9): a segment goes to the
	// satellite, which takes the bytes, and the producer lets them go as
	// the satellite says it holds them. Without a satellite it is
	// `committed`.
	Accepted bool
	// Satellite routes requests through the satellite the CP assigned,
	// where Cfg.Via says to (§41.5); nil is directly to the nodes.
	Satellite *SatelliteLink

	// defaulted fills in what Cfg leaves out, once: every source's upload
	// loop calls Upload, each from a goroutine of its own.
	defaulted sync.Once
	engine    *upload.Uploader
	// passed counts requests a satellite passed through rather than
	// accepted; plain says the log told that `accepted` went as
	// `committed` for want of a satellite.
	passed atomic.Int64
	plain  atomic.Bool
}

// Result is what became of a segment.
type Result = upload.Result

// init fills in Cfg and makes the engine, once.
func (u *Uploader) init() {
	u.defaulted.Do(func() {
		u.Cfg.defaults()
		u.engine = &upload.Uploader{Cfg: u.Cfg.engine(), Laminae: u.Laminae, Log: u.Log, Written: u.Written, Router: linkRouter{u}}
		if u.m != nil {
			m := u.m
			u.engine.Hooks = upload.Hooks{
				Stored: func(d time.Duration) {
					m.uploadDuration.Record(context.Background(), float64(d.Microseconds())/1000)
				},
				Retry:   func(kind string) { m.retries.Add(context.Background(), 1, kindAttr(kind)) },
				Request: func(via bool) { m.uploadRequests.Add(context.Background(), 1, pathAttr(via)) },
			}
		}
	})
}

// Upload sends one segment: live while it grows, buffered once closed. It
// answers when the segment is durable somewhere, handed to a satellite,
// or lost.
func (u *Uploader) Upload(ctx context.Context, al *api.Allocation, seg *Segment) Result {
	u.init()
	var sent upload.Sent
	if u.Accepted {
		res, s, ok := u.handoff(ctx, al, seg)
		if ok {
			return res
		}
		sent = s
		if res.Passed {
			// The satellite passed it through: the rest goes as under
			// `committed`, and the result says so.
			out := u.engine.UploadSent(ctx, al, seg, sent)
			out.Passed = true

			return out
		}
	}

	return u.engine.UploadSent(ctx, al, seg, sent)
}

// GiveUp reports a lamina the producer will not upload: it becomes LOST
// (§13). The CP leaves a lamina that is no longer PENDING as it is, so a
// segment stored after all is not undone. key is for the log.
func (u *Uploader) GiveUp(ctx context.Context, lamina []byte, key, reason string) {
	u.init()
	u.engine.GiveUp(ctx, lamina, key, reason)
}

// Skip tells the CP nothing was stored for a segment on purpose (§38.10):
// its lamina becomes SKIPPED with the span and the reason.
func (u *Uploader) Skip(ctx context.Context, al *api.Allocation, ended time.Time, reason api.LaminaSkipReason) error {
	u.init()

	return u.engine.Skip(ctx, al, ended, reason)
}

func mustId(b []byte) pdid.Id { return upload.MustId(b) }

// satelliteFailed tells the satellite link, and counts a fallback.
func (u *Uploader) satelliteFailed(err error) {
	if u.Satellite.failed(err) && u.m != nil {
		u.m.satelliteFallbacks.Add(context.Background(), 1)
	}
}

// linkRouter is the satellite link as the upload client asks it: where
// each request goes, and how the satellite did (§41.5).
type linkRouter struct{ u *Uploader }

func (r linkRouter) Route(cand *api.Candidate) (string, bool, error) {
	eps := cand.GetEndpoints()
	if len(eps) == 0 {
		return "", false, errors.New("no endpoint")
	}
	base, via := r.u.Satellite.route(eps[0])

	return base, via, nil
}

func (r linkRouter) Prepare(req *http.Request, via bool) { r.u.Satellite.prepare(req, via) }
func (r linkRouter) Sent(via bool)                       { r.u.Satellite.sent(via) }
func (r linkRouter) Failed(err error)                    { r.u.satelliteFailed(err) }
func (r linkRouter) Succeeded()                          { r.u.Satellite.succeeded() }
func (r linkRouter) DirectFailed()                       { r.u.Satellite.directFailed() }
