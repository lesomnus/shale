package producer

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/token"
)

// Renewal (§12.1, §12.2): an attempt and its put token are valid for
// `allocation_ttl`. An attempt still in progress when two thirds of its
// token's life have passed asks LaminaService.Renew for a fresh one, on
// the same attempt and so the same key and target, and the requests that
// follow carry it. One the CP refuses because the attempt is over (past its
// TTL, or no longer ALLOCATED) is not renewed again: the upload gets a new
// attempt instead.

// renewFraction is how far through its token's life an attempt renews,
// as a host renews its certificate (§33.5).
const renewFraction = 2.0 / 3

// errExpired is an attempt whose TTL passed before it could be renewed:
// the CP refuses to renew it, and the candidates issued with it share its
// TTL, so the segment needs a new attempt rather than the next candidate
// (§12.2).
var errExpired = errors.New("the attempt expired before it could be renewed")

// lease is one attempt's put token, kept fresh while the attempt runs.
type lease struct {
	laminae api.LaminaServiceClient
	log     *slog.Logger
	lamina  []byte
	attempt []byte
	key     string
	now     func() time.Time

	mu      sync.Mutex
	tok     string
	renewAt time.Time // zero: the token says no expiry, nothing to renew
	exp     time.Time
	err     error // the CP refused for good
}

func newLease(laminae api.LaminaServiceClient, log *slog.Logger, al *api.Allocation, cand *api.Candidate) *lease {
	l := &lease{laminae: laminae, log: log, lamina: al.GetLaminaId(), attempt: cand.GetAttemptId(), key: cand.GetLaminaKey(), now: time.Now}
	l.set(cand.GetToken())

	return l
}

// set takes a token and works out when it is due for renewal, from its own
// claims: when it was issued and when it expires.
func (l *lease) set(tok string) {
	l.tok = tok
	l.renewAt, l.exp = time.Time{}, time.Time{}
	c, err := token.Parse(tok)
	if err != nil || c.GetExp() == nil {
		return
	}
	l.exp = c.GetExp().AsTime()
	iat := l.exp
	if c.GetIat() != nil {
		iat = c.GetIat().AsTime()
	}
	l.renewAt = iat.Add(time.Duration(float64(l.exp.Sub(iat)) * renewFraction))
}

// token is the token for the next request: renewed first when it is due.
// It answers errExpired once the CP has said the attempt is over.
func (l *lease) token(ctx context.Context) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err == nil && l.laminae != nil && !l.renewAt.IsZero() && !l.now().Before(l.renewAt) {
		l.renew(ctx)
	}

	return l.tok, l.err
}

// renew asks the CP once; the lock is held. A refusal that will not change
// ends the lease; anything else leaves the token as it is, to be asked for
// again before the next request or by the keeper.
func (l *lease) renew(ctx context.Context) {
	rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	al, err := l.laminae.Renew(rctx, api.LaminaRenewRequest_builder{
		Ref:     api.LaminaRef_builder{Id: l.lamina}.Build(),
		Attempt: api.AttemptRef_builder{Id: l.attempt}.Build(),
	}.Build())
	if err != nil {
		switch status.Code(err) {
		case codes.FailedPrecondition, codes.NotFound, codes.InvalidArgument:
			l.err = errExpired
			l.log.Warn("attempt not renewed; it needs a new one", "key", l.key, "err", err.Error())
		default:
			// The CP is unreachable or busy: the token is good until it
			// expires, and the node allows some skew past that (§33.2).
			l.log.Warn("renew", "key", l.key, "expires", l.exp, "err", err.Error())
		}

		return
	}
	for _, c := range al.GetCandidates() {
		if string(c.GetAttemptId()) == string(l.attempt) {
			l.set(c.GetToken())
			l.log.Debug("attempt renewed", "key", l.key, "expires", l.exp)

			return
		}
	}
	// The answer does not carry this attempt: it was superseded meanwhile.
	l.err = errExpired
	l.log.Warn("attempt not renewed; the CP no longer offers it", "key", l.key)
}

// keep renews the token in the background while ctx lasts, so an attempt
// whose one request outlives the TTL (a live upload, a slow link) is still
// ALLOCATED, with a token in hand, when it has to resume.
func (l *lease) keep(ctx context.Context) {
	for {
		l.mu.Lock()
		at, done := l.renewAt, l.err != nil || l.laminae == nil
		if !done && !at.IsZero() && !l.now().Before(at) {
			l.renew(ctx)
			at, done = l.renewAt, l.err != nil
			if !done && !l.now().Before(at) {
				// Not renewed, and not refused: ask again in a while, well
				// before the token runs out.
				at = l.now().Add(min(max(l.exp.Sub(l.now())/4, 100*time.Millisecond), 10*time.Second))
			}
		}
		l.mu.Unlock()
		if done || at.IsZero() {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Until(at)):
		}
	}
}
