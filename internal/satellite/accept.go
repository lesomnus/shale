package satellite

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/storage"
	"github.com/lesomnus/shale/internal/token"
	"github.com/lesomnus/shale/internal/upload"
)

// `retain: accepted` (§41.9): store and forward. A producer that asks for
// it (Shale-Retain) sends the allocation with its first request
// (Shale-Handoff); the satellite checks every candidate's token, has the
// CP record it as the lamina's holder, and takes the bytes into its
// retained pool, answering each request with the offset below which it
// holds them (Shale-Accepted): 204 while more is to come, 202 once it
// holds the whole segment. The producer lets those bytes go. The satellite
// delivers the segment itself, with the upload client a producer runs:
// the candidates in order, resuming on 409 and 503, Reallocate when they
// run out, the token renewed while it lasts, each failure reported, all
// of it in the producer's name.
//
// What it will not take it passes through, as under `committed`: the
// node's own answer, marked Shale-Pass with the reason, so the producer
// keeps its bytes. It never writes accepted bytes to disk, and never
// evicts them: a segment leaves the pool delivered or given up.

// acceptedRetryAfter is the Retry-After of a request whose bytes the pool
// has no room for, in seconds: segments leave it as they are delivered.
const acceptedRetryAfter = "5"

// minReserve is the least a segment reserves: a part of a live upload.
const minReserve = 1 << 20

// heldMap is the segments the satellite holds, by lamina.
func (p *Proxy) heldOf(lamina pdid.Id) *held {
	p.heldMu.Lock()
	defer p.heldMu.Unlock()

	return p.held[lamina]
}

// accepted serves a request under `retain: accepted`. It answers false to
// pass the request through to the node as under `committed`, with the
// reason set in Shale-Pass; and, when it served the request, whether it
// read the body to its end. live, when not nil, is handed the bytes the
// satellite takes, as they come (§41.8): the relay is fed from what the
// producer sends, not from what the satellite later delivers.
func (p *Proxy) accepted(w http.ResponseWriter, r *http.Request, caller pdid.Id, c *api.TokenClaims, key string, live *tap) (bool, bool) {
	lamina, err := pdid.From(c.GetRecord().GetLaminaId())
	if err != nil {
		w.Header().Set(upload.HdrPass, upload.PassUnrecorded)
		return false, false
	}
	if r.Method == http.MethodHead {
		return p.headHeld(w, caller, lamina), false
	}

	var h *held
	var fresh bool
	for {
		h, fresh = p.claim(lamina, caller)
		if fresh {
			break
		}
		// Another request of the segment is admitting it: wait for it.
		<-h.ready
		if h.err == nil {
			break
		}
		// That one found it was not to be taken; this one asks again.
	}
	if h.producer != caller {
		own(w, ReasonToken, http.StatusForbidden, "another producer's lamina")
		return true, false
	}
	if fresh {
		if why, ok := p.admit(w, r, h, caller, c, key, lamina); !ok {
			p.unclaim(h, errors.New(why))
			if why == "" {
				// Refused outright: the answer is written.
				return true, false
			}
			p.passed.Add(1)
			w.Header().Set(upload.HdrPass, why)
			p.Log.Info("passed through rather than accepted", "producer", caller.String(), "key", key, "why", why)

			return false, false
		}
	}

	return true, p.take(w, r, h, live)
}

// claim is the entry of a lamina, made when there is none: fresh says the
// caller is to admit it and close ready.
func (p *Proxy) claim(lamina, caller pdid.Id) (*held, bool) {
	p.heldMu.Lock()
	defer p.heldMu.Unlock()
	if p.held == nil {
		p.held = map[pdid.Id]*held{}
	}
	if h, ok := p.held[lamina]; ok {
		return h, false
	}
	h := newHeld(lamina, caller, "", nil, 0, time.Time{})
	p.held[lamina] = h

	return h, true
}

// unclaim drops an entry that was not admitted, and lets those waiting
// on it go.
func (p *Proxy) unclaim(h *held, err error) {
	p.heldMu.Lock()
	if p.held[h.lamina] == h {
		delete(p.held, h.lamina)
	}
	p.heldMu.Unlock()
	h.err = err
	close(h.ready)
}

// admit decides whether a new segment is taken: the satellite takes
// `accepted`, the request starts it, the handoff is the producer's and
// every token in it checks, the pool and the producer's share have room,
// and the CP records the satellite as the lamina's holder. Not taken, it
// answers the Shale-Pass reason to pass it through, or "" once it has
// refused the request itself.
func (p *Proxy) admit(w http.ResponseWriter, r *http.Request, h *held, caller pdid.Id, c *api.TokenClaims, key string, lamina pdid.Id) (string, bool) {
	if p.Refuse || p.retained == nil {
		return upload.PassRefused, false
	}
	off, err := strconv.ParseInt(strings.TrimSpace(r.Header.Get(storage.HdrUploadOffset)), 10, 64)
	if err != nil || off < 0 {
		own(w, ReasonRequest, http.StatusBadRequest, "no Upload-Offset")
		return "", false
	}
	if off != 0 {
		// It holds nothing of this segment, and the producer let go of
		// what is below off: told so, the producer cuts it short.
		w.Header().Set(upload.HdrAccepted, "0")
		w.Header().Set(storage.HdrUploadOffset, "0")
		own(w, ReasonRequest, http.StatusConflict, "the satellite holds nothing of this segment")
		return "", false
	}
	hd, err := upload.DecodeHandoff(r.Header.Get(upload.HdrHandoff))
	if err != nil {
		own(w, ReasonRequest, http.StatusBadRequest, "no handoff: "+err.Error())
		return "", false
	}
	if err := p.checkHandoff(hd, caller, lamina, key); err != nil {
		p.Log.Info("refused a handoff", "producer", caller.String(), "key", key, "err", err.Error())
		own(w, ReasonToken, http.StatusForbidden, err.Error())
		return "", false
	}

	reserve := reservation(r, hd.GetAllocation(), c.GetMaxLength())
	if ok, why := p.retained.admit(caller, reserve); !ok {
		return why, false
	}
	if p.record != nil {
		rctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		err := p.record(rctx, lamina)
		cancel()
		if err != nil {
			p.retained.free(caller, reserve)
			p.Log.Warn("the CP did not record the satellite as the lamina's holder", "key", key, "err", err.Error())
			return upload.PassUnrecorded, false
		}
	}

	started := time.UnixMilli(c.GetRecord().GetDateStartedMs())
	if v, err := time.Parse(time.RFC3339Nano, r.Header.Get(storage.HdrDateStarted)); err == nil {
		started = v
	}
	h.key, h.handoff, h.maxLen, h.started, h.charged = key, hd, c.GetMaxLength(), started, reserve
	close(h.ready)
	p.Log.Info("accepted", "producer", caller.String(), "key", key, "reserved", reserve)
	go p.deliver(h)

	return "", true
}

// reservation is what a new segment reserves of the pool: the size its
// upload says it will be, else the allocation's size hint, at least a
// part, at most max_length.
func reservation(r *http.Request, al *api.Allocation, maxLen int64) int64 {
	n := al.GetSizeHint()
	for _, k := range []string{storage.HdrUploadLength, storage.HdrSizeHint} {
		if v, err := strconv.ParseInt(r.Header.Get(k), 10, 64); err == nil && v > n {
			n = v
		}
	}
	if r.ContentLength > n {
		n = r.ContentLength
	}
	n = max(n, minReserve)
	if maxLen > 0 && n > maxLen {
		n = maxLen
	}

	return n
}

// checkHandoff checks the allocation a producer hands over: its lamina is
// the one the request's token records, the request's key is one of its
// candidates', and every candidate's token is one the node it names
// would take, issued to this producer for this lamina and this attempt
// (§41.3, §41.9).
func (p *Proxy) checkHandoff(hd *api.Handoff, caller, lamina pdid.Id, key string) error {
	al := hd.GetAllocation()
	if string(al.GetLaminaId()) != string(lamina.Bytes()) {
		return errors.New("the handoff names another lamina than the token")
	}
	if len(al.GetCandidates()) == 0 {
		return errors.New("the handoff has no candidates")
	}
	ours := false
	for i, cand := range al.GetCandidates() {
		c, err := p.Verifier.Verify(cand.GetToken())
		if err != nil {
			return fmt.Errorf("candidate %d: %w", i, err)
		}
		node, err := pdid.From(cand.GetNodeId())
		if err != nil || node.Domain() != DomNode || string(c.GetAud()) != string(node.Bytes()) {
			return fmt.Errorf("candidate %d: the token names another node", i)
		}
		if err := token.Check(c, c.GetAud(), api.TokenOp_TOKEN_OP_PUT); err != nil {
			return fmt.Errorf("candidate %d: %w", i, err)
		}
		if c.GetLaminaKey() == "" || c.GetLaminaKey() != cand.GetLaminaKey() {
			return fmt.Errorf("candidate %d: the token names another key", i)
		}
		if actor, err := pdid.From(c.GetActor()); err != nil || actor != caller {
			return fmt.Errorf("candidate %d: the token was issued to another producer", i)
		}
		if string(c.GetRecord().GetLaminaId()) != string(lamina.Bytes()) {
			return fmt.Errorf("candidate %d: the token is for another lamina", i)
		}
		if len(cand.GetAttemptId()) > 0 && string(c.GetAttemptId()) != string(cand.GetAttemptId()) {
			return fmt.Errorf("candidate %d: the token is for another attempt", i)
		}
		if c.GetRecord().GetCaptureEpoch() != al.GetCaptureEpoch() {
			return fmt.Errorf("candidate %d: the token is for another capture", i)
		}
		if cand.GetLaminaKey() == key {
			ours = true
		}
	}
	if !ours {
		return errors.New("the request's key is none of the handoff's candidates'")
	}

	return nil
}

// take reads a request's bytes into a segment the satellite holds, and
// answers what it holds: 204 with more to come, 202 once the producer
// sent the whole segment. live, when not nil, is handed what it takes, as
// it takes it (§41.8). It answers whether what it read goes on from where
// the next request will: the body read to its end, or a prefix of it the
// pool had room for.
func (p *Proxy) take(w http.ResponseWriter, r *http.Request, h *held, live *tap) bool {
	h.touch()
	n, closed, _ := h.state()
	answer := func(status int, n int64) {
		h.ack(n)
		w.Header().Set(upload.HdrAccepted, strconv.FormatInt(n, 10))
		w.Header().Set(storage.HdrUploadOffset, strconv.FormatInt(n, 10))
		if status == http.StatusAccepted {
			w.Header().Set(storage.HdrUploadComplete, "?1")
		} else {
			w.Header().Set(storage.HdrUploadComplete, "?0")
		}
		w.WriteHeader(status)
	}
	if closed {
		answer(http.StatusAccepted, n)
		return false
	}
	off, err := strconv.ParseInt(strings.TrimSpace(r.Header.Get(storage.HdrUploadOffset)), 10, 64)
	if err != nil || off != n {
		// Not where the satellite stands: from there, as a node answers.
		answer(http.StatusConflict, n)
		return false
	}
	h.mu.Lock()
	gone, full := h.aborted != nil, h.n >= h.charged
	h.mu.Unlock()
	if gone {
		own(w, ReasonRequest, http.StatusGone, "the satellite let this segment go")
		return false
	}
	if full && !p.retained.hasRoom() {
		// Past its reservation, and the pool has no room at all: the rest
		// waits at the producer (§41.9).
		w.Header().Set(upload.HdrAccepted, strconv.FormatInt(n, 10))
		w.Header().Set("Retry-After", acceptedRetryAfter)
		own(w, ReasonBusy, http.StatusServiceUnavailable, "the retained pool is full")
		return false
	}

	keep := func(b []byte) {
		h.write(b)
		p.acceptedBytes.Add(int64(len(b)))
		if live != nil {
			live.write(b)
		}
	}
	buf := make([]byte, 32<<10)
	for {
		k, rerr := r.Body.Read(buf)
		if k > 0 {
			if h.maxLen > 0 && h.Len()+int64(k) > h.maxLen {
				w.Header().Set(upload.HdrAccepted, strconv.FormatInt(h.Len(), 10))
				own(w, ReasonRequest, http.StatusRequestEntityTooLarge, "the segment exceeds max_length")
				return false
			}
			if m := p.room(h, int64(k)); m < int64(k) {
				// No room for more than m: what it holds stays, and the rest
				// waits at the producer until there is (§41.9).
				keep(buf[:m])
				p.Log.Info("the retained pool is full; the producer keeps the rest for now", "key", h.key, "held", h.Len())
				w.Header().Set("Retry-After", acceptedRetryAfter)
				answer(http.StatusNoContent, h.Len())
				return true
			}
			keep(buf[:k])
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			// The producer went away: what came stays, and it asks where
			// the segment stands when it is back.
			return false
		}
	}

	n = h.Len()
	if strings.TrimSpace(r.Header.Get(storage.HdrUploadComplete)) != "?1" {
		answer(http.StatusNoContent, n)
		return true
	}
	if v := r.Header.Get(storage.HdrUploadLength); v != "" {
		if l, err := strconv.ParseInt(v, 10, 64); err != nil || l != n {
			answer(http.StatusConflict, n)
			return false
		}
	}
	ended := r.Trailer.Get(storage.HdrDateEnded)
	if ended == "" {
		ended = r.Header.Get(storage.HdrDateEnded)
	}
	t, _ := time.Parse(time.RFC3339Nano, ended)
	h.close(t)
	answer(http.StatusAccepted, n)

	return true
}

// room charges the pool for k more bytes of a segment, past what it
// reserved.
func (p *Proxy) room(h *held, k int64) int64 {
	h.mu.Lock()
	over := h.n + k - h.charged
	h.mu.Unlock()
	if over <= 0 {
		return k
	}
	// A part's worth at a time, so a growing segment asks seldom; what
	// the pool has left when it has less.
	got := p.retained.grow(h.producer, max(over, int64(minReserve)))
	h.mu.Lock()
	h.charged += got
	h.mu.Unlock()
	if got >= over {
		return k
	}

	return k - (over - got)
}

// headHeld answers where a segment the satellite holds stands; one it
// does not hold is passed on to the node.
func (p *Proxy) headHeld(w http.ResponseWriter, caller, lamina pdid.Id) bool {
	h := p.heldOf(lamina)
	if h == nil || h.producer != caller {
		return false
	}
	select {
	case <-h.ready:
	default:
		return false
	}
	if h.err != nil {
		return false
	}
	n, closed, _ := h.state()
	h.ack(n)
	w.Header().Set(upload.HdrAccepted, strconv.FormatInt(n, 10))
	w.Header().Set(storage.HdrUploadOffset, strconv.FormatInt(n, 10))
	if closed {
		w.Header().Set(storage.HdrUploadComplete, "?1")
	} else {
		w.Header().Set(storage.HdrUploadComplete, "?0")
	}
	w.WriteHeader(http.StatusOK)

	return true
}

// ---- delivery -------------------------------------------------------

// nodeRouter sends a delivery's requests to the node each candidate
// names, at the address the CP gives the satellite for it (§41.3).
type nodeRouter struct {
	ctx    context.Context
	routes *routes
}

func (r nodeRouter) Route(cand *api.Candidate) (string, bool, error) {
	node, err := pdid.From(cand.GetNodeId())
	if err != nil {
		return "", false, err
	}
	base, err := r.routes.base(r.ctx, node)

	return base, false, err
}

func (nodeRouter) Prepare(*http.Request, bool, *upload.Request) {}
func (nodeRouter) Sent(bool)                                    {}
func (nodeRouter) Failed(error)                                 {}
func (nodeRouter) Succeeded()                                   {}
func (nodeRouter) DirectFailed()                                {}

// limits are a handoff's timeouts, within reason.
func limits(hd *api.Handoff) (resume, abandon, retryCap time.Duration, retries int) {
	clamp := func(ms int64, def, lo, hi time.Duration) time.Duration {
		d := time.Duration(ms) * time.Millisecond
		if d <= 0 {
			return def
		}

		return min(max(d, lo), hi)
	}
	resume = clamp(hd.GetResumeTimeoutMs(), 2*time.Minute, time.Second, time.Hour)
	abandon = clamp(hd.GetAbandonTimeoutMs(), 5*time.Minute, time.Second, time.Hour)
	retryCap = clamp(hd.GetRetryAfterCapMs(), 2*time.Minute, time.Second, time.Hour)
	retries = int(hd.GetPlacementRetries())
	if retries <= 0 || retries > 10 {
		retries = 3
	}

	return
}

// deliver uploads a held segment to its candidates, as the producer would
// have (§13), in its name: the tokens are the producer's, and the CP takes
// the satellite's calls for the producers assigned to it. Rounds that fail
// are tried again after a backoff, until abandon_timeout passes without
// any byte reaching a node; then the segment is given up and reported
// LOST, as a producer reports one it gives up.
func (p *Proxy) deliver(h *held) {
	base := p.ctx
	if base == nil {
		base = context.Background()
	}
	ctx, cancel := context.WithCancel(base)
	h.mu.Lock()
	h.cancel = cancel
	done := h.done
	h.mu.Unlock()
	defer cancel()
	if done {
		// Let go before its delivery began.
		return
	}

	resume, abandon, retryCap, retries := limits(h.handoff)
	eng := &upload.Uploader{
		Cfg:     upload.Config{ResumeTimeout: resume, RetryAfterCap: retryCap, PlacementRetries: retries, Client: p.Client},
		Laminae: p.Laminae,
		Log:     p.Log.With("producer", h.producer.String()),
		Router:  nodeRouter{ctx: ctx, routes: p.routes},
	}
	eng.Cfg.Defaults()
	al := h.handoff.GetAllocation()
	var backoff time.Duration
	lastProgress, read := time.Now(), int64(0)
	for {
		res := eng.Upload(ctx, al, h)
		if res.Stored {
			p.finish(h, "delivered")
			p.delivered.Add(1)
			p.Log.Info("delivered", "key", h.key, "bytes", h.Len(), "attempts", res.Attempts)
			return
		}
		if ctx.Err() != nil {
			return
		}
		if res.Allocation != nil {
			al = res.Allocation
		}
		h.mu.Lock()
		if h.read > read {
			read, lastProgress = h.read, time.Now()
		}
		h.mu.Unlock()
		if errors.Is(res.Err, upload.ErrExpired) {
			// The attempts outlived their TTL unrenewed. A producer would
			// ask for its slot again; the satellite asks for the next
			// candidate, and gives the segment up when there is none.
			rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			next, err := p.Laminae.Reallocate(rctx, api.LaminaReallocateRequest_builder{Ref: api.LaminaRef_builder{Id: h.lamina.Bytes()}.Build()}.Build())
			cancel()
			if err != nil {
				p.giveUp(ctx, eng, h, res.Err)
				return
			}
			al = next
		}
		if time.Since(lastProgress) > abandon {
			p.giveUp(ctx, eng, h, res.Err)
			return
		}
		if backoff == 0 {
			backoff = time.Second
		} else {
			backoff = min(2*backoff, 30*time.Second)
		}
		p.Log.Warn("not delivered yet; keeping the segment", "key", h.key, "err", errText(res.Err), "retry_in", backoff.String())
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
	}
}

// giveUp reports a held segment LOST, in the producer's name, and lets it
// go (§13, §41.9).
func (p *Proxy) giveUp(ctx context.Context, eng *upload.Uploader, h *held, why error) {
	h.mu.Lock()
	done := h.done
	h.mu.Unlock()
	if done {
		return
	}
	reason := "the satellite could not deliver what it accepted"
	if why != nil {
		reason += ": " + why.Error()
	}
	p.Log.Warn("given up", "key", h.key, "bytes", h.Len(), "why", reason)
	if eng != nil {
		eng.GiveUp(context.WithoutCancel(ctx), h.lamina.Bytes(), h.key, reason)
	}
	p.givenUp.Add(1)
	p.finish(h, reason)
}

// finish lets a held segment go: delivered, given up, or abandoned.
func (p *Proxy) finish(h *held, why string) {
	h.mu.Lock()
	if h.done {
		h.mu.Unlock()
		return
	}
	h.done = true
	charged := h.charged
	cancel := h.cancel
	h.mu.Unlock()
	h.abort(errors.New(why))
	if cancel != nil {
		cancel()
	}
	p.heldMu.Lock()
	if p.held[h.lamina] == h {
		delete(p.held, h.lamina)
	}
	p.heldMu.Unlock()
	p.retained.free(h.producer, charged)
}

// sweep lets go the segments whose producer stopped sending before they
// ended (§41.9): silent for its resume_timeout, after which the producer
// gives the segment up itself. What it was never told the satellite holds
// is still the producer's, and goes with nothing said; what a node took
// the start of, the node makes an incomplete lamina of by its abandon rule
// (§15); and the rest, which nobody has, is reported LOST.
func (p *Proxy) sweep(ctx context.Context) {
	p.heldMu.Lock()
	hs := make([]*held, 0, len(p.held))
	for _, h := range p.held {
		hs = append(hs, h)
	}
	p.heldMu.Unlock()
	for _, h := range hs {
		select {
		case <-h.ready:
		default:
			continue
		}
		if h.err != nil {
			continue
		}
		resume, _, _, _ := limits(h.handoff)
		h.mu.Lock()
		quiet := !h.closed && !h.done && time.Since(h.touched) > resume+5*time.Second
		acked, read := h.acked, h.read
		h.mu.Unlock()
		if !quiet {
			continue
		}
		switch {
		case acked == 0:
			p.Log.Info("the producer went on without the satellite", "key", h.key)
			p.finish(h, "the producer went on without the satellite")
		case read > 0:
			p.Log.Warn("the producer stopped sending; the node keeps what it has", "key", h.key, "held", h.Len())
			p.givenUp.Add(1)
			p.finish(h, "the producer stopped sending")
		default:
			eng := &upload.Uploader{Laminae: p.Laminae, Log: p.Log}
			p.giveUp(ctx, eng, h, errors.New("the producer stopped sending before any node took the segment"))
		}
	}
}

// stop reports every segment still held LOST as the satellite stops: what
// it accepted goes with the process (§41.9). Bounded, so a satellite that
// cannot reach the CP still stops; the CP finds what is not reported.
func (p *Proxy) stop(ctx context.Context) {
	p.heldMu.Lock()
	hs := make([]*held, 0, len(p.held))
	for _, h := range p.held {
		hs = append(hs, h)
	}
	p.heldMu.Unlock()
	eng := &upload.Uploader{Laminae: p.Laminae, Log: p.Log}
	done := make(chan struct{}, len(hs))
	n := 0
	for _, h := range hs {
		select {
		case <-h.ready:
		default:
			continue
		}
		h.mu.Lock()
		acked, fin := h.acked, h.done
		h.mu.Unlock()
		if h.err != nil || fin {
			continue
		}
		if acked == 0 {
			p.finish(h, "the satellite stopped")
			continue
		}
		n++
		go func(h *held) {
			defer func() { done <- struct{}{} }()
			p.giveUp(ctx, eng, h, errors.New("the satellite stopped"))
		}(h)
	}
	for ; n > 0; n-- {
		select {
		case <-done:
		case <-ctx.Done():
			return
		}
	}
}

// holding is the laminae held now, for the heartbeat.
func (p *Proxy) holding() [][]byte {
	p.heldMu.Lock()
	defer p.heldMu.Unlock()
	out := make([][]byte, 0, len(p.held))
	for id, h := range p.held {
		select {
		case <-h.ready:
			if h.err == nil {
				out = append(out, id.Bytes())
			}
		default:
		}
	}

	return out
}
