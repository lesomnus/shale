package satellite

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/token"
	"github.com/lesomnus/shale/internal/upload"
)

// laminae is the CP's LaminaService as a delivery calls it: what it was
// told, and no reallocation.
type laminae struct {
	api.LaminaServiceClient

	mu       sync.Mutex
	attempts []string
	failures []string
}

func (l *laminae) ReportAttempt(_ context.Context, req *api.LaminaReportAttemptRequest, _ ...grpc.CallOption) (*api.Attempt, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.attempts = append(l.attempts, string(req.GetAttempt().GetId()))

	return &api.Attempt{}, nil
}

func (l *laminae) Reallocate(context.Context, *api.LaminaReallocateRequest, ...grpc.CallOption) (*api.Allocation, error) {
	return nil, errors.New("every eligible sink has been tried")
}

func (l *laminae) ReportFailure(_ context.Context, req *api.LaminaReportFailureRequest, _ ...grpc.CallOption) (*api.Lamina, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.failures = append(l.failures, req.GetReason())

	return &api.Lamina{}, nil
}

func (l *laminae) reported() ([]string, []string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]string(nil), l.attempts...), append([]string(nil), l.failures...)
}

// accepting makes the rig take `retain: accepted` with a retained pool of
// budget, and answers the CP's side: what it was asked to record.
func (r *rig) accepting(budget int64) (*laminae, *[]pdid.Id) {
	l := &laminae{}
	var mu sync.Mutex
	recorded := &[]pdid.Id{}
	r.proxy.Laminae = l
	r.proxy.retained = newRetained(budget, 0)
	r.proxy.record = func(_ context.Context, lamina pdid.Id) error {
		mu.Lock()
		defer mu.Unlock()
		*recorded = append(*recorded, lamina)
		return nil
	}
	// What a test leaves held is let go before its nodes stop, which wait
	// for the deliveries reading it.
	r.t.Cleanup(func() {
		r.proxy.heldMu.Lock()
		hs := make([]*held, 0, len(r.proxy.held))
		for _, h := range r.proxy.held {
			hs = append(hs, h)
		}
		r.proxy.heldMu.Unlock()
		for _, h := range hs {
			r.proxy.finish(h, "the test is over")
		}
	})

	return l, recorded
}

// allocation is a lamina's allocation with a candidate on each node, every
// token issued to producer p for it; edit changes a candidate's claims.
func (r *rig) allocation(p *producer, lamina pdid.Id, nodes []*node, edit func(i int, c *api.TokenClaims)) *api.Allocation {
	var cands []*api.Candidate
	for i, n := range nodes {
		at := pdid.New(10)
		k := key + "-" + strconv.Itoa(i)
		tok := r.token(n.id, k, p.id, func(c *api.TokenClaims) {
			c.SetAttemptId(at.Bytes())
			c.SetMaxLength(64 << 20)
			c.SetRecord(api.LaminaRecord_builder{LaminaId: lamina.Bytes(), AttemptId: at.Bytes(), CaptureEpoch: "e1"}.Build())
			if edit != nil {
				edit(i, c)
			}
		})
		cands = append(cands, api.Candidate_builder{AttemptId: at.Bytes(), NodeId: n.id.Bytes(), Token: tok, LaminaKey: k}.Build())
	}

	return api.Allocation_builder{LaminaId: lamina.Bytes(), Candidates: cands, LaminaKey: cands[0].GetLaminaKey(), CaptureEpoch: "e1", SizeHint: 1 << 20}.Build()
}

// handoffPut is one request of a segment under `retain: accepted`, to the
// first candidate's key.
func (r *rig) handoffPut(p *producer, al *api.Allocation, offset int64, body []byte, complete bool, total int64, hd *api.Handoff) *http.Response {
	r.t.Helper()
	cand := al.GetCandidates()[0]
	// A part of a live segment is chunked, as a producer sends it; the
	// request that completes one has its length.
	var rd io.Reader = bytes.NewReader(body)
	if !complete {
		rd = io.MultiReader(rd)
	}
	req, err := http.NewRequest(http.MethodPut, r.srv.URL+"/"+cand.GetLaminaKey(), rd)
	require.NoError(r.t, err)
	req.Header.Set("Authorization", token.Scheme+" "+cand.GetToken())
	req.Header.Set("Upload-Offset", strconv.FormatInt(offset, 10))
	req.Header.Set(upload.HdrRetain, upload.RetainAccepted)
	req.Header.Set("Shale-Date-Started", "2026-11-15T00:00:00Z")
	if offset == 0 {
		v, err := upload.EncodeHandoff(hd)
		require.NoError(r.t, err)
		req.Header.Set(upload.HdrHandoff, v)
	}
	if complete {
		req.Header.Set("Upload-Complete", "?1")
		req.Header.Set("Upload-Length", strconv.FormatInt(total, 10))
		req.Header.Set("Shale-Date-Ended", "2026-11-15T00:02:00Z")
	} else {
		req.Header.Set("Upload-Complete", "?0")
	}
	resp, err := p.client.Do(req)
	require.NoError(r.t, err)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	return resp
}

// §41.9: the satellite takes a segment's bytes and says how much it holds
// at every request, 202 once it holds all of it; then it delivers the
// segment itself, failing over from a node that refuses it to the next
// candidate, in the producer's name, with the candidate's own token and
// nothing of the handoff. The pool is free again once the node has it.
func TestAcceptedTakesAndDelivers(t *testing.T) {
	refusing, taking := newNode(t), newNode(t)
	refusing.answer = func(w http.ResponseWriter) { w.WriteHeader(http.StatusForbidden) }
	r := newRig(t, 64<<20, 64<<10, refusing, taking)
	cp, recorded := r.accepting(32 << 20)
	p := r.producer(true)
	lamina := pdid.New(9)
	al := r.allocation(p, lamina, []*node{refusing, taking}, nil)
	hd := api.Handoff_builder{Allocation: al, ResumeTimeoutMs: 5000, AbandonTimeoutMs: 30000}.Build()

	first := bytes.Repeat([]byte("a"), 300_000)
	resp := r.handoffPut(p, al, 0, first, false, 0, hd)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	require.Equal(t, "300000", resp.Header.Get(upload.HdrAccepted), "it holds the first part")
	require.Equal(t, []pdid.Id{lamina}, *recorded, "the CP recorded it as the lamina's holder first")
	used, _, _ := r.proxy.retained.usage()
	require.Equal(t, int64(1<<20), used, "the segment reserved its size hint")

	// Asked where it stands, it says what it holds.
	req, err := http.NewRequest(http.MethodHead, r.srv.URL+"/"+al.GetLaminaKey(), nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", token.Scheme+" "+al.GetCandidates()[0].GetToken())
	req.Header.Set(upload.HdrRetain, upload.RetainAccepted)
	resp, err = p.client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "300000", resp.Header.Get(upload.HdrAccepted))
	require.Equal(t, "?0", resp.Header.Get("Upload-Complete"))

	// A request from elsewhere than where it stands is told where.
	resp = r.handoffPut(p, al, 100, []byte("x"), false, 0, hd)
	require.Equal(t, http.StatusConflict, resp.StatusCode)
	require.Equal(t, "300000", resp.Header.Get(upload.HdrAccepted))

	rest := bytes.Repeat([]byte("b"), 2_000_000)
	resp = r.handoffPut(p, al, 300_000, rest, true, 2_300_000, hd)
	require.Equal(t, http.StatusAccepted, resp.StatusCode, "it holds the whole segment")
	require.Equal(t, "2300000", resp.Header.Get(upload.HdrAccepted))

	// Delivered: the first candidate refused it, the second has it whole.
	require.Eventually(t, func() bool { return len(taking.requests()) > 0 && r.proxy.delivered.Load() == 1 }, 10*time.Second, 20*time.Millisecond)
	got := taking.requests()[0]
	require.Equal(t, append(first, rest...), got.body)
	require.Equal(t, "/"+al.GetCandidates()[1].GetLaminaKey(), got.path)
	require.Equal(t, token.Scheme+" "+al.GetCandidates()[1].GetToken(), got.header.Get("Authorization"), "the candidate's own token")
	require.Equal(t, "2026-11-15T00:00:00Z", got.header.Get("Shale-Date-Started"))
	require.Equal(t, "2026-11-15T00:02:00Z", got.header.Get("Shale-Date-Ended"))
	for _, n := range []*node{refusing, taking} {
		for _, v := range n.requests() {
			require.Empty(t, v.header.Get(upload.HdrHandoff), "a node never sees the other candidates' tokens")
			require.Empty(t, v.header.Get(upload.HdrRetain))
		}
	}
	attempts, failures := cp.reported()
	require.Equal(t, []string{string(al.GetCandidates()[0].GetAttemptId())}, attempts, "the refusal reported, as the producer would")
	require.Empty(t, failures)
	used, _, _ = r.proxy.retained.usage()
	require.Zero(t, used, "delivered, it leaves the pool")
	require.Empty(t, r.proxy.holding())
}

// The handoff is checked as a request's token is (§41.3): every
// candidate's token must be one its node would take, issued to this
// producer, for this lamina. Refused, nothing is held, and the producer
// goes on as under `committed`.
func TestAcceptedHandoffChecked(t *testing.T) {
	n1, n2 := newNode(t), newNode(t)
	r := newRig(t, 64<<20, 64<<10, n1, n2)
	_, recorded := r.accepting(32 << 20)
	p := r.producer(true)
	other := pdid.New(DomProducer)
	for name, edit := range map[string]func(i int, c *api.TokenClaims){
		"another producer's": func(i int, c *api.TokenClaims) {
			if i == 1 {
				c.SetActor(other.Bytes())
			}
		},
		"another lamina's": func(i int, c *api.TokenClaims) {
			if i == 1 {
				c.GetRecord().SetLaminaId(pdid.New(9).Bytes())
			}
		},
		"a read token": func(i int, c *api.TokenClaims) {
			if i == 1 {
				c.SetOp(api.TokenOp_TOKEN_OP_GET)
			}
		},
	} {
		al := r.allocation(p, pdid.New(9), []*node{n1, n2}, edit)
		resp := r.handoffPut(p, al, 0, []byte("abc"), false, 0, api.Handoff_builder{Allocation: al}.Build())
		require.Equal(t, http.StatusForbidden, resp.StatusCode, name)
		require.Equal(t, ReasonToken, resp.Header.Get(HdrSatellite), name)
		require.Empty(t, resp.Header.Get(upload.HdrAccepted), name)
	}
	require.Empty(t, *recorded)
	require.Empty(t, r.proxy.holding())
	require.Empty(t, n1.requests())

	// A continuation of a segment it does not hold: it says it holds
	// nothing, so the producer cuts what it let go.
	al := r.allocation(p, pdid.New(9), []*node{n1}, nil)
	resp := r.handoffPut(p, al, 5000, []byte("abc"), false, 0, nil)
	require.Equal(t, http.StatusConflict, resp.StatusCode)
	require.Equal(t, "0", resp.Header.Get(upload.HdrAccepted))
}

// What the satellite does not take it passes through: the node's answer,
// marked with why (§41.9). `satellite.accept: false` takes nothing; a pool
// with no room for the segment takes nothing of it. The node is sent
// nothing of the handoff either way.
func TestAcceptedPassedThrough(t *testing.T) {
	n := newNode(t)
	r := newRig(t, 64<<20, 64<<10, n)
	p := r.producer(true)

	r.proxy.Refuse = true
	al := r.allocation(p, pdid.New(9), []*node{n}, nil)
	resp := r.handoffPut(p, al, 0, []byte("abc"), true, 3, api.Handoff_builder{Allocation: al}.Build())
	require.Equal(t, http.StatusCreated, resp.StatusCode, "the node's answer")
	require.Equal(t, upload.PassRefused, resp.Header.Get(upload.HdrPass))
	require.Empty(t, resp.Header.Get(upload.HdrAccepted))
	require.Len(t, n.requests(), 1)
	require.Empty(t, n.requests()[0].header.Get(upload.HdrHandoff))

	r.proxy.Refuse = false
	_, recorded := r.accepting(512 << 10)
	al = r.allocation(p, pdid.New(9), []*node{n}, nil)
	resp = r.handoffPut(p, al, 0, []byte("abc"), true, 3, api.Handoff_builder{Allocation: al}.Build())
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.Equal(t, upload.PassFull, resp.Header.Get(upload.HdrPass), "a segment reserves a part at least, more than the pool")
	require.Empty(t, *recorded, "nothing taken, nothing recorded")
	require.Equal(t, int64(2), r.proxy.passed.Load())

	// The CP not recording it is no taking either.
	r.accepting(32 << 20)
	r.proxy.record = func(context.Context, pdid.Id) error { return errors.New("unreachable") }
	al = r.allocation(p, pdid.New(9), []*node{n}, nil)
	resp = r.handoffPut(p, al, 0, []byte("abc"), true, 3, api.Handoff_builder{Allocation: al}.Build())
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.Equal(t, upload.PassUnrecorded, resp.Header.Get(upload.HdrPass))
	used, _, _ := r.proxy.retained.usage()
	require.Zero(t, used, "the reservation given back")
}

// A segment the satellite holds the start of, and has no room to grow:
// it keeps what it holds, says so with Retry-After, and refuses more 503
// until there is room, evicting nothing (§41.9).
func TestAcceptedFullKeepsWhatItHolds(t *testing.T) {
	n := newNode(t)
	hold := make(chan struct{})
	n.mu.Lock()
	n.hold = hold
	n.mu.Unlock()
	r := newRig(t, 64<<20, 64<<10, n)
	r.accepting(1536 << 10)
	p := r.producer(true)
	al := r.allocation(p, pdid.New(9), []*node{n}, nil)
	hd := api.Handoff_builder{Allocation: al, ResumeTimeoutMs: 30000, AbandonTimeoutMs: 30000}.Build()

	resp := r.handoffPut(p, al, 0, bytes.Repeat([]byte("a"), 2<<20), false, 0, hd)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	require.Equal(t, "5", resp.Header.Get("Retry-After"))
	got, err := strconv.ParseInt(resp.Header.Get(upload.HdrAccepted), 10, 64)
	require.NoError(t, err)
	require.Positive(t, got)
	require.Equal(t, int64(1536<<10), got, "as much as the pool holds")
	used, _, budget := r.proxy.retained.usage()
	require.Equal(t, budget, used)

	resp = r.handoffPut(p, al, got, []byte("more"), false, 0, hd)
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	require.Equal(t, ReasonBusy, resp.Header.Get(HdrSatellite))
	require.Equal(t, strconv.FormatInt(got, 10), resp.Header.Get(upload.HdrAccepted), "what it holds stays")

	// Another producer's new segment passes through rather than wait.
	q := r.producer(true)
	other := r.allocation(q, pdid.New(9), []*node{n}, nil)
	close(hold)
	resp = r.handoffPut(q, other, 0, []byte("abc"), true, 3, api.Handoff_builder{Allocation: other}.Build())
	require.Equal(t, upload.PassFull, resp.Header.Get(upload.HdrPass))
	h := r.proxy.heldOf(mustLamina(t, al))
	require.Equal(t, got, h.Len())

	// Let go, its delivery ends, and the pool is free.
	r.proxy.finish(h, "the test is over")
	used, _, _ = r.proxy.retained.usage()
	require.Zero(t, used)
}

func mustLamina(t *testing.T, al *api.Allocation) pdid.Id {
	id, err := pdid.From(al.GetLaminaId())
	require.NoError(t, err)

	return id
}

// A segment no candidate takes is tried again until abandon_timeout passes
// without any byte reaching a node, then given up: reported LOST in the
// producer's name, as the producer reports one (§13, §41.9).
func TestAcceptedGivenUp(t *testing.T) {
	n := newNode(t)
	n.answer = func(w http.ResponseWriter) { w.WriteHeader(http.StatusForbidden) }
	r := newRig(t, 64<<20, 64<<10, n)
	cp, _ := r.accepting(32 << 20)
	p := r.producer(true)
	al := r.allocation(p, pdid.New(9), []*node{n}, nil)
	hd := api.Handoff_builder{Allocation: al, ResumeTimeoutMs: 1000, AbandonTimeoutMs: 1000}.Build()
	resp := r.handoffPut(p, al, 0, []byte("abcdef"), true, 6, hd)
	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	require.Eventually(t, func() bool { _, f := cp.reported(); return len(f) == 1 }, 15*time.Second, 50*time.Millisecond)
	_, failures := cp.reported()
	require.Contains(t, failures[0], "could not deliver")
	require.Equal(t, int64(1), r.proxy.givenUp.Load())
	used, _, _ := r.proxy.retained.usage()
	require.Zero(t, used)
}
