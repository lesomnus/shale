package producer

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/token"
)

// renewer is a LaminaService that answers Renew only, with a fresh token
// for the attempt asked about, or the error it is told to.
type renewer struct {
	api.LaminaServiceClient
	t   *testing.T
	key token.Key
	ttl time.Duration

	mu    sync.Mutex
	err   error
	calls int
}

func (r *renewer) sign(iat time.Time, ttl time.Duration) string {
	tok, err := r.key.Sign(api.TokenClaims_builder{Iat: timestamppb.New(iat), Exp: timestamppb.New(iat.Add(ttl))}.Build())
	require.NoError(r.t, err)

	return tok
}

func (r *renewer) Renew(ctx context.Context, req *api.LaminaRenewRequest, _ ...grpc.CallOption) (*api.Allocation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.err != nil {
		return nil, r.err
	}

	return api.Allocation_builder{Candidates: []*api.Candidate{
		api.Candidate_builder{AttemptId: []byte("other"), Token: "not ours"}.Build(),
		api.Candidate_builder{AttemptId: req.GetAttempt().GetId(), Token: r.sign(time.Now(), r.ttl)}.Build(),
	}}.Build(), nil
}

func (r *renewer) n() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.calls
}

func newRenewer(t *testing.T, ttl time.Duration) *renewer {
	k, err := token.Generate("k")
	require.NoError(t, err)

	return &renewer{t: t, key: k, ttl: ttl}
}

func leaseOf(r *renewer, tok string) *lease {
	al := api.Allocation_builder{LaminaId: []byte("lamina")}.Build()
	cand := api.Candidate_builder{AttemptId: []byte("attempt"), Token: tok, LaminaKey: "laminae/x"}.Build()

	return newLease(r, slog.Default(), al, cand)
}

// A token is renewed two thirds of the way through its life, and not
// before: the attempt is the same, the token is the one for it.
func TestLeaseRenewsAtTwoThirds(t *testing.T) {
	r := newRenewer(t, time.Hour)
	now := time.Now()

	fresh := r.sign(now.Add(-time.Minute), 3*time.Minute)
	l := leaseOf(r, fresh)
	tok, err := l.token(context.Background())
	require.NoError(t, err)
	require.Equal(t, fresh, tok, "a third of the way through: kept")
	require.Zero(t, r.n())

	due := r.sign(now.Add(-2*time.Minute-time.Second), 3*time.Minute)
	l = leaseOf(r, due)
	tok, err = l.token(context.Background())
	require.NoError(t, err)
	require.NotEqual(t, due, tok, "past two thirds: renewed")
	require.Equal(t, 1, r.n())
	c, err := token.Parse(tok)
	require.NoError(t, err)
	require.WithinDuration(t, now.Add(time.Hour), c.GetExp().AsTime(), 5*time.Second, "the attempt's own token, not another candidate's")

	// Renewed once is good for another two thirds of an hour.
	_, err = l.token(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, r.n())
}

// An attempt the CP will not renew, because it is past its TTL or no
// longer allocated, ends the lease: the upload needs a new attempt. A CP
// that does not answer leaves the token as it is.
func TestLeaseRefusedOrUnreachable(t *testing.T) {
	r := newRenewer(t, time.Hour)
	stale := r.sign(time.Now().Add(-time.Hour), 30*time.Minute)

	r.err = status.Error(codes.Unavailable, "no CP")
	l := leaseOf(r, stale)
	tok, err := l.token(context.Background())
	require.NoError(t, err, "unreachable is not refused")
	require.Equal(t, stale, tok)

	r.err = status.Error(codes.FailedPrecondition, "attempt expired")
	_, err = l.token(context.Background())
	require.ErrorIs(t, err, errExpired)
	n := r.n()
	_, err = l.token(context.Background())
	require.ErrorIs(t, err, errExpired)
	require.Equal(t, n, r.n(), "refused once is refused: not asked again")
}

// The keeper renews without a request asking, so an attempt whose one
// request outlives the TTL is still allocated when it ends.
func TestLeaseKeeps(t *testing.T) {
	r := newRenewer(t, 600*time.Millisecond)
	l := leaseOf(r, r.sign(time.Now(), 600*time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { l.keep(ctx); close(done) }()

	time.Sleep(2 * time.Second)
	require.GreaterOrEqual(t, r.n(), 3, "renewed every 400 ms for two seconds")
	l.mu.Lock()
	exp := l.exp
	l.mu.Unlock()
	require.True(t, exp.After(time.Now()), "the token in hand has not expired")

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("the keeper did not stop with its context")
	}
}
