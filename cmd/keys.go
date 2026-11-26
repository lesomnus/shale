package cmd

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/payday/auth"
	"github.com/lesomnus/payday/frame"
	"github.com/lesomnus/payday/grpcx"
	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/internal/identity"
	"github.com/lesomnus/shale/server/core"
)

// A person's key for Shale (§33.1): an `rt_` roster mints a person for the
// app Shale (`ApiKey.app`), on its account page or through a terminal's
// sign-in, which acts as them here, narrowed to some of Shale's methods
// they hold, with an end if it has one. The tenant API takes it as
// `Authorization: Bearer rt_…`. roster is asked who it is as Shale
// (`TokenService/Introspect`, [identity.Store.Key]); the call is then held
// to the key's methods by payday's interceptor and to what the person may
// do by the gate, as a session's is.

// authorityKey is the [auth.Identity.Authority] of a caller a person's key
// admitted: what tells the resolver that the holder it names may be
// somebody arriving for the first time, whose rows are made as at a first
// sign-in.
const authorityKey = "roster-key"

// keysRetry is how long a failure to ask roster about a key is
// remembered, as for a session's standing.
const keysRetry = standingRetry

// keysMax bounds what is remembered: a key is asked about by whoever holds
// a string that starts `rt_`, so what is remembered of strings that are no
// key is not left to grow with them.
const keysMax = 4096

// keys is what roster said of the keys presented here, by a digest of the
// key (the key itself is never kept): who it acts as, what it allows, when
// it ends, or that it is none.
//
// # How long
//
// For `auth.operators.ttl`, 30 s by default: the time a grant taken away at
// roster, or a session's person suspended there, already takes to land
// here, so a key ended at roster -- on roster's account page, or by Shale
// at a later page -- is refused within the same window and no later. Never
// beyond the key's own end, which is refused here whatever was said. And
// dropped at once, with every stream a key of theirs opened, for a person
// roster's sync stream says stopped being in good standing (signed out
// everywhere, suspended, erased); where there is no stream (the roster in
// this process, a key not allowed `/roster.SyncService/Watch`), within the
// TTL, since roster refuses a suspended person's keys itself. A key with a
// stream open is asked about again each time this runs out, and a key
// roster no longer knows -- ended there -- cuts that key's streams
// ([keys.streams]).
//
// What is kept is **per key**: the methods are the key's, and two keys of
// one person are each held to their own. Nothing here is kept per person
// but the channel that ends their streams.
type keys struct {
	ask func(ctx context.Context, token string) (auth.Identity, error)
	ttl time.Duration
	now func() time.Time

	mu    sync.Mutex
	known map[[sha256.Size]byte]keyEntry
	// stops closes, per person, when roster says they stopped being in
	// good standing: what a stream opened with a key of theirs is cut by
	// (payday's `auth.Identity.Changed`).
	stops map[pdid.Id]chan struct{}
	// gen counts the drops, so an answer roster gave before one is not
	// kept after it.
	gen uint64
	// open are the keys with a stream open, by digest: each is asked
	// about again whenever what roster said of it runs out, for as long
	// as one of its streams is open.
	open map[[sha256.Size]byte]*keyStreams
}

// keyStreams are the streams one key has open.
type keyStreams struct {
	n int
	// gone closes when roster says the key is none any more: its streams
	// are cut.
	gone chan struct{}
	// done closes when the last of them closes: nothing more is asked.
	done chan struct{}
}

type keyEntry struct {
	at  time.Time
	id  auth.Identity
	err error
}

func newKeys(id *identity.Store, ttl time.Duration) *keys {
	if ttl <= 0 {
		ttl = identity.OperatorsTtl
	}

	return &keys{ask: id.Key, ttl: ttl, now: time.Now, known: map[[sha256.Size]byte]keyEntry{}, stops: map[pdid.Id]chan struct{}{}, open: map[[sha256.Size]byte]*keyStreams{}}
}

// until is how long an entry is believed.
func (k *keys) until(e keyEntry) time.Time {
	keep := k.ttl
	if e.err != nil && !errors.Is(e.err, identity.ErrNoKey) {
		keep = keysRetry
	}
	t := e.at.Add(keep)
	if ends := e.id.DateExpired; e.err == nil && !ends.IsZero() && ends.Before(t) {
		t = ends
	}

	return t
}

// lookup is who a key is, from what roster said while that stands.
func (k *keys) lookup(ctx context.Context, token string) (auth.Identity, error) {
	sum := sha256.Sum256([]byte(token))
	now := k.now()
	k.mu.Lock()
	e, ok := k.known[sum]
	gen := k.gen
	k.mu.Unlock()
	if !ok || !now.Before(k.until(e)) {
		id, err := k.ask(ctx, token)
		e = keyEntry{at: now, id: id, err: err}
		// A call that ended while roster was asked keeps nothing: its
		// failure is its own and not roster's.
		if err == nil || ctx.Err() == nil {
			k.mu.Lock()
			if k.gen == gen {
				k.keep(sum, e, now)
			}
			k.mu.Unlock()
		}
	}
	if e.err != nil {
		return auth.Identity{}, e.err
	}
	if ends := e.id.DateExpired; !ends.IsZero() && !now.Before(ends) {
		return auth.Identity{}, fmt.Errorf("%w: it ended at %s", identity.ErrNoKey, ends.UTC().Format(time.RFC3339))
	}
	holder, err := pdid.Parse(e.id.Id)
	if err != nil {
		return auth.Identity{}, fmt.Errorf("%w: roster named nobody: %w", identity.ErrNoKey, err)
	}
	id := e.id
	k.mu.Lock()
	stop, ok := k.stops[holder]
	if !ok {
		stop = make(chan struct{})
		k.stops[holder] = stop
	}
	k.mu.Unlock()
	id.Changed = stop

	return id, nil
}

// keep remembers an answer; the lock is held.
func (k *keys) keep(sum [sha256.Size]byte, e keyEntry, now time.Time) {
	if len(k.known) >= keysMax {
		for s, v := range k.known {
			if !now.Before(k.until(v)) {
				delete(k.known, s)
			}
		}
		if len(k.known) >= keysMax {
			// Everything is still believed: forget it all, which costs
			// roster one question per key in use and widens nothing.
			clear(k.known)
		}
	}
	k.known[sum] = e
}

// drop forgets every key of a person, and ends what they opened with one.
func (k *keys) drop(holder pdid.Id) {
	who := holder.String()
	k.mu.Lock()
	defer k.mu.Unlock()
	k.gen++
	for s, v := range k.known {
		if v.err == nil && v.id.Id == who {
			delete(k.known, s)
		}
	}
	if stop, ok := k.stops[holder]; ok {
		close(stop)
		delete(k.stops, holder)
	}
}

// keyOf is the person's key a call carries, told apart by its prefix
// before roster is asked anything: empty for a call with none, which goes
// the way it went before there was this -- a session, a certificate, the
// plain header, or nobody.
func keyOf(md metadata.MD) string {
	for _, v := range md.Get(auth.Header) {
		rest, ok := strings.CutPrefix(v, auth.BearerScheme+" ")
		if !ok {
			continue
		}
		if token := strings.TrimSpace(rest); strings.HasPrefix(token, identity.PrefixPersonKey) {
			return token
		}
	}

	return ""
}

// handler reads a person's key, the way payday's `auth.Bearer` reads a
// token: a key that is none is refused as the one answer `no such token`,
// whichever way it was none, and a roster that could not be asked is
// Unavailable. The key is in no answer and no log.
func (k *keys) handler() auth.Handler {
	return auth.HandlerFunc(func(ctx context.Context) (auth.Identity, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		token := keyOf(md)
		if token == "" {
			return auth.Identity{}, auth.ErrNoCredential
		}
		id, err := k.lookup(ctx, token)
		if err != nil {
			if errors.Is(err, auth.ErrUnavailable) {
				return auth.Identity{}, fmt.Errorf("%s: %w", auth.BearerScheme, err)
			}

			return auth.Identity{}, fmt.Errorf("%s: %w", auth.BearerScheme, auth.ErrUnknownToken)
		}
		id.Method, id.Authority = auth.MethodBearer, authorityKey

		return id, nil
	})
}

// ends puts a key's end on a call it admitted (core.WithCredentialEnds), so
// that what the call signs -- a view token, a read token, an export ticket
// -- lives no longer than the key. A stream needs nothing of this: payday
// cuts it at the key's end (`auth.InterceptorStream`, by
// `Identity.DateExpired`).
func (k *keys) ends() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		f, ok := frame.From(ctx)
		if !ok || f.Authentication.Method != auth.MethodBearer {
			return handler(ctx, req)
		}
		md, _ := metadata.FromIncomingContext(ctx)
		token := keyOf(md)
		if token == "" {
			return handler(ctx, req)
		}
		// Asked a moment ago by the handler, so this is what it was told.
		id, err := k.lookup(ctx, token)
		switch {
		case errors.Is(err, auth.ErrUnavailable):
			return nil, status.Error(codes.Unavailable, err.Error())
		case err != nil:
			return nil, status.Error(codes.Unauthenticated, auth.BearerScheme+": "+auth.ErrUnknownToken.Error())
		}
		if !id.DateExpired.IsZero() {
			ctx = core.WithCredentialEnds(ctx, id.DateExpired)
		}

		return handler(ctx, req)
	}
}

// errKeyEnded cuts a stream whose key roster no longer knows.
var errKeyEnded = errors.New("the key this stream was opened with was ended at roster")

// streams cuts a stream opened with a key when roster says the key is
// none any more -- ended at roster, on its account page or by Shale --
// within the TTL, as a stream of a person stopped is cut (`drop`), and
// that key's streams alone: another key of the same person goes on.
//
// While a key has a stream open, [keys.follow] asks roster about it again
// each time what it said runs out. A roster that cannot be asked then
// cuts nothing: the stream is cut when roster answers that the key is
// none, or at the key's end, which payday sees to. Nothing is asked about
// a key with no stream open.
func (k *keys) streams() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := ss.Context()
		f, ok := frame.From(ctx)
		if !ok || f.Authentication.Method != auth.MethodBearer {
			return handler(srv, ss)
		}
		md, _ := metadata.FromIncomingContext(ctx)
		token := keyOf(md)
		if token == "" {
			return handler(srv, ss)
		}
		gone, release := k.opened(token)
		defer release()
		ctx, cancel := context.WithCancelCause(ctx)
		defer cancel(nil)
		go func() {
			select {
			case <-gone:
				cancel(errKeyEnded)
			case <-ctx.Done():
			}
		}()
		err := handler(srv, grpcx.StreamWithContext(ss, ctx))
		if errors.Is(context.Cause(ctx), errKeyEnded) {
			return status.Error(codes.Unauthenticated, errKeyEnded.Error())
		}

		return err
	}
}

// opened counts a stream of a key, following the key from its first, and
// answers what closes when roster says it is none and the stream's end.
func (k *keys) opened(token string) (<-chan struct{}, func()) {
	sum := sha256.Sum256([]byte(token))
	k.mu.Lock()
	v, ok := k.open[sum]
	if !ok {
		v = &keyStreams{gone: make(chan struct{}), done: make(chan struct{})}
		k.open[sum] = v
		go k.follow(sum, token, v)
	}
	v.n++
	k.mu.Unlock()

	return v.gone, func() {
		k.mu.Lock()
		defer k.mu.Unlock()
		v.n--
		if v.n == 0 {
			close(v.done)
			if k.open[sum] == v {
				delete(k.open, sum)
			}
		}
	}
}

// followWait is how long roster is given each time a key with a stream
// open is asked about again.
const followWait = 10 * time.Second

// follow asks roster about a key again each time what it said runs out,
// until the key's last stream closes; `no such token` cuts its streams.
func (k *keys) follow(sum [sha256.Size]byte, token string, v *keyStreams) {
	for {
		k.mu.Lock()
		e, ok := k.known[sum]
		k.mu.Unlock()
		wait := time.Duration(0)
		if ok {
			wait = k.until(e).Sub(k.now())
		}
		wait = max(wait, 50*time.Millisecond)
		t := time.NewTimer(wait)
		select {
		case <-v.done:
			t.Stop()
			return
		case <-t.C:
		}
		ctx, cancel := context.WithTimeout(context.Background(), followWait)
		_, err := k.lookup(ctx, token)
		cancel()
		if err != nil && !errors.Is(err, auth.ErrUnavailable) {
			k.mu.Lock()
			close(v.gone)
			if k.open[sum] == v {
				delete(k.open, sum)
			}
			k.mu.Unlock()

			return
		}
	}
}
