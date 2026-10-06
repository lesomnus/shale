package identity

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/roster/rstr"
)

// An app's exchange token (§33.8) is introspected as Shale in the tenant
// it was issued in and answers the app's holder, with its alias; one
// roster does not know, or one asked about in another tenant, is refused.
func TestApp(t *testing.T) {
	ctx := context.Background()
	f := newFakeRoster(t, "hday")
	portal := f.addHolder("portal")
	f.exchanged["rd_portal"] = portal
	s := f.open(t)
	tenant := pdid.Id(f.tenant.GetId())

	p, err := s.App(ctx, tenant, "rd_portal")
	require.NoError(t, err)
	require.Equal(t, pdid.Id(portal), p.Id)
	require.Equal(t, "portal", p.Alias)

	_, err = s.App(ctx, tenant, "rd_nobody")
	require.ErrorIs(t, err, ErrRefused)

	other := f.addTenant("other")
	f.nominate("other", true)
	f.exchanged["rd_elsewhere"] = f.addHolderIn(other, "portal")
	_, err = s.App(ctx, tenant, "rd_elsewhere")
	require.ErrorIs(t, err, ErrRefused, "a token of another tenant is no token here")
}

// The sync stream (§33.8): somebody is told once per standing; a repeat of
// the same state wakes nobody and a later one does; good standing is not
// news; after a break the stream is dialed again and what it says is told
// again, since nothing is replayed; and a roster that refuses the stream
// is ErrNoSync at once.
func TestWatch(t *testing.T) {
	f := newFakeRoster(t, "hday")
	s := f.open(t)
	alice, bob := f.addHolder("alice"), f.addHolder("bob")
	t1 := time.Unix(1_700_000_000, 0).UTC()
	event := func(holder []byte, invalidated, disabled time.Time) *rstr.SyncEvent {
		b := rstr.SyncEvent_builder{Holder: holder, Tenant: f.tenant.GetId()}
		if !invalidated.IsZero() {
			b.DateInvalidated = timestamppb.New(invalidated)
		}
		if !disabled.IsZero() {
			b.DateDisabled = timestamppb.New(disabled)
		}
		return b.Build()
	}

	var mu sync.Mutex
	var heard []Standing
	told := func() []Standing {
		mu.Lock()
		defer mu.Unlock()
		return append([]Standing(nil), heard...)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- s.Watch(ctx, func(_ context.Context, v Standing) {
			mu.Lock()
			heard = append(heard, v)
			mu.Unlock()
		})
	}()

	f.sync <- event(alice, t1, time.Time{})
	f.sync <- event(alice, t1, time.Time{})
	f.sync <- event(bob, time.Time{}, time.Time{})
	f.sync <- event(alice, t1, t1.Add(time.Minute))
	require.Eventually(t, func() bool { return len(told()) == 2 }, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, pdid.Id(alice), told()[0].Holder)
	require.Equal(t, t1, told()[0].Since())
	require.Equal(t, t1.Add(time.Minute), told()[1].Since(), "suspended after")
	require.Equal(t, pdid.Id(f.tenant.GetId()), told()[0].Tenant)

	// A break, and the same state told again on the next stream.
	f.sync <- nil
	require.Eventually(t, func() bool { return f.syncs.Load() == 2 }, 5*time.Second, 10*time.Millisecond)
	f.sync <- event(alice, t1, t1.Add(time.Minute))
	require.Eventually(t, func() bool { return len(told()) == 3 }, 5*time.Second, 10*time.Millisecond)
	cancel()
	require.NoError(t, <-done)

	// Refused, it says so rather than dialing forever.
	f.noSync.Store(true)
	err := s.Watch(context.Background(), func(context.Context, Standing) {})
	require.True(t, errors.Is(err, ErrNoSync), "%v", err)
}
