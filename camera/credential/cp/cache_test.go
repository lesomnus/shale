package cp

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestStreamingCacheRequiresMediaAndPreservesAuthenticatedExpiry(t *testing.T) {
	f := newProviderFixture(t)
	f.response.SetCacheUntil(timestamppb.New(f.now.Add(2 * time.Minute)))
	cacheOptions := CacheOptions{Directory: filepath.Join(t.TempDir(), "private"), KeyMaterial: bytes.Repeat([]byte{42}, 64), MaxAge: 2 * time.Minute, Now: func() time.Time { return f.now }}
	cache, err := NewStreamingCache(cacheOptions)
	require.NoError(t, err)
	sourceID, err := id(f.response.GetDelivery().GetSourceId(), 8, false)
	require.NoError(t, err)
	assignment, err := id(f.response.GetDelivery().GetAssignmentId(), 28, false)
	require.NoError(t, err)
	source := camera.SourceAuthContext{SourceID: sourceID, Revision: 3, Scope: f.scope, AssignmentID: assignment, AssignmentGeneration: 1}
	upstream := true
	denied := false
	resolver := func(_ context.Context, r *api.CameraCredentialResolveRequest) (*api.CameraCredentialLease, error) {
		require.EqualValues(t, 120, r.GetCacheSeconds())
		if denied {
			return nil, status.Error(codes.PermissionDenied, "private remote diagnostics")
		}
		if !upstream {
			return nil, status.Error(codes.Unavailable, "private remote diagnostics")
		}
		return proto.Clone(f.response).(*api.CameraCredentialLease), nil
	}
	provider, err := NewResolver(resolver, Options{Role: Producer, Scope: f.scope, SourceID: sourceID, SourceRevision: 3, AssignmentID: assignment, AssignmentGeneration: 1, Cache: cache, Now: cacheOptions.Now})
	require.NoError(t, err)
	lease, err := provider.Acquire(t.Context(), f.request())
	require.NoError(t, err)
	lease.Close()
	files, err := os.ReadDir(cacheOptions.Directory)
	require.NoError(t, err)
	require.Empty(t, files, "a successful resolve is not applied-media evidence")
	upstream = false
	_, err = provider.Acquire(t.Context(), f.request())
	require.ErrorIs(t, err, ErrUnavailable)
	proof := AppliedProof{Source: source, Ref: f.ref, AssignmentID: assignment, AssignmentGeneration: 1, CaptureEpoch: "actual-current-capture", FirstMediaAt: f.now, LastMediaAt: f.now.Add(time.Second), Frames: 30, Keyframes: 1}
	bad := proof
	bad.Source.Revision++
	require.ErrorIs(t, cache.RecordApplied(t.Context(), bad), ErrInvalidLease)
	require.NoError(t, cache.RecordApplied(t.Context(), proof))
	files, err = os.ReadDir(cacheOptions.Directory)
	require.NoError(t, err)
	require.Len(t, files, 1)
	path := filepath.Join(cacheOptions.Directory, files[0].Name())
	ciphertext, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NotContains(t, string(ciphertext), "synthetic-password")
	require.NotContains(t, string(ciphertext), "synthetic-user")
	st, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0600), st.Mode().Perm())
	// A new process with the same machine key can reopen only an already
	// applied entry. An approved replacement key must go back to the CP.
	reopened, err := NewStreamingCache(cacheOptions)
	require.NoError(t, err)
	provider.opts.Cache = reopened
	lease, err = provider.Acquire(t.Context(), f.request())
	require.NoError(t, err)
	newGeneration := source
	newGeneration.AssignmentGeneration++
	_, err = provider.ForSource(newGeneration).Acquire(t.Context(), f.request())
	require.ErrorIs(t, err, ErrUnavailable, "a previous assignment's cache grant cannot authorize a replacement generation")
	require.NoError(t, lease.Use(t.Context(), func(m camera.CredentialMaterial) error {
		require.Equal(t, "synthetic-password\xfe\x00", m.Password)
		return nil
	}))
	lease.Close()
	wrongKey := cacheOptions
	wrongKey.KeyMaterial = bytes.Repeat([]byte{43}, 64)
	wrong, err := NewStreamingCache(wrongKey)
	require.NoError(t, err)
	_, err = wrong.recover(t.Context(), source, f.ref, camera.AccessRead, 30*time.Second)
	require.ErrorIs(t, err, ErrUnavailable)
	for _, verb := range []string{"%v", "%+v", "%#v", "%d", "%x"} {
		require.Equal(t, cacheMarker, fmt.Sprintf(verb, cache))
		require.Equal(t, cacheMarker, fmt.Sprintf(verb, *cache))
		require.Equal(t, cacheMarker, fmt.Sprintf(verb, cacheOptions))
	}
	// File timestamps are not the grant clock. Neither an outage nor touching
	// the file extends the authenticated cache-until embedded in ciphertext.
	f.now = f.now.Add(121 * time.Second)
	require.NoError(t, os.Chtimes(path, f.now, f.now))
	_, err = provider.Acquire(t.Context(), f.request())
	require.ErrorIs(t, err, ErrUnavailable)
}

func TestStreamingCacheDenialTamperAndStopPurge(t *testing.T) {
	f := newProviderFixture(t)
	f.response.SetCacheUntil(timestamppb.New(f.now.Add(time.Minute)))
	cache, err := NewStreamingCache(CacheOptions{Directory: filepath.Join(t.TempDir(), "private"), KeyMaterial: bytes.Repeat([]byte{1}, 32), MaxAge: time.Minute, Now: func() time.Time { return f.now }})
	require.NoError(t, err)
	sourceID, _ := id(f.response.GetDelivery().GetSourceId(), 8, false)
	assignment, _ := id(f.response.GetDelivery().GetAssignmentId(), 28, false)
	source := camera.SourceAuthContext{SourceID: sourceID, Revision: 3, Scope: f.scope, AssignmentID: assignment, AssignmentGeneration: 1}
	cache.remember(source, f.ref, f.response)
	proof := AppliedProof{Source: source, Ref: f.ref, AssignmentID: assignment, AssignmentGeneration: 1, CaptureEpoch: "capture", FirstMediaAt: f.now, LastMediaAt: f.now.Add(time.Second), Frames: 2, Keyframes: 1}
	require.NoError(t, cache.RecordApplied(t.Context(), proof))
	name := filepath.Join(cache.dir, cacheName(source, f.ref))
	sealed, err := os.ReadFile(name)
	require.NoError(t, err)
	sealed[len(sealed)-1] ^= 1
	require.NoError(t, os.WriteFile(name, sealed, 0600))
	_, err = cache.recover(t.Context(), source, f.ref, camera.AccessRead, time.Second)
	require.ErrorIs(t, err, ErrUnavailable)
	cache.remember(source, f.ref, f.response)
	require.NoError(t, cache.RecordApplied(t.Context(), proof))
	p, err := NewResolver(func(context.Context, *api.CameraCredentialResolveRequest) (*api.CameraCredentialLease, error) {
		return nil, status.Error(codes.PermissionDenied, "denied")
	}, Options{Role: Producer, Scope: f.scope, SourceID: sourceID, SourceRevision: 3, AssignmentID: assignment, AssignmentGeneration: 1, Cache: cache, Now: cache.now})
	require.NoError(t, err)
	_, err = p.Acquire(t.Context(), f.request())
	require.ErrorIs(t, err, ErrRejected)
	_, err = os.Stat(name)
	require.True(t, os.IsNotExist(err), "online revocation must discard a previously usable cache entry")
	cache.remember(source, f.ref, f.response)
	require.NoError(t, cache.RecordApplied(t.Context(), proof))
	require.NoError(t, cache.InvalidateSource(sourceID))
	_, err = cache.recover(t.Context(), source, f.ref, camera.AccessRead, time.Second)
	require.ErrorIs(t, err, ErrUnavailable)
	_, err = NewResolver(func(context.Context, *api.CameraCredentialResolveRequest) (*api.CameraCredentialLease, error) {
		return nil, nil
	}, Options{Role: Worker, Scope: f.scope, Cache: cache})
	require.ErrorIs(t, err, camera.ErrInvalid)
	_, err = NewResolver(func(context.Context, *api.CameraCredentialResolveRequest) (*api.CameraCredentialLease, error) {
		return nil, nil
	}, Options{Role: Producer, Scope: f.scope, Cache: &StreamingCache{}})
	require.ErrorIs(t, err, camera.ErrInvalid)
	require.ErrorIs(t, (&StreamingCache{}).RecordApplied(t.Context(), proof), ErrUnavailable)
}

func TestStreamingCachePrunesAuthenticatedExpiryAtCapacity(t *testing.T) {
	f := newProviderFixture(t)
	cache, err := NewStreamingCache(CacheOptions{Directory: filepath.Join(t.TempDir(), "cache"), KeyMaterial: bytes.Repeat([]byte{9}, 32), MaxAge: time.Minute, MaxEntries: 1, Now: func() time.Time { return f.now }})
	require.NoError(t, err)
	sourceID, _ := id(f.response.GetDelivery().GetSourceId(), 8, false)
	assignment, _ := id(f.response.GetDelivery().GetAssignmentId(), 28, false)
	source := camera.SourceAuthContext{SourceID: sourceID, Revision: 3, Scope: f.scope, AssignmentID: assignment, AssignmentGeneration: 1}
	proof := AppliedProof{Source: source, Ref: f.ref, AssignmentID: assignment, AssignmentGeneration: 1, CaptureEpoch: "first", FirstMediaAt: f.now, LastMediaAt: f.now.Add(time.Second), Frames: 2, Keyframes: 1}
	f.response.SetCacheUntil(timestamppb.New(f.now.Add(time.Minute)))
	cache.remember(source, f.ref, f.response)
	require.NoError(t, cache.RecordApplied(t.Context(), proof))
	f.now = f.now.Add(61 * time.Second)
	f.ref.Version = "replacement-version"
	f.response.SetCacheUntil(timestamppb.New(f.now.Add(time.Minute)))
	proof.Ref = f.ref
	proof.CaptureEpoch = "second"
	proof.FirstMediaAt = f.now
	proof.LastMediaAt = f.now.Add(time.Second)
	cache.remember(source, f.ref, f.response)
	require.NoError(t, cache.RecordApplied(t.Context(), proof), "expired records after a crash/ref change cannot permanently exhaust the cap")
	files, err := os.ReadDir(cache.dir)
	require.NoError(t, err)
	require.Len(t, files, 1)
}
