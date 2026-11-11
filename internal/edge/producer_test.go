package edge

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	credentialcp "github.com/lesomnus/shale/camera/credential/cp"
	"github.com/lesomnus/shale/camera/credential/material"
	"github.com/lesomnus/shale/source"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCacheObserverBindsOnlineLeaseToActualSourceProofAndPurgesStop(t *testing.T) {
	now := time.Now()
	tenant, set, producer, network := pdid.New(1), pdid.New(7), pdid.New(22), pdid.New(26)
	sourceID, assignment, binding := pdid.New(8), pdid.New(28), pdid.New(29)
	scope := camera.EnrollmentScope{TenantID: tenant.String(), SetID: set.String(), ProducerID: producer.String(), NetworkID: network.String()}
	ref := camera.CredentialRef{Provider: "cp", ID: pdid.New(32).String(), Version: "1", Purpose: camera.AuthStreaming, Scope: scope, Recoverable: true}
	m, err := material.New("synthetic-user", "synthetic-password")
	require.NoError(t, err)
	payload, err := m.MarshalBinary()
	require.NoError(t, err)
	response := api.CameraCredentialLease_builder{Credential: api.CameraCredentialReference_builder{Provider: ref.Provider, Id: ref.ID, Version: ref.Version, Purpose: string(ref.Purpose)}.Build(), Material: payload, Expires: timestamppb.New(now.Add(30 * time.Second)), CacheUntil: timestamppb.New(now.Add(time.Minute)), Access: string(camera.AccessRead), Delivery: api.CameraCredentialDelivery_builder{TenantId: tenant.Bytes(), SetId: set.Bytes(), ProducerId: producer.Bytes(), NetworkId: network.Bytes(), DeviceId: pdid.New(27).Bytes(), BindingId: binding.Bytes(), SourceId: sourceID.Bytes(), AssignmentId: assignment.Bytes(), AssignmentGeneration: 2, SourceRevision: 3}.Build()}.Build()
	dir := filepath.Join(t.TempDir(), "private")
	cache, err := credentialcp.NewStreamingCache(credentialcp.CacheOptions{Directory: dir, KeyMaterial: bytes.Repeat([]byte{42}, 32), MaxAge: time.Minute, Now: func() time.Time { return now }})
	require.NoError(t, err)
	online := true
	provider, err := credentialcp.NewResolver(func(context.Context, *api.CameraCredentialResolveRequest) (*api.CameraCredentialLease, error) {
		if !online {
			return nil, status.Error(codes.Unavailable, "private diagnostic")
		}
		return proto.Clone(response).(*api.CameraCredentialLease), nil
	}, credentialcp.Options{Role: credentialcp.Producer, Scope: scope, Cache: cache, Now: func() time.Time { return now }})
	require.NoError(t, err)
	sourceContext := camera.SourceAuthContext{SourceID: sourceID.String(), Revision: 3, Scope: scope, AssignmentID: assignment.String(), AssignmentGeneration: 2}
	request := camera.AuthRequest{Scope: scope, Purpose: camera.AuthStreaming, ExistingRef: &ref, Requirement: camera.AuthRequirement{Access: camera.AccessRead, Recoverable: true}}
	auth := provider.ForSource(sourceContext)
	lease, err := auth.Acquire(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, lease.Close())
	observer := &cacheObserver{}
	observer.set(cache)
	proof := source.AppliedProof{ApplyTicket: source.ApplyTicket{Application: source.Application{Scope: source.Scope{TenantID: scope.TenantID, SetID: scope.SetID, ProducerID: scope.ProducerID, NetworkID: scope.NetworkID}, Revision: source.Revision{AssignmentID: assignment.String(), Generation: 2, Catalog: 4}, BindingID: binding.String(), SourceID: sourceID.String(), SpecRevision: 3}, Sequence: 2, Epoch: "current-capture-epoch"}, Media: source.MediaProgress{FirstMediaAt: now, LastMediaAt: now.Add(time.Second), Frames: 10, Keyframes: 1}, UsedCredentialRef: &source.CredentialRef{Provider: ref.Provider, ID: ref.ID, Version: ref.Version, Purpose: string(ref.Purpose)}}
	online = false
	_, err = auth.Acquire(t.Context(), request)
	require.ErrorIs(t, err, credentialcp.ErrUnavailable)
	wrong := proof
	wrong.SpecRevision++
	observer.Applied(t.Context(), wrong)
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, files, "unrelated applied revision cannot seal this credential")
	observer.Applied(t.Context(), proof)
	lease, err = auth.Acquire(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, lease.Close())
	files, err = os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, files, 1)
	sealed, err := os.ReadFile(filepath.Join(dir, files[0].Name()))
	require.NoError(t, err)
	require.NotContains(t, string(sealed), "synthetic-password")
	observer.Stopped(t.Context(), source.StoppedProof{ApplyTicket: proof.ApplyTicket, Reason: source.StopDisconnected})
	_, err = auth.Acquire(t.Context(), request)
	require.ErrorIs(t, err, credentialcp.ErrUnavailable)
	files, err = os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, files)
}
