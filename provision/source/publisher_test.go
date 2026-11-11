package source_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/provision"
	publication "github.com/lesomnus/shale/provision/source"
	catalog "github.com/lesomnus/shale/source"
	"github.com/stretchr/testify/require"
)

type commitFunc func(context.Context, provision.Deployment) (publication.Receipt, error)

func (f commitFunc) Commit(ctx context.Context, d provision.Deployment) (publication.Receipt, error) {
	return f(ctx, d)
}

type observeFunc func(context.Context, provision.Deployment, provision.BindingState) (provision.BindingState, error)

func (f observeFunc) Observe(ctx context.Context, d provision.Deployment, b provision.BindingState) (provision.BindingState, error) {
	return f(ctx, d, b)
}

type providerFunc func(context.Context) (catalog.Snapshot, error)

func (f providerFunc) Snapshot(ctx context.Context) (catalog.Snapshot, error) { return f(ctx) }
func (f providerFunc) WaitForChange(context.Context, catalog.Revision) (catalog.Revision, error) {
	return catalog.Revision{}, catalog.ErrUnavailable
}

type fixture struct {
	deployment provision.Deployment
	receipt    publication.Receipt
	initial    catalog.Snapshot
	next       catalog.Snapshot
	publisher  *publication.Publisher
}

func setup(t *testing.T) *fixture {
	t.Helper()
	id := func(domain pdid.Domain) string { return pdid.New(domain).String() }
	scope := catalog.Scope{TenantID: id(1), SiteID: id(19), SetID: id(7), ProducerID: id(22), NetworkID: id(26)}
	revision := catalog.Revision{AssignmentID: id(28), Generation: 1, Catalog: 1}
	profile := catalog.Profile{Codec: "h264", Width: 160, Height: 90, FPS: 10, MaxBitrate: 1_000_000}
	unrelated := catalog.Spec{BindingID: id(29), SourceID: id(8), Revision: 1, Alias: "unrelated", Input: "rtsp://192.0.2.1/first", Profile: profile}
	initial := catalog.Snapshot{Revision: revision, Scope: scope, Sources: []catalog.Spec{unrelated}}
	next := initial.Clone()
	next.Revision.Catalog = 2
	added := catalog.Spec{BindingID: id(29), SourceID: id(8), Revision: 2, Alias: "door", Input: "rtsp://192.0.2.2/main", Profile: profile}
	next.Sources = append(next.Sources, added)
	d := provision.Deployment{OperationID: "publish-door-v1", Request: provision.Request{
		ID: "enroll-door-v1", BindingID: "front-door", Alias: "door", Scope: camera.EnrollmentScope{
			TenantID: scope.TenantID, SiteID: scope.SiteID, SetID: scope.SetID, ProducerID: scope.ProducerID, NetworkID: scope.NetworkID,
		},
	}}
	receipt := publication.Receipt{Binding: provision.BindingState{BindingID: d.Request.BindingID, Alias: added.Alias, SourceID: added.SourceID, DeploymentID: d.OperationID, DeployedAt: time.Now().UTC()}, Revision: next.Revision}
	memory, err := catalog.NewMemory(initial)
	require.NoError(t, err)
	f := &fixture{deployment: d, receipt: receipt, initial: initial, next: next}
	f.publisher = &publication.Publisher{
		AssignmentID: revision.AssignmentID, Scope: scope, Memory: memory,
		Committer: commitFunc(func(context.Context, provision.Deployment) (publication.Receipt, error) { return f.receipt, nil }),
		Provider:  providerFunc(func(context.Context) (catalog.Snapshot, error) { return f.next, nil }),
	}
	return f
}

func accepted(t *testing.T, p *publication.Publisher) catalog.Snapshot {
	t.Helper()
	snapshot, err := p.Memory.Snapshot(context.Background())
	require.NoError(t, err)
	return snapshot
}

func TestPublisherCommitsBeforeReadingTheFullProducerCatalog(t *testing.T) {
	f := setup(t)
	var order []string
	f.receipt.Binding.Ingest, f.receipt.Binding.Live, f.receipt.Binding.Committed = true, true, true
	f.receipt.Binding.CommittedAt = time.Now()
	f.publisher.Committer = commitFunc(func(_ context.Context, d provision.Deployment) (publication.Receipt, error) {
		require.Equal(t, f.deployment.OperationID, d.OperationID)
		require.Equal(t, f.initial, accepted(t, f.publisher), "no uncommitted source is visible")
		order = append(order, "commit")
		return f.receipt, nil
	})
	f.publisher.Provider = providerFunc(func(context.Context) (catalog.Snapshot, error) {
		require.Equal(t, []string{"commit"}, order)
		order = append(order, "snapshot")
		return f.next, nil
	})
	out, err := f.publisher.Publish(context.Background(), f.deployment)
	require.NoError(t, err)
	require.Equal(t, []string{"commit", "snapshot"}, order)
	require.Equal(t, f.receipt.Binding.SourceID, out.SourceID)
	require.False(t, out.Ingest)
	require.False(t, out.Live)
	require.False(t, out.Committed)
	require.True(t, out.CommittedAt.IsZero())
	require.Equal(t, f.next, accepted(t, f.publisher), "unrelated Sources remain in the complete snapshot")
	f.next.Sources[0].Alias = "changed-after-read"
	require.Equal(t, "unrelated", accepted(t, f.publisher).Sources[0].Alias)
}

func TestPublisherCommitFailureNeverExposesTheSource(t *testing.T) {
	f := setup(t)
	written := errors.New("rtsp://camera:reserved-secret@192.0.2.2/main")
	f.publisher.Committer = commitFunc(func(context.Context, provision.Deployment) (publication.Receipt, error) {
		return publication.Receipt{}, written
	})
	f.publisher.Provider = providerFunc(func(context.Context) (catalog.Snapshot, error) {
		t.Fatal("a failed commit must not refresh the catalog")
		return catalog.Snapshot{}, nil
	})
	_, err := f.publisher.Publish(context.Background(), f.deployment)
	require.ErrorIs(t, err, written)
	require.Nil(t, errors.Unwrap(err), "raw transport diagnostics are not exposed through Unwrap")
	require.NotContains(t, fmt.Sprintf("%v %+v %#v", err, err, err), "reserved-secret")
	require.Equal(t, f.initial, accepted(t, f.publisher))
}

func TestPublisherDurableSuccessSurvivesNotificationFailure(t *testing.T) {
	for _, failure := range []struct {
		name string
		code publication.RefreshFailure
		edit func(*fixture, context.CancelFunc)
	}{
		{"unavailable", publication.RefreshUnavailable, func(f *fixture, _ context.CancelFunc) {
			f.publisher.Provider = providerFunc(func(context.Context) (catalog.Snapshot, error) {
				return catalog.Snapshot{}, errors.New("failed rtsp://camera:encoded%40secret@192.0.2.2/main")
			})
		}},
		{"canceled-after-commit", publication.RefreshCanceled, func(f *fixture, cancel context.CancelFunc) {
			f.publisher.Committer = commitFunc(func(context.Context, provision.Deployment) (publication.Receipt, error) {
				cancel()
				return f.receipt, nil
			})
		}},
		{"stale", publication.RefreshStale, func(f *fixture, _ context.CancelFunc) { f.next = f.initial }},
		{"foreign-scope", publication.RefreshConflict, func(f *fixture, _ context.CancelFunc) { f.next.Scope.TenantID = pdid.New(1).String() }},
		{"foreign-assignment", publication.RefreshConflict, func(f *fixture, _ context.CancelFunc) { f.next.Revision.AssignmentID = pdid.New(28).String() }},
		{"same-revision-missing-source", publication.RefreshConflict, func(f *fixture, _ context.CancelFunc) { f.next.Sources = f.next.Sources[:1] }},
		{"new-generation-same-catalog-missing-source", publication.RefreshConflict, func(f *fixture, _ context.CancelFunc) {
			f.next.Revision.Generation++
			f.next.Sources = f.next.Sources[:1]
		}},
		{"invalid-input", publication.RefreshInvalid, func(f *fixture, _ context.CancelFunc) {
			f.next.Sources[1].Input = "rtsp://camera:secret@192.0.2.2/main"
		}},
	} {
		t.Run(failure.name, func(t *testing.T) {
			f := setup(t)
			full := f.next.Clone()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			failure.edit(f, cancel)
			var failures []publication.RefreshFailure
			f.publisher.OnRefreshFailure = func(code publication.RefreshFailure) { failures = append(failures, code) }
			out, err := f.publisher.Publish(ctx, f.deployment)
			require.NoError(t, err, "the committed mutation must not be retried because its local hint failed")
			require.Equal(t, f.receipt.Binding, out)
			require.Equal(t, []publication.RefreshFailure{failure.code}, failures)
			require.Equal(t, f.initial, accepted(t, f.publisher))
			f.publisher.Provider = providerFunc(func(context.Context) (catalog.Snapshot, error) { return full, nil })
			require.NoError(t, f.publisher.Refresh(context.Background()))
			require.Equal(t, full, accepted(t, f.publisher), "resnapshot repairs dropped notifications")
		})
	}
}

func TestPublisherAdvancedCatalogCanAlreadyStopTheCommittedSource(t *testing.T) {
	f := setup(t)
	f.next.Revision.Catalog++
	f.next.Sources = f.next.Sources[:1]
	_, err := f.publisher.Publish(context.Background(), f.deployment)
	require.NoError(t, err)
	require.Equal(t, f.next, accepted(t, f.publisher), "a newer central stop must not be undone from a single receipt")
}

func TestPublisherLostCommitResponseReplaysTheSameOperation(t *testing.T) {
	f := setup(t)
	attempts, writes := 0, 0
	committed := map[string]publication.Receipt{}
	f.publisher.Committer = commitFunc(func(_ context.Context, d provision.Deployment) (publication.Receipt, error) {
		attempts++
		if _, exists := committed[d.OperationID]; !exists {
			writes++
			committed[d.OperationID] = f.receipt
			return publication.Receipt{}, errors.New("commit response lost")
		}
		return committed[d.OperationID], nil
	})
	_, err := f.publisher.Publish(context.Background(), f.deployment)
	require.Error(t, err)
	require.Equal(t, f.initial, accepted(t, f.publisher))
	out, err := f.publisher.Publish(context.Background(), f.deployment)
	require.NoError(t, err)
	require.Equal(t, f.receipt.Binding, out)
	require.Equal(t, 2, attempts)
	require.Equal(t, 1, writes)
	require.Equal(t, f.next, accepted(t, f.publisher))
	require.NoError(t, f.publisher.Refresh(context.Background()))
	require.Equal(t, 2, attempts, "refresh is not another camera publication")
}

func TestPublisherRejectsForeignDeploymentAndBrokenReceipts(t *testing.T) {
	for _, name := range []string{"foreign-deployment", "bad-source-domain", "different-source", "different-logical-key", "different-alias", "different-operation", "missing-time", "wrong-assignment", "unpaired-cache"} {
		t.Run(name, func(t *testing.T) {
			f := setup(t)
			switch name {
			case "foreign-deployment":
				f.deployment.Request.Scope.ProducerID = pdid.New(22).String()
			case "bad-source-domain":
				f.receipt.Binding.SourceID = pdid.New(29).String()
			case "different-source":
				f.deployment.Previous.SourceID = pdid.New(8).String()
			case "different-logical-key":
				f.receipt.Binding.BindingID = "another-role"
			case "different-alias":
				f.receipt.Binding.Alias = "another-camera"
			case "different-operation":
				f.receipt.Binding.DeploymentID = "another-operation"
			case "missing-time":
				f.receipt.Binding.DeployedAt = time.Time{}
			case "wrong-assignment":
				f.receipt.Revision.AssignmentID = pdid.New(28).String()
			case "unpaired-cache":
				f.publisher.Provider = nil
			}
			f.publisher.Provider = providerFunc(func(context.Context) (catalog.Snapshot, error) {
				t.Fatal("invalid publication must not read or expose a catalog")
				return catalog.Snapshot{}, nil
			})
			if name == "unpaired-cache" {
				f.publisher.Provider = nil
			}
			calls := 0
			f.publisher.Committer = commitFunc(func(context.Context, provision.Deployment) (publication.Receipt, error) {
				calls++
				return f.receipt, nil
			})
			_, err := f.publisher.Publish(context.Background(), f.deployment)
			require.Error(t, err)
			if name == "foreign-deployment" || name == "unpaired-cache" {
				require.Zero(t, calls)
			} else {
				require.Equal(t, 1, calls)
			}
			require.Equal(t, f.initial, accepted(t, f.publisher))
		})
	}
}

func TestPublisherIndependentCommitAndObservedMediaAreSeparate(t *testing.T) {
	f := setup(t)
	f.publisher.Provider, f.publisher.Memory = nil, nil
	b, err := f.publisher.Publish(context.Background(), f.deployment)
	require.NoError(t, err)
	_, err = f.publisher.Observe(context.Background(), f.deployment, b)
	require.ErrorIs(t, err, provision.ErrNotReady)
	require.ErrorIs(t, f.publisher.Refresh(context.Background()), catalog.ErrUnavailable)
	calls := 0
	f.publisher.Observer = observeFunc(func(_ context.Context, d provision.Deployment, got provision.BindingState) (provision.BindingState, error) {
		calls++
		require.Equal(t, f.deployment, d)
		require.Equal(t, b, got)
		got.Ingest, got.Live, got.Committed = true, true, true
		got.CommittedAt = time.Now()
		return got, nil
	})
	observed, err := f.publisher.Observe(context.Background(), f.deployment, b)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	require.True(t, observed.Ingest && observed.Live && observed.Committed)
	f.publisher.Observer = observeFunc(func(_ context.Context, _ provision.Deployment, got provision.BindingState) (provision.BindingState, error) {
		got.Ingest = true
		return got, provision.ErrNotReady
	})
	partial, err := f.publisher.Observe(context.Background(), f.deployment, b)
	require.ErrorIs(t, err, provision.ErrNotReady)
	require.True(t, partial.Ingest, "readiness progress is preserved without attesting Active")
	require.False(t, partial.Live || partial.Committed)
	f.publisher.Observer = observeFunc(func(_ context.Context, _ provision.Deployment, got provision.BindingState) (provision.BindingState, error) {
		got.SourceID = pdid.New(8).String()
		return got, nil
	})
	_, err = f.publisher.Observe(context.Background(), f.deployment, b)
	require.ErrorIs(t, err, catalog.ErrConflict)
}

func TestPublisherRefreshRedactsTransportDiagnostics(t *testing.T) {
	f := setup(t)
	f.publisher.Provider = providerFunc(func(context.Context) (catalog.Snapshot, error) {
		return catalog.Snapshot{}, errors.New("rtsp://camera:password%40secret@192.0.2.2/main")
	})
	err := f.publisher.Refresh(context.Background())
	require.ErrorIs(t, err, catalog.ErrUnavailable)
	require.Nil(t, errors.Unwrap(err))
	require.NotContains(t, fmt.Sprintf("%v %+v %#v", err, err, err), "password%40secret")
	require.Equal(t, f.initial, accepted(t, f.publisher))
}

func TestPublisherForeignSnapshotCannotInitializeEmptyMemory(t *testing.T) {
	for _, field := range []string{"assignment", "tenant", "site", "set", "producer", "network"} {
		t.Run(field, func(t *testing.T) {
			f := setup(t)
			f.publisher.Memory = &catalog.Memory{}
			switch field {
			case "assignment":
				f.next.Revision.AssignmentID = pdid.New(28).String()
			case "tenant":
				f.next.Scope.TenantID = pdid.New(1).String()
			case "site":
				f.next.Scope.SiteID = pdid.New(19).String()
			case "set":
				f.next.Scope.SetID = pdid.New(7).String()
			case "producer":
				f.next.Scope.ProducerID = pdid.New(22).String()
			case "network":
				f.next.Scope.NetworkID = pdid.New(26).String()
			}
			require.ErrorIs(t, f.publisher.Refresh(context.Background()), catalog.ErrConflict)
			_, err := f.publisher.Memory.Snapshot(context.Background())
			require.ErrorIs(t, err, catalog.ErrUnavailable)
		})
	}
}

func TestPublisherRejectsUnsafeDeploymentBeforeCommit(t *testing.T) {
	for _, name := range []string{"incomplete-reference", "tenant", "site", "set", "producer", "network", "purpose", "not-recoverable", "userinfo", "auth-query", "fragment"} {
		t.Run(name, func(t *testing.T) {
			f := setup(t)
			f.deployment.Credential = camera.CredentialRef{Provider: "cp", ID: "door-stream", Version: "1", Purpose: camera.AuthStreaming, Scope: f.deployment.Request.Scope, Recoverable: true}
			f.deployment.Profile.StreamURI = "rtsp://192.0.2.2/main?profile=main"
			switch name {
			case "incomplete-reference":
				f.deployment.Credential.Version = ""
			case "tenant":
				f.deployment.Credential.Scope.TenantID = pdid.New(1).String()
			case "site":
				f.deployment.Credential.Scope.SiteID = ""
			case "set":
				f.deployment.Credential.Scope.SetID = pdid.New(7).String()
			case "producer":
				f.deployment.Credential.Scope.ProducerID = pdid.New(22).String()
			case "network":
				f.deployment.Credential.Scope.NetworkID = pdid.New(26).String()
			case "purpose":
				f.deployment.Credential.Purpose = camera.AuthManagement
			case "not-recoverable":
				f.deployment.Credential.Recoverable = false
			case "userinfo":
				f.deployment.Profile.StreamURI = "rtsp://operator:publication-private-secret@192.0.2.2/main"
			case "auth-query":
				f.deployment.Profile.StreamURI = "rtsp://192.0.2.2/main?opaque_auth=publication-private-secret%2frest"
			case "fragment":
				f.deployment.Profile.StreamURI = "rtsp://192.0.2.2/main#publication-private-secret"
			}
			commits, reads := 0, 0
			f.publisher.Committer = commitFunc(func(context.Context, provision.Deployment) (publication.Receipt, error) {
				commits++
				return f.receipt, nil
			})
			f.publisher.Provider = providerFunc(func(context.Context) (catalog.Snapshot, error) {
				reads++
				return f.next, nil
			})
			_, err := f.publisher.Publish(context.Background(), f.deployment)
			require.Error(t, err)
			require.Zero(t, commits, "a durable implementation must not receive an unsafe credential or URI")
			require.Zero(t, reads)
			require.NotContains(t, fmt.Sprintf("%v %+v %#v", err, err, err), "publication-private-secret")
			require.Equal(t, f.initial, accepted(t, f.publisher))
		})
	}
}

func TestPublisherAllowsUnauthenticatedOrExactRecoverableStreamingReference(t *testing.T) {
	for _, authenticated := range []bool{false, true} {
		f := setup(t)
		f.deployment.Profile.StreamURI = "rtsp://192.0.2.2/main?profile=main"
		if authenticated {
			f.deployment.Credential = camera.CredentialRef{Provider: "cp", ID: "door-stream", Version: "1", Purpose: camera.AuthStreaming, Scope: f.deployment.Request.Scope, Recoverable: true}
		}
		_, err := f.publisher.Publish(context.Background(), f.deployment)
		require.NoError(t, err)
		require.Equal(t, f.next, accepted(t, f.publisher))
	}
}
