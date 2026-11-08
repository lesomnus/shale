package edge

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/internal/producer"
	"github.com/lesomnus/shale/provision"
	publication "github.com/lesomnus/shale/provision/source"
	"github.com/lesomnus/shale/source"
	"github.com/stretchr/testify/require"
)

type brokerCatalog struct {
	mu    sync.Mutex
	value source.Snapshot
	err   error
	reads chan struct{}
}

func (p *brokerCatalog) Snapshot(ctx context.Context) (source.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return source.Snapshot{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.reads != nil {
		select {
		case p.reads <- struct{}{}:
		default:
		}
	}
	return p.value.Clone(), p.err
}
func (*brokerCatalog) WaitForChange(ctx context.Context, _ source.Revision) (source.Revision, error) {
	<-ctx.Done()
	return source.Revision{}, ctx.Err()
}
func (p *brokerCatalog) set(v source.Snapshot, err error) {
	p.mu.Lock()
	p.value = v
	p.err = err
	p.mu.Unlock()
}

func brokerFixture(t *testing.T) (*sourceBroker, *brokerCatalog, *replicaProvider, provision.Deployment, publication.Receipt) {
	t.Helper()
	scope := source.Scope{TenantID: pdid.New(1).String(), SetID: pdid.New(7).String(), ProducerID: pdid.New(22).String(), NetworkID: pdid.New(26).String()}
	v := source.Snapshot{Scope: scope, Revision: source.Revision{AssignmentID: pdid.New(28).String(), Generation: 1}}
	upstream := &brokerCatalog{value: v}
	b := newSourceBroker()
	p, err := b.bind(t.Context(), producer.SourceSession{TenantID: scope.TenantID, SetID: scope.SetID, ProducerID: scope.ProducerID}, func(context.Context, producer.SourceSession) (source.Provider, error) { return upstream, nil })
	require.NoError(t, err)
	r := p.(*replicaProvider)
	sourceID := pdid.New(8).String()
	d := provision.Deployment{OperationID: "durable-operation", Request: provision.Request{Scope: camera.EnrollmentScope{TenantID: scope.TenantID, SetID: scope.SetID, ProducerID: scope.ProducerID, NetworkID: scope.NetworkID}, BindingID: "logical-camera"}}
	receipt := publication.Receipt{Revision: v.Revision, Binding: provision.BindingState{BindingID: d.Request.BindingID, SourceID: sourceID, Alias: "entrance", DeploymentID: d.OperationID, DeployedAt: time.Now()}}
	receipt.Revision.Catalog = 1
	return b, upstream, r, d, receipt
}

type brokerCommitFunc func(context.Context, provision.Deployment) (publication.Receipt, error)

func (f brokerCommitFunc) Commit(ctx context.Context, d provision.Deployment) (publication.Receipt, error) {
	return f(ctx, d)
}

func committedCatalog(r *replicaProvider, receipt publication.Receipt) source.Snapshot {
	return source.Snapshot{Revision: receipt.Revision, Scope: source.Scope{TenantID: r.session.TenantID, SetID: r.session.SetID, ProducerID: r.session.ProducerID}, Sources: []source.Spec{{BindingID: pdid.New(29).String(), SourceID: receipt.Binding.SourceID, Revision: receipt.Revision.Catalog, Alias: receipt.Binding.Alias, Input: "rtsp://192.0.2.10/main", Profile: source.Profile{Codec: "h264", Width: 160, Height: 90, FPS: 10, MaxBitrate: 128000}, Credential: &source.CredentialRef{Provider: "cp", ID: pdid.New(32).String(), Version: "1", Purpose: "streaming"}}}}
}

func TestBrokerCommitNotifiesOnlyAfterDurableSuccessAndWakesReader(t *testing.T) {
	b, upstream, r, d, receipt := brokerFixture(t)
	initial, err := r.Snapshot(t.Context())
	require.NoError(t, err)
	upstream.reads = make(chan struct{}, 4)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	done := make(chan source.Revision, 1)
	go func() {
		revision, err := r.WaitForChange(ctx, initial.Revision)
		if err == nil {
			done <- revision
		}
	}()
	<-upstream.reads
	commit := notifyingCommitter{broker: b, commit: brokerCommitFunc(func(context.Context, provision.Deployment) (publication.Receipt, error) {
		accepted, err := r.memory.Snapshot(t.Context())
		require.NoError(t, err)
		require.Empty(t, accepted.Sources)
		v := committedCatalog(r, receipt)
		v.Scope = initial.Scope
		upstream.set(v, nil)
		return receipt, nil
	})}
	got, err := commit.Commit(t.Context(), d)
	require.NoError(t, err)
	require.Equal(t, receipt, got)
	select {
	case rev := <-done:
		require.Equal(t, receipt.Revision, rev)
	case <-ctx.Done():
		t.Fatal("committed catalog did not wake reader")
	}
	current, err := r.Snapshot(t.Context())
	require.NoError(t, err)
	require.Len(t, current.Sources, 1)
}

func TestBrokerFailureNeverReplacesAcceptedCatalogWithEmpty(t *testing.T) {
	b, upstream, r, d, receipt := brokerFixture(t)
	initial, err := r.Snapshot(t.Context())
	require.NoError(t, err)
	v := committedCatalog(r, receipt)
	v.Scope = initial.Scope
	upstream.set(v, nil)
	_, err = r.Snapshot(t.Context())
	require.NoError(t, err)
	upstream.set(source.Snapshot{}, source.ErrUnavailable)
	_, err = r.Snapshot(t.Context())
	require.ErrorIs(t, err, source.ErrUnavailable)
	commit := notifyingCommitter{broker: b, commit: brokerCommitFunc(func(context.Context, provision.Deployment) (publication.Receipt, error) { return receipt, nil })}
	got, err := commit.Commit(t.Context(), d)
	require.NoError(t, err)
	require.Equal(t, receipt, got, "lost local hint cannot undo durable publication")
	accepted, err := r.memory.Snapshot(t.Context())
	require.NoError(t, err)
	require.Equal(t, v, accepted)
	failed := errors.New("durable failure")
	commit.commit = brokerCommitFunc(func(context.Context, provision.Deployment) (publication.Receipt, error) {
		return publication.Receipt{}, failed
	})
	_, err = commit.Commit(t.Context(), d)
	require.ErrorIs(t, err, failed)
}

func TestBrokerCannotSeedFromWorkerReceiptAndRejectsForeignProducer(t *testing.T) {
	b, upstream, r, d, receipt := brokerFixture(t)
	_, err := r.memory.Snapshot(t.Context())
	require.ErrorIs(t, err, source.ErrUnavailable)
	upstream.set(source.Snapshot{}, source.ErrUnavailable)
	require.ErrorIs(t, b.refresh(t.Context(), d, receipt), source.ErrUnavailable)
	_, err = r.memory.Snapshot(t.Context())
	require.ErrorIs(t, err, source.ErrUnavailable)
	foreign := committedCatalog(r, receipt)
	foreign.Scope.NetworkID = d.Request.Scope.NetworkID
	foreign.Scope.ProducerID = pdid.New(22).String()
	upstream.set(foreign, nil)
	_, err = r.Snapshot(t.Context())
	require.ErrorIs(t, err, source.ErrConflict)
	_, err = r.memory.Snapshot(t.Context())
	require.ErrorIs(t, err, source.ErrUnavailable)
	// Independent workers still commit when no Producer has attached locally.
	commit := notifyingCommitter{broker: newSourceBroker(), commit: brokerCommitFunc(func(context.Context, provision.Deployment) (publication.Receipt, error) { return receipt, nil })}
	_, err = commit.Commit(t.Context(), d)
	require.NoError(t, err)
}
