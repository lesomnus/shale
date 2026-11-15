package cameraworker

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func transportObservationFixture(t *testing.T) (*Worker, *api.CameraWorkerAssignment, *api.CameraPolicyObserveRequest, *api.CameraPolicyObserveResponse) {
	t.Helper()
	w, assigned, _ := discoveryFixture(t)
	n := assigned.GetNetwork()
	n.SetDateUpdated(timestamppb.Now())
	a := assigned.GetAssignment()
	d := api.CameraDevice_builder{Id: pdid.New(27).Bytes(), Tenant: n.GetTenant(), Network: n, ObservationRevision: 3, Endpoints: []*api.CameraObservedEndpoint{api.CameraObservedEndpoint_builder{Uri: "http://192.0.2.10:2020/onvif/device_service"}.Build()}}.Build()
	moved := []*api.CameraObservedEndpoint{api.CameraObservedEndpoint_builder{Uri: "http://192.0.2.20:2020/onvif/device_service"}.Build()}
	now := time.Now()
	g := api.CameraTransportGrant_builder{
		CandidateId: strings.Repeat("c", 32), AssignmentId: a.GetId(), AssignmentGeneration: a.GetGeneration(), DeviceId: d.GetId(), NetworkId: n.GetId(), NetworkVersion: n.GetDateUpdated(), ObservationRevision: d.GetObservationRevision(),
		JobId: pdid.New(30).Bytes(), Policy: api.CameraRevisionReference_builder{Id: pdid.New(41).String(), Revision: "1"}.Build(),
		Trust:    api.CameraDeviceTLSIdentity_builder{DeviceId: cameraID(d.GetId()), SpkiSha256: "sha256:" + strings.Repeat("a", 64)}.Build(),
		Endpoint: "https://192.0.2.20:443", DateIssued: timestamppb.New(now), DateExpires: timestamppb.New(now.Add(2 * time.Minute)),
	}.Build()
	d.SetTransport(api.CameraTransportState_builder{Candidates: []*api.CameraTransportCandidate{api.CameraTransportCandidate_builder{Id: g.GetCandidateId(), Endpoints: moved, State: "pending", Code: "verification_required", Grant: g}.Build()}}.Build())
	request := api.CameraPolicyObserveRequest_builder{Assignment: api.CameraAssignmentRef_builder{Id: a.GetId()}.Build(), AssignmentGeneration: a.GetGeneration(), DiscoveryId: "urn:uuid:9c654a77-3a86-4466-b189-7bbdc38ad248", Endpoints: moved}.Build()
	return w, assigned, request, api.CameraPolicyObserveResponse_builder{Code: "transport_verification_required", Device: d, TransportGrant: g}.Build()
}

type transportPolicyClient struct {
	api.CameraPolicyServiceClient
	verify func(context.Context, *api.CameraPolicyVerifyTransportRequest) (*api.CameraPolicyObserveResponse, error)
}

func (c transportPolicyClient) VerifyTransport(ctx context.Context, q *api.CameraPolicyVerifyTransportRequest, _ ...grpc.CallOption) (*api.CameraPolicyObserveResponse, error) {
	return c.verify(ctx, q)
}

func TestTransportGrantNeedsTodaysAssignmentAndTheObservedHostBeforeAnyLANIO(t *testing.T) {
	w, assigned, request, observed := transportObservationFixture(t)
	require.NoError(t, transportGrantScope(assigned, w.ID().Bytes(), request, observed, time.Now()))
	for name, edit := range map[string]func(*api.CameraWorkerAssignment, *api.CameraPolicyObserveRequest, *api.CameraPolicyObserveResponse){
		"generation": func(a *api.CameraWorkerAssignment, _ *api.CameraPolicyObserveRequest, _ *api.CameraPolicyObserveResponse) {
			a.GetAssignment().SetGeneration(99)
		},
		"worker": func(a *api.CameraWorkerAssignment, _ *api.CameraPolicyObserveRequest, _ *api.CameraPolicyObserveResponse) {
			a.GetAssignment().SetWorkerId(pdid.New(Domain).Bytes())
		},
		"network": func(a *api.CameraWorkerAssignment, _ *api.CameraPolicyObserveRequest, _ *api.CameraPolicyObserveResponse) {
			a.GetNetwork().SetDateUpdated(timestamppb.New(time.Now().Add(time.Second)))
		},
		"prefix": func(a *api.CameraWorkerAssignment, _ *api.CameraPolicyObserveRequest, _ *api.CameraPolicyObserveResponse) {
			a.GetNetwork().SetCidrs([]string{"198.51.100.0/24"})
		},
		"other_host": func(_ *api.CameraWorkerAssignment, _ *api.CameraPolicyObserveRequest, o *api.CameraPolicyObserveResponse) {
			o.GetTransportGrant().SetEndpoint("https://192.0.2.21:443")
		},
		"not_observed": func(_ *api.CameraWorkerAssignment, r *api.CameraPolicyObserveRequest, _ *api.CameraPolicyObserveResponse) {
			r.GetEndpoints()[0].SetUri("http://192.0.2.30:2020/onvif/device_service")
		},
		"plain_http": func(_ *api.CameraWorkerAssignment, _ *api.CameraPolicyObserveRequest, o *api.CameraPolicyObserveResponse) {
			o.GetTransportGrant().SetEndpoint("http://192.0.2.20:443")
		},
		"path": func(_ *api.CameraWorkerAssignment, _ *api.CameraPolicyObserveRequest, o *api.CameraPolicyObserveResponse) {
			o.GetTransportGrant().SetEndpoint("https://192.0.2.20:443/login")
		},
		"expired": func(_ *api.CameraWorkerAssignment, _ *api.CameraPolicyObserveRequest, o *api.CameraPolicyObserveResponse) {
			o.GetTransportGrant().SetDateExpires(timestamppb.New(time.Now().Add(-time.Second)))
		},
		"long_lived": func(_ *api.CameraWorkerAssignment, _ *api.CameraPolicyObserveRequest, o *api.CameraPolicyObserveResponse) {
			o.GetTransportGrant().SetDateExpires(timestamppb.New(time.Now().Add(time.Hour)))
		},
		"revision": func(_ *api.CameraWorkerAssignment, _ *api.CameraPolicyObserveRequest, o *api.CameraPolicyObserveResponse) {
			o.GetDevice().SetObservationRevision(4)
		},
		"other_device": func(_ *api.CameraWorkerAssignment, _ *api.CameraPolicyObserveRequest, o *api.CameraPolicyObserveResponse) {
			o.GetTransportGrant().GetTrust().SetDeviceId(pdid.New(27).String())
		},
		"not_pending": func(_ *api.CameraWorkerAssignment, _ *api.CameraPolicyObserveRequest, o *api.CameraPolicyObserveResponse) {
			o.GetDevice().GetTransport().GetCandidates()[0].SetState("operator_required")
		},
		"not_stored": func(_ *api.CameraWorkerAssignment, _ *api.CameraPolicyObserveRequest, o *api.CameraPolicyObserveResponse) {
			o.GetDevice().ClearTransport()
		},
		"request_scope": func(_ *api.CameraWorkerAssignment, r *api.CameraPolicyObserveRequest, _ *api.CameraPolicyObserveResponse) {
			r.SetAssignmentGeneration(6)
		},
	} {
		a, r, o := proto.Clone(assigned).(*api.CameraWorkerAssignment), proto.Clone(request).(*api.CameraPolicyObserveRequest), proto.Clone(observed).(*api.CameraPolicyObserveResponse)
		edit(a, r, o)
		// The stored candidate carries the same grant the response does, so
		// each case meets its own check rather than a mismatch.
		if c := o.GetDevice().GetTransport().GetCandidates(); len(c) == 1 {
			c[0].SetGrant(proto.Clone(o.GetTransportGrant()).(*api.CameraTransportGrant))
		}
		probed := false
		err := w.verifyTransport(t.Context(), discoveryWorkerClient{poll: func(context.Context, *api.CameraWorkerPollRequest) (*api.CameraWorkerPollResponse, error) {
			return api.CameraWorkerPollResponse_builder{Assignments: []*api.CameraWorkerAssignment{a}}.Build(), nil
		}}, transportPolicyClient{verify: func(context.Context, *api.CameraPolicyVerifyTransportRequest) (*api.CameraPolicyObserveResponse, error) {
			t.Fatalf("%s: an unscoped grant reached the CP", name)
			return nil, nil
		}}, r, o, func(context.Context, *api.CameraTransportGrant) (string, error) { probed = true; return "", nil })
		require.Error(t, err, name)
		require.False(t, probed, "%s: no handshake outside the grant's scope", name)
	}
}

func TestTransportVerificationReportsThePresentedKeyOnly(t *testing.T) {
	w, assigned, request, observed := transportObservationFixture(t)
	workers := discoveryWorkerClient{poll: func(context.Context, *api.CameraWorkerPollRequest) (*api.CameraWorkerPollResponse, error) {
		return api.CameraWorkerPollResponse_builder{Assignments: []*api.CameraWorkerAssignment{assigned}}.Build(), nil
	}}
	var calls int
	policies := transportPolicyClient{verify: func(_ context.Context, q *api.CameraPolicyVerifyTransportRequest) (*api.CameraPolicyObserveResponse, error) {
		calls++
		require.True(t, proto.Equal(observed.GetTransportGrant(), q.GetGrant()))
		require.Equal(t, "sha256:"+strings.Repeat("b", 64), q.GetSpkiSha256(), "the presented key goes to the CP, whatever it is")
		return api.CameraPolicyObserveResponse_builder{Code: "transport_key_mismatch"}.Build(), nil
	}}
	var deadline time.Time
	require.NoError(t, w.verifyTransport(t.Context(), workers, policies, request, observed, func(ctx context.Context, g *api.CameraTransportGrant) (string, error) {
		deadline, _ = ctx.Deadline()
		require.Equal(t, "https://192.0.2.20:443", g.GetEndpoint())
		return "sha256:" + strings.Repeat("b", 64), nil
	}))
	require.Equal(t, 1, calls)
	require.False(t, deadline.After(observed.GetTransportGrant().GetDateExpires().AsTime()), "the handshake ends with the grant")
	// An unreachable candidate reports nothing; it stays pending and expires.
	err := w.verifyTransport(t.Context(), workers, policies, request, observed, func(context.Context, *api.CameraTransportGrant) (string, error) {
		return "", context.DeadlineExceeded
	})
	require.Error(t, err)
	require.Equal(t, 1, calls)
	// An old CP sends no grant: nothing is done.
	observed.ClearTransportGrant()
	require.NoError(t, w.verifyTransport(t.Context(), workers, policies, request, observed, func(context.Context, *api.CameraTransportGrant) (string, error) {
		t.Fatal("no grant, no handshake")
		return "", nil
	}))
}

func TestRecoveredCameraMustReportItsRecordedSerialAndMAC(t *testing.T) {
	eps := []*api.CameraObservedEndpoint{api.CameraObservedEndpoint_builder{Uri: "http://192.0.2.20:2020/onvif/device_service"}.Build()}
	d := api.CameraDevice_builder{Endpoints: eps, Identity: api.CameraIdentityEvidence_builder{Serial: "unit-1", Macs: []string{"02:00:00:00:00:01"}, Authenticated: true}.Build()}.Build()
	same := camera.DeviceHint{Serial: "unit-1", MAC: "02:00:00:00:00:01", Evidence: []camera.Evidence{{Authenticated: true}}}
	noMAC := same
	noMAC.MAC = ""
	require.NoError(t, recordedIdentity(d, noMAC), "elsewhere an inspection without a MAC is as before")
	d.SetTransport(api.CameraTransportState_builder{Candidates: []*api.CameraTransportCandidate{api.CameraTransportCandidate_builder{State: "accepted", Endpoints: eps}.Build()}}.Build())
	require.NoError(t, recordedIdentity(d, same))
	require.ErrorIs(t, recordedIdentity(d, noMAC), camera.ErrAmbiguousIdentity, "at verified endpoints the MAC must be reported")
	other := same
	other.MAC = "02:00:00:00:00:02"
	require.ErrorIs(t, recordedIdentity(d, other), camera.ErrAmbiguousIdentity)
	other = same
	other.Serial = "unit-2"
	require.ErrorIs(t, recordedIdentity(d, other), camera.ErrAmbiguousIdentity)
}
