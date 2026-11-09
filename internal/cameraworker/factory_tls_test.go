package cameraworker

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/vendors/tplink"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func factoryObservationFixture(t *testing.T) (*Worker, *api.CameraWorkerAssignment, *api.CameraPolicyObserveResponse) {
	t.Helper()
	w, assigned, _ := discoveryFixture(t)
	n := assigned.GetNetwork()
	n.SetCidrs([]string{"192.168.250.0/24"})
	n.SetSeeds([]string{"http://192.168.250.10:2020/onvif"})
	n.SetDateUpdated(timestamppb.Now())
	n.SetActivePolicyId(pdid.New(41).Bytes())
	d := api.CameraDevice_builder{Id: pdid.New(27).Bytes(), Tenant: n.GetTenant(), Network: n, ObservationRevision: 3, Endpoints: []*api.CameraObservedEndpoint{api.CameraObservedEndpoint_builder{Uri: n.GetSeeds()[0]}.Build()}}.Build()
	p := api.CameraFactoryTLSPlan_builder{AssignmentId: assigned.GetAssignment().GetId(), AssignmentGeneration: assigned.GetAssignment().GetGeneration(), DeviceId: d.GetId(), ObservationRevision: d.GetObservationRevision(), Policy: api.CameraRevisionReference_builder{Id: cameraID(n.GetActivePolicyId()), Revision: "1"}.Build(), Bootstrap: api.CameraBootstrapProfile_builder{CompatibilityProfile: tplink.ProfileC420Known, WebScheme: "https", WebPort: 443}.Build(), Endpoint: "https://192.168.250.10:443", DateIssued: timestamppb.Now(), DateExpires: timestamppb.New(time.Now().Add(time.Minute)), NetworkVersion: n.GetDateUpdated(), OperationKey: "public-plan-1"}.Build()
	d.SetFactoryTls(api.CameraFactoryTLSState_builder{Plan: p}.Build())
	return w, assigned, api.CameraPolicyObserveResponse_builder{Code: "factory_tls_required", Device: d, FactoryTlsPlan: p}.Build()
}

type factoryPolicyClient struct {
	api.CameraPolicyServiceClient
	trust func(context.Context, *api.CameraPolicyTrustFactoryTLSRequest) (*api.CameraPolicyObserveResponse, error)
}

func (c factoryPolicyClient) TrustFactoryTLS(ctx context.Context, q *api.CameraPolicyTrustFactoryTLSRequest, _ ...grpc.CallOption) (*api.CameraPolicyObserveResponse, error) {
	return c.trust(ctx, q)
}

func TestFactoryTLSWorkerRequiresFreshAssignmentBeforeAnyLANIO(t *testing.T) {
	w, assigned, observation := factoryObservationFixture(t)
	require.NoError(t, factoryTLSPlanScope(assigned, w.ID().Bytes(), observation, time.Now()))
	for _, edit := range []func(*api.CameraWorkerAssignment, *api.CameraPolicyObserveResponse){
		func(a *api.CameraWorkerAssignment, o *api.CameraPolicyObserveResponse) {
			a.GetAssignment().SetGeneration(99)
		},
		func(a *api.CameraWorkerAssignment, o *api.CameraPolicyObserveResponse) {
			a.GetAssignment().SetWorkerId(pdid.New(Domain).Bytes())
		},
		func(a *api.CameraWorkerAssignment, o *api.CameraPolicyObserveResponse) {
			a.GetNetwork().SetDateUpdated(timestamppb.New(time.Now().Add(time.Second)))
		},
		func(a *api.CameraWorkerAssignment, o *api.CameraPolicyObserveResponse) {
			a.GetNetwork().SetActivePolicyId(pdid.New(41).Bytes())
		},
		func(a *api.CameraWorkerAssignment, o *api.CameraPolicyObserveResponse) {
			o.GetFactoryTlsPlan().SetEndpoint("https://192.168.250.11:443")
		},
		func(a *api.CameraWorkerAssignment, o *api.CameraPolicyObserveResponse) {
			o.GetFactoryTlsPlan().SetDateExpires(timestamppb.New(time.Now().Add(-time.Second)))
		},
		func(a *api.CameraWorkerAssignment, o *api.CameraPolicyObserveResponse) {
			o.GetDevice().SetObservationRevision(4)
		},
		func(a *api.CameraWorkerAssignment, o *api.CameraPolicyObserveResponse) {
			o.GetDevice().SetIdentity(api.CameraIdentityEvidence_builder{Authenticated: true}.Build())
		},
	} {
		a, o := proto.Clone(assigned).(*api.CameraWorkerAssignment), proto.Clone(observation).(*api.CameraPolicyObserveResponse)
		edit(a, o)
		called := false
		err := w.completeFactoryTLS(t.Context(), discoveryWorkerClient{poll: func(context.Context, *api.CameraWorkerPollRequest) (*api.CameraWorkerPollResponse, error) {
			return api.CameraWorkerPollResponse_builder{Assignments: []*api.CameraWorkerAssignment{a}}.Build(), nil
		}}, factoryPolicyClient{trust: func(context.Context, *api.CameraPolicyTrustFactoryTLSRequest) (*api.CameraPolicyObserveResponse, error) {
			t.Fatal("unapproved factory state reached CP trust")
			return nil, nil
		}}, o, func(context.Context, *api.CameraFactoryTLSPlan) (string, error) { called = true; return "", nil })
		require.Error(t, err)
		require.False(t, called)
	}
}

func TestFactoryTLSWorkerCommitsOnlyRequiredAnonymousFactoryEvidence(t *testing.T) {
	w, a, o := factoryObservationFixture(t)
	workers := discoveryWorkerClient{poll: func(context.Context, *api.CameraWorkerPollRequest) (*api.CameraWorkerPollResponse, error) {
		return api.CameraWorkerPollResponse_builder{Assignments: []*api.CameraWorkerAssignment{a}}.Build(), nil
	}}
	var requests int
	policies := factoryPolicyClient{trust: func(_ context.Context, q *api.CameraPolicyTrustFactoryTLSRequest) (*api.CameraPolicyObserveResponse, error) {
		requests++
		require.True(t, proto.Equal(o.GetFactoryTlsPlan(), q.GetPlan()))
		require.True(t, q.GetActivationRequired())
		require.Equal(t, cameraID(o.GetDevice().GetId()), q.GetIdentity().GetDeviceId())
		require.Equal(t, "sha256:"+strings.Repeat("a", 64), q.GetIdentity().GetSpkiSha256())
		return &api.CameraPolicyObserveResponse{}, nil
	}}
	require.NoError(t, w.completeFactoryTLS(t.Context(), workers, policies, o, func(context.Context, *api.CameraFactoryTLSPlan) (string, error) {
		return "sha256:" + strings.Repeat("a", 64), nil
	}))
	require.Equal(t, 1, requests)
	err := w.completeFactoryTLS(t.Context(), workers, policies, o, func(context.Context, *api.CameraFactoryTLSPlan) (string, error) {
		return "", camera.ErrManualActivation
	})
	require.ErrorIs(t, err, camera.ErrManualActivation)
	require.Equal(t, 1, requests, "initialized or unsupported devices remain inventory only")
}

func TestFactoryTLSWorkerProbeDeadlineCannotOutlivePlan(t *testing.T) {
	w, a, o := factoryObservationFixture(t)
	expires := time.Now().Add(5 * time.Second)
	o.GetFactoryTlsPlan().SetDateExpires(timestamppb.New(expires))
	workers := discoveryWorkerClient{poll: func(context.Context, *api.CameraWorkerPollRequest) (*api.CameraWorkerPollResponse, error) {
		return api.CameraWorkerPollResponse_builder{Assignments: []*api.CameraWorkerAssignment{a}}.Build(), nil
	}}
	policies := factoryPolicyClient{trust: func(context.Context, *api.CameraPolicyTrustFactoryTLSRequest) (*api.CameraPolicyObserveResponse, error) {
		return &api.CameraPolicyObserveResponse{}, nil
	}}
	require.NoError(t, w.completeFactoryTLS(t.Context(), workers, policies, o, func(ctx context.Context, _ *api.CameraFactoryTLSPlan) (string, error) {
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.False(t, deadline.After(expires), "public LAN I/O must stop when its CP plan expires")
		return "sha256:" + strings.Repeat("a", 64), nil
	}))
}

func factoryCertificate(t *testing.T, serial int64) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{SerialNumber: big.NewInt(serial), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	leaf, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
}

func TestFactoryTLSProbeIsAnonymousAndRejectsReadyCameraAndChangedKey(t *testing.T) {
	for _, state := range []struct {
		name      string
		code      int
		changeKey bool
		success   bool
	}{{"factory", -40405, false, true}, {"initialized", -40401, false, false}, {"unknown", 0, false, false}, {"changed-key", -40405, true, false}} {
		t.Run(state.name, func(t *testing.T) {
			first, second := factoryCertificate(t, 1), factoryCertificate(t, 2)
			var handshakes, requests atomic.Int64
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || r.URL.RawQuery != "" {
					t.Error("public factory probe carried authentication")
				}
				body, err := io.ReadAll(io.LimitReader(r.Body, 1024))
				require.NoError(t, err)
				var request map[string]any
				require.NoError(t, json.Unmarshal(body, &request))
				require.Equal(t, map[string]any{"method": "do", "user_management": map[string]any{"get_encrypt_info": nil}}, request)
				w.WriteHeader(http.StatusUnauthorized)
				_ = json.NewEncoder(w).Encode(map[string]any{"error_code": -40401, "data": map[string]any{"code": state.code}})
			}))
			server.Config.ErrorLog = log.New(io.Discard, "", 0)
			server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
				cert := first
				if handshakes.Add(1) > 1 && state.changeKey {
					cert = second
				}
				return &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}}, nil
			}}
			server.StartTLS()
			defer server.Close()
			u := server.URL
			p := api.CameraFactoryTLSPlan_builder{Endpoint: u, Bootstrap: api.CameraBootstrapProfile_builder{CompatibilityProfile: tplink.ProfileC420Known}.Build()}.Build()
			pin, err := probeFactoryTLS(t.Context(), p)
			if state.success {
				require.NoError(t, err)
				hash := sha256.Sum256(first.Leaf.RawSubjectPublicKeyInfo)
				require.Equal(t, "sha256:"+hex.EncodeToString(hash[:]), pin)
				require.EqualValues(t, 1, requests.Load())
			} else {
				require.Error(t, err)
				require.Empty(t, pin)
				if state.changeKey {
					require.Zero(t, requests.Load(), "a changed key must fail before even the anonymous HTTP challenge")
				}
			}
		})
	}
}
