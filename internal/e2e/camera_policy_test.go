package e2e_test

import (
	"context"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	cameramodel "github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/hostagent"
	"github.com/lesomnus/shale/internal/identity"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/shale/server/core"
	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func cameraPolicyControl(t *testing.T) (*cmd.Server, string, *grpc.ClientConn, context.Context) {
	t.Helper()
	// Database/CA/Roster initialization is outside the RPC deadline. The RPCs
	// still use actual TLS, machine certificates and Roster operator grants.
	server, cp, admin := cameraTLSControl(t, func(c *cmd.Config) {
		c.Auth.Operators = identity.OperatorsConfig{Tenant: "acme", Ttl: 10 * time.Millisecond}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	return server, cp, admin, ctx
}

func cameraPolicyID(t *testing.T, raw []byte) string {
	t.Helper()
	id, err := pdid.From(raw)
	require.NoError(t, err)
	return id.String()
}

func cameraPolicyNetwork(t *testing.T, ctx context.Context, admin *grpc.ClientConn, key string, site *api.SiteRef) *api.CameraNetwork {
	t.Helper()
	v, err := api.NewCameraNetworkServiceClient(admin).Configure(ctx, api.CameraNetworkConfigureRequest_builder{Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Site: site, Alias: key, Cidrs: []string{"192.0.2.0/24"}, InterfaceHint: "fixture-camera"}.Build())
	require.NoError(t, err)
	return v
}

func cameraPolicySite(t *testing.T, ctx context.Context, admin *grpc.ClientConn, key string) *api.SiteRef {
	t.Helper()
	site, err := api.NewSiteServiceClient(admin).Add(ctx, api.SiteAddRequest_builder{Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: key}.Build())
	require.NoError(t, err)
	return api.SiteRef_builder{Id: site.GetId()}.Build()
}

func cameraPolicySet(t *testing.T, ctx context.Context, admin *grpc.ClientConn, key string, site *api.SiteRef) *api.Set {
	t.Helper()
	set, err := api.NewSetServiceClient(admin).Add(ctx, api.SetAddRequest_builder{Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: key, Site: site}.Build())
	require.NoError(t, err)
	return set
}

func cameraPolicyRule(id string, scope *api.CameraPresetScope) *api.CameraPresetRule {
	return api.CameraPresetRule_builder{Id: id, Scope: scope,
		Settings:     api.CameraPresetSettings_builder{PowerLineFrequency: 60, Video: api.CameraPresetVideo_builder{Codec: "h264", Width: 1280, Height: 720, Fps: 30000.0 / 1001, BitrateBps: 2_000_000, GopFrames: 30}.Build(), Preserve: []string{cameramodel.PresetFieldProfile}}.Build(),
		Alternatives: api.CameraPresetAlternatives_builder{Candidates: []*api.CameraPresetSettings{api.CameraPresetSettings_builder{Video: api.CameraPresetVideo_builder{Width: 640, Height: 360, Fps: 15}.Build()}.Build()}}.Build(),
		Required:     []string{cameramodel.PresetFieldFrequency, cameramodel.PresetFieldCodec},
	}.Build()
}

func cameraPolicyPresetRequest(key, revision string, rules ...*api.CameraPresetRule) *api.CameraPresetCreateRequest {
	return api.CameraPresetCreateRequest_builder{Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), PresetKey: key, Revision: revision, Name: key, Rules: rules}.Build()
}

func cameraPolicyCreateRequest(network *api.CameraNetwork, preset *api.CameraPreset, key, revision string) *api.CameraPolicyCreateRequest {
	return api.CameraPolicyCreateRequest_builder{Network: api.CameraNetworkRef_builder{Id: network.GetId()}.Build(), Preset: api.CameraPresetRef_builder{Id: preset.GetId()}.Build(), PolicyKey: key, Revision: revision, Name: key,
		Enrollment: api.CameraEnrollmentPolicy_builder{Automatic: true, AllowAny: true, MaxDevices: 4}.Build(),
	}.Build()
}

func TestCameraPolicyImmutableTemplatesAndActivationCAS(t *testing.T) {
	_, _, admin, ctx := cameraPolicyControl(t)
	network := cameraPolicyNetwork(t, ctx, admin, "policy-cas", nil)
	tenant := cameraPolicyID(t, network.GetTenant().GetId())
	base := cameraPolicyRule("defaults", api.CameraPresetScope_builder{TenantId: tenant}.Build())
	override := api.CameraPresetRule_builder{Id: "model", Scope: api.CameraPresetScope_builder{TenantId: tenant}.Build(), Match: api.CameraPresetMatch_builder{Model: "TestCam"}.Build(), Settings: api.CameraPresetSettings_builder{Video: api.CameraPresetVideo_builder{Fps: 15}.Build()}.Build(), Alternatives: api.CameraPresetAlternatives_builder{}.Build()}.Build()
	request := cameraPolicyPresetRequest("standard", "1", base, override)
	presets := api.NewCameraPresetServiceClient(admin)
	first, err := presets.Create(ctx, request)
	require.NoError(t, err)
	require.Equal(t, 30000.0/1001, first.GetRules()[0].GetSettings().GetVideo().GetFps())
	require.NotNil(t, first.GetRules()[1].GetAlternatives(), "explicit empty alternatives survive the database and wire")
	require.Empty(t, first.GetRules()[1].GetAlternatives().GetCandidates())
	reordered := proto.Clone(request).(*api.CameraPresetCreateRequest)
	slices.Reverse(reordered.GetRules())
	slices.Reverse(reordered.GetRules()[1].GetRequired())
	repeated, err := presets.Create(ctx, reordered)
	require.NoError(t, err)
	require.Equal(t, first.GetId(), repeated.GetId())
	require.True(t, proto.Equal(first.GetDateUpdated(), repeated.GetDateUpdated()), "an identical revision is not rewritten")
	changed := proto.Clone(request).(*api.CameraPresetCreateRequest)
	changed.GetRules()[0].GetSettings().GetVideo().SetFps(25)
	_, err = presets.Create(ctx, changed)
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	clearedPresence := proto.Clone(request).(*api.CameraPresetCreateRequest)
	clearedPresence.GetRules()[1].ClearAlternatives()
	_, err = presets.Create(ctx, clearedPresence)
	require.Equal(t, codes.AlreadyExists, status.Code(err), "inherit and explicitly empty are different immutable policies")
	changed.SetRevision("2")
	second, err := presets.Create(ctx, changed)
	require.NoError(t, err)
	require.NotEqual(t, first.GetId(), second.GetId())
	current, err := presets.Get(ctx, api.CameraPresetGetRequest_builder{Ref: api.CameraPresetRef_builder{Id: first.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, 30000.0/1001, current.GetRules()[0].GetSettings().GetVideo().GetFps())

	policies := api.NewCameraPolicyServiceClient(admin)
	policyRequest := cameraPolicyCreateRequest(network, first, "capture", "1")
	policy, err := policies.Create(ctx, policyRequest)
	require.NoError(t, err)
	replayed, err := policies.Create(ctx, policyRequest)
	require.NoError(t, err)
	require.Equal(t, policy.GetId(), replayed.GetId())
	policyChange := proto.Clone(policyRequest).(*api.CameraPolicyCreateRequest)
	policyChange.GetEnrollment().SetMaxDevices(3)
	_, err = policies.Create(ctx, policyChange)
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	policyChange.SetRevision("2")
	newPolicy, err := policies.Create(ctx, policyChange)
	require.NoError(t, err)
	active, err := policies.Activate(ctx, api.CameraPolicyActivateRequest_builder{Ref: api.CameraPolicyRef_builder{Id: policy.GetId()}.Build(), ExpectedNetworkVersion: network.GetDateUpdated()}.Build())
	require.NoError(t, err)
	require.Equal(t, policy.GetId(), active.GetActivePolicyId())
	_, err = policies.Activate(ctx, api.CameraPolicyActivateRequest_builder{Ref: api.CameraPolicyRef_builder{Id: newPolicy.GetId()}.Build(), ExpectedNetworkVersion: network.GetDateUpdated()}.Build())
	require.Equal(t, codes.Aborted, status.Code(err))
	unchanged, err := api.NewCameraNetworkServiceClient(admin).Get(ctx, api.CameraNetworkGetRequest_builder{Ref: api.CameraNetworkRef_builder{Id: network.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, policy.GetId(), unchanged.GetActivePolicyId(), "a stale activation cannot change the active pointer")
	active, err = policies.Activate(ctx, api.CameraPolicyActivateRequest_builder{Ref: api.CameraPolicyRef_builder{Id: newPolicy.GetId()}.Build(), ExpectedNetworkVersion: unchanged.GetDateUpdated()}.Build())
	require.NoError(t, err)
	require.Equal(t, newPolicy.GetId(), active.GetActivePolicyId())
}

func TestCameraPolicyTemplateScopeAndMixedSiteSelectors(t *testing.T) {
	server, _, admin, ctx := cameraPolicyControl(t)
	west, east := cameraPolicySite(t, ctx, admin, "west"), cameraPolicySite(t, ctx, admin, "east")
	westSet := cameraPolicySet(t, ctx, admin, "west-set", west)
	unsited := cameraPolicySet(t, ctx, admin, "unsited-set", nil)
	westNetwork, eastNetwork := cameraPolicyNetwork(t, ctx, admin, "west-lan", west), cameraPolicyNetwork(t, ctx, admin, "east-lan", east)
	tenant := cameraPolicyID(t, westNetwork.GetTenant().GetId())
	device, err := api.NewCameraDeviceServiceClient(admin).Claim(ctx, api.CameraDeviceClaimRequest_builder{Network: api.CameraNetworkRef_builder{Id: eastNetwork.GetId()}.Build(), DeviceKey: "east-camera"}.Build())
	require.NoError(t, err)
	presets := api.NewCameraPresetServiceClient(admin)
	for _, test := range []struct {
		name    string
		set     *api.Set
		network *api.CameraNetwork
		device  []byte
	}{
		{name: "set-west-network-east", set: westSet, network: eastNetwork},
		{name: "set-west-device-east", set: westSet, device: device.GetId()},
		{name: "set-null-network-west", set: unsited, network: westNetwork},
	} {
		t.Run(test.name, func(t *testing.T) {
			scope := api.CameraPresetScope_builder{TenantId: tenant, SetId: cameraPolicyID(t, test.set.GetId())}.Build()
			if test.network != nil {
				scope.SetNetworkId(cameraPolicyID(t, test.network.GetId()))
			}
			rule := cameraPolicyRule("defaults", scope)
			if len(test.device) > 0 {
				rule.SetMatch(api.CameraPresetMatch_builder{DeviceId: cameraPolicyID(t, test.device)}.Build())
			}
			_, err := presets.Create(ctx, cameraPolicyPresetRequest("mixed-site", test.name, rule))
			require.Equal(t, codes.PermissionDenied, status.Code(err), "an omitted site must not hide contradictory inventory parents, including NULL")
		})
	}
	globalRules := []*api.CameraPresetRule{
		cameraPolicyRule("west", api.CameraPresetScope_builder{TenantId: tenant, SiteId: cameraPolicyID(t, west.GetId())}.Build()),
		cameraPolicyRule("east", api.CameraPresetScope_builder{TenantId: tenant, SiteId: cameraPolicyID(t, east.GetId())}.Build()),
	}
	global, err := presets.Create(ctx, cameraPolicyPresetRequest("global-template", "1", globalRules...))
	require.NoError(t, err)
	policies := api.NewCameraPolicyServiceClient(admin)
	for i, network := range []*api.CameraNetwork{westNetwork, eastNetwork} {
		key := []string{"global-west", "global-east"}[i]
		policy, err := policies.Create(ctx, cameraPolicyCreateRequest(network, global, key, "1"))
		require.NoError(t, err)
		require.Equal(t, network.GetSite().GetId(), policy.GetSite().GetId(), "a global template can serve each exact site's network")
	}
	specificRequest := cameraPolicyPresetRequest("west-only", "1", globalRules[0])
	specificRequest.SetSite(west)
	specific, err := presets.Create(ctx, specificRequest)
	require.NoError(t, err)
	_, err = policies.Create(ctx, cameraPolicyCreateRequest(eastNetwork, specific, "wrong-site", "1"))
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	_, err = policies.Create(ctx, cameraPolicyCreateRequest(westNetwork, specific, "correct-site", "1"))
	require.NoError(t, err)
	_, _, err = server.Identity.Seed(ctx, "globex", "boss")
	require.NoError(t, err)
	foreignTenant, err := server.ProvisionTenant(ctx, "globex")
	require.NoError(t, err)
	foreignRequest := cameraPolicyPresetRequest("foreign", "1", cameraPolicyRule("defaults", api.CameraPresetScope_builder{TenantId: foreignTenant.String()}.Build()))
	foreignRequest.SetTenant(api.TenantRef_builder{Id: foreignTenant.Bytes()}.Build())
	_, err = presets.Create(ctx, foreignRequest)
	require.Contains(t, []codes.Code{codes.NotFound, codes.PermissionDenied}, status.Code(err), "a known foreign tenant is not a writable template namespace")
}

func TestCameraPolicyImmutableRetrySurvivesProducerReassignment(t *testing.T) {
	server, cp, admin, ctx := cameraPolicyControl(t)
	producer, firstSet, _ := cameraWorkerProducer(t, ctx, server, cp, admin, "preset-producer", nil)
	secondSet := cameraPolicySet(t, ctx, admin, "replacement-set", nil)
	rule := cameraPolicyRule("producer", api.CameraPresetScope_builder{TenantId: cameraPolicyID(t, firstSet.GetTenant().GetId()), SetId: cameraPolicyID(t, firstSet.GetId()), ProducerId: cameraPolicyID(t, producer.GetId())}.Build())
	request := cameraPolicyPresetRequest("stable-replay", "1", rule)
	presets := api.NewCameraPresetServiceClient(admin)
	first, err := presets.Create(ctx, request)
	require.NoError(t, err)
	moved, err := api.NewProducerServiceClient(admin).Adopt(ctx, api.ProducerAdoptRequest_builder{Ref: api.ProducerRef_builder{Id: producer.GetId()}.Build(), Set: api.SetRef_builder{Id: secondSet.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, secondSet.GetId(), moved.GetSet().GetId())
	replayed, err := presets.Create(ctx, request)
	require.NoError(t, err)
	require.Equal(t, first.GetId(), replayed.GetId(), "an immutable replay does not rebind itself to today's Producer graph")
	require.True(t, proto.Equal(first.GetDateUpdated(), replayed.GetDateUpdated()))
	newRequest := proto.Clone(request).(*api.CameraPresetCreateRequest)
	newRequest.SetRevision("2")
	_, err = presets.Create(ctx, newRequest)
	require.Equal(t, codes.PermissionDenied, status.Code(err), "a new revision still validates current inventory consistency")
}

func TestCameraPolicyManagementAndTemplateReadPermissions(t *testing.T) {
	server, cp, admin, ctx := cameraPolicyControl(t)
	network := cameraPolicyNetwork(t, ctx, admin, "authorization", nil)
	request := cameraPolicyPresetRequest("restricted-template", "1", cameraPolicyRule("defaults", api.CameraPresetScope_builder{TenantId: cameraPolicyID(t, network.GetTenant().GetId())}.Build()))
	presets, policies := api.NewCameraPresetServiceClient(admin), api.NewCameraPolicyServiceClient(admin)
	preset, err := presets.Create(ctx, request)
	require.NoError(t, err)
	policyRequest := cameraPolicyCreateRequest(network, preset, "restricted-policy", "1")
	policy, err := policies.Create(ctx, policyRequest)
	require.NoError(t, err)
	presetRef, policyRef := api.CameraPresetRef_builder{Id: preset.GetId()}.Build(), api.CameraPolicyRef_builder{Id: policy.GetId()}.Build()
	for name, operation := range map[string]func() error{
		"preset Add": func() error {
			_, err := presets.Add(ctx, api.CameraPresetAddRequest_builder{Tenant: request.GetTenant(), PresetKey: "bypass", Revision: "1", Rules: request.GetRules()}.Build())
			return err
		},
		"preset Patch": func() error {
			_, err := presets.Patch(ctx, api.CameraPresetPatchRequest_builder{Ref: presetRef, Name: z.Ptr("bypass")}.Build())
			return err
		},
		"preset Apply": func() error {
			_, err := presets.Apply(ctx, api.CameraPresetApplyRequest_builder{Ref: presetRef}.Build())
			return err
		},
		"preset Erase": func() error { _, err := presets.Erase(ctx, presetRef); return err },
		"policy Add": func() error {
			_, err := policies.Add(ctx, api.CameraPolicyAddRequest_builder{Tenant: request.GetTenant(), Network: policyRequest.GetNetwork(), Preset: presetRef, PolicyKey: "bypass", Revision: "1", Enrollment: policyRequest.GetEnrollment()}.Build())
			return err
		},
		"policy Patch": func() error {
			_, err := policies.Patch(ctx, api.CameraPolicyPatchRequest_builder{Ref: policyRef, Name: z.Ptr("bypass")}.Build())
			return err
		},
		"policy Apply": func() error {
			_, err := policies.Apply(ctx, api.CameraPolicyApplyRequest_builder{Ref: policyRef}.Build())
			return err
		},
		"policy Erase": func() error { _, err := policies.Erase(ctx, policyRef); return err },
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, codes.PermissionDenied, status.Code(operation()), "even operators cannot bypass immutable custom operations")
		})
	}
	_, err = server.Identity.AddPerson(ctx, "acme", "policy-viewer", "Policy viewer")
	require.NoError(t, err)
	viewer := cameraTLSPrincipal(t, server, cp, "@acme/policy-viewer")
	_, worker, _ := cameraWorkerMachine(t, ctx, server, cp, admin, "policy-worker", nil)
	_, _, producer := cameraWorkerProducer(t, ctx, server, cp, admin, "policy-producer", nil)
	reader := cameraPolicyReader(t, ctx, server, cp, admin)
	for _, role := range []struct {
		name string
		conn *grpc.ClientConn
	}{{"viewer", viewer}, {"worker", worker}, {"producer", producer}, {"reader", reader}} {
		t.Run(role.name, func(t *testing.T) {
			p, q := api.NewCameraPresetServiceClient(role.conn), api.NewCameraPolicyServiceClient(role.conn)
			_, err := p.Create(ctx, request)
			require.Equal(t, codes.PermissionDenied, status.Code(err))
			_, err = q.Create(ctx, policyRequest)
			require.Equal(t, codes.PermissionDenied, status.Code(err))
			_, err = q.Activate(ctx, api.CameraPolicyActivateRequest_builder{Ref: policyRef, ExpectedNetworkVersion: network.GetDateUpdated()}.Build())
			require.Equal(t, codes.PermissionDenied, status.Code(err))
			_, err = p.Get(ctx, api.CameraPresetGetRequest_builder{Ref: presetRef}.Build())
			require.Equal(t, codes.PermissionDenied, status.Code(err))
			_, err = p.List(ctx, api.CameraPresetListRequest_builder{}.Build())
			require.Equal(t, codes.PermissionDenied, status.Code(err))
			_, err = q.Get(ctx, api.CameraPolicyGetRequest_builder{Ref: policyRef}.Build())
			require.Equal(t, codes.PermissionDenied, status.Code(err))
			_, err = q.List(ctx, api.CameraPolicyListRequest_builder{}.Build())
			require.Equal(t, codes.PermissionDenied, status.Code(err))
			watchCtx, cancel := context.WithTimeout(ctx, time.Second)
			presetWatch, watchErr := p.Watch(watchCtx, api.CameraPresetWatchRequest_builder{}.Build())
			if watchErr == nil {
				_, watchErr = presetWatch.Recv()
			}
			cancel()
			require.Equal(t, codes.PermissionDenied, status.Code(watchErr), "template subscriptions are not a separate read bypass")
			watchCtx, cancel = context.WithTimeout(ctx, time.Second)
			policyWatch, watchErr := q.Watch(watchCtx, api.CameraPolicyWatchRequest_builder{}.Build())
			if watchErr == nil {
				_, watchErr = policyWatch.Recv()
			}
			cancel()
			require.Equal(t, codes.PermissionDenied, status.Code(watchErr))
		})
	}
	unchanged, err := presets.Get(ctx, api.CameraPresetGetRequest_builder{Ref: presetRef}.Build())
	require.NoError(t, err)
	require.True(t, proto.Equal(preset, unchanged), "all rejected calls preserve the immutable row")
	current, err := api.NewCameraNetworkServiceClient(admin).Get(ctx, api.CameraNetworkGetRequest_builder{Ref: policyRequest.GetNetwork()}.Build())
	require.NoError(t, err)
	require.Empty(t, current.GetActivePolicyId(), "denied activation does not change the network")
}

func cameraPolicyReader(t *testing.T, ctx context.Context, server *cmd.Server, cp string, admin *grpc.ClientConn) *grpc.ClientConn {
	t.Helper()
	agent := &hostagent.Agent{Kind: core.DomReader, HardwareId: "policy-reader", Store: pki.Store{Dir: t.TempDir()}, Cp: cp, CaHash: pki.Fingerprint(server.CA.Cert), Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	require.NoError(t, agent.Init())
	join := func(ctx context.Context, conn *grpc.ClientConn, host *api.HostJoin) (hostagent.Answer, error) {
		v, err := api.NewReaderServiceClient(conn).Join(ctx, api.ReaderJoinRequest_builder{Tenant: "acme", Host: host}.Build())
		if err != nil {
			return hostagent.Answer{}, err
		}
		return hostagent.Answer{JoinAnswer: v.GetAnswer()}, nil
	}
	bootstrap, err := agent.Dial(ctx)
	require.NoError(t, err)
	defer bootstrap.Close()
	host, err := agent.HostJoin()
	require.NoError(t, err)
	pending, err := join(ctx, bootstrap, host)
	require.NoError(t, err)
	_, err = api.NewReaderServiceClient(admin).Adopt(ctx, api.ReaderAdoptRequest_builder{Ref: api.ReaderRef_builder{Id: pending.GetId()}.Build()}.Build())
	require.NoError(t, err)
	_, err = agent.Join(ctx, join)
	require.NoError(t, err)
	conn, err := agent.Dial(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}
