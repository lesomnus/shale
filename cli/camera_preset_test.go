package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lesomnus/payday/pdcmd"
	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func presetPublishID(t *testing.T, entity string) pdid.Id {
	t.Helper()
	domain, ok := pdid.Lookup(entity)
	require.True(t, ok)
	return pdid.New(domain)
}

func presetPublishFile(t *testing.T, text string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "camera-presets.yaml")
	require.NoError(t, os.WriteFile(path, []byte(text), 0600))
	return path
}

func presetPublishDocument(tenant string, reverse bool) string {
	rules := []string{
		fmt.Sprintf("  - id: tenant\n    scope: {tenant_id: %s}\n    settings:\n      video: {profile_token: profile_1, codec: h264, width: 1920, height: 1080}\n    required: [video.codec]\n", tenant),
		fmt.Sprintf("  - id: model\n    scope: {tenant_id: %s}\n    match: {manufacturer: TP-Link, model: EasyCam C420}\n    settings:\n      initialization: {power_line_frequency: 60, smart_codec: off}\n      video: {fps: 30, bitrate: 4096000, gop: 25}\n    required: [initialization.smart_codec]\n    fallbacks: []\n", tenant),
	}
	if reverse {
		rules[0], rules[1] = rules[1], rules[0]
	}
	return "id: office-cameras\nrevision: c420-v1\nrules:\n" + strings.Join(rules, "")
}

func TestPresetPublishBuildPreservesPortablePresenceAndCanonicalOrdering(t *testing.T) {
	tenant := presetPublishID(t, "shale.Tenant")
	path := presetPublishFile(t, presetPublishDocument(tenant.String(), false))
	req, err := buildCameraPresetPublish(cameraPresetPublishOptions{file: path})
	require.NoError(t, err)
	require.Equal(t, tenant.Bytes(), req.GetTenant().GetId())
	require.Nil(t, req.GetSite())
	require.Equal(t, "office-cameras", req.GetPresetKey())
	require.Equal(t, "c420-v1", req.GetRevision(), "source revision, not the portable '~fingerprint' revision")
	require.Equal(t, "office-cameras", req.GetName())
	require.Len(t, req.GetRules(), 2)
	byID := map[string]*api.CameraPresetRule{}
	for _, rule := range req.GetRules() {
		byID[rule.GetId()] = rule
	}
	require.Nil(t, byID["tenant"].GetAlternatives())
	require.NotNil(t, byID["model"].GetAlternatives())
	require.Empty(t, byID["model"].GetAlternatives().GetCandidates())
	require.Equal(t, int32(60), byID["model"].GetSettings().GetPowerLineFrequency())
	require.Equal(t, "off", byID["model"].GetSettings().GetSmartCodec())
	require.Contains(t, byID["model"].GetRequired(), "initialization.smart_codec")
	require.Equal(t, float64(30), byID["model"].GetSettings().GetVideo().GetFps())
	reordered, err := buildCameraPresetPublish(cameraPresetPublishOptions{file: presetPublishFile(t, presetPublishDocument(tenant.String(), true))})
	require.NoError(t, err)
	require.True(t, proto.Equal(req, reordered), "equivalent rule order must yield the same immutable request")
}

func TestPresetPublishRejectsInvalidAndCrossScopeDocumentsBeforeConnecting(t *testing.T) {
	tenant, foreign := presetPublishID(t, "shale.Tenant"), presetPublishID(t, "shale.Tenant")
	site := presetPublishID(t, "shale.Site")
	valid := presetPublishDocument(tenant.String(), false)
	for name, o := range map[string]cameraPresetPublishOptions{
		"missing file":          {},
		"unknown field":         {file: presetPublishFile(t, valid+"password: private-camera-value\n")},
		"multiple documents":    {file: presetPublishFile(t, valid+"---\nid: other\n")},
		"cross tenant":          {file: presetPublishFile(t, strings.Replace(valid, tenant.String(), foreign.String(), 1))},
		"wrong explicit tenant": {file: presetPublishFile(t, valid), tenant: foreign.String()},
		"tenant alias":          {file: presetPublishFile(t, strings.ReplaceAll(valid, tenant.String(), "hday"))},
		"wrong tenant domain":   {file: presetPublishFile(t, strings.ReplaceAll(valid, tenant.String(), site.String()))},
		"noncanonical tenant":   {file: presetPublishFile(t, strings.ReplaceAll(valid, tenant.String(), strings.ToUpper(tenant.String())))},
		"cross site":            {file: presetPublishFile(t, valid), site: site.String()},
		"wrong site domain":     {file: presetPublishFile(t, valid), site: tenant.String()},
		"invalid cp revision":   {file: presetPublishFile(t, strings.Replace(valid, "c420-v1", "c420-v1~hash", 1))},
		"wire overflow":         {file: presetPublishFile(t, strings.Replace(valid, "gop: 25", "gop: 2147483648", 1))},
	} {
		t.Run(name, func(t *testing.T) {
			connection := &presetPublishConnector{err: errors.New("connection must not be attempted")}
			err := publishCameraPreset(t.Context(), io.Discard, connection, o)
			require.Error(t, err)
			require.Zero(t, connection.calls)
			require.NotContains(t, err.Error(), "private-camera-value")
		})
	}
	scoped := strings.ReplaceAll(valid, "tenant_id: "+tenant.String(), "tenant_id: "+tenant.String()+", site_id: "+site.String())
	req, err := buildCameraPresetPublish(cameraPresetPublishOptions{file: presetPublishFile(t, scoped), tenant: tenant.String(), site: site.String(), name: "Office C420"})
	require.NoError(t, err)
	require.Equal(t, site.Bytes(), req.GetSite().GetId())
	require.Equal(t, "Office C420", req.GetName())
	// Without an explicit site, valid per-site selectors remain in a tenant template.
	req, err = buildCameraPresetPublish(cameraPresetPublishOptions{file: presetPublishFile(t, scoped)})
	require.NoError(t, err)
	require.Nil(t, req.GetSite())
	require.Equal(t, site.String(), req.GetRules()[0].GetScope().GetSiteId())
}

type presetPublishConnection struct {
	grpc.ClientConnInterface
	invoke func(context.Context, string, any, any) error
}

func (c presetPublishConnection) Invoke(ctx context.Context, method string, in, out any, _ ...grpc.CallOption) error {
	return c.invoke(ctx, method, in, out)
}

type presetPublishConnector struct {
	conn   pdcmd.Conn
	err    error
	calls  int
	closed int
}

func (c *presetPublishConnector) Connect(context.Context) (pdcmd.Conn, func(), error) {
	c.calls++
	return c.conn, func() { c.closed++ }, c.err
}

func TestPresetPublishDryRunDoesNotConnectOrReadAuthentication(t *testing.T) {
	tenant := presetPublishID(t, "shale.Tenant")
	path := presetPublishFile(t, presetPublishDocument(tenant.String(), false))
	connection := &presetPublishConnector{err: errors.New("must not read authentication")}
	var out bytes.Buffer
	err := publishCameraPreset(t.Context(), &out, connection, cameraPresetPublishOptions{file: path, dryRun: true})
	require.NoError(t, err)
	require.Zero(t, connection.calls)
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(out.Bytes(), &decoded))
	require.Equal(t, tenant.String(), decoded["tenant"].(map[string]any)["id"])
	require.Equal(t, "c420-v1", decoded["revision"])
	// The actual CLI takes the same early path with unusable TLS/auth configuration.
	output, err := run(t, &cmd.Config{Client: cmd.ClientConfig{Addr: "https://must-not-be-contacted.invalid", CaFile: "/does-not-exist/cp-ca"}}, "", "camera-preset", "publish", "--file", path, "--dry-run")
	require.NoError(t, err)
	require.Contains(t, output, tenant.String())
}

func TestPresetPublishUsesOnlyTypedCreateAndPropagatesAuthorizationDenial(t *testing.T) {
	tenant, createdID := presetPublishID(t, "shale.Tenant"), presetPublishID(t, "shale.CameraPreset")
	path := presetPublishFile(t, presetPublishDocument(tenant.String(), false))
	var received *api.CameraPresetCreateRequest
	calls := 0
	connection := &presetPublishConnector{conn: presetPublishConnection{invoke: func(ctx context.Context, method string, in, out any) error {
		calls++
		require.Equal(t, api.CameraPresetService_Create_FullMethodName, method)
		received = proto.Clone(in.(*api.CameraPresetCreateRequest)).(*api.CameraPresetCreateRequest)
		row := out.(*api.CameraPreset)
		row.SetId(createdID.Bytes())
		row.SetPresetKey(received.GetPresetKey())
		row.SetRevision(received.GetRevision())
		return nil
	}}}
	var out bytes.Buffer
	require.NoError(t, publishCameraPreset(t.Context(), &out, connection, cameraPresetPublishOptions{file: path}))
	require.Equal(t, 1, calls)
	require.Equal(t, 1, connection.closed)
	require.Equal(t, tenant.Bytes(), received.GetTenant().GetId())
	require.Contains(t, out.String(), createdID.String())
	connection.conn = presetPublishConnection{invoke: func(context.Context, string, any, any) error {
		return status.Error(codes.PermissionDenied, "not authorized")
	}}
	out.Reset()
	err := publishCameraPreset(t.Context(), &out, connection, cameraPresetPublishOptions{file: path})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.Equal(t, 2, connection.closed)
	require.Empty(t, out.String())
}

func TestCameraPresetAndPolicyPublicationCommandsAreMounted(t *testing.T) {
	for _, group := range []string{"camera-preset", "camera-policy"} {
		count := 0
		for _, command := range Cmd(&cmd.Config{}).Commands {
			if command.Name == group {
				count++
			}
		}
		require.Equal(t, 1, count, "publication groups belong only to the tenant API")
	}
	for _, path := range [][]string{{"camera-preset", "publish"}, {"camera-preset", "create"}, {"camera-policy", "create"}, {"camera-policy", "activate"}, {"camera-policy", "enroll"}} {
		out, err := run(t, &cmd.Config{}, "", append(path, "--help")...)
		require.NoError(t, err, path)
		require.Contains(t, out, path[1])
	}
}
