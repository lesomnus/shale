package cameracredential_test

import (
	"context"
	"testing"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/server/pd"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// Exercise the generated wrapper rather than assuming that a secret field
// annotation also covers every ordinary response and streaming shape.
func TestVersionGeneratedResponsesAreMetadataOnly(t *testing.T) {
	row := api.CameraCredentialVersion_builder{
		Version: "v1", IntentKey: "prepare-1", Access: "read", Backend: "cp-aes256gcm-hkdf-sha256-v1",
		SealedEnvelope: []byte("synthetic-sealed-envelope"), ImportTag: []byte("synthetic-keyed-import-tag"),
		RequestHash: []byte("nonsecret-metadata-fingerprint"), PrepareTag: []byte("synthetic-keyed-username-tag"),
	}.Build()
	fake := &versionRows{row: row}
	public := pd.NewSecret(versionServer{rows: fake}).CameraCredentialVersion()
	ctx := context.Background()
	for name, call := range map[string]func() (*api.CameraCredentialVersion, error){
		"get": func() (*api.CameraCredentialVersion, error) {
			return public.Get(ctx, &api.CameraCredentialVersionGetRequest{})
		},
		"add": func() (*api.CameraCredentialVersion, error) {
			return public.Add(ctx, &api.CameraCredentialVersionAddRequest{})
		},
		"patch": func() (*api.CameraCredentialVersion, error) {
			return public.Patch(ctx, &api.CameraCredentialVersionPatchRequest{})
		},
		"apply": func() (*api.CameraCredentialVersion, error) {
			return public.Apply(ctx, &api.CameraCredentialVersionApplyRequest{})
		},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := call()
			if err != nil {
				t.Fatal(err)
			}
			assertMetadata(t, got)
		})
	}
	listed, err := public.List(ctx, &api.CameraCredentialVersionListRequest{})
	if err != nil || len(listed.GetItems()) != 2 {
		t.Fatal("list failed")
	}
	for _, got := range listed.GetItems() {
		assertMetadata(t, got)
	}
	stream := &versionStream{}
	if err := public.Watch(&api.CameraCredentialVersionWatchRequest{}, stream); err != nil {
		t.Fatal(err)
	}
	if stream.got == nil || len(stream.got.GetItems()) != 1 {
		t.Fatal("watch did not deliver its snapshot")
	}
	assertMetadata(t, stream.got.GetItems()[0].GetValue())
	if len(row.GetSealedEnvelope()) == 0 || len(row.GetImportTag()) == 0 || len(row.GetRequestHash()) == 0 || len(row.GetPrepareTag()) == 0 {
		t.Fatal("ordinary redaction modified the persisted private record")
	}
}

func assertMetadata(t *testing.T, row *api.CameraCredentialVersion) {
	t.Helper()
	if row == nil || row.GetVersion() != "v1" || row.GetIntentKey() != "prepare-1" {
		t.Fatal("redaction lost ordinary reference metadata")
	}
	if len(row.GetSealedEnvelope()) != 0 || len(row.GetImportTag()) != 0 || len(row.GetRequestHash()) != 0 || len(row.GetPrepareTag()) != 0 {
		t.Fatal("ordinary response contains sealed credential storage")
	}
}

type versionServer struct {
	api.Server
	rows *versionRows
}

func (s versionServer) CameraCredentialVersion() api.CameraCredentialVersionServiceServer {
	return s.rows
}

type versionRows struct {
	api.UnimplementedCameraCredentialVersionServiceServer
	row *api.CameraCredentialVersion
}

func (s *versionRows) value() *api.CameraCredentialVersion {
	return proto.Clone(s.row).(*api.CameraCredentialVersion)
}
func (s *versionRows) Get(context.Context, *api.CameraCredentialVersionGetRequest) (*api.CameraCredentialVersion, error) {
	return s.value(), nil
}
func (s *versionRows) Add(context.Context, *api.CameraCredentialVersionAddRequest) (*api.CameraCredentialVersion, error) {
	return s.value(), nil
}
func (s *versionRows) Patch(context.Context, *api.CameraCredentialVersionPatchRequest) (*api.CameraCredentialVersion, error) {
	return s.value(), nil
}
func (s *versionRows) Apply(context.Context, *api.CameraCredentialVersionApplyRequest) (*api.CameraCredentialVersion, error) {
	return s.value(), nil
}
func (s *versionRows) List(context.Context, *api.CameraCredentialVersionListRequest) (*api.CameraCredentialVersionListResponse, error) {
	return api.CameraCredentialVersionListResponse_builder{Items: []*api.CameraCredentialVersion{s.value(), s.value()}}.Build(), nil
}
func (s *versionRows) Watch(_ *api.CameraCredentialVersionWatchRequest, stream grpc.ServerStreamingServer[api.CameraCredentialVersionWatchResponse]) error {
	return stream.Send(api.CameraCredentialVersionWatchResponse_builder{
		Items: []*api.CameraCredentialVersionWatchItem{api.CameraCredentialVersionWatchItem_builder{Value: s.value()}.Build()},
	}.Build())
}

type versionStream struct {
	grpc.ServerStream
	got *api.CameraCredentialVersionWatchResponse
}

func (s *versionStream) Send(value *api.CameraCredentialVersionWatchResponse) error {
	s.got = value
	return nil
}
