package core

import (
	"testing"

	"github.com/lesomnus/shale/api"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// A source added says what its bytes are when a person says so (§7, §38.9):
// a pushed stream of records is read by its content type, and the producer's
// proposal at negotiation fills it only where it is empty. What else a person
// says when adding it is kept as said; the set, the tenant, the site and the
// ordinal are the set's and the CP's.
func TestSourceAddKeepsWhatItIsTold(t *testing.T) {
	f := newCapFixture(t)
	profile := api.SegmentProfile_builder{MaxBitrate: 2_000_000, DurationSeconds: 30, KeyframeIntervalMs: 1000}.Build()
	v, err := f.core.Source().Add(f.ctx, api.SourceAddRequest_builder{
		Set: api.SetRef_builder{Id: f.set.GetId()}.Build(), Alias: "lidar", Name: "Roof lidar", Desc: "points",
		Labels: map[string]string{"robot": "r1"}, Zone: "roof", Profile: profile, ContentType: "application/x-mcap",
	}.Build())
	require.NoError(t, err)
	got, err := f.sink.Source().Get(t.Context(), api.SourceGetRequest_builder{Ref: api.SourceRef_builder{Id: v.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, "application/x-mcap", got.GetContentType())
	require.Equal(t, "Roof lidar", got.GetName())
	require.Equal(t, "points", got.GetDesc())
	require.Equal(t, map[string]string{"robot": "r1"}, got.GetLabels())
	require.Equal(t, "roof", got.GetZone())
	require.True(t, proto.Equal(profile, got.GetProfile()))
	require.Equal(t, int32(len(f.srcs)), got.GetOrdinal())

	// None said is none: a camera's, which an empty value means.
	v, err = f.core.Source().Add(f.ctx, api.SourceAddRequest_builder{Set: api.SetRef_builder{Id: f.set.GetId()}.Build(), Alias: "porch"}.Build())
	require.NoError(t, err)
	require.Empty(t, v.GetContentType())
}
