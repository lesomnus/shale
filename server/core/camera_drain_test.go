package core

import (
	"fmt"
	"testing"
	"time"

	"github.com/lesomnus/shale/api"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCameraDrainHistoryBoundsAndImmutableDeadlines(t *testing.T) {
	now := time.Now().UTC()
	makeApp := func(seq int64) *api.CameraApplication {
		return api.CameraApplication_builder{Sequence: seq, CaptureEpoch: fmt.Sprintf("%032x", seq), IntentKey: fmt.Sprint(seq), DateStarted: timestamppb.New(now.Add(-time.Minute)), SegmentProfile: api.SegmentProfile_builder{MaxBitrate: 2_000_000, DurationSeconds: 10}.Build()}.Build()
	}
	b := api.CameraBinding_builder{StartingApplication: makeApp(21), AppliedApplication: makeApp(20)}.Build()
	for i := int64(1); i < 20; i++ {
		b.SetPreviousApplications(append(b.GetPreviousApplications(), cameraDrainApplication(makeApp(i), now.Add(-time.Minute))))
	}
	history := cameraDrainHistory(b, now)
	require.Len(t, history, 16, "retired capture processes cannot accumulate unbounded durable history")
	require.Equal(t, int64(21), history[0].GetSequence())
	require.Equal(t, int64(6), history[len(history)-1].GetSequence())
	require.Nil(t, b.GetStartingApplication().GetDrainCutoff(), "building history never mutates the current caller-owned application")
	var old *api.CameraApplication
	for _, app := range history {
		if app.GetSequence() == 21 {
			old = proto.Clone(app).(*api.CameraApplication)
		}
	}
	b.SetPreviousApplications(history)
	b.SetStartingApplication(makeApp(22))
	b.SetAppliedApplication(cameraDrainApplication(b.GetAppliedApplication(), now))
	next := cameraDrainHistory(b, now.Add(2*time.Minute))
	for _, app := range next {
		if app.GetSequence() == 21 {
			require.True(t, proto.Equal(old, app), "another Begin/retry cannot extend an already retired cutoff or profile")
		}
	}
	expired := cameraDrainHistory(b, now.Add(6*time.Minute))
	for _, app := range expired {
		require.NotEqual(t, int64(21), app.GetSequence(), "expired historical tickets are not revived by a later current capture")
	}
	for _, app := range history {
		require.Equal(t, int64(2_000_000), app.GetSegmentProfile().GetMaxBitrate(), "historical storage ceilings stay frozen")
	}
}
