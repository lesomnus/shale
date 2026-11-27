package e2e_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/shale/api"
	"github.com/stretchr/testify/require"
)

// A camera job's publication patches its Source with the version it read,
// and every heartbeat of the Source's producer writes the Source's
// starvation outside any transaction. Publish holds the Source before it
// reads it, so a heartbeat meanwhile waits for the publication instead of
// moving the version under it: a hundred publications, one after another,
// beside a producer heartbeating every few milliseconds about the Source,
// are each accepted, and every heartbeat is stored. On SQLite a heartbeat
// waited for the publication's transaction before the hold too; it is on
// PostgreSQL (SHALE_E2E_DB_DSN) that a heartbeat's write would otherwise
// commit inside one.
func TestPublishWhileTheProducerHeartbeats(t *testing.T) {
	c := cameraControlCluster(t)
	ctx := t.Context()
	admin := c.dial("@acme/admin")
	set := sourceCatalogSet(t, admin, "publish-beside-heartbeats")
	host := sourceCatalogAdopt(t, c, admin, set, "publish-beside-heartbeats-owner")
	a := sourceCatalogAssignment(t, admin, cameraNetwork(t, admin, "publish-beside-heartbeats-lan", nil), host)
	pub, app, owner := liveEpochApply(t, c, admin, a, "beside")
	source := pub.GetBinding().GetSource().GetId()

	beating, stop := context.WithCancel(ctx)
	var (
		wg     sync.WaitGroup
		beats  int64
		failed error
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		producers := api.NewProducerServiceClient(owner)
		for beating.Err() == nil {
			_, err := producers.Heartbeat(beating, api.ProducerHeartbeatRequest_builder{Sources: []*api.SourceReport{
				api.SourceReport_builder{SourceId: source, CaptureEpoch: app.GetCaptureEpoch(), Episodes: 1, SecondsTotal: 1, InputUp: true}.Build(),
			}}.Build())
			if beating.Err() != nil {
				return
			}
			if err != nil {
				failed = err
				return
			}
			beats++
			time.Sleep(2 * time.Millisecond)
		}
	}()

	profile := pub.GetJob().GetObserved().GetEffective()
	prior := pub.GetJob()
	for i := range 100 {
		v := lifecyclePublish(t, admin, a, pub.GetBinding(), prior, "rtsp://192.0.2.10/beside", profile, nil, fmt.Sprintf("beside-%d", i))
		prior = v.GetJob()
	}
	stop()
	wg.Wait()
	require.NoError(t, failed, "a heartbeat beside the publications")
	require.Positive(t, beats)
	t.Logf("%d heartbeats beside 100 publications", beats)

	// Every heartbeat's starvation was stored: none was lost to, or lost,
	// a publication.
	got, err := api.NewSourceServiceClient(admin).Get(ctx, api.SourceGetRequest_builder{Ref: api.SourceRef_builder{Id: source}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, beats, got.GetStarvation().GetSecondsTotal(), "each heartbeat counts one second")
}
