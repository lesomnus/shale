package storage

import (
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/stretchr/testify/require"
)

func TestUploadAgeRemainsObservableDuringLiveWrite(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)
	sink := &Sink{Id: pdid.New(DomSink)}
	d := &DataPlane{uploads: map[string]*upload{}}
	d.adoptOpen(sink, []OpenFile{{Key: "live-recording", Record: api.LaminaRecord_builder{DateStartedMs: now.Add(-7 * time.Second).UnixMilli()}.Build(), Modified: now}})
	u := d.uploads[d.uploadKey(sink, "live-recording")]
	// HTTP PUT owns this lock throughout a potentially unbounded live upload.
	// Heartbeat age metrics still have to finish while that body is arriving.
	u.mu.Lock()
	defer u.mu.Unlock()
	result := make(chan map[*Sink]time.Duration, 1)
	go func() { result <- d.uploadAges(now) }()
	select {
	case ages := <-result:
		require.Equal(t, 7*time.Second, ages[sink])
	case <-time.After(time.Second):
		t.Fatal("heartbeat metrics waited for an open live upload")
	}
}

func TestUploadAgeConcurrentRecordReplacement(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)
	sink := &Sink{Id: pdid.New(DomSink)}
	u := &upload{sink: sink}
	u.setRecord(api.LaminaRecord_builder{DateStartedMs: now.Add(-time.Second).UnixMilli()}.Build())
	d := &DataPlane{uploads: map[string]*upload{"live": u}}
	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		for i := 0; i < 10000; i++ {
			u.mu.Lock()
			// Resume can replace the pointer and initialize a fresh protobuf while
			// the node samples metrics; readers must not inspect mutable record state.
			record := &api.LaminaRecord{}
			record.SetDateStartedMs(now.Add(-time.Duration(i%2+1) * time.Second).UnixMilli())
			u.setRecord(record)
			u.mu.Unlock()
		}
	}()
	for i := 0; i < 10000; i++ {
		age := d.uploadAges(now)[sink]
		require.True(t, age == time.Second || age == 2*time.Second, "invalid live age snapshot")
	}
	writer.Wait()
}
