package drivers

import (
	"net/http"
	"sync/atomic"
	"testing"
)

func TestOnlyApprovedPinnedC420UsesDeviceTimeAndConstructionDoesNoIO(t *testing.T) {
	var calls atomic.Int32
	s, pin := factoryWeb(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	for _, pinned := range []bool{false, true} {
		o := pinnedWebOptions(t, s, pin)
		if !pinned {
			o.DeviceTLSIdentity = nil
		}
		d, err := New(o)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(d.Close)
		if d.baseline.UseDeviceTimeForAuthentication != pinned {
			t.Fatal("device time mode escaped the approved C420 path")
		}
	}
	if calls.Load() != 0 {
		t.Fatal("driver constructor performed camera I/O")
	}
}
