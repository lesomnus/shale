package drivers

import (
	"context"
	"crypto/x509"
	"time"

	"github.com/lesomnus/shale/camera"
)

var _ camera.ClockReader = Clocks{}

// ObserveClock reads the device's own clock at its current HTTP(S) endpoint
// with public ONVIF GetSystemDateAndTime, through this driver's scoped
// transport: no credential, cookie or redirect. It never sets the clock.
func (d *Driver) ObserveClock(ctx context.Context, h camera.DeviceHint) (camera.DeviceClock, error) {
	endpoint, err := d.endpoint(ctx, h)
	if err != nil {
		return camera.DeviceClock{}, err
	}
	return d.baseline.ReadClock(ctx, endpoint.String())
}

// Clocks reads cameras' clocks within a discovery scope: a fresh driver for
// each reading, which dials only hosts inside the scope's prefixes and keeps
// nothing afterwards.
type Clocks struct {
	// Roots is optional camera certificate trust, as in Options.
	Roots *x509.CertPool
	// Timeout bounds one reading; zero is five seconds.
	Timeout time.Duration
}

func (c Clocks) ReadClock(ctx context.Context, scope camera.DiscoveryScope, h camera.DeviceHint) (camera.DeviceClock, error) {
	timeout := c.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	d, err := New(Options{NetworkID: scope.ID, CIDRs: scope.CIDRs, Roots: c.Roots, Timeout: timeout})
	if err != nil {
		return camera.DeviceClock{}, err
	}
	defer d.Close()
	return d.ObserveClock(ctx, h)
}
