package edge

import (
	"context"
	"net/http"
	"net/url"
	"slices"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/discovery"
	"github.com/lesomnus/shale/camera/drivers"
	"github.com/lesomnus/shale/camera/media"
	"github.com/lesomnus/shale/internal/camerabootstrap"
	"github.com/lesomnus/shale/internal/cameratls"
	"github.com/lesomnus/shale/internal/cameraworker"
	"github.com/lesomnus/shale/provision"
	publication "github.com/lesomnus/shale/provision/source"
	"github.com/lesomnus/shale/source"
)

func buildWorker(cfg cameraworker.Config, broker *sourceBroker, options CameraOptions) (runner, error) {
	if options.Roots != nil {
		options.Roots = options.Roots.Clone()
	}
	if cfg.Discoverer == nil {
		cfg.Discoverer = discovery.Discoverer{}
	}
	var worker *cameraworker.Worker
	if cfg.Executor == nil {
		cfg.Executor = cameraworker.ExecutorFunc(func(ctx context.Context, e *cameraworker.Execution) error {
			auth, err := e.Credentials()
			if err != nil {
				return camera.ErrAuthRequired
			}
			client, err := worker.HTTPClient()
			if err != nil {
				return camera.ErrForbidden
			}
			defer client.CloseIdleConnections()
			p := cameraworker.Provisioner{
				Auth: func(*cameraworker.Execution) camera.AuthProvider { return auth },
				Devices: cameraworker.LeasedDevices{Factory: func(_ context.Context, w *api.CameraWorkerWork) (cameraworker.DeviceAdapters, error) {
					network, err := pdid.From(w.GetAssignment().GetNetwork().GetId())
					if err != nil || network.Domain() != 26 {
						return cameraworker.DeviceAdapters{}, camera.ErrInvalid
					}
					var hosts []string
					var deviceID string
					identity := cameratls.Decode(w.GetJob().GetInputs().GetDeviceTlsIdentity())
					if identity != nil {
						device, err := pdid.From(w.GetDevice().GetId())
						if err != nil || device.Domain() != 27 {
							return cameraworker.DeviceAdapters{}, camera.ErrInvalid
						}
						deviceID = device.String()
						for _, endpoint := range w.GetDevice().GetEndpoints() {
							u, err := url.Parse(endpoint.GetUri())
							if err != nil || u.Hostname() == "" {
								return cameraworker.DeviceAdapters{}, camera.ErrInvalid
							}
							if !slices.Contains(hosts, u.Hostname()) {
								hosts = append(hosts, u.Hostname())
							}
						}
					}
					driver, err := drivers.New(drivers.Options{NetworkID: network.String(), CIDRs: w.GetNetwork().GetCidrs(), Bootstrap: camerabootstrap.Decode(w.GetJob().GetInputs().GetBootstrap()), Roots: options.Roots, DeviceTLSIdentity: identity, DeviceID: deviceID, DeviceHosts: hosts})
					if err != nil {
						return cameraworker.DeviceAdapters{}, err
					}
					return cameraworker.DeviceAdapters{Inspector: driver, Configurator: driver, Bootstrapper: driver, Initializer: driver, Verifier: &media.Verifier{AuthProvider: auth, Ffmpeg: options.FFmpeg, Ffprobe: options.FFprobe, NetworkScope: network.String()}, Release: driver.Close}, nil
				}},
				Publisher: func(e *cameraworker.Execution) provision.SourcePublisher {
					w := e.Snapshot()
					a := w.GetAssignment()
					return &publication.Publisher{AssignmentID: edgeID(a.GetId()), Scope: source.Scope{TenantID: edgeID(a.GetTenant().GetId()), SiteID: edgeID(a.GetSite().GetId()), SetID: edgeID(a.GetSet().GetId()), ProducerID: edgeID(a.GetProducer().GetId()), NetworkID: edgeID(a.GetNetwork().GetId())}, Committer: notifyingCommitter{commit: cameraworker.JobCommitter{Execution: e}, broker: broker}, Observer: newJobObserver(e, func() *http.Client { return client }, options)}
				},
			}
			return p.Execute(ctx, e)
		})
	}
	var err error
	worker, err = cameraworker.New(cfg)
	return worker, err
}

func edgeID(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	id, err := pdid.From(raw)
	if err != nil {
		return ""
	}
	return id.String()
}
