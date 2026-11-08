package cameraworker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type factoryDevice struct {
	call func(string, context.Context, camera.DeviceHint) error
}

func (d factoryDevice) Inspect(ctx context.Context, h camera.DeviceHint, _ camera.CredentialLease) (camera.Inspection, error) {
	return camera.Inspection{}, d.call("inspect", ctx, h)
}
func (d factoryDevice) Plan(ctx context.Context, i camera.Inspection, _ camera.DesiredState) (camera.ChangePlan, error) {
	return camera.ChangePlan{}, d.call("plan", ctx, i.Device)
}
func (d factoryDevice) Apply(ctx context.Context, h camera.DeviceHint, _ camera.CredentialLease, _ camera.ChangePlan) (camera.CurrentState, error) {
	return camera.CurrentState{}, d.call("apply", ctx, h)
}
func (d factoryDevice) Activation(ctx context.Context, h camera.DeviceHint) (camera.ActivationState, error) {
	return camera.ActivationReady, d.call("activation", ctx, h)
}
func (d factoryDevice) Activate(ctx context.Context, h camera.DeviceHint, _ camera.CredentialLease) error {
	return d.call("activate", ctx, h)
}
func (d factoryDevice) PlanInitialization(ctx context.Context, h camera.DeviceHint, _ camera.CredentialLease, _ camera.DeviceSettings) (camera.InitializationPlan, error) {
	return camera.InitializationPlan{}, d.call("plan_initialization", ctx, h)
}
func (d factoryDevice) ApplyInitialization(ctx context.Context, h camera.DeviceHint, _ camera.CredentialLease, _ camera.InitializationPlan) (camera.DeviceSettings, error) {
	return camera.DeviceSettings{}, d.call("apply_initialization", ctx, h)
}
func (d factoryDevice) Verify(ctx context.Context, r camera.MediaRequest) (camera.VerifiedState, error) {
	return camera.VerifiedState{}, d.call("verify", ctx, r.Device)
}

func (d factoryDevice) adapters(release func()) DeviceAdapters {
	return DeviceAdapters{Inspector: d, Configurator: d, Bootstrapper: d, Initializer: d, Verifier: d, Release: release}
}

type factoryOperation struct {
	name string
	call func(context.Context, LeasedDevices, camera.DeviceHint, *api.CameraWorkerWork) error
}

func factoryMediaRequest(h camera.DeviceHint, w *api.CameraWorkerWork) camera.MediaRequest {
	scope := enrollmentScope(w.GetAssignment())
	return camera.MediaRequest{Device: h, Scope: scope, NetworkScope: scope.NetworkID, Profile: camera.MediaProfile{StreamURI: "rtsp://192.0.2.200/stream"}}
}

func factoryOperations() []factoryOperation {
	return []factoryOperation{
		{"inspect", func(ctx context.Context, d LeasedDevices, h camera.DeviceHint, _ *api.CameraWorkerWork) error {
			_, err := d.Inspect(ctx, h, nil)
			return err
		}},
		{"plan", func(ctx context.Context, d LeasedDevices, h camera.DeviceHint, _ *api.CameraWorkerWork) error {
			_, err := d.Plan(ctx, camera.Inspection{Device: h}, camera.DesiredState{})
			return err
		}},
		{"apply", func(ctx context.Context, d LeasedDevices, h camera.DeviceHint, _ *api.CameraWorkerWork) error {
			_, err := d.Apply(ctx, h, nil, camera.ChangePlan{})
			return err
		}},
		{"activation", func(ctx context.Context, d LeasedDevices, h camera.DeviceHint, _ *api.CameraWorkerWork) error {
			_, err := d.Activation(ctx, h)
			return err
		}},
		{"activate", func(ctx context.Context, d LeasedDevices, h camera.DeviceHint, _ *api.CameraWorkerWork) error {
			return d.Activate(ctx, h, nil)
		}},
		{"plan_initialization", func(ctx context.Context, d LeasedDevices, h camera.DeviceHint, _ *api.CameraWorkerWork) error {
			_, err := d.PlanInitialization(ctx, h, nil, camera.DeviceSettings{})
			return err
		}},
		{"apply_initialization", func(ctx context.Context, d LeasedDevices, h camera.DeviceHint, _ *api.CameraWorkerWork) error {
			_, err := d.ApplyInitialization(ctx, h, nil, camera.InitializationPlan{})
			return err
		}},
		{"verify", func(ctx context.Context, d LeasedDevices, h camera.DeviceHint, w *api.CameraWorkerWork) error {
			_, err := d.Verify(ctx, factoryMediaRequest(h, w))
			return err
		}},
	}
}

func TestDeviceFactoryUsesFreshNetworkInputsAndTransportForEveryOperation(t *testing.T) {
	for _, operation := range factoryOperations() {
		t.Run(operation.name, func(t *testing.T) {
			e, w := checkpointFixture(t)
			w.GetNetwork().SetCidrs([]string{"192.0.2.0/24"})
			w.GetJob().GetInputs().SetBootstrap(api.CameraBootstrapProfile_builder{CompatibilityProfile: "pinned-driver", WebScheme: "https", WebPort: 8443}.Build())
			h := deviceHint(w)
			factories, calls, releases := 0, 0, 0
			d := LeasedDevices{Execution: e, Factory: func(ctx context.Context, fresh *api.CameraWorkerWork) (DeviceAdapters, error) {
				factories++
				require.Equal(t, w.GetNetwork().GetCidrs(), fresh.GetNetwork().GetCidrs())
				require.True(t, proto.Equal(w.GetJob().GetInputs(), fresh.GetJob().GetInputs()))
				require.Equal(t, "pinned-driver", fresh.GetJob().GetInputs().GetBootstrap().GetCompatibilityProfile())
				require.NotSame(t, w, fresh, "the constructor receives a detached fresh grant")
				deadline, ok := ctx.Deadline()
				require.True(t, ok)
				require.Equal(t, fresh.GetJob().GetLeaseExpires().AsTime(), deadline)
				endpoint := fresh.GetDevice().GetEndpoints()[0].GetUri()
				driver := factoryDevice{call: func(name string, driverCtx context.Context, actual camera.DeviceHint) error {
					calls++
					require.Equal(t, calls-1, releases, "the current operation retains its adapters through the driver call")
					require.Equal(t, operation.name, name)
					require.Equal(t, endpoint, actual.Endpoints[0].URL)
					require.Equal(t, h.ID, actual.ID)
					require.Same(t, ctx, driverCtx)
					return nil
				}}
				return driver.adapters(func() { releases++ }), nil
			}}
			require.NoError(t, operation.call(t.Context(), d, h, w))
			w.GetNetwork().SetCidrs([]string{"192.0.2.128/25"})
			w.GetDevice().GetEndpoints()[0].SetUri("http://192.0.2.200/onvif/device_service")
			require.NoError(t, operation.call(t.Context(), d, h, w))
			require.Equal(t, 2, factories)
			require.Equal(t, 2, calls)
			require.Equal(t, 2, releases)
		})
	}
}

func TestDeviceFactoryReleaseOwnsSuccessErrorAndCancellation(t *testing.T) {
	secret := errors.New("factory or device response contains a password")
	for _, operation := range factoryOperations() {
		for _, outcome := range []string{"success", "factory_error", "missing_adapter", "driver_error", "constructor_canceled", "driver_canceled"} {
			t.Run(operation.name+"/"+outcome, func(t *testing.T) {
				e, w := checkpointFixture(t)
				w.GetNetwork().SetCidrs([]string{"192.0.2.0/24"})
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				calls, releases := 0, 0
				d := LeasedDevices{Execution: e, Factory: func(context.Context, *api.CameraWorkerWork) (DeviceAdapters, error) {
					driver := factoryDevice{call: func(_ string, driverCtx context.Context, _ camera.DeviceHint) error {
						calls++
						require.Zero(t, releases, "cleanup must follow the driver call")
						if outcome == "driver_error" {
							return secret
						}
						if outcome == "driver_canceled" {
							cancel()
							<-driverCtx.Done()
							return driverCtx.Err()
						}
						return nil
					}}
					adapters := driver.adapters(func() { releases++ })
					if outcome == "factory_error" {
						return adapters, secret
					}
					if outcome == "missing_adapter" {
						return DeviceAdapters{Release: adapters.Release}, nil
					}
					if outcome == "constructor_canceled" {
						cancel()
					}
					return adapters, nil
				}}
				err := operation.call(ctx, d, deviceHint(w), w)
				require.Equal(t, 1, releases)
				switch outcome {
				case "factory_error", "driver_error":
					require.ErrorIs(t, err, secret)
					require.NotContains(t, err.Error(), "password")
				case "constructor_canceled", "driver_canceled":
					require.ErrorIs(t, err, context.Canceled)
				case "missing_adapter":
					if operation.name == "activation" {
						require.NoError(t, err)
					} else {
						require.Error(t, err)
					}
				default:
					require.NoError(t, err)
				}
				if outcome == "factory_error" || outcome == "constructor_canceled" || outcome == "missing_adapter" {
					require.Zero(t, calls)
				} else {
					require.Equal(t, 1, calls)
				}
			})
		}
	}
}

func TestDeviceFactoryNeverRunsWithoutFreshGrantOrMatchingTransport(t *testing.T) {
	for _, reason := range []string{"denied", "wrong_inventory", "no_endpoint", "expired"} {
		t.Run(reason, func(t *testing.T) {
			e, w := checkpointFixture(t)
			h := deviceHint(w)
			if reason == "denied" {
				e.workers = workerClient{check: func(context.Context, *api.CameraWorkerCheckJobRequest) (*api.CameraWorkerWork, error) {
					return nil, status.Error(codes.PermissionDenied, "revoked")
				}}
			} else if reason == "wrong_inventory" {
				h.ID = "another-inventory-device"
			} else if reason == "no_endpoint" {
				w.GetDevice().SetEndpoints(nil)
			} else {
				w.GetJob().SetLeaseExpires(timestamppb.New(time.Now().Add(-time.Second)))
			}
			factories := 0
			d := LeasedDevices{Execution: e, Factory: func(context.Context, *api.CameraWorkerWork) (DeviceAdapters, error) {
				factories++
				return DeviceAdapters{}, nil
			}}
			_, err := d.Inspect(t.Context(), h, nil)
			require.Error(t, err)
			require.Zero(t, factories)
		})
	}
}

func TestDeviceFactoryVerifyChecksFreshScopeAndPrefixBeforeConstruction(t *testing.T) {
	for _, change := range []func(*camera.MediaRequest){
		func(r *camera.MediaRequest) { r.Profile.StreamURI = "rtsp://192.0.2.20/stream" },
		func(r *camera.MediaRequest) { r.NetworkScope = "another-network" },
		func(r *camera.MediaRequest) { r.Scope.ProducerID = "another-producer" },
	} {
		e, w := checkpointFixture(t)
		w.GetNetwork().SetCidrs([]string{"192.0.2.128/25"})
		r := factoryMediaRequest(deviceHint(w), w)
		change(&r)
		factories := 0
		d := LeasedDevices{Execution: e, Factory: func(context.Context, *api.CameraWorkerWork) (DeviceAdapters, error) {
			factories++
			return DeviceAdapters{}, nil
		}}
		_, err := d.Verify(t.Context(), r)
		require.ErrorIs(t, err, camera.ErrForbidden)
		require.Zero(t, factories)
	}
}

func TestDeviceFactoryRejectsEveryMixedStaticConfiguration(t *testing.T) {
	for _, set := range []struct {
		name string
		set  func(*LeasedDevices, factoryDevice)
	}{
		{"inspector", func(d *LeasedDevices, driver factoryDevice) { d.Inspector = driver }},
		{"configurator", func(d *LeasedDevices, driver factoryDevice) { d.Configurator = driver }},
		{"bootstrapper", func(d *LeasedDevices, driver factoryDevice) { d.Bootstrapper = driver }},
		{"initializer", func(d *LeasedDevices, driver factoryDevice) { d.Initializer = driver }},
		{"verifier", func(d *LeasedDevices, driver factoryDevice) { d.Verifier = driver }},
	} {
		for _, operation := range factoryOperations() {
			t.Run(set.name+"/"+operation.name, func(t *testing.T) {
				e, w := checkpointFixture(t)
				factories, grants, calls := 0, 0, 0
				e.workers = workerClient{check: func(context.Context, *api.CameraWorkerCheckJobRequest) (*api.CameraWorkerWork, error) {
					grants++
					return proto.Clone(w).(*api.CameraWorkerWork), nil
				}}
				d := LeasedDevices{Execution: e, Factory: func(context.Context, *api.CameraWorkerWork) (DeviceAdapters, error) {
					factories++
					return DeviceAdapters{}, nil
				}}
				set.set(&d, factoryDevice{call: func(string, context.Context, camera.DeviceHint) error { calls++; return nil }})
				require.ErrorIs(t, operation.call(t.Context(), d, deviceHint(w), w), camera.ErrInvalid)
				require.Zero(t, factories)
				require.Zero(t, grants)
				require.Zero(t, calls)
			})
		}
	}
}

func TestDeviceFactoryPreservesLegacyStaticAdaptersAndNilCapabilities(t *testing.T) {
	for _, operation := range factoryOperations() {
		t.Run(operation.name, func(t *testing.T) {
			e, w := checkpointFixture(t)
			w.GetNetwork().SetCidrs([]string{"192.0.2.0/24"})
			calls := 0
			driver := factoryDevice{call: func(name string, _ context.Context, _ camera.DeviceHint) error {
				calls++
				require.Equal(t, operation.name, name)
				return nil
			}}
			d := LeasedDevices{Execution: e, Inspector: driver, Configurator: driver, Bootstrapper: driver, Initializer: driver, Verifier: driver}
			require.NoError(t, operation.call(t.Context(), d, deviceHint(w), w))
			require.Equal(t, 1, calls)
		})
	}
	// Optional legacy capabilities have always been usable without an Execution.
	d := LeasedDevices{}
	state, err := d.Activation(t.Context(), camera.DeviceHint{})
	require.NoError(t, err)
	require.Equal(t, camera.ActivationUnknown, state)
	require.ErrorIs(t, d.Activate(t.Context(), camera.DeviceHint{}, nil), camera.ErrManualActivation)
	_, err = d.PlanInitialization(t.Context(), camera.DeviceHint{}, nil, camera.DeviceSettings{})
	require.ErrorIs(t, err, camera.ErrUnsupported)
	_, err = d.ApplyInitialization(t.Context(), camera.DeviceHint{}, nil, camera.InitializationPlan{})
	require.ErrorIs(t, err, camera.ErrUnsupported)
}
