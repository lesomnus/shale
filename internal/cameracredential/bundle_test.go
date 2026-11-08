package cameracredential

import (
	"context"
	"testing"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/internal/camerasecret"
	"github.com/stretchr/testify/require"
)

func newBundleFixture(t *testing.T) (*sqlFixture, func(string) pdid.Id) {
	f := newSQLFixture(t)
	ctx := t.Context()
	tenant := api.TenantRef_builder{Id: f.subject.TenantID.Bytes()}.Build()
	set, err := f.server.Set().Add(ctx, api.SetAddRequest_builder{Id: pdid.New(7).Bytes(), Tenant: tenant, Alias: "bundle-set"}.Build())
	require.NoError(t, err)
	producer, err := f.server.Producer().Add(ctx, api.ProducerAddRequest_builder{Id: pdid.New(22).Bytes(), Tenant: tenant, Set: api.SetRef_builder{Id: set.GetId()}.Build(), Alias: "bundle-producer", HardwareId: "bundle-hardware"}.Build())
	require.NoError(t, err)
	device, err := f.db.CameraDevice.Get(ctx, f.subject.DeviceID.Uuid())
	require.NoError(t, err)
	assignment, err := f.server.CameraAssignment().Add(ctx, api.CameraAssignmentAddRequest_builder{Id: pdid.New(28).Bytes(), Tenant: tenant, Alias: "bundle-assignment", Set: api.SetRef_builder{Id: set.GetId()}.Build(), Producer: api.ProducerRef_builder{Id: producer.GetId()}.Build(), Network: api.CameraNetworkRef_builder{Id: device.NetworkId[:]}.Build(), WorkerSlot: "local", Generation: 1}.Build())
	require.NoError(t, err)
	ordinal := int32(0)
	newBinding := func(alias string) pdid.Id {
		source, err := f.server.Source().Add(ctx, api.SourceAddRequest_builder{Id: pdid.New(8).Bytes(), Tenant: tenant, Alias: alias, Ordinal: ordinal, Set: api.SetRef_builder{Id: set.GetId()}.Build()}.Build())
		ordinal++
		require.NoError(t, err)
		binding, err := f.server.CameraBinding().Add(ctx, api.CameraBindingAddRequest_builder{Id: pdid.New(29).Bytes(), Tenant: tenant, Alias: alias, LogicalKey: alias, SourceAlias: alias, Assignment: api.CameraAssignmentRef_builder{Id: assignment.GetId()}.Build(), Device: api.CameraDeviceRef_builder{Id: f.subject.DeviceID.Bytes()}.Build(), Source: api.SourceRef_builder{Id: source.GetId()}.Build()}.Build())
		require.NoError(t, err)
		id, err := pdid.From(binding.GetId())
		require.NoError(t, err)
		return id
	}
	return f, newBinding
}

func TestSQLStoreBundleDurabilityAndNoImplicitActivation(t *testing.T) {
	f, newBinding := newBundleFixture(t)
	ctx := t.Context()
	r := BundleRequest{TenantID: f.subject.TenantID, DeviceID: f.subject.DeviceID, BindingID: newBinding("first"), IntentKey: "bundle-first", InitialUsername: []byte("admin")}
	generated := 0
	generate := func(context.Context) (Material, error) {
		generated++
		return NewMaterial("admin", "one-installed-password")
	}
	require.ErrorIs(t, f.tx(func(store Store) error { _, err := PrepareBundle(ctx, store, r, generate); return err }), ErrNotFound)
	require.Zero(t, generated, "an initialized camera without credentials must wait for import")
	count, err := f.db.CameraCredentialVersion.Query().Count(ctx)
	require.NoError(t, err)
	require.Zero(t, count)
	r.AllowActivation = true
	var first, replay, second Bundle
	require.NoError(t, f.tx(func(s Store) (err error) { first, err = PrepareBundle(ctx, s, r, generate); return err }))
	require.NoError(t, f.tx(func(s Store) (err error) { replay, err = PrepareBundle(ctx, s, r, generate); return err }))
	require.Equal(t, first, replay)
	require.Equal(t, 1, generated)
	// A purpose-specific imported account must survive later enrollment;
	// fallback reuse may not silently replace it with the initial account.
	management := Subject{TenantID: r.TenantID, DeviceID: r.DeviceID, Purpose: camerasecret.Management}
	imported := sqlMaterial(t, "imported-manager", "separate-installed-password")
	var importedVersion Version
	require.NoError(t, f.tx(func(s Store) (err error) {
		importedVersion, err = s.Import(ctx, ImportRequest{Intent: Intent{Subject: management, Key: "import-existing-manager", Access: camera.AccessConfigure}, Material: imported, ExpectedCurrent: &first.Management})
		return err
	}))
	r.BindingID = newBinding("second")
	r.IntentKey = "bundle-second"
	r.InitialUsername = []byte("new-activation-option")
	r.AllowActivation = false
	require.NoError(t, f.tx(func(s Store) (err error) { second, err = PrepareBundle(ctx, s, r, generate); return err }))
	require.Equal(t, 1, generated, "a second logical binding cannot rotate the device account")
	require.NotEqual(t, first.Streaming.Subject.BindingID, second.Streaming.Subject.BindingID)
	require.NoError(t, f.tx(func(s Store) error {
		got, err := s.Resolve(ctx, second.Management)
		require.Equal(t, imported, got)
		return err
	}))
	require.NoError(t, f.tx(func(s Store) error {
		got, err := s.Resolve(ctx, importedVersion.Ref)
		require.Equal(t, imported, got)
		return err
	}))
	require.NoError(t, f.tx(func(s Store) error {
		got, err := s.Resolve(ctx, first.Streaming)
		require.NoError(t, err)
		return got.Use(ctx, func(m camera.CredentialMaterial) error {
			require.Equal(t, "one-installed-password", m.Password)
			return nil
		})
	}))
}

func TestSQLStoreBundleInitializedImportsWithoutInitialAdmin(t *testing.T) {
	for _, allow := range []bool{false, true} {
		t.Run(map[bool]string{false: "initialized", true: "activation-allowed"}[allow], func(t *testing.T) {
			f, newBinding := newBundleFixture(t)
			ctx := t.Context()
			binding := newBinding("imported-binding")
			manager := sqlMaterial(t, "existing-manager", "existing-management-password")
			viewer := sqlMaterial(t, "existing-viewer", "existing-stream-password")
			for _, account := range []struct {
				subject  Subject
				access   camera.AccessLevel
				material Material
			}{
				{Subject{TenantID: f.subject.TenantID, DeviceID: f.subject.DeviceID, Purpose: camerasecret.Management}, camera.AccessConfigure, manager},
				{Subject{TenantID: f.subject.TenantID, DeviceID: f.subject.DeviceID, BindingID: binding, Purpose: camerasecret.Streaming}, camera.AccessRead, viewer},
			} {
				require.NoError(t, f.tx(func(s Store) error {
					_, err := s.Import(ctx, ImportRequest{Intent: Intent{Subject: account.subject, Key: "original-import", Access: account.access}, Material: account.material})
					return err
				}))
			}
			r := BundleRequest{TenantID: f.subject.TenantID, DeviceID: f.subject.DeviceID, BindingID: binding, IntentKey: "initialized-enrollment", InitialUsername: []byte("new-account-only"), AllowActivation: allow}
			var bundle, replay Bundle
			generated := 0
			generate := func(context.Context) (Material, error) {
				generated++
				return NewMaterial("new-account-only", "unused-new-activation-password")
			}
			require.NoError(t, f.tx(func(s Store) (err error) { bundle, err = PrepareBundle(ctx, s, r, generate); return err }))
			require.NoError(t, f.tx(func(s Store) (err error) { replay, err = PrepareBundle(ctx, s, r, generate); return err }))
			require.Equal(t, bundle, replay)
			if !allow {
				require.Zero(t, generated)
				require.True(t, bundle.InitialAdmin.CredentialID.IsZero())
			}
			require.NoError(t, f.tx(func(s Store) error {
				got, err := s.Resolve(ctx, bundle.Management)
				require.Equal(t, manager, got)
				return err
			}))
			require.NoError(t, f.tx(func(s Store) error {
				got, err := s.Resolve(ctx, bundle.Streaming)
				require.Equal(t, viewer, got)
				return err
			}))
		})
	}
}
