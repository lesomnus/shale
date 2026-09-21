package core

import (
	"context"
	"time"

	"github.com/lesomnus/z"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/shale/api"
)

// Keys and policies (§35.5): rotation, and activating one version of each
// policy.

type coreSigningKey struct {
	Core
	api.SigningKeyServiceServer
}

func (s Core) SigningKey() api.SigningKeyServiceServer {
	return coreSigningKey{s, s.Next().SigningKey()}
}

// Rotate adds a key. Without `immediate` the new key is ACTIVE until every
// live node reports holding it, then the leader promotes it to SIGNING and
// retires the old one after the longest token lifetime (§33.3, the leader's
// job in spin.go). With `immediate`, for a compromised key, it signs at
// once and the old key is retired at once.
func (s coreSigningKey) Rotate(ctx context.Context, req *api.SigningKeyRotateRequest) (*api.SigningKey, error) {
	if _, err := actor(ctx); err != nil {
		return nil, err
	}

	var out *api.SigningKey
	err := s.ownTx(ctx, func(ctx context.Context, own api.Server) error {
		if req.GetImmediate() {
			vs, err := own.SigningKey().List(ctx, api.SigningKeyListRequest_builder{Size: 100}.Build())
			if err != nil {
				return err
			}
			for _, v := range vs.GetItems() {
				if v.GetState() == api.SigningKeyState_SIGNING_KEY_STATE_RETIRED {
					continue
				}
				if _, err := own.SigningKey().Patch(ctx, api.SigningKeyPatchRequest_builder{
					Ref:              api.SigningKeyRef_builder{Id: v.GetId()}.Build(),
					State:            z.Ptr(api.SigningKeyState_SIGNING_KEY_STATE_RETIRED),
					DateRetired:      timestamppb.New(s.d.now()),
					DateUpdatedForce: z.Ptr(true),
				}.Build()); err != nil {
					return err
				}
			}
		}
		v, err := s.d.Keys.Create(ctx, own, req.GetImmediate())
		out = v

		return err
	})
	if err != nil {
		return nil, err
	}

	return out, nil
}

type corePlacementPolicy struct {
	Core
	api.PlacementPolicyServiceServer
}

func (s Core) PlacementPolicy() api.PlacementPolicyServiceServer {
	return corePlacementPolicy{s, s.Next().PlacementPolicy()}
}

func (s corePlacementPolicy) Activate(ctx context.Context, req *api.PlacementPolicyActivateRequest) (*api.PlacementPolicy, error) {
	if _, err := actor(ctx); err != nil {
		return nil, err
	}
	target, err := s.PlacementPolicyServiceServer.Get(ctx, api.PlacementPolicyGetRequest_builder{Ref: req.GetRef()}.Build())
	if err != nil {
		return nil, err
	}
	var out *api.PlacementPolicy
	err = s.tx(ctx, func(ctx context.Context, nx api.Server) error {
		vs, err := nx.PlacementPolicy().List(ctx, api.PlacementPolicyListRequest_builder{Size: 100}.Build())
		if err != nil {
			return err
		}
		for _, v := range vs.GetItems() {
			if v.GetActive() && string(v.GetId()) != string(target.GetId()) {
				if _, err := nx.PlacementPolicy().Patch(ctx, api.PlacementPolicyPatchRequest_builder{
					Ref: api.PlacementPolicyRef_builder{Id: v.GetId()}.Build(), Active: z.Ptr(false), DateUpdatedForce: z.Ptr(true),
				}.Build()); err != nil {
					return err
				}
			}
		}
		v, err := nx.PlacementPolicy().Patch(ctx, api.PlacementPolicyPatchRequest_builder{
			Ref: api.PlacementPolicyRef_builder{Id: target.GetId()}.Build(), Active: z.Ptr(true),
			DateActivated: timestamppb.New(s.d.now()), DateUpdatedForce: z.Ptr(true),
		}.Build())
		out = v

		return err
	})
	if err != nil {
		return nil, err
	}
	s.invalidatePolicies()

	return out, nil
}

type coreUploadPolicy struct {
	Core
	api.UploadPolicyServiceServer
}

func (s Core) UploadPolicy() api.UploadPolicyServiceServer {
	return coreUploadPolicy{s, s.Next().UploadPolicy()}
}

func (s coreUploadPolicy) Activate(ctx context.Context, req *api.UploadPolicyActivateRequest) (*api.UploadPolicy, error) {
	if _, err := actor(ctx); err != nil {
		return nil, err
	}
	target, err := s.UploadPolicyServiceServer.Get(ctx, api.UploadPolicyGetRequest_builder{Ref: req.GetRef()}.Build())
	if err != nil {
		return nil, err
	}
	var out *api.UploadPolicy
	err = s.tx(ctx, func(ctx context.Context, nx api.Server) error {
		vs, err := nx.UploadPolicy().List(ctx, api.UploadPolicyListRequest_builder{Size: 100}.Build())
		if err != nil {
			return err
		}
		for _, v := range vs.GetItems() {
			if v.GetActive() && string(v.GetId()) != string(target.GetId()) {
				if _, err := nx.UploadPolicy().Patch(ctx, api.UploadPolicyPatchRequest_builder{
					Ref: api.UploadPolicyRef_builder{Id: v.GetId()}.Build(), Active: z.Ptr(false), DateUpdatedForce: z.Ptr(true),
				}.Build()); err != nil {
					return err
				}
			}
		}
		v, err := nx.UploadPolicy().Patch(ctx, api.UploadPolicyPatchRequest_builder{
			Ref: api.UploadPolicyRef_builder{Id: target.GetId()}.Build(), Active: z.Ptr(true),
			DateActivated: timestamppb.New(s.d.now()), DateUpdatedForce: z.Ptr(true),
		}.Build())
		out = v

		return err
	})
	if err != nil {
		return nil, err
	}
	s.invalidatePolicies()

	// Stored profiles are re-clamped on the next Negotiate: bumping every
	// set's profile_version makes producers negotiate again (§12.6).
	sets, err := s.own(ctx).Set().List(ctx, api.SetListRequest_builder{Size: 100}.Build())
	if err == nil {
		for _, st := range sets.GetItems() {
			v := st.GetProfileVersion() + 1
			s.own(ctx).Set().Patch(ctx, api.SetPatchRequest_builder{
				Ref: api.SetRef_builder{Id: st.GetId()}.Build(), ProfileVersion: &v, DateUpdatedForce: z.Ptr(true),
			}.Build())
		}
	}

	return out, nil
}

type coreAddressPolicy struct {
	Core
	api.AddressPolicyServiceServer
}

func (s Core) AddressPolicy() api.AddressPolicyServiceServer {
	return coreAddressPolicy{s, s.Next().AddressPolicy()}
}

func (s coreAddressPolicy) Activate(ctx context.Context, req *api.AddressPolicyActivateRequest) (*api.AddressPolicy, error) {
	if _, err := actor(ctx); err != nil {
		return nil, err
	}
	target, err := s.AddressPolicyServiceServer.Get(ctx, api.AddressPolicyGetRequest_builder{Ref: req.GetRef()}.Build())
	if err != nil {
		return nil, err
	}
	var out *api.AddressPolicy
	err = s.tx(ctx, func(ctx context.Context, nx api.Server) error {
		vs, err := nx.AddressPolicy().List(ctx, api.AddressPolicyListRequest_builder{Size: 100}.Build())
		if err != nil {
			return err
		}
		for _, v := range vs.GetItems() {
			if v.GetActive() && string(v.GetId()) != string(target.GetId()) {
				if _, err := nx.AddressPolicy().Patch(ctx, api.AddressPolicyPatchRequest_builder{
					Ref: api.AddressPolicyRef_builder{Id: v.GetId()}.Build(), Active: z.Ptr(false), DateUpdatedForce: z.Ptr(true),
				}.Build()); err != nil {
					return err
				}
			}
		}
		v, err := nx.AddressPolicy().Patch(ctx, api.AddressPolicyPatchRequest_builder{
			Ref: api.AddressPolicyRef_builder{Id: target.GetId()}.Build(), Active: z.Ptr(true),
			DateActivated: timestamppb.New(s.d.now()), DateUpdatedForce: z.Ptr(true),
		}.Build())
		out = v

		return err
	})
	if err != nil {
		return nil, err
	}
	s.invalidatePolicies()

	return out, nil
}

// tokenLifetimeMax is the longest a token can live, which is how long a
// retired key must still verify (§33.3).
func tokenLifetimeMax(b Bounds) time.Duration {
	alloc := b.HorizonMax + 10*time.Minute + b.AbandonMax + 5*time.Minute
	if b.PublishTokenTTL > alloc {
		return b.PublishTokenTTL
	}

	return alloc
}
