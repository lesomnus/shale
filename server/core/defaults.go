package core

import (
	"context"
	"sync"
	"time"

	"github.com/lesomnus/shale/api"
)

// The decided defaults of §36.1. A deployment changes them through the
// policy entities, never by editing these.
const (
	DefaultEpoch           = time.Hour
	DefaultRetentionExpire = 30 * 24 * time.Hour

	DefaultMaxBitrateCap int64 = 32_000_000
	DefaultTargetObject  int64 = 64 << 20
	DefaultMinObject     int64 = 32 << 20
	DefaultMaxObject     int64 = 512 << 20

	DefaultKeyframeMs int64 = 2000
	MinKeyframeMs     int64 = 500
	MaxKeyframeMs     int64 = 4000

	DefaultIdleTimeout    = 30 * time.Second
	MinIdleTimeout        = 10 * time.Second
	MaxIdleTimeout        = 5 * time.Minute
	DefaultAbandonTimeout = 5 * time.Minute
	MinAbandonTimeout     = time.Minute
	MaxAbandonTimeout     = 30 * time.Minute
	DefaultHorizon        = 10 * time.Minute
	MinHorizon            = time.Minute
	MaxHorizon            = 30 * time.Minute

	DefaultMaxOpenAttempts = 32
	DefaultClockTolerance  = 5 * time.Minute
	DefaultReadTokenTTL    = time.Hour
	DefaultViewTokenTTL    = time.Hour
	DefaultPublishTokenTTL = 24 * time.Hour

	DefaultAbandonGrace = time.Hour
	DefaultRowRetention = 30 * 24 * time.Hour

	DefaultNodeDownAfter     = 30 * time.Second
	DefaultProducerDownAfter = 90 * time.Second
	DefaultJoinPendingTTL    = 24 * time.Hour

	DefaultMaxSinkCapacity int64 = 64_000_000_000_000
	DefaultForecastMargin        = 1.5

	// EncoderBurst is one encoder burst, the VBV buffer a capped-VBR encoder
	// may release at once, which max_length allows for (§12.6).
	EncoderBurst = 2 * time.Second

	// Candidates is how many ranked targets an allocation carries, each with
	// its own attempt and token (§13).
	Candidates = 3

	// SizeHintFactor is what a live upload reserves: the expected rate times
	// the duration times this (§12.2).
	SizeHintFactor = 1.2
)

// Bounds are the active UploadPolicy's bounds with the defaults filled in.
type Bounds struct {
	MaxBitrateCap   int64
	TargetObject    int64
	MinObject       int64
	MaxObject       int64
	KeyframeDefault time.Duration
	KeyframeMin     time.Duration
	KeyframeMax     time.Duration
	IdleDefault     time.Duration
	IdleMin         time.Duration
	IdleMax         time.Duration
	AbandonDefault  time.Duration
	AbandonMin      time.Duration
	AbandonMax      time.Duration
	HorizonDefault  time.Duration
	HorizonMin      time.Duration
	HorizonMax      time.Duration
	DefaultMode     api.UploadMode
	MaxOpenAttempts int
	ClockTolerance  time.Duration
	ReadTokenTTL    time.Duration
	ViewTokenTTL    time.Duration
	PublishTokenTTL time.Duration
	Version         int64
}

// DefaultBounds are the bounds with nothing configured.
func DefaultBounds() Bounds {
	return Bounds{
		MaxBitrateCap:   DefaultMaxBitrateCap,
		TargetObject:    DefaultTargetObject,
		MinObject:       DefaultMinObject,
		MaxObject:       DefaultMaxObject,
		KeyframeDefault: time.Duration(DefaultKeyframeMs) * time.Millisecond,
		KeyframeMin:     time.Duration(MinKeyframeMs) * time.Millisecond,
		KeyframeMax:     time.Duration(MaxKeyframeMs) * time.Millisecond,
		IdleDefault:     DefaultIdleTimeout,
		IdleMin:         MinIdleTimeout,
		IdleMax:         MaxIdleTimeout,
		AbandonDefault:  DefaultAbandonTimeout,
		AbandonMin:      MinAbandonTimeout,
		AbandonMax:      MaxAbandonTimeout,
		HorizonDefault:  DefaultHorizon,
		HorizonMin:      MinHorizon,
		HorizonMax:      MaxHorizon,
		DefaultMode:     api.UploadMode_UPLOAD_MODE_LIVE,
		MaxOpenAttempts: DefaultMaxOpenAttempts,
		ClockTolerance:  DefaultClockTolerance,
		ReadTokenTTL:    DefaultReadTokenTTL,
		ViewTokenTTL:    DefaultViewTokenTTL,
		PublishTokenTTL: DefaultPublishTokenTTL,
	}
}

// boundsFrom fills the defaults with what a policy says.
func boundsFrom(p *api.UploadPolicy) Bounds {
	b := DefaultBounds()
	if p == nil {
		return b
	}
	b.Version = p.GetVersion()
	u := p.GetBounds()
	if u == nil {
		return b
	}

	set := func(dst *int64, v int64) {
		if v > 0 {
			*dst = v
		}
	}
	setD := func(dst *time.Duration, secs int64, unit time.Duration) {
		if secs > 0 {
			*dst = time.Duration(secs) * unit
		}
	}

	set(&b.MaxBitrateCap, u.GetMaxBitrateCap())
	set(&b.TargetObject, u.GetTargetObject())
	set(&b.MinObject, u.GetMinObject())
	set(&b.MaxObject, u.GetMaxObject())
	setD(&b.KeyframeDefault, u.GetKeyframeIntervalDefaultMs(), time.Millisecond)
	setD(&b.KeyframeMin, u.GetKeyframeIntervalMinMs(), time.Millisecond)
	setD(&b.KeyframeMax, u.GetKeyframeIntervalMaxMs(), time.Millisecond)
	setD(&b.IdleDefault, u.GetIdleTimeoutDefault(), time.Second)
	setD(&b.IdleMin, u.GetIdleTimeoutMin(), time.Second)
	setD(&b.IdleMax, u.GetIdleTimeoutMax(), time.Second)
	setD(&b.AbandonDefault, u.GetAbandonTimeoutDefault(), time.Second)
	setD(&b.AbandonMin, u.GetAbandonTimeoutMin(), time.Second)
	setD(&b.AbandonMax, u.GetAbandonTimeoutMax(), time.Second)
	setD(&b.HorizonDefault, u.GetAllocationHorizonDefault(), time.Second)
	setD(&b.HorizonMin, u.GetAllocationHorizonMin(), time.Second)
	setD(&b.HorizonMax, u.GetAllocationHorizonMax(), time.Second)
	if u.GetDefaultMode() != api.UploadMode_UPLOAD_MODE_UNSPECIFIED {
		b.DefaultMode = u.GetDefaultMode()
	}
	if u.GetMaxOpenAttempts() > 0 {
		b.MaxOpenAttempts = int(u.GetMaxOpenAttempts())
	}
	setD(&b.ClockTolerance, u.GetClockTolerance(), time.Second)
	setD(&b.ReadTokenTTL, u.GetReadTokenTtl(), time.Second)
	setD(&b.ViewTokenTTL, u.GetViewTokenTtl(), time.Second)
	setD(&b.PublishTokenTTL, u.GetPublishTokenTtl(), time.Second)

	return b
}

// policies caches the active policies for a few seconds, since every
// allocation reads them and they change once in a blue moon.
type policies struct {
	mu      sync.Mutex
	at      time.Time
	upload  Bounds
	place   *api.PlacementParams
	placeV  int64
	address *api.AddressParams
}

const policyTTL = 5 * time.Second

// TrustedProxies is the active address policy's list of proxies whose
// PROXY protocol header is believed (§34.10).
func (d *Deps) TrustedProxies(ctx context.Context) []string {
	_, _, _, address, err := (Core{d: d}).policies(ctx)
	if err != nil || address == nil {
		return nil
	}

	return address.GetTrustedProxies()
}

func (s Core) policies(ctx context.Context) (Bounds, *api.PlacementParams, int64, *api.AddressParams, error) {
	p := &s.d.pol
	p.mu.Lock()
	defer p.mu.Unlock()

	now := s.d.now()
	if now.Sub(p.at) < policyTTL && !p.at.IsZero() {
		return p.upload, p.place, p.placeV, p.address, nil
	}

	up, err := s.d.Own.UploadPolicy().List(ctx, api.UploadPolicyListRequest_builder{Size: 100}.Build())
	if err != nil {
		return Bounds{}, nil, 0, nil, err
	}
	p.upload = DefaultBounds()
	for _, v := range up.GetItems() {
		if v.GetActive() {
			p.upload = boundsFrom(v)
		}
	}

	pl, err := s.d.Own.PlacementPolicy().List(ctx, api.PlacementPolicyListRequest_builder{Size: 100}.Build())
	if err != nil {
		return Bounds{}, nil, 0, nil, err
	}
	p.place, p.placeV = nil, 0
	for _, v := range pl.GetItems() {
		if v.GetActive() {
			p.place, p.placeV = v.GetParams(), v.GetVersion()
		}
	}

	ad, err := s.d.Own.AddressPolicy().List(ctx, api.AddressPolicyListRequest_builder{Size: 100}.Build())
	if err != nil {
		return Bounds{}, nil, 0, nil, err
	}
	p.address = nil
	for _, v := range ad.GetItems() {
		if v.GetActive() {
			p.address = v.GetParams()
		}
	}

	p.at = now

	return p.upload, p.place, p.placeV, p.address, nil
}

// invalidatePolicies drops the cache after an activation.
func (s Core) invalidatePolicies() {
	s.d.pol.mu.Lock()
	s.d.pol.at = time.Time{}
	s.d.pol.mu.Unlock()
}

func clampD(v, lo, hi time.Duration) time.Duration {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}

	return v
}

func maxSinkCapacity(p *api.PlacementParams) int64 {
	if p != nil && p.GetMaxSinkCapacity() > 0 {
		return p.GetMaxSinkCapacity()
	}

	return DefaultMaxSinkCapacity
}

func forecastMargin(p *api.PlacementParams) float64 {
	if p != nil && p.GetForecastMargin() > 0 {
		return p.GetForecastMargin()
	}

	return DefaultForecastMargin
}

// retentionOf is a set's retention with the defaults filled in (§20.2).
func retentionOf(set *api.Set) (expire time.Duration, del time.Duration, none bool) {
	expire = DefaultRetentionExpire
	r := set.GetRetention()
	if r.GetExpireSeconds() > 0 {
		expire = time.Duration(r.GetExpireSeconds()) * time.Second
	}
	del = expire
	if r.GetDeleteNone() {
		return expire, 0, true
	}
	if r.GetDeleteSeconds() > 0 {
		del = time.Duration(r.GetDeleteSeconds()) * time.Second
		if del < expire {
			del = expire
		}
	}

	return expire, del, false
}

// epochOf is a set's epoch with the default filled in (§11).
func epochOf(set *api.Set) time.Duration {
	if e := set.GetPlacement().GetEpochSeconds(); e > 0 {
		return time.Duration(e) * time.Second
	}

	return DefaultEpoch
}

func spreadOf(set *api.Set) api.SetSpread {
	if v := set.GetPlacement().GetSetSpread(); v != api.SetSpread_SET_SPREAD_UNSPECIFIED {
		return v
	}

	return api.SetSpread_SET_SPREAD_SPREAD
}
