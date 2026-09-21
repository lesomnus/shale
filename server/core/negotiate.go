package core

import (
	"context"
	"fmt"
	"math"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
)

// The upload profile negotiation of §12.6: the producer proposes, the CP
// clamps every value into the active UploadPolicy's bounds and the set's own
// caps, stores the result, and answers it with the adjustments it made.

type coreSet struct {
	Core
	api.SetServiceServer
}

func (s Core) Set() api.SetServiceServer {
	return coreSet{s, s.Next().Set()}
}

// segmentDefaults fills a proposal with the defaults a set that never
// negotiated uses: 64 MB ÷ max_bitrate, a 2 s keyframe interval.
func segmentDefaults(p *api.SegmentProfile, b Bounds) *api.SegmentProfile {
	out := api.SegmentProfile_builder{
		MaxBitrate:         p.GetMaxBitrate(),
		DurationSeconds:    p.GetDurationSeconds(),
		KeyframeIntervalMs: p.GetKeyframeIntervalMs(),
	}.Build()
	if out.GetKeyframeIntervalMs() == 0 {
		out.SetKeyframeIntervalMs(b.KeyframeDefault.Milliseconds())
	}

	return out
}

// clampSegment is the per-source half of negotiation. `epoch` bounds the
// duration from above (§25); `totalScale` < 1 applies a set-level cap.
func clampSegment(p *api.SegmentProfile, b Bounds, epoch time.Duration, adjust func(field, proposed, agreed, reason string)) (*api.SegmentProfile, error) {
	br := p.GetMaxBitrate()
	if br <= 0 {
		return nil, status.Error(codes.InvalidArgument, "max_bitrate: a source must declare its ceiling")
	}
	if br > b.MaxBitrateCap {
		adjust("max_bitrate", fmt.Sprint(br), fmt.Sprint(b.MaxBitrateCap), "the policy caps any single max_bitrate")
		br = b.MaxBitrateCap
	}

	kf := p.GetKeyframeIntervalMs()
	if kf == 0 {
		kf = b.KeyframeDefault.Milliseconds()
	}
	if c := clampD(time.Duration(kf)*time.Millisecond, b.KeyframeMin, b.KeyframeMax); c.Milliseconds() != kf {
		adjust("keyframe_interval", fmt.Sprint(kf), fmt.Sprint(c.Milliseconds()), "outside the policy's bounds")
		kf = c.Milliseconds()
	}

	// Bytes per second at the ceiling.
	bps := float64(br) / 8

	dur := p.GetDurationSeconds()
	proposed := dur
	if dur <= 0 {
		dur = int64(math.Round(float64(b.TargetLamina) / bps))
	}

	// The ceiling max_bitrate × duration should be at least min_lamina and
	// must be at most max_lamina. Only the duration moves: the camera's
	// ceiling is not the CP's to change.
	minDur := int64(math.Ceil(float64(b.MinLamina) / bps))
	maxDur := int64(math.Floor(float64(b.MaxLamina) / bps))
	epochCap := int64(epoch.Seconds() / 4)

	if dur < minDur {
		dur = minDur
	}
	if dur > maxDur {
		dur = maxDur
	}
	// The epoch rule wins: the floor is a target, not a bound (§12.6).
	if dur > epochCap {
		dur = epochCap
	}
	if dur < 1 {
		dur = 1
	}
	if proposed > 0 && dur != proposed {
		adjust("segment_duration", fmt.Sprint(proposed), fmt.Sprint(dur), "keeps max_bitrate × duration within the lamina size bounds and a quarter of the epoch")
	}

	return api.SegmentProfile_builder{
		MaxBitrate:         br,
		DurationSeconds:    dur,
		KeyframeIntervalMs: kf,
	}.Build(), nil
}

// clampLink is the per-set half.
func clampLink(l *api.LinkProfile, b Bounds, adjust func(field, proposed, agreed, reason string)) *api.LinkProfile {
	mode := l.GetMode()
	if mode == api.UploadMode_UPLOAD_MODE_UNSPECIFIED {
		mode = b.DefaultMode
	}

	idle := time.Duration(l.GetIdleTimeoutSeconds()) * time.Second
	if idle == 0 {
		idle = b.IdleDefault
	}
	if c := clampD(idle, b.IdleMin, b.IdleMax); c != idle {
		adjust("idle_timeout", idle.String(), c.String(), "outside the policy's bounds")
		idle = c
	}

	abandon := time.Duration(l.GetAbandonTimeoutSeconds()) * time.Second
	if abandon == 0 {
		abandon = b.AbandonDefault
	}
	if c := clampD(abandon, b.AbandonMin, b.AbandonMax); c != abandon {
		adjust("abandon_timeout", abandon.String(), c.String(), "outside the policy's bounds")
		abandon = c
	}
	if abandon < 2*idle {
		adjust("abandon_timeout", abandon.String(), (2 * idle).String(), "at least twice idle_timeout, so the two cannot be inverted")
		abandon = 2 * idle
	}

	horizon := time.Duration(l.GetAllocationHorizonSeconds()) * time.Second
	if horizon == 0 {
		horizon = b.HorizonDefault
	}
	if c := clampD(horizon, b.HorizonMin, b.HorizonMax); c != horizon {
		adjust("allocation_horizon", horizon.String(), c.String(), "outside the policy's bounds")
		horizon = c
	}

	return api.LinkProfile_builder{
		Mode:                     mode,
		IdleTimeoutSeconds:       int64(idle.Seconds()),
		AbandonTimeoutSeconds:    int64(abandon.Seconds()),
		AllocationHorizonSeconds: int64(horizon.Seconds()),
	}.Build()
}

// MaxLength is what a stream at its ceiling can produce for one segment:
// max_bitrate × (duration + keyframe interval + one encoder burst) (§12.6).
func MaxLength(p *api.SegmentProfile) int64 {
	secs := float64(p.GetDurationSeconds()) + float64(p.GetKeyframeIntervalMs())/1000 + EncoderBurst.Seconds()

	return int64(math.Ceil(float64(p.GetMaxBitrate()) / 8 * secs))
}

// linkOf is a set's agreed link profile, or the defaults.
func linkOf(set *api.Set, b Bounds) *api.LinkProfile {
	l := set.GetLink()
	if l == nil || l.GetMode() == api.UploadMode_UPLOAD_MODE_UNSPECIFIED {
		return clampLink(l, b, func(string, string, string, string) {})
	}

	return l
}

// segmentOf is a source's agreed segment profile, or what the defaults make
// of its declared ceiling; nil when the source never declared one.
func segmentOf(src *api.Source, set *api.Set, b Bounds) *api.SegmentProfile {
	p := src.GetProfile()
	if p.GetMaxBitrate() <= 0 {
		return nil
	}
	if p.GetDurationSeconds() > 0 && p.GetKeyframeIntervalMs() > 0 {
		return p
	}

	out, err := clampSegment(segmentDefaults(p, b), b, epochOf(set), func(string, string, string, string) {})
	if err != nil {
		return nil
	}

	return out
}

func (s coreSet) Negotiate(ctx context.Context, req *api.SetNegotiateRequest) (*api.SetNegotiateResponse, error) {
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}

	b, _, _, _, err := s.policies(ctx)
	if err != nil {
		return nil, err
	}

	set, err := s.SetServiceServer.Get(ctx, api.SetGetRequest_builder{
		Ref: req.GetRef(),
	}.Build())
	if err != nil {
		return nil, err
	}

	if kindOf(f.Actor) == DomProducer {
		if err := s.producerOwns(ctx, f.Actor, set); err != nil {
			return nil, err
		}
	}

	var adjustments []*api.Adjustment
	adjustFor := func(source []byte) func(field, proposed, agreed, reason string) {
		return func(field, proposed, agreed, reason string) {
			adjustments = append(adjustments, api.Adjustment_builder{
				SourceId: source, Field: field, Proposed: proposed, Agreed: agreed, Reason: reason,
			}.Build())
		}
	}

	link := clampLink(req.GetLink(), b, adjustFor(nil))
	epoch := epochOf(set)

	// Every member, so a source left out of the proposal keeps what it had.
	members, err := s.members(ctx, set.GetId())
	if err != nil {
		return nil, err
	}
	byId := map[string]*api.Source{}
	for _, m := range members {
		byId[string(m.GetId())] = m
	}

	proposed := map[string]*api.SegmentProfile{}
	for _, sp := range req.GetSources() {
		src, err := s.Next().Source().Get(ctx, api.SourceGetRequest_builder{Ref: sp.GetSource()}.Build())
		if err != nil {
			return nil, err
		}
		if string(src.GetSet().GetId()) != string(set.GetId()) {
			return nil, invalid("sources", "a source of another set")
		}
		proposed[string(src.GetId())] = sp.GetProfile()
	}

	// Clamp each, then the set's total cap, which scales every ceiling
	// proportionally (§12.6).
	agreed := map[string]*api.SegmentProfile{}
	var total int64
	for id, m := range byId {
		p, ok := proposed[id]
		if !ok {
			p = m.GetProfile()
		}
		if p.GetMaxBitrate() <= 0 {
			// Never declared: nothing to agree on yet.
			continue
		}
		out, err := clampSegment(segmentDefaults(p, b), b, epoch, adjustFor(m.GetId()))
		if err != nil {
			return nil, err
		}
		agreed[id] = out
		total += out.GetMaxBitrate()
	}
	if cap := set.GetMaxBitrateTotal(); cap > 0 && total > cap {
		scale := float64(cap) / float64(total)
		for id, p := range agreed {
			br := int64(float64(p.GetMaxBitrate()) * scale)
			adjustFor([]byte(id))("max_bitrate", fmt.Sprint(p.GetMaxBitrate()), fmt.Sprint(br), "the set's max_bitrate_total caps the sum of its members' ceilings")
			p.SetMaxBitrate(br)
			again, err := clampSegment(p, b, epoch, func(string, string, string, string) {})
			if err != nil {
				return nil, err
			}
			agreed[id] = again
		}
	}

	version := set.GetProfileVersion()
	changed := !equalLink(set.GetLink(), link)
	for id, p := range agreed {
		if !equalSegment(byId[id].GetProfile(), p) {
			changed = true
		}
	}
	if changed || version == 0 {
		version++
	}

	err = s.tx(ctx, func(next api.Server) error {
		if changed || version != set.GetProfileVersion() {
			if _, err := next.Set().Patch(ctx, api.SetPatchRequest_builder{
				Ref:            api.SetRef_builder{Id: set.GetId()}.Build(),
				Link:           link,
				ProfileVersion: &version,
				DateUpdated:    set.GetDateUpdated(),
			}.Build()); err != nil {
				return err
			}
		}
		for id, p := range agreed {
			m := byId[id]
			if equalSegment(m.GetProfile(), p) {
				continue
			}
			if _, err := next.Source().Patch(ctx, api.SourcePatchRequest_builder{
				Ref:         api.SourceRef_builder{Id: m.GetId()}.Build(),
				Profile:     p,
				DateUpdated: m.GetDateUpdated(),
			}.Build()); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	var out []*api.SourceAgreed
	for _, m := range members {
		p, ok := agreed[string(m.GetId())]
		if !ok {
			continue
		}
		out = append(out, api.SourceAgreed_builder{
			SourceId:  m.GetId(),
			Ordinal:   m.GetOrdinal(),
			Profile:   p,
			MaxLength: MaxLength(p),
		}.Build())
	}

	resp := api.SetNegotiateResponse_builder{
		ProfileVersion: version,
		Link:           link,
		Sources:        out,
		Adjustments:    adjustments,
	}.Build()

	if kindOf(f.Actor) == DomProducer {
		if ra, err := s.relayAssignment(ctx, f.Actor, set); err == nil && ra != nil {
			resp.SetRelay(ra)
		}
	}

	return resp, nil
}

func equalLink(a, b *api.LinkProfile) bool {
	return a.GetMode() == b.GetMode() &&
		a.GetIdleTimeoutSeconds() == b.GetIdleTimeoutSeconds() &&
		a.GetAbandonTimeoutSeconds() == b.GetAbandonTimeoutSeconds() &&
		a.GetAllocationHorizonSeconds() == b.GetAllocationHorizonSeconds()
}

func equalSegment(a, b *api.SegmentProfile) bool {
	return a.GetMaxBitrate() == b.GetMaxBitrate() &&
		a.GetDurationSeconds() == b.GetDurationSeconds() &&
		a.GetKeyframeIntervalMs() == b.GetKeyframeIntervalMs()
}

// members lists a set's sources in ordinal order.
func (s Core) members(ctx context.Context, setId []byte) ([]*api.Source, error) {
	var out []*api.Source
	after := ""
	for {
		vs, err := s.Next().Source().List(ctx, api.SourceListRequest_builder{
			Filters: []*api.SourceFilter{api.SourceFilter_builder{Set: api.SetRef_builder{Id: setId}.Build()}.Build()},
			Size:    500,
			After:   after,
		}.Build())
		if err != nil {
			return nil, err
		}
		out = append(out, vs.GetItems()...)
		if vs.GetNext() == "" {
			break
		}
		after = vs.GetNext()
	}

	// Ordinal order.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1].GetOrdinal() > out[j].GetOrdinal(); j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}

	return out, nil
}

// producerOwns refuses a producer acting on a set that is not its own.
func (s Core) producerOwns(ctx context.Context, producer pdid.Id, set *api.Set) error {
	p, err := s.d.Own.Producer().Get(ctx, api.ProducerGetRequest_builder{
		Ref: api.ProducerRef_builder{Id: producer.Bytes()}.Build(),
	}.Build())
	if err != nil {
		return err
	}
	if string(p.GetSet().GetId()) != string(set.GetId()) {
		return status.Errorf(codes.PermissionDenied, "this producer records another set")
	}

	return nil
}
