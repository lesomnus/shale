package relay

import (
	"context"

	"github.com/lesomnus/otx"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// The relay's instruments (§31, §39).
type metrics struct {
	attached metric.Int64Gauge
	active   metric.Int64Gauge
	viewers  metric.Int64Gauge
	sessions metric.Int64Counter
	// What comes in and goes out, how long a viewer waits for the first
	// frame, and the host.
	ingress    metric.Int64Counter
	egress     metric.Int64Counter
	recent     metric.Int64Counter
	rewind     metric.Int64Gauge
	firstFrame metric.Float64Histogram
	// How long a joining viewer took to catch up with the live edge, and
	// the packets viewers asked for again (§39.4).
	catchUp metric.Float64Histogram
	nacked  metric.Int64Counter
	// How long the pacer holds a fragment's earliest sample past the
	// fragment's arrival while somebody watches, and the times its clock
	// moved (§39.4).
	paceDelay  metric.Float64Histogram
	paceResets metric.Int64Counter
	// How long a producer held a payload before handing it to its stream,
	// how long the uplink took it from there, and the payloads whose two
	// clocks disagreed by more than the uplink took (§39.3).
	producerQueue metric.Float64Histogram
	uplink        metric.Float64Histogram
	skewed        metric.Int64Counter
	// How long a video sample waited here before a viewer was sent it,
	// and what timed viewers report of the rest of its way (§39.4).
	hold          metric.Float64Histogram
	viewerNetwork metric.Float64Histogram
	viewerPlayout metric.Float64Histogram
	viewerTotal   metric.Float64Histogram
	cpu           metric.Float64Gauge
}

func newMetrics(ctx context.Context) *metrics {
	o := otx.From(ctx)

	return &metrics{
		attached:      o.Int64Gauge("shale.relay.attached_producers", metric.WithDescription("producers with a stream open")),
		active:        o.Int64Gauge("shale.relay.active_sources", metric.WithDescription("sources being sent")),
		viewers:       o.Int64Gauge("shale.relay.viewers", metric.WithDescription("WHEP sessions open")),
		sessions:      o.Int64Counter("shale.relay.sessions", metric.WithDescription("sessions by outcome: started, renewed with a fresh token, or refused with the reason")),
		ingress:       o.Int64Counter("shale.relay.ingress_bytes", metric.WithDescription("TS bytes taken from producers"), metric.WithUnit("By")),
		egress:        o.Int64Counter("shale.relay.egress_bytes", metric.WithDescription("media bytes handed to viewers' tracks"), metric.WithUnit("By")),
		recent:        o.Int64Counter("shale.relay.recent_bytes", metric.WithDescription("bytes of the recent window handed out"), metric.WithUnit("By")),
		rewind:        o.Int64Gauge("shale.relay.rewind_bytes", metric.WithDescription("bytes the recent windows hold together"), metric.WithUnit("By")),
		firstFrame:    o.Float64Histogram("shale.relay.first_frame_ms", metric.WithDescription("from a session's offer to the connection over which its first frame goes"), metric.WithUnit("ms")),
		catchUp:       o.Float64Histogram("shale.relay.catch_up_ms", metric.WithDescription("from a session's keyframe to the end of its group of pictures, sent faster than real time, at the live edge"), metric.WithUnit("ms")),
		nacked:        o.Int64Counter("shale.relay.nacked_packets", metric.WithDescription("video packets viewers reported lost and asked for again")),
		paceDelay:     o.Float64Histogram("shale.relay.pace_delay_ms", metric.WithDescription("how long a fragment's earliest sample waits to be sent after the fragment came, while somebody watches: what pacing adds to the delay the fragment came with"), metric.WithUnit("ms")),
		paceResets:    o.Int64Counter("shale.relay.pace_resets", metric.WithDescription("times the pacing clock moved, by why: late, a sample more than 100 ms behind its time, after which every sample waits that much more; ahead, a jump of more than 3 s")),
		producerQueue: o.Float64Histogram("shale.relay.producer_queue_ms", metric.WithDescription("t2 - t1: how long a producer held a live payload, from having it to handing it to the relay stream, on the producer's clock, per producer host"), metric.WithUnit("ms")),
		uplink:        o.Float64Histogram("shale.relay.uplink_ms", metric.WithDescription("t3 - t2: from a producer handing a live payload to its stream to the relay receiving it, across the two hosts' clocks: the stream's buffers and the network, per producer host"), metric.WithUnit("ms")),
		skewed:        o.Int64Counter("shale.relay.uplink_skewed", metric.WithDescription("live payloads that arrived before the producer said it sent them: its clock is ahead of the relay's by more than the uplink takes, per producer host")),
		hold:          o.Float64Histogram("shale.relay.hold_ms", metric.WithDescription("t4 - t3: from a video sample's fragment arriving to the sample going to a viewer, per viewer and frame, by path: paced, sent at its time; catch_up, from the group of pictures a joining viewer is handed"), metric.WithUnit("ms")),
		viewerNetwork: o.Float64Histogram("shale.relay.viewer_network_ms", metric.WithDescription("t5 - t4, as timed viewers report it: from the relay sending a frame to the viewer having all of it"), metric.WithUnit("ms")),
		viewerPlayout: o.Float64Histogram("shale.relay.viewer_playout_ms", metric.WithDescription("t6 - t5, as timed viewers report it: from having a frame to showing it, the browser's jitter buffer, decoding and rendering"), metric.WithUnit("ms")),
		viewerTotal:   o.Float64Histogram("shale.relay.viewer_total_ms", metric.WithDescription("t6 - t1, as timed viewers report it: from the producer having a frame to a viewer showing it"), metric.WithUnit("ms")),
		cpu:           o.Float64Gauge("shale.relay.cpu", metric.WithDescription("one-minute load average over the CPU count")),
	}
}

func outcomeAttr(outcome string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("outcome", outcome))
}

// pathAttr is how a viewer got a frame: at its time, or catching up.
func pathAttr(owed bool) metric.MeasurementOption {
	if owed {
		return metric.WithAttributes(attribute.String("path", "catch_up"))
	}
	return metric.WithAttributes(attribute.String("path", "paced"))
}

func movedAttr(why string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("why", why))
}
