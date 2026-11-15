package upload

import (
	"encoding/base64"
	"errors"
	"net/http"

	"google.golang.org/protobuf/proto"

	"github.com/lesomnus/shale/api"
)

// The satellite's side of the upload protocol (§41.3, §41.9): what a
// satellite adds to a Storage Node's paths and headers, which a node never
// sends and never sees.

const (
	// HdrSatellite is on every answer the satellite makes itself, rather
	// than passes on from a node, and says why (§41.3). A node never sends
	// it, so a producer tells the satellite's refusals from a node's by it.
	HdrSatellite = "Shale-Satellite"
	// HdrProducer names the calling producer in development mode, which is
	// plaintext and has no client certificate to read it from (§34.7), as
	// the plain header names the caller to the CP. Never read otherwise.
	HdrProducer = "Shale-Producer"

	// HdrRetain is on a producer's request under `retain: accepted`
	// (§41.9): it asks the satellite to take the bytes itself.
	HdrRetain = "Shale-Retain"
	// HdrHandoff carries the Handoff, base64url without padding, on the
	// requests of a segment the satellite holds nothing of yet: the
	// allocation with every candidate and its token, and the producer's
	// limits.
	HdrHandoff = "Shale-Handoff"
	// HdrAccepted is on a satellite's answer that it holds the segment's
	// bytes below this offset: the producer may let them go. With 204 more
	// is to come; with 202 the satellite holds the whole segment, and it is
	// handed off.
	HdrAccepted = "Shale-Accepted"
	// HdrPass is on a node's answer the satellite passed through rather
	// than took the bytes of, and says why: the producer keeps them, as
	// under `committed`.
	HdrPass = "Shale-Pass"
)

// RetainAccepted is HdrRetain's value.
const RetainAccepted = "accepted"

// The reasons HdrSatellite gives.
const (
	// ReasonUnassigned: the caller is not a producer this satellite
	// serves. The producer's failure is the satellite's, not the node's.
	ReasonUnassigned = "unassigned"
	// ReasonBusy: the pool has no room; 503 with Retry-After, as a busy
	// node answers.
	ReasonBusy = "busy"
	// ReasonToken: the token is not one the node it names would take; the
	// status is the one that node would answer.
	ReasonToken = "token"
	// ReasonUnreachable: the node the token names could not be reached
	// from here, or the CP gave no way to it.
	ReasonUnreachable = "unreachable"
	// ReasonRequest: a method or a path the upload paths do not have.
	ReasonRequest = "request"
)

// The reasons HdrPass gives.
const (
	// PassRefused: the satellite does not take `accepted`
	// (`satellite.accept: false`).
	PassRefused = "refused"
	// PassFull: the retained pool has no room for the segment.
	PassFull = "full"
	// PassShare: the producer holds its share of the pool already.
	PassShare = "share"
	// PassUnrecorded: the CP did not record the satellite as the lamina's
	// holder, or could not be asked.
	PassUnrecorded = "unrecorded"
)

// EncodeHandoff is a Handoff as HdrHandoff carries it.
func EncodeHandoff(h *api.Handoff) (string, error) {
	b, err := proto.Marshal(h)
	if err != nil {
		return "", err
	}

	return base64.RawURLEncoding.EncodeToString(b), nil
}

// DecodeHandoff reads HdrHandoff.
func DecodeHandoff(s string) (*api.Handoff, error) {
	if s == "" {
		return nil, errors.New("no handoff")
	}
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	h := &api.Handoff{}
	if err := proto.Unmarshal(b, h); err != nil {
		return nil, err
	}

	return h, nil
}

// SatelliteFailure says whether an answer, or its absence, is the
// satellite's failure rather than the node's: no answer at all from the
// satellite, or its refusal of this producer. Every other answer that
// comes through the satellite is the node's, or the satellite's in the
// node's words (a busy 503), and is handled as it always was. A request
// the producer itself gave up, its context ended, is neither's.
func SatelliteFailure(viaSatellite bool, resp *http.Response, err error) bool {
	if !viaSatellite {
		return false
	}
	if resp == nil {
		return err != nil
	}

	return resp.Header.Get(HdrSatellite) == ReasonUnassigned
}
