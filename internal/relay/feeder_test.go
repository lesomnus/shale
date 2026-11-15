package relay

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"
	"testing"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/proto"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/fmp4"
	"github.com/lesomnus/shale/internal/pki"
)

func (s *epochStream) stops(id pdid.Id) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, sent := range s.sent {
		if string(sent.GetStop().GetSourceId()) == string(id.Bytes()) {
			n++
		}
	}
	return n
}

// stream is a fresh attachment of the fixture's relay, as a new Attach makes.
func (f epochFixture) stream() *attachment {
	return &attachment{r: f.r, stream: &epochStream{ctx: f.t.Context()}, sources: map[pdid.Id]*source{}, grants: map[pdid.Id]*api.CaptureEpochGrant{}, pending: map[pdid.Id]bool{}, updating: true, feeds: map[pdid.Id]bool{}}
}

// feedClaims are a satellite's feed token for the producer's sources.
func (f epochFixture) feedClaims(sat pdid.Id, sources []pdid.Id, grants ...*api.CaptureEpochGrant) *api.TokenClaims {
	c := f.claims(sources, grants...)
	c.SetOp(api.TokenOp_TOKEN_OP_FEED)
	c.SetFeeder(sat.Bytes())
	return c
}

// authorizeAs signs claims and has a stream authorize them as the relay would
// at a Hello.
func (f epochFixture) authorizeAs(a *attachment, c *api.TokenClaims) (bool, error) {
	f.t.Helper()
	signed, err := f.key.Sign(proto.Clone(c).(*api.TokenClaims))
	require.NoError(f.t, err)
	verified, err := f.r.verify(signed, api.TokenOp_TOKEN_OP_PUBLISH, api.TokenOp_TOKEN_OP_FEED)
	if err != nil {
		return false, err
	}
	return a.authorize(verified)
}

func feederOfSource(s *source) feeder {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.feeder
}

// §39.3, §41.8: a satellite that feeds a producer's source is its feeder
// over the producer's own stream, which is told Stop and sends nothing; a
// fresh Hello of the producer's does not take the source back; the
// satellite's Feed without it does, and the producer's stream is started
// again.
func TestSatelliteFeedsOverProducer(t *testing.T) {
	f := newEpochFixture(t)
	id := pdid.New(8)
	ok, err := f.apply(f.claims([]pdid.Id{id}))
	require.NoError(t, err)
	require.True(t, ok)
	producer := f.a.stream.(*epochStream)
	s := f.r.sources.get(id)
	require.Equal(t, feeder(f.a), feederOfSource(s))
	require.Equal(t, 1, producer.starts(id), "always: the producer's stream is started")

	sat := f.stream()
	ok, err = f.authorizeAs(sat, f.feedClaims(pdid.New(DomSatellite), []pdid.Id{id}))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, feeder(f.a), feederOfSource(s), "a satellite that feeds nothing yet takes nothing")

	sat.feed(map[pdid.Id]bool{id: true})
	require.Equal(t, feeder(sat), feederOfSource(s))
	require.Equal(t, 1, producer.stops(id), "the producer's stream is told to stop")
	require.Equal(t, 1, sat.stream.(*epochStream).starts(id), "the satellite is started")

	// What the producer's stream still sends is not taken.
	s.feedEpoch(f.a, "", []byte("not an init"))
	s.mu.Lock()
	require.Zero(t, s.bytes)
	s.mu.Unlock()

	// A fresh token on the producer's stream changes nothing.
	f.at = f.at.Add(time.Second)
	ok, err = f.apply(f.claims([]pdid.Id{id}))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, feeder(sat), feederOfSource(s), "a producer's fresh Hello does not take a satellite's source")
	require.Equal(t, 1, producer.starts(id))

	// Handed back: the producer's stream feeds again.
	sat.feed(map[pdid.Id]bool{})
	require.Equal(t, feeder(f.a), feederOfSource(s))
	require.Equal(t, 1, sat.stream.(*epochStream).stops(id))
	require.Equal(t, 2, producer.starts(id))
}

// A satellite whose stream ends hands its sources to the producer's stream
// at once, started as they were; one whose token no longer names a source
// does as well.
func TestSatelliteGoneHandsBack(t *testing.T) {
	f := newEpochFixture(t)
	id := pdid.New(8)
	ok, err := f.apply(f.claims([]pdid.Id{id}))
	require.NoError(t, err)
	require.True(t, ok)
	producer := f.a.stream.(*epochStream)
	s := f.r.sources.get(id)
	satId := pdid.New(DomSatellite)

	sat := f.stream()
	ok, err = f.authorizeAs(sat, f.feedClaims(satId, []pdid.Id{id}))
	require.NoError(t, err)
	require.True(t, ok)
	sat.feed(map[pdid.Id]bool{id: true})
	require.Equal(t, feeder(sat), feederOfSource(s))
	s.detach(sat)
	require.Equal(t, feeder(f.a), feederOfSource(s))
	require.Equal(t, 2, producer.starts(id), "the producer's stream is started again")

	again := f.stream()
	ok, err = f.authorizeAs(again, f.feedClaims(satId, []pdid.Id{id}))
	require.NoError(t, err)
	require.True(t, ok)
	again.feed(map[pdid.Id]bool{id: true})
	require.Equal(t, feeder(again), feederOfSource(s))
	f.at = f.at.Add(time.Second)
	ok, err = f.authorizeAs(again, f.feedClaims(satId, nil))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, feeder(f.a), feederOfSource(s), "a token without the source hands it back")
	require.Equal(t, 3, producer.starts(id))
	s.mu.Lock()
	require.False(t, s.epochDenied)
	require.False(t, s.stopped)
	s.mu.Unlock()
}

// Of two satellites that feed a source, the newer: the one that said so
// last. The other is told Stop.
func TestNewerSatelliteFeeds(t *testing.T) {
	f := newEpochFixture(t)
	id := pdid.New(8)
	ok, err := f.apply(f.claims([]pdid.Id{id}))
	require.NoError(t, err)
	require.True(t, ok)
	s := f.r.sources.get(id)
	one, two := f.stream(), f.stream()
	for _, a := range []*attachment{one, two} {
		ok, err := f.authorizeAs(a, f.feedClaims(pdid.New(DomSatellite), []pdid.Id{id}))
		require.NoError(t, err)
		require.True(t, ok)
	}
	one.feed(map[pdid.Id]bool{id: true})
	two.feed(map[pdid.Id]bool{id: true})
	require.Equal(t, feeder(two), feederOfSource(s))
	require.Equal(t, 1, one.stream.(*epochStream).stops(id))
	// Saying it again changes nothing.
	two.feed(map[pdid.Id]bool{id: true})
	require.Equal(t, feeder(two), feederOfSource(s))
	two.feed(map[pdid.Id]bool{})
	require.Equal(t, feeder(one), feederOfSource(s), "the other satellite still feeds it")
}

// A satellite's stream takes data only as the feeder, and only for a
// capture its own grant names; one whose grant is older than the source's
// is not elected, and the producer's stream that holds the current grant
// feeds.
func TestSatelliteStaleGrantDoesNotFeed(t *testing.T) {
	f := newEpochFixture(t)
	id := pdid.New(8)
	ok, err := f.apply(f.claims([]pdid.Id{id}, epochGrant(id, "e-1", 1)))
	require.NoError(t, err)
	require.True(t, ok)
	s := f.r.sources.get(id)
	sat := f.stream()
	ok, err = f.authorizeAs(sat, f.feedClaims(pdid.New(DomSatellite), []pdid.Id{id}, epochGrant(id, "e-1", 1)))
	require.NoError(t, err)
	require.True(t, ok)
	sat.feed(map[pdid.Id]bool{id: true})
	require.Equal(t, feeder(sat), feederOfSource(s))

	// The producer begins the next capture: its grant is newer.
	f.at = f.at.Add(time.Second)
	ok, err = f.apply(f.claims([]pdid.Id{id}, epochGrant(id, "e-2", 2)))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, feeder(f.a), feederOfSource(s), "a satellite whose grant is older does not feed")

	// The producer's Reset is taken while the satellite feeds, once its
	// grant catches up.
	ok, err = f.authorizeAs(sat, f.feedClaims(sat.satellite, []pdid.Id{id}, epochGrant(id, "e-2", 2)))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, feeder(sat), feederOfSource(s))
	require.True(t, s.resetEpoch(f.a, f.a.grants[id], "e-2", true), "the producer's own stream stops its capture")
	s.mu.Lock()
	require.True(t, s.stopped)
	s.mu.Unlock()
}

// A feed token names its satellite, and a publish token none; a stream does
// not change from one kind to the other.
func TestFeedTokenRules(t *testing.T) {
	f := newEpochFixture(t)
	id := pdid.New(8)
	none := f.claims([]pdid.Id{id})
	none.SetOp(api.TokenOp_TOKEN_OP_FEED)
	_, err := f.authorizeAs(f.stream(), none)
	require.Error(t, err, "a feed token names a satellite")
	wrong := f.claims([]pdid.Id{id})
	wrong.SetFeeder(pdid.New(DomSatellite).Bytes())
	_, err = f.authorizeAs(f.stream(), wrong)
	require.Error(t, err, "a publish token names none")
	other := f.feedClaims(pdid.New(22), []pdid.Id{id})
	_, err = f.authorizeAs(f.stream(), other)
	require.Error(t, err, "a satellite, not another host")

	ok, err := f.apply(f.claims([]pdid.Id{id}))
	require.NoError(t, err)
	require.True(t, ok)
	f.at = f.at.Add(time.Second)
	_, err = f.apply(f.feedClaims(pdid.New(DomSatellite), []pdid.Id{id}))
	require.Error(t, err, "a producer's stream does not become a satellite's")

	// A producer's own stream feeds what its token names, and never says so.
	f.a.feed(map[pdid.Id]bool{})
	require.Equal(t, feeder(f.a), feederOfSource(f.r.sources.get(id)))
}

// The satellite that feeds a source is the one whose Data the source takes.
func TestSatelliteFeedTakesItsData(t *testing.T) {
	f := newEpochFixture(t)
	id := pdid.New(8)
	ok, err := f.apply(f.claims([]pdid.Id{id}))
	require.NoError(t, err)
	require.True(t, ok)
	s := f.r.sources.get(id)
	sat := f.stream()
	ok, err = f.authorizeAs(sat, f.feedClaims(pdid.New(DomSatellite), []pdid.Id{id}))
	require.NoError(t, err)
	require.True(t, ok)
	sat.feed(map[pdid.Id]bool{id: true})
	b, err := os.ReadFile("../fmp4/testdata/av.mp4")
	require.NoError(t, err)
	init, frags := splitAll(t, b)
	s.feedEpoch(sat, "", init.Bytes)
	for _, fr := range frags[:3] {
		s.feedEpoch(sat, "", fr.Bytes)
	}
	s.mu.Lock()
	require.NotNil(t, s.init)
	require.Positive(t, s.bytes)
	s.mu.Unlock()
}

func splitAll(t *testing.T, b []byte) (*fmp4.Init, []*fmp4.Fragment) {
	var init *fmp4.Init
	var frags []*fmp4.Fragment
	s := &fmp4.Splitter{Unit: func(u *fmp4.Unit) {
		if u.Init != nil {
			init = u.Init
		} else {
			frags = append(frags, u.Frag)
		}
	}}
	s.Write(b)
	require.NoError(t, s.Err())
	require.NotNil(t, init)
	return init, frags
}

// A feed token is good only from the satellite it names, as its client
// certificate says (§39.7).
func TestFeedFromTheSatelliteItNames(t *testing.T) {
	dir := t.TempDir()
	r, err := New(Config{StateDir: dir})
	require.NoError(t, err)
	now := time.Now()
	ca, err := pki.NewCA("test", now)
	require.NoError(t, err)
	require.NoError(t, pki.Store{Dir: dir}.SetBundle(ca.Bundle()))
	sat := pdid.New(DomSatellite)
	issue := func(id pdid.Id) *x509.Certificate {
		k, err := pki.NewHostKey()
		require.NoError(t, err)
		csr, err := k.CSR("host")
		require.NoError(t, err)
		c, err := ca.IssueHost(csr, id, pki.Names{}, now)
		require.NoError(t, err)
		return c
	}
	as := func(c *x509.Certificate) context.Context {
		state := tls.ConnectionState{}
		if c != nil {
			state.PeerCertificates = []*x509.Certificate{c}
		}
		return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: state}})
	}
	require.NoError(t, r.peerIs(as(issue(sat)), sat))
	require.Error(t, r.peerIs(as(issue(pdid.New(DomSatellite))), sat), "another satellite")
	require.Error(t, r.peerIs(as(nil), sat), "no certificate")
	other, err := pki.NewCA("other", now)
	require.NoError(t, err)
	k, _ := pki.NewHostKey()
	csr, _ := k.CSR("host")
	forged, err := other.IssueHost(csr, sat, pki.Names{}, now)
	require.NoError(t, err)
	require.Error(t, r.peerIs(as(forged), sat), "a certificate of another CA")
	require.Error(t, r.peerIs(context.Background(), sat), "no peer")
}
