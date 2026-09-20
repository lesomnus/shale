package placement

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
)

const (
	domNode   pdid.Domain = 12
	domDevice pdid.Domain = 13
	domSink   pdid.Domain = 14
	domSet    pdid.Domain = 7
	domSource pdid.Domain = 8
)

// cluster makes n nodes with d devices each and one sink per device.
func cluster(n, d int) Cluster {
	var c Cluster
	for range n {
		nid := pdid.New(domNode)
		for range d {
			did := pdid.New(domDevice)
			c.Sinks = append(c.Sinks, Sink{Id: pdid.New(domSink), Device: did, Node: nid, Weight: 16e12, Eligible: true})
		}
	}

	return c
}

func TestSpreadDistinctNodesThenDevices(t *testing.T) {
	x := require.New(t)

	c := cluster(4, 3)
	k := Key{Set: pdid.New(domSet), Epoch: 3600, Version: 1}

	nodes := map[pdid.Id]int{}
	devices := map[pdid.Id]int{}
	for i := range 8 {
		r := Rank(c, k, Member{Source: pdid.New(domSource), Ordinal: i}, api.SetSpread_SET_SPREAD_SPREAD)
		x.NotEmpty(r)
		x.Len(r, 12, "every eligible sink is a candidate")
		nodes[r[0].Node]++
		devices[r[0].Device]++
	}

	// 8 members over 4 nodes: every node holds exactly two.
	x.Len(nodes, 4)
	for _, v := range nodes {
		x.Equal(2, v)
	}
	// And all on distinct devices.
	x.Len(devices, 8)
}

func TestDeterministicAndStable(t *testing.T) {
	x := require.New(t)

	c := cluster(5, 2)
	k := Key{Set: pdid.New(domSet), Epoch: 7200, Version: 1}
	m := Member{Source: pdid.New(domSource), Ordinal: 2}

	a := Rank(c, k, m, api.SetSpread_SET_SPREAD_SPREAD)
	b := Rank(c, k, m, api.SetSpread_SET_SPREAD_SPREAD)
	x.Equal(a, b)

	// Another epoch usually moves the member.
	moved := 0
	for e := int64(0); e < 20; e++ {
		r := Rank(c, Key{Set: k.Set, Epoch: e * 3600, Version: 1}, m, api.SetSpread_SET_SPREAD_SPREAD)
		if r[0].Id != a[0].Id {
			moved++
		}
	}
	x.Greater(moved, 5)
}

func TestIneligibleMovesOnlyItsMembers(t *testing.T) {
	x := require.New(t)

	c := cluster(4, 2)
	k := Key{Set: pdid.New(domSet), Epoch: 3600, Version: 1}
	members := make([]Member, 4)
	for i := range members {
		members[i] = Member{Source: pdid.New(domSource), Ordinal: i}
	}

	before := make([]Sink, 4)
	for i, m := range members {
		before[i] = Rank(c, k, m, api.SetSpread_SET_SPREAD_SPREAD)[0]
	}

	// Take the node of member 1 down.
	down := before[1].Node
	for i := range c.Sinks {
		if c.Sinks[i].Node == down {
			c.Sinks[i].Eligible = false
		}
	}

	for i, m := range members {
		r := Rank(c, k, m, api.SetSpread_SET_SPREAD_SPREAD)
		x.NotEmpty(r)
		x.NotEqual(down, r[0].Node)
		if i != 1 {
			x.Equal(before[i].Id, r[0].Id, "member %d stayed", i)
		} else {
			x.NotEqual(before[i].Id, r[0].Id)
		}
		for _, s := range r {
			x.True(s.Eligible)
		}
	}
}

func TestPackAndNone(t *testing.T) {
	x := require.New(t)

	c := cluster(3, 2)
	k := Key{Set: pdid.New(domSet), Epoch: 3600, Version: 1}

	var packed []pdid.Id
	for i := range 4 {
		r := Rank(c, k, Member{Source: pdid.New(domSource), Ordinal: i}, api.SetSpread_SET_SPREAD_PACK)
		packed = append(packed, r[0].Id)
	}
	for _, id := range packed[1:] {
		x.Equal(packed[0], id, "pack puts the whole set on one sink")
	}

	// none: keyed by the source alone, so the ordinal does not matter.
	src := pdid.New(domSource)
	a := Rank(c, k, Member{Source: src, Ordinal: 0}, api.SetSpread_SET_SPREAD_NONE)
	b := Rank(c, k, Member{Source: src, Ordinal: 5}, api.SetSpread_SET_SPREAD_NONE)
	x.Equal(a[0].Id, b[0].Id)
}

func TestWeightsFollowCapacity(t *testing.T) {
	x := require.New(t)

	// Two nodes, one twice the size of the other: it should get about
	// twice the keys.
	c := cluster(2, 1)
	c.Sinks[0].Weight = 16e12
	c.Sinks[1].Weight = 32e12

	count := map[pdid.Id]int{}
	for range 2000 {
		k := Key{Set: pdid.New(domSet), Epoch: 0, Version: 1}
		r := Rank(c, k, Member{Source: pdid.New(domSource), Ordinal: 0}, api.SetSpread_SET_SPREAD_SPREAD)
		count[r[0].Id]++
	}

	small, big := float64(count[c.Sinks[0].Id]), float64(count[c.Sinks[1].Id])
	ratio := big / small
	x.InDelta(2.0, ratio, 0.35, "big/small = %.2f", ratio)
}

func TestEmpty(t *testing.T) {
	x := require.New(t)
	x.Nil(Rank(Cluster{}, Key{}, Member{}, api.SetSpread_SET_SPREAD_SPREAD))

	c := cluster(2, 1)
	for i := range c.Sinks {
		c.Sinks[i].Eligible = false
	}
	x.Empty(Rank(c, Key{}, Member{}, api.SetSpread_SET_SPREAD_SPREAD))
}

func TestEpoch(t *testing.T) {
	x := require.New(t)
	x.Equal(int64(3600), Epoch(3601, 3600))
	x.Equal(int64(7200), Epoch(7200, 3600))
	x.Equal(int64(3600), Epoch(3600, 0))
}
