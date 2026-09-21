// Package placement chooses sinks for new laminae (§11, and the decision
// report's D1-D9).
//
// It is a pure function of the key, the cluster as the caller describes it,
// and the weights, so every Control Plane replica answers alike without
// coordinating. Rankings are computed over every node and device, eligible
// or not; eligibility only decides which entries are skipped, so a node that
// fails mid-epoch moves only the members that were on it.
package placement

import (
	"encoding/binary"
	"math"
	"sort"

	"github.com/cespare/xxhash/v2"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
)

// Sink is one target with its weight.
type Sink struct {
	Id     pdid.Id
	Device pdid.Id
	Node   pdid.Id
	// Weight is the sink's capacity, clamped and scaled by health (§27); the
	// caller decides it.
	Weight float64
	// Eligible says whether new writes may go here now (D7).
	Eligible bool
}

// Cluster is every sink of every device of every node, eligible or not.
type Cluster struct {
	Sinks []Sink
}

// Key is what a set's placement is keyed by (D3).
type Key struct {
	Set     pdid.Id
	Epoch   int64
	Version int64
}

// Member is the source being placed.
type Member struct {
	Source  pdid.Id
	Ordinal int
}

type node struct {
	id       pdid.Id
	weight   float64
	eligible bool
	devices  []*device
}

type device struct {
	id       pdid.Id
	weight   float64
	eligible bool
	sinks    []Sink
}

// Rank answers the ranked candidate sinks for one member: the first is the
// target, the rest the retry order (D5, D7). Ineligible sinks are left out;
// an empty answer means nothing can take the write.
func Rank(c Cluster, k Key, m Member, spread api.SetSpread) []Sink {
	nodes := group(c)
	if len(nodes) == 0 {
		return nil
	}

	var (
		nodeKey   = keyOf(k.Set, k.Epoch, k.Version)
		sourceKey = keyOf(m.Source, k.Epoch, k.Version)
		ordinal   = m.Ordinal
	)

	switch spread {
	case api.SetSpread_SET_SPREAD_PACK:
		ordinal = 0
	case api.SetSpread_SET_SPREAD_NONE:
		nodeKey = sourceKey
		ordinal = 0
	}

	// 1. The node ranking, keyed by the set, weighted by the node's capacity.
	sort.SliceStable(nodes, func(i, j int) bool {
		return score(nodeKey, nodes[i].id, nodes[i].weight) > score(nodeKey, nodes[j].id, nodes[j].weight)
	})

	N := len(nodes)
	p := ordinal % N
	q := (ordinal / N)

	var out []Sink
	for step := 0; step < N; step++ {
		n := nodes[(p+step)%N]
		if !n.eligible {
			continue
		}

		// 2. The device ranking within the node, keyed by the set again so
		// members wrapping onto one node take different positions.
		sort.SliceStable(n.devices, func(i, j int) bool {
			return score(nodeKey, n.devices[i].id, n.devices[i].weight) > score(nodeKey, n.devices[j].id, n.devices[j].weight)
		})

		Nd := len(n.devices)
		if Nd == 0 {
			continue
		}
		start := q % Nd
		if step > 0 {
			// A fallback node: its own position for this member is where the
			// walk begins, so a member displaced from its node lands next to
			// a sibling only when nothing else is free (soft).
			start = q % Nd
		}

		for ds := 0; ds < Nd; ds++ {
			d := n.devices[(start+ds)%Nd]
			if !d.eligible {
				continue
			}

			// 3. The sink, keyed by the source, so two members that share a
			// device still spread over its sinks.
			sinks := append([]Sink(nil), d.sinks...)
			sort.SliceStable(sinks, func(i, j int) bool {
				return score(sourceKey, sinks[i].Id, sinks[i].Weight) > score(sourceKey, sinks[j].Id, sinks[j].Weight)
			})

			for _, s := range sinks {
				if s.Eligible {
					out = append(out, s)
				}
			}
		}
	}

	return out
}

// group builds the node → device → sink tree the rankings walk. A node is
// eligible when any of its devices is, and a device when any of its sinks is,
// so the walk only descends into what can take a write.
func group(c Cluster) []*node {
	byNode := map[pdid.Id]*node{}
	// Devices are keyed per node: a device the CP still records on one
	// node while a sink of it was re-homed to another (§28.3) is a device
	// of each, with each node's own sinks, and every node in the ranking
	// has at least one device to walk.
	byDevice := map[[2]pdid.Id]*device{}
	var nodes []*node

	for _, s := range c.Sinks {
		n, ok := byNode[s.Node]
		if !ok {
			n = &node{id: s.Node}
			byNode[s.Node] = n
			nodes = append(nodes, n)
		}

		d, ok := byDevice[[2]pdid.Id{s.Node, s.Device}]
		if !ok {
			d = &device{id: s.Device}
			byDevice[[2]pdid.Id{s.Node, s.Device}] = d
			n.devices = append(n.devices, d)
		}

		d.sinks = append(d.sinks, s)
		d.weight += s.Weight
		n.weight += s.Weight
		if s.Eligible {
			d.eligible = true
			n.eligible = true
		}
	}

	// A deterministic order before sorting, so ties (equal scores are all but
	// impossible, but a stable sort needs a stable input) are resolved alike
	// on every replica.
	sort.Slice(nodes, func(i, j int) bool { return less(nodes[i].id, nodes[j].id) })
	for _, n := range nodes {
		sort.Slice(n.devices, func(i, j int) bool { return less(n.devices[i].id, n.devices[j].id) })
		for _, d := range n.devices {
			sort.Slice(d.sinks, func(i, j int) bool { return less(d.sinks[i].Id, d.sinks[j].Id) })
		}
	}

	return nodes
}

func less(a, b pdid.Id) bool {
	for i := range a {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}

	return false
}

func keyOf(id pdid.Id, epoch, version int64) []byte {
	b := make([]byte, 0, 32)
	b = append(b, id[:]...)
	b = binary.BigEndian.AppendUint64(b, uint64(epoch))
	b = binary.BigEndian.AppendUint64(b, uint64(version))

	return b
}

// score is weighted rendezvous hashing (D1): -w / ln(u), with u drawn from
// the hash of the key and the target, uniform in (0, 1). A weight of zero
// scores lowest of all, so a sink nobody sized still has a defined place.
func score(key []byte, target pdid.Id, weight float64) float64 {
	h := xxhash.New()
	h.Write(key)
	h.Write(target[:])
	v := h.Sum64()

	// 53 bits of mantissa, and never exactly zero, so ln(u) is finite and
	// negative.
	u := (float64(v>>11) + 0.5) / float64(uint64(1)<<53)
	if weight <= 0 {
		return math.Inf(-1)
	}

	return -weight / math.Log(u)
}

// Epoch is the bucket a data time falls in, in seconds since the UNIX epoch,
// for an epoch length in seconds (D3).
func Epoch(unixSeconds, epochSeconds int64) int64 {
	if epochSeconds <= 0 {
		epochSeconds = 3600
	}

	return (unixSeconds / epochSeconds) * epochSeconds
}
