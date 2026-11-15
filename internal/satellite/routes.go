package satellite

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
)

// routes are where the satellite forwards what a token names for each node
// (§41.3): the node's data plane as the CP's address resolver gives it for
// the satellite's own network, asked for by node id and kept until the
// answer's expiry. It is the one place a request's destination comes from;
// nothing a caller sends names a host.
type routes struct {
	// fetch asks the CP.
	fetch func(ctx context.Context, node pdid.Id) (*api.SatelliteRouteResponse, error)
	// dev allows a plaintext data plane, which only development mode hands
	// out (§34.7).
	dev bool
	now func() time.Time

	mu     sync.Mutex
	byNode map[pdid.Id]route
	flight singleflight.Group
}

type route struct {
	base    string
	expires time.Time
}

// staleFor is how long past its expiry a route is still used while the CP
// cannot be asked: a node keeps its address, and a satellite that stopped
// forwarding whenever the CP was away would make it the weak link again.
const staleFor = time.Hour

var errNoRoute = errors.New("no route to the node")

func newRoutes(fetch func(ctx context.Context, node pdid.Id) (*api.SatelliteRouteResponse, error), dev bool) *routes {
	return &routes{fetch: fetch, dev: dev, now: time.Now, byNode: map[pdid.Id]route{}}
}

// base is scheme://host:port of the node's data plane.
func (r *routes) base(ctx context.Context, node pdid.Id) (string, error) {
	now := r.now()
	r.mu.Lock()
	cached, ok := r.byNode[node]
	r.mu.Unlock()
	if ok && now.Before(cached.expires) {
		return cached.base, nil
	}

	v, err, _ := r.flight.Do(node.String(), func() (any, error) {
		fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		resp, err := r.fetch(fctx, node)
		if err != nil {
			return nil, err
		}
		base, err := r.baseOf(resp.GetEndpoints())
		if err != nil {
			return nil, err
		}
		exp := r.now().Add(time.Minute)
		if e := resp.GetDateExpires(); e != nil && e.AsTime().After(r.now()) {
			exp = e.AsTime()
		}
		r.mu.Lock()
		r.byNode[node] = route{base: base, expires: exp}
		r.mu.Unlock()

		return base, nil
	})
	if err != nil {
		if ok && now.Before(cached.expires.Add(staleFor)) {
			return cached.base, nil
		}

		return "", fmt.Errorf("%w: %w", errNoRoute, err)
	}

	return v.(string), nil
}

// all is every node's route held now, expired or not.
func (r *routes) all() map[pdid.Id]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[pdid.Id]string, len(r.byNode))
	for id, v := range r.byNode {
		out[id] = v.base
	}

	return out
}

// baseOf is the first endpoint the CP gave that the satellite may use:
// https, or http in development mode.
func (r *routes) baseOf(eps []*api.Endpoint) (string, error) {
	for _, ep := range eps {
		switch ep.GetScheme() {
		case "https":
		case "http":
			if !r.dev {
				continue
			}
		default:
			continue
		}
		if ep.GetHost() == "" || ep.GetPort() <= 0 || ep.GetPort() > 65535 {
			continue
		}

		return ep.GetScheme() + "://" + net.JoinHostPort(ep.GetHost(), strconv.Itoa(int(ep.GetPort()))), nil
	}

	return "", errNoRoute
}
