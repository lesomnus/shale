package core

import (
	"context"

	"github.com/lesomnus/shale/api"
)

// Rules on a site's generated verbs: its kind is one of those there are
// (§7). Who may write a site is the gate's, as for every generated verb.

type coreSite struct {
	Core
	api.SiteServiceServer
}

func (s Core) Site() api.SiteServiceServer {
	return coreSite{s, s.Next().Site()}
}

func (s coreSite) Add(ctx context.Context, req *api.SiteAddRequest) (*api.Site, error) {
	if err := knownVisibility(req.GetVisibility()); err != nil {
		return nil, err
	}

	return s.SiteServiceServer.Add(ctx, req)
}

func (s coreSite) Patch(ctx context.Context, req *api.SitePatchRequest) (*api.Site, error) {
	if req.HasVisibility() {
		if err := knownVisibility(req.GetVisibility()); err != nil {
			return nil, err
		}
	}

	return s.SiteServiceServer.Patch(ctx, req)
}

// knownVisibility refuses a kind there is no rule for: one the reads do
// not know would be MEMBERS to them and something else to whoever wrote it.
func knownVisibility(v api.SiteVisibility) error {
	switch v {
	case api.SiteVisibility_SITE_VISIBILITY_UNSPECIFIED, api.SiteVisibility_SITE_VISIBILITY_MEMBERS,
		api.SiteVisibility_SITE_VISIBILITY_COMPANY, api.SiteVisibility_SITE_VISIBILITY_EVERYBODY:
		return nil
	}

	return invalid("visibility", "a site is seen by its MEMBERS, the COMPANY's people or EVERYBODY")
}
