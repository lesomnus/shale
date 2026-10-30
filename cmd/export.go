package cmd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/exporthttp"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/shale/server/core"
)

// ExportConfig bounds the optional native MP4 download endpoint. Media passes
// through these CP replicas, so deployments must opt in and size their ingress.
type ExportConfig struct {
	Enabled bool `yaml:"enabled"`
	// PublicURL is the externally reachable endpoint, including its path.
	// Empty returns /exports/recording.mp4 relative to the tenant HTTP origin.
	PublicURL string `yaml:"public_url"`
	// MaxConcurrent and PerActor are per process; zero means 2 and 1.
	MaxConcurrent int `yaml:"max_concurrent"`
	PerActor      int `yaml:"per_actor"`
	// MaxDuration bounds planning and streaming together; zero is 30 min.
	MaxDuration time.Duration `yaml:"max_duration"`
}

func (c ExportConfig) endpoint() string {
	if c.PublicURL != "" {
		return c.PublicURL
	}
	return "/exports/recording.mp4"
}

func (c ExportConfig) check(dev bool) error {
	if c.MaxConcurrent < 0 || c.PerActor < 0 || c.MaxDuration < 0 {
		return errors.New("export limits cannot be negative")
	}
	if !c.Enabled {
		return nil
	}
	u, err := url.Parse(c.endpoint())
	if err != nil || u.User != nil || u.Fragment != "" || u.RawQuery != "" || u.Opaque != "" || u.Path == "" {
		return errors.New("export.public_url must be an HTTPS endpoint URL or absolute endpoint path without credentials, query or fragment")
	}
	if u.IsAbs() {
		if u.Host == "" || (u.Scheme != "https" && !(dev && u.Scheme == "http")) {
			return errors.New("export.public_url must use HTTPS (HTTP is development-only)")
		}
	} else if u.Host != "" || !strings.HasPrefix(u.Path, "/") || strings.HasPrefix(u.Path, "//") {
		return errors.New("export.public_url must be an absolute endpoint path or HTTPS URL")
	}
	return nil
}

func (s *Server) exportHandler() (http.Handler, error) {
	roots, err := pki.SystemRoots()
	if err != nil || roots == nil {
		roots = x509.NewCertPool()
	}
	if s.CA != nil {
		roots.AddCert(s.CA.Cert)
	}
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots},
		ForceAttemptHTTP2:     true,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		IdleConnTimeout:       90 * time.Second,
		MaxIdleConns:          16,
		MaxIdleConnsPerHost:   4,
		MaxConnsPerHost:       8,
		DisableCompression:    true,
	}
	client := &http.Client{Transport: transport}
	c := s.cfg.Export
	return &exporthttp.Handler{
		Resolve: func(ctx context.Context, ticket string) (*exporthttp.Request, error) {
			plan, err := core.New(s.Walled, s.Deps).ResolveExport(ctx, ticket)
			if err != nil {
				return nil, err
			}
			out := &exporthttp.Request{Source: plan.Source.GetAlias(), Actor: plan.Actor.String(), From: plan.From, To: plan.To}
			if out.Source == "" {
				out.Source = fmt.Sprintf("%x", plan.Source.GetId())
			}
			for _, row := range plan.Laminae {
				if row.GetState() != api.ReadState_READ_STATE_AVAILABLE || row.GetIncomplete() || row.GetUrl() == "" {
					continue
				}
				id := append([]byte(nil), row.GetLaminaId()...)
				size := row.GetSize()
				out.Recordings = append(out.Recordings, exporthttp.Recording{
					Start: row.GetDateStarted().AsTime(), Size: size, URL: row.GetUrl(),
					Refresh: func(ctx context.Context) (string, error) {
						fresh, err := plan.Refresh(ctx, id)
						if err != nil {
							return "", err
						}
						if fresh.GetSize() != size || fresh.GetState() != api.ReadState_READ_STATE_AVAILABLE || fresh.GetIncomplete() {
							return "", errors.New("the admitted recording is no longer available")
						}
						return fresh.GetUrl(), nil
					},
				})
			}
			return out, nil
		},
		Client: client, AllowHTTP: s.cfg.IsDev(), MaxConcurrent: c.MaxConcurrent, PerActor: c.PerActor, MaxDuration: c.MaxDuration,
	}, nil
}
