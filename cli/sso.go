package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"

	"github.com/lesomnus/shale/cmd"
)

// `shale login --sso` (§33.1): the CLI signs an operator in through the
// deployment's issuer with the device grant of RFC 8628 -- it has no
// browser and holds no secret -- and hands the id_token that ends in to
// the control plane once, for the same session cookie a password would
// have minted. The token is not kept.
//
// The issuer and the CLI's client are the control plane's to say
// (`GET /session/ways`), so nothing about the issuer is configured here.

// LoginSso signs in through the issuer and keeps a session for each
// surface: the cluster API alone with `cluster`, else the tenant API and,
// when the CLI is configured for one, the cluster API too -- one code
// typed, two sessions.
func LoginSso(ctx context.Context, c *cmd.Config, out io.Writer, cluster bool) error {
	surfaces := []bool{false}
	if cluster {
		surfaces = []bool{true}
	} else if c.Client.ClusterAddr != "" || c.Client.ClusterWeb != "" {
		surfaces = append(surfaces, true)
	}

	ways, err := waysOf(ctx, c, apiAddr(c, surfaces[0]), surfaces[0])
	if err != nil {
		return err
	}
	if !ways.Sso || ways.Issuer == "" {
		return errors.New("this deployment has no single sign-on: sign in with `shale login @tenant/alias`")
	}
	if ways.DeviceClientId == "" {
		return errors.New("this deployment's single sign-on has no client for the CLI (auth.oidc.device_client_id)")
	}

	raw, err := deviceFlow(ctx, out, ways)
	if err != nil {
		return err
	}

	body, _ := json.Marshal(map[string]string{"id_token": raw})
	for _, cl := range surfaces {
		addr := apiAddr(c, cl)
		cookie, err := postForSession(ctx, c, addr, cl, "/sso/token", body)
		if err != nil {
			return err
		}
		if err := keepSession(addr, cookie); err != nil {
			return err
		}
		fmt.Fprintf(out, "signed in at %s\n", addr)
	}

	return nil
}

func waysOf(ctx context.Context, c *cmd.Config, addr string, cluster bool) (cmd.Ways, error) {
	base, plain, err := webBase(c, addr, cluster)
	if err != nil {
		return cmd.Ways{}, err
	}
	client, err := webClient(c, plain)
	if err != nil {
		return cmd.Ways{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/session/ways", nil)
	if err != nil {
		return cmd.Ways{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return cmd.Ways{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return cmd.Ways{}, fmt.Errorf("%s/session/ways: %s; a control plane from before single sign-on?", base, resp.Status)
	}
	var v cmd.Ways
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<16)).Decode(&v); err != nil {
		return cmd.Ways{}, err
	}

	return v, nil
}

// deviceFlow asks the issuer for a code, says where to confirm it, and
// waits for somebody to: the id_token at the end of it.
func deviceFlow(ctx context.Context, out io.Writer, ways cmd.Ways) (string, error) {
	p, err := oidc.NewProvider(ctx, ways.Issuer)
	if err != nil {
		return "", fmt.Errorf("the issuer %s: %w", ways.Issuer, err)
	}
	ep := p.Endpoint()
	if ep.DeviceAuthURL == "" {
		// Hydra's, for an issuer whose discovery does not say.
		ep.DeviceAuthURL = strings.TrimRight(ways.Issuer, "/") + "/oauth2/device/auth"
	}
	// A public client: its id in the body and no secret anywhere.
	ep.AuthStyle = oauth2.AuthStyleInParams
	cfg := &oauth2.Config{ClientID: ways.DeviceClientId, Endpoint: ep, Scopes: []string{oidc.ScopeOpenID, "profile", "email"}}

	da, err := cfg.DeviceAuth(ctx)
	if err != nil {
		return "", fmt.Errorf("the issuer would not start a device sign-in: %w", err)
	}
	to := da.VerificationURIComplete
	if to == "" {
		to = da.VerificationURI
	}
	fmt.Fprintf(out, "open %s\nand confirm the code %s\n", to, da.UserCode)

	tok, err := cfg.DeviceAccessToken(ctx, da)
	if err != nil {
		return "", fmt.Errorf("the sign-in did not complete: %w", err)
	}
	raw, ok := tok.Extra("id_token").(string)
	if !ok || raw == "" {
		return "", errors.New("the issuer answered no id_token: is `openid` among the device client's scopes?")
	}

	return raw, nil
}
